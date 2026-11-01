import hashlib
import re
import sqlite3
import struct
from collections import namedtuple
from contextlib import closing
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from obskit.cycle import (
    Rollover,
    carried,
    month_manifest_name,
    plan_rollover,
    roll_over,
)
from obskit.gates import GateError
from obskit.manifest import Asset
from obskit.release import CreateOnceError, PriorStateError, fetch_prior
from obskit.sqlite import table_counts
from obskit.zst import compress_file, sha256_file
from toy_store import (
    BASE,
    BASE_DECEMBER,
    BASE_NOVEMBER,
    BASE_OCTOBER,
    CURRENT,
    DECEMBER,
    DELTA,
    LAST_TWO_DAYS,
    MANIFEST,
    MANIFEST_NOVEMBER,
    MANIFEST_OCTOBER,
    NOVEMBER,
    OCTOBER,
    PART_NOVEMBER,
    PART_OCTOBER,
    STATE,
    STATE_SCHEMA,
    TOY,
    ToyStream,
    sealed_name,
)


class Build:
    """Another zstd build than the one before: what it writes expands to the same bytes,
    but is not the same file, as a newer library's output may not be. It marks its
    output with a skippable frame, which every zstd reader passes over."""

    def __init__(self, label: str) -> None:
        self.label = label.encode()

    def __call__(self, src: Path, dst: Path) -> None:
        compress_file(src, dst)
        with open(dst, "ab") as out:
            out.write(struct.pack("<II", 0x184D2A50, len(self.label)) + self.label)


def test_names_match_the_registry():
    s = replace(TOY, stream="pypi-registry")
    assert s.state_db == "pypi-registry-state.db"
    assert s.open_delta_db == "pypi-registry-delta-open.db"
    assert s.delta_name(date(2026, 11, 1)) == "pypi-registry-delta-2026-11-01.db"
    assert s.base_state_name(2026, 11) == "pypi-registry-state-base-2026-11.db"
    assert s.month_part_name(2026, 11) == "pypi-registry-2026-11.db"
    assert month_manifest_name(2026, 11) == "manifest-2026-11.json"
    # A month or a day below 10 has two digits all the same, as the registry writes it.
    assert s.delta_name(date(2026, 1, 2)) == "pypi-registry-delta-2026-01-02.db"
    assert s.base_state_name(2026, 1) == "pypi-registry-state-base-2026-01.db"
    assert s.month_part_name(2026, 1) == "pypi-registry-2026-01.db"
    assert month_manifest_name(2026, 1) == "manifest-2026-01.json"


@pytest.mark.parametrize(
    ("state_tables", "named"),
    [
        (("meta",), "it leaves out items"),
        (("meta", "items", "puts"), "the state schema makes no puts"),
        (("meta", "items", "meta"), "it names meta more than once"),
        ((), "it leaves out items, meta"),
    ],
    ids=[
        "a table left out",
        "a table the state lacks",
        "a table named twice",
        "no table",
    ],
)
def test_store_must_compare_every_state_table(state_tables, named):
    # Each table of the state schema is named once, in any order. SQLite's own tables,
    # like the sqlite_sequence of an AUTOINCREMENT key, are not the stream's.
    replace(TOY, state_tables=("items", "meta"))
    counted = STATE_SCHEMA + "CREATE TABLE runs (n INTEGER PRIMARY KEY AUTOINCREMENT);"
    replace(TOY, state_schema=counted, state_tables=("meta", "items", "runs"))

    # A month close compares only the tables that the Store names, so a table left out
    # would pass a month close without being compared.
    with pytest.raises(ValueError, match=re.escape(named)):
        replace(TOY, state_tables=state_tables)


def test_plan_rollover_of_the_first_run_starts_the_months_base():
    # The first run starts the month's base and seals nothing.
    assert plan_rollover(None, date(2026, 11, 28)) == Rollover(None, None, (2026, 11))


def test_plan_rollover_on_the_open_day_does_nothing():
    # Another run on the open delta's day changes nothing.
    assert plan_rollover(date(2026, 11, 2), date(2026, 11, 2)) == Rollover(
        None, None, None
    )


def test_plan_rollover_on_a_later_day_seals_the_open_day():
    # A new day seals the open delta's day, in the same month.
    assert plan_rollover(date(2026, 11, 2), date(2026, 11, 3)) == Rollover(
        date(2026, 11, 2), None, None
    )
    # A day with no run in between is not sealed: there is nothing to seal for it.
    assert plan_rollover(date(2026, 11, 2), date(2026, 11, 5)) == Rollover(
        date(2026, 11, 2), None, None
    )


def test_plan_rollover_in_a_later_month_closes_the_open_month():
    # A new month also closes the open delta's month and starts the new one's base.
    assert plan_rollover(date(2026, 11, 30), date(2026, 12, 1)) == Rollover(
        date(2026, 11, 30), (2026, 11), (2026, 12)
    )
    assert plan_rollover(date(2026, 12, 31), date(2027, 1, 1)) == Rollover(
        date(2026, 12, 31), (2026, 12), (2027, 1)
    )
    # After a gap, the month of the open delta closes, and the base is today's month.
    assert plan_rollover(date(2026, 10, 15), date(2026, 12, 2)) == Rollover(
        date(2026, 10, 15), (2026, 10), (2026, 12)
    )
    # The same day of another month is a new month, not the same day.
    assert plan_rollover(date(2026, 11, 3), date(2026, 12, 3)) == Rollover(
        date(2026, 11, 3), (2026, 11), (2026, 12)
    )


@pytest.mark.parametrize(
    ("open_day", "today"),
    [(date(2026, 11, 3), date(2026, 11, 2)), (date(2027, 1, 1), date(2026, 12, 31))],
    ids=["a day back", "a year back"],
)
def test_plan_rollover_refuses_a_clock_that_went_back(open_day, today):
    # The open delta is from a later day than the run's clock gives. Sealing it under
    # today's name, or carrying it on, would file the rows under the wrong day.
    with pytest.raises(ValueError, match=str(open_day)):
        plan_rollover(open_day, today)


@pytest.mark.parametrize(
    ("case", "named"),
    [
        ("no state", STATE),
        ("no open delta", DELTA),
        ("open delta not state", DELTA),
        ("state in a month release", STATE),
        ("open delta of no day", DELTA),
        ("open delta of a month", DELTA),
    ],
)
def test_carried_needs_state_and_open_delta(tmp_path, case, named):
    # What a first run publishes: the state and an empty open delta, and the base.
    manifest = ToyStream(tmp_path).seed(BASE, {date(2026, 11, 2): []})
    state, delta, base = manifest.assets
    assert carried(TOY, manifest) == (state, delta)
    assets = {
        "no state": [delta, base],
        "no open delta": [state, base],
        "open delta not state": [state, replace(delta, role="public"), base],
        "state in a month release": [replace(state, tag=NOVEMBER), delta, base],
        "open delta of no day": [state, replace(delta, period=None), base],
        "open delta of a month": [state, replace(delta, period="2026-11"), base],
    }[case]

    # The prior state would otherwise be an empty one, or no state at all.
    with pytest.raises(PriorStateError, match=re.escape(named)):
        carried(TOY, replace(manifest, assets=assets))


def test_seal_reuses_prior_open_delta_bytes(tmp_path, monkeypatch):
    stream = ToyStream(tmp_path / "day")
    stream.seed(BASE, {date(2026, 11, 29): [("a", "3"), ("c", "4")]})
    prior = stream.entries()
    open_bytes = stream.github.asset_bytes(CURRENT, DELTA)
    stream.github.calls.clear()

    # Another run on the open delta's day goes on adding to the open delta.
    workdir, added = stream.roll_over(date(2026, 11, 29))

    assert (added, stream.writes()) == ([], [])
    assert table_counts(workdir / "kit-toy-delta-open.db") == {"puts": 2}

    # The next run has another zstd, which would compress the delta to other bytes.
    monkeypatch.setattr("obskit.release.compress_file", Build("b"))
    again = stream.folder("again") / DELTA
    Build("b")(stream.expand(CURRENT, DELTA), again)
    assert again.read_bytes() != open_bytes

    workdir, added = stream.roll_over(date(2026, 11, 30))

    # The sealed delta is the file that the last run published, byte for byte.
    sealed = sealed_name("2026-11-29")
    assert stream.assets(NOVEMBER) == sorted([BASE_NOVEMBER, sealed])
    assert stream.github.asset_bytes(NOVEMBER, sealed) == open_bytes
    (entry,) = added
    assert entry.asset_sha256 == prior[DELTA].asset_sha256
    assert entry == replace(
        prior[DELTA],
        name=sealed,
        tag=NOVEMBER,
        role="public",
        period="2026-11-29",
        closed=True,
    )
    # A new open delta, with no row yet, takes the name of the one that was sealed.
    assert table_counts(workdir / "kit-toy-delta-open.db") == {"puts": 0}

    # So is the base copy that the end of the month makes, with yet another build.
    # The last run of November published with build b, and the first of December has
    # build c.
    stream = ToyStream(tmp_path / "month")
    stream.seed(BASE, LAST_TWO_DAYS)
    prior = stream.entries()
    state_bytes = stream.github.asset_bytes(CURRENT, STATE)
    open_bytes = stream.github.asset_bytes(CURRENT, DELTA)
    monkeypatch.setattr("obskit.release.compress_file", Build("c"))

    _, added = stream.roll_over(date(2026, 12, 1))

    assert stream.github.asset_bytes(DECEMBER, BASE_DECEMBER) == state_bytes
    assert {asset.name: asset for asset in added}[BASE_DECEMBER] == replace(
        prior[STATE], name=BASE_DECEMBER, tag=DECEMBER, period="2026-12", closed=True
    )
    assert stream.github.asset_bytes(NOVEMBER, sealed_name("2026-11-30")) == open_bytes


def test_seal_refuses_bytes_that_differ_from_the_prior(tmp_path):
    stream = ToyStream(tmp_path)
    stream.seed(BASE, {date(2026, 11, 29): [("a", "3"), ("c", "4")]})
    workdir = stream.workdir()
    prior = fetch_prior(stream.gh, TOY.stream, workdir, bootstrap=False)
    state, delta = carried(TOY, prior)
    # The open delta's file is no longer the one that was published and checked: it
    # was compressed again, with another build, which gives other bytes. The seal
    # would add them to November's release for good, under an entry that gives the
    # published bytes (R20).
    packed = workdir / DELTA
    Build("b")(workdir / TOY.open_delta_db, packed)
    stream.github.calls.clear()
    differ = (
        f"{packed} has the sha256 {sha256_file(packed)}, but its entry gives "
        f"{delta.asset_sha256}"
    )

    with pytest.raises(RuntimeError, match=re.escape(differ)):
        roll_over(stream.gh, TOY, workdir, prior, (state, delta), date(2026, 11, 30))

    # Nothing is added to November's release, or written anywhere else.
    assert stream.writes() == []
    assert stream.assets(NOVEMBER) == [BASE_NOVEMBER]


def test_month_close_builds_month_part_and_new_base(tmp_path):
    stream = ToyStream(tmp_path)
    seeded = stream.seed(
        BASE,
        {
            date(2026, 11, 27): [("c", "3")],
            date(2026, 11, 28): [("a", "4"), ("d", "5")],
            date(2026, 11, 29): [("b", "6")],
            date(2026, 11, 30): [("e", "7")],
        },
    )
    # The manifest lists its assets in reverse, so the month's sealed deltas come newest
    # first. The replay takes them in day order all the same, or it could not give back
    # the state.
    last = replace(seeded, assets=seeded.assets[::-1])
    stream.replace_manifest(last)
    last_state = stream.github.asset_bytes(CURRENT, STATE)

    workdir, added = stream.roll_over(date(2026, 12, 1))

    days = ["2026-11-27", "2026-11-28", "2026-11-29", "2026-11-30"]
    month_assets = [BASE_NOVEMBER, *map(sealed_name, days), PART_NOVEMBER]
    assert stream.assets(NOVEMBER) == sorted([*month_assets, MANIFEST_NOVEMBER])
    assert stream.assets(DECEMBER) == [BASE_DECEMBER]
    # December starts from the state that November ended with, as published.
    assert stream.github.asset_bytes(DECEMBER, BASE_DECEMBER) == last_state
    # The part holds every row of the month's deltas. Each of them has puts, so the
    # merge met seqs from more than one delta.
    sealed = [table_counts(stream.expand(NOVEMBER, sealed_name(day))) for day in days]
    assert all(counts["puts"] for counts in sealed)
    merged = table_counts(stream.expand(NOVEMBER, PART_NOVEMBER))
    assert merged == {"puts": sum(counts["puts"] for counts in sealed)}
    # The rollover returns the entries of what it added, which the next manifest lists
    # with the closed assets of the last one.
    assert [asset.name for asset in added] == [
        sealed_name("2026-11-30"),
        PART_NOVEMBER,
        MANIFEST_NOVEMBER,
        BASE_DECEMBER,
    ]
    entries = {asset.name: asset for asset in [*last.assets, *added]}
    part = entries[PART_NOVEMBER]
    assert {table: info.rows for table, info in part.tables.items()} == merged
    assert (part.tag, part.role, part.period, part.closed) == (
        NOVEMBER,
        "public",
        "2026-11",
        True,
    )
    base = entries[BASE_DECEMBER]
    assert (base.tag, base.role, base.period, base.closed) == (
        DECEMBER,
        "state",
        "2026-12",
        True,
    )
    # The month's manifest lists the month's assets as the current manifest does.
    # The rest of it is the last manifest of the month, so it takes nothing from the
    # run but the part's entry, and a run with the same build writes the same bytes.
    month = stream.manifest(NOVEMBER, MANIFEST_NOVEMBER)
    assert month.assets == [entries[name] for name in month_assets]
    assert replace(month, assets=last.assets) == last
    data = stream.github.asset_bytes(NOVEMBER, MANIFEST_NOVEMBER)
    digest = hashlib.sha256(data).hexdigest()
    assert entries[MANIFEST_NOVEMBER] == Asset(
        name=MANIFEST_NOVEMBER,
        tag=NOVEMBER,
        role="public",
        format="json",
        asset_bytes=len(data),
        asset_sha256=digest,
        db_bytes=len(data),
        db_sha256=digest,
        tables={},
        period="2026-11",
        closed=True,
    )
    # With them, the stream has every closed asset that it should.
    assert sorted(name for name, asset in entries.items() if asset.closed) == sorted(
        [*month_assets, MANIFEST_NOVEMBER, BASE_DECEMBER]
    )
    # The close of a month holds a month of deltas, so what it has used goes as it
    # goes: the downloads of the base copy and the sealed deltas once they expand, the
    # replayed base once it checks, and each delta once the part has its rows. Left
    # are the files that the rollover added to the month releases and the part's
    # database, and what `fetch_prior` downloaded: the manifest, the state, and the
    # published files of the state and the open delta, which it keeps for R20. Only
    # the open delta's database is new: the prior one went into the part.
    assert sorted(path.name for path in workdir.iterdir()) == sorted(
        [
            MANIFEST,
            MANIFEST_NOVEMBER,
            "kit-toy-2026-11.db",
            PART_NOVEMBER,
            sealed_name("2026-11-30"),  # the upload, which is the published open delta
            BASE_DECEMBER,
            "kit-toy-state.db",
            STATE,
            "kit-toy-delta-open.db",
            DELTA,
        ]
    )
    assert table_counts(workdir / "kit-toy-delta-open.db") == {"puts": 0}


def test_month_close_logs_the_free_disk_as_it_starts_and_ends(
    tmp_path, monkeypatch, capsys
):
    Usage = namedtuple("Usage", "total used free")
    free = iter([12_345_678_901, 9_876_543_210])
    asked: list[tuple[Path, bool]] = []

    def disk_usage(path):
        # Where it is asked, and whether the month's part is made yet.
        asked.append((Path(path), (Path(path) / PART_NOVEMBER).exists()))
        return Usage(14_000_000_000, 0, next(free))

    monkeypatch.setattr("obskit.cycle.shutil.disk_usage", disk_usage)
    day = ToyStream(tmp_path / "day")
    day.seed(BASE, {date(2026, 11, 29): [("a", "3")]})
    month = ToyStream(tmp_path / "month")
    month.seed(BASE, LAST_TWO_DAYS)
    # A day that ends is not a month close, and says nothing of the disk.
    day.roll_over(date(2026, 11, 30))
    assert (asked, capsys.readouterr().err) == ([], "")

    workdir, _ = month.roll_over(date(2026, 12, 1))

    assert asked == [(workdir, False), (workdir, True)]
    assert capsys.readouterr().err.splitlines() == [
        f"month close 2026-11 starts: 12.3 GB free on {workdir}",
        f"month close 2026-11 ends: 9.9 GB free on {workdir}",
    ]


def test_month_close_replay_mismatch_fails(tmp_path):
    stream = ToyStream(tmp_path)
    stream.seed(BASE, LAST_TWO_DAYS)
    # Someone changes a value in November's base copy, and lists the changed copy in
    # the manifest, so that it downloads and checks. The month replays onto it, but
    # cannot give back the state that the runs built.
    base_db = stream.expand(NOVEMBER, BASE_NOVEMBER)
    with closing(sqlite3.connect(base_db)) as conn:
        conn.execute("UPDATE items SET value = 'changed' WHERE key = 'b'")
        conn.commit()
    packed = stream.folder("edited") / BASE_NOVEMBER
    compress_file(base_db, packed)
    stream.replace_asset(NOVEMBER, BASE_NOVEMBER, packed)
    manifest = stream.manifest()
    edited = [
        replace(
            asset,
            asset_bytes=packed.stat().st_size,
            asset_sha256=sha256_file(packed),
            db_bytes=base_db.stat().st_size,
            db_sha256=sha256_file(base_db),
        )
        if asset.name == BASE_NOVEMBER
        else asset
        for asset in manifest.assets
    ]
    stream.replace_manifest(replace(manifest, assets=edited))
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()

    # No reason could publish past it: what the run would publish could not be
    # restored from the month's releases.
    with pytest.raises(GateError, match="cannot be overridden") as caught:
        stream.roll_over(date(2026, 12, 1))

    (failure,) = caught.value.failures
    assert failure.rule == "replay"
    assert "differs from the state in items" in failure.detail
    # Nothing is published, to the current release or to any other: not the seal of
    # 11-30, nor the month's part, nor December's base.
    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held
    assert stream.assets(NOVEMBER) == sorted([BASE_NOVEMBER, sealed_name("2026-11-29")])
    assert stream.github.release(DECEMBER) is None


@pytest.mark.parametrize("case", ["not listed", "not in the release"])
def test_month_close_without_base_copy_fails(tmp_path, case):
    stream = ToyStream(tmp_path)
    manifest = stream.seed(BASE, LAST_TWO_DAYS)
    if case == "not listed":
        others = [asset for asset in manifest.assets if asset.name != BASE_NOVEMBER]
        stream.replace_manifest(replace(manifest, assets=others))
    else:
        stream.delete_asset(NOVEMBER, BASE_NOVEMBER)
    stream.github.calls.clear()

    # The month cannot be rebuilt to check it, so it cannot be closed.
    with pytest.raises(PriorStateError, match=re.escape(BASE_NOVEMBER)):
        stream.roll_over(date(2026, 12, 1))

    # Nothing is added: not the seal of 11-30, nor anything of December.
    assert stream.writes() == []
    assert stream.github.release(DECEMBER) is None


def test_gap_closes_only_the_open_month(tmp_path):
    stream = ToyStream(tmp_path)
    stream.seed(
        BASE, {date(2026, 10, 14): [("a", "3")], date(2026, 10, 15): [("c", "4")]}
    )
    last_state = stream.github.asset_bytes(CURRENT, STATE)

    # No run went on from mid-October until December 2.
    _, added = stream.roll_over(date(2026, 12, 2))

    # Days without a run have no delta, so only the open delta's day is sealed, and
    # only its month closes. November had no run, so it has no release, and December
    # begins from the state as October ended.
    october = [BASE_OCTOBER, sealed_name("2026-10-14"), sealed_name("2026-10-15")]
    assert stream.assets(OCTOBER) == sorted([*october, PART_OCTOBER, MANIFEST_OCTOBER])
    assert stream.github.release(NOVEMBER) is None
    assert stream.assets(DECEMBER) == [BASE_DECEMBER]
    assert stream.github.asset_bytes(DECEMBER, BASE_DECEMBER) == last_state
    assert [(asset.name, asset.tag, asset.period) for asset in added] == [
        (sealed_name("2026-10-15"), OCTOBER, "2026-10-15"),
        (PART_OCTOBER, OCTOBER, "2026-10"),
        (MANIFEST_OCTOBER, OCTOBER, "2026-10"),
        (BASE_DECEMBER, DECEMBER, "2026-12"),
    ]


def test_month_close_run_again_adds_the_same_files_only_with_the_same_build(
    tmp_path, monkeypatch
):
    stream = ToyStream(tmp_path)
    stream.seed(BASE, LAST_TWO_DAYS)
    _, added = stream.roll_over(date(2026, 12, 1))
    rolled = {
        NOVEMBER: sorted(
            [
                BASE_NOVEMBER,
                sealed_name("2026-11-29"),
                sealed_name("2026-11-30"),
                PART_NOVEMBER,
                MANIFEST_NOVEMBER,
            ]
        ),
        DECEMBER: [BASE_DECEMBER],
    }
    assert {tag: stream.assets(tag) for tag in rolled} == rolled
    kept = {
        (tag, name): stream.github.asset_bytes(tag, name)
        for tag, names in rolled.items()
        for name in names
    }
    stream.github.calls.clear()

    # The run stopped after its rollover, so it published nothing to the current
    # release, and the next run rolls over from the same prior. With the same zstd and
    # SQLite it builds the same files, finds them in place, and adds nothing.
    _, again = stream.roll_over(date(2026, 12, 1))

    assert again == added
    assert stream.writes() == []

    # With another zstd, the part that it builds afresh is other bytes. The part in the
    # release is never replaced, so the rollover stops, before it adds anything, for a
    # person to look at.
    monkeypatch.setattr("obskit.release.compress_file", Build("b"))

    with pytest.raises(CreateOnceError, match=PART_NOVEMBER):
        stream.roll_over(date(2026, 12, 1))

    assert stream.writes() == []
    assert {tag: stream.assets(tag) for tag in rolled} == rolled
    assert {key: stream.github.asset_bytes(*key) for key in kept} == kept
