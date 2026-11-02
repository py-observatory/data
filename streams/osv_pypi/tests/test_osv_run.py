import dataclasses
import json
import os
import tomllib
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from obskit import cycle
from obskit.emails import holds_address
from obskit.gates import GateError, GateFailure
from obskit.github import GitHub
from obskit.http import Http, UpstreamError
from obskit.manifest import TableInfo, Watermark
from obskit.release import fetch_assets
from obskit.sqlite import diff_tables, open_db
from osv_pypi.dump import MAX_MEMBER_BYTES
from osv_pypi.index import DUMP_URL, INDEX_URL, record_url
from osv_pypi.run import STORE, RunReport, StaleIndexError, main
from osv_pypi.store import DELTA_SCHEMA, STATE_SCHEMA, STATE_TABLES
from osv_run_helpers import (
    BASE_DECEMBER,
    BASE_NOVEMBER,
    CURRENT,
    DECEMBER,
    DELTA,
    MANIFEST,
    MANIFEST_NOVEMBER,
    NOVEMBER,
    PART_NOVEMBER,
    STATE,
    Stream,
    advisories,
    close_connections,  # autouse: it closes what each test of this module opened
    meta_of,
    record,
    sealed_name,
    written,
)

STREAM_TOML = Path(__file__).parents[1] / "stream.toml"
# The generation of the index that the first run of each test reads. Each later run
# reads a copy with a higher one.
GENERATION = 1793526521815797
# When the advisories of the first runs last changed, a later change, and the newest
# line of the index on 2026-11-01, which has nine fraction digits.
OLD = "2026-10-01T08:00:00Z"
LATER = "2026-10-20T12:00:00Z"
NEWEST = "2026-11-01T09:45:03.305960502Z"
# The time of the first run of most tests.
BOOTSTRAP_AT = "2026-11-02T06:00:00Z"


@pytest.fixture(autouse=True)
def no_sqlite_tmpdir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start each test with no SQLITE_TMPDIR, and put back what was there when it ends.
    `main` sets it for the rest of the process, which would carry it into later
    tests."""
    monkeypatch.setenv("SQLITE_TMPDIR", "")  # so that the original is put back
    monkeypatch.delenv("SQLITE_TMPDIR")


def upload_names(stream: Stream) -> list[str]:
    """The names of the assets uploaded, in order."""
    return [
        parse_qs(urlsplit(url).query)["name"][0]
        for method, url, _ in stream.github.calls
        if method == "POST" and url.startswith("https://uploads.github.com/")
    ]


# ---- the bootstrap, and the runs after it


def test_bootstrap_publishes_state_delta_base_and_manifest(tmp_path):
    vulnerability = "GHSA-boot-0000-0001"
    lagging = "GHSA-boot-0000-0002"  # its copy in the dump is older than the index's
    undumped = "GHSA-boot-0000-0003"  # listed, and not in the dump
    unlisted = "GHSA-boot-0000-0004"  # in the dump, and not listed
    malware = "MAL-2026-1"  # the kind of a record follows from its id
    withdrawn = "PYSEC-2026-1"
    withdrawn_at = "2026-09-30T00:00:00Z"
    listed = {
        vulnerability: NEWEST,
        lagging: LATER,
        undumped: LATER,
        malware: OLD,
        withdrawn: OLD,
    }
    dump = {
        vulnerability: record(vulnerability, NEWEST),
        malware: record(malware, OLD),
        withdrawn: record(withdrawn, OLD, withdrawn=withdrawn_at),
        lagging: record(lagging, OLD),
        unlisted: record(unlisted, OLD),
    }
    stream = Stream(tmp_path)

    report = stream.run(
        BOOTSTRAP_AT,
        GENERATION,
        listed,
        dump=dump,
        records={lagging: record(lagging, LATER), undumped: record(undumped, LATER)},
        bootstrap=True,
    )

    # The current release holds the state, the open delta and the manifest. The
    # month's release holds the state as the month began, which is the same bytes.
    assert stream.assets(CURRENT) == sorted([STATE, DELTA, MANIFEST])
    assert stream.assets(NOVEMBER) == [BASE_NOVEMBER]
    base_bytes = stream.github.asset_bytes(NOVEMBER, BASE_NOVEMBER)
    assert base_bytes == stream.github.asset_bytes(CURRENT, STATE)
    # The index comes first, then the dump. Then the records that the dump lacked are
    # fetched, by id: the newer version of the one whose copy lags, and the one that
    # the dump does not have.
    assert stream.requested() == [
        INDEX_URL,
        DUMP_URL,
        record_url(lagging),
        record_url(undumped),
    ]
    # Every listed record of the dump is a version in the first open delta, in the
    # dump's order, so the first month's part will hold the first version that the
    # stream saw of each advisory. The fetched versions and the cursor come after.
    assert written(stream.open(CURRENT, DELTA), 0) == [
        ("version", vulnerability, NEWEST),
        ("version", malware, OLD),
        ("version", withdrawn, OLD),
        ("version", lagging, OLD),
        ("version", lagging, LATER),
        ("version", undumped, LATER),
        ("cursor", GENERATION),
    ]
    # The state is that delta applied: each listed advisory is active, and first seen
    # at the bootstrap, which is not when it first appeared.
    state = stream.open(CURRENT, STATE)
    rows = state.execute(
        "SELECT id, status, first_seen, kind, modified, withdrawn FROM advisories"
    )
    assert {advisory_id: tuple(rest) for advisory_id, *rest in rows} == {
        vulnerability: ("active", BOOTSTRAP_AT, "vulnerability", NEWEST, None),
        lagging: ("active", BOOTSTRAP_AT, "vulnerability", LATER, None),
        undumped: ("active", BOOTSTRAP_AT, "vulnerability", LATER, None),
        malware: ("active", BOOTSTRAP_AT, "malware", OLD, None),
        withdrawn: ("active", BOOTSTRAP_AT, "vulnerability", OLD, withdrawn_at),
    }
    assert meta_of(state) == {
        "applied_seq": "7",
        "bootstrap_at": BOOTSTRAP_AT,
        "cursor": str(GENERATION),
    }

    manifest = stream.manifest()
    state_entry, delta_entry, base_entry = manifest.assets
    assert (state_entry.name, state_entry.tag, state_entry.role) == (
        STATE,
        CURRENT,
        "state",
    )
    assert (state_entry.period, state_entry.closed) == (None, False)
    assert state_entry.tables == {
        "meta": TableInfo(3, "snapshot"),
        "advisories": TableInfo(5, "snapshot"),
    }
    assert (delta_entry.name, delta_entry.tag, delta_entry.role) == (
        DELTA,
        CURRENT,
        "state",
    )
    assert (delta_entry.period, delta_entry.closed) == ("2026-11-02", False)
    assert delta_entry.tables == {
        "versions": TableInfo(6, "append"),
        "events": TableInfo(0, "events"),
        "cursors": TableInfo(1, "append"),
    }
    assert base_entry == dataclasses.replace(
        state_entry, name=BASE_NOVEMBER, tag=NOVEMBER, period="2026-11", closed=True
    )
    # The data goes through the newest time in the index, cut to the second, and the
    # cursor is the index's generation, as in the manifest of every later run.
    assert manifest.data_through == Watermark("2026-11-01T09:45:03Z", "instant")
    assert manifest.cursor == str(GENERATION)
    assert manifest.coverage == {
        "advisories": 5,
        "malware": 1,
        "withdrawn": 1,
        "removed": 0,
        "remaining": 0,
    }
    assert manifest.complete is True
    assert manifest.generated_at == BOOTSTRAP_AT
    licences = tomllib.loads(STREAM_TOML.read_text(encoding="utf-8"))["upstreams"]
    assert manifest.licences == licences
    assert (manifest.series_breaks, manifest.corrections, manifest.overrides) == (
        [],
        [],
        [],
    )
    # The dump's records are new to the stream, and so is the one that it lacked. The
    # one whose copy lagged changed since.
    assert report == RunReport(
        new=5,
        changed=1,
        removed=0,
        returned=0,
        remaining=0,
        requests=4,
        unparsed=[],
        gate_failures=[],
    )


def test_second_run_records_a_new_and_a_changed_advisory(tmp_path):
    kept, changed, new = "GHSA-keep-0000-0001", "GHSA-chng-0000-0001", "MAL-2026-2"
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT, GENERATION, {kept: OLD, changed: OLD})
    stream.osv.requests.clear()

    report = stream.run(
        "2026-11-02T12:00:00Z",
        GENERATION + 1,
        {kept: OLD, changed: NEWEST, new: NEWEST},
        records={changed: record(changed, NEWEST), new: record(new, NEWEST)},
    )

    # Only the records whose index entry moved are fetched, by id.
    assert stream.requested() == [INDEX_URL, record_url(changed), record_url(new)]
    # The day's open delta goes on after the bootstrap's three rows.
    delta = stream.open(CURRENT, DELTA)
    assert written(delta, 3) == [
        ("version", changed, NEWEST),
        ("version", new, NEWEST),
        ("cursor", GENERATION + 1),
    ]
    # The state is the delta applied, through its last seq. The new advisory was first
    # seen by this run.
    state = stream.open(CURRENT, STATE)
    assert advisories(state) == {
        kept: ("active", OLD),
        changed: ("active", NEWEST),
        new: ("active", NEWEST),
    }
    assert dict(state.execute("SELECT id, first_seen FROM advisories")) == {
        kept: BOOTSTRAP_AT,
        changed: BOOTSTRAP_AT,
        new: "2026-11-02T12:00:00Z",
    }
    assert meta_of(state) == {
        "applied_seq": "6",
        "bootstrap_at": BOOTSTRAP_AT,
        "cursor": str(GENERATION + 1),
    }
    manifest = stream.manifest()
    entries = stream.entries()
    assert (manifest.cursor, manifest.data_through) == (
        str(GENERATION + 1),
        Watermark("2026-11-01T09:45:03Z", "instant"),
    )
    assert manifest.coverage == {
        "advisories": 3,
        "malware": 1,
        "withdrawn": 0,
        "removed": 0,
        "remaining": 0,
    }
    assert manifest.complete is True
    assert entries[DELTA].period == "2026-11-02"
    assert {table: info.rows for table, info in entries[DELTA].tables.items()} == {
        "versions": 4,
        "events": 0,
        "cursors": 2,
    }
    assert entries[BASE_NOVEMBER].closed, "the closed asset is carried over"
    assert report == RunReport(
        new=1,
        changed=1,
        removed=0,
        returned=0,
        remaining=0,
        requests=3,
        unparsed=[],
        gate_failures=[],
    )


def test_stale_index_refused_without_writes(tmp_path):
    kept, changed = "GHSA-keep-0000-0001", "GHSA-chng-0000-0001"
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT, GENERATION, {kept: OLD, changed: OLD})
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()
    stream.osv.requests.clear()

    # The next day's index is an older copy than the one that the state was built
    # from, though it lists a change. A new day would seal the open delta, so a write
    # could come early.
    with pytest.raises(StaleIndexError, match=f"{GENERATION - 1}.*{GENERATION}"):
        stream.run("2026-11-03T06:00:00Z", GENERATION - 1, {kept: OLD, changed: LATER})

    # The run read its prior state and the index, and wrote nothing at all.
    assert stream.writes() == []
    assert stream.requested() == [INDEX_URL]
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held
    assert stream.assets(NOVEMBER) == [BASE_NOVEMBER]


def test_mass_removal_needs_override(tmp_path):
    names = [f"GHSA-mass-0000-{number:04d}" for number in range(207)]
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT, GENERATION, dict.fromkeys(names, OLD))

    # A hundred advisories leave the index. That is the most that one run may remove.
    report = stream.run(
        "2026-11-02T08:00:00Z", GENERATION + 1, dict.fromkeys(names[100:], OLD)
    )

    assert (report.removed, report.gate_failures) == (100, [])

    # The limit is on one run: the day's delta now holds 105 removals.
    report = stream.run(
        "2026-11-02T09:00:00Z", GENERATION + 2, dict.fromkeys(names[105:], OLD)
    )

    assert (report.removed, report.gate_failures) == (5, [])

    # One more than the limit in one run is held for a person to look at, as an index
    # that was cut short would be. The index still lists one advisory, since an empty
    # one is refused as a broken copy before any of this.
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()
    failure = GateFailure("removals", "101 removed events in one run (limit 100)")

    with pytest.raises(GateError) as caught:
        stream.run(
            "2026-11-02T10:00:00Z", GENERATION + 3, dict.fromkeys(names[206:], OLD)
        )

    assert caught.value.failures == [failure]
    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held

    # A person looks, and gives a reason. The run publishes, and logs why.
    reason = "OSV withdrew a batch of advisories from its PyPI files."
    report = stream.run(
        "2026-11-02T12:00:00Z",
        GENERATION + 3,
        dict.fromkeys(names[206:], OLD),
        override_reason=reason,
    )

    assert (report.removed, report.gate_failures) == (101, [failure])
    manifest = stream.manifest()
    assert manifest.overrides == [
        {
            "at": "2026-11-02T12:00:00Z",
            "reason": reason,
            "failures": [{"rule": "removals", "detail": failure.detail}],
        }
    ]
    # A removed advisory stays in the state, so the table loses no row.
    assert manifest.coverage == {
        "advisories": 1,
        "malware": 0,
        "withdrawn": 0,
        "removed": 206,
        "remaining": 0,
    }
    assert stream.entries()[STATE].tables["advisories"] == TableInfo(207, "snapshot")


# ---- the day and the month


def test_day_rollover_seals_the_open_delta(tmp_path):
    first, second, third = (f"GHSA-roll-0000-000{number}" for number in (1, 2, 3))
    stream = Stream(tmp_path)
    stream.bootstrap(
        "2026-11-02T17:00:00Z", GENERATION, dict.fromkeys((first, second, third), OLD)
    )
    # Each run after it finds one advisory changed.
    stream.run(
        "2026-11-02T23:00:00Z",
        GENERATION + 1,
        {first: LATER, second: OLD, third: OLD},
        records={first: record(first, LATER)},
    )
    open_bytes = stream.github.asset_bytes(CURRENT, DELTA)
    stream.run(
        "2026-11-03T05:00:00Z",
        GENERATION + 2,
        {first: LATER, second: LATER, third: OLD},
        records={second: record(second, LATER)},
    )
    stream.run(
        "2026-11-03T11:00:00Z",
        GENERATION + 3,
        dict.fromkeys((first, second, third), LATER),
        records={third: record(third, LATER)},
    )

    # The first run of 11-03 sealed the delta of 11-02 into the month's release, once:
    # the file that the last run of 11-02 published, byte for byte.
    sealed = sealed_name("2026-11-02")
    assert stream.assets(NOVEMBER) == sorted([BASE_NOVEMBER, sealed])
    assert upload_names(stream).count(sealed) == 1
    assert stream.github.asset_bytes(NOVEMBER, sealed) == open_bytes
    listed = [asset for asset in stream.manifest().assets if asset.name == sealed]
    assert [(a.tag, a.role, a.period, a.closed) for a in listed] == [
        (NOVEMBER, "public", "2026-11-02", True)
    ]
    assert stream.entries()[DELTA].period == "2026-11-03"
    # The sealed delta holds the rows of the runs of 11-02, and the open delta those
    # of 11-03, numbered on from them.
    assert written(stream.open(NOVEMBER, sealed), 0) == [
        ("version", first, OLD),
        ("version", second, OLD),
        ("version", third, OLD),
        ("cursor", GENERATION),
        ("version", first, LATER),
        ("cursor", GENERATION + 1),
    ]
    open_delta = stream.open(CURRENT, DELTA)
    assert written(open_delta, 0) == [
        ("version", second, LATER),
        ("cursor", GENERATION + 2),
        ("version", third, LATER),
        ("cursor", GENERATION + 3),
    ]
    assert open_delta.execute("SELECT seq FROM cursors").fetchall() == [(8,), (10,)]


def test_month_close_builds_the_month_part(tmp_path):
    first, second, third = (f"GHSA-mnth-0000-000{number}" for number in (1, 2, 3))
    malware = "MAL-2026-3"
    stream = Stream(tmp_path)
    stream.bootstrap(
        "2026-11-28T06:00:00Z", GENERATION, dict.fromkeys((first, second, third), OLD)
    )
    # 11-29: the first changes, the second leaves the index, and a malware report is
    # new.
    stream.run(
        "2026-11-29T06:00:00Z",
        GENERATION + 1,
        {first: LATER, third: OLD, malware: LATER},
        records={first: record(first, LATER), malware: record(malware, LATER)},
    )
    # 11-30: the second is listed again with a later time, and the third is withdrawn.
    listed = dict.fromkeys((first, second, third, malware), LATER)
    stream.run(
        "2026-11-30T06:00:00Z",
        GENERATION + 2,
        listed,
        records={
            second: record(second, LATER),
            third: record(third, LATER, withdrawn=LATER),
        },
    )
    last_state = stream.github.asset_bytes(CURRENT, STATE)
    assert stream.manifest().coverage == {
        "advisories": 4,
        "malware": 1,
        "withdrawn": 1,
        "removed": 0,
        "remaining": 0,
    }

    # The first run of December closes November. Nothing changed upstream.
    stream.run("2026-12-01T06:00:00Z", GENERATION + 3, listed)

    days = ["2026-11-28", "2026-11-29", "2026-11-30"]
    month_assets = [BASE_NOVEMBER, *map(sealed_name, days), PART_NOVEMBER]
    assert stream.assets(NOVEMBER) == sorted([*month_assets, MANIFEST_NOVEMBER])
    assert stream.assets(DECEMBER) == [BASE_DECEMBER]
    # December starts from the state that November ended with, as published.
    assert stream.github.asset_bytes(DECEMBER, BASE_DECEMBER) == last_state
    # The part holds every row of the month's deltas, in seq order. It starts with the
    # first run's version of each advisory, the first that the stream saw, and it
    # holds the removal, the return and the withdrawal.
    sealed = [written(stream.open(NOVEMBER, sealed_name(day)), 0) for day in days]
    assert sealed == [
        [
            ("version", first, OLD),
            ("version", second, OLD),
            ("version", third, OLD),
            ("cursor", GENERATION),
        ],
        [
            ("removed", second),
            ("version", first, LATER),
            ("version", malware, LATER),
            ("cursor", GENERATION + 1),
        ],
        [
            ("returned", second),
            ("version", second, LATER),
            ("version", third, LATER),
            ("cursor", GENERATION + 2),
        ],
    ]
    part_rows = written(stream.open(NOVEMBER, PART_NOVEMBER), 0)
    assert part_rows == [row for rows in sealed for row in rows]
    # Before it built the part, the run rebuilt November from its base copy and its
    # sealed deltas with the stream's own apply, and found the state that November
    # ended with. A restore from the releases alone finds it too. November's base
    # already holds the first run's rows, which the replay passes over.
    entries = stream.entries()
    asked = [entries[BASE_NOVEMBER], *(entries[sealed_name(day)] for day in days)]
    base_db, *delta_dbs = fetch_assets(stream.gh, asked, stream.folder("restore"))
    cycle.replay(STORE, base_db, delta_dbs)
    ended = stream.expand(DECEMBER, BASE_DECEMBER)
    with closing(open_db(base_db, STATE_SCHEMA)) as rebuilt:
        assert diff_tables(rebuilt, ended, STATE_TABLES) == []
    part = entries[PART_NOVEMBER]
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
    month = stream.manifest(NOVEMBER, MANIFEST_NOVEMBER)
    assert month.assets == [entries[name] for name in month_assets]
    # The current manifest lists every closed asset of the stream, and the new day's
    # open delta.
    assert sorted(name for name, asset in entries.items() if asset.closed) == sorted(
        [*month_assets, MANIFEST_NOVEMBER, BASE_DECEMBER]
    )
    assert entries[DELTA].period == "2026-12-01"


def test_an_error_while_processing_publishes_nothing(tmp_path):
    first, second = "GHSA-fail-0000-0001", "GHSA-fail-0000-0002"
    listed = {first: OLD, second: OLD}
    stream = Stream(tmp_path)
    # The dump holds a record, and then a member too large to be one, which read_dump
    # refuses only when it comes to it: after the bootstrap has written the first.
    oversized = {first: record(first, OLD), second: b" " * (MAX_MEMBER_BYTES + 1)}

    with pytest.raises(ValueError, match="over the limit"):
        stream.run(BOOTSTRAP_AT, GENERATION, listed, dump=oversized, bootstrap=True)

    # The error is not caught. The version that was written is never committed, and
    # nothing is published, so the stream has still never published.
    failed = stream.tmp_path / f"run-{stream.runs}" / "osv-pypi-delta-open.db"
    with closing(open_db(failed, DELTA_SCHEMA)) as conn:
        assert written(conn, 0) == []
    assert stream.writes() == []
    assert stream.github.release(CURRENT) is None
    stream.bootstrap("2026-11-02T07:00:00Z", GENERATION, listed)
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()

    # A later run is refused a record, which Http raises for. That run publishes
    # nothing either.
    new = "GHSA-fail-0000-0003"
    stream.osv.add(record_url(new), status=403)

    with pytest.raises(UpstreamError) as caught:
        stream.run("2026-11-02T12:00:00Z", GENERATION + 1, {**listed, new: LATER})

    assert caught.value.url == record_url(new)
    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held


# ---- the command line


def test_main_exits_2_after_publishing_with_unparsed_records(
    tmp_path, monkeypatch, capsys
):
    stream = Stream(tmp_path)
    made: list[tuple[str, str]] = []

    def github(repo: str, token: str) -> GitHub:
        made.append((repo, token))
        return stream.gh

    # main makes its clients, and these are the test's, on the fake GitHub and the
    # fake OSV.
    monkeypatch.setattr("osv_pypi.run.GitHub", github)
    monkeypatch.setattr("osv_pypi.run.Http", lambda: stream.http)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    kept, broken = "GHSA-main-0000-0001", "GHSA-main-0000-0002"
    stream.serve(GENERATION, {kept: OLD}, dump={kept: record(kept, OLD)})
    assert main(["run", "--bootstrap"]) == 0
    capsys.readouterr()
    # The next index lists a new advisory whose record does not parse. The refusal
    # shows the bad value, and an address with it.
    body = record(broken, LATER, aliases=[{"id": "jane@example.org"}])
    stream.serve(GENERATION + 1, {kept: OLD, broken: LATER}, records={broken: body})

    assert main(["run"]) == 2

    # It exits 2 after the publish: the manifest has the new cursor, and says that one
    # advisory is left to the next run, which fetches the record again.
    manifest = stream.manifest()
    assert manifest.cursor == str(GENERATION + 1)
    assert (manifest.coverage["remaining"], manifest.complete) == (1, False)
    assert advisories(stream.open(CURRENT, STATE)) == {kept: ("active", OLD)}
    assert made == [("py-observatory/data", "t0ken")] * 2
    # The report names the record by its id alone. Why it was refused goes to stderr
    # with no address, because the log of a public repository is public.
    logged = capsys.readouterr()
    assert json.loads(logged.out) == dataclasses.asdict(
        RunReport(
            new=0,
            changed=0,
            removed=0,
            returned=0,
            remaining=1,
            requests=2,
            unparsed=[broken],
            gate_failures=[],
        )
    )
    assert logged.err.startswith(f"skipped {broken}: ")
    assert not holds_address(logged.out + logged.err)


def test_main_parses_arguments_and_workdir(tmp_path, monkeypatch, capsys):
    made: list[tuple[str, str]] = []
    calls: list[dict] = []

    class Client(GitHub):
        def __init__(self, repo: str, token: str, **options) -> None:
            super().__init__(repo, token, **options)
            made.append((repo, token))

    report = RunReport(
        new=1,
        changed=2,
        removed=3,
        returned=4,
        remaining=5,
        requests=6,
        unparsed=[],
        gate_failures=[],
    )

    def run_once_here(gh, http, workdir, **options) -> RunReport:
        assert isinstance(gh, Client)
        assert isinstance(http, Http)
        assert list(workdir.iterdir()) == []
        calls.append(
            {
                "gh": gh,
                "workdir": workdir,
                "sqlite_tmpdir": os.environ.get("SQLITE_TMPDIR"),
                **options,
            }
        )
        return report

    monkeypatch.setattr("osv_pypi.run.GitHub", Client)
    monkeypatch.setattr("osv_pypi.run.run_once", run_once_here)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)

    assert main(["run"]) == 0

    assert made == [("py-observatory/data", "t0ken")]
    (call,) = calls
    assert (call["bootstrap"], call["override_reason"], call["max_seconds"]) == (
        False,
        None,
        1200,
    )
    assert call["now"].utcoffset() == timedelta(0)
    # A work directory of its own in ./work, which is removed afterwards. SQLite's
    # temporary files go in ./work as well, on the work's disk.
    assert call["workdir"].parent == tmp_path / "work"
    assert not call["workdir"].exists()
    assert call["sqlite_tmpdir"] == str((tmp_path / "work").resolve())
    assert json.loads(capsys.readouterr().out) == dataclasses.asdict(report)

    monkeypatch.setenv("GITHUB_REPOSITORY", "someone/data")
    monkeypatch.setenv("SQLITE_TMPDIR", str(tmp_path / "chosen"))
    disk = tmp_path / "mnt"
    disk.mkdir()
    argv = [
        "run",
        "--bootstrap",
        "--override-reason",
        "checked",
        "--max-seconds",
        "60",
        "--workdir",
        str(disk / "work"),
    ]

    assert main(argv) == 0

    assert made[-1] == ("someone/data", "t0ken")
    assert (calls[-1]["bootstrap"], calls[-1]["override_reason"]) == (True, "checked")
    assert calls[-1]["max_seconds"] == 60
    assert calls[-1]["workdir"].parent == disk / "work"
    assert calls[-1]["gh"] is not calls[0]["gh"], "one client for each process"
    # A folder for SQLite that is set already is kept.
    assert calls[-1]["sqlite_tmpdir"] == str(tmp_path / "chosen")

    # The workflow passes an empty reason when nobody gave one. A relative folder is
    # taken from where the command runs.
    assert main(["run", "--override-reason", "", "--workdir", "elsewhere"]) == 0

    assert calls[-1]["override_reason"] is None
    assert calls[-1]["workdir"].parent == tmp_path / "elsewhere"

    monkeypatch.delenv("GITHUB_TOKEN")
    with pytest.raises(KeyError, match="GITHUB_TOKEN"):
        main(["run"])
