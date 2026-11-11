import hashlib
import platform
from contextlib import closing
from dataclasses import replace
from datetime import date

import pytest

import obskit
from obskit.cycle import gate, publish_first, replay
from obskit.gates import GateError, GateFailure
from obskit.manifest import Manifest, TableInfo, Watermark
from obskit.release import fetch_assets
from obskit.sqlite import diff_tables, open_db, table_counts
from toy_store import (
    BASE,
    BASE_DECEMBER,
    BASE_NOVEMBER,
    CURRENT,
    DECEMBER,
    DELTA,
    LAST_TWO_DAYS,
    MANIFEST,
    MANIFEST_NOVEMBER,
    NOVEMBER,
    PART_NOVEMBER,
    REPO,
    ROLLUP,
    STATE,
    TOY,
    TOY_WITH_ROLLUP,
    ToyStream,
    sealed_name,
    write_delta,
    write_rollup,
    write_state,
)


def not_as_listed(stream: ToyStream) -> list[str]:
    """The assets of the current manifest whose release holds other bytes under their
    name than their entry gives. One that the release lacks raises KeyError."""
    return [
        asset.name
        for asset in stream.manifest().assets
        if hashlib.sha256(stream.github.asset_bytes(asset.tag, asset.name)).hexdigest()
        != asset.asset_sha256
    ]


def claim_rows(stream: ToyStream, name: str, table: str, rows: int) -> None:
    """Edit the current manifest by hand, to say that its asset `name` holds `rows`
    rows in `table`."""
    manifest = stream.manifest()
    edited = [
        replace(
            asset,
            tables={**asset.tables, table: replace(asset.tables[table], rows=rows)},
        )
        if asset.name == name
        else asset
        for asset in manifest.assets
    ]
    stream.replace_manifest(replace(manifest, assets=edited))


def restored_differences(stream: ToyStream, names: list[str]) -> list[str]:
    """Rebuild the state from the releases alone, as a restore would: the base copy
    `names[0]`, with each delta of `names[1:]` replayed onto it, in order, as the runs
    applied them. Each is the asset that the current manifest lists by that name.
    Returns the state tables in which the result differs from the published state."""
    entries = stream.entries()
    asked = [entries[name] for name in [*names, STATE]]
    base, *deltas, state = fetch_assets(stream.gh, asked, stream.folder("restore"))
    replay(TOY, base, deltas)
    with closing(open_db(base, TOY.state_schema)) as restored:
        return diff_tables(restored, state, TOY.state_tables)


def test_publish_first_makes_base_and_current(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "0123456789abcdef0123456789abcdef01234567")
    monkeypatch.setenv("GITHUB_RUN_ID", "1234567890")
    stream = ToyStream(tmp_path)
    # The stream's first run has written what it read to the open delta, and applied
    # it to the state.
    workdir = stream.workdir()
    write_state(workdir / TOY.state_db, BASE, applied_seq=2)
    write_delta(workdir / TOY.open_delta_db, [(1, "a", "1"), (2, "b", "2")])

    publish_first(
        stream.gh,
        TOY,
        workdir,
        today=date(2026, 11, 2),
        moment="2026-11-02T06:00:00Z",
        data_through=Watermark("2026-11-02T05:00:00Z", "instant"),
        cursor="7",
        coverage={"items": 2, "remaining": 1},
        complete=False,
    )

    # The current release holds the state, the open delta and the manifest. The
    # month's release holds the state as the month began, which is the same bytes.
    assert stream.assets(CURRENT) == sorted([STATE, DELTA, MANIFEST])
    assert stream.assets(NOVEMBER) == [BASE_NOVEMBER]
    state_bytes = stream.github.asset_bytes(CURRENT, STATE)
    assert stream.github.asset_bytes(NOVEMBER, BASE_NOVEMBER) == state_bytes
    for tag in (CURRENT, NOVEMBER):
        made = stream.github.release(tag)
        assert (made["prerelease"], made["make_latest"]) == (True, "false")
    # The manifest lists exactly those three, each as its release holds it.
    manifest = stream.manifest()
    state, delta, base = manifest.assets
    assert not_as_listed(stream) == []
    assert (state.name, state.tag, state.role, state.period, state.closed) == (
        STATE,
        CURRENT,
        "state",
        None,
        False,
    )
    assert state.tables == {
        "items": TableInfo(2, "snapshot"),
        "meta": TableInfo(1, "snapshot"),
    }
    assert (delta.name, delta.tag, delta.role, delta.period, delta.closed) == (
        DELTA,
        CURRENT,
        "state",
        "2026-11-02",
        False,
    )
    assert delta.tables == {"puts": TableInfo(2, "append")}
    assert base == replace(
        state, name=BASE_NOVEMBER, tag=NOVEMBER, period="2026-11", closed=True
    )
    # The rest is what the run gave, who published it, and the stream's upstreams. A
    # first manifest has no logs to go on from.
    assert manifest == Manifest(
        stream="kit-toy",
        producer={
            "repo": REPO,
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "run_id": "1234567890",
            "obskit": obskit.__version__,
            "python": platform.python_version(),
        },
        generated_at="2026-11-02T06:00:00Z",
        complete=False,
        coverage={"items": 2, "remaining": 1},
        data_through=Watermark("2026-11-02T05:00:00Z", "instant"),
        cursor="7",
        licences=TOY.licences,
        assets=[state, delta, base],
    )


def test_publish_first_writes_series_breaks(tmp_path):
    stream = ToyStream(tmp_path)
    workdir = stream.workdir()
    write_state(workdir / TOY.state_db, BASE, applied_seq=2)
    write_delta(workdir / TOY.open_delta_db, [(1, "a", "1"), (2, "b", "2")])
    breaks = [
        {"day": "2018-07-26", "note": "Earlier days are undercounted."},
        {"day": "2026-08-24", "note": "The upstream stopped counting a kind."},
    ]

    publish_first(
        stream.gh,
        TOY,
        workdir,
        today=date(2026, 11, 2),
        moment="2026-11-02T06:00:00Z",
        data_through=Watermark("2026-11-02", "day"),
        cursor="2",
        coverage={"items": 2},
        complete=True,
        series_breaks=breaks,
    )

    # The stream's known breaks are in its first manifest, and every later manifest
    # goes on from them.
    assert stream.manifest().series_breaks == breaks
    stream.run("2026-11-02T12:00:00Z", [("c", "3")])
    assert stream.manifest().series_breaks == breaks


def test_publish_blocks_on_a_gate_failure_unless_overridden(tmp_path):
    stream = ToyStream(tmp_path)
    seeded = stream.seed(BASE, LAST_TWO_DAYS)
    # A run before was overridden, and its manifest logs that.
    earlier = {
        "at": "2026-11-29T06:00:00Z",
        "reason": "an earlier check",
        "failures": [{"rule": "cursor", "detail": "the cursor went back"}],
    }
    stream.replace_manifest(replace(seeded, overrides=[earlier]))
    # Someone edits the manifest to say that the open delta held 1,000 puts. The next
    # run's open delta holds 2, so an append table seems to have lost rows.
    claim_rows(stream, DELTA, "puts", 1_000)
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()
    failure = GateFailure(
        "rows",
        f"the append table puts in {DELTA} (2026-11-30) went from 1,000 rows to 2",
    )

    with pytest.raises(GateError) as caught:
        stream.run("2026-11-30T12:00:00Z", [("d", "5")])

    # Nothing is published, to the current release or to any other.
    assert caught.value.failures == [failure]
    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held

    # A person looks, and gives a reason. The run publishes, and logs why after the
    # override before it.
    failures = stream.run(
        "2026-11-30T13:00:00Z", [("d", "5")], override_reason="checked"
    )

    assert failures == [failure]
    logged = {
        "at": "2026-11-30T13:00:00Z",
        "reason": "checked",
        "failures": [{"rule": "rows", "detail": failure.detail}],
    }
    assert stream.manifest().overrides == [earlier, logged]

    # The next run is compared with what was published, and passes. The log stays,
    # and a reason with nothing to override adds nothing to it.
    assert stream.run("2026-11-30T18:00:00Z", [("e", "6")]) == []
    assert stream.run("2026-11-30T19:00:00Z", [], override_reason="checked") == []
    assert stream.manifest().overrides == [earlier, logged]


def test_extra_failures_count_as_gate_failures(tmp_path):
    stream = ToyStream(tmp_path)
    stream.seed(BASE, LAST_TWO_DAYS)
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()
    # The kit's gates pass, and a check of the stream's own fails, as a limit on
    # removals would.
    removals = GateFailure("removals", "101 removed events in one run (limit 100)")

    with pytest.raises(GateError) as caught:
        stream.run("2026-11-30T12:00:00Z", [("d", "5")], extra=[removals])

    assert caught.value.failures == [removals]
    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held

    # A reason publishes past it, as past any gate, and the log names it.
    failures = stream.run(
        "2026-11-30T13:00:00Z",
        [("d", "5")],
        extra=[removals],
        override_reason="checked",
    )

    assert failures == [removals]
    assert stream.manifest().overrides == [
        {
            "at": "2026-11-30T13:00:00Z",
            "reason": "checked",
            "failures": [{"rule": "removals", "detail": removals.detail}],
        }
    ]

    # When a gate of the kit's fails as well, the stream's failures come after it.
    prior = stream.manifest()
    behind = replace(prior, data_through=Watermark("2026-11-29", "day"))

    with pytest.raises(GateError) as caught:
        gate(
            prior,
            behind,
            allowed_shrink={},
            extra=[removals],
            override_reason=None,
            moment="2026-11-30T14:00:00Z",
        )

    assert caught.value.failures == [
        GateFailure(
            "data_through", "data_through (day) went back from 2026-11-30 to 2026-11-29"
        ),
        removals,
    ]


def test_publish_lets_a_snapshot_table_shrink_by_what_the_stream_allows(tmp_path):
    stream = ToyStream(tmp_path)
    stream.seed(BASE, LAST_TWO_DAYS)
    # The manifest says that the state held 5 items. The next run's state holds 4, so
    # a snapshot table has lost a row, as one does when something upstream goes.
    claim_rows(stream, STATE, "items", 5)
    failure = GateFailure(
        "rows", f"the snapshot table items in {STATE} went from 5 rows to 4"
    )

    # A loss that the stream does not explain fails the gate.
    with pytest.raises(GateError) as caught:
        stream.run("2026-11-30T12:00:00Z", [("d", "5")])

    assert caught.value.failures == [failure]

    # One that the run's removals explain is published, with nothing to override.
    failures = stream.run(
        "2026-11-30T13:00:00Z", [("d", "5")], allowed_shrink={"items": 1}
    )

    assert (failures, stream.manifest().overrides) == ([], [])


def test_publish_carries_closed_assets_and_logs(tmp_path):
    stream = ToyStream(tmp_path)
    seeded = stream.seed(BASE, LAST_TWO_DAYS)
    # The logs of the last manifest, as people and earlier runs left them.
    logs = {
        "series_breaks": [{"at": "2026-11-15", "tables": ["items"], "why": "a"}],
        "corrections": [{"at": "2026-11-20", "tables": ["items"], "what": "b"}],
        "overrides": [
            {
                "at": "2026-11-29T06:00:00Z",
                "reason": "checked",
                "failures": [{"rule": "rows", "detail": "c"}],
            }
        ],
    }
    stream.replace_manifest(replace(seeded, **logs))

    # December's first run closes November. It leaves some of what it found to the
    # next run, so its data is not complete.
    stream.run("2026-12-01T06:00:00Z", [("d", "5")], complete=False)

    # The state and the open delta are this run's. The closed assets of the last
    # manifest follow them unchanged, and then those that the rollover added.
    manifest = stream.manifest()
    assert [asset.name for asset in manifest.assets] == [
        STATE,
        DELTA,
        BASE_NOVEMBER,
        sealed_name("2026-11-29"),
        sealed_name("2026-11-30"),
        PART_NOVEMBER,
        MANIFEST_NOVEMBER,
        BASE_DECEMBER,
    ]
    assert manifest.assets[2:4] == [asset for asset in seeded.assets if asset.closed]
    assert all(asset.closed for asset in manifest.assets[2:])
    assert manifest.assets[1].period == "2026-12-01"
    assert not_as_listed(stream) == []
    # The logs go on. The rest is what this run gave.
    assert (manifest.series_breaks, manifest.corrections, manifest.overrides) == (
        logs["series_breaks"],
        logs["corrections"],
        logs["overrides"],
    )
    assert manifest.generated_at == "2026-12-01T06:00:00Z"
    assert manifest.data_through == Watermark("2026-12-01", "day")
    assert (manifest.cursor, manifest.coverage, manifest.complete) == (
        "3",
        {"items": 4},
        False,
    )


def test_cycle_end_to_end(tmp_path):
    stream = ToyStream(tmp_path)

    # 11-30: the stream's first run publishes the state, and a later run that day
    # adds to the open delta.
    stream.first("2026-11-30T06:00:00Z", [("a", "1"), ("b", "2")])
    assert restored_differences(stream, [BASE_NOVEMBER, DELTA]) == []
    stream.run("2026-11-30T12:00:00Z", [("a", "3"), ("c", "4")])
    assert restored_differences(stream, [BASE_NOVEMBER, DELTA]) == []

    # 12-01: the run seals 11-30, closes November, and starts December with a base.
    stream.run("2026-12-01T06:00:00Z", [("b", "5"), ("d", "6")])
    assert stream.assets(NOVEMBER) == sorted(
        [BASE_NOVEMBER, sealed_name("2026-11-30"), PART_NOVEMBER, MANIFEST_NOVEMBER]
    )
    assert stream.assets(DECEMBER) == [BASE_DECEMBER]
    assert restored_differences(stream, [BASE_DECEMBER, DELTA]) == []
    # November's part holds every put of the month, the first run's among them, as
    # the part of osv_pypi's first month holds the first version of each advisory.
    assert table_counts(stream.expand(NOVEMBER, PART_NOVEMBER)) == {"puts": 4}

    # 12-02: the run seals 12-01.
    stream.run("2026-12-02T06:00:00Z", [("a", "7"), ("e", "8")])
    assert stream.assets(DECEMBER) == sorted([BASE_DECEMBER, sealed_name("2026-12-01")])

    # December's base, its sealed delta and the open delta, replayed as the runs
    # applied them, give back the state that the last run published. Without the
    # open delta, they fall short of it.
    december = [BASE_DECEMBER, sealed_name("2026-12-01")]
    assert restored_differences(stream, [*december, DELTA]) == []
    assert restored_differences(stream, december) == ["items", "meta"]
    # The manifest lists every asset that the stream has published.
    assert [asset.name for asset in stream.manifest().assets] == [
        STATE,
        DELTA,
        BASE_NOVEMBER,
        sealed_name("2026-11-30"),
        PART_NOVEMBER,
        MANIFEST_NOVEMBER,
        BASE_DECEMBER,
        sealed_name("2026-12-01"),
    ]


def test_a_derived_database_is_published_beside_the_state(tmp_path):
    stream = ToyStream(tmp_path)
    stream.store = TOY_WITH_ROLLUP
    stream.seed(BASE, LAST_TWO_DAYS)
    rollup = write_rollup(stream.folder("derived") / "kit-toy-rollup.db", 3)

    stream.run("2026-11-30T12:00:00Z", [("d", "5")], derived=[rollup])

    # The current release holds it beside the state and the open delta, and the
    # manifest lists it as a public asset with no period that is not closed.
    held = [name for name in stream.assets(CURRENT) if "swap-prev-" not in name]
    assert held == sorted([STATE, DELTA, ROLLUP, MANIFEST])
    manifest = stream.manifest()
    assert [asset.name for asset in manifest.assets[:3]] == [STATE, DELTA, ROLLUP]
    derived = manifest.assets[2]
    assert (derived.tag, derived.role, derived.period, derived.closed) == (
        CURRENT,
        "public",
        None,
        False,
    )
    assert derived.tables == {"rollup": TableInfo(3, "derived")}
    assert not_as_listed(stream) == []
    # The month's release does not get it.
    assert ROLLUP not in stream.assets(NOVEMBER)


def test_publish_first_takes_a_derived_database_too(tmp_path):
    stream = ToyStream(tmp_path)
    workdir = stream.workdir()
    write_state(workdir / TOY.state_db, BASE, applied_seq=2)
    write_delta(workdir / TOY.open_delta_db, [(1, "a", "1"), (2, "b", "2")])
    rollup = write_rollup(stream.folder("derived") / "kit-toy-rollup.db", 2)

    publish_first(
        stream.gh,
        TOY_WITH_ROLLUP,
        workdir,
        today=date(2026, 11, 2),
        moment="2026-11-02T06:00:00Z",
        data_through=Watermark("2026-11-02", "day"),
        cursor="2",
        coverage={"items": 2},
        complete=True,
        derived=[rollup],
    )

    assert stream.assets(CURRENT) == sorted([STATE, DELTA, ROLLUP, MANIFEST])
    assert stream.assets(NOVEMBER) == [BASE_NOVEMBER]
    assert [asset.name for asset in stream.manifest().assets] == [
        STATE,
        DELTA,
        ROLLUP,
        BASE_NOVEMBER,
    ]


def test_a_derived_table_must_be_of_class_derived(tmp_path):
    stream = ToyStream(tmp_path)
    stream.store = TOY_WITH_ROLLUP
    stream.seed(BASE, LAST_TWO_DAYS)
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    # The state is a snapshot table, so a database that holds it is not derived.
    wrong = write_state(
        stream.folder("derived") / "kit-toy-rollup.db", {}, applied_seq=0
    )
    stream.github.calls.clear()

    with pytest.raises(ValueError, match="items"):
        stream.run("2026-11-30T12:00:00Z", [("d", "5")], derived=[wrong])

    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held


def test_a_derived_asset_may_shrink_or_go_without_failing_a_gate(tmp_path):
    stream = ToyStream(tmp_path)
    stream.store = TOY_WITH_ROLLUP
    stream.seed(BASE, LAST_TWO_DAYS)
    folder = stream.folder("derived")
    stream.run(
        "2026-11-30T12:00:00Z",
        [("d", "5")],
        derived=[write_rollup(folder / "kit-toy-rollup.db", 5)],
    )
    assert stream.entries()[ROLLUP].tables["rollup"].rows == 5

    # It loses rows, and no gate objects.
    failures = stream.run(
        "2026-11-30T13:00:00Z",
        [("e", "6")],
        derived=[write_rollup(stream.folder("derived") / "kit-toy-rollup.db", 2)],
    )

    assert failures == []
    assert stream.entries()[ROLLUP].tables["rollup"].rows == 2

    # It is left out of the next publish, and no gate objects to that either.
    failures = stream.run("2026-11-30T14:00:00Z", [("f", "7")])

    assert failures == []
    assert ROLLUP not in stream.entries()
