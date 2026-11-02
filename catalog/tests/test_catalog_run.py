import hashlib
import json
import sqlite3
import tempfile
from collections.abc import Callable
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from catalog import run as catalog_run
from catalog import serving
from catalog.config import load_config
from catalog.inputs import fingerprint, needed_assets
from catalog.run import RunReport, main, run_once
from catalog.serving import CarryError
from catalog_helpers import REPO, TOKEN, no_sleep, publish_osv, publish_registry
from obskit.gates import GateError
from obskit.github import GitHub, GitHubError
from obskit.manifest import Manifest, Watermark, instant, loads
from obskit.release import CreateOnceError, add_create_once, publish_current
from obskit.testing import FakeGitHub
from obskit.zst import decompress_file

CONFIG = load_config()
CURRENT = "catalog-current"
FIRST = "2026-11-02"
SECOND = "2026-11-03"
# The last day of November, for the tests of the month's copy.
LAST = "2026-11-30"
MONTH_COPY = "catalog-month-2026-11"
# Each test's first run is at noon on the day of the streams' first publish.
T0 = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
HOUR = timedelta(hours=1)
MINUTE = timedelta(minutes=1)
UPLOADED = "2026-10-05T00:00:00Z"


@pytest.fixture
def fake() -> FakeGitHub:
    return FakeGitHub(REPO)


@pytest.fixture
def gh(fake: FakeGitHub) -> GitHub:
    return GitHub(REPO, TOKEN, transport=fake, sleep=no_sleep)


@pytest.fixture
def main_at(
    gh: GitHub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[[datetime, list[str]], int]:
    """`main`, run on the test's GitHub as if at the time given, from `tmp_path`."""
    # run_directory sets SQLITE_TMPDIR for the rest of the process. Setting it here
    # first has monkeypatch put back what was there once the test ends.
    monkeypatch.setenv("SQLITE_TMPDIR", "")
    monkeypatch.delenv("SQLITE_TMPDIR")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(catalog_run, "github_from_env", lambda: gh)
    real_run_once = catalog_run.run_once

    def run_main(now: datetime, argv: list[str]) -> int:
        def run_once_at_now(gh: GitHub, workdir: Path, _: datetime, **options):
            return real_run_once(gh, workdir, now, **options)

        monkeypatch.setattr(catalog_run, "run_once", run_once_at_now)
        return main(argv)

    return run_main


def registry(
    gh: GitHub,
    day: str,
    *names: str,
    sealed: tuple[str, ...] = (),
    created: tuple[str, ...] = (),
    complete: bool = True,
) -> Manifest:
    """Publish the registry as its run on `day` leaves it, with a project of each of
    `names` that has a release 1.0, with its file and its record, an empty sealed delta
    of each day of `sealed`, and an event in the open delta for the creation of each
    project of `created`. Its manifest says that the data is complete unless
    `complete` is False."""
    state: dict[str, list[dict]] = {
        "projects": [],
        "versions_seen": [],
        "files_current": [],
    }
    delta: dict[str, list[dict]] = {
        "files": [],
        "release_records": [],
        "events": [
            {"name": name, "incarnation": 1, "kind": "created", "observed_at": UPLOADED}
            for name in created
        ],
    }
    for name in names:
        file = f"{name}-1.0.tar.gz"
        state["projects"].append({"name": name})
        state["versions_seen"].append({"name": name, "version": "1.0", "record": 1})
        state["files_current"].append({"name": name, "filename": file})
        delta["files"].append(
            {"name": name, "filename": file, "version": "1.0", "upload_time": UPLOADED}
        )
        delta["release_records"].append(
            {"name": name, "version": "1.0", "uploaded_at": UPLOADED}
        )
    return publish_registry(
        gh,
        state=state,
        open_day=day,
        open_delta=delta,
        sealed={sealed_day: {} for sealed_day in sealed},
        complete=complete,
    )


def osv(
    gh: GitHub,
    day: str,
    *ids: str,
    generated_at: str | None = None,
    sealed: tuple[str, ...] = (),
) -> Manifest:
    """Publish osv as its run on `day` leaves it, with an advisory of each of `ids`,
    and an empty sealed delta of each day of `sealed`."""
    advisories = [{"id": advisory} for advisory in ids]
    return publish_osv(
        gh,
        advisories=advisories,
        open_day=day,
        generated_at=generated_at,
        sealed={sealed_day: {} for sealed_day in sealed},
    )


def fingerprint_of(manifest: Manifest) -> str:
    """The fingerprint of what the catalog reads of the stream that published
    `manifest`."""
    return fingerprint(needed_assets(manifest, CONFIG.streams[manifest.stream].tables))


def run(
    gh: GitHub,
    tmp_path: Path,
    now: datetime,
    *,
    bootstrap: bool = False,
    override_reason: str | None = None,
) -> RunReport:
    """One run at `now`, in a new directory of its own."""
    workdir = Path(tempfile.mkdtemp(prefix="run-", dir=tmp_path))
    return run_once(
        gh,
        workdir,
        now,
        config=CONFIG,
        bootstrap=bootstrap,
        override_reason=override_reason,
    )


def report(**changed) -> RunReport:
    """The report of a run that built nothing, found every stream readable and fresh,
    and left a catalog-current that says its data is complete, with the fields in
    `changed` changed."""
    fields = {
        "built": False,
        "reason": "unchanged",
        "complete": True,
        "carried": [],
        "stale": [],
        "unreadable": [],
        "pins": [],
        "month_copy": None,
        "unparsed_requirements": 0,
    }
    return RunReport(**{**fields, **changed})


def published(fake: FakeGitHub) -> Manifest:
    """The manifest that catalog-current holds."""
    return loads(fake.asset_bytes(CURRENT, "manifest.json").decode())


def serving_db(fake: FakeGitHub, tmp_path: Path) -> Path:
    """The serving database that catalog-current holds, expanded in `tmp_path`."""
    packed = tmp_path / "published.db.zst"
    packed.write_bytes(fake.asset_bytes(CURRENT, "catalog.db.zst"))
    db = tmp_path / "published.db"
    decompress_file(packed, db)
    return db


def select(db: Path, query: str) -> list[tuple]:
    with closing(sqlite3.connect(db)) as con:
        return con.execute(query).fetchall()


def writes(fake: FakeGitHub) -> list[tuple[str, str]]:
    """The requests that changed a release since the fake's calls were last cleared."""
    return [(method, url) for method, url, _ in fake.calls if method != "GET"]


def a_registry_502(fake: FakeGitHub) -> None:
    """Every attempt to read pypi-registry-current gets a 502, for one read."""
    fake.fail("GET", "pypi-registry-current", 502, times=6)


def test_bootstrap_publishes_catalog_current_as_the_latest_release(gh, fake, tmp_path):
    registry_manifest = registry(gh, FIRST, "foo")
    osv_manifest = osv(gh, "2026-11-01", "PYSEC-1")

    assert run(gh, tmp_path, T0, bootstrap=True) == report(
        built=True, reason="bootstrap"
    )

    release = fake.release(CURRENT)
    assert release["prerelease"] is False
    assert release["make_latest"] == "true"
    assert sorted(asset["name"] for asset in release["assets"]) == [
        "catalog.db.zst",
        "manifest.json",
    ]
    manifest = published(fake)
    assert manifest.stream == "catalog"
    (asset,) = manifest.assets
    assert (asset.name, asset.tag, asset.role, asset.closed) == (
        "catalog.db.zst",
        CURRENT,
        "public",
        False,
    )
    # A build's projects and events are the registry's, which never shrink, so the
    # gates hold them to the last build's rows. Every other table is made afresh.
    classes = {table: info.history_class for table, info in asset.tables.items()}
    assert (classes.pop("project"), classes.pop("event")) == ("append", "events")
    assert set(classes.values()) == {"derived"}
    assert (manifest.complete, manifest.cursor) == (True, None)
    assert manifest.coverage == {
        "first_build": "2026-11-02",
        "last_pin": None,
        "over_budget": False,
        "streams": {
            "pypi-registry": {
                "carried": False,
                "complete": True,
                "data_through": {"granularity": "serial", "value": "20261102"},
                "data_through_since": "2026-11-02T12:00:00Z",
                "error": None,
                "fingerprint": fingerprint_of(registry_manifest),
                "generated_at": "2026-11-02T00:00:00Z",
                "stale": False,
            },
            "osv-pypi": {
                "carried": False,
                "complete": True,
                "data_through": {
                    "granularity": "instant",
                    "value": "2026-11-01T00:00:00Z",
                },
                "data_through_since": "2026-11-02T12:00:00Z",
                "error": None,
                "fingerprint": fingerprint_of(osv_manifest),
                "generated_at": "2026-11-01T00:00:00Z",
                "stale": False,
            },
        },
    }
    # Each upstream once, with its stream, by stream and then by name.
    assert [(licence["stream"], licence["name"]) for licence in manifest.licences] == [
        ("osv-pypi", "GitHub Advisory Database (GHSA-*)"),
        ("osv-pypi", "OSV.dev data dumps"),
        ("osv-pypi", "OpenSSF Malicious Packages (MAL-*)"),
        ("osv-pypi", "PyPA Advisory Database (PYSEC-*)"),
        ("pypi-registry", "PyPI Simple and JSON APIs"),
    ]
    upstreams = {
        "osv-pypi": osv_manifest.licences,
        "pypi-registry": registry_manifest.licences,
    }
    for licence in manifest.licences:
        stream = licence.pop("stream")
        assert licence in upstreams[stream]
    db = serving_db(fake, tmp_path)
    assert select(db, "SELECT stream, carried FROM input ORDER BY stream") == [
        ("osv-pypi", 0),
        ("pypi-registry", 0),
    ]
    assert select(db, "SELECT name FROM project") == [("foo",)]
    assert select(db, "SELECT id FROM advisory") == [("PYSEC-1",)]


def never_published(gh: GitHub, fake: FakeGitHub) -> None:
    """osv has published, and the registry never has, so its release is a 404."""
    osv(gh, FIRST, "PYSEC-1")


def fails_to_stage(gh: GitHub, fake: FakeGitHub) -> None:
    """Both streams have published, and every attempt to download osv's state gets a
    502."""
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    fake.fail("GET", "name=osv-pypi-state.db.zst", 502, times=6)


@pytest.mark.parametrize(
    ("broken", "error"),
    [
        pytest.param(never_published, CarryError, id="a stream that cannot be read"),
        pytest.param(fails_to_stage, GitHubError, id="a stream that fails to stage"),
    ],
)
def test_bootstrap_refuses_when_a_stream_cannot_be_read(
    gh, fake, tmp_path, broken, error
):
    broken(gh, fake)

    # A first build needs every stream, and has no earlier build to carry one from.
    with pytest.raises(error):
        run(gh, tmp_path, T0, bootstrap=True)

    assert fake.release(CURRENT) is None


def test_unchanged_inputs_build_nothing(gh, fake, tmp_path):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)
    fake.calls.clear()

    assert run(gh, tmp_path, T0 + HOUR) == report(reason="unchanged")

    assert writes(fake) == []
    assert published(fake).generated_at == instant(T0)


def test_changed_inputs_wait_for_the_min_gap(gh, fake, tmp_path):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)
    changed = registry(gh, FIRST, "foo", "bar")
    fake.calls.clear()

    assert run(gh, tmp_path, T0 + 30 * MINUTE) == report(reason="too soon")
    assert writes(fake) == []

    assert run(gh, tmp_path, T0 + 60 * MINUTE) == report(
        built=True, reason="inputs changed"
    )
    manifest = published(fake)
    assert manifest.generated_at == instant(T0 + 60 * MINUTE)
    entry = manifest.coverage["streams"]["pypi-registry"]
    assert entry["fingerprint"] == fingerprint_of(changed)
    projects = select(serving_db(fake, tmp_path), "SELECT name FROM project")
    assert sorted(projects) == [("bar",), ("foo",)]


def test_the_heartbeat_builds_after_the_max_gap(gh, fake, tmp_path):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)

    assert run(gh, tmp_path, T0 + 24 * HOUR - MINUTE) == report(reason="unchanged")
    assert run(gh, tmp_path, T0 + 24 * HOUR) == report(built=True, reason="heartbeat")

    manifest = published(fake)
    assert manifest.generated_at == instant(T0 + 24 * HOUR)
    # The first build's day stays.
    assert manifest.coverage["first_build"] == "2026-11-02"


def test_a_build_over_the_budget_warns_and_still_publishes(
    gh, fake, tmp_path, monkeypatch
):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    monkeypatch.setattr(serving, "BUDGET_BYTES", 0)

    with pytest.warns(UserWarning, match="over the budget"):
        report_of_run = run(gh, tmp_path, T0, bootstrap=True)

    assert report_of_run == report(built=True, reason="bootstrap")
    manifest = published(fake)
    assert manifest.coverage["over_budget"] is True
    assert manifest.complete is True


def test_an_unreadable_stream_is_carried_from_the_previous_build(
    gh, fake, tmp_path, main_at, capsys
):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)
    before = published(fake).coverage["streams"]["pypi-registry"]
    osv(gh, SECOND, "PYSEC-1", "PYSEC-2")
    a_registry_502(fake)

    assert main_at(T0 + 24 * HOUR, ["run"]) == 2

    assert json.loads(capsys.readouterr().out) == {
        "built": True,
        "reason": "inputs changed",
        "complete": False,
        "carried": ["pypi-registry"],
        "stale": [],
        "unreadable": ["pypi-registry"],
        "pins": [],
        "month_copy": None,
        "unparsed_requirements": 0,
    }
    manifest = published(fake)
    assert manifest.generated_at == instant(T0 + 24 * HOUR)
    assert manifest.complete is False
    entry = manifest.coverage["streams"]["pypi-registry"]
    assert (entry["carried"], entry["stale"]) == (True, False)
    assert "HTTP 502" in entry["error"]
    # What the catalog knows of the stream is what the last build read.
    kept = ("data_through", "data_through_since", "generated_at", "fingerprint")
    assert {key: entry[key] for key in kept} == {key: before[key] for key in kept}
    assert manifest.coverage["streams"]["osv-pypi"]["carried"] is False
    # The registry's tables come from the last build, and the database says so.
    db = serving_db(fake, tmp_path)
    assert select(db, "SELECT name FROM project") == [("foo",)]
    assert select(db, "SELECT stream, carried FROM input ORDER BY stream") == [
        ("osv-pypi", 0),
        ("pypi-registry", 1),
    ]
    assert select(db, "SELECT id FROM advisory ORDER BY id") == [
        ("PYSEC-1",),
        ("PYSEC-2",),
    ]


def test_a_carried_stream_that_can_be_read_again_is_rebuilt(
    gh, fake, tmp_path, main_at, capsys
):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)
    osv(gh, SECOND, "PYSEC-1", "PYSEC-2")
    a_registry_502(fake)
    carried_at = T0 + 2 * HOUR
    assert run(gh, tmp_path, carried_at) == report(
        built=True,
        reason="inputs changed",
        complete=False,
        carried=["pypi-registry"],
        unreadable=["pypi-registry"],
    )

    # The registry can be read again, with the assets that the last good build read,
    # so its fingerprint is the one recorded. It changed all the same, since the build
    # says that it was carried, and it waits for the min gap as any change does.
    assert run(gh, tmp_path, carried_at + 30 * MINUTE) == report(
        reason="too soon", complete=False
    )
    assert main_at(carried_at + HOUR, ["run"]) == 0

    printed = json.loads(capsys.readouterr().out)
    assert (printed["built"], printed["reason"]) == (True, "inputs changed")
    assert (printed["carried"], printed["unreadable"]) == ([], [])
    manifest = published(fake)
    assert manifest.generated_at == instant(carried_at + HOUR)
    entry = manifest.coverage["streams"]["pypi-registry"]
    assert (entry["carried"], entry["error"]) == (False, None)
    assert manifest.complete is True
    db = serving_db(fake, tmp_path)
    assert select(db, "SELECT stream, carried FROM input ORDER BY stream") == [
        ("osv-pypi", 0),
        ("pypi-registry", 0),
    ]


def test_a_run_that_builds_nothing_fails_while_the_catalog_is_incomplete(
    gh, fake, tmp_path, main_at, capsys
):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)
    a_registry_502(fake)
    carried_at = T0 + HOUR
    assert main_at(carried_at, ["run"]) == 2
    capsys.readouterr()

    # The registry can be read again, and it is too soon to build. The run finds
    # nothing wrong, but catalog-current still says that the registry was carried, so
    # the run fails, and the catalog's issue stays open until a build says otherwise.
    assert main_at(carried_at + 30 * MINUTE, ["run"]) == 2

    printed = json.loads(capsys.readouterr().out)
    assert (printed["built"], printed["reason"]) == (False, "too soon")
    assert (printed["carried"], printed["stale"], printed["unreadable"]) == ([], [], [])
    assert printed["complete"] is False

    assert main_at(carried_at + HOUR, ["run"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert (printed["built"], printed["complete"]) == (True, True)


def damage_osv_state(
    gh: GitHub, fake: FakeGitHub, tmp_path: Path, damage: Callable[[bytes], bytes]
) -> None:
    """Publish osv-pypi again with the compressed bytes of its state put through
    `damage`, and a manifest that gives the damaged bytes' sha256, as a stream that
    published a damaged file would. The download then matches its entry, and only
    expanding it fails."""
    tag = "osv-pypi-current"
    manifest = loads(fake.asset_bytes(tag, "manifest.json").decode())
    work = Path(tempfile.mkdtemp(prefix="damaged-", dir=tmp_path))
    files = []
    assets = []
    for asset in manifest.assets:
        if asset.tag == tag:
            data = fake.asset_bytes(tag, asset.name)
            if asset.name == "osv-pypi-state.db.zst":
                data = damage(data)
                asset = replace(asset, asset_sha256=hashlib.sha256(data).hexdigest())
            (work / asset.name).write_bytes(data)
            files.append(work / asset.name)
        assets.append(asset)
    publish_current(
        gh, "osv-pypi", files, replace(manifest, assets=assets), bootstrap=False
    )


def osv_state_502(gh: GitHub, fake: FakeGitHub, tmp_path: Path) -> None:
    """Every attempt to download osv's state gets a 502."""
    fake.fail("GET", "name=osv-pypi-state.db.zst", 502, times=6)


def osv_state_cut_short(gh: GitHub, fake: FakeGitHub, tmp_path: Path) -> None:
    """osv's state is published cut off halfway, so zstd finds no end to it."""
    damage_osv_state(gh, fake, tmp_path, lambda data: data[: len(data) // 2])


def osv_state_not_zstd(gh: GitHub, fake: FakeGitHub, tmp_path: Path) -> None:
    """osv's state is published as bytes that are not zstd at all."""
    damage_osv_state(gh, fake, tmp_path, lambda data: b"not zstd " * 16)


@pytest.mark.parametrize(
    ("broken", "error"),
    [
        pytest.param(osv_state_502, "HTTP 502", id="a failed download"),
        pytest.param(
            osv_state_cut_short,
            "ended before the end-of-stream marker",
            id="a file cut short",
        ),
        pytest.param(
            osv_state_not_zstd,
            "Unable to decompress Zstandard data",
            id="a file that is not zstd",
        ),
    ],
)
def test_a_stream_that_fails_to_stage_is_carried(gh, fake, tmp_path, broken, error):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)
    before = published(fake).coverage["streams"]["osv-pypi"]
    osv(gh, SECOND, "PYSEC-1", "PYSEC-2")
    broken(gh, fake, tmp_path)
    workdir = tmp_path / "work"
    workdir.mkdir()

    result = run_once(
        gh,
        workdir,
        T0 + 24 * HOUR,
        config=CONFIG,
        bootstrap=False,
        override_reason=None,
    )

    # It was read, so it is not unreadable, but its tables are the last build's.
    assert result == report(
        built=True, reason="inputs changed", complete=False, carried=["osv-pypi"]
    )
    entry = published(fake).coverage["streams"]["osv-pypi"]
    assert entry["carried"] is True
    assert error in entry["error"]
    # What the catalog knows of the stream is what the last build read, and not what
    # the manifest that it could not stage says.
    kept = ("data_through", "data_through_since", "generated_at", "fingerprint")
    assert {key: entry[key] for key in kept} == {key: before[key] for key in kept}
    assert select(serving_db(fake, tmp_path), "SELECT id FROM advisory") == [
        ("PYSEC-1",)
    ]
    # What it had downloaded is gone.
    assert not (workdir / "osv-pypi").exists()


def test_each_stream_says_whether_its_data_is_complete(gh, fake, tmp_path):
    # The registry's sweep has not read every project yet.
    registry(gh, FIRST, "foo", complete=False)
    osv(gh, FIRST, "PYSEC-1")

    assert run(gh, tmp_path, T0, bootstrap=True) == report(
        built=True, reason="bootstrap"
    )

    manifest = published(fake)
    streams = manifest.coverage["streams"]
    assert (streams["pypi-registry"]["complete"], streams["osv-pypi"]["complete"]) == (
        False,
        True,
    )
    # The catalog read every stream, and none is stale, so its own data is complete.
    assert manifest.complete is True
    db = serving_db(fake, tmp_path)
    assert select(db, "SELECT stream, complete FROM input ORDER BY stream") == [
        ("osv-pypi", 1),
        ("pypi-registry", 0),
    ]

    # The registry publishes data that it calls complete, and fails to stage, so the
    # build carries it, with what the last build read of it.
    registry(gh, SECOND, "foo")
    fake.fail("GET", "name=pypi-registry-state.db.zst", 502, times=6)
    assert run(gh, tmp_path, T0 + 24 * HOUR) == report(
        built=True, reason="inputs changed", complete=False, carried=["pypi-registry"]
    )

    assert published(fake).coverage["streams"]["pypi-registry"]["complete"] is False
    db = serving_db(fake, tmp_path)
    assert select(
        db, "SELECT stream, complete, carried FROM input ORDER BY stream"
    ) == [
        ("osv-pypi", 1, 0),
        ("pypi-registry", 0, 1),
    ]


def test_a_stream_whose_data_through_stands_still_turns_stale(gh, fake, tmp_path):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)

    # Nothing has changed, so the heartbeat builds, and the registry's data_through
    # was first seen at the bootstrap, 29 hours before.
    assert run(gh, tmp_path, T0 + 29 * HOUR) == report(built=True, reason="heartbeat")
    entry = published(fake).coverage["streams"]["pypi-registry"]
    assert entry["data_through_since"] == instant(T0)
    assert entry["stale"] is False

    # Two hours on, osv has moved, and the registry has stood still for 31 hours,
    # which is longer than its 30.
    osv(gh, SECOND, "PYSEC-1", "PYSEC-2")
    assert run(gh, tmp_path, T0 + 31 * HOUR) == report(
        built=True, reason="inputs changed", complete=False, stale=["pypi-registry"]
    )
    manifest = published(fake)
    streams = manifest.coverage["streams"]
    assert streams["pypi-registry"]["data_through_since"] == instant(T0)
    assert streams["pypi-registry"]["stale"] is True
    assert streams["osv-pypi"]["data_through_since"] == instant(T0 + 31 * HOUR)
    assert streams["osv-pypi"]["stale"] is False
    assert manifest.complete is False

    # A run that builds nothing still finds it stale.
    assert run(gh, tmp_path, T0 + 32 * HOUR) == report(
        complete=False, stale=["pypi-registry"]
    )


def test_a_stream_that_turns_stale_counts_as_a_change(gh, fake, tmp_path):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)
    last_build = T0 + 29 * HOUR + 30 * MINUTE
    assert run(gh, tmp_path, last_build) == report(built=True, reason="heartbeat")

    # Nothing changes, and the registry's data_through, which the catalog first saw
    # at the bootstrap, turns stale after 30 hours. The manifest says that it is
    # fresh, so the catalog builds once the min gap has passed.
    assert run(gh, tmp_path, last_build + 31 * MINUTE) == report(
        reason="too soon", stale=["pypi-registry"]
    )
    assert run(gh, tmp_path, last_build + 50 * MINUTE) == report(
        built=True, reason="inputs changed", complete=False, stale=["pypi-registry"]
    )

    manifest = published(fake)
    assert manifest.coverage["streams"]["pypi-registry"]["stale"] is True
    assert manifest.complete is False


def test_a_stream_that_turns_unreadable_counts_as_a_change(gh, fake, tmp_path):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)

    # Nothing else changes, and the registry cannot be read. The manifest says that it
    # was read, so the catalog builds once the min gap has passed.
    a_registry_502(fake)
    assert run(gh, tmp_path, T0 + 30 * MINUTE) == report(
        reason="too soon", unreadable=["pypi-registry"]
    )
    a_registry_502(fake)
    assert run(gh, tmp_path, T0 + 50 * MINUTE) == report(
        built=True,
        reason="inputs changed",
        complete=False,
        carried=["pypi-registry"],
        unreadable=["pypi-registry"],
    )

    manifest = published(fake)
    assert manifest.coverage["streams"]["pypi-registry"]["carried"] is True
    assert manifest.complete is False

    # Now the manifest says so, and a stream that stays unreadable changes nothing.
    a_registry_502(fake)
    assert run(gh, tmp_path, T0 + 2 * HOUR) == report(
        complete=False, unreadable=["pypi-registry"]
    )


def test_data_through_is_the_oldest_input_generated_at(gh, fake, tmp_path):
    registry(gh, FIRST, "foo")
    osv(gh, "2026-11-01", "PYSEC-1")

    run(gh, tmp_path, T0, bootstrap=True)

    assert published(fake).data_through == Watermark("2026-11-01T00:00:00Z", "instant")

    # A carried stream's inputs are still used, so its generated_at counts.
    osv(gh, SECOND, "PYSEC-1", "PYSEC-2")
    a_registry_502(fake)
    run(gh, tmp_path, T0 + 24 * HOUR)

    assert published(fake).data_through == Watermark("2026-11-02T00:00:00Z", "instant")


def test_older_inputs_need_an_override(gh, fake, tmp_path):
    registry(gh, SECOND, "foo")
    osv(gh, SECOND, "PYSEC-1")
    first = T0 + 24 * HOUR
    run(gh, tmp_path, first, bootstrap=True)
    # osv publishes again, saying that the publish was made a day before its last.
    osv(gh, SECOND, "PYSEC-1", "PYSEC-2", generated_at="2026-11-02T00:00:00Z")
    fake.calls.clear()

    with pytest.raises(GateError, match="data_through"):
        run(gh, tmp_path, first + HOUR)

    assert writes(fake) == []
    assert published(fake).generated_at == instant(first)

    assert run(gh, tmp_path, first + HOUR, override_reason="checked") == report(
        built=True, reason="inputs changed"
    )

    manifest = published(fake)
    assert manifest.data_through == Watermark("2026-11-02T00:00:00Z", "instant")
    (override,) = manifest.overrides
    assert (override["at"], override["reason"]) == (instant(first + HOUR), "checked")
    assert [failure["rule"] for failure in override["failures"]] == ["data_through"]

    # The next build keeps the log.
    assert run(gh, tmp_path, first + 25 * HOUR) == report(
        built=True, reason="heartbeat"
    )
    assert published(fake).overrides == [override]


def test_a_build_that_lost_registry_events_needs_an_override(gh, fake, tmp_path):
    registry(gh, FIRST, "foo", "bar", created=("foo", "bar"))
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)
    # The registry is published again from an earlier state, which had seen only the
    # first of the two events.
    registry(gh, FIRST, "foo", "bar", created=("foo",))
    fake.calls.clear()

    with pytest.raises(GateError, match="event"):
        run(gh, tmp_path, T0 + HOUR)

    assert writes(fake) == []
    assert published(fake).generated_at == instant(T0)

    assert run(gh, tmp_path, T0 + HOUR, override_reason="checked") == report(
        built=True, reason="inputs changed"
    )

    manifest = published(fake)
    (override,) = manifest.overrides
    assert (override["at"], override["reason"]) == (instant(T0 + HOUR), "checked")
    (failure,) = override["failures"]
    assert failure["rule"] == "rows"
    assert "the events table event" in failure["detail"]
    db = serving_db(fake, tmp_path)
    assert select(db, "SELECT name FROM event") == [("foo",)]


def test_main_prints_the_report_and_returns_its_code(
    gh, fake, tmp_path, main_at, capsys
):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    work = tmp_path / "elsewhere"

    assert main_at(T0, ["run", "--bootstrap", "--workdir", str(work)]) == 0

    assert json.loads(capsys.readouterr().out) == {
        "built": True,
        "reason": "bootstrap",
        "complete": True,
        "carried": [],
        "stale": [],
        "unreadable": [],
        "pins": [],
        "month_copy": None,
        "unparsed_requirements": 0,
    }
    assert published(fake).generated_at == instant(T0)
    # The run worked in a directory of its own there, which is gone.
    assert list(work.iterdir()) == []

    # The registry has stood still for longer than its 30 hours. That changes what the
    # manifest says, so the run builds, and the workflow hears of it from the exit code.
    assert main_at(T0 + 31 * HOUR, ["run", "--workdir", str(work)]) == 2

    printed = json.loads(capsys.readouterr().out)
    assert (printed["built"], printed["reason"]) == (True, "inputs changed")
    assert printed["stale"] == ["pypi-registry"]


def test_the_run_writes_each_pin_that_is_ready_and_records_the_last(
    gh, fake, tmp_path, monkeypatch
):
    monkeypatch.setenv("GITHUB_SHA", "8f3c2a1")
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    first = datetime(2026, 11, 2, 23, 40, tzinfo=UTC)
    run(gh, tmp_path, first, bootstrap=True)
    # Early on the 3rd, both streams seal the 2nd.
    registry(gh, SECOND, "foo", sealed=(FIRST,))
    osv(gh, SECOND, "PYSEC-1", sealed=(FIRST,))

    # It is too soon to build, and the pin of the 2nd is written all the same.
    assert run(gh, tmp_path, first + 30 * MINUTE) == report(
        reason="too soon", pins=["v2026-11-02"]
    )
    pin = json.loads(fake.asset_bytes("v2026-11-02", "release.json"))
    assert (pin["day"], pin["commit"], pin["repo"]) == (FIRST, "8f3c2a1", REPO)
    assert [stream["complete"] for stream in pin["streams"].values()] == [True, True]
    assert published(fake).coverage["last_pin"] is None
    written = fake.release("v2026-11-02")

    # The next build finds the pin, leaves it alone, and records it.
    assert run(gh, tmp_path, first + 50 * MINUTE) == report(
        built=True, reason="inputs changed"
    )
    assert fake.release("v2026-11-02") == written
    assert published(fake).coverage["last_pin"] == FIRST

    # On the 5th, the registry has sealed the 3rd, and osv the 3rd and the 4th. The
    # build writes the pin of the 3rd and records it in its own manifest. The pin of
    # the 4th waits for the registry.
    registry(gh, "2026-11-04", "foo", sealed=(SECOND,))
    osv(gh, "2026-11-05", "PYSEC-1", sealed=(SECOND, "2026-11-04"))
    assert run(gh, tmp_path, datetime(2026, 11, 5, 1, 0, tzinfo=UTC)) == report(
        built=True, reason="inputs changed", pins=["v2026-11-03"]
    )
    assert published(fake).coverage["last_pin"] == SECOND
    assert fake.release("v2026-11-04") is None


def bootstrap_at_the_end_of_november(gh: GitHub, tmp_path: Path) -> datetime:
    """Publish both streams on the last day of November, bootstrap the catalog late
    that day, and return the time of the build. No pin is due until December's runs,
    and none is ready before the streams seal the 30th."""
    registry(gh, LAST, "foo")
    osv(gh, LAST, "PYSEC-1")
    built = datetime(2026, 11, 30, 23, 40, tzinfo=UTC)
    run(gh, tmp_path, built, bootstrap=True)
    return built


@pytest.mark.parametrize(
    "change", [False, True], ids=["inputs unchanged", "inputs changed too soon"]
)
def test_the_first_run_of_a_month_always_builds(gh, fake, tmp_path, change):
    built = bootstrap_at_the_end_of_november(gh, tmp_path)
    if change:
        registry(gh, LAST, "foo", "bar")

    # Twenty minutes on, December has begun, and its first run builds whether or not
    # an input has changed, before the min gap has passed.
    assert run(gh, tmp_path, built + 20 * MINUTE) == report(
        built=True, reason="month began", month_copy=MONTH_COPY
    )

    manifest = published(fake)
    assert manifest.generated_at == instant(built + 20 * MINUTE)
    assert [(asset.name, asset.closed) for asset in manifest.assets] == [
        ("catalog.db.zst", False),
        ("catalog-2026-11.db.zst", True),
        ("manifest-2026-11.json", True),
    ]


def test_the_month_copy_is_made_once(gh, fake, tmp_path):
    built = bootstrap_at_the_end_of_november(gh, tmp_path)
    # December's first run copies November's last build, and then fails to publish.
    fake.fail("POST", "name=swap-next-catalog.db.zst", 502, times=6)
    with pytest.raises(GitHubError):
        run(gh, tmp_path, built + HOUR)
    copy = fake.release(MONTH_COPY)
    assert [asset["name"] for asset in copy["assets"]] == [
        "catalog-2026-11.db.zst",
        "manifest-2026-11.json",
    ]
    assert published(fake).generated_at == instant(built)
    fake.calls.clear()

    # The run again makes the same copy, finds it there, and uploads none of it.
    assert run(gh, tmp_path, built + 2 * HOUR) == report(
        built=True, reason="month began", month_copy=MONTH_COPY
    )
    assert fake.release(MONTH_COPY) == copy
    assert not any(f"/releases/{copy['id']}/" in url for _, url in writes(fake))
    copied = [
        (asset.tag, asset.name, asset.closed)
        for asset in published(fake).assets
        if asset.tag == MONTH_COPY
    ]
    assert copied == [
        (MONTH_COPY, "catalog-2026-11.db.zst", True),
        (MONTH_COPY, "manifest-2026-11.json", True),
    ]

    # Later runs of the month make no copy, and every later manifest keeps the
    # entries, each once.
    osv(gh, "2026-12-01", "PYSEC-1", "PYSEC-2")
    fake.calls.clear()
    assert run(gh, tmp_path, built + 3 * HOUR) == report(
        built=True, reason="inputs changed"
    )
    assert not any(MONTH_COPY in url for _, url, _ in fake.calls)
    assert [
        (asset.tag, asset.name, asset.closed)
        for asset in published(fake).assets
        if asset.tag == MONTH_COPY
    ] == copied


def the_download_fails(gh: GitHub, fake: FakeGitHub, tmp_path: Path) -> None:
    """Every attempt to download the serving database of catalog-current gets a 502."""
    fake.fail("GET", "name=catalog.db.zst", 502, times=6)


def the_copy_is_there_with_other_bytes(
    gh: GitHub, fake: FakeGitHub, tmp_path: Path
) -> None:
    """catalog-month-2026-11 holds catalog-2026-11.db.zst already, with other bytes."""
    stray = tmp_path / "catalog-2026-11.db.zst"
    stray.write_bytes(b"other bytes")
    add_create_once(gh, "catalog", MONTH_COPY, [stray])


@pytest.mark.parametrize(
    ("broken", "error"),
    [
        pytest.param(the_download_fails, GitHubError, id="the download fails"),
        pytest.param(
            the_copy_is_there_with_other_bytes,
            CreateOnceError,
            id="the copy is there with other bytes",
        ),
    ],
)
def test_a_failed_month_copy_stops_the_run_before_it_publishes(
    gh, fake, tmp_path, broken, error
):
    built = bootstrap_at_the_end_of_november(gh, tmp_path)
    before = fake.release(CURRENT)
    broken(gh, fake, tmp_path)

    with pytest.raises(error):
        run(gh, tmp_path, built + HOUR)

    assert fake.release(CURRENT) == before
