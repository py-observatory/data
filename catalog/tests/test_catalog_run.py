import json
import sqlite3
import tempfile
from collections.abc import Callable
from contextlib import closing
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
from obskit.testing import FakeGitHub
from obskit.zst import decompress_file

CONFIG = load_config()
CURRENT = "catalog-current"
FIRST = "2026-11-02"
SECOND = "2026-11-03"
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


def registry(gh: GitHub, day: str, *names: str) -> Manifest:
    """Publish the registry as its run on `day` leaves it, with a project of each of
    `names` that has a release 1.0, with its file and its record."""
    state: dict[str, list[dict]] = {
        "projects": [],
        "versions_seen": [],
        "files_current": [],
    }
    delta: dict[str, list[dict]] = {"files": [], "release_records": []}
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
    return publish_registry(gh, state=state, open_day=day, open_delta=delta)


def osv(gh: GitHub, day: str, *ids: str, generated_at: str | None = None) -> Manifest:
    """Publish osv as its run on `day` leaves it, with an advisory of each of `ids`."""
    advisories = [{"id": advisory} for advisory in ids]
    return publish_osv(
        gh, advisories=advisories, open_day=day, generated_at=generated_at
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
    """The report of a run that built nothing and found every stream readable and
    fresh, with the fields in `changed` changed."""
    fields = {
        "built": False,
        "reason": "unchanged",
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
    assert {info.history_class for info in asset.tables.values()} == {"derived"}
    assert (manifest.complete, manifest.cursor) == (True, None)
    assert manifest.coverage == {
        "first_build": "2026-11-02",
        "last_pin": None,
        "over_budget": False,
        "streams": {
            "pypi-registry": {
                "carried": False,
                "data_through": {"granularity": "serial", "value": "20261102"},
                "data_through_since": "2026-11-02T12:00:00Z",
                "error": None,
                "fingerprint": fingerprint_of(registry_manifest),
                "generated_at": "2026-11-02T00:00:00Z",
                "stale": False,
            },
            "osv-pypi": {
                "carried": False,
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


def test_a_stream_that_fails_to_stage_is_carried(gh, fake, tmp_path):
    registry(gh, FIRST, "foo")
    osv(gh, FIRST, "PYSEC-1")
    run(gh, tmp_path, T0, bootstrap=True)
    before = published(fake).coverage["streams"]["osv-pypi"]
    osv(gh, SECOND, "PYSEC-1", "PYSEC-2")
    fake.fail("GET", "name=osv-pypi-state.db.zst", 502, times=6)
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
    assert result == report(built=True, reason="inputs changed", carried=["osv-pypi"])
    entry = published(fake).coverage["streams"]["osv-pypi"]
    assert entry["carried"] is True
    assert "HTTP 502" in entry["error"]
    # What the catalog knows of the stream is what the last build read, and not what
    # the manifest that it could not stage says.
    kept = ("data_through", "data_through_since", "generated_at", "fingerprint")
    assert {key: entry[key] for key in kept} == {key: before[key] for key in kept}
    assert select(serving_db(fake, tmp_path), "SELECT id FROM advisory") == [
        ("PYSEC-1",)
    ]
    # What it had downloaded is gone.
    assert not (workdir / "osv-pypi").exists()


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
        built=True, reason="inputs changed", stale=["pypi-registry"]
    )
    manifest = published(fake)
    streams = manifest.coverage["streams"]
    assert streams["pypi-registry"]["data_through_since"] == instant(T0)
    assert streams["pypi-registry"]["stale"] is True
    assert streams["osv-pypi"]["data_through_since"] == instant(T0 + 31 * HOUR)
    assert streams["osv-pypi"]["stale"] is False
    assert manifest.complete is False

    # A run that builds nothing still finds it stale.
    assert run(gh, tmp_path, T0 + 32 * HOUR) == report(stale=["pypi-registry"])


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

    # The heartbeat builds, and the registry has stood still for longer than its 30
    # hours, which the workflow hears of from the exit code.
    assert main_at(T0 + 31 * HOUR, ["run", "--workdir", str(work)]) == 2

    printed = json.loads(capsys.readouterr().out)
    assert (printed["built"], printed["reason"]) == (True, "heartbeat")
    assert printed["stale"] == ["pypi-registry"]
