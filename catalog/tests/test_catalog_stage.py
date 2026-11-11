import re
import sqlite3
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest

from catalog.config import StreamAdmission, load_config
from catalog.inputs import fingerprint, needed_assets, read_stream
from catalog.stage import (
    DownloadsStage,
    READS,
    STAGE_SCHEMA,
    STAGERS,
    OsvStage,
    RegistryStage,
    stage_downloads,
    stage_osv,
    stage_registry,
)
from catalog_helpers import (
    REPO,
    TOKEN,
    no_sleep,
    publish_downloads,
    publish_osv,
    publish_registry,
)
from obskit.github import GitHub
from obskit.manifest import Asset, Manifest, Watermark, producer
from obskit.release import prepare_asset, publish_current
from obskit.sqlite import open_db, table_counts
from obskit.testing import FakeGitHub
from pypi_downloads import serving as downloads_serving
from pypi_downloads import store as downloads_store
from pypi_registry.store import DELTA_SCHEMA, STATE_SCHEMA

REGISTRY = load_config().streams["pypi-registry"]
OSV = load_config().streams["osv-pypi"]

NAME = "sampleproject"
SDIST = "sampleproject-1.0.tar.gz"
WHEEL = "sampleproject-1.0-py3-none-any.whl"
UPLOADED = "2017-08-06T03:43:27.195869Z"
LATER = "2017-08-06T03:44:02.000000Z"


@pytest.fixture
def fake() -> FakeGitHub:
    return FakeGitHub(REPO)


@pytest.fixture
def gh(fake: FakeGitHub) -> GitHub:
    return GitHub(REPO, TOKEN, transport=fake, sleep=no_sleep)


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    path = tmp_path / "work"
    path.mkdir()
    return path


def file(filename: str, version: str | None, uploaded: str | None) -> dict:
    """A row of a delta's files: a file of sampleproject, as the delta first saw it."""
    return {
        "name": NAME,
        "filename": filename,
        "version": version,
        "upload_time": uploaded,
    }


def current(filename: str, *, yanked: str | None = None) -> dict:
    """A row of the state's files_current: a file that sampleproject has now, yanked for
    the reason `yanked` when one is given."""
    return {
        "name": NAME,
        "filename": filename,
        "yanked": int(yanked is not None),
        "yanked_reason": yanked,
    }


def staged(gh: GitHub, manifest: Manifest, workdir: Path) -> RegistryStage:
    """The registry staged from the assets that the catalog needs of `manifest`."""
    return stage_registry(gh, needed_assets(manifest, REGISTRY.tables), workdir)


def select(db: Path, query: str) -> list[tuple]:
    with closing(sqlite3.connect(db)) as con:
        return con.execute(query).fetchall()


def release_files(stage: RegistryStage) -> list[tuple]:
    return select(
        stage.stage, "SELECT * FROM release_files ORDER BY name, incarnation, version"
    )


def names(assets: list[Asset]) -> list[str]:
    return [asset.name for asset in assets]


def listing(directory: Path) -> list[str]:
    return sorted(path.name for path in directory.iterdir())


def downloads(fake: FakeGitHub, manifest: Manifest) -> list[str]:
    """The assets downloaded from the releases that `manifest` lists assets of, by name
    and in order. A download asks for an asset by its API URL, so each is named by what
    its release holds when this is called."""
    named = {}
    for tag in {asset.tag for asset in manifest.assets}:
        release = fake.release(tag)
        for held in release["assets"] if release else ():
            named[held["url"]] = held["name"]
    return [
        named[url]
        for _, url, headers in fake.calls
        if headers.get("Accept") == "application/octet-stream"
    ]


def test_read_stream_finds_the_assets_and_their_fingerprint(gh, workdir):
    manifest = publish_registry(
        gh,
        state={},
        open_day="2026-11-03",
        open_delta={},
        sealed={"2026-11-01": {}, "2026-11-02": {}},
    )

    read = read_stream(gh, REGISTRY, workdir)

    assert read.stream == "pypi-registry"
    assert read.manifest == manifest
    # The state, and then the deltas in day order, the open delta last. The base copy of
    # November is listed beside them, and is not read.
    assert names(read.assets) == [
        "pypi-registry-state.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-02.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]
    assert "pypi-registry-state-base-2026-11.db.zst" in names(manifest.assets)
    assert read.fingerprint == fingerprint(read.assets)
    assert read.error is None
    # The manifest is all that is downloaded.
    assert listing(workdir) == ["manifest.json"]


def no_release(gh: GitHub, fake: FakeGitHub, tmp_path: Path) -> StreamAdmission:
    """Nothing is published, so GitHub answers 404 for pypi-registry-current."""
    return REGISTRY


def a_502_on_the_manifest(
    gh: GitHub, fake: FakeGitHub, tmp_path: Path
) -> StreamAdmission:
    """The release is there, and every attempt to download its manifest gets a 502."""
    publish_registry(gh, state={}, open_day="2026-11-02", open_delta={})
    fake.fail("GET", "name=manifest.json", 502, times=6)
    return REGISTRY


def another_streams_manifest(
    gh: GitHub, fake: FakeGitHub, tmp_path: Path
) -> StreamAdmission:
    """The registry's current release holds osv's manifest."""
    publish_osv(gh, advisories=[], open_day="2026-11-02")
    foreign = tmp_path / "manifest.json"
    foreign.write_bytes(fake.asset_bytes("osv-pypi-current", "manifest.json"))
    gh.upload(gh.create_release("pypi-registry-current"), "manifest.json", foreign)
    return REGISTRY


def a_table_that_no_asset_holds(
    gh: GitHub, fake: FakeGitHub, tmp_path: Path
) -> StreamAdmission:
    """The manifest reads, and lists no asset that holds a table that is admitted."""
    publish_registry(gh, state={}, open_day="2026-11-02", open_delta={})
    return replace(REGISTRY, tables=REGISTRY.tables | {"nonesuch"})


@pytest.mark.parametrize(
    ("unreadable", "reason"),
    [
        pytest.param(no_release, "never published", id="no release"),
        pytest.param(a_502_on_the_manifest, "HTTP 502", id="a 502 on the manifest"),
        pytest.param(
            another_streams_manifest, "'osv-pypi'", id="a manifest of another stream"
        ),
        pytest.param(
            a_table_that_no_asset_holds, "nonesuch", id="a table that no asset holds"
        ),
    ],
)
def test_read_stream_reports_an_unreadable_stream(
    gh, fake, tmp_path, workdir, unreadable, reason
):
    admission = unreadable(gh, fake, tmp_path)

    read = read_stream(gh, admission, workdir)

    # The error says why, and nothing else is given.
    assert read.stream == "pypi-registry"
    assert read.error is not None
    assert reason in read.error
    assert read.manifest is None
    assert read.assets == []
    assert read.fingerprint is None


def test_release_files_sum_a_release_across_parts(gh, workdir):
    manifest = publish_registry(
        gh,
        state={"files_current": [current(SDIST), current(WHEEL)]},
        open_day="2026-11-02",
        open_delta={"files": [file(WHEEL, "1.0", LATER)]},
        sealed={"2026-11-01": {"files": [file(SDIST, "1.0", UPLOADED)]}},
    )

    stage = staged(gh, manifest, workdir)

    # One row for the release, uploaded when its first file was, with both files ever
    # and both now, and neither yanked.
    assert release_files(stage) == [(NAME, 1, "1.0", UPLOADED, 2, 2, 0, None)]


def test_release_files_take_yanks_and_removals_from_the_state(gh, workdir):
    # The deltas saw no file yanked. The state yanks both files that 1.0 has now, and no
    # longer has its py2 wheel, which was removed. The sdist came first of the two files
    # that the delta of 11-01 holds.
    removed = "sampleproject-1.0-py2-none-any.whl"
    manifest = publish_registry(
        gh,
        state={
            "files_current": [
                current(SDIST, yanked="broken"),
                current(WHEEL, yanked="broken"),
            ]
        },
        open_day="2026-11-02",
        open_delta={"files": [file(WHEEL, "1.0", LATER)]},
        sealed={
            "2026-11-01": {
                "files": [file(SDIST, "1.0", UPLOADED), file(removed, "1.0", LATER)]
            }
        },
    )

    stage = staged(gh, manifest, workdir)

    assert release_files(stage) == [(NAME, 1, "1.0", UPLOADED, 3, 2, 2, "broken")]


def test_release_files_keep_the_earliest_upload_and_the_first_reason(gh, workdir):
    # The three files of 2.0 came one in each part. The first part's file has no upload
    # time, and the last part's was uploaded before the others. The state yanks the
    # second and the third, each for its own reason, and the release keeps the reason of
    # the first part that gives one.
    early, middle, late = (f"sampleproject-2.0-{n}.tar.gz" for n in (1, 2, 3))
    manifest = publish_registry(
        gh,
        state={
            "files_current": [
                current(early),
                current(middle, yanked="unsafe"),
                current(late, yanked="superseded"),
            ]
        },
        open_day="2026-11-02",
        open_delta={"files": [file(late, "2.0", UPLOADED)]},
        sealed={"2026-11-01": {"files": [file(middle, "2.0", LATER)]}},
        parts={"2026-10": {"files": [file(early, "2.0", None)]}},
    )

    stage = staged(gh, manifest, workdir)

    assert release_files(stage) == [(NAME, 1, "2.0", UPLOADED, 3, 3, 2, "unsafe")]


def test_release_files_keep_each_incarnation_apart(gh, workdir):
    # In October, sampleproject was removed, with its files, and registered again, and
    # both of its incarnations released a 1.0. October's part holds both.
    old = "sampleproject-1.0.zip"
    new = {**file(SDIST, "1.0", LATER), "incarnation": 2}
    manifest = publish_registry(
        gh,
        state={"files_current": [{**current(SDIST), "incarnation": 2}]},
        open_day="2026-11-02",
        open_delta={},
        parts={"2026-10": {"files": [file(old, "1.0", UPLOADED), new]}},
    )

    stage = staged(gh, manifest, workdir)

    assert release_files(stage) == [
        (NAME, 1, "1.0", UPLOADED, 1, 0, 0, None),
        (NAME, 2, "1.0", LATER, 1, 1, 0, None),
    ]


def test_files_without_a_version_are_left_out(gh, workdir):
    # PyPI's page gave no version for the installer, so it is of no release.
    installer = "sampleproject-setup.exe"
    manifest = publish_registry(
        gh,
        state={"files_current": [current(SDIST), current(installer)]},
        open_day="2026-11-02",
        open_delta={
            "files": [file(SDIST, "1.0", UPLOADED), file(installer, None, UPLOADED)]
        },
    )

    stage = staged(gh, manifest, workdir)

    assert release_files(stage) == [(NAME, 1, "1.0", UPLOADED, 1, 1, 0, None)]


def test_records_and_events_come_from_every_part(gh, workdir):
    created = {
        "seq": 3,
        "name": NAME,
        "incarnation": 1,
        "kind": "created",
        "observed_at": "2026-10-05T01:00:00Z",
    }
    file_removed = {
        "seq": 41,
        "name": NAME,
        "incarnation": 1,
        "kind": "file_removed",
        "version": "1.0",
        "filename": "sampleproject-1.0-py2-none-any.whl",
        "observed_at": "2026-11-01T01:00:00Z",
    }
    yanked = {
        "seq": 57,
        "name": NAME,
        "incarnation": 1,
        "kind": "yanked",
        "version": "1.0",
        "filename": SDIST,
        "detail": "broken",
        "source_at": "2026-11-02T00:30:00Z",
        "observed_at": "2026-11-02T01:00:00Z",
    }
    manifest = publish_registry(
        gh,
        state={},
        open_day="2026-11-02",
        open_delta={
            "release_records": [{"name": NAME, "version": "1.2", "summary": "Third"}],
            "events": [yanked],
        },
        sealed={
            "2026-11-01": {
                "release_records": [{"name": NAME, "version": "1.1", "summary": "Two"}],
                "events": [file_removed],
            }
        },
        parts={
            "2026-10": {
                "release_records": [{"name": NAME, "version": "1.0", "summary": "One"}],
                "events": [created],
            }
        },
    )

    stage = staged(gh, manifest, workdir)

    # Every part's record, in the order that the stream numbered them, which is the
    # order of the parts' periods.
    assert select(
        stage.stage,
        "SELECT name, incarnation, version, summary FROM release_records ORDER BY seq",
    ) == [(NAME, 1, "1.0", "One"), (NAME, 1, "1.1", "Two"), (NAME, 1, "1.2", "Third")]
    # Every part's event, whole, with the seq that the stream gave it.
    assert select(stage.stage, "SELECT * FROM events ORDER BY seq") == [
        (3, NAME, 1, "created", None, None, None, None, "2026-10-05T01:00:00Z"),
        (
            41,
            NAME,
            1,
            "file_removed",
            "1.0",
            "sampleproject-1.0-py2-none-any.whl",
            None,
            None,
            "2026-11-01T01:00:00Z",
        ),
        (
            57,
            NAME,
            1,
            "yanked",
            "1.0",
            SDIST,
            "broken",
            "2026-11-02T00:30:00Z",
            "2026-11-02T01:00:00Z",
        ),
    ]


@pytest.mark.parametrize(
    ("table", "row"),
    [
        pytest.param(
            "release_records", {"name": NAME, "version": "1.0"}, id="a record"
        ),
        pytest.param(
            "events",
            {"seq": 7, "name": NAME, "incarnation": 1, "kind": "created"},
            id="an event",
        ),
    ],
)
def test_a_record_in_two_parts_fails_the_stage(gh, workdir, table, row):
    # A record is keyed by its project, incarnation and version, and an event by its
    # seq. Read from two parts, either would be counted twice.
    manifest = publish_registry(
        gh,
        state={},
        open_day="2026-11-02",
        open_delta={table: [row]},
        sealed={"2026-11-01": {table: [row]}},
    )

    with pytest.raises(
        sqlite3.IntegrityError, match=f"UNIQUE constraint failed: {table}"
    ):
        staged(gh, manifest, workdir)


class Watching:
    """The replies of `fake`, and what `workdir` holds each time that an asset's bytes
    are asked for."""

    def __init__(self, fake: FakeGitHub, workdir: Path) -> None:
        self.fake = fake
        self.workdir = workdir
        self.held: list[list[str]] = []

    def request(self, method, url, headers, body):
        if headers.get("Accept") == "application/octet-stream":
            self.held.append(listing(self.workdir))
        return self.fake.request(method, url, headers, body)


def test_each_part_is_deleted_once_read(fake, gh, workdir, capsys):
    manifest = publish_registry(
        gh,
        state={},
        open_day="2026-11-02",
        open_delta={},
        sealed={"2026-11-01": {}},
        parts={"2026-10": {}},
    )
    watching = Watching(fake, workdir)
    watched = GitHub(REPO, TOKEN, transport=watching, sleep=no_sleep)

    stage = stage_registry(watched, needed_assets(manifest, REGISTRY.tables), workdir)

    # The state, then October's part, the delta of 11-01 and the open delta. The state
    # comes into an empty directory, and each part once the one before it is gone, so
    # that only the state and the stage are left.
    left = sorted([stage.state.name, stage.stage.name])
    assert watching.held == [[], left, left, left]
    assert listing(workdir) == left
    # The free disk is logged as the staging starts and as it ends.
    where = re.escape(str(workdir))
    starts, ends = capsys.readouterr().err.splitlines()
    assert re.fullmatch(
        rf"staging pypi-registry starts: [0-9.]+ GB free on {where}", starts
    )
    assert re.fullmatch(
        rf"staging pypi-registry ends: [0-9.]+ GB free on {where}", ends
    )


@pytest.mark.parametrize(
    ("lacking", "table"),
    [
        pytest.param("pypi-registry-state.db", "meta", id="a state without meta"),
        pytest.param(
            "pypi-registry-delta-open.db", "serials", id="a delta without serials"
        ),
    ],
)
def test_a_database_that_lacks_a_table_is_refused(
    gh, tmp_path, workdir, lacking, table
):
    # The release is made by hand, as no stream would make it: one of its databases
    # lacks a table, which the stage does not read. It is not the registry's database,
    # so nothing is staged from it.
    made = tmp_path / "made"
    made.mkdir()
    files, entries = [], []
    for name, schema, period in [
        ("pypi-registry-state.db", STATE_SCHEMA, None),
        ("pypi-registry-delta-open.db", DELTA_SCHEMA, "2026-11-02"),
    ]:
        with closing(open_db(made / name, schema, create=True)) as con:
            if name == lacking:
                con.execute(f"DROP TABLE {table}")
        packed, entry = prepare_asset(
            made / name,
            made,
            tag="pypi-registry-current",
            role="state",
            classes=dict.fromkeys(table_counts(made / name), "append"),
            period=period,
        )
        files.append(packed)
        entries.append(entry)
    manifest = Manifest(
        stream="pypi-registry",
        producer=producer(REPO),
        generated_at="2026-11-02T00:00:00Z",
        complete=True,
        coverage={},
        data_through=Watermark("20261102", "serial"),
        cursor=None,
        licences=[],
        assets=entries,
    )
    publish_current(gh, "pypi-registry", files, manifest, bootstrap=True)

    with pytest.raises(ValueError, match=f"lacks {table}"):
        staged(gh, manifest, workdir)


def test_the_stage_tables_match_the_registrys(tmp_path):
    with (
        closing(open_db(tmp_path / "stage.db", STAGE_SCHEMA, create=True)) as stage,
        closing(open_db(tmp_path / "delta.db", DELTA_SCHEMA, create=True)) as delta,
    ):
        for table in ("release_records", "events"):
            columns = stage.execute(f"PRAGMA table_info({table})").fetchall()
            assert columns == delta.execute(f"PRAGMA table_info({table})").fetchall()
            assert columns[0][1] == "seq"


def test_the_stages_read_exactly_the_admitted_tables():
    assert READS == {s: a.tables for s, a in load_config().streams.items()}
    assert set(STAGERS) == set(READS)


def test_stage_osv_downloads_only_the_state(gh, fake, workdir):
    manifest = publish_osv(
        gh,
        advisories=[
            {"id": "PYSEC-2026-1", "summary": "A flaw"},
            {"id": "MAL-2026-2", "kind": "malware"},
        ],
        open_day="2026-11-02",
    )
    fake.calls.clear()

    stage = stage_osv(gh, needed_assets(manifest, OSV.tables), workdir)

    # The manifest lists the open delta and November's base copy beside the state.
    assert names(manifest.assets) == [
        "osv-pypi-state.db.zst",
        "osv-pypi-delta-open.db.zst",
        "osv-pypi-state-base-2026-11.db.zst",
    ]
    assert downloads(fake, manifest) == ["osv-pypi-state.db.zst"]
    assert stage == OsvStage(state=workdir / "osv-pypi-state.db")
    assert listing(workdir) == ["osv-pypi-state.db"]
    assert select(stage.state, "SELECT id, kind FROM advisories ORDER BY id") == [
        ("MAL-2026-2", "malware"),
        ("PYSEC-2026-1", "vulnerability"),
    ]


DOWNLOADS_TABLES = frozenset(
    {"projects", "days", "breaks"}
    | {"folded", "months", "monthly", "top_set", "daily", "recent"}
)


def test_the_downloads_stage_holds_the_state_and_the_serving_database(
    gh, fake, workdir
):
    manifest = publish_downloads(
        gh,
        state={
            "projects": [{"project_id": 1, "name": "sampleproject"}],
            "days": [{"day": 20_000, "seq": 1, "downloads": 7}],
            "breaks": [{"day": 19_999}],
        },
        serving={
            "folded": [{"day": 20_000, "seq": 1}],
            "monthly": [
                {
                    "project_id": 1,
                    "month": 202_110,
                    "downloads": 7,
                    "mirror_downloads": 2,
                }
            ],
        },
        open_day="2026-11-02",
        open_delta={"projects": [{"project_id": 1, "name": "sampleproject"}]},
    )
    fake.calls.clear()

    stage = stage_downloads(gh, needed_assets(manifest, DOWNLOADS_TABLES), workdir)

    assert stage == DownloadsStage(
        state=workdir / "pypi-downloads-state.db",
        serving=workdir / "pypi-downloads-serving.db",
    )
    assert downloads(fake, manifest) == [
        "pypi-downloads-state.db.zst",
        "pypi-downloads-serving.db.zst",
    ]
    assert listing(workdir) == ["pypi-downloads-serving.db", "pypi-downloads-state.db"]
    assert select(stage.state, "SELECT project_id, name FROM projects") == [
        (1, "sampleproject")
    ]
    assert select(stage.serving, "SELECT * FROM monthly") == [(1, 202_110, 7, 2)]


@pytest.mark.parametrize(
    "table", downloads_serving.TABLES, ids=lambda t: f"a serving database without {t}"
)
def test_a_serving_database_of_another_schema_is_refused(gh, tmp_path, workdir, table):
    # The release is made by hand, as no stream would make it: the serving database
    # lacks a table, so it is not the stream's serving database.
    made = tmp_path / "made"
    made.mkdir()
    files, entries = [], []
    for name, schema, role in [
        ("pypi-downloads-state.db", downloads_store.STATE_SCHEMA, "state"),
        ("pypi-downloads-serving.db", downloads_serving.SCHEMA, "public"),
    ]:
        with closing(open_db(made / name, schema, create=True)) as con:
            if name.endswith("serving.db"):
                con.execute(f"DROP TABLE {table}")
        packed, entry = prepare_asset(
            made / name,
            made,
            tag="pypi-downloads-current",
            role=role,
            classes=dict.fromkeys(table_counts(made / name), "derived"),
        )
        files.append(packed)
        entries.append(entry)
    manifest = Manifest(
        stream="pypi-downloads",
        producer=producer(REPO),
        generated_at="2026-11-02T00:00:00Z",
        complete=False,
        coverage={},
        data_through=Watermark("2026-11-02", "day"),
        cursor=None,
        licences=[],
        assets=entries,
    )
    publish_current(gh, "pypi-downloads", files, manifest, bootstrap=True)

    with pytest.raises(ValueError, match=f"lacks {table}"):
        stage_downloads(gh, entries, workdir)
