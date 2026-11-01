import dataclasses
import hashlib
import json
import platform
import sqlite3
import struct
from collections.abc import Sequence
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

import obskit
from obskit.gates import GateFailure
from obskit.github import GitHub
from obskit.http import Http
from obskit.manifest import Asset, Manifest, TableInfo, Watermark, dumps, loads
from obskit.release import CreateOnceError, PriorStateError, fetch_assets
from obskit.sqlite import table_counts
from obskit.testing import FakeGitHub, FakeHttp
from obskit.zst import compress_file, decompress_file, sha256_file
from pypi_registry.layout import OPEN_DELTA_DB, STATE_DB, base_state_name, delta_name
from pypi_registry.run import (
    GateError,
    RunReport,
    StaleRootError,
    main,
    replay,
    run_once,
)
from pypi_registry.simple import ACCEPT, ROOT_URL, project_url, version_url
from pypi_registry.store import STATE_SCHEMA, diff_states, open_db
from run_helpers import (
    OPENED,
    Clock,
    Ticking,
    dist,
    files_of,
    page,
    project_rows,
    release,
    versions_of,
    written,
)

# A time long before any run.
OLD = "2026-01-01T00:00:00Z"


# ---- the run: prior state, rollover, apply, manifest, gates and publish

REPO = "py-observatory/data"
CURRENT = "pypi-registry-current"
NOVEMBER = "pypi-registry-month-2026-11"
DECEMBER = "pypi-registry-month-2026-12"
MANIFEST = "manifest.json"
STATE = f"{STATE_DB}.zst"
DELTA = f"{OPEN_DELTA_DB}.zst"
BASE_NOVEMBER = "pypi-registry-state-base-2026-11.db.zst"
BASE_DECEMBER = "pypi-registry-state-base-2026-12.db.zst"
PART_NOVEMBER = "pypi-registry-2026-11.db.zst"
MANIFEST_NOVEMBER = "manifest-2026-11.json"
# The projects that the runs below start from, with their serials.
PROJECTS = {"alpha": 10, "beta": 20, "gamma": 30}
# What a manifest says of the stream's upstream: stream.toml's entry.
LICENCES = [
    {
        "name": "PyPI Simple and JSON APIs",
        "url": "https://pypi.org/",
        "licence": "NOASSERTION",
        "terms": "https://policies.python.org/pypi.org/Terms-of-Use/",
    }
]
DELTA_TABLES = (
    "cursors",
    "events",
    "files",
    "release_records",
    "serials",
    "swept",
    "versions",
)


def sealed_name(day: str) -> str:
    """The asset of the sealed delta of `day`, given as YYYY-MM-DD."""
    return f"{delta_name(date.fromisoformat(day))}.zst"


def no_sleep(seconds: float) -> None:
    """GitHub's retries and pacing wait for nothing in a test."""


def root(serial: int, projects: dict[str, int]) -> dict:
    """The Simple API root as PyPI serves it, listing `projects` with their serials. It
    carries `meta.api-version` and `meta._last-serial`, which the parser requires."""
    return {
        "meta": {"api-version": "1.4", "_last-serial": serial},
        "projects": [
            {"name": name, "_last-serial": serial} for name, serial in projects.items()
        ],
    }


class Stream:
    """The runs of the stream, on one fake GitHub and one fake PyPI.

    Each run has a work directory of its own, as each run has a new runner, so all that
    a run leaves to the next goes through the releases. The GitHub client is one for
    the whole test, as `main` makes one for a process.
    """

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.github = FakeGitHub(REPO)
        self.gh = GitHub(REPO, "t0ken", transport=self.github, sleep=no_sleep)
        self.pypi = FakeHttp()
        self.clock = Clock()
        self.http = Http(
            transport=Ticking(self.pypi, self.clock),
            sleep=self.clock.sleep,
            clock=self.clock,
        )
        self.runs = 0
        self.made = 0  # files made by the helpers below, each in a folder of its own

    def run(
        self,
        at: str,
        serial: int,
        projects: dict[str, int],
        *,
        pages: Sequence[dict] = (),
        releases: Sequence[tuple[str, str]] = (),
        bootstrap: bool = False,
        override_reason: str | None = None,
        max_seconds: float = 18_000,
    ) -> RunReport:
        """A run at `at`, a UTC time, that reads a root at `serial` listing `projects`.

        The root and each of `pages` are served once, the pages at their projects'
        URLs, and so is the release record of each (name, version) of `releases`. Any
        other page is a 404, as a project that PyPI does not have is.
        """
        self.pypi.add(ROOT_URL, body=root(serial, projects), times=1)
        for body in pages:
            self.pypi.add(project_url(body["name"]), body=body, times=1)
        for name, version in releases:
            self.pypi.add(
                version_url(name, version), body=release(name, version), times=1
            )
        self.runs += 1
        workdir = self.tmp_path / f"run-{self.runs}"
        workdir.mkdir()
        return run_once(
            self.gh,
            self.http,
            workdir,
            now=datetime.fromisoformat(at),
            bootstrap=bootstrap,
            override_reason=override_reason,
            max_seconds=max_seconds,
            clock=self.clock,
        )

    def folder(self, label: str) -> Path:
        self.made += 1
        path = self.tmp_path / label / str(self.made)
        path.mkdir(parents=True)
        return path

    def manifest(self, tag: str = CURRENT, name: str = MANIFEST) -> Manifest:
        return loads(self.github.asset_bytes(tag, name).decode())

    def entries(self) -> dict[str, Asset]:
        """The assets that the current manifest lists, by name."""
        return {asset.name: asset for asset in self.manifest().assets}

    def assets(self, tag: str) -> list[str]:
        """The names of the assets of the release `tag`, sorted."""
        release = self.github.release(tag)
        return sorted(asset["name"] for asset in release["assets"]) if release else []

    def expand(self, tag: str, name: str) -> Path:
        """The database of the asset `name` of `tag`, expanded into a file."""
        packed = self.folder("published") / name
        packed.write_bytes(self.github.asset_bytes(tag, name))
        db = packed.with_suffix("")
        decompress_file(packed, db)
        return db

    def open(self, tag: str, name: str) -> sqlite3.Connection:
        """The database of the asset `name` of `tag`, opened to read."""
        conn = sqlite3.connect(f"{self.expand(tag, name).as_uri()}?mode=ro", uri=True)
        OPENED.append(conn)
        return conn

    def writes(self) -> list[tuple[str, str]]:
        """The requests to GitHub that change something, in order."""
        return [
            (method, url) for method, url, _ in self.github.calls if method != "GET"
        ]

    def replace_asset(self, tag: str, name: str, path: Path) -> None:
        """Put the file at `path` in place of the asset `name` of `tag`, as a hand edit
        would."""
        release = self.gh.release_by_tag(tag)
        held = {asset["name"]: asset for asset in self.gh.assets(release)}
        self.gh.delete(held[name])
        self.gh.upload(release, name, path)

    def replace_manifest(self, manifest: Manifest) -> None:
        """Put `manifest` in place of the current release's manifest.json."""
        path = self.folder("edited") / MANIFEST
        path.write_text(dumps(manifest), encoding="utf-8")
        self.replace_asset(CURRENT, MANIFEST, path)


def upload_names(stream: Stream) -> list[str]:
    """The names of the assets uploaded, in order."""
    return [
        parse_qs(urlsplit(url).query)["name"][0]
        for method, url, _ in stream.github.calls
        if method == "POST" and url.startswith("https://uploads.github.com/")
    ]


def meta_of(state: sqlite3.Connection) -> dict[str, str]:
    return dict(state.execute("SELECT key, value FROM meta").fetchall())


def test_bootstrap_publishes_state_base_and_manifest(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "0123456789abcdef0123456789abcdef01234567")
    monkeypatch.setenv("GITHUB_RUN_ID", "1234567890")
    stream = Stream(tmp_path)

    report = stream.run("2026-11-02T06:00:00Z", 100, PROJECTS, bootstrap=True)

    # The current release holds the state, the open delta and the manifest. The
    # month's release holds the state as the month began, which is the same bytes.
    assert stream.assets(CURRENT) == sorted([STATE, DELTA, MANIFEST])
    assert stream.assets(NOVEMBER) == [BASE_NOVEMBER]
    base_bytes = stream.github.asset_bytes(NOVEMBER, BASE_NOVEMBER)
    assert base_bytes == stream.github.asset_bytes(CURRENT, STATE)
    for tag in (CURRENT, NOVEMBER):
        made = stream.github.release(tag)
        assert (made["prerelease"], made["make_latest"]) == (True, "false")
    # Each project of the root is in the state at its serial, not swept, and first
    # seen at the bootstrap, which is not when it first appeared.
    state = stream.open(CURRENT, STATE)
    assert state.execute("SELECT * FROM projects ORDER BY name").fetchall() == [
        (name, 1, serial, "active", 0, "2026-11-02T06:00:00Z", 0)
        for name, serial in sorted(PROJECTS.items())
    ]
    assert meta_of(state) == {
        "applied_seq": "0",
        "bootstrap_at": "2026-11-02T06:00:00Z",
        "cursor": "100",
    }
    assert files_of(state) == versions_of(state) == []
    # No event is written, and no row of any kind: the open delta is empty.
    assert table_counts(stream.expand(CURRENT, DELTA)) == dict.fromkeys(DELTA_TABLES, 0)

    manifest = stream.manifest()
    state_entry, delta_entry, base_entry = manifest.assets
    assert (state_entry.name, state_entry.tag, state_entry.role) == (
        STATE,
        CURRENT,
        "state",
    )
    assert (state_entry.period, state_entry.closed) == (None, False)
    assert state_entry.tables == {
        "files_current": TableInfo(0, "snapshot"),
        "meta": TableInfo(3, "snapshot"),
        "projects": TableInfo(3, "append"),
        "versions_seen": TableInfo(0, "append"),
    }
    assert (delta_entry.name, delta_entry.tag, delta_entry.role) == (
        DELTA,
        CURRENT,
        "state",
    )
    assert (delta_entry.period, delta_entry.closed) == ("2026-11-02", False)
    assert base_entry == dataclasses.replace(
        state_entry, name=BASE_NOVEMBER, tag=NOVEMBER, period="2026-11", closed=True
    )
    assert manifest.data_through == Watermark("100", "serial")
    assert manifest.cursor == "100"
    assert manifest.coverage == {
        "projects": 3,
        "swept": 0,
        "unswept": 3,
        "remaining": 0,
        "bootstrap_at": "2026-11-02T06:00:00Z",
    }
    assert manifest.complete is True
    assert manifest.generated_at == "2026-11-02T06:00:00Z"
    assert manifest.licences == LICENCES
    assert manifest.producer == {
        "repo": REPO,
        "commit": "0123456789abcdef0123456789abcdef01234567",
        "run_id": "1234567890",
        "obskit": obskit.__version__,
        "python": platform.python_version(),
    }
    assert (manifest.series_breaks, manifest.corrections, manifest.overrides) == (
        [],
        [],
        [],
    )
    # The bootstrap reads the root, and no page.
    assert [url for url, _ in stream.pypi.requests] == [ROOT_URL]
    assert stream.pypi.requests[0][1]["Accept"] == ACCEPT
    assert report == RunReport(
        new=0,
        changed=0,
        vanished=0,
        returned=0,
        versions_fetched=0,
        requests=1,
        swept=0,
        remaining=0,
        gate_failures=[],
    )


def test_second_run_records_changes(tmp_path):
    stream = Stream(tmp_path)
    stream.run("2026-11-02T06:00:00Z", 100, PROJECTS, bootstrap=True)
    # beta gains a file, and with it a version, after the bootstrap.
    beta = page(
        "beta", 25, dist("beta-1.0.tar.gz", "2026-11-02T09:00:00Z"), versions=["1.0"]
    )

    report = stream.run(
        "2026-11-02T12:00:00Z",
        110,
        {**PROJECTS, "beta": 25},
        pages=[beta],
        releases=[("beta", "1.0")],
    )

    delta = stream.open(CURRENT, DELTA)
    assert written(delta, 0) == [
        ("file", "beta", 1, "beta-1.0.tar.gz", 1),
        ("version", "beta", 1, "1.0", 1),
        ("record", "beta", 1, "1.0"),
        ("swept", "beta", 1, "active"),
        ("serial", "beta", 25),
        ("cursor", 110),
    ]
    # The state is the delta applied, through its last seq.
    state = stream.open(CURRENT, STATE)
    assert project_rows(state)["beta"] == (1, 25, "active", 1)
    assert files_of(state) == [("beta", 1, "beta-1.0.tar.gz", 0, None)]
    assert versions_of(state) == [("beta", 1, "1.0", 1)]
    assert meta_of(state) == {
        "applied_seq": "6",
        "bootstrap_at": "2026-11-02T06:00:00Z",
        "cursor": "110",
    }
    manifest = stream.manifest()
    entries = stream.entries()
    assert (manifest.cursor, manifest.data_through) == (
        "110",
        Watermark("110", "serial"),
    )
    assert entries[DELTA].period == "2026-11-02"
    assert {table: info.rows for table, info in entries[DELTA].tables.items()} == {
        "cursors": 1,
        "events": 0,
        "files": 1,
        "release_records": 1,
        "serials": 1,
        "swept": 1,
        "versions": 1,
    }
    assert entries[BASE_NOVEMBER].closed, "the closed asset is carried over"
    assert manifest.coverage == {
        "projects": 3,
        "swept": 1,
        "unswept": 2,
        "remaining": 0,
        "bootstrap_at": "2026-11-02T06:00:00Z",
    }
    assert manifest.complete is True
    assert report == RunReport(
        new=0,
        changed=1,
        vanished=0,
        returned=0,
        versions_fetched=1,
        requests=3,
        swept=1,
        remaining=0,
        gate_failures=[],
    )


def test_stale_root_refused_without_writes(tmp_path):
    stream = Stream(tmp_path)
    stream.run("2026-11-02T06:00:00Z", 100, PROJECTS, bootstrap=True)
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()
    stream.pypi.requests.clear()

    # The next day's root is an older copy than the one that the state was built
    # from. A new day would seal the open delta, so a write could come early.
    with pytest.raises(StaleRootError, match="99"):
        stream.run("2026-11-03T06:00:00Z", 99, {**PROJECTS, "beta": 25})

    # The run read its prior state and the root, and wrote nothing at all.
    assert stream.writes() == []
    assert [url for url, _ in stream.pypi.requests] == [ROOT_URL]
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held
    assert stream.assets(NOVEMBER) == [BASE_NOVEMBER]


def test_day_rollover_seals_delta_once(tmp_path):
    stream = Stream(tmp_path)
    stream.run("2026-11-02T17:00:00Z", 100, PROJECTS, bootstrap=True)
    # Each run finds one project changed, whose page lists no file.
    stream.run(
        "2026-11-02T23:00:00Z",
        110,
        {**PROJECTS, "alpha": 11},
        pages=[page("alpha", 11)],
    )
    stream.run(
        "2026-11-03T05:00:00Z",
        120,
        {**PROJECTS, "alpha": 11, "beta": 21},
        pages=[page("beta", 21)],
    )
    stream.run(
        "2026-11-03T11:00:00Z",
        130,
        {"alpha": 11, "beta": 21, "gamma": 31},
        pages=[page("gamma", 31)],
    )

    sealed = sealed_name("2026-11-02")
    assert stream.assets(NOVEMBER) == sorted([BASE_NOVEMBER, sealed])
    assert upload_names(stream).count(sealed) == 1
    listed = [asset for asset in stream.manifest().assets if asset.name == sealed]
    assert [(a.tag, a.role, a.period, a.closed) for a in listed] == [
        (NOVEMBER, "public", "2026-11-02", True)
    ]
    assert stream.entries()[DELTA].period == "2026-11-03"
    # The sealed delta holds the rows of the runs of 11-02, and the open delta those
    # of 11-03, numbered on from them.
    assert written(stream.open(NOVEMBER, sealed), 0) == [
        ("swept", "alpha", 1, "active"),
        ("serial", "alpha", 11),
        ("cursor", 110),
    ]
    open_delta = stream.open(CURRENT, DELTA)
    assert written(open_delta, 0) == [
        ("swept", "beta", 1, "active"),
        ("serial", "beta", 21),
        ("cursor", 120),
        ("swept", "gamma", 1, "active"),
        ("serial", "gamma", 31),
        ("cursor", 130),
    ]
    assert open_delta.execute("SELECT seq FROM cursors").fetchall() == [(6,), (9,)]


def test_month_close_builds_month_part_and_new_base(tmp_path):
    stream = Stream(tmp_path)
    stream.run("2026-11-27T06:00:00Z", 100, PROJECTS, bootstrap=True)
    # 11-28: a new project, with a file and its release.
    new = page(
        "delta", 105, dist("delta-1.0.tar.gz", "2026-11-28T01:00:00Z"), versions=["1.0"]
    )
    stream.run(
        "2026-11-28T06:00:00Z",
        110,
        {**PROJECTS, "delta": 105},
        pages=[new],
        releases=[("delta", "1.0")],
    )
    # 11-29: gamma has gone, and its probe is a 404. beta changed.
    stream.run(
        "2026-11-29T06:00:00Z",
        120,
        {"alpha": 10, "beta": 21, "delta": 105},
        pages=[page("beta", 21)],
    )
    # 11-30: another new project, with no file.
    stream.run(
        "2026-11-30T06:00:00Z",
        125,
        {"alpha": 10, "beta": 21, "delta": 105, "eta": 122},
        pages=[page("eta", 122)],
    )
    last_state = stream.github.asset_bytes(CURRENT, STATE)
    last = stream.manifest()
    # The removed project is not counted, and alpha is not read in full yet.
    assert last.coverage == {
        "projects": 4,
        "swept": 3,
        "unswept": 1,
        "remaining": 0,
        "bootstrap_at": "2026-11-27T06:00:00Z",
    }

    stream.run(
        "2026-12-01T06:00:00Z",
        130,
        {"alpha": 11, "beta": 21, "delta": 105, "eta": 122},
        pages=[page("alpha", 11)],
    )

    days = ["2026-11-27", "2026-11-28", "2026-11-29", "2026-11-30"]
    month_assets = [BASE_NOVEMBER, *map(sealed_name, days), PART_NOVEMBER]
    assert stream.assets(NOVEMBER) == sorted([*month_assets, MANIFEST_NOVEMBER])
    assert stream.assets(DECEMBER) == [BASE_DECEMBER]
    # December starts from the state that November ended with, as published.
    assert stream.github.asset_bytes(DECEMBER, BASE_DECEMBER) == last_state
    # The part holds every row of the month's deltas. Three of them have events,
    # serials and cursors, so the merge met seqs from more than one delta.
    sealed = [table_counts(stream.expand(NOVEMBER, sealed_name(day))) for day in days]
    assert all(c["events"] and c["serials"] and c["cursors"] for c in sealed[1:])
    merged = table_counts(stream.expand(NOVEMBER, PART_NOVEMBER))
    assert merged == {table: sum(c[table] for c in sealed) for table in DELTA_TABLES}
    entries = stream.entries()
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
    assert dataclasses.replace(month, assets=last.assets) == last
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
    # The current manifest lists every closed asset of the stream, and the new day's
    # open delta.
    assert sorted(name for name, asset in entries.items() if asset.closed) == sorted(
        [*month_assets, MANIFEST_NOVEMBER, BASE_DECEMBER]
    )
    assert entries[DELTA].period == "2026-12-01"


def restored_differences(stream: Stream, *, with_open_delta: bool = True) -> list[str]:
    """Rebuild the current state from the releases alone, as a restore would: the base
    copy of the open delta's month, then the month's sealed deltas in day order and
    the open delta, replayed as the runs applied them. Returns the tables in which the
    result differs from the published state."""
    entries = stream.entries()
    day = date.fromisoformat(entries[DELTA].period)
    base = entries[f"{base_state_name(day.year, day.month)}.zst"]
    sealed = sorted(
        (
            asset
            for asset in entries.values()
            if asset.tag == base.tag and asset.name.startswith("pypi-registry-delta-")
        ),
        key=lambda asset: asset.period,
    )
    asked = [base, *sealed, entries[DELTA], entries[STATE]]
    base_db, *deltas, state_db = fetch_assets(
        stream.gh, asked, stream.folder("restore")
    )
    replay(base_db, deltas if with_open_delta else deltas[:-1])
    with closing(open_db(base_db, STATE_SCHEMA)) as restored:
        return diff_states(restored, state_db)


def test_restore_month_base_plus_deltas_equals_current(tmp_path):
    stream = Stream(tmp_path)
    held = page("held", 45, status="quarantined")
    listed = {"alpha": 10, "beta": 20, "gamma": 30, "held": 40}
    runs = [
        # 11-29: the bootstrap; then a new project, and an old one read in full.
        {"at": "2026-11-29T06:00:00Z", "serial": 100, "bootstrap": True},
        {
            "at": "2026-11-29T12:00:00Z",
            "serial": 110,
            "listed": {"alpha": 11, "delta": 105},
            "pages": [
                page("alpha", 11, dist("alpha-1.0.tar.gz", OLD), versions=["1.0"]),
                page(
                    "delta",
                    105,
                    dist("delta-1.0.tar.gz", "2026-11-29T08:00:00Z"),
                    versions=["1.0"],
                ),
            ],
            "releases": [("delta", "1.0")],
        },
        # 11-30: gamma is removed and held is quarantined; then a file of delta is
        # yanked, and another added.
        {
            "at": "2026-11-30T06:00:00Z",
            "serial": 120,
            "listed": {"gamma": None, "held": None},
            "pages": [held],
        },
        {
            "at": "2026-11-30T18:00:00Z",
            "serial": 130,
            "listed": {"delta": 106},
            "pages": [
                held,
                page(
                    "delta",
                    106,
                    dist("delta-1.0.tar.gz", "2026-11-29T08:00:00Z", yanked="broken"),
                    dist("delta-1.1.tar.gz", "2026-11-30T10:00:00Z"),
                    versions=["1.0", "1.1"],
                ),
            ],
            "releases": [("delta", "1.1")],
        },
        # 12-01: November closes and alpha's file is deleted; then gamma is
        # registered again.
        {
            "at": "2026-12-01T06:00:00Z",
            "serial": 140,
            "listed": {"alpha": 12},
            "pages": [held, page("alpha", 12)],
        },
        {
            "at": "2026-12-01T12:00:00Z",
            "serial": 150,
            "listed": {"gamma": 145},
            "pages": [
                held,
                page(
                    "gamma",
                    145,
                    dist("gamma-2.0.tar.gz", "2026-12-01T10:00:00Z"),
                    versions=["2.0"],
                ),
            ],
            "releases": [("gamma", "2.0")],
        },
        # 12-02: held is out of quarantine and listed again.
        {
            "at": "2026-12-02T06:00:00Z",
            "serial": 160,
            "listed": {"held": 155},
            "pages": [
                page("held", 155, dist("held-1.0.tar.gz", OLD), versions=["1.0"])
            ],
        },
    ]
    for step in runs:
        # A name listed as None leaves the root.
        for name, serial in step.get("listed", {}).items():
            if serial is None:
                del listed[name]
            else:
                listed[name] = serial
        stream.run(
            step["at"],
            step["serial"],
            listed,
            pages=step.get("pages", ()),
            releases=step.get("releases", ()),
            bootstrap=step.get("bootstrap", False),
        )

        # After each run, the releases alone give back the state that it published.
        assert restored_differences(stream) == [], step["at"]

    # The runs wrote the events that they were given, so seven kinds of event went
    # through the restores: November's are in its part, and December's are in its
    # sealed delta and the open delta. Without the open delta, the state differs.
    def kinds(tag: str, name: str) -> list[str]:
        rows = stream.open(tag, name).execute("SELECT kind FROM events ORDER BY seq")
        return [kind for (kind,) in rows]

    assert kinds(NOVEMBER, PART_NOVEMBER) == [
        "created",
        "removed",
        "quarantined",
        "yanked",
    ]
    assert kinds(DECEMBER, sealed_name("2026-12-01")) == [
        "file_removed",
        "reregistered",
    ]
    assert kinds(CURRENT, DELTA) == ["unquarantined"]
    assert restored_differences(stream, with_open_delta=False) != []
    state = stream.open(CURRENT, STATE)
    assert project_rows(state) == {
        "alpha": (1, 12, "active", 1),
        "beta": (1, 20, "active", 0),
        "delta": (1, 106, "active", 1),
        "gamma": (2, 145, "active", 1),
        "held": (1, 155, "active", 1),
    }


def test_gate_failure_blocks_publish_unless_override(tmp_path):
    stream = Stream(tmp_path)
    stream.run("2026-11-02T06:00:00Z", 100, PROJECTS, bootstrap=True)
    # Someone edits the manifest to say that the state held 1,000 projects. The next
    # state holds 3, so an append table seems to have lost rows.
    manifest = stream.manifest()
    state, *others = manifest.assets
    tables = {**state.tables, "projects": TableInfo(1_000, "append")}
    edited = dataclasses.replace(state, tables=tables)
    stream.replace_manifest(dataclasses.replace(manifest, assets=[edited, *others]))
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()
    failure = GateFailure(
        "rows",
        f"the append table projects in {STATE} went from 1,000 rows to 3",
    )

    with pytest.raises(GateError) as caught:
        stream.run("2026-11-02T12:00:00Z", 110, PROJECTS)

    # Nothing is published.
    assert caught.value.failures == [failure]
    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held

    # A person looks, and gives a reason. The run publishes, and logs why.
    reason = "The manifest was edited by hand; the state is right."
    report = stream.run("2026-11-02T13:00:00Z", 110, PROJECTS, override_reason=reason)

    assert report.gate_failures == [failure]
    logged = {
        "at": "2026-11-02T13:00:00Z",
        "reason": reason,
        "failures": [{"rule": "rows", "detail": failure.detail}],
    }
    assert stream.manifest().overrides == [logged]

    # The next run is compared with what was published, and passes. The log stays.
    report = stream.run("2026-11-02T18:00:00Z", 120, PROJECTS)

    assert report.gate_failures == []
    assert stream.manifest().overrides == [logged]


def test_mass_removal_needs_override(tmp_path):
    stream = Stream(tmp_path)
    names = [f"p{index:04d}" for index in range(2_006)]
    stream.run("2026-11-02T06:00:00Z", 100, dict.fromkeys(names, 10), bootstrap=True)

    # A thousand names leave the root, and the probe of each is a 404.
    report = stream.run("2026-11-02T08:00:00Z", 110, dict.fromkeys(names[1_000:], 10))

    assert (report.vanished, report.gate_failures) == (1_000, [])

    # The limit is on one run: the day's delta now holds 1,005 removals.
    report = stream.run("2026-11-02T09:00:00Z", 115, dict.fromkeys(names[1_005:], 10))

    assert (report.vanished, report.gate_failures) == (5, [])

    # One more than the limit in one run is held for a person to look at, though
    # the removals explain every file that goes.
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()
    failure = GateFailure("removals", "1,001 removed events in one run (limit 1,000)")

    with pytest.raises(GateError) as caught:
        stream.run("2026-11-02T10:00:00Z", 120, {})

    assert caught.value.failures == [failure]
    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held

    reason = "PyPI removed a wave of spam projects."
    report = stream.run("2026-11-02T12:00:00Z", 120, {}, override_reason=reason)

    assert (report.vanished, report.gate_failures) == (1_001, [failure])
    assert stream.manifest().overrides == [
        {
            "at": "2026-11-02T12:00:00Z",
            "reason": reason,
            "failures": [{"rule": "removals", "detail": failure.detail}],
        }
    ]
    removed = stream.open(CURRENT, STATE).execute(
        "SELECT count(*) FROM projects WHERE status = 'removed'"
    )
    assert removed.fetchone() == (2_006,)


@pytest.mark.parametrize(
    "override_reason", [None, "Checked by hand."], ids=["no reason", "a reason"]
)
def test_month_close_replay_mismatch_fails(tmp_path, override_reason):
    stream = Stream(tmp_path)
    stream.run("2026-11-29T06:00:00Z", 100, PROJECTS, bootstrap=True)
    stream.run(
        "2026-11-30T06:00:00Z",
        110,
        {**PROJECTS, "alpha": 11},
        pages=[page("alpha", 11)],
    )
    # Someone changes a value in November's base copy, and lists the changed copy in
    # the manifest, so that it downloads and checks. The month replays onto it, but
    # cannot give back the state that the runs built.
    base_db = stream.expand(NOVEMBER, BASE_NOVEMBER)
    with closing(sqlite3.connect(base_db)) as conn:
        conn.execute("UPDATE projects SET first_seen = ? WHERE name = 'beta'", (OLD,))
        conn.commit()
    packed = stream.folder("edited") / BASE_NOVEMBER
    compress_file(base_db, packed)
    stream.replace_asset(NOVEMBER, BASE_NOVEMBER, packed)
    manifest = stream.manifest()
    edited = [
        dataclasses.replace(
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
    stream.replace_manifest(dataclasses.replace(manifest, assets=edited))
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()

    # A reason does not publish past it, as it does past a gate: what the run
    # would publish could not be restored from the month's releases.
    with pytest.raises(GateError, match="cannot be overridden") as caught:
        stream.run(
            "2026-12-01T06:00:00Z",
            120,
            {**PROJECTS, "alpha": 11},
            override_reason=override_reason,
        )

    (failure,) = caught.value.failures
    assert failure.rule == "replay"
    assert "differs from the state in projects" in failure.detail
    # Nothing is published, to the current release or to any other: not the seal of
    # 11-30, nor the month's part, nor December's base.
    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held
    assert stream.assets(NOVEMBER) == sorted([BASE_NOVEMBER, sealed_name("2026-11-29")])
    assert stream.github.release(DECEMBER) is None


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


def test_seal_reuses_prior_open_delta_bytes(tmp_path, monkeypatch):
    stream = Stream(tmp_path)
    stream.run("2026-11-29T06:00:00Z", 100, PROJECTS, bootstrap=True)
    stream.run(
        "2026-11-29T12:00:00Z",
        110,
        {**PROJECTS, "alpha": 11},
        pages=[page("alpha", 11)],
    )
    prior = stream.entries()
    open_bytes = stream.github.asset_bytes(CURRENT, DELTA)
    # The next run has another zstd, which would compress the delta to other bytes.
    monkeypatch.setattr("obskit.release.compress_file", Build("b"))
    again = stream.folder("again") / DELTA
    Build("b")(stream.expand(CURRENT, DELTA), again)
    assert again.read_bytes() != open_bytes

    stream.run(
        "2026-11-30T06:00:00Z",
        120,
        {**PROJECTS, "alpha": 11, "beta": 21},
        pages=[page("beta", 21)],
    )

    # The sealed delta is the file that the last run published, byte for byte.
    sealed = sealed_name("2026-11-29")
    assert stream.github.asset_bytes(NOVEMBER, sealed) == open_bytes
    assert stream.entries()[sealed].asset_sha256 == prior[DELTA].asset_sha256
    assert stream.entries()[sealed] == dataclasses.replace(
        prior[DELTA],
        name=sealed,
        tag=NOVEMBER,
        role="public",
        period="2026-11-29",
        closed=True,
    )

    # So is the base copy that the end of the month makes, with yet another build.
    prior = stream.entries()
    state_bytes = stream.github.asset_bytes(CURRENT, STATE)
    open_bytes = stream.github.asset_bytes(CURRENT, DELTA)
    monkeypatch.setattr("obskit.release.compress_file", Build("c"))

    stream.run("2026-12-01T06:00:00Z", 130, {**PROJECTS, "alpha": 11, "beta": 21})

    assert stream.github.asset_bytes(DECEMBER, BASE_DECEMBER) == state_bytes
    assert stream.entries()[BASE_DECEMBER] == dataclasses.replace(
        prior[STATE], name=BASE_DECEMBER, tag=DECEMBER, period="2026-12", closed=True
    )
    assert stream.github.asset_bytes(NOVEMBER, sealed_name("2026-11-30")) == open_bytes


@pytest.mark.parametrize(
    ("case", "named"),
    [
        ("no state", STATE),
        ("no open delta", DELTA),
        ("open delta not state", DELTA),
        ("state in a month release", STATE),
        ("open delta of no day", DELTA),
    ],
)
def test_run_refuses_prior_without_state(tmp_path, case, named):
    stream = Stream(tmp_path)
    stream.run("2026-11-02T06:00:00Z", 100, PROJECTS, bootstrap=True)
    manifest = stream.manifest()
    state, delta, base = manifest.assets
    assets = {
        "no state": [delta, base],
        "no open delta": [state, base],
        "open delta not state": [
            state,
            dataclasses.replace(delta, role="public"),
            base,
        ],
        "state in a month release": [
            dataclasses.replace(state, tag=NOVEMBER),
            delta,
            base,
        ],
        "open delta of no day": [state, dataclasses.replace(delta, period=None), base],
    }[case]
    stream.replace_manifest(dataclasses.replace(manifest, assets=assets))
    stream.github.calls.clear()
    stream.pypi.requests.clear()

    # The prior state would otherwise be an empty one, or no state at all.
    with pytest.raises(PriorStateError, match=named):
        stream.run("2026-11-02T12:00:00Z", 110, PROJECTS)

    assert stream.writes() == []
    assert stream.pypi.requests == [], "it is refused before the root is read"


def test_failed_processing_publishes_nothing_and_rollover_is_redone(
    tmp_path, monkeypatch
):
    stream = Stream(tmp_path)
    stream.run("2026-11-30T06:00:00Z", 100, PROJECTS, bootstrap=True)
    stream.run(
        "2026-11-30T12:00:00Z",
        110,
        {**PROJECTS, "alpha": 11},
        pages=[page("alpha", 11)],
    )
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    listed = {**PROJECTS, "alpha": 11, "beta": 21}
    # The first run of December closes November, and then reads a page that is not
    # JSON, which ends the run.
    monkeypatch.setenv("GITHUB_RUN_ID", "1")
    stream.pypi.add(project_url("beta"), body=b"<html>maintenance</html>", times=1)

    with pytest.raises(ValueError, match="not JSON"):
        stream.run("2026-12-01T06:00:00Z", 120, listed)

    # Nothing is published to the current release. What the rollover added to the
    # months stays: it is made from what was published, and the run's build.
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held
    rolled = {
        NOVEMBER: sorted(
            [BASE_NOVEMBER, sealed_name("2026-11-30"), PART_NOVEMBER, MANIFEST_NOVEMBER]
        ),
        DECEMBER: [BASE_DECEMBER],
    }
    assert {tag: stream.assets(tag) for tag in rolled} == rolled
    kept = {
        (tag, name): stream.github.asset_bytes(tag, name)
        for tag, names in rolled.items()
        for name in names
    }

    # A later run, with another run id but the same zstd and SQLite, does the same
    # rollover again. It finds the same bytes in place, adds nothing to the months,
    # and publishes. With another build it would stop (see the next test).
    monkeypatch.setenv("GITHUB_RUN_ID", "2")
    stream.github.calls.clear()

    stream.run("2026-12-01T12:00:00Z", 130, listed, pages=[page("beta", 21)])

    assert {tag: stream.assets(tag) for tag in rolled} == rolled
    assert {key: stream.github.asset_bytes(*key) for key in kept} == kept
    assert all(name.startswith("swap-next-") for name in upload_names(stream))
    entries = stream.entries()
    assert all(name in entries for names in rolled.values() for name in names)
    assert entries[DELTA].period == "2026-12-01"


def test_month_part_of_another_build_stops_the_run(tmp_path, monkeypatch):
    stream = Stream(tmp_path)
    stream.run("2026-11-30T06:00:00Z", 100, PROJECTS, bootstrap=True)
    stream.run(
        "2026-11-30T12:00:00Z",
        110,
        {**PROJECTS, "alpha": 11},
        pages=[page("alpha", 11)],
    )
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    listed = {**PROJECTS, "alpha": 11, "beta": 21}
    # The first run of December adds November's part, and then fails on a page.
    stream.pypi.add(project_url("beta"), body=b"<html>maintenance</html>", times=1)
    with pytest.raises(ValueError, match="not JSON"):
        stream.run("2026-12-01T06:00:00Z", 120, listed)
    part = stream.github.asset_bytes(NOVEMBER, PART_NOVEMBER)
    # The next attempt has another zstd, so the part that it builds afresh is other
    # bytes. The part in the release is never replaced, so the run stops, before it
    # adds anything, for a person to look at.
    monkeypatch.setattr("obskit.release.compress_file", Build("b"))
    stream.github.calls.clear()

    with pytest.raises(CreateOnceError, match=PART_NOVEMBER):
        stream.run("2026-12-01T12:00:00Z", 130, listed, pages=[page("beta", 21)])

    assert stream.writes() == []
    assert stream.github.asset_bytes(NOVEMBER, PART_NOVEMBER) == part
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held


def test_run_out_of_time_publishes_incomplete(tmp_path):
    stream = Stream(tmp_path)
    stream.run("2026-11-02T06:00:00Z", 100, PROJECTS, bootstrap=True)
    listed = {"alpha": 11, "beta": 21, "gamma": 31}

    # Each request takes a second, and the root takes the first. Two projects start
    # within the budget, and the third does not.
    report = stream.run(
        "2026-11-02T12:00:00Z",
        110,
        listed,
        pages=[page("alpha", 11), page("beta", 21)],
        max_seconds=2.5,
    )

    assert (report.changed, report.remaining) == (2, 1)
    manifest = stream.manifest()
    assert (manifest.complete, manifest.coverage["remaining"]) == (False, 1)

    # gamma keeps its old serial, so the next run reads it.
    report = stream.run("2026-11-02T18:00:00Z", 110, listed, pages=[page("gamma", 31)])

    assert (report.changed, report.remaining) == (1, 0)
    manifest = stream.manifest()
    assert (manifest.complete, manifest.coverage["remaining"]) == (True, 0)


def test_main_runs_once_with_one_github_client(tmp_path, monkeypatch, capsys):
    made: list[tuple[str, str]] = []
    calls: list[dict] = []

    class Client(GitHub):
        def __init__(self, repo: str, token: str, **options) -> None:
            super().__init__(repo, token, **options)
            made.append((repo, token))

    report = RunReport(1, 2, 3, 4, 5, 6, 7, 0, [])

    def run_once_here(gh, http, workdir, **options) -> RunReport:
        assert isinstance(gh, Client)
        assert isinstance(http, Http)
        # A work directory of its own, empty, under ./work.
        assert workdir.parent == tmp_path / "work"
        assert list(workdir.iterdir()) == []
        calls.append({"gh": gh, "workdir": workdir, **options})
        return report

    monkeypatch.setattr("pypi_registry.run.GitHub", Client)
    monkeypatch.setattr("pypi_registry.run.run_once", run_once_here)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)

    assert main(["run"]) == 0

    assert made == [("py-observatory/data", "t0ken")]
    (call,) = calls
    assert (call["bootstrap"], call["override_reason"], call["max_seconds"]) == (
        False,
        None,
        18_000,
    )
    assert call["now"].utcoffset() == timedelta(0)
    assert not call["workdir"].exists(), "the work directory is removed afterwards"
    assert json.loads(capsys.readouterr().out) == dataclasses.asdict(report)

    monkeypatch.setenv("GITHUB_REPOSITORY", "someone/data")
    argv = ["run", "--bootstrap", "--override-reason", "checked", "--max-seconds", "60"]

    assert main(argv) == 0

    assert made[-1] == ("someone/data", "t0ken")
    assert (calls[-1]["bootstrap"], calls[-1]["override_reason"]) == (True, "checked")
    assert calls[-1]["max_seconds"] == 60
    assert calls[-1]["gh"] is not calls[0]["gh"], "one client for each process"

    # The workflow passes an empty reason when nobody gave one.
    assert main(["run", "--override-reason", ""]) == 0
    assert calls[-1]["override_reason"] is None

    monkeypatch.delenv("GITHUB_TOKEN")
    with pytest.raises(KeyError, match="GITHUB_TOKEN"):
        main(["run"])
