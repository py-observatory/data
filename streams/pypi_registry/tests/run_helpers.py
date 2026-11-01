"""What the tests of processing and of the run share: a clock and a transport that
let a test say how long requests take, PyPI's replies as a test builds them, readers
of the rows that a run writes, and the runs of a stream on a fake GitHub and a fake
PyPI, with the names of the releases they publish."""

import json
import sqlite3
from collections.abc import Sequence
from contextlib import closing
from datetime import date, datetime
from pathlib import Path

from obskit.github import GitHub
from obskit.http import Http
from obskit.manifest import Asset, Manifest, dumps, loads
from obskit.release import fetch_assets
from obskit.testing import FakeGitHub, FakeHttp
from obskit.zst import decompress_file
from opened_connections import OPENED
from pypi_registry.layout import OPEN_DELTA_DB, STATE_DB, base_state_name, delta_name
from pypi_registry.run import RunReport, replay, run_once, sweep
from pypi_registry.simple import ROOT_URL, project_url, version_url
from pypi_registry.store import STATE_SCHEMA, diff_states, open_db

FIXTURES = Path(__file__).parent / "fixtures"
# When the state was built, and an upload on either side of that moment.
BOOTSTRAP_AT = "2026-10-01T00:00:00Z"
BEFORE = "2026-09-15T12:00:00.000000Z"
AFTER = "2026-10-20T12:00:00.000000Z"


class Clock:
    """A clock that moves only when it is moved, so no test waits for real."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class Ticking:
    """The pages of `fake`, each request taking one second on `clock`. A run spends its
    time budget on requests, so a test can say how many fit in it."""

    def __init__(self, fake: FakeHttp, clock: Clock) -> None:
        self.fake = fake
        self.clock = clock

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        self.clock.now += 1.0
        return self.fake.request(method, url, headers, body)


def dist(
    filename: str, uploaded: str | None = AFTER, *, yanked: bool | str = False
) -> dict:
    """A file of a project page, as PEP 691 JSON writes it. A yank may be a reason."""
    return {
        "filename": filename,
        "url": f"https://files.pythonhosted.org/packages/ab/cd/{filename}",
        "hashes": {"sha256": "0" * 64},
        "requires-python": ">=3.9",
        "size": 1000,
        "upload-time": uploaded,
        "yanked": yanked,
        "core-metadata": False,
        "provenance": None,
    }


def page(
    name: str,
    serial: int,
    *files: dict,
    versions: Sequence[str] = (),
    status: str | None = None,
) -> dict:
    """A project page as PyPI serves it. It carries `meta.api-version` and
    `meta._last-serial`, which the parser requires. With no status, it has no
    `project-status`, which means active."""
    body = {
        "meta": {"api-version": "1.4", "_last-serial": serial},
        "name": name,
        "files": list(files),
        "versions": list(versions),
    }
    if status is not None:
        body["project-status"] = {"status": status}
    return body


def release(name: str, version: str) -> dict:
    """The JSON API's page of one release: the sample reply, as another project and
    version. It carries `info.name` and `info.version`, which a record requires."""
    path = FIXTURES / "version-sampleproject-4.0.0.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["info"] = {**document["info"], "name": name, "version": version}
    return document


# For each table of a delta, the values of a row that a test looks at.
_ROWS = {
    "event": "SELECT seq, name, incarnation, kind, version, filename, detail "
    "FROM events",
    "file": "SELECT seq, name, incarnation, filename, first_seen_exact FROM files",
    "version": "SELECT seq, name, incarnation, version, record FROM versions",
    "record": "SELECT seq, name, incarnation, version FROM release_records",
    "serial": "SELECT seq, name, serial FROM serials",
    "swept": "SELECT seq, name, incarnation, status FROM swept",
    "cursor": "SELECT seq, serial FROM cursors",
}


def written(conn: sqlite3.Connection, after_seq: int) -> list[tuple]:
    """The rows of the delta above `after_seq` in `seq` order, each as its kind and the
    values that a test looks at."""
    rows = []
    for kind, query in _ROWS.items():
        found = conn.execute(f"{query} WHERE seq > ?", (after_seq,))
        rows += [(seq, kind, *values) for seq, *values in found]
    return [row[1:] for row in sorted(rows)]


def project_rows(state: sqlite3.Connection) -> dict[str, tuple]:
    """Each project of the state, with its incarnation, serial, status and swept."""
    query = "SELECT name, incarnation, last_serial, status, swept FROM projects"
    return {name: tuple(rest) for name, *rest in state.execute(query)}


def files_of(state: sqlite3.Connection) -> list[tuple]:
    return state.execute("SELECT * FROM files_current ORDER BY 1, 2, 3").fetchall()


def versions_of(state: sqlite3.Connection) -> list[tuple]:
    return state.execute("SELECT * FROM versions_seen ORDER BY 1, 2, 3").fetchall()


# ---- the runs of a stream, on a fake GitHub and a fake PyPI

# The releases of the stream, and the assets that they hold, by name.
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

        The root is served once, and so is each of `pages` and `releases` (see
        `serve`).
        """
        self.pypi.add(ROOT_URL, body=root(serial, projects), times=1)
        self.serve(pages, releases)
        return run_once(
            self.gh,
            self.http,
            self.workdir(),
            now=datetime.fromisoformat(at),
            bootstrap=bootstrap,
            override_reason=override_reason,
            max_seconds=max_seconds,
            clock=self.clock,
        )

    def sweep(
        self,
        at: str,
        *,
        pages: Sequence[dict] = (),
        releases: Sequence[tuple[str, str]] = (),
        max_seconds: float = 16_200,
    ) -> RunReport:
        """A sweep at `at`, a UTC time. It reads no root, so none is served, and each of
        `pages` and `releases` is served once (see `serve`)."""
        self.serve(pages, releases)
        return sweep(
            self.gh,
            self.http,
            self.workdir(),
            now=datetime.fromisoformat(at),
            max_seconds=max_seconds,
            clock=self.clock,
        )

    def serve(
        self, pages: Sequence[dict], releases: Sequence[tuple[str, str]] = ()
    ) -> None:
        """Serve each of `pages` once, at its project's URL, and the release record of
        each (name, version) of `releases` once. Any other page is a 404, as a project
        that PyPI does not have is."""
        for body in pages:
            self.pypi.add(project_url(body["name"]), body=body, times=1)
        for name, version in releases:
            self.pypi.add(
                version_url(name, version), body=release(name, version), times=1
            )

    def workdir(self) -> Path:
        """A new, empty work directory, as each run has a new runner."""
        self.runs += 1
        path = self.tmp_path / f"run-{self.runs}"
        path.mkdir()
        return path

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


def meta_of(state: sqlite3.Connection) -> dict[str, str]:
    return dict(state.execute("SELECT key, value FROM meta").fetchall())


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
