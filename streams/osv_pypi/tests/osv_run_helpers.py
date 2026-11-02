"""What the tests of processing and of the run share: a clock and a transport that let
a test say how long requests take, OSV's records, index and dump as a test builds them,
readers of the rows that a run writes, the connections that the tests open and the
fixture that closes them, and the runs of the stream on a fake GitHub and a fake OSV,
with the names of the releases that they publish."""

import io
import json
import sqlite3
import zipfile
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest

from obskit.github import GitHub
from obskit.http import Http
from obskit.manifest import Asset, Manifest, loads
from obskit.testing import FakeGitHub, FakeHttp
from obskit.zst import decompress_file
from osv_pypi.index import DUMP_URL, INDEX_URL, instant_key, record_url
from osv_pypi.run import RunReport, run_once

FIXTURES = Path(__file__).parent / "fixtures"

# The connections that the tests open. SQLite warns about a connection that is collected
# while it is open, so `close_connections` closes what a test opened when it ends. It is
# autouse, so a test module that imports it has it run around each of its tests.
OPENED: list[sqlite3.Connection] = []


@pytest.fixture(autouse=True)
def close_connections() -> Iterator[None]:
    yield
    while OPENED:
        OPENED.pop().close()


class Clock:
    """A clock that moves only when it is moved, so no test waits for real."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class Ticking:
    """The replies of `fake`, each request taking one second on `clock`. A run spends
    its time budget on requests, so a test can say how many fit in it."""

    def __init__(self, fake: FakeHttp, clock: Clock) -> None:
        self.fake = fake
        self.clock = clock

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        self.clock.now += 1.0
        return self.fake.request(method, url, headers, body)


def record(advisory_id: str, modified: str, **edits: object) -> bytes:
    """A record of `advisory_id` as OSV serves it, modified at `modified`: the GHSA
    record in fixtures/ with that id and that time, and the keys of `edits` set."""
    document = json.loads((FIXTURES / "ghsa.json").read_bytes())
    document.update(edits, id=advisory_id, modified=modified)
    return json.dumps(document).encode()


def advisories(state: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    """Each advisory of the state, with its status and when its version was
    modified."""
    rows = state.execute("SELECT id, status, modified FROM advisories")
    return {advisory_id: (status, modified) for advisory_id, status, modified in rows}


def written(conn: sqlite3.Connection, after_seq: int) -> list[tuple]:
    """Every row of the delta above `after_seq`, in seq order: `("version", id,
    modified)` for a version, `(kind, id)` for a removal or a return, and `("cursor",
    generation)` for a cursor."""
    rows = conn.execute(
        "SELECT seq, 'version', id, modified FROM versions WHERE seq > :after "
        "UNION ALL SELECT seq, kind, id, NULL FROM events WHERE seq > :after "
        "UNION ALL SELECT seq, 'cursor', generation, NULL FROM cursors "
        "WHERE seq > :after ORDER BY seq",
        {"after": after_seq},
    )
    return [
        (kind, what) if modified is None else (kind, what, modified)
        for _, kind, what, modified in rows
    ]


def meta_of(state: sqlite3.Connection) -> dict[str, str]:
    """The meta table of the state, each key with its value."""
    return dict(state.execute("SELECT key, value FROM meta").fetchall())


# ---- the runs of the stream, on a fake GitHub and a fake OSV

# The releases of the stream, and the assets that they hold, by name.
REPO = "py-observatory/data"
CURRENT = "osv-pypi-current"
NOVEMBER = "osv-pypi-month-2026-11"
DECEMBER = "osv-pypi-month-2026-12"
MANIFEST = "manifest.json"
STATE = "osv-pypi-state.db.zst"
DELTA = "osv-pypi-delta-open.db.zst"
BASE_NOVEMBER = "osv-pypi-state-base-2026-11.db.zst"
BASE_DECEMBER = "osv-pypi-state-base-2026-12.db.zst"
PART_NOVEMBER = "osv-pypi-2026-11.db.zst"
MANIFEST_NOVEMBER = "manifest-2026-11.json"


def sealed_name(day: str) -> str:
    """The asset of the sealed delta of `day`, given as YYYY-MM-DD."""
    return f"osv-pypi-delta-{day}.db.zst"


def no_sleep(seconds: float) -> None:
    """GitHub's retries and pacing wait for nothing in a test."""


def index_body(listed: dict[str, str]) -> bytes:
    """OSV's index as OSV serves it, listing each advisory of `listed` at its time: a
    line `<time>,<id>` for each, newest first."""
    lines = sorted(
        listed.items(), key=lambda item: (instant_key(item[1]), item[0]), reverse=True
    )
    return "".join(f"{time},{advisory_id}\n" for advisory_id, time in lines).encode()


def dump_body(records: dict[str, bytes]) -> bytes:
    """OSV's full dump as OSV serves it: a zip that holds each record of `records` as
    `<id>.json`, in order, deflated as OSV's is."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for advisory_id, body in records.items():
            archive.writestr(f"{advisory_id}.json", body)
    return buffer.getvalue()


class Stream:
    """The runs of the stream, on one fake GitHub and one fake OSV.

    Each run has a work directory of its own, as each run has a new runner, so all that
    a run leaves to the next goes through the releases. The GitHub client is one for
    the whole test, as `main` makes one for a process.
    """

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.github = FakeGitHub(REPO)
        self.gh = GitHub(REPO, "t0ken", transport=self.github, sleep=no_sleep)
        self.osv = FakeHttp()
        self.clock = Clock()
        self.http = Http(
            transport=Ticking(self.osv, self.clock),
            sleep=self.clock.sleep,
            clock=self.clock,
        )
        self.runs = 0
        self.made = 0  # files made by the helpers below, each in a folder of its own

    def serve(
        self,
        generation: int,
        listed: dict[str, str],
        *,
        records: dict[str, bytes] | None = None,
        dump: dict[str, bytes] | None = None,
    ) -> None:
        """Serve, once each, an index of generation `generation` that lists each
        advisory of `listed` at its time, the dump that holds the records of `dump`,
        when it is given, and each record of `records` at its advisory's address. Any
        other address is a 404, as a record that OSV does not have is."""
        headers = {"x-goog-generation": str(generation)}
        self.osv.add(INDEX_URL, body=index_body(listed), headers=headers, times=1)
        if dump is not None:
            self.osv.add(DUMP_URL, body=dump_body(dump), times=1)
        for advisory_id, body in (records or {}).items():
            self.osv.add(record_url(advisory_id), body=body, times=1)

    def run(
        self,
        at: str,
        generation: int,
        listed: dict[str, str],
        *,
        records: dict[str, bytes] | None = None,
        dump: dict[str, bytes] | None = None,
        bootstrap: bool = False,
        override_reason: str | None = None,
        max_seconds: float = 1200,
    ) -> RunReport:
        """A run at `at`, a UTC time, that reads what `serve` serves: an index of
        generation `generation` that lists `listed`, the dump `dump` and the records of
        `records`."""
        self.serve(generation, listed, records=records, dump=dump)
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

    def bootstrap(self, at: str, generation: int, listed: dict[str, str]) -> RunReport:
        """The stream's first run, at `at`, whose index of generation `generation`
        lists `listed`, and whose dump holds the record of each of those advisories at
        its time, so that it fetches no record."""
        dump = {
            advisory_id: record(advisory_id, time)
            for advisory_id, time in listed.items()
        }
        return self.run(at, generation, listed, dump=dump, bootstrap=True)

    def requested(self) -> list[str]:
        """The URLs that the runs requested of OSV, in order."""
        return [url for url, _ in self.osv.requests]

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
