"""What the tests of processing and of the run share: a clock and a transport that
let a test say how long requests take, PyPI's replies as a test builds them, and
readers of the rows that a run writes."""

import json
import sqlite3
from collections.abc import Sequence
from pathlib import Path

from obskit.testing import FakeHttp

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


# The connections that the tests open. SQLite warns about a connection that is
# collected while it is open, so conftest.py closes them when each test ends.
OPENED: list[sqlite3.Connection] = []


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
