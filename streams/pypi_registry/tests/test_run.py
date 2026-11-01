import dataclasses
import hashlib
import json
import math
import platform
import sqlite3
import struct
from collections.abc import Iterator, Sequence
from contextlib import closing
from dataclasses import fields
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

import obskit
from obskit.gates import GateFailure
from obskit.github import GitHub
from obskit.http import Http, UpstreamError
from obskit.manifest import Asset, Manifest, TableInfo, Watermark, dumps, loads
from obskit.release import CreateOnceError, PriorStateError, fetch_assets
from obskit.sqlite import table_counts
from obskit.testing import FakeGitHub, FakeHttp
from obskit.zst import compress_file, decompress_file, sha256_file
from pypi_registry.layout import OPEN_DELTA_DB, STATE_DB, base_state_name, delta_name
from pypi_registry.run import (
    GateError,
    ProcessReport,
    RunReport,
    StaleRootError,
    main,
    process_projects,
    replay,
    run_once,
)
from pypi_registry.simple import ACCEPT, ROOT_URL, Root, project_url, version_url
from pypi_registry.store import (
    DELTA_SCHEMA,
    STATE_SCHEMA,
    Delta,
    apply_delta,
    diff_states,
    open_db,
    state_rows,
)

FIXTURES = Path(__file__).parent / "fixtures"
# When the state was built, and an upload on either side of that moment.
BOOTSTRAP_AT = "2026-10-01T00:00:00Z"
BEFORE = "2026-09-15T12:00:00.000000Z"
AFTER = "2026-10-20T12:00:00.000000Z"
# A time long before any run.
OLD = "2026-01-01T00:00:00Z"
# The time of the runs, which the delta gives each row that has a time.
OBSERVED_AT = "2026-11-02T06:00:00Z"
CURSOR = 1000


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
# collected while it is open, so each test closes what it opened when it ends.
OPENED: list[sqlite3.Connection] = []


@pytest.fixture(autouse=True)
def close_connections() -> Iterator[None]:
    yield
    while OPENED:
        OPENED.pop().close()


def project(
    name: str,
    serial: int,
    *,
    incarnation: int = 1,
    status: str = "active",
    swept: int = 1,
) -> tuple:
    """A row of `projects`, as the bootstrap writes it and the runs since then left it."""
    return (name, incarnation, serial, status, swept, BOOTSTRAP_AT, 0)


def make_state(
    tmp_path: Path,
    projects: Sequence[tuple],
    *,
    files: Sequence[tuple] = (),
    versions: Sequence[tuple] = (),
) -> sqlite3.Connection:
    """A state as the bootstrap built it, holding `projects`, and the files
    (`files_current`) and versions (`versions_seen`) that runs since then added."""
    state = open_db(tmp_path / "state.db", STATE_SCHEMA, create=True)
    OPENED.append(state)
    meta = [
        ("applied_seq", "0"),
        ("bootstrap_at", BOOTSTRAP_AT),
        ("cursor", str(CURSOR)),
    ]
    state.executemany("INSERT INTO meta VALUES (?, ?)", meta)
    state.executemany("INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?, ?)", projects)
    state.executemany("INSERT INTO files_current VALUES (?, ?, ?, ?, ?)", files)
    state.executemany("INSERT INTO versions_seen VALUES (?, ?, ?, ?)", versions)
    state.commit()
    return state


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


def event(
    name: str, incarnation: int, kind: str, *, version=None, filename=None, detail=None
) -> tuple:
    """An event as `written` gives it."""
    return ("event", name, incarnation, kind, version, filename, detail)


def counts(**given: int) -> ProcessReport:
    """A report with the counts given, and 0 for the others."""
    return ProcessReport(**{field.name: 0 for field in fields(ProcessReport)} | given)


def project_rows(state: sqlite3.Connection) -> dict[str, tuple]:
    """Each project of the state, with its incarnation, serial, status and swept."""
    query = "SELECT name, incarnation, last_serial, status, swept FROM projects"
    return {name: tuple(rest) for name, *rest in state.execute(query)}


def files_of(state: sqlite3.Connection) -> list[tuple]:
    return state.execute("SELECT * FROM files_current ORDER BY 1, 2, 3").fetchall()


def versions_of(state: sqlite3.Connection) -> list[tuple]:
    return state.execute("SELECT * FROM versions_seen ORDER BY 1, 2, 3").fetchall()


class Day:
    """The runs of one day. Each processes a root into the day's delta, as step 5 of the
    run does, and then applies the rows it wrote to the state, as step 6 does."""

    def __init__(self, tmp_path: Path, state: sqlite3.Connection, fake: FakeHttp):
        self.state = state
        self.fake = fake
        self.clock = Clock()
        self.http = Http(
            transport=Ticking(fake, self.clock),
            sleep=self.clock.sleep,
            clock=self.clock,
        )
        self.conn = open_db(tmp_path / "delta.db", DELTA_SCHEMA, create=True)
        OPENED.append(self.conn)

    def run(
        self, root: Root, *, budget: float = math.inf
    ) -> tuple[ProcessReport, list[tuple]]:
        """Process `root` within `budget` seconds and apply the delta. Returns the
        report and the rows that this run wrote."""
        (applied,) = self.state.execute(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'applied_seq'"
        ).fetchone()
        delta = Delta(self.conn, OBSERVED_AT, first_seq=applied + 1)
        before = state_rows(self.state)
        report = process_projects(
            self.state,
            delta,
            self.http,
            root,
            deadline=self.clock() + budget,
            clock=self.clock,
        )
        # Processing reads the state and never writes it. The delta is applied after.
        assert state_rows(self.state) == before
        assert not self.state.in_transaction
        self.conn.commit()
        rows = written(self.conn, applied)
        top = apply_delta(self.state, self.conn, after_seq=applied)
        self.state.execute(
            "UPDATE meta SET value = ? WHERE key = 'applied_seq'", (str(top),)
        )
        self.state.commit()
        return report, rows

    def requested(self) -> list[str]:
        """The URLs requested so far, in order."""
        return [url for url, _ in self.fake.requests]


# ---- names that left the root


def test_vanished_project_removed_and_quarantined(tmp_path):
    state = make_state(
        tmp_path,
        [
            *(project(name, 10) for name in ("ended", "gone", "held", "here", "kept")),
            project("frozen", 10, status="quarantined"),
        ],
        files=[
            ("ended", 1, "ended-1.0.tar.gz", 0, None),
            ("gone", 1, "gone-1.0.tar.gz", 0, None),
            ("gone", 1, "gone-1.1.tar.gz", 1, "broken"),
            ("held", 1, "held-1.0.tar.gz", 0, None),
        ],
    )
    fake = FakeHttp()
    # `ended` is gone for good (410) and `gone` has no page (404). `held` is
    # quarantined, and `here` is still there, though the root does not list it.
    # `frozen` was quarantined before, and the root does not list it either.
    fake.add(project_url("ended"), status=410)
    fake.add(project_url("frozen"), body=page("frozen", 12, status="quarantined"))
    fake.add(project_url("held"), body=page("held", 15, status="quarantined"))
    fake.add(project_url("here"), body=page("here", 10))
    day = Day(tmp_path, state, fake)

    report, rows = day.run(Root(CURSOR + 10, {"kept": 10}))

    # The names are probed in order, the quarantined one among the vanished ones. A
    # removal and a quarantine are events about the incarnation that the state holds,
    # and a probe writes no serial.
    assert rows == [
        event("ended", 1, "removed"),
        event("gone", 1, "removed"),
        event("held", 1, "quarantined"),
    ]
    probed = ("ended", "frozen", "gone", "held", "here")
    assert day.requested() == [project_url(name) for name in probed]
    assert all(headers["Accept"] == ACCEPT for _, headers in fake.requests)
    # The three files that the removals delete are the shrink that the gates allow.
    assert report == counts(vanished=5, files_removed=3)
    # Applied, a removal deletes the files and a quarantine keeps them. The project
    # that is still there is left as it was.
    assert project_rows(state) == {
        "ended": (1, 10, "removed", 1),
        "frozen": (1, 10, "quarantined", 1),
        "gone": (1, 10, "removed", 1),
        "held": (1, 10, "quarantined", 1),
        "here": (1, 10, "active", 1),
        "kept": (1, 10, "active", 1),
    }
    assert files_of(state) == [("held", 1, "held-1.0.tar.gz", 0, None)]


def test_quarantine_exit_keeps_incarnation_and_removal_return_increments(tmp_path):
    state = make_state(
        tmp_path,
        [project("q", 20, status="quarantined"), project("r", 30, status="removed")],
        files=[("q", 1, "q-1.0.tar.gz", 0, None)],
        versions=[("q", 1, "1.0", 1), ("r", 1, "0.1", 1)],
    )
    fake = FakeHttp()
    q_page = page(
        "q",
        25,
        dist("q-1.0.tar.gz", BEFORE),
        dist("q-1.1.tar.gz"),
        versions=["1.0", "1.1"],
    )
    fake.add(project_url("q"), body=q_page)
    fake.add(version_url("q", "1.1"), body=release("q", "1.1"))
    fake.add(
        project_url("r"), body=page("r", 35, dist("r-2.0.tar.gz"), versions=["2.0"])
    )
    fake.add(version_url("r", "2.0"), body=release("r", "2.0"))
    day = Day(tmp_path, state, fake)

    report, rows = day.run(Root(CURSOR + 10, {"q": 25, "r": 35}))

    assert rows == [
        # A quarantine that ends is the same project. It keeps its incarnation and its
        # files, and its page is processed as a changed project's.
        event("q", 1, "unquarantined"),
        ("file", "q", 1, "q-1.1.tar.gz", 1),
        ("version", "q", 1, "1.1", 1),
        ("record", "q", 1, "1.1"),
        ("serial", "q", 25),
        # A removed name that is listed again was registered again: a new incarnation,
        # read in full as a new project is, without `created`, which the state refuses
        # for a name that it holds.
        event("r", 2, "reregistered"),
        ("file", "r", 2, "r-2.0.tar.gz", 1),
        ("version", "r", 2, "2.0", 1),
        ("record", "r", 2, "2.0"),
        ("swept", "r", 2, "active"),
        ("serial", "r", 35),
    ]
    assert report == counts(returned=2, versions_fetched=2, swept=1)
    assert project_rows(state) == {
        "q": (1, 25, "active", 1),
        "r": (2, 35, "active", 1),
    }
    assert files_of(state) == [
        ("q", 1, "q-1.0.tar.gz", 0, None),
        ("q", 1, "q-1.1.tar.gz", 0, None),
        ("r", 2, "r-2.0.tar.gz", 0, None),
    ]
    # The versions of the removed incarnation stay with it.
    assert versions_of(state) == [
        ("q", 1, "1.0", 1),
        ("q", 1, "1.1", 1),
        ("r", 1, "0.1", 1),
        ("r", 2, "2.0", 1),
    ]


def test_quarantined_then_deleted_is_removed_and_reregistration_increments(tmp_path):
    # Three quarantined projects that the root does not list. `held` is then deleted,
    # `still` stays quarantined, and `back` is released but not listed again yet.
    state = make_state(
        tmp_path,
        [project(name, 50, status="quarantined") for name in ("back", "held", "still")],
        files=[
            ("held", 1, "held-1.0.tar.gz", 0, None),
            ("held", 1, "held-1.1.tar.gz", 0, None),
            ("still", 1, "still-1.0.tar.gz", 0, None),
        ],
    )
    fake = FakeHttp()
    back = page("back", 55, dist("back-1.0.tar.gz", BEFORE), versions=["1.0"])
    fake.add(project_url("back"), body=back)
    fake.add(project_url("held"), status=404, times=1)
    fake.add(project_url("still"), body=page("still", 56, status="quarantined"))
    day = Day(tmp_path, state, fake)

    report, rows = day.run(Root(CURSOR + 10, {}))

    # Only the deletion is written. Its files go when it is applied, and they are the
    # shrink that the gates allow.
    assert rows == [event("held", 1, "removed")]
    assert day.requested() == [project_url(name) for name in ("back", "held", "still")]
    assert report == counts(vanished=3, files_removed=2)
    assert project_rows(state)["held"] == (1, 50, "removed", 1)
    assert files_of(state) == [("still", 1, "still-1.0.tar.gz", 0, None)]

    # Someone registers the name again. It is a new project, and not the quarantined
    # one coming back with its incarnation.
    fake.add(
        project_url("held"),
        body=page("held", 60, dist("held-0.1.tar.gz"), versions=["0.1"]),
    )
    fake.add(version_url("held", "0.1"), body=release("held", "0.1"))

    report, rows = day.run(Root(CURSOR + 20, {"held": 60}))

    assert rows == [
        event("held", 2, "reregistered"),
        ("file", "held", 2, "held-0.1.tar.gz", 1),
        ("version", "held", 2, "0.1", 1),
        ("record", "held", 2, "0.1"),
        ("swept", "held", 2, "active"),
        ("serial", "held", 60),
    ]
    # `back` and `still` are probed again, and still write nothing.
    assert report == counts(vanished=2, returned=1, versions_fetched=1, swept=1)
    assert project_rows(state)["held"] == (2, 60, "active", 1)


# ---- the time budget


def test_budget_exhaustion_leaves_projects_pending(tmp_path):
    changed = ("alpha", "beta", "gamma")
    state = make_state(
        tmp_path,
        [
            project("back", 30, status="removed"),
            project("old", 5),
            *(project(name, 40) for name in changed),
        ],
    )
    fake = FakeHttp()
    fake.add(project_url("back"), body=page("back", 60))
    fake.add(project_url("fresh"), body=page("fresh", 50))
    for name in changed:
        fake.add(project_url(name), body=page(name, 41))
    day = Day(tmp_path, state, fake)
    root = Root(CURSOR + 10, {"back": 60, "fresh": 50, **dict.fromkeys(changed, 41)})

    # Each request takes a second, and the budget is checked before each project, so
    # the fourth project starts at 3 s and is the last one that starts.
    report, rows = day.run(root, budget=3.5)

    # The vanished name comes first, then the returned one, then the new one, and then
    # the changed ones by name.
    reached = ("old", "back", "fresh", "alpha")
    assert day.requested() == [project_url(name) for name in reached]
    assert rows == [
        event("old", 1, "removed"),
        event("back", 2, "reregistered"),
        ("swept", "back", 2, "active"),
        ("serial", "back", 60),
        event("fresh", 1, "created"),
        ("swept", "fresh", 1, "active"),
        ("serial", "fresh", 50),
        ("serial", "alpha", 41),
    ]
    assert report == counts(
        vanished=1, returned=1, new=1, changed=1, swept=2, remaining=2
    )
    # The projects that were not reached get no serial row, so they keep the old one,
    # and the next run finds them changed again.
    serials = {name: row[1] for name, row in project_rows(state).items()}
    assert serials == {
        "alpha": 41,
        "back": 60,
        "beta": 40,
        "fresh": 50,
        "gamma": 40,
        "old": 5,
    }

    report, rows = day.run(root)

    assert rows == [("serial", "beta", 41), ("serial", "gamma", 41)]
    assert report == counts(changed=2)


# ---- quarantined pages


@pytest.mark.parametrize("swept", [1, 0], ids=["swept", "unswept"])
def test_project_quarantined_between_root_and_page(tmp_path, swept):
    # The root lists the project as changed, but by the time its page is read the
    # project is quarantined. The page lists no files, which says nothing of the files
    # that the state holds.
    files = [
        ("demo", 1, "demo-1.0.tar.gz", 0, None),
        ("demo", 1, "demo-1.1.tar.gz", 1, "broken"),
    ]
    stored = files if swept else []
    state = make_state(tmp_path, [project("demo", 40, swept=swept)], files=stored)
    fake = FakeHttp()
    fake.add(project_url("demo"), body=page("demo", 42, status="quarantined"))
    day = Day(tmp_path, state, fake)

    report, rows = day.run(Root(CURSOR + 10, {"demo": 41}))

    # A quarantine event and the serial: no file_removed and no record fetch, and for
    # a project that is not swept yet, no swept row.
    assert rows == [event("demo", 1, "quarantined"), ("serial", "demo", 42)]
    assert day.requested() == [project_url("demo")]
    assert report == counts(changed=1)
    assert project_rows(state) == {"demo": (1, 42, "quarantined", swept)}
    assert files_of(state) == stored


def test_returned_but_still_quarantined_writes_no_events(tmp_path):
    # PyPI may list a quarantined project in the root, and then the project is
    # returned at every run. Its page says that it is still quarantined.
    state = make_state(
        tmp_path,
        [project("held", 60, status="quarantined")],
        files=[("held", 1, "held-1.0.tar.gz", 0, None)],
    )
    fake = FakeHttp()
    fake.add(project_url("held"), body=page("held", 61, status="quarantined"))
    day = Day(tmp_path, state, fake)

    report, rows = day.run(Root(CURSOR + 10, {"held": 61}))

    assert rows == [("serial", "held", 61)]
    assert report == counts(returned=1)
    assert project_rows(state) == {"held": (1, 61, "quarantined", 1)}
    assert files_of(state) == [("held", 1, "held-1.0.tar.gz", 0, None)]

    # The next run finds the same, and writes no event either.
    report, rows = day.run(Root(CURSOR + 20, {"held": 61}))

    assert rows == [("serial", "held", 61)]
    assert report == counts(returned=1)


@pytest.mark.parametrize(
    ("stored", "incarnation", "kind", "group"),
    [
        ([], 1, "created", "new"),
        ([project("demo", 30, status="removed")], 2, "reregistered", "returned"),
    ],
    ids=["new", "reregistered"],
)
def test_new_project_with_quarantined_page(tmp_path, stored, incarnation, kind, group):
    state = make_state(tmp_path, stored)
    fake = FakeHttp()
    fake.add(project_url("demo"), body=page("demo", 70, status="quarantined"))
    day = Day(tmp_path, state, fake)

    # The page changed again after the root was read.
    report, rows = day.run(Root(CURSOR + 10, {"demo": 69}))

    # The project starts and is quarantined. It is swept with the status of its page,
    # so the sweep does not read it again, and there is nothing to diff or fetch. The
    # serial is the page's: the serial of what was read.
    assert rows == [
        event("demo", incarnation, kind),
        event("demo", incarnation, "quarantined"),
        ("swept", "demo", incarnation, "quarantined"),
        ("serial", "demo", 70),
    ]
    assert day.requested() == [project_url("demo")]
    assert report == counts(**{group: 1}, swept=1)
    assert project_rows(state) == {"demo": (incarnation, 70, "quarantined", 1)}


# ---- changed projects


def test_changed_swept_project_records_every_kind_of_change(tmp_path):
    state = make_state(
        tmp_path,
        [project("demo", 40)],
        files=[
            ("demo", 1, "demo-1.0.tar.gz", 0, None),  # stays as it is
            ("demo", 1, "demo-1.1.tar.gz", 1, "broken"),  # is unyanked
            ("demo", 1, "demo-1.2.tar.gz", 0, None),  # is yanked
            ("demo", 1, "demo-1.3.tar.gz", 0, None),  # is deleted
        ],
        versions=[("demo", 1, version, 1) for version in ("1.0", "1.1", "1.2", "1.3")],
    )
    fake = FakeHttp()
    demo = page(
        "demo",
        45,
        dist("demo-1.0.tar.gz", BEFORE),
        dist("demo-1.1.tar.gz", BEFORE),
        dist("demo-1.2.tar.gz", BEFORE, yanked="do not use"),
        dist("demo-2.0.tar.gz"),
        dist("demo-3.0.tar.gz"),
        versions=["1.0", "1.1", "1.2", "1.3", "2.0", "3.0"],
        status="archived",
    )
    fake.add(project_url("demo"), body=demo)
    # 2.0 has a record, and the JSON API has no page for 3.0 (a 404).
    fake.add(version_url("demo", "2.0"), body=release("demo", "2.0"))
    day = Day(tmp_path, state, fake)

    report, rows = day.run(Root(CURSOR + 10, {"demo": 45}))

    assert rows == [
        event("demo", 1, "status_changed", detail="archived"),
        ("file", "demo", 1, "demo-2.0.tar.gz", 1),
        ("file", "demo", 1, "demo-3.0.tar.gz", 1),
        event("demo", 1, "file_removed", version="1.3", filename="demo-1.3.tar.gz"),
        event("demo", 1, "unyanked", version="1.1", filename="demo-1.1.tar.gz"),
        event(
            "demo",
            1,
            "yanked",
            version="1.2",
            filename="demo-1.2.tar.gz",
            detail="do not use",
        ),
        ("version", "demo", 1, "2.0", 1),
        ("record", "demo", 1, "2.0"),
        ("version", "demo", 1, "3.0", 0),
        ("serial", "demo", 45),
    ]
    # Only the versions that the state had not seen are fetched.
    assert day.requested() == [
        project_url("demo"),
        version_url("demo", "2.0"),
        version_url("demo", "3.0"),
    ]
    assert report == counts(changed=1, versions_fetched=2, files_removed=1)
    # The record is observed at the time of the run.
    records = day.conn.execute("SELECT version_raw, observed_at FROM release_records")
    assert records.fetchall() == [("2.0", OBSERVED_AT)]
    assert project_rows(state) == {"demo": (1, 45, "archived", 1)}
    assert files_of(state) == [
        ("demo", 1, "demo-1.0.tar.gz", 0, None),
        ("demo", 1, "demo-1.1.tar.gz", 0, None),
        ("demo", 1, "demo-1.2.tar.gz", 1, "do not use"),
        ("demo", 1, "demo-2.0.tar.gz", 0, None),
        ("demo", 1, "demo-3.0.tar.gz", 0, None),
    ]
    assert versions_of(state)[-2:] == [("demo", 1, "2.0", 1), ("demo", 1, "3.0", 0)]


def test_unswept_project_is_read_in_full(tmp_path):
    # A project that the bootstrap found, and that is not swept yet, changes, so its
    # page is read in full, as the sweep would read it.
    state = make_state(tmp_path, [project("old", 70, swept=0)])
    fake = FakeHttp()
    old = page(
        "old",
        75,
        dist("old-1.0.tar.gz", BEFORE),
        dist("old-1.0.zip", None),  # a file with no upload time
        dist("old-2.0-py3-none-any.whl", BEFORE),
        # Half a second after the bootstrap. As text, it would sort before it.
        dist("old-2.0.tar.gz", "2026-10-01T00:00:00.500000Z"),
        versions=["1.0", "2.0", "3.0"],
        status="archived",
    )
    fake.add(project_url("old"), body=old)
    fake.add(version_url("old", "2.0"), body=release("old", "2.0"))
    day = Day(tmp_path, state, fake)

    report, rows = day.run(Root(CURSOR + 10, {"old": 75}))

    assert rows == [
        # A file is exact only when it was uploaded after the bootstrap. A file from
        # before was there before the project was first read.
        ("file", "old", 1, "old-1.0.tar.gz", 0),
        ("file", "old", 1, "old-1.0.zip", 0),
        ("file", "old", 1, "old-2.0-py3-none-any.whl", 0),
        ("file", "old", 1, "old-2.0.tar.gz", 1),
        # Only a version with a file uploaded after the bootstrap gets its record. The
        # history fill holds the releases from before, and 3.0 has no file.
        ("version", "old", 1, "1.0", 0),
        ("version", "old", 1, "2.0", 1),
        ("record", "old", 1, "2.0"),
        ("version", "old", 1, "3.0", 0),
        # The swept row carries the status of the page, and no event is written.
        ("swept", "old", 1, "archived"),
        ("serial", "old", 75),
    ]
    assert day.requested() == [project_url("old"), version_url("old", "2.0")]
    assert report == counts(changed=1, versions_fetched=1, swept=1)
    assert project_rows(state) == {"old": (1, 75, "archived", 1)}


# ---- pages that are not used


@pytest.mark.parametrize("status", [404, 410])
def test_page_not_found_skips_the_project(tmp_path, status):
    state = make_state(
        tmp_path,
        [
            project("changed", 40),
            project("held", 50, status="quarantined"),
            project("removed", 60, status="removed"),
        ],
    )
    fake = FakeHttp()
    for name in ("changed", "fresh", "held", "removed"):
        fake.add(project_url(name), status=status)
    day = Day(tmp_path, state, fake)
    before = state_rows(state)

    root = Root(CURSOR + 10, {"changed": 41, "fresh": 1, "held": 51, "removed": 61})
    report, rows = day.run(root)

    # The root lists each of them, so a missing page may be a passing fault. Nothing is
    # written, not even a serial, and the next run tries again. A project that is
    # really gone leaves the root, and is probed then.
    assert rows == []
    assert len(day.requested()) == 4
    assert report == counts(remaining=4)
    assert state_rows(state) == before


# The fresh page of `demo` in the stale-page tests: a file from before the bootstrap,
# and one from after it, which the stale copy of the page lacks.
_FRESH_ROWS = [
    ("file", "demo", 1, "demo-1.0.tar.gz", 0),
    ("file", "demo", 1, "demo-1.1.tar.gz", 1),
    ("version", "demo", 1, "1.0", 0),
    ("version", "demo", 1, "1.1", 1),
    ("record", "demo", 1, "1.1"),
]


def _at(incarnation: int, rows: list[tuple]) -> list[tuple]:
    """`rows` of `demo` at another incarnation: the third value of each."""
    return [(kind, name, incarnation, *rest) for kind, name, _, *rest in rows]


# For each group that fetches a listed page: what the state holds of `demo`, and the
# rows and counts of the run that reads the fresh page.
STALE = {
    "new": (
        [],
        [event("demo", 1, "created"), *_FRESH_ROWS, ("swept", "demo", 1, "active")],
        counts(new=1, versions_fetched=1, swept=1),
    ),
    "returned-removed": (
        [project("demo", 30, status="removed")],
        [
            event("demo", 2, "reregistered"),
            *_at(2, _FRESH_ROWS),
            ("swept", "demo", 2, "active"),
        ],
        counts(returned=1, versions_fetched=1, swept=1),
    ),
    "returned-quarantined": (
        [project("demo", 40, status="quarantined")],
        [event("demo", 1, "unquarantined"), *_FRESH_ROWS[1:2], *_FRESH_ROWS[3:]],
        counts(returned=1, versions_fetched=1),
    ),
    "changed": (
        [project("demo", 40)],
        [*_FRESH_ROWS[1:2], *_FRESH_ROWS[3:]],
        counts(changed=1, versions_fetched=1),
    ),
}


@pytest.mark.parametrize("group", STALE)
def test_stale_page_skipped_and_retried(tmp_path, group):
    stored, rows_after, report_after = STALE[group]
    # A project that the state holds at incarnation 1 has the file from before.
    held = stored and stored[0][3] != "removed"
    state = make_state(
        tmp_path,
        stored,
        files=[("demo", 1, "demo-1.0.tar.gz", 0, None)] if held else [],
        versions=[("demo", 1, "1.0", 1)] if held else [],
    )
    fake = FakeHttp()
    # The root says the project changed at serial 45, but the first copy of its page
    # is older than that, and lacks the file that the change added.
    stale = page("demo", 43, dist("demo-1.0.tar.gz", BEFORE), versions=["1.0"])
    fake.add(project_url("demo"), body=stale, times=1)
    fresh = page(
        "demo",
        45,
        dist("demo-1.0.tar.gz", BEFORE),
        dist("demo-1.1.tar.gz"),
        versions=["1.0", "1.1"],
    )
    fake.add(project_url("demo"), body=fresh)
    fake.add(version_url("demo", "1.1"), body=release("demo", "1.1"))
    day = Day(tmp_path, state, fake)
    before = state_rows(state)
    root = Root(CURSOR + 10, {"demo": 45})

    report, rows = day.run(root)

    # Nothing is written, not even the serial, so the project is still to do.
    assert rows == []
    assert report == counts(remaining=1)
    assert state_rows(state) == before

    report, rows = day.run(root)

    assert rows == [*rows_after, ("serial", "demo", 45)]
    assert report == report_after


def test_stale_probe_writes_nothing_and_probes_again(tmp_path):
    # The last run read the page of `demo` at serial 50. Now the root does not list
    # it, and the probe gets an older copy of its page, which says it is quarantined.
    # A quarantine is recorded for good, so an old copy must not record one.
    state = make_state(
        tmp_path,
        [project("demo", 50)],
        files=[("demo", 1, "demo-1.0.tar.gz", 0, None)],
    )
    fake = FakeHttp()
    old = page("demo", 48, status="quarantined")
    fake.add(project_url("demo"), body=old, times=1)
    fake.add(project_url("demo"), body=page("demo", 52, status="quarantined"))
    day = Day(tmp_path, state, fake)
    before = state_rows(state)
    root = Root(CURSOR + 10, {})

    report, rows = day.run(root)

    assert rows == []
    assert report == counts(remaining=1)
    assert state_rows(state) == before

    # The next run probes it again, and a page as new as the state counts.
    report, rows = day.run(root)

    assert rows == [event("demo", 1, "quarantined")]
    assert report == counts(vanished=1)


def test_page_older_than_the_one_read_before_is_stale(tmp_path):
    # The last run read the page at serial 50, after a file was added at 48. Now the
    # root lists the project at 46: a copy of the root from before that change, though
    # newer than the cursor. A page at 47 is not below the root's serial, but it is
    # older than what the state holds and lacks the file, so a diff of it would record
    # a removal that never happened.
    state = make_state(
        tmp_path,
        [project("demo", 50)],
        files=[
            ("demo", 1, "demo-1.0.tar.gz", 0, None),
            ("demo", 1, "demo-1.1.tar.gz", 0, None),
        ],
        versions=[("demo", 1, "1.0", 1), ("demo", 1, "1.1", 1)],
    )
    fake = FakeHttp()
    older = page("demo", 47, dist("demo-1.0.tar.gz", BEFORE), versions=["1.0"])
    fake.add(project_url("demo"), body=older, times=1)
    current = page(
        "demo",
        50,
        dist("demo-1.0.tar.gz", BEFORE),
        dist("demo-1.1.tar.gz"),
        versions=["1.0", "1.1"],
    )
    fake.add(project_url("demo"), body=current)
    day = Day(tmp_path, state, fake)
    root = Root(CURSOR + 10, {"demo": 46})

    report, rows = day.run(root)

    assert rows == []
    assert report == counts(remaining=1)

    report, rows = day.run(root)

    assert rows == [("serial", "demo", 50)]
    assert report == counts(changed=1)


# ---- keys that a delta holds once


def test_second_run_of_the_day_repeats_no_key(tmp_path):
    # The first run of the day reads a new project and an unswept one in full, and the
    # second finds both changed again. What the first run wrote is in the state by
    # then, and is not written again: a delta holds each file, version and record once,
    # and a month's deltas are merged by key.
    state = make_state(tmp_path, [project("old", 70, swept=0)])
    fake = FakeHttp()
    old = page(
        "old",
        75,
        dist("old-1.0.tar.gz", BEFORE),
        dist("old-2.0.tar.gz"),
        versions=["1.0", "2.0"],
    )
    pages = {
        "new": [
            page("new", 80, dist("new-1.0.tar.gz"), versions=["1.0"]),
            page(
                "new",
                85,
                dist("new-1.0.tar.gz"),
                dist("new-1.1.tar.gz"),
                versions=["1.0", "1.1"],
            ),
        ],
        "old": [old, {**old, "meta": {"api-version": "1.4", "_last-serial": 77}}],
    }
    for name, (first, second) in pages.items():
        fake.add(project_url(name), body=first, times=1)
        fake.add(project_url(name), body=second)
    for name, version in [("new", "1.0"), ("new", "1.1"), ("old", "2.0")]:
        fake.add(version_url(name, version), body=release(name, version))
    day = Day(tmp_path, state, fake)
    day.run(Root(CURSOR + 10, {"new": 80, "old": 75}))

    report, rows = day.run(Root(CURSOR + 20, {"new": 85, "old": 77}))

    assert rows == [
        ("file", "new", 1, "new-1.1.tar.gz", 1),
        ("version", "new", 1, "1.1", 1),
        ("record", "new", 1, "1.1"),
        ("serial", "new", 85),
        ("serial", "old", 77),
    ]
    assert report == counts(changed=2, versions_fetched=1)


def test_page_read_in_full_writes_only_what_the_state_lacks(tmp_path):
    # A project that is not swept holds no files: only a full read adds them, and it
    # ends with a swept row. Should one hold some all the same, a full read writes only
    # the keys that the state lacks, so apply_delta and the month's merge never meet a
    # key twice.
    state = make_state(
        tmp_path,
        [project("demo", 40, swept=0)],
        files=[("demo", 1, "demo-1.0.tar.gz", 0, None)],
        versions=[("demo", 1, "1.0", 1)],
    )
    fake = FakeHttp()
    demo = page(
        "demo",
        45,
        dist("demo-1.0.tar.gz"),
        dist("demo-2.0.tar.gz"),
        versions=["1.0", "2.0"],
    )
    fake.add(project_url("demo"), body=demo)
    fake.add(version_url("demo", "2.0"), body=release("demo", "2.0"))
    day = Day(tmp_path, state, fake)

    report, rows = day.run(Root(CURSOR + 10, {"demo": 45}))

    assert rows == [
        ("file", "demo", 1, "demo-2.0.tar.gz", 1),
        ("version", "demo", 1, "2.0", 1),
        ("record", "demo", 1, "2.0"),
        ("swept", "demo", 1, "active"),
        ("serial", "demo", 45),
    ]
    assert report == counts(changed=1, versions_fetched=1, swept=1)


# ---- replies that end the run


def _without_version(document: dict) -> dict:
    info = {key: value for key, value in document["info"].items() if key != "version"}
    return {**document, "info": info}


@pytest.mark.parametrize(
    ("url", "body", "message"),
    [
        (project_url("demo"), b"<html>maintenance</html>", "not JSON"),
        (project_url("demo"), page("other", 45), "'other'"),
        (version_url("demo", "2.0"), b"[]", "must be a JSON object"),
        (
            version_url("demo", "2.0"),
            _without_version(release("demo", "2.0")),
            "version",
        ),
        (version_url("demo", "2.0"), release("demo", "2.1"), "2.1"),
        (version_url("demo", "2.0"), release("other", "2.0"), "'other'"),
    ],
    ids=[
        "page-not-json",
        "page-of-another-project",
        "release-not-an-object",
        "release-without-a-version",
        "release-of-another-version",
        "release-of-another-project",
    ],
)
def test_reply_of_the_wrong_shape_fails_the_run(tmp_path, url, body, message):
    # Malformed data from upstream is not caught. The run fails and publishes nothing,
    # because a guess would be stored as a fact. Here, in turn: a page that is not
    # JSON, the page of another project, a release reply that is not an object, one
    # with no version, one for another version and one for another project.
    state = make_state(tmp_path, [project("demo", 40)])
    fake = FakeHttp()
    fake.add(url, body=body)
    fake.add(
        project_url("demo"),
        body=page("demo", 45, dist("demo-2.0.tar.gz"), versions=["2.0"]),
    )
    fake.add(version_url("demo", "2.0"), body=release("demo", "2.0"))
    day = Day(tmp_path, state, fake)

    with pytest.raises(ValueError, match=message):
        day.run(Root(CURSOR + 10, {"demo": 45}))


def test_release_may_spell_the_project_another_way(tmp_path):
    # The JSON API gives the project's name as it was registered. Only its canonical
    # form has to be the project that was asked for, and the spelling is kept.
    state = make_state(tmp_path, [project("demo-project", 40)])
    fake = FakeHttp()
    fake.add(
        project_url("demo-project"),
        body=page(
            "demo-project", 45, dist("demo_project-2.0.tar.gz"), versions=["2.0"]
        ),
    )
    fake.add(version_url("demo-project", "2.0"), body=release("Demo_Project", "2.0"))
    day = Day(tmp_path, state, fake)

    report, rows = day.run(Root(CURSOR + 10, {"demo-project": 45}))

    assert ("record", "demo-project", 1, "2.0") in rows
    records = day.conn.execute("SELECT name, name_raw FROM release_records")
    assert records.fetchall() == [("demo-project", "Demo_Project")]


@pytest.mark.parametrize(
    "url", [project_url("demo"), version_url("demo", "2.0")], ids=["page", "release"]
)
def test_not_modified_reply_fails_the_run(tmp_path, url):
    # No ETag is sent, so a 304 answers nothing that was asked, and there is no copy
    # that it could say is current.
    state = make_state(tmp_path, [project("demo", 40)])
    fake = FakeHttp()
    fake.add(url, status=304)
    fake.add(
        project_url("demo"),
        body=page("demo", 45, dist("demo-2.0.tar.gz"), versions=["2.0"]),
    )
    day = Day(tmp_path, state, fake)

    with pytest.raises(UpstreamError) as caught:
        day.run(Root(CURSOR + 10, {"demo": 45}))

    assert (caught.value.url, caught.value.status) == (url, 304)


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
    # The rest of it is the last manifest of the month, so it is made from what was
    # published, and a run that does it again writes the same bytes.
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


def test_month_close_replay_mismatch_fails(tmp_path):
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

    with pytest.raises(GateError, match="projects") as caught:
        stream.run("2026-12-01T06:00:00Z", 120, {**PROJECTS, "alpha": 11})

    assert [failure.rule for failure in caught.value.failures] == ["replay"]
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
    # months stays: it is made from the published state alone.
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

    # A later run, with another run id, does the same rollover again. It finds the
    # same bytes in place, adds nothing to the months, and publishes.
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
