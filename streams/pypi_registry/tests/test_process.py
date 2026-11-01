import math
import sqlite3
from collections.abc import Sequence
from dataclasses import fields
from pathlib import Path

import pytest

from obskit.http import Http, UpstreamError
from obskit.testing import FakeHttp
from pypi_registry.process import ProcessReport, process_projects
from pypi_registry.simple import ACCEPT, Root, project_url, version_url
from pypi_registry.store import (
    DELTA_SCHEMA,
    STATE_SCHEMA,
    Delta,
    apply_delta,
    open_db,
    state_rows,
)
from run_helpers import (
    BEFORE,
    BOOTSTRAP_AT,
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

# The time of the runs, which the delta gives each row that has a time.
OBSERVED_AT = "2026-11-02T06:00:00Z"
CURSOR = 1000


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


def event(
    name: str, incarnation: int, kind: str, *, version=None, filename=None, detail=None
) -> tuple:
    """An event as `written` gives it."""
    return ("event", name, incarnation, kind, version, filename, detail)


def counts(**given: int) -> ProcessReport:
    """A report with the counts given, and 0 for the others."""
    return ProcessReport(**{field.name: 0 for field in fields(ProcessReport)} | given)


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
