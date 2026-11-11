import json
import shutil
import sqlite3
import tomllib
from collections.abc import Callable, Iterator
from contextlib import closing
from pathlib import Path

import pytest

from obskit.sqlite import consolidate
from pypi_registry.diff import Event
from pypi_registry.records import RECORD_COLUMNS, release_record
from pypi_registry.simple import FileInfo
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
STREAM_TOML = Path(__file__).parents[1] / "stream.toml"
OBSERVED_AT = "2026-11-01T06:00:00Z"
# When the base state of these tests first saw its projects.
FIRST_SEEN = "2026-10-01T00:00:00Z"

STATE_COLUMNS = {
    "meta": ["key", "value"],
    "projects": [
        "name",
        "incarnation",
        "last_serial",
        "status",
        "swept",
        "first_seen",
        "first_seen_exact",
    ],
    "files_current": ["name", "incarnation", "filename", "yanked", "yanked_reason"],
    "versions_seen": ["name", "incarnation", "version", "record"],
}
STATE_KEYS = {
    "meta": ["key"],
    "projects": ["name"],
    "files_current": ["name", "incarnation", "filename"],
    "versions_seen": ["name", "incarnation", "version"],
}
# The columns that the state declares a type for, and the columns that may be null. The
# columns of a key cannot be null in a table that is WITHOUT ROWID.
STATE_TYPES = {
    "meta": {"key": "TEXT", "value": "TEXT"},
    "projects": {
        "name": "TEXT",
        "incarnation": "INTEGER",
        "last_serial": "INTEGER",
        "status": "TEXT",
        "swept": "INTEGER",
        "first_seen": "TEXT",
        "first_seen_exact": "INTEGER",
    },
    "files_current": {"yanked": "INTEGER"},
    "versions_seen": {"record": "INTEGER"},
}
STATE_NULLABLE = {
    "meta": set(),
    "projects": set(),
    "files_current": {"yanked_reason"},
    "versions_seen": set(),
}
DELTA_COLUMNS = {
    "files": [
        "seq",
        "name",
        "incarnation",
        "filename",
        "version",
        "size",
        "upload_time",
        "requires_python",
        "yanked",
        "yanked_reason",
        "has_metadata",
        "has_provenance",
        "first_seen",
        "first_seen_exact",
    ],
    "versions": ["seq", "name", "incarnation", "version", "record"],
    "release_records": ["seq", *RECORD_COLUMNS],
    "events": [
        "seq",
        "name",
        "incarnation",
        "kind",
        "version",
        "filename",
        "detail",
        "source_at",
        "observed_at",
    ],
    "serials": ["seq", "name", "serial"],
    "swept": ["seq", "name", "incarnation", "status"],
    "cursors": ["seq", "serial", "fetched_at"],
}
# The key of each table of a delta. A table that is keyed by what its rows are about is
# WITHOUT ROWID. The others are keyed by their `seq`, which is their rowid.
DELTA_KEYS = {
    "files": ["name", "incarnation", "filename"],
    "versions": ["name", "incarnation", "version"],
    "release_records": ["name", "incarnation", "version"],
    "events": ["seq"],
    "serials": ["seq"],
    "swept": ["seq"],
    "cursors": ["seq"],
}

type Rows = dict[str, list[tuple]]
type Writer = Callable[[Delta], None]


def project(
    name: str,
    *,
    incarnation: int = 1,
    serial: int = 5,
    status: str = "active",
    swept: int = 1,
    first_seen: str = FIRST_SEEN,
    exact: int = 0,
) -> tuple:
    """A row of `projects`. The defaults are those of a project that the base state
    held from its first run: swept, and first seen at a time that is not exact."""
    return (name, incarnation, serial, status, swept, first_seen, exact)


# A state as a run leaves it. `demo` has a file that is yanked, `other` is a second
# project that a test of one project should leave alone, and `gone` was removed.
BASE: Rows = {
    "meta": [("applied_seq", "0"), ("bootstrap_at", FIRST_SEEN), ("cursor", "100")],
    "projects": [project("demo"), project("gone", status="removed"), project("other")],
    "files_current": [
        ("demo", 1, "demo-1.0.tar.gz", 0, None),
        ("demo", 1, "demo-1.1.tar.gz", 1, "old reason"),
        ("other", 1, "other-2.0.tar.gz", 0, None),
    ],
    "versions_seen": [
        ("demo", 1, "1.0", 1),
        ("demo", 1, "1.1", 0),
        ("gone", 1, "0.1", 1),
        ("other", 1, "2.0", 1),
    ],
}


def file_info(
    filename: str,
    *,
    version: str | None = "1.0",
    yanked: bool = False,
    reason: str | None = None,
) -> FileInfo:
    return FileInfo(
        filename=filename,
        version=version,
        size=1234,
        upload_time="2026-10-30T12:00:00.000000Z",
        requires_python=">=3.9",
        yanked=yanked,
        yanked_reason=reason,
        has_metadata=True,
        has_provenance=False,
    )


def record_of(name: str = "demo", incarnation: int = 1, version: str = "1.0") -> dict:
    """The record of a release, made from the sample reply with another name and
    version."""
    path = FIXTURES / "version-sampleproject-4.0.0.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    info = {**document["info"], "name": name, "version": version}
    return release_record(
        name, incarnation, info, document["urls"], observed_at=OBSERVED_AT
    )


# The connections that the helpers below open. SQLite warns about a connection that is
# collected while it is open, so each test closes what it opened when it ends.
OPENED: list[sqlite3.Connection] = []


@pytest.fixture(autouse=True)
def close_connections() -> Iterator[None]:
    yield
    while OPENED:
        OPENED.pop().close()


def opened(conn: sqlite3.Connection) -> sqlite3.Connection:
    OPENED.append(conn)
    return conn


def new_delta(
    tmp_path: Path, name: str = "delta.db", **options: int
) -> tuple[sqlite3.Connection, Delta]:
    conn = opened(open_db(tmp_path / name, DELTA_SCHEMA, create=True))
    return conn, Delta(conn, OBSERVED_AT, **options)


def seeded_state(tmp_path: Path, name: str, rows: Rows) -> sqlite3.Connection:
    """A new state database that holds `rows`, the way a bootstrap writes one."""
    state = opened(open_db(tmp_path / name, STATE_SCHEMA, create=True))
    for table, table_rows in rows.items():
        if table_rows:
            marks = ", ".join("?" * len(table_rows[0]))
            state.executemany(f"INSERT INTO {table} VALUES ({marks})", table_rows)
    state.commit()
    return state


def base_file(tmp_path: Path, name: str) -> Path:
    """A copy of the base state as a file that nothing has open."""
    base = tmp_path / "base.db"
    if not base.exists():
        seeded_state(tmp_path, "base.db", BASE).close()
    shutil.copy(base, tmp_path / name)
    return tmp_path / name


def base_copy(tmp_path: Path, name: str) -> sqlite3.Connection:
    """An open copy of the base state, so that a test can change it as it likes."""
    return opened(open_db(base_file(tmp_path, name), STATE_SCHEMA))


def apply_rows(state: sqlite3.Connection, tmp_path: Path, write: Writer) -> int:
    """Write rows with `write` into a new delta, apply the delta to `state` in one pass,
    and return what `apply_delta` returned."""
    name = f"applied-{len(list(tmp_path.glob('applied-*.db')))}.db"
    conn, delta = new_delta(tmp_path, name)
    write(delta)
    conn.commit()
    with closing(conn):
        return apply_delta(state, conn)


def apply_events(state: sqlite3.Connection, tmp_path: Path, *events: Event) -> int:
    def write(delta: Delta) -> None:
        for event in events:
            delta.event(event)

    return apply_rows(state, tmp_path, write)


def store_applied(state: sqlite3.Connection, seq: int) -> None:
    """What a run does with the number that `apply_delta` returns."""
    state.execute(
        "INSERT INTO meta (key, value) VALUES ('applied_seq', ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (str(seq),),
    )
    state.commit()


def rows_of(conn: sqlite3.Connection, table: str) -> list[tuple]:
    return conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()


def table_info(conn: sqlite3.Connection, table: str) -> list[tuple]:
    return conn.execute(f"PRAGMA table_info({table})").fetchall()


def user_tables(conn: sqlite3.Connection) -> dict[str, bool]:
    """Each table of the database, with whether it is WITHOUT ROWID."""
    return {
        name: bool(without_rowid)
        for schema, name, kind, _, without_rowid, _ in conn.execute("PRAGMA table_list")
        if schema == "main" and kind == "table" and not name.startswith("sqlite_")
    }


def key_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    keyed = sorted((row[5], row[1]) for row in table_info(conn, table) if row[5])
    return [name for _, name in keyed]


# ---- the schemas


def test_schemas_create_tables(tmp_path):
    with closing(open_db(tmp_path / "state.db", STATE_SCHEMA, create=True)) as state:
        # Every table of the state is WITHOUT ROWID.
        assert user_tables(state) == dict.fromkeys(STATE_COLUMNS, True)
        for table, columns in STATE_COLUMNS.items():
            info = table_info(state, table)
            assert [row[1] for row in info] == columns
            assert key_columns(state, table) == STATE_KEYS[table]
            assert {row[1]: row[2] for row in info if row[2]} == STATE_TYPES[table]
            assert {row[1] for row in info if not row[3]} == STATE_NULLABLE[table]
    with closing(open_db(tmp_path / "delta.db", DELTA_SCHEMA, create=True)) as delta:
        assert user_tables(delta) == {
            "files": True,
            "versions": True,
            "release_records": True,
            "events": False,
            "serials": False,
            "swept": False,
            "cursors": False,
        }
        for table, columns in DELTA_COLUMNS.items():
            info = table_info(delta, table)
            assert [row[1] for row in info] == columns
            assert key_columns(delta, table) == DELTA_KEYS[table]
            # `seq` is the first column of every table, an integer, and never null.
            seq = info[0]
            assert (seq[1], seq[2]) == ("seq", "INTEGER")
            if DELTA_KEYS[table] == ["seq"]:
                assert seq[5] == 1  # it is the key, and so the rowid
            else:
                assert seq[3] == 1  # it is declared NOT NULL


def test_release_records_columns_match_records():
    with closing(sqlite3.connect(":memory:")) as scratch:
        scratch.executescript(DELTA_SCHEMA)
        names = [row[1] for row in table_info(scratch, "release_records")]
    assert names[0] == "seq"
    assert names[1:] == list(RECORD_COLUMNS)
    # A record that `release_record` makes has just those keys.
    assert list(record_of()) == list(RECORD_COLUMNS)


def test_every_table_has_a_history_class_and_every_class_has_a_table():
    # The publish gates read the history class of each table from the stream's own
    # file, so a table that it does not list, or lists and nothing makes, is a mistake.
    config = tomllib.loads(STREAM_TOML.read_text(encoding="utf-8"))
    with closing(sqlite3.connect(":memory:")) as scratch:
        scratch.executescript(STATE_SCHEMA)
        scratch.executescript(DELTA_SCHEMA)
        assert sorted(user_tables(scratch)) == sorted(config["tables"])


# ---- the delta: numbering


def test_seq_is_global_and_increasing(tmp_path):
    conn, delta = new_delta(tmp_path)
    # One row for each method, in an order that mixes the tables.
    delta.event(Event("demo", 1, "created"))
    delta.file("demo", 1, file_info("demo-1.0.tar.gz"), exact=True)
    delta.cursor(100, OBSERVED_AT)
    delta.version("demo", 1, "1.0", record=True)
    delta.serial("demo", 7)
    delta.record(record_of())
    delta.swept("demo", 1, "active")
    delta.event(Event("demo", 1, "yanked", filename="demo-1.0.tar.gz", detail="x"))
    conn.commit()
    found = sorted(
        (seq, table)
        for table in DELTA_COLUMNS
        for (seq,) in conn.execute(f"SELECT seq FROM {table}")
    )
    # One sequence, with no number used twice and none left out, that follows the order
    # of the calls and not the order of the tables.
    assert found == [
        (1, "events"),
        (2, "files"),
        (3, "cursors"),
        (4, "versions"),
        (5, "serials"),
        (6, "release_records"),
        (7, "swept"),
        (8, "events"),
    ]


def test_first_seq_continues_numbering(tmp_path):
    path = tmp_path / "delta.db"
    # A delta that starts at 101 numbers its first row 101.
    with closing(open_db(path, DELTA_SCHEMA, create=True)) as conn:
        delta = Delta(conn, OBSERVED_AT, first_seq=101)
        delta.event(Event("demo", 1, "created"))
        delta.serial("demo", 7)
        conn.commit()
        assert [seq for (seq,) in conn.execute("SELECT seq FROM events")] == [101]
    # Opened again with the default start, it goes on after its highest seq.
    with closing(open_db(path, DELTA_SCHEMA)) as conn:
        delta = Delta(conn, OBSERVED_AT, first_seq=1)
        delta.event(Event("demo", 1, "quarantined"))
        conn.commit()
        assert [seq for (seq,) in conn.execute("SELECT seq FROM events")] == [101, 103]
    # A start that is past its highest seq is kept.
    with closing(open_db(path, DELTA_SCHEMA)) as conn:
        delta = Delta(conn, OBSERVED_AT, first_seq=500)
        delta.event(Event("demo", 1, "unquarantined"))
        conn.commit()
        found = [seq for (seq,) in conn.execute("SELECT seq FROM events")]
        assert found == [101, 103, 500]


WRITERS: list[tuple[str, Writer]] = [
    ("files", lambda d: d.file("demo", 1, file_info("demo-1.0.tar.gz"), exact=True)),
    ("versions", lambda d: d.version("demo", 1, "1.0", record=True)),
    ("release_records", lambda d: d.record(record_of())),
    ("events", lambda d: d.event(Event("demo", 1, "created"))),
    ("serials", lambda d: d.serial("demo", 7)),
    ("swept", lambda d: d.swept("demo", 1, "active")),
    ("cursors", lambda d: d.cursor(100, OBSERVED_AT)),
]


@pytest.mark.parametrize("write", [w[1] for w in WRITERS], ids=[w[0] for w in WRITERS])
def test_the_highest_seq_may_be_in_any_table(tmp_path, write):
    # Whichever table holds the highest seq, a delta that is opened again numbers its
    # next row after it. A table that was left out of the count would repeat a seq.
    path = tmp_path / "delta.db"
    with closing(open_db(path, DELTA_SCHEMA, create=True)) as conn:
        write(Delta(conn, OBSERVED_AT, first_seq=40))
        conn.commit()
    with closing(open_db(path, DELTA_SCHEMA)) as conn:
        Delta(conn, OBSERVED_AT).event(Event("other", 1, "created"))
        conn.commit()
        found = conn.execute("SELECT seq FROM events WHERE name = 'other'")
        assert found.fetchall() == [(41,)]


@pytest.mark.parametrize("first_seq", [0, -1])
def test_first_seq_must_be_positive(tmp_path, first_seq):
    # A state starts with nothing applied, which is 0, and applies the rows above that.
    # A row numbered 0 would never be applied.
    conn = open_db(tmp_path / "delta.db", DELTA_SCHEMA, create=True)
    with closing(conn), pytest.raises(ValueError, match="first_seq"):
        Delta(conn, OBSERVED_AT, first_seq=first_seq)


def test_a_row_that_is_refused_takes_no_seq(tmp_path):
    conn, delta = new_delta(tmp_path)
    delta.file("demo", 1, file_info("demo-1.0.tar.gz"), exact=True)
    with pytest.raises(sqlite3.IntegrityError):
        delta.file("demo", 1, file_info("demo-1.0.tar.gz"), exact=True)
    delta.serial("demo", 7)
    assert rows_of(conn, "serials") == [(2, "demo", 7)]


# ---- the delta: what each method writes


def test_each_method_writes_its_row(tmp_path):
    conn, delta = new_delta(tmp_path)
    delta.event(
        Event("demo", 2, "yanked", "1.0", "demo-1.0.tar.gz", "broken", "2026-10-30")
    )
    delta.event(Event("demo", 2, "created"))
    delta.file("demo", 2, file_info("demo-1.0.tar.gz"), exact=True)
    odd = FileInfo("odd.egg", None, None, None, None, True, "bad", False, True)
    delta.file("demo", 2, odd, exact=False)
    delta.version("demo", 2, "1.0", record=True)
    delta.version("demo", 2, "0.9", record=False)
    record = record_of("demo", 2, "1.0")
    delta.record(record)
    delta.serial("demo", 7)
    delta.swept("demo", 2, "archived")
    delta.cursor(1000, "2026-11-01T06:00:05Z")

    # An event takes the time of the delta as the time it was observed.
    assert rows_of(conn, "events") == [
        (1, "demo", 2, "yanked", "1.0", "demo-1.0.tar.gz", "broken", "2026-10-30")
        + (OBSERVED_AT,),
        (2, "demo", 2, "created", None, None, None, None, OBSERVED_AT),
    ]
    # A file keeps what its page said. Its first sight is the time of the delta, and it
    # is exact only when the caller says so. Flags are stored as 0 and 1.
    assert rows_of(conn, "files") == [
        (3, "demo", 2, "demo-1.0.tar.gz", "1.0", 1234)
        + ("2026-10-30T12:00:00.000000Z", ">=3.9", 0, None, 1, 0, OBSERVED_AT, 1),
        (4, "demo", 2, "odd.egg", None, None)
        + (None, None, 1, "bad", 0, 1, OBSERVED_AT, 0),
    ]
    assert rows_of(conn, "versions") == [
        (5, "demo", 2, "1.0", 1),
        (6, "demo", 2, "0.9", 0),
    ]
    # A record goes in whole, in the order of its columns, with its booleans as 0 and 1.
    (stored,) = rows_of(conn, "release_records")
    assert stored[0] == 7
    assert dict(zip(RECORD_COLUMNS, stored[1:], strict=True)) == {
        key: int(value) if isinstance(value, bool) else value
        for key, value in record.items()
    }
    assert rows_of(conn, "serials") == [(8, "demo", 7)]
    assert rows_of(conn, "swept") == [(9, "demo", 2, "archived")]
    assert rows_of(conn, "cursors") == [(10, 1000, "2026-11-01T06:00:05Z")]


def test_a_record_needs_exactly_the_record_columns(tmp_path):
    conn, delta = new_delta(tmp_path)
    good = record_of()
    missing = {key: value for key, value in good.items() if key != "summary"}
    extra = {**good, "readme_text": "the whole README"}
    with pytest.raises(ValueError, match="summary"):
        delta.record(missing)
    with pytest.raises(ValueError, match="readme_text"):
        delta.record(extra)
    assert rows_of(conn, "release_records") == []
    # Neither took a seq.
    delta.serial("demo", 1)
    assert rows_of(conn, "serials") == [(1, "demo", 1)]


def test_a_delta_tells_the_time_of_its_run(tmp_path):
    # A release record carries the time it was observed, and the run takes it from the
    # delta, so the record and the rows around it give one time.
    _, delta = new_delta(tmp_path)
    assert delta.observed_at == OBSERVED_AT
    with pytest.raises(AttributeError):
        delta.observed_at = "2026-11-01T07:00:00Z"


# ---- apply_delta: what each row does


def test_apply_delta_semantics(tmp_path):
    meta = [("applied_seq", "0"), ("bootstrap_at", FIRST_SEEN), ("cursor", "100")]
    state = seeded_state(tmp_path, "state.db", {"meta": meta})

    def write(delta: Delta) -> None:
        delta.event(Event("demo", 1, "created"))  # 1
        delta.file("demo", 1, file_info("demo-1.0.tar.gz"), exact=True)  # 2
        wheel = file_info("demo-1.0-py3-none-any.whl", yanked=True, reason="bad wheel")
        delta.file("demo", 1, wheel, exact=True)  # 3
        old = file_info("demo-0.9.tar.gz", version="0.9")
        delta.file("demo", 1, old, exact=False)  # 4
        delta.version("demo", 1, "1.0", record=True)  # 5
        delta.record(record_of("demo", 1, "1.0"))  # 6
        delta.version("demo", 1, "0.9", record=False)  # 7
        yank = Event("demo", 1, "yanked", filename="demo-1.0.tar.gz", detail="broken")
        delta.event(yank)  # 8
        gone = Event("demo", 1, "file_removed", filename="demo-0.9.tar.gz")
        delta.event(gone)  # 9
        delta.swept("demo", 1, "archived")  # 10
        delta.serial("demo", 42)  # 11
        delta.cursor(1000, "2026-11-01T06:00:05Z")  # 12

    assert apply_rows(state, tmp_path, write) == 12
    assert state_rows(state) == {
        # The run stores `applied_seq`, and `apply_delta` leaves it alone.
        "meta": [
            ("applied_seq", "0"),
            ("bootstrap_at", FIRST_SEEN),
            ("cursor", "1000"),
        ],
        # Created at the time of the delta, and exactly. The swept row set `swept` and
        # the status of the page, and the serial row set the serial.
        "projects": [("demo", 1, 42, "archived", 1, OBSERVED_AT, 1)],
        # The wheel came in yanked, the sdist was yanked by an event, and the oldest
        # file was added and then removed.
        "files_current": [
            ("demo", 1, "demo-1.0-py3-none-any.whl", 1, "bad wheel"),
            ("demo", 1, "demo-1.0.tar.gz", 1, "broken"),
        ],
        "versions_seen": [("demo", 1, "0.9", 0), ("demo", 1, "1.0", 1)],
    }


def test_created_adds_a_project(tmp_path):
    state = base_copy(tmp_path, "state.db")
    apply_events(state, tmp_path, Event("fresh", 1, "created", source_at="2026-10-31"))
    # It is first seen now, and exactly, with nothing fetched and nothing swept.
    fresh = project("fresh", serial=0, swept=0, first_seen=OBSERVED_AT, exact=1)
    rows = state_rows(state)
    assert rows["projects"] == [
        project("demo"),
        fresh,
        project("gone", status="removed"),
        project("other"),
    ]
    for table in ("meta", "files_current", "versions_seen"):
        assert rows[table] == BASE[table]


def test_created_takes_the_incarnation_of_the_event(tmp_path):
    # An event is about an incarnation, and the project is created at the one it names.
    state = base_copy(tmp_path, "state.db")
    apply_events(state, tmp_path, Event("fresh", 3, "created"))
    fresh = project(
        "fresh", incarnation=3, serial=0, swept=0, first_seen=OBSERVED_AT, exact=1
    )
    assert fresh in state_rows(state)["projects"]


def test_reregistered_starts_the_new_incarnation(tmp_path):
    state = base_copy(tmp_path, "state.db")
    apply_events(state, tmp_path, Event("gone", 2, "reregistered"))
    # It is active again, with nothing swept and a first sight of now. That sight is
    # exact, though the old incarnation's was a bootstrap's and was not: the project is
    # a new one, and it was seen when it came back. The serial is still the old one,
    # which the serial row that goes with the event will replace.
    again = project("gone", incarnation=2, swept=0, first_seen=OBSERVED_AT, exact=1)
    rows = state_rows(state)
    assert rows["projects"] == [project("demo"), again, project("other")]
    # What was seen of the old incarnation stays.
    assert rows["versions_seen"] == BASE["versions_seen"]
    assert rows["files_current"] == BASE["files_current"]


@pytest.mark.parametrize("status", ["active", "quarantined", "archived", "deprecated"])
def test_reregistered_is_refused_for_a_project_that_is_not_removed(tmp_path, status):
    # Only a name that was removed can be registered again. A project in any other
    # status is the same project, and a quarantined one keeps its files, so a
    # `reregistered` event for it means that the delta and the state disagree.
    rows = {**BASE, "projects": [project("demo", status=status)]}
    state = seeded_state(tmp_path, "state.db", rows)
    with pytest.raises(ValueError, match="removed project 'demo' below incarnation 2"):
        apply_events(state, tmp_path, Event("demo", 2, "reregistered"))
    assert state_rows(state)["projects"] == [project("demo", status=status)]
    assert not state.in_transaction


def test_removed_drops_the_files_of_that_incarnation(tmp_path):
    state = base_copy(tmp_path, "state.db")
    # A file of another incarnation of the name is not the removed project's.
    state.execute(
        "INSERT INTO files_current VALUES ('demo', 2, 'demo-9.0.tar.gz', 0, NULL)"
    )
    state.commit()
    apply_events(state, tmp_path, Event("demo", 1, "removed"))
    rows = state_rows(state)
    assert rows["projects"] == [
        project("demo", status="removed"),
        project("gone", status="removed"),
        project("other"),
    ]
    assert rows["files_current"] == [
        ("demo", 2, "demo-9.0.tar.gz", 0, None),
        ("other", 1, "other-2.0.tar.gz", 0, None),
    ]
    # The versions that were seen stay seen.
    assert rows["versions_seen"] == BASE["versions_seen"]


def test_a_quarantine_keeps_the_files_and_an_unquarantine_adds_none(tmp_path):
    state = base_copy(tmp_path, "state.db")
    conn, delta = new_delta(tmp_path)
    delta.event(Event("demo", 1, "quarantined"))
    conn.commit()
    seq = apply_delta(state, conn)
    assert state_rows(state)["projects"][0] == project("demo", status="quarantined")
    assert state_rows(state)["files_current"] == BASE["files_current"]

    delta.event(Event("demo", 1, "unquarantined"))
    conn.commit()
    apply_delta(state, conn, after_seq=seq)
    conn.close()
    assert state_rows(state)["projects"][0] == project("demo")
    # The files are those of before, with the yank of the one, and none was added.
    assert state_rows(state)["files_current"] == BASE["files_current"]


def test_status_changed_takes_the_detail(tmp_path):
    state = base_copy(tmp_path, "state.db")
    apply_events(state, tmp_path, Event("demo", 1, "status_changed", detail="archived"))
    rows = state_rows(state)
    assert rows["projects"][0] == project("demo", status="archived")
    assert rows["files_current"] == BASE["files_current"]


def test_status_changed_without_a_status_is_refused(tmp_path):
    # A project's status cannot be null, and a missing one is not turned into a status.
    state = base_copy(tmp_path, "state.db")
    with pytest.raises(sqlite3.IntegrityError):
        apply_events(state, tmp_path, Event("demo", 1, "status_changed"))
    assert state_rows(state) == BASE


def test_file_removed_deletes_that_file(tmp_path):
    state = base_copy(tmp_path, "state.db")
    gone = Event("demo", 1, "file_removed", filename="demo-1.0.tar.gz")
    apply_events(state, tmp_path, gone)
    rows = state_rows(state)
    assert rows["files_current"] == [
        ("demo", 1, "demo-1.1.tar.gz", 1, "old reason"),
        ("other", 1, "other-2.0.tar.gz", 0, None),
    ]
    assert rows["projects"] == BASE["projects"]


def test_yanked_and_unyanked_update_the_file(tmp_path):
    state = base_copy(tmp_path, "state.db")
    apply_events(
        state,
        tmp_path,
        # Yanked with a reason, still yanked for another reason, and yanked with none.
        Event("demo", 1, "yanked", filename="demo-1.0.tar.gz", detail="broken build"),
        Event("demo", 1, "yanked", filename="demo-1.1.tar.gz", detail="new reason"),
        Event("other", 1, "yanked", filename="other-2.0.tar.gz"),
    )
    assert state_rows(state)["files_current"] == [
        ("demo", 1, "demo-1.0.tar.gz", 1, "broken build"),
        ("demo", 1, "demo-1.1.tar.gz", 1, "new reason"),
        ("other", 1, "other-2.0.tar.gz", 1, None),
    ]
    apply_events(
        state,
        tmp_path,
        Event("demo", 1, "unyanked", filename="demo-1.1.tar.gz"),
        # An unyank can carry a note, and the note is not the reason of a yank.
        Event("demo", 1, "unyanked", filename="demo-1.0.tar.gz", detail="was broken"),
    )
    # An unyank clears the reason with the flag.
    assert state_rows(state)["files_current"] == [
        ("demo", 1, "demo-1.0.tar.gz", 0, None),
        ("demo", 1, "demo-1.1.tar.gz", 0, None),
        ("other", 1, "other-2.0.tar.gz", 1, None),
    ]


def test_a_file_row_adds_the_file_with_its_yank(tmp_path):
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        delta.file("demo", 1, file_info("demo-2.0.tar.gz", version="2.0"), exact=True)
        wheel = file_info("demo-2.0-py3-none-any.whl", yanked=True, reason="bad")
        delta.file("demo", 1, wheel, exact=False)

    apply_rows(state, tmp_path, write)
    assert state_rows(state)["files_current"] == [
        ("demo", 1, "demo-1.0.tar.gz", 0, None),
        ("demo", 1, "demo-1.1.tar.gz", 1, "old reason"),
        ("demo", 1, "demo-2.0-py3-none-any.whl", 1, "bad"),
        ("demo", 1, "demo-2.0.tar.gz", 0, None),
        ("other", 1, "other-2.0.tar.gz", 0, None),
    ]


@pytest.mark.parametrize(
    ("was", "seen", "now"), [(0, 0, 0), (0, 1, 1), (1, 0, 1), (1, 1, 1)]
)
def test_a_version_row_keeps_the_higher_record(tmp_path, was, seen, now):
    # `record` says whether a release has a record. Seeing the version again without
    # one never takes one away, and a record can be added later.
    rows = {**BASE, "versions_seen": [("demo", 1, "1.0", was)]}
    state = seeded_state(tmp_path, "state.db", rows)
    apply_rows(
        state, tmp_path, lambda d: d.version("demo", 1, "1.0", record=bool(seen))
    )
    assert state_rows(state)["versions_seen"] == [("demo", 1, "1.0", now)]


def test_a_version_row_adds_a_version_that_is_new(tmp_path):
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        delta.version("demo", 1, "2.0", record=True)
        delta.version("demo", 2, "1.0", record=False)  # the same version, later

    apply_rows(state, tmp_path, write)
    assert state_rows(state)["versions_seen"] == [
        ("demo", 1, "1.0", 1),
        ("demo", 1, "1.1", 0),
        ("demo", 1, "2.0", 1),
        ("demo", 2, "1.0", 0),
        ("gone", 1, "0.1", 1),
        ("other", 1, "2.0", 1),
    ]


def test_a_serial_row_sets_the_serial_even_to_a_lower_one(tmp_path):
    # A serial that differs either way is worth fetching, so a lower one is written, and
    # the state follows what was written.
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        delta.serial("demo", 9)
        delta.serial("other", 2)

    apply_rows(state, tmp_path, write)
    assert state_rows(state)["projects"] == [
        project("demo", serial=9),
        project("gone", status="removed"),
        project("other", serial=2),
    ]


def test_a_swept_row_sets_swept_and_the_status_of_the_page(tmp_path):
    rows = {**BASE, "projects": [project("demo", swept=0), project("other", swept=0)]}
    state = seeded_state(tmp_path, "state.db", rows)
    apply_rows(state, tmp_path, lambda d: d.swept("demo", 1, "deprecated"))
    assert state_rows(state)["projects"] == [
        project("demo", status="deprecated", swept=1),
        project("other", swept=0),
    ]


def test_a_cursor_row_sets_the_cursor(tmp_path):
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        delta.cursor(150, "2026-11-01T06:00:01Z")
        delta.cursor(160, "2026-11-01T06:00:02Z")

    apply_rows(state, tmp_path, write)
    # The last row wins, and the cursor is text, as `meta` holds every value.
    assert state_rows(state)["meta"] == [
        ("applied_seq", "0"),
        ("bootstrap_at", FIRST_SEEN),
        ("cursor", "160"),
    ]


def test_a_cursor_row_adds_the_cursor_when_there_is_none(tmp_path):
    state = seeded_state(tmp_path, "state.db", {})
    apply_rows(state, tmp_path, lambda d: d.cursor(150, "2026-11-01T06:00:01Z"))
    assert state_rows(state)["meta"] == [("cursor", "150")]


def test_a_cursor_row_without_a_serial_is_refused(tmp_path):
    # The serial goes to `meta.value` as it is, and a null is refused there. It is not
    # turned into the text 'None' and kept as a cursor.
    state = base_copy(tmp_path, "state.db")
    conn, _ = new_delta(tmp_path)
    conn.execute("INSERT INTO cursors VALUES (1, NULL, '2026-11-01T06:00:01Z')")
    conn.commit()
    with closing(conn), pytest.raises(sqlite3.IntegrityError, match="meta.value"):
        apply_delta(state, conn)
    assert state_rows(state) == BASE


def test_applying_a_record_marks_the_version_recorded(tmp_path):
    # demo 1.1 was seen without a record. A record that comes later, in a row of its
    # own, marks it, and changes nothing else.
    state = base_copy(tmp_path, "state.db")
    apply_rows(state, tmp_path, lambda d: d.record(record_of("demo", 1, "1.1")))
    assert state_rows(state) == {
        **BASE,
        "versions_seen": [
            ("demo", 1, "1.0", 1),
            ("demo", 1, "1.1", 1),
            ("gone", 1, "0.1", 1),
            ("other", 1, "2.0", 1),
        ],
    }


def test_a_record_after_its_version_row_leaves_the_version_recorded(tmp_path):
    # A run that reads a new version writes the version with record=True and then the
    # record, as every delta has. The record marks a version that is marked already.
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        delta.version("demo", 1, "2.0", record=True)
        delta.record(record_of("demo", 1, "2.0"))

    assert apply_rows(state, tmp_path, write) == 2
    assert ("demo", 1, "2.0", 1) in state_rows(state)["versions_seen"]


@pytest.mark.parametrize(
    ("incarnation", "version"), [(1, "1.2"), (2, "1.0")], ids=["version", "incarnation"]
)
def test_a_record_for_an_unseen_version_is_refused(tmp_path, incarnation, version):
    # The state holds no such version: not 1.2 of demo, and not 1.0 of a second
    # incarnation. A record of it means that the delta and the state disagree.
    state = base_copy(tmp_path, "state.db")
    conn, delta = new_delta(tmp_path)
    delta.record(record_of("demo", incarnation, version))
    conn.commit()
    with closing(conn), pytest.raises(ValueError, match="the state has no") as caught:
        apply_delta(state, conn)
    assert f"version {version!r}" in str(caught.value)
    assert any(
        "row 1 of the release_records table" in note for note in caught.value.__notes__
    )
    assert state_rows(state) == BASE


# ---- apply_delta: order, and what it returns


def test_rows_apply_in_seq_order_across_tables(tmp_path):
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        # A file that is added and then removed is gone. Applied by table, with the
        # events first, the removal would find no file.
        delta.file("demo", 1, file_info("demo-3.0.tar.gz", version="3.0"), exact=True)
        delta.event(Event("demo", 1, "file_removed", filename="demo-3.0.tar.gz"))
        # A file that is added after its project was removed is there. Applied by
        # table, with the files first, the removal would drop it.
        delta.event(Event("other", 1, "removed"))
        delta.file("other", 1, file_info("other-3.0.tar.gz", version="3.0"), exact=True)
        # A yank of a file that was added a moment before.
        delta.file("demo", 1, file_info("demo-4.0.tar.gz", version="4.0"), exact=True)
        delta.event(Event("demo", 1, "yanked", filename="demo-4.0.tar.gz", detail="x"))

    apply_rows(state, tmp_path, write)
    rows = state_rows(state)
    assert rows["files_current"] == [
        ("demo", 1, "demo-1.0.tar.gz", 0, None),
        ("demo", 1, "demo-1.1.tar.gz", 1, "old reason"),
        ("demo", 1, "demo-4.0.tar.gz", 1, "x"),
        ("other", 1, "other-3.0.tar.gz", 0, None),
    ]
    assert rows["projects"][2] == project("other", status="removed")


def test_apply_delta_returns_the_highest_seq_applied(tmp_path):
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        delta.serial("demo", 6)
        delta.event(Event("demo", 1, "quarantined"))
        delta.cursor(150, OBSERVED_AT)
        delta.swept("demo", 1, "quarantined")
        delta.version("demo", 1, "2.0", record=True)

    assert apply_rows(state, tmp_path, write) == 5


def test_a_release_record_counts_toward_the_seq_applied(tmp_path):
    # A record is a row of the delta like any other, and a delta that ends with one has
    # been applied through it. The next delta starts after that seq.
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        delta.serial("demo", 6)
        delta.record(record_of("demo", 1, "1.0"))
        delta.record(record_of("demo", 1, "1.1"))

    assert apply_rows(state, tmp_path, write) == 3


def test_apply_delta_with_nothing_to_apply_returns_after_seq(tmp_path):
    state = base_copy(tmp_path, "state.db")
    conn, delta = new_delta(tmp_path)
    # A delta with no rows has nothing to apply, whatever the state has applied.
    assert apply_delta(state, conn) == 0
    assert apply_delta(state, conn, after_seq=7) == 7
    delta.serial("demo", 9)  # 1
    delta.serial("other", 8)  # 2
    conn.commit()
    # The state has applied through the last row of the delta, as it has after the
    # last pass of a day, so nothing is applied and nothing is lost.
    assert apply_delta(state, conn, after_seq=2) == 2
    conn.close()
    assert state_rows(state) == BASE


@pytest.mark.parametrize("after_seq", [3, 50])
def test_a_delta_numbered_below_after_seq_is_refused(tmp_path, after_seq):
    # A delta that has rows, all of them below what the state has applied, was not
    # numbered from there. None of its rows would ever be applied, and returning
    # `after_seq` would let that pass for a delta with nothing in it.
    state = base_copy(tmp_path, "state.db")
    conn, delta = new_delta(tmp_path)
    delta.serial("demo", 9)  # 1
    delta.record(record_of("demo", 1, "1.2"))  # 2
    conn.commit()
    with closing(conn), pytest.raises(ValueError, match="below after_seq") as caught:
        apply_delta(state, conn, after_seq=after_seq)
    assert f"is 2, which is below after_seq {after_seq}" in str(caught.value)
    assert state_rows(state) == BASE
    assert not state.in_transaction


def test_a_delta_whose_only_rows_are_records_is_numbered_by_them(tmp_path):
    # Records count toward the highest seq, so a delta of records alone, applied
    # through its last, is applied and is not refused.
    state = base_copy(tmp_path, "state.db")
    conn, delta = new_delta(tmp_path, first_seq=4)
    delta.record(record_of("demo", 1, "1.2"))  # 4
    conn.commit()
    with closing(conn):
        assert apply_delta(state, conn, after_seq=4) == 4
        with pytest.raises(ValueError, match="below after_seq"):
            apply_delta(state, conn, after_seq=5)


@pytest.mark.parametrize(
    ("after_seq", "demo", "other"), [(0, 11, 12), (1, 5, 12), (2, 5, 5), (3, 5, 5)]
)
def test_apply_delta_applies_only_the_rows_above_after_seq(
    tmp_path, after_seq, demo, other
):
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        delta.serial("demo", 11)  # 1
        delta.serial("other", 12)  # 2
        delta.cursor(200, OBSERVED_AT)  # 3

    conn, delta = new_delta(tmp_path)
    write(delta)
    conn.commit()
    with closing(conn):
        assert apply_delta(state, conn, after_seq=after_seq) == max(after_seq, 3)
    projects = {row[0]: row[2] for row in state_rows(state)["projects"]}
    assert (projects["demo"], projects["other"]) == (demo, other)
    cursor = dict(state_rows(state)["meta"])["cursor"]
    assert cursor == ("200" if after_seq < 3 else "100")


def test_apply_delta_commits_the_state(tmp_path):
    state = base_copy(tmp_path, "state.db")
    apply_rows(state, tmp_path, lambda d: d.serial("demo", 9))
    assert not state.in_transaction
    # Another connection to the file sees the change, so it is on disk.
    with closing(open_db(tmp_path / "state.db", STATE_SCHEMA)) as other:
        assert state_rows(other)["projects"][0] == project("demo", serial=9)


def test_apply_delta_commits_inside_a_transaction_that_is_open(tmp_path):
    # A caller can have a write that is not committed yet. The state is committed when
    # `apply_delta` returns, that write with it.
    state = base_copy(tmp_path, "state.db")
    state.execute("INSERT INTO meta VALUES ('note', 'before')")
    assert state.in_transaction
    apply_rows(state, tmp_path, lambda d: d.serial("demo", 9))
    assert not state.in_transaction
    with closing(open_db(tmp_path / "state.db", STATE_SCHEMA)) as other:
        rows = state_rows(other)
        assert ("note", "before") in rows["meta"]
        assert rows["projects"][0] == project("demo", serial=9)


def test_apply_delta_only_reads_the_delta(tmp_path):
    # A sealed delta can be opened read-only, and a replay must work from one.
    state = base_copy(tmp_path, "state.db")
    conn, delta = new_delta(tmp_path)
    delta.serial("demo", 9)
    conn.commit()
    conn.close()
    uri = f"{(tmp_path / 'delta.db').resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as readonly:
        assert apply_delta(state, readonly) == 1
    assert state_rows(state)["projects"][0] == project("demo", serial=9)


# ---- apply_delta: rows that do not fit


def test_apply_delta_is_all_or_nothing(tmp_path):
    state = base_copy(tmp_path, "state.db")
    conn, delta = new_delta(tmp_path)
    delta.event(Event("fresh", 1, "created"))
    delta.serial("demo", 99)
    delta.event(Event("ghost", 1, "removed"))  # the state has no project `ghost`
    conn.commit()
    with pytest.raises(ValueError, match="ghost"):
        apply_delta(state, conn)
    # Nothing that came before the bad row was kept, and no transaction is left open.
    assert state_rows(state) == BASE
    assert not state.in_transaction

    # The state can be used again, and the same delta applies once the bad row is gone.
    conn.execute("DELETE FROM events WHERE name = 'ghost'")
    conn.commit()
    assert apply_delta(state, conn) == 2
    conn.close()
    projects = {row[0]: row for row in state_rows(state)["projects"]}
    assert projects["demo"][2] == 99
    assert "fresh" in projects


def test_a_failed_apply_leaves_the_callers_own_write_alone(tmp_path):
    # Only what the call did is undone. A write that the caller made before it, and has
    # not committed, is still there and is the caller's to commit or roll back.
    state = base_copy(tmp_path, "state.db")
    state.execute("INSERT INTO meta VALUES ('note', 'before')")
    with pytest.raises(ValueError, match="ghost"):
        apply_events(
            state,
            tmp_path,
            Event("fresh", 1, "created"),
            Event("ghost", 1, "removed"),
        )
    assert state.in_transaction
    rows = state_rows(state)
    assert ("note", "before") in rows["meta"]
    assert rows["projects"] == BASE["projects"]
    state.rollback()
    assert state_rows(state) == BASE


MISFITS: list[tuple[str, Writer]] = [
    ("removed, no such project", lambda d: d.event(Event("ghost", 1, "removed"))),
    (
        "quarantined, no such project",
        lambda d: d.event(Event("ghost", 1, "quarantined")),
    ),
    (
        "unquarantined, no such project",
        lambda d: d.event(Event("ghost", 1, "unquarantined")),
    ),
    (
        "status_changed, no such project",
        lambda d: d.event(Event("ghost", 1, "status_changed", detail="archived")),
    ),
    (
        "reregistered, no such project",
        lambda d: d.event(Event("ghost", 2, "reregistered")),
    ),
    ("removed, another incarnation", lambda d: d.event(Event("demo", 2, "removed"))),
    (
        "reregistered, not a later incarnation",
        lambda d: d.event(Event("gone", 1, "reregistered")),  # removed, at 1
    ),
    (
        "reregistered, a project that is not removed",
        lambda d: d.event(Event("demo", 2, "reregistered")),  # active, at 1
    ),
    (
        "file_removed, no such file",
        lambda d: d.event(Event("demo", 1, "file_removed", filename="no-such.tar.gz")),
    ),
    (
        "yanked, no such file",
        lambda d: d.event(Event("demo", 1, "yanked", filename="no-such.tar.gz")),
    ),
    (
        "unyanked, no such file",
        lambda d: d.event(Event("demo", 1, "unyanked", filename="no-such.tar.gz")),
    ),
    (
        "yanked, a file of another incarnation",
        lambda d: d.event(Event("demo", 2, "yanked", filename="demo-1.0.tar.gz")),
    ),
    ("swept, no such project", lambda d: d.swept("ghost", 1, "active")),
    ("swept, another incarnation", lambda d: d.swept("demo", 2, "active")),
    ("serial, no such project", lambda d: d.serial("ghost", 3)),
]


@pytest.mark.parametrize("write", [m[1] for m in MISFITS], ids=[m[0] for m in MISFITS])
def test_a_row_that_does_not_fit_the_state_is_refused(tmp_path, write):
    # Each of these is about a project or a file that the state does not have. Such a
    # row means the delta and the state disagree, and a silent skip would hide it.
    state = base_copy(tmp_path, "state.db")
    conn, delta = new_delta(tmp_path)
    write(delta)
    conn.commit()
    with closing(conn), pytest.raises(ValueError, match="the state has no") as caught:
        apply_delta(state, conn)
    # The error says which row it was.
    assert any("row 1 of" in note for note in caught.value.__notes__)
    assert state_rows(state) == BASE


def test_created_for_a_project_that_is_there_is_refused(tmp_path):
    # A name that comes back after a removal is `reregistered`. A second `created` for a
    # name that the state holds is an error, and it does not replace the project.
    state = base_copy(tmp_path, "state.db")
    with pytest.raises(sqlite3.IntegrityError):
        apply_events(state, tmp_path, Event("demo", 1, "created"))
    assert state_rows(state) == BASE


def test_a_file_row_for_a_file_that_is_there_is_refused(tmp_path):
    state = base_copy(tmp_path, "state.db")
    again = file_info("demo-1.0.tar.gz")
    with pytest.raises(sqlite3.IntegrityError):
        apply_rows(state, tmp_path, lambda d: d.file("demo", 1, again, exact=True))
    assert state_rows(state) == BASE


def test_a_row_of_an_unknown_event_kind_is_refused(tmp_path):
    state = base_copy(tmp_path, "state.db")
    conn, _ = new_delta(tmp_path)
    conn.execute(
        "INSERT INTO events (seq, name, incarnation, kind, observed_at) "
        "VALUES (1, 'demo', 1, 'teleported', ?)",
        (OBSERVED_AT,),
    )
    conn.commit()
    with closing(conn), pytest.raises(ValueError, match="teleported"):
        apply_delta(state, conn)
    assert state_rows(state) == BASE


def test_two_rows_with_one_seq_are_refused(tmp_path):
    # Order is by `seq`. Two rows that share one leave the order open, so the delta is
    # refused and not applied in some order of the tables.
    state = base_copy(tmp_path, "state.db")
    conn, _ = new_delta(tmp_path)
    conn.execute("INSERT INTO cursors VALUES (3, 150, 'x')")
    conn.execute("INSERT INTO serials VALUES (3, 'demo', 9)")
    conn.commit()
    with closing(conn), pytest.raises(ValueError, match="seq 3"):
        apply_delta(state, conn)
    assert state_rows(state) == BASE


# ---- replay


def workload() -> list[Writer]:
    """A day's changes, each step one row, in an order that a run could write them.

    It uses every kind of row and every kind of event, and it touches the same project
    again after other projects (`demo` three times, and `other` three times). Each step
    fits the state that the steps before it leave from BASE.
    """
    return [
        lambda d: d.event(Event("fresh", 1, "created")),  # 1
        lambda d: d.file("fresh", 1, file_info("fresh-1.0.tar.gz"), exact=True),
        lambda d: d.file(
            "fresh", 1, file_info("fresh-1.0-py3-none-any.whl"), exact=True
        ),
        lambda d: d.version("fresh", 1, "1.0", record=True),
        lambda d: d.record(record_of("fresh", 1, "1.0")),  # 5
        lambda d: d.swept("fresh", 1, "active"),
        lambda d: d.serial("fresh", 9),
        lambda d: d.event(
            Event(
                "demo", 1, "yanked", filename="demo-1.0.tar.gz", detail="broken build"
            )
        ),
        lambda d: d.event(Event("demo", 1, "unyanked", filename="demo-1.1.tar.gz")),
        lambda d: d.event(
            Event("other", 1, "file_removed", filename="other-2.0.tar.gz")
        ),  # 10
        lambda d: d.file(
            "other", 1, file_info("other-2.1.tar.gz", version="2.1"), exact=True
        ),
        lambda d: d.version("other", 1, "2.1", record=True),
        lambda d: d.version("other", 1, "2.0", record=False),  # a version seen before
        lambda d: d.event(Event("demo", 1, "quarantined")),
        lambda d: d.event(Event("demo", 1, "unquarantined")),  # 15
        lambda d: d.event(Event("demo", 1, "status_changed", detail="archived")),
        lambda d: d.serial("demo", 11),
        lambda d: d.event(Event("other", 1, "removed")),
        lambda d: d.event(Event("gone", 2, "reregistered")),
        lambda d: d.file(
            "gone", 2, file_info("gone-0.2.tar.gz", version="0.2"), exact=True
        ),  # 20
        lambda d: d.swept("gone", 2, "active"),
        lambda d: d.serial("gone", 12),
        lambda d: d.event(Event("fresh", 1, "removed")),
        lambda d: d.cursor(2000, "2026-11-01T06:00:09Z"),
    ]


# The state that the whole workload leaves, which a test can check by hand.
AFTER_WORKLOAD: Rows = {
    "meta": [("applied_seq", "0"), ("bootstrap_at", FIRST_SEEN), ("cursor", "2000")],
    "projects": [
        project("demo", serial=11, status="archived"),
        project("fresh", serial=9, status="removed", first_seen=OBSERVED_AT, exact=1),
        project("gone", incarnation=2, serial=12, first_seen=OBSERVED_AT, exact=1),
        project("other", status="removed"),
    ],
    "files_current": [
        ("demo", 1, "demo-1.0.tar.gz", 1, "broken build"),
        ("demo", 1, "demo-1.1.tar.gz", 0, None),
        ("gone", 2, "gone-0.2.tar.gz", 0, None),
    ],
    "versions_seen": [
        ("demo", 1, "1.0", 1),
        ("demo", 1, "1.1", 0),
        ("fresh", 1, "1.0", 1),
        ("gone", 1, "0.1", 1),
        ("other", 1, "2.0", 1),
        ("other", 1, "2.1", 1),
    ],
}


def test_the_workload_gives_the_state_worked_out_by_hand(tmp_path):
    state = base_copy(tmp_path, "state.db")

    def write(delta: Delta) -> None:
        for step in workload():
            step(delta)

    assert apply_rows(state, tmp_path, write) == len(workload())
    assert state_rows(state) == AFTER_WORKLOAD


def in_one_pass_and_in_two(
    tmp_path: Path, split: int
) -> tuple[sqlite3.Connection, sqlite3.Connection, int, int]:
    """Apply the workload to a copy of the base state in one pass, and to another copy
    in two passes. The delta of the two passes holds the first `split` rows at the first
    pass and the rest at the second, which starts after what the first returned, as the
    runs of a day do. Return the two states, and what each pass of the second returned.
    """
    steps = workload()
    one_pass = base_copy(tmp_path, "one-pass.db")
    whole_conn, whole = new_delta(tmp_path, "whole.db")
    for step in steps:
        step(whole)
    whole_conn.commit()
    apply_delta(one_pass, whole_conn)

    two_passes = base_copy(tmp_path, "two-passes.db")
    conn, delta = new_delta(tmp_path, "growing.db")
    for step in steps[:split]:
        step(delta)
    conn.commit()
    first = apply_delta(two_passes, conn)
    for step in steps[split:]:
        step(delta)
    conn.commit()
    second = apply_delta(two_passes, conn, after_seq=first)
    return one_pass, two_passes, first, second


def test_replay_equals_incremental(tmp_path):
    # One delta applied in two passes, with the second pass starting after what the
    # first returned, gives the state of one pass over a copy of the same base.
    one_pass, two_passes, first, second = in_one_pass_and_in_two(tmp_path, 12)
    assert (first, second) == (12, len(workload()))
    assert state_rows(two_passes) == state_rows(one_pass) == AFTER_WORKLOAD


@pytest.mark.parametrize("split", range(len(workload()) + 1))
def test_replay_equals_incremental_at_every_split(tmp_path, split):
    # The same, with the cut at every row of the delta: before any, between each two
    # rows and after the last. A row never applies twice and none is missed.
    one_pass, two_passes, first, second = in_one_pass_and_in_two(tmp_path, split)
    assert (first, second) == (split, len(workload()))
    assert state_rows(two_passes) == state_rows(one_pass)


def test_a_restore_from_the_base_and_whole_deltas_equals_the_runs(tmp_path):
    steps = workload()
    live = base_copy(tmp_path, "live.db")
    applied = 0

    # Day one has two runs that add to one open delta. Day two starts a new delta that
    # goes on from what the state has applied.
    conn, delta = new_delta(tmp_path, "day-1.db", first_seq=applied + 1)
    for run in (steps[:6], steps[6:12]):
        for step in run:
            step(delta)
        conn.commit()
        applied = apply_delta(live, conn, after_seq=applied)
        store_applied(live, applied)
    conn.close()
    conn, delta = new_delta(tmp_path, "day-2.db", first_seq=applied + 1)
    for step in steps[12:]:
        step(delta)
    conn.commit()
    day_two = [
        seq
        for table in DELTA_COLUMNS
        for (seq,) in conn.execute(f"SELECT seq FROM {table}")
    ]
    assert min(day_two) == 13  # the row after the 12 that day one applied
    assert sorted(day_two) == list(range(13, len(steps) + 1))
    applied = apply_delta(live, conn, after_seq=applied)
    store_applied(live, applied)
    conn.close()

    # A restore starts from the base and applies each day's whole delta, in order.
    restored = base_copy(tmp_path, "restored.db")
    seq = 0
    for day in ("day-1.db", "day-2.db"):
        with closing(open_db(tmp_path / day, DELTA_SCHEMA)) as sealed:
            seq = apply_delta(restored, sealed, after_seq=seq)
        store_applied(restored, seq)

    assert seq == applied == len(steps)
    assert state_rows(restored) == state_rows(live)


# ---- state_rows


def test_state_rows_lists_every_table_sorted(tmp_path):
    rows: Rows = {
        "meta": [("cursor", "7"), ("applied_seq", "0")],
        "projects": [project("b"), project("a")],
        "files_current": [
            ("b", 1, "b-1.tar.gz", 0, None),
            ("a", 2, "a-1.tar.gz", 1, "x"),
            ("a", 1, "a-2.tar.gz", 0, None),
            ("a", 1, "a-1.tar.gz", 0, None),
        ],
        "versions_seen": [("b", 1, "2", 1), ("a", 1, "10", 0), ("a", 1, "2", 1)],
    }
    state = seeded_state(tmp_path, "state.db", rows)
    found = state_rows(state)
    assert list(found) == ["meta", "projects", "files_current", "versions_seen"]
    assert found == {table: sorted(table_rows) for table, table_rows in rows.items()}
    assert all(
        type(row) is tuple for table_rows in found.values() for row in table_rows
    )


def test_state_rows_of_an_empty_state(tmp_path):
    state = opened(open_db(tmp_path / "state.db", STATE_SCHEMA, create=True))
    assert state_rows(state) == {
        "meta": [],
        "projects": [],
        "files_current": [],
        "versions_seen": [],
    }


# ---- diff_states


def run_sql(path: Path, sql: str) -> None:
    """Change a database file that nothing has open."""
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(sql)
        conn.commit()


# One change to a single value of a single row, for each table. The last two are the
# changes that a comparison of values could miss: a null that gets a value, and a
# number that becomes the text of that number.
CHANGES = {
    "a value of meta": (
        "meta",
        "UPDATE meta SET value = '101' WHERE key = 'cursor'",
    ),
    "a serial": (
        "projects",
        "UPDATE projects SET last_serial = 6 WHERE name = 'demo'",
    ),
    "a status": (
        "projects",
        "UPDATE projects SET status = 'archived' WHERE name = 'other'",
    ),
    "a yank": (
        "files_current",
        "UPDATE files_current SET yanked = 1 WHERE filename = 'demo-1.0.tar.gz'",
    ),
    "a null that gets a value": (
        "files_current",
        "UPDATE files_current SET yanked_reason = 'x' "
        "WHERE filename = 'other-2.0.tar.gz'",
    ),
    "a number that becomes its text": (
        "files_current",
        "UPDATE files_current SET incarnation = '1' "
        "WHERE filename = 'other-2.0.tar.gz'",
    ),
    "a record flag": (
        "versions_seen",
        "UPDATE versions_seen SET record = 0 WHERE name = 'demo' AND version = '1.0'",
    ),
}


# The comparison is tested with `diff_tables`, in the kit. This runs `diff_states` over
# the tables of the state itself, so that a change in any of its four is named.
@pytest.mark.parametrize("label", CHANGES)
def test_diff_states_names_the_table_that_has_a_changed_value(tmp_path, label):
    table, change = CHANGES[label]
    a = base_copy(tmp_path, "a.db")
    b = base_file(tmp_path, "b.db")
    run_sql(b, change)
    assert diff_states(a, b) == [table]


# ---- the deltas of a month merge


def write_day(delta: Delta, tag: str) -> None:
    """One row in each table of a delta, with keys that no other tag shares."""
    name = f"p-{tag}"
    delta.event(Event(name, 1, "created"))
    delta.file(name, 1, file_info(f"{name}-1.0.tar.gz"), exact=True)
    delta.version(name, 1, "1.0", record=True)
    delta.record(record_of(name, 1, "1.0"))
    delta.serial(name, 7)
    delta.swept(name, 1, "active")
    delta.cursor(100, OBSERVED_AT)


def test_deltas_that_continue_the_numbering_merge_into_a_month(tmp_path):
    state = base_copy(tmp_path, "state.db")
    first_conn, first = new_delta(tmp_path, "day-1.db")
    write_day(first, "a")
    first_conn.commit()
    applied = apply_delta(state, first_conn)
    first_conn.close()
    # The next day's delta starts after what the state has applied.
    second_conn, second = new_delta(tmp_path, "day-2.db", first_seq=applied + 1)
    write_day(second, "b")
    second_conn.commit()
    second_conn.close()

    counts = consolidate(
        [tmp_path / "day-1.db", tmp_path / "day-2.db"], tmp_path / "m.db"
    )
    assert counts == dict.fromkeys(DELTA_COLUMNS, 2)


def test_deltas_that_each_start_at_one_cannot_be_merged(tmp_path):
    # This is why the numbering goes on from one delta to the next: two deltas that
    # both start at 1 repeat a seq, and the merge refuses the repeat.
    for tag in ("a", "b"):
        conn, delta = new_delta(tmp_path, f"day-{tag}.db")
        write_day(delta, tag)
        conn.commit()
        conn.close()
    inputs = [tmp_path / "day-a.db", tmp_path / "day-b.db"]
    with pytest.raises(sqlite3.IntegrityError, match="seq"):
        consolidate(inputs, tmp_path / "m.db")
