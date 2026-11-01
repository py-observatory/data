import shutil
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

import pytest

from obskit.sqlite import consolidate, diff_tables, open_db, table_counts

SCHEMA = "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)"

# A delta-like schema: a WITHOUT ROWID table with a composite key, integer-keyed
# tables and an explicit index.
DELTA_LIKE = """
CREATE TABLE files (seq, name, filename, PRIMARY KEY (name, filename)) WITHOUT ROWID;
CREATE TABLE events (seq INTEGER PRIMARY KEY, name, kind);
CREATE TABLE cursors (seq INTEGER PRIMARY KEY, serial);
CREATE INDEX events_by_name ON events (name);
"""
DELTA_TABLES = ("files", "events", "cursors")

# The tables of a stream's state: WITHOUT ROWID, keyed by one column or by several, with
# columns that have no declared type and a column that may be null. A stream gives their
# names to `diff_tables`, as `STATE_TABLES` does here.
STATE_LIKE = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE projects (
    name TEXT PRIMARY KEY, last_serial INTEGER NOT NULL, status TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE files_current (
    name, incarnation, filename, yanked INTEGER NOT NULL, yanked_reason,
    PRIMARY KEY (name, incarnation, filename)
) WITHOUT ROWID;
CREATE TABLE versions_seen (
    name, incarnation, version, record INTEGER NOT NULL,
    PRIMARY KEY (name, incarnation, version)
) WITHOUT ROWID;
"""
STATE_TABLES = ("meta", "projects", "files_current", "versions_seen")


def make_db(path: Path, schema: str, **rows: list[tuple]) -> Path:
    """Create a database from `schema`, then fill each table named in `rows`."""
    with closing(sqlite3.connect(path)) as con:
        con.executescript(schema)
        for table, values in rows.items():
            marks = ", ".join("?" * len(values[0]))
            con.executemany(f"INSERT INTO {table} VALUES ({marks})", values)
        con.commit()
    return path


def schema_sql(path: Path) -> list[str]:
    with closing(sqlite3.connect(path)) as con:
        return sorted(
            sql
            for (sql,) in con.execute(
                "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL"
            )
        )


def page_count(path: Path) -> int:
    with closing(sqlite3.connect(path)) as con:
        return con.execute("PRAGMA page_count").fetchone()[0]


def scrambled_rows(start: int, stop: int) -> list[tuple]:
    """Rows in key order whose `v` values come in a scrambled order."""
    return [(k, f"{k * 7919 % 100003:06d}-{'y' * 20}") for k in range(start, stop)]


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


def test_table_counts(tmp_path):
    db = make_db(
        tmp_path / "x.db",
        "CREATE TABLE a (n); CREATE TABLE b (n);",
        a=[(1,), (2,), (3,)],
    )

    assert table_counts(db) == {"a": 3, "b": 0}


def test_table_counts_skips_internal_tables(tmp_path):
    db = make_db(
        tmp_path / "x.db",
        "CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT, n);"
        "CREATE TABLE sqliteish (n);",
        a=[(None, 1), (None, 2)],
        sqliteish=[(1,)],
    )
    with closing(sqlite3.connect(db)) as con:
        names = [n for (n,) in con.execute("SELECT name FROM sqlite_master")]
    assert "sqlite_sequence" in names, "AUTOINCREMENT should have created it"

    assert table_counts(db) == {"a": 2, "sqliteish": 1}


@pytest.mark.parametrize("name", ["plain.db", "odd name #1?%.db"])
def test_table_counts_takes_relative_and_awkward_paths(tmp_path, monkeypatch, name):
    monkeypatch.chdir(tmp_path)
    db = make_db(Path(name), "CREATE TABLE a (n);", a=[(1,)])

    assert table_counts(db) == {"a": 1}


def test_table_counts_of_a_missing_db_is_an_error(tmp_path):
    missing = tmp_path / "missing.db"

    with pytest.raises(FileNotFoundError, match="missing.db"):
        table_counts(missing)

    assert not missing.exists()


def test_consolidate_sums(tmp_path):
    a = make_db(tmp_path / "a.db", SCHEMA, t=[(1, "a1"), (2, "a2")])
    b = make_db(tmp_path / "b.db", SCHEMA, t=[(3, "b3"), (4, "b4"), (5, "b5")])
    out = tmp_path / "out.db"

    assert consolidate([a, b], out) == {"t": 5}

    assert table_counts(out) == {"t": 5}
    assert schema_sql(out) == schema_sql(a)
    with closing(sqlite3.connect(out)) as con:
        rows = con.execute("SELECT k, v FROM t ORDER BY k").fetchall()
    assert rows == [(1, "a1"), (2, "a2"), (3, "b3"), (4, "b4"), (5, "b5")]


def test_consolidate_merges_every_table_and_copies_indexes(tmp_path):
    a = make_db(
        tmp_path / "a.db",
        DELTA_LIKE,
        files=[(1, "p", "p-1.whl")],
        events=[(2, "p", "created")],
    )
    b = make_db(
        tmp_path / "b.db",
        DELTA_LIKE,
        files=[(3, "q", "q-1.whl"), (4, "q", "q-2.whl")],
        cursors=[(5, 100)],
    )
    out = tmp_path / "out.db"

    assert consolidate([a, b], out) == {"cursors": 1, "events": 1, "files": 3}

    assert schema_sql(out) == schema_sql(a)
    with closing(sqlite3.connect(out)) as con:
        assert con.execute("SELECT seq FROM files ORDER BY seq").fetchall() == [
            (1,),
            (3,),
            (4,),
        ]
        indexes = con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL"
        ).fetchall()
    assert indexes == [("events_by_name",)]


def test_consolidate_refuses_duplicate_keys(tmp_path):
    a = make_db(tmp_path / "a.db", SCHEMA, t=[(1, "x"), (2, "y")])
    b = make_db(tmp_path / "b.db", SCHEMA, t=[(2, "z"), (3, "w")])
    out = tmp_path / "out.db"

    with pytest.raises(sqlite3.IntegrityError):
        consolidate([a, b], out)

    assert list(tmp_path.glob("out.db*")) == []


def test_consolidate_refuses_schema_mismatch(tmp_path):
    a = make_db(tmp_path / "a.db", SCHEMA, t=[(1, "x")])
    b = make_db(
        tmp_path / "b.db",
        "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT, extra TEXT)",
        t=[(2, "y", "z")],
    )
    out = tmp_path / "out.db"

    with pytest.raises(ValueError, match="different schema"):
        consolidate([a, b], out)

    assert not out.exists()


def test_consolidate_refuses_existing_dst(tmp_path):
    a = make_db(tmp_path / "a.db", SCHEMA, t=[(1, "x")])
    out = tmp_path / "out.db"
    out.write_bytes(b"not to be touched")

    with pytest.raises(FileExistsError):
        consolidate([a], out)

    assert out.read_bytes() == b"not to be touched"


def test_consolidate_checks_counts(tmp_path):
    # ON CONFLICT IGNORE makes a plain INSERT drop a duplicate row without an error.
    schema = "CREATE TABLE t (k INTEGER PRIMARY KEY ON CONFLICT IGNORE, v TEXT)"
    a = make_db(tmp_path / "a.db", schema, t=[(1, "x")])
    b = make_db(tmp_path / "b.db", schema, t=[(1, "y")])
    out = tmp_path / "out.db"

    with pytest.raises(sqlite3.IntegrityError, match=r"t: 1 rows, expected 2"):
        consolidate([a, b], out)

    assert list(tmp_path.glob("out.db*")) == []


def test_consolidate_needs_an_input(tmp_path):
    out = tmp_path / "out.db"

    with pytest.raises(ValueError, match="at least one"):
        consolidate([], out)

    assert not out.exists()


def test_consolidate_of_a_missing_input_creates_nothing(tmp_path):
    a = make_db(tmp_path / "a.db", SCHEMA, t=[(1, "x")])
    missing = tmp_path / "missing.db"
    out = tmp_path / "out.db"

    with pytest.raises(FileNotFoundError, match="missing.db"):
        consolidate([a, missing], out)

    assert not missing.exists()
    assert not out.exists()


def test_consolidate_leaves_internal_tables_to_sqlite(tmp_path):
    schema = "CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT, n)"
    a = make_db(tmp_path / "a.db", schema, a=[(1, "x")])
    b = make_db(tmp_path / "b.db", schema, a=[(2, "y")])

    assert consolidate([a, b], tmp_path / "out.db") == {"a": 2}


def test_consolidate_vacuums_the_result(tmp_path):
    # Rows go in by key, which scatters the entries of this index across its pages.
    # The merged file keeps that slack until VACUUM packs it.
    schema = SCHEMA + "; CREATE INDEX t_v ON t (v)"
    a = make_db(tmp_path / "a.db", schema, t=scrambled_rows(0, 300))
    b = make_db(tmp_path / "b.db", schema, t=scrambled_rows(300, 600))
    out = tmp_path / "out.db"

    consolidate([a, b], out)

    packed = tmp_path / "packed.db"
    shutil.copy(out, packed)
    with closing(sqlite3.connect(packed, autocommit=True)) as con:
        con.execute("VACUUM")
    assert page_count(out) == page_count(packed)


def test_consolidate_keeps_its_inputs_by_default(tmp_path):
    a = make_db(tmp_path / "a.db", SCHEMA, t=[(1, "a1")])
    b = make_db(tmp_path / "b.db", SCHEMA, t=[(2, "b2")])
    before = {path: path.read_bytes() for path in (a, b)}

    consolidate([a, b], tmp_path / "out.db")

    assert {path: path.read_bytes() for path in (a, b)} == before


def test_consolidate_can_remove_each_input_once_merged(tmp_path):
    inputs = [
        make_db(tmp_path / f"day-{day}.db", SCHEMA, t=[(day, "a"), (day + 3, "b")])
        for day in range(3)
    ]
    out = tmp_path / "out.db"

    assert consolidate(inputs, out, remove_inputs=True) == {"t": 6}

    assert list(tmp_path.iterdir()) == [out]
    with closing(sqlite3.connect(out)) as con:
        assert con.execute("SELECT k FROM t ORDER BY k").fetchall() == [
            (k,) for k in range(6)
        ]


def test_consolidate_removes_only_the_inputs_merged_before_a_failure(tmp_path):
    a = make_db(tmp_path / "a.db", SCHEMA, t=[(1, "x"), (2, "y")])
    b = make_db(tmp_path / "b.db", SCHEMA, t=[(2, "z"), (3, "w")])
    out = tmp_path / "out.db"

    with pytest.raises(sqlite3.IntegrityError):
        consolidate([a, b], out, remove_inputs=True)

    # a was merged, and is gone for good: whoever retries fetches it again. b's merge
    # failed, so b is left. The result is removed, as after any failure.
    assert not a.exists()
    assert b.exists()
    assert list(tmp_path.glob("out.db*")) == []


def test_consolidate_checks_counts_when_it_removes_inputs(tmp_path):
    # Both inputs merge without an error and are removed, and the check of the
    # counts still has what they held, because it counts them before removing any.
    schema = "CREATE TABLE t (k INTEGER PRIMARY KEY ON CONFLICT IGNORE, v TEXT)"
    a = make_db(tmp_path / "a.db", schema, t=[(1, "x")])
    b = make_db(tmp_path / "b.db", schema, t=[(1, "y")])
    out = tmp_path / "out.db"

    with pytest.raises(sqlite3.IntegrityError, match=r"t: 1 rows, expected 2"):
        consolidate([a, b], out, remove_inputs=True)

    assert list(tmp_path.iterdir()) == []


def test_consolidate_a_month_of_inputs(tmp_path):
    # More inputs than SQLite lets one connection attach at once (10 by default).
    inputs = [
        make_db(
            tmp_path / f"day-{day:02d}.db",
            SCHEMA,
            t=[(2 * day, "a"), (2 * day + 1, "b")],
        )
        for day in range(31)
    ]

    assert consolidate(inputs, tmp_path / "out.db") == {"t": 62}


# ---- open_db


def table_names(conn: sqlite3.Connection) -> list[str]:
    """The names of the tables of the database, sorted."""
    return sorted(
        name
        for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    )


def test_open_db_never_creates_silently(tmp_path):
    missing = tmp_path / "state.db"
    # A missing file is an error, and the file is not made. It does not become an empty
    # database.
    with pytest.raises(FileNotFoundError, match="state.db"):
        open_db(missing, STATE_LIKE)
    with pytest.raises(FileNotFoundError, match="state.db"):
        open_db(missing, STATE_LIKE, create=False)
    with pytest.raises(FileNotFoundError):
        open_db(tmp_path / "no-such-folder" / "state.db", STATE_LIKE)
    assert list(tmp_path.iterdir()) == []

    # A new database needs a path that is free, and a file that is there is left as it
    # was.
    present = tmp_path / "present.db"
    present.write_bytes(b"not to be touched")
    with pytest.raises(FileExistsError, match="present.db"):
        open_db(present, STATE_LIKE, create=True)
    assert present.read_bytes() == b"not to be touched"

    # Nor is a missing folder made for a new database.
    with pytest.raises(FileNotFoundError):
        open_db(tmp_path / "no-such-folder" / "state.db", STATE_LIKE, create=True)
    assert [path.name for path in tmp_path.iterdir()] == ["present.db"]


def test_sqlite_is_told_not_to_make_the_file_either(tmp_path, monkeypatch):
    # The check that the file is there can pass for a file that is gone a moment later.
    # With that check taken away, SQLite itself still refuses to make a missing file.
    monkeypatch.setattr(Path, "resolve", lambda self, strict=False: self.absolute())
    with pytest.raises(sqlite3.OperationalError):
        open_db(tmp_path / "state.db", STATE_LIKE)
    assert list(tmp_path.iterdir()) == []


def test_open_db_makes_the_schema_and_opens_it_again(tmp_path):
    path = tmp_path / "state.db"
    with closing(open_db(path, STATE_LIKE, create=True)) as state:
        assert table_names(state) == sorted(STATE_TABLES)
        state.execute("INSERT INTO meta VALUES ('cursor', '7')")
        state.commit()
    # Opened again, it holds what was written, and it can be written to.
    with closing(open_db(path, STATE_LIKE)) as state:
        assert state.execute("SELECT * FROM meta").fetchall() == [("cursor", "7")]
        state.execute("INSERT INTO meta VALUES ('applied_seq', '0')")
        state.commit()
    with closing(open_db(path, STATE_LIKE)) as state:
        rows = state.execute("SELECT * FROM meta ORDER BY key").fetchall()
        assert rows == [("applied_seq", "0"), ("cursor", "7")]


@pytest.mark.parametrize("name", ["plain.db", "odd name #1?%.db", "ünï.db"])
def test_open_db_takes_awkward_paths(tmp_path, name):
    # The path goes into a URI, so a `?`, `#` or `%` in it must not be read as one.
    with closing(open_db(tmp_path / name, STATE_LIKE, create=True)) as state:
        state.execute("INSERT INTO meta VALUES ('cursor', '7')")
        state.commit()
    with closing(open_db(tmp_path / name, STATE_LIKE)) as state:
        assert state.execute("SELECT value FROM meta").fetchall() == [("7",)]
    assert [path.name for path in tmp_path.iterdir()] == [name]


def test_open_db_takes_a_relative_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with closing(open_db(Path("state.db"), STATE_LIKE, create=True)):
        pass
    with closing(open_db(Path("state.db"), STATE_LIKE)) as state:
        assert table_names(state) == sorted(STATE_TABLES)


def make_file(path: Path, kind: str) -> None:
    """A file of one of four kinds: `empty` (zero bytes), `delta`, `state`, and `state
    without a table`, which lacks `versions_seen`."""
    if kind == "empty":
        path.touch()  # SQLite reads a file of zero bytes as a new, empty database
        return
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript(DELTA_LIKE if kind == "delta" else STATE_LIKE)
        if kind == "state without a table":
            conn.execute("DROP TABLE versions_seen")
        conn.commit()


@pytest.mark.parametrize(
    ("kind", "schema", "missing"),
    [
        ("empty", STATE_LIKE, sorted(STATE_TABLES)),
        ("empty", DELTA_LIKE, sorted(DELTA_TABLES)),
        ("delta", STATE_LIKE, sorted(STATE_TABLES)),
        ("state", DELTA_LIKE, sorted(DELTA_TABLES)),
        ("state without a table", STATE_LIKE, ["versions_seen"]),
    ],
    ids=[
        "empty file as a state",
        "empty file as a delta",
        "delta as a state",
        "state as a delta",
        "state with a table gone",
    ],
)
def test_open_db_refuses_a_file_that_is_not_of_the_schema(
    tmp_path, kind, schema, missing
):
    # A file that is there but lacks tables of the schema must not open as an empty
    # database, which is what SQLite makes of a file of zero bytes. It is refused, and
    # the error ends with the tables that are missing, and only those.
    path = tmp_path / "state.db"
    make_file(path, kind)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="state.db") as caught:
        open_db(path, schema)
    assert str(caught.value).endswith(f"it lacks {', '.join(missing)}")
    # Nothing was written to the file, and nothing was made beside it.
    assert path.read_bytes() == before
    assert [p.name for p in tmp_path.iterdir()] == ["state.db"]


def test_open_db_opens_a_database_that_has_more_tables(tmp_path):
    # Only the tables of the schema have to be there.
    path = tmp_path / "state.db"
    make_file(path, "state")
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE extra (x)")
        conn.commit()
    with closing(open_db(path, STATE_LIKE)) as state:
        assert table_names(state) == sorted([*STATE_TABLES, "extra"])


@pytest.mark.parametrize(
    "schema",
    ["CREATE TABLE (", "CREATE TABLE a (x); CREATE TABLE a (y);"],
    ids=["bad syntax", "fails after the first table"],
)
def test_a_failed_create_leaves_no_file(tmp_path, schema):
    with pytest.raises(sqlite3.Error):
        open_db(tmp_path / "bad.db", schema, create=True)
    assert list(tmp_path.iterdir()) == []


def test_open_db_closes_every_connection_it_made_when_it_refuses(tmp_path, monkeypatch):
    # A refusal must not leave a connection open on the file, whether the file lacks
    # the tables of the schema or the schema of a new file does not run.
    made: list[sqlite3.Connection] = []
    connect = sqlite3.connect

    def watching(*args, **kwargs):
        conn = connect(*args, **kwargs)
        made.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, "connect", watching)
    (tmp_path / "empty.db").touch()
    with pytest.raises(ValueError, match="lacks"):
        open_db(tmp_path / "empty.db", STATE_LIKE)
    with pytest.raises(sqlite3.Error):
        open_db(tmp_path / "bad.db", "CREATE TABLE (", create=True)
    assert made
    for conn in made:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            conn.execute("SELECT 1")


# ---- diff_tables


# The state that these tests start from, as a run leaves it: `demo` has a file that is
# yanked, `other` is a second project that a test of one project should leave alone, and
# `gone` was removed.
BASE: dict[str, list[tuple]] = {
    "meta": [("applied_seq", "0"), ("cursor", "100")],
    "projects": [("demo", 5, "active"), ("gone", 5, "removed"), ("other", 5, "active")],
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


def base_file(tmp_path: Path, name: str) -> Path:
    """A copy of the base state as a file that nothing has open."""
    return make_db(tmp_path / name, STATE_LIKE, **BASE)


def base_copy(tmp_path: Path, name: str) -> sqlite3.Connection:
    """An open copy of the base state, so that a test can change it as it likes."""
    return opened(open_db(base_file(tmp_path, name), STATE_LIKE))


def rows_of_state(conn: sqlite3.Connection) -> dict[str, list[tuple]]:
    """Every row of the four tables of the state, each table's rows sorted."""
    return {
        table: sorted(tuple(row) for row in conn.execute(f"SELECT * FROM {table}"))
        for table in STATE_TABLES
    }


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
# One row taken out of each table.
REMOVALS = {
    "meta": "DELETE FROM meta WHERE key = 'cursor'",
    "projects": "DELETE FROM projects WHERE name = 'other'",
    "files_current": "DELETE FROM files_current WHERE filename = 'demo-1.1.tar.gz'",
    "versions_seen": "DELETE FROM versions_seen WHERE name = 'gone'",
}


def attached(conn: sqlite3.Connection) -> list[str]:
    """The names of the databases that `conn` has open."""
    return [row[1] for row in conn.execute("PRAGMA database_list")]


@pytest.mark.parametrize("name", ["b.db", "odd name #1?%.db"])
def test_diff_tables_of_equal_databases_is_empty(tmp_path, name):
    a = base_copy(tmp_path, "a.db")
    assert diff_tables(a, base_file(tmp_path, name), STATE_TABLES) == []
    # The two are equal whatever is in them, and that includes nothing at all.
    empty = opened(open_db(make_db(tmp_path / "empty-a.db", STATE_LIKE), STATE_LIKE))
    make_db(tmp_path / "empty-b.db", STATE_LIKE)
    assert diff_tables(empty, tmp_path / "empty-b.db", STATE_TABLES) == []


@pytest.mark.parametrize("label", CHANGES)
def test_diff_tables_names_the_table_that_has_a_changed_value(tmp_path, label):
    table, change = CHANGES[label]
    a = base_copy(tmp_path, "a.db")
    b = base_file(tmp_path, "b.db")
    run_sql(b, change)
    assert diff_tables(a, b, STATE_TABLES) == [table]


@pytest.mark.parametrize("table", REMOVALS)
@pytest.mark.parametrize("side", ["b lacks a row of a", "a lacks a row of b"])
def test_diff_tables_sees_a_row_that_is_missing_on_either_side(tmp_path, table, side):
    a = base_copy(tmp_path, "a.db")
    b = base_file(tmp_path, "b.db")
    if side == "b lacks a row of a":
        run_sql(b, REMOVALS[table])
    else:
        a.execute(REMOVALS[table])
        a.commit()
    assert diff_tables(a, b, STATE_TABLES) == [table]


def test_diff_tables_sees_a_table_in_which_many_rows_differ(tmp_path):
    a = base_copy(tmp_path, "a.db")
    b = base_file(tmp_path, "b.db")
    run_sql(b, "UPDATE files_current SET yanked = 1 - yanked")  # all three rows
    assert diff_tables(a, b, STATE_TABLES) == ["files_current"]


def test_diff_tables_lists_the_tables_that_differ_sorted(tmp_path):
    a = base_copy(tmp_path, "a.db")
    b = base_file(tmp_path, "b.db")
    for label in ("a value of meta", "a serial", "a record flag"):
        run_sql(b, CHANGES[label][1])
    # Three tables, found in the order of the schema, and listed by name.
    assert diff_tables(a, b, STATE_TABLES) == ["meta", "projects", "versions_seen"]
    run_sql(b, CHANGES["a yank"][1])
    assert diff_tables(a, b, STATE_TABLES) == [
        "files_current",
        "meta",
        "projects",
        "versions_seen",
    ]


def test_diff_tables_compares_the_tables_it_is_given(tmp_path):
    a = base_copy(tmp_path, "a.db")
    b = base_file(tmp_path, "b.db")
    for label in ("a value of meta", "a serial"):
        run_sql(b, CHANGES[label][1])
    # A table that is not given goes unseen, and each one that is given is named when it
    # differs.
    assert diff_tables(a, b, ["meta"]) == ["meta"]
    assert diff_tables(a, b, ["projects", "versions_seen"]) == ["projects"]
    assert diff_tables(a, b, ["files_current", "versions_seen"]) == []
    assert diff_tables(a, b, []) == []
    # Any iterable will do, and the names come back sorted whatever order they came in.
    assert diff_tables(a, b, (name for name in ("projects", "meta"))) == [
        "meta",
        "projects",
    ]
    assert attached(a) == ["main"]


def test_diff_tables_leaves_both_databases_as_they_were(tmp_path):
    a = base_copy(tmp_path, "a.db")
    b = base_file(tmp_path, "b.db")
    run_sql(b, CHANGES["a serial"][1])
    before = b.read_bytes()
    assert diff_tables(a, b, STATE_TABLES) == ["projects"]
    # Nothing is left attached, nothing was written, and `a` goes on as it was.
    assert attached(a) == ["main"]
    assert b.read_bytes() == before
    assert not a.in_transaction
    assert rows_of_state(a) == BASE
    assert diff_tables(a, b, STATE_TABLES) == ["projects"]
    a.execute("UPDATE meta SET value = '7' WHERE key = 'cursor'")
    a.commit()
    assert diff_tables(a, b, STATE_TABLES) == ["meta", "projects"]


def test_diff_tables_does_not_load_rows_into_python(tmp_path):
    # The tables are compared in SQL, and only a count comes back for each comparison.
    # Each one is written as `ORDER BY 1 LIMIT 1` so that SQLite merges the two tables
    # as it reads them in key order, and stops at the first row that differs. SQLite
    # 3.49 and 3.50 build a temporary b-tree of a whole table for a plainer form of the
    # same query, which for millions of rows is slow and never stops early.
    a = base_copy(tmp_path, "a.db")
    b = base_file(tmp_path, "b.db")
    statements: list[str] = []
    a.set_trace_callback(statements.append)
    assert diff_tables(a, b, STATE_TABLES) == []
    a.set_trace_callback(None)
    queries = [s for s in statements if s.startswith("SELECT")]
    assert queries
    for query in queries:
        assert query.startswith("SELECT count(*) FROM (SELECT * FROM")
        assert query.endswith(" ORDER BY 1 LIMIT 1)")
    a.execute("ATTACH DATABASE ? AS compared", (str(b),))
    for query in queries:
        plan = [row[3] for row in a.execute("EXPLAIN QUERY PLAN " + query)]
        assert not any("TEMP B-TREE" in line for line in plan), (query, plan)
    a.execute("DETACH DATABASE compared")


def test_diff_tables_needs_a_connection_with_no_transaction_open(tmp_path):
    # SQLite attaches a database inside a transaction, but then cannot detach it, so
    # the other database would stay attached to the caller's connection. A write that is
    # not committed yet is an error, nothing is attached, and the comparison does not
    # commit the write for the caller.
    a = base_copy(tmp_path, "a.db")
    b = base_file(tmp_path, "b.db")
    a.execute("UPDATE meta SET value = '7' WHERE key = 'cursor'")
    with pytest.raises(ValueError, match="transaction open"):
        diff_tables(a, b, STATE_TABLES)
    assert a.in_transaction
    assert attached(a) == ["main"]
    a.commit()
    assert diff_tables(a, b, STATE_TABLES) == ["meta"]
    assert attached(a) == ["main"]


def test_diff_tables_refuses_a_file_that_is_missing(tmp_path):
    # Attaching a path that is not there would make an empty database, and an empty
    # database would differ from `a` in every table. It must be an error.
    a = base_copy(tmp_path, "a.db")
    before = sorted(p.name for p in tmp_path.iterdir())
    with pytest.raises(FileNotFoundError, match="gone.db"):
        diff_tables(a, tmp_path / "gone.db", STATE_TABLES)
    assert sorted(p.name for p in tmp_path.iterdir()) == before
    assert attached(a) == ["main"]


def test_diff_tables_of_a_file_that_lacks_a_table_is_an_error(tmp_path):
    a = base_copy(tmp_path, "a.db")
    make_file(tmp_path / "delta.db", "delta")
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        diff_tables(a, tmp_path / "delta.db", STATE_TABLES)
    assert attached(a) == ["main"]
    # An empty file is the same: it has none of the tables.
    make_file(tmp_path / "empty.db", "empty")
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        diff_tables(a, tmp_path / "empty.db", STATE_TABLES)
    assert attached(a) == ["main"]
    assert (tmp_path / "empty.db").stat().st_size == 0
