import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from obskit.sqlite import consolidate, table_counts

SCHEMA = "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)"

# A delta-like schema: a WITHOUT ROWID table with a composite key, integer-keyed
# tables and an explicit index.
DELTA_LIKE = """
CREATE TABLE files (seq, name, filename, PRIMARY KEY (name, filename)) WITHOUT ROWID;
CREATE TABLE events (seq INTEGER PRIMARY KEY, name, kind);
CREATE TABLE cursors (seq INTEGER PRIMARY KEY, serial);
CREATE INDEX events_by_name ON events (name);
"""


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
