"""SQLite helpers: row counts for the manifest, the monthly merge of deltas, a database
that is opened only where it should be, and the comparison of two databases."""

import sqlite3
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path

# SQLite keeps its own tables under the reserved prefix `sqlite_` (sqlite_sequence,
# sqlite_stat1, ...). The underscore is escaped so that only that prefix matches:
# `sqliteish` is a legal user table.
_USER_OBJECT = r"name NOT LIKE 'sqlite\_%' ESCAPE '\'"


def _read_only_uri(db: Path) -> str:
    """A URI that opens `db` read-only, so a wrong path is a FileNotFoundError and not
    a new, empty database, and an input is never written to."""
    return f"{db.resolve(strict=True).as_uri()}?mode=ro"


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _counts(con: sqlite3.Connection) -> dict[str, int]:
    names = [
        name
        for (name,) in con.execute(
            "SELECT name FROM sqlite_master "
            f"WHERE type = 'table' AND {_USER_OBJECT} ORDER BY name"
        )
    ]
    return {
        name: con.execute(f"SELECT count(*) FROM {_quote(name)}").fetchone()[0]
        for name in names
    }


def table_counts(db: Path) -> dict[str, int]:
    """The row count of each user table in `db`, leaving out the `sqlite_*` tables."""
    with closing(sqlite3.connect(_read_only_uri(db), uri=True)) as con:
        return _counts(con)


def _schema_sql(db: Path) -> list[str]:
    """The CREATE statements of `db` in creation order. The indexes that SQLite makes
    for PRIMARY KEY and UNIQUE have no SQL of their own, so they drop out."""
    with closing(sqlite3.connect(_read_only_uri(db), uri=True)) as con:
        return [
            sql
            for (sql,) in con.execute(
                "SELECT sql FROM sqlite_master "
                f"WHERE sql IS NOT NULL AND {_USER_OBJECT} ORDER BY rowid"
            )
        ]


def consolidate(
    inputs: list[Path], dst: Path, *, remove_inputs: bool = False
) -> dict[str, int]:
    """Merge databases that share one schema into the new database `dst`.

    Returns the row count of each table, which equals its sum over `inputs`. Raises
    FileExistsError if `dst` exists (it is left alone), ValueError for no inputs or
    for schemas that differ, and sqlite3.IntegrityError when a key repeats or the
    merge loses a row. Once the build starts, any failure removes `dst`.

    With `remove_inputs`, each input is deleted as soon as its rows are merged, so
    that the disk never holds every input and the whole result at once. The inputs
    are counted before any is deleted, so the check of the merged counts is the
    same. An input that was deleted stays deleted when a later step fails.
    """
    if not inputs:
        raise ValueError("consolidate needs at least one input database")
    if dst.exists():
        raise FileExistsError(f"{dst} already exists")

    schemas = [_schema_sql(path) for path in inputs]
    for path, schema in zip(inputs[1:], schemas[1:]):
        if set(schema) != set(schemas[0]):
            raise ValueError(f"{path} has a different schema from {inputs[0]}")

    expected: dict[str, int] = {}
    for path in inputs:
        for table, n in table_counts(path).items():
            expected[table] = expected.get(table, 0) + n

    try:
        with closing(sqlite3.connect(dst, autocommit=True, uri=True)) as con:
            for sql in schemas[0]:
                con.execute(sql)
            for path in inputs:
                con.execute("ATTACH DATABASE ? AS src", (_read_only_uri(path),))
                con.execute("BEGIN")
                for table in expected:
                    quoted = _quote(table)
                    con.execute(f"INSERT INTO main.{quoted} SELECT * FROM src.{quoted}")
                con.execute("COMMIT")
                con.execute("DETACH DATABASE src")
                if remove_inputs:
                    path.unlink()
            counts = _counts(con)
            if counts != expected:
                differ = "; ".join(
                    f"{table}: {counts[table]} rows, expected {n}"
                    for table, n in expected.items()
                    if counts[table] != n
                )
                raise sqlite3.IntegrityError(
                    f"merged row counts differ from the inputs ({differ})"
                )
            con.execute("VACUUM")
    except BaseException:
        dst.unlink(missing_ok=True)
        raise
    return counts


def _table_names(conn: sqlite3.Connection) -> set[str]:
    """The names of the tables of a database, leaving out the ones SQLite keeps."""
    return {
        name
        for (name,) in conn.execute(
            "SELECT name FROM sqlite_master "
            r"WHERE type = 'table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\'"
        )
    }


def _require_tables(conn: sqlite3.Connection, path: Path, schema: str) -> None:
    """Raise ValueError unless the database holds every table that `schema` makes."""
    with closing(sqlite3.connect(":memory:")) as scratch:
        scratch.executescript(schema)
        wanted = _table_names(scratch)
    lacking = sorted(wanted - _table_names(conn))
    if lacking:
        raise ValueError(
            f"{path} is not a database of this schema: it lacks {', '.join(lacking)}"
        )


def open_db(path: Path, schema: str, *, create: bool = False) -> sqlite3.Connection:
    """Open the database at `path`, or make it from `schema`.

    With `create` the file must not exist yet. It is made, and `schema` is run in it. If
    that fails, the file is removed. Without `create` the file must exist, and it is
    opened to read and write. It must hold every table of `schema`, and it may hold
    more.

    SQLite is never asked to make a file that is missing, because it would make an empty
    database and say nothing. A state that should be there and is not must be an error,
    and not a new state with no projects in it. A file of zero bytes is the same trap,
    because SQLite reads it as a new, empty database, and so is a database of another
    kind, so neither is opened as one. Raises FileExistsError when `create` finds the
    file, FileNotFoundError when `create` is not set and the file is not there, or when
    the folder is not there, and ValueError when the file lacks a table of `schema`.

    The connection is as SQLite opens it: what is written is in an open transaction
    until it is committed.
    """
    if create:
        path.touch(exist_ok=False)
    # `as_uri` percent-encodes the path, so a `?`, `#` or `%` in it is not read as part
    # of the URI. It needs an absolute path, which `resolve` makes, and with `strict` it
    # raises for a file that is not there. `mode=rw` is the second guard: it makes
    # SQLite refuse a missing file where it would otherwise create one.
    conn = sqlite3.connect(f"{path.resolve(strict=True).as_uri()}?mode=rw", uri=True)
    try:
        if create:
            conn.executescript(schema)
        else:
            _require_tables(conn, path, schema)
    except BaseException:
        conn.close()
        if create:
            path.unlink(missing_ok=True)
        raise
    return conn


def _only_in(conn: sqlite3.Connection, table: str, here: str, there: str) -> bool:
    """Whether the table in the database `here` has a row that the one in `there` lacks.

    SQLite does the work, and only a count comes back. The query is written so that
    SQLite reads the two tables in key order and merges them as it goes, which takes no
    memory and stops at the first row that differs. `ORDER BY 1`, the first column of
    the key of every state table, is what asks for that, with the `LIMIT 1`. A plain
    EXCEPT, or an EXISTS around it, is merged like this by newer SQLite (3.53 and 3.54
    were tried), but 3.49 and 3.50 build a temporary b-tree of the whole first table
    instead, which took four times as long on three million rows and never stopped
    early.
    """
    (count,) = conn.execute(
        f"SELECT count(*) FROM (SELECT * FROM {here}.{table} "
        f"EXCEPT SELECT * FROM {there}.{table} ORDER BY 1 LIMIT 1)"
    ).fetchone()
    return count > 0


def diff_tables(a: sqlite3.Connection, b: Path, tables: Iterable[str]) -> list[str]:
    """The names of those of `tables` in which `a` and the database at `b` differ,
    sorted. An empty list means that the two are equal in each of them.

    For each table it asks whether either has a row that the other lacks, where a row is
    the same only if every value is the same, a null with a null, and a number is not
    the text of that number. The comparison is in SQL, and no row is read into Python,
    so it works for tables of millions of rows, where reading them into Python would
    need gigabytes. A table should be keyed by its first column, as a state table is, so
    that SQLite can read it in key order (see `_only_in`). `b` is attached to `a` while
    it runs, and it is detached again whatever happens. Neither database is written to.

    `a` must have no transaction open, so commit first. SQLite attaches inside one, but
    then will not detach the database that the transaction has read, and `b` would stay
    attached to the caller's connection. A connection opened with `autocommit=False` is
    always in a transaction, so it cannot be compared. Raises ValueError for a
    connection that has a transaction open, FileNotFoundError when `b` is not there,
    because attaching it would make an empty database, and sqlite3.OperationalError
    when `b` lacks one of `tables`.
    """
    if a.in_transaction:
        raise ValueError(
            "the connection has a transaction open, so the other state could not be "
            "detached again: commit before comparing"
        )
    a.execute("ATTACH DATABASE ? AS compared", (str(b.resolve(strict=True)),))
    try:
        return sorted(
            table
            for table in tables
            if _only_in(a, table, "main", "compared")
            or _only_in(a, table, "compared", "main")
        )
    finally:
        a.execute("DETACH DATABASE compared")
