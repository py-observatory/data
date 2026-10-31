"""SQLite helpers: row counts for the manifest, and the monthly merge of deltas."""

import sqlite3
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


def consolidate(inputs: list[Path], dst: Path) -> dict[str, int]:
    """Merge databases that share one schema into the new database `dst`.

    Returns the row count of each table, which equals its sum over `inputs`. Raises
    FileExistsError if `dst` exists (it is left alone), ValueError for no inputs or
    for schemas that differ, and sqlite3.IntegrityError when a key repeats or the
    merge loses a row. Once the build starts, any failure removes `dst`.
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
