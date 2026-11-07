"""Reading and checking a counts file that the counter wrote, and writing its day into
the stream's open delta.

A counts file is two objects (see the counter's output): a summary in JSON and a SQLite
database compressed with zstd. Both come from a bucket, so they are data and nothing
else. `read_counts` checks them in full before the caller writes a row, and refuses with
`CountsError` for anything the counter never meant to write.
"""

import hashlib
import json
import sqlite3
from compression import zstd
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pypi_downloads.store import Delta, day_number, insert_counts

from obskit.delta import atomic
from obskit.names import canonical_name
from obskit.sqlite import open_db

# The counter's copy of this text is checked against this one by a test, so it stays one
# plain string literal.
COUNTS_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE counts (
    project TEXT PRIMARY KEY,
    downloads INTEGER NOT NULL CHECK (downloads > 0),
    mirror_downloads INTEGER NOT NULL CHECK (mirror_downloads BETWEEN 0 AND downloads)
) WITHOUT ROWID;
"""

# A day's database is about 20 MB. A file that expands past this is not one the counter
# wrote, and the limit keeps a small compressed file from filling the disk.
MAX_DB_BYTES = 1 << 30

_MAX_NAME_LENGTH = 256
_CHUNK_SIZE = 1024 * 1024
_NAMES_PER_QUERY = 500

_INT_KEYS = (
    "db_bytes",
    "rows_counted",
    "rows_bigquery",
    "read_bytes",
    "null_project_rows",
)
_STR_KEYS = ("day", "db_sha256", "partition_modified")


class CountsError(ValueError):
    """A counts file that is not what the counter writes."""


@dataclass(frozen=True)
class Counted:
    """One day's counts, checked. `counts` maps each canonical project name to
    `(downloads, mirror_downloads)`, and `renamed` is how many names of the file were
    not canonical and were merged into their canonical form."""

    day: date
    rows_bigquery: int
    rows_counted: int
    partition_modified: str
    read_bytes: int
    null_project_rows: int
    counts: dict[str, tuple[int, int]]
    renamed: int


def _read_summary(summary: bytes) -> dict:
    try:
        parsed = json.loads(summary)
    except ValueError as e:
        raise CountsError(f"the summary is not JSON: {e}") from None
    if not isinstance(parsed, dict):
        raise CountsError("the summary is not a JSON object")
    for key in _STR_KEYS:
        if not isinstance(parsed.get(key), str):
            raise CountsError(f"the summary lacks {key} as text")
    for key in _INT_KEYS:
        value = parsed.get(key)
        if not isinstance(value, int) or isinstance(value, bool):
            raise CountsError(f"the summary lacks {key} as an integer")
    return parsed


def _expand(db_zst: Path, db: Path) -> None:
    """Expand `db_zst` into `db`, and stop as soon as it passes MAX_DB_BYTES."""
    written = 0
    try:
        with zstd.open(db_zst, "rb") as fin, open(db, "wb") as fout:
            while chunk := fin.read(_CHUNK_SIZE):
                written += len(chunk)
                if written > MAX_DB_BYTES:
                    raise CountsError(
                        f"the database is over {MAX_DB_BYTES} bytes once decompressed"
                    )
                fout.write(chunk)
    except zstd.ZstdError as e:
        raise CountsError(f"the database is not valid zstd: {e}") from None


def _check_name(name: str) -> None:
    if not name or len(name) > _MAX_NAME_LENGTH or not name.isprintable():
        raise CountsError(
            f"a project name is empty, over {_MAX_NAME_LENGTH} characters or not "
            f"printable: {name[:40]!r}"
        )


def _merged(conn: sqlite3.Connection) -> tuple[dict[str, tuple[int, int]], int]:
    """The rows of the database with their names canonicalised, and how many names
    changed. Each row is checked as it is read."""
    counts: dict[str, tuple[int, int]] = {}
    renamed = 0
    for name, downloads, mirror in conn.execute(
        "SELECT project, downloads, mirror_downloads FROM counts"
    ):
        _check_name(name)
        if not isinstance(downloads, int) or downloads < 1:
            raise CountsError(f"{name!r} has a downloads count below 1: {downloads!r}")
        if not isinstance(mirror, int) or not 0 <= mirror <= downloads:
            raise CountsError(
                f"{name!r} has a mirror count outside 0..downloads: {mirror!r}"
            )
        canonical = canonical_name(name)
        if canonical != name:
            renamed += 1
        _check_name(canonical)
        d, m = counts.get(canonical, (0, 0))
        counts[canonical] = (d + downloads, m + mirror)
    return counts, renamed


def read_counts(summary: bytes, db_zst: bytes, day: date, workdir: Path) -> Counted:
    """Check the counts file of `day` and return its counts.

    `summary` and `db_zst` are the two objects as downloaded. The database is expanded
    and opened in `workdir`, read-only, against `COUNTS_SCHEMA`; the expanded copy is
    removed before this returns. Raises CountsError, naming the reason, for a summary
    that is not a JSON object or lacks a key, a day that is not `day`, a digest or size
    that does not match `db_zst`, a database over MAX_DB_BYTES, a meta day that is not
    `day`, rows that do not all count, totals that do not add up, a name that is empty,
    over 256 characters or not printable, and a count out of range. Nothing is written
    to any stream database.
    """
    parsed = _read_summary(summary)
    if parsed["day"] != day.isoformat():
        raise CountsError(f"the summary is for {parsed['day']}, not {day.isoformat()}")
    if parsed["db_bytes"] != len(db_zst):
        raise CountsError(
            f"the summary says {parsed['db_bytes']} bytes, the database has {len(db_zst)}"
        )
    if parsed["db_sha256"] != hashlib.sha256(db_zst).hexdigest():
        raise CountsError("the summary's db_sha256 is not the database's")
    if parsed["rows_counted"] != parsed["rows_bigquery"]:
        raise CountsError(
            f"rows_counted {parsed['rows_counted']} is not rows_bigquery "
            f"{parsed['rows_bigquery']}"
        )

    zst_path = workdir / f"counts-{day.isoformat()}.db.zst"
    db_path = workdir / f"counts-{day.isoformat()}.db"
    zst_path.write_bytes(db_zst)
    try:
        _expand(zst_path, db_path)
        zst_path.unlink()
        try:
            conn = open_db(db_path, COUNTS_SCHEMA)
        except (sqlite3.DatabaseError, ValueError) as e:
            raise CountsError(f"the database cannot be opened: {e}") from None
        try:
            conn.execute("PRAGMA query_only = ON")
            row = conn.execute("SELECT value FROM meta WHERE key = 'day'").fetchone()
            if row is None or row[0] != day.isoformat():
                raise CountsError(
                    f"the database's meta day is {row and row[0]!r}, not {day.isoformat()}"
                )
            counts, renamed = _merged(conn)
        except sqlite3.DatabaseError as e:
            raise CountsError(f"the database cannot be read: {e}") from None
        finally:
            conn.close()
    finally:
        zst_path.unlink(missing_ok=True)
        db_path.unlink(missing_ok=True)

    total = sum(d for d, _ in counts.values()) + parsed["null_project_rows"]
    if total != parsed["rows_counted"]:
        raise CountsError(
            f"downloads {sum(d for d, _ in counts.values())} plus null_project_rows "
            f"{parsed['null_project_rows']} is not rows_counted {parsed['rows_counted']}"
        )
    return Counted(
        day=day,
        rows_bigquery=parsed["rows_bigquery"],
        rows_counted=parsed["rows_counted"],
        partition_modified=parsed["partition_modified"],
        read_bytes=parsed["read_bytes"],
        null_project_rows=parsed["null_project_rows"],
        counts=counts,
        renamed=renamed,
    )


def _known_ids(conn: sqlite3.Connection, names: list[str]) -> dict[str, int]:
    """The ids of those of `names` that `conn` has in its `projects`."""
    found: dict[str, int] = {}
    for start in range(0, len(names), _NAMES_PER_QUERY):
        chunk = names[start : start + _NAMES_PER_QUERY]
        marks = ", ".join("?" * len(chunk))
        found.update(
            conn.execute(
                f"SELECT name, project_id FROM projects WHERE name IN ({marks})", chunk
            )
        )
    return found


def _top_id(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COALESCE(MAX(project_id), 0) FROM projects").fetchone()[
        0
    ]


def write_day(
    state: sqlite3.Connection,
    conn: sqlite3.Connection,
    delta: Delta,
    counted: Counted,
    *,
    execution: dict | None,
    counted_at: str,
) -> int:
    """Write `counted` into the open delta and return how many projects were new.

    `state` is the state database, `conn` is the delta's connection and `delta` writes to
    it. A name is known if the state or the delta has it already, because earlier days
    of the same run are in the delta and not yet in the state. Each new name takes the
    next id above every id of the two, in name order, with the day as its `first_day`.
    The counts go in by project id, and the `days` row last, with the `name`,
    `vcpu_seconds` and `gib_seconds` of `execution`, or None for a day taken from the
    bucket.

    All of it is one unit (`obskit.delta.atomic`): it commits when the day is written,
    and an error partway, such as a day that was counted already, undoes every row of
    the day and leaves the delta as it was. It commits `conn`, so earlier uncommitted
    rows of the delta are committed too.
    """
    day = day_number(counted.day)
    names = sorted(counted.counts)
    ids = _known_ids(state, names)
    ids.update(_known_ids(conn, names))
    new = [name for name in names if name not in ids]
    with atomic(conn):
        next_id = max(_top_id(state), _top_id(conn)) + 1
        for name in new:
            ids[name] = next_id
            delta.project(next_id, name, day)
            next_id += 1
        insert_counts(
            conn,
            (
                (ids[name], day, *counted.counts[name])
                for name in sorted(names, key=ids.__getitem__)
            ),
        )
        delta.day(
            day=day,
            rows_bigquery=counted.rows_bigquery,
            rows_counted=counted.rows_counted,
            partition_modified=counted.partition_modified,
            projects=len(counted.counts),
            downloads=sum(d for d, _ in counted.counts.values()),
            mirror_downloads=sum(m for _, m in counted.counts.values()),
            null_project_rows=counted.null_project_rows,
            read_bytes=counted.read_bytes,
            execution=execution["name"] if execution else None,
            vcpu_seconds=execution["vcpu_seconds"] if execution else None,
            gib_seconds=execution["gib_seconds"] if execution else None,
            counted_at=counted_at,
        )
    return len(new)
