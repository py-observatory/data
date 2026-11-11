"""The state, the deltas that record each change to it, and the replay of a delta.

The stream is event sourced. A run writes everything that it learns into a delta
database, as rows numbered by `seq`, and then applies those rows to the state database
with `apply_delta`. The state is only what the deltas say. To restore a day, start from
the state as it stood at the start of the month and apply that month's deltas again, in
order. That rebuilds the state only if applying a delta is exact, so `apply_delta`
takes everything from the rows of the delta, applies them in `seq` order, and reads
neither the clock nor anything else. A delta that reaches the state in one pass, or in
several (the runs of a day each apply what the runs before them left), gives the same
state.

`seq` is one sequence for all of the stream's deltas, and not one for each delta. A new
delta is opened with a first `seq` that is one above the highest that the state has
applied, so the deltas of a month never repeat a `seq` and can be merged into one
database by key.

The state has four tables:

- `meta` holds the `cursor` (the serial of the root that was last read), `bootstrap_at`
  (when the state was first built) and `applied_seq` (the highest `seq` that was
  applied). A delta sets only the cursor. The run stores the other two.
- `projects` has one row for each project name, for its current incarnation. `swept` is
  1 once the project's own page has been read in full. `first_seen_exact` is 1 when
  `first_seen` is when the project was first seen, and 0 when the project was already
  there at the first run, which makes `first_seen` the time of that run.
- `files_current` holds the files that a project has now, with their yank.
- `versions_seen` holds the versions that have been seen, and whether each has a release
  record. A record can come after its version, in a row of its own, and applying it
  marks the version.

A delta has one table for each kind of row. `events`, `serials`, `swept` and
`cursors` are keyed by `seq`. `files`, `versions` and `release_records` are keyed by
what they are about, so a delta holds a file or a release once, and each also has a
`seq`.
"""

import heapq
import sqlite3
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from operator import itemgetter
from pathlib import Path

# open_db stays importable from this module, where it was first defined.
from obskit.sqlite import diff_tables, open_db as open_db
from pypi_registry.diff import Event
from pypi_registry.records import RECORD_COLUMNS
from pypi_registry.simple import FileInfo

STATE_SCHEMA = """
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE projects (
    name TEXT PRIMARY KEY,
    incarnation INTEGER NOT NULL,
    last_serial INTEGER NOT NULL,
    status TEXT NOT NULL,
    swept INTEGER NOT NULL,
    first_seen TEXT NOT NULL,
    first_seen_exact INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE files_current (
    name,
    incarnation,
    filename,
    yanked INTEGER NOT NULL,
    yanked_reason,
    PRIMARY KEY (name, incarnation, filename)
) WITHOUT ROWID;

CREATE TABLE versions_seen (
    name,
    incarnation,
    version,
    record INTEGER NOT NULL,
    PRIMARY KEY (name, incarnation, version)
) WITHOUT ROWID;
"""

# The table of release records is made from RECORD_COLUMNS, as the records are.
DELTA_SCHEMA = f"""
CREATE TABLE files (
    seq INTEGER NOT NULL,
    name,
    incarnation,
    filename,
    version,
    size,
    upload_time,
    requires_python,
    yanked,
    yanked_reason,
    has_metadata,
    has_provenance,
    first_seen,
    first_seen_exact,
    PRIMARY KEY (name, incarnation, filename)
) WITHOUT ROWID;

CREATE TABLE versions (
    seq INTEGER NOT NULL,
    name,
    incarnation,
    version,
    record,
    PRIMARY KEY (name, incarnation, version)
) WITHOUT ROWID;

CREATE TABLE release_records (
    seq INTEGER NOT NULL,
    {", ".join(RECORD_COLUMNS)},
    PRIMARY KEY (name, incarnation, version)
) WITHOUT ROWID;

CREATE TABLE events (
    seq INTEGER PRIMARY KEY,
    name,
    incarnation,
    kind,
    version,
    filename,
    detail,
    source_at,
    observed_at
);

CREATE TABLE serials (
    seq INTEGER PRIMARY KEY,
    name,
    serial
);

CREATE TABLE swept (
    seq INTEGER PRIMARY KEY,
    name,
    incarnation,
    status
);

CREATE TABLE cursors (
    seq INTEGER PRIMARY KEY,
    serial,
    fetched_at
);
"""

_STATE_TABLES = ("meta", "projects", "files_current", "versions_seen")
_DELTA_TABLES = (
    "files",
    "versions",
    "release_records",
    "events",
    "serials",
    "swept",
    "cursors",
)


def _insert(table: str, *columns: str) -> str:
    """The statement that inserts one row into `table`, with a `?` for each column."""
    marks = ", ".join("?" * len(columns))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks})"


_INSERT_EVENT = _insert(
    "events",
    "seq",
    "name",
    "incarnation",
    "kind",
    "version",
    "filename",
    "detail",
    "source_at",
    "observed_at",
)
_INSERT_FILE = _insert(
    "files",
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
)
_INSERT_VERSION = _insert("versions", "seq", "name", "incarnation", "version", "record")
_INSERT_RECORD = _insert("release_records", "seq", *RECORD_COLUMNS)
_INSERT_SERIAL = _insert("serials", "seq", "name", "serial")
_INSERT_SWEPT = _insert("swept", "seq", "name", "incarnation", "status")
_INSERT_CURSOR = _insert("cursors", "seq", "serial", "fetched_at")


def _highest_seq(conn: sqlite3.Connection) -> int | None:
    """The highest `seq` in any table of the delta, or None when it has no row."""
    highests = " UNION ALL ".join(
        f"SELECT max(seq) AS seq FROM {table}" for table in _DELTA_TABLES
    )
    (highest,) = conn.execute(f"SELECT max(seq) FROM ({highests})").fetchone()
    return highest


class Delta:
    """The writer of one delta database. Each row it writes has the next `seq`.

    `conn` is the connection to the delta, from `open_db`. `observed_at` is the time of
    the run, and every row that has a time takes it: an event's `observed_at`, and the
    `first_seen` of a file. A row goes into the open transaction of the connection. This
    class does not commit, so the caller commits `conn` before anything else reads the
    delta.

    Numbering starts at `first_seq` and goes on after the highest `seq` that the delta
    holds already, in any table. A delta that is opened again carries on from its last
    row, and a delta with no rows starts at `first_seq`. A row that is refused takes no
    `seq`. Raises ValueError for a `first_seq` below 1: a state that has applied nothing
    has applied through 0 and applies the rows above that, so a row numbered 0 would
    never be applied.

    Opening a `Delta` reads the whole delta once to find its highest `seq`, so a run
    opens one and keeps it, and does not open one for each project.
    """

    def __init__(
        self, conn: sqlite3.Connection, observed_at: str, *, first_seq: int = 1
    ) -> None:
        if first_seq < 1:
            raise ValueError(f"first_seq must be at least 1, not {first_seq}")
        self._conn = conn
        self._observed_at = observed_at
        highest = _highest_seq(conn)
        self._next_seq = first_seq if highest is None else max(first_seq, highest + 1)

    @property
    def observed_at(self) -> str:
        """The time of the run, which every row with a time takes. A release record is
        made with it too, so that the record and the rows around it give one time."""
        return self._observed_at

    def _write(self, statement: str, values: tuple) -> None:
        self._conn.execute(statement, (self._next_seq, *values))
        self._next_seq += 1

    def event(self, e: Event) -> None:
        """Record an event, observed at the time of the delta."""
        self._write(
            _INSERT_EVENT,
            (
                e.name,
                e.incarnation,
                e.kind,
                e.version,
                e.filename,
                e.detail,
                e.source_at,
                self._observed_at,
            ),
        )

    def file(self, name: str, incarnation: int, f: FileInfo, *, exact: bool) -> None:
        """Record a file that a project page lists, first seen at the time of the delta.
        `exact` says whether that is when the file appeared. It is not for a file that
        was there before the project was first read."""
        self._write(
            _INSERT_FILE,
            (
                name,
                incarnation,
                f.filename,
                f.version,
                f.size,
                f.upload_time,
                f.requires_python,
                int(f.yanked),
                f.yanked_reason,
                int(f.has_metadata),
                int(f.has_provenance),
                self._observed_at,
                int(exact),
            ),
        )

    def version(
        self, name: str, incarnation: int, version: str, *, record: bool
    ) -> None:
        """Record that a version was seen, and whether it has a release record."""
        self._write(_INSERT_VERSION, (name, incarnation, version, int(record)))

    def record(self, row: dict) -> None:
        """Record a release record, as `release_record` makes it.

        Its keys must be exactly RECORD_COLUMNS. A record with another key is refused
        and not cut down to the columns, because a key that is left out is data that is
        lost. Raises ValueError for a key that is missing or not a column.
        """
        if row.keys() != set(RECORD_COLUMNS):
            missing = [column for column in RECORD_COLUMNS if column not in row]
            extra = sorted(row.keys() - set(RECORD_COLUMNS))
            raise ValueError(
                "a release record must have the keys of RECORD_COLUMNS and no others "
                f"(missing: {', '.join(missing) or 'none'}; "
                f"not columns: {', '.join(extra) or 'none'})"
            )
        self._write(_INSERT_RECORD, tuple(row[column] for column in RECORD_COLUMNS))

    def serial(self, name: str, serial: int) -> None:
        """Record the serial that a project is at."""
        self._write(_INSERT_SERIAL, (name, serial))

    def swept(self, name: str, incarnation: int, status: str) -> None:
        """Record that the page of a project was read in full, and the status that it
        gave."""
        self._write(_INSERT_SWEPT, (name, incarnation, status))

    def cursor(self, serial: int, fetched_at: str) -> None:
        """Record the serial of the root that this run read, and when it was fetched."""
        self._write(_INSERT_CURSOR, (serial, fetched_at))


def _project(name: str, incarnation: int) -> str:
    return f"project {name!r} at incarnation {incarnation}"


def _expect_one(cursor: sqlite3.Cursor, what: str) -> None:
    """Raise unless the statement that `cursor` ran changed exactly one row."""
    if cursor.rowcount != 1:
        raise ValueError(f"the state has no {what}")


def _set_status(
    state: sqlite3.Connection, name: str, incarnation: int, status: str | None
) -> None:
    cursor = state.execute(
        "UPDATE projects SET status = ? WHERE name = ? AND incarnation = ?",
        (status, name, incarnation),
    )
    _expect_one(cursor, _project(name, incarnation))


def _apply_event(state: sqlite3.Connection, row: tuple) -> None:
    _, name, incarnation, kind, filename, detail, observed_at = row
    match kind:
        case "created":
            state.execute(
                "INSERT INTO projects (name, incarnation, last_serial, status, swept, "
                "first_seen, first_seen_exact) VALUES (?, ?, 0, 'active', 0, ?, 1)",
                (name, incarnation, observed_at),
            )
        case "reregistered":
            # The event carries the new incarnation. It can only follow a removal: the
            # project that it replaces must be removed, and of an earlier incarnation. A
            # quarantined project keeps its files and is the same project when it
            # returns. The condition is also what stops a replay from applying the same
            # event twice.
            cursor = state.execute(
                "UPDATE projects SET incarnation = ?, status = 'active', swept = 0, "
                "first_seen = ?, first_seen_exact = 1 "
                "WHERE name = ? AND incarnation < ? AND status = 'removed'",
                (incarnation, observed_at, name, incarnation),
            )
            _expect_one(
                cursor, f"removed project {name!r} below incarnation {incarnation}"
            )
        case "removed":
            _set_status(state, name, incarnation, "removed")
            state.execute(
                "DELETE FROM files_current WHERE name = ? AND incarnation = ?",
                (name, incarnation),
            )
        case "quarantined":
            _set_status(state, name, incarnation, "quarantined")
        case "unquarantined":
            _set_status(state, name, incarnation, "active")
        case "status_changed":
            _set_status(state, name, incarnation, detail)
        case "file_removed":
            cursor = state.execute(
                "DELETE FROM files_current "
                "WHERE name = ? AND incarnation = ? AND filename = ?",
                (name, incarnation, filename),
            )
            _expect_one(cursor, f"file {filename!r} of {_project(name, incarnation)}")
        case "yanked" | "unyanked":
            cursor = state.execute(
                "UPDATE files_current SET yanked = ?, yanked_reason = ? "
                "WHERE name = ? AND incarnation = ? AND filename = ?",
                (
                    kind == "yanked",
                    detail if kind == "yanked" else None,
                    name,
                    incarnation,
                    filename,
                ),
            )
            _expect_one(cursor, f"file {filename!r} of {_project(name, incarnation)}")
        case _:
            raise ValueError(f"the delta has an event of an unknown kind, {kind!r}")


def _apply_file(state: sqlite3.Connection, row: tuple) -> None:
    _, name, incarnation, filename, yanked, yanked_reason = row
    state.execute(
        "INSERT INTO files_current "
        "(name, incarnation, filename, yanked, yanked_reason) VALUES (?, ?, ?, ?, ?)",
        (name, incarnation, filename, yanked, yanked_reason),
    )


def _apply_version(state: sqlite3.Connection, row: tuple) -> None:
    _, name, incarnation, version, record = row
    state.execute(
        "INSERT INTO versions_seen (name, incarnation, version, record) "
        "VALUES (?, ?, ?, ?) ON CONFLICT (name, incarnation, version) "
        "DO UPDATE SET record = max(record, excluded.record)",
        (name, incarnation, version, record),
    )


def _apply_serial(state: sqlite3.Connection, row: tuple) -> None:
    _, name, serial = row
    cursor = state.execute(
        "UPDATE projects SET last_serial = ? WHERE name = ?", (serial, name)
    )
    _expect_one(cursor, f"project {name!r}")


def _apply_swept(state: sqlite3.Connection, row: tuple) -> None:
    _, name, incarnation, status = row
    cursor = state.execute(
        "UPDATE projects SET swept = 1, status = ? WHERE name = ? AND incarnation = ?",
        (status, name, incarnation),
    )
    _expect_one(cursor, _project(name, incarnation))


def _apply_cursor(state: sqlite3.Connection, row: tuple) -> None:
    _, serial = row
    # The serial goes in as it is. `meta.value` is TEXT, so a number is stored as its
    # text, and a null is refused there and does not become the text 'None'.
    state.execute(
        "INSERT INTO meta (key, value) VALUES ('cursor', ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (serial,),
    )


def _apply_release_record(state: sqlite3.Connection, row: tuple) -> None:
    """The delta keeps the record itself, and the state only that the version has one.

    The version must be in the state. A run that reads a new version writes its
    versions row first and its record next, so the record marks a version that is
    marked already. The records pass writes the record of a version that the state
    holds without one, and no versions row, so it is the record that marks it.
    """
    _, name, incarnation, version = row
    cursor = state.execute(
        "UPDATE versions_seen SET record = 1 "
        "WHERE name = ? AND incarnation = ? AND version = ?",
        (name, incarnation, version),
    )
    _expect_one(cursor, f"version {version!r} of {_project(name, incarnation)}")


# For each table of a delta, the columns that `apply_delta` reads, `seq` first, and the
# function that applies a row of them. A release record is read for its key alone.
_APPLY: dict[str, tuple[str, Callable[[sqlite3.Connection, tuple], None]]] = {
    "files": ("seq, name, incarnation, filename, yanked, yanked_reason", _apply_file),
    "versions": ("seq, name, incarnation, version, record", _apply_version),
    "release_records": ("seq, name, incarnation, version", _apply_release_record),
    "events": (
        "seq, name, incarnation, kind, filename, detail, observed_at",
        _apply_event,
    ),
    "serials": ("seq, name, serial", _apply_serial),
    "swept": ("seq, name, incarnation, status", _apply_swept),
    "cursors": ("seq, serial", _apply_cursor),
}


def _rows_after(
    delta: sqlite3.Connection, after_seq: int
) -> Iterator[tuple[int, str, tuple]]:
    """The rows of every table of the delta with a `seq` above `after_seq`, as `(seq,
    table, row)` in `seq` order. Each table is read in its own order, and the merge
    reads one row of each at a time, so its memory does not grow with the delta."""

    def table_rows(table: str, columns: str) -> Iterator[tuple[int, str, tuple]]:
        cursor = delta.execute(
            f"SELECT {columns} FROM {table} WHERE seq > ? ORDER BY seq", (after_seq,)
        )
        return ((row[0], table, row) for row in cursor)

    streams = [table_rows(table, columns) for table, (columns, _) in _APPLY.items()]
    return heapq.merge(*streams, key=itemgetter(0))


@contextmanager
def _atomic(conn: sqlite3.Connection) -> Generator[None]:
    """Run the block as one unit on `conn`. Its changes are committed when it ends, and
    an error in it undoes all of them. A savepoint does this whether or not the
    connection already has a transaction open."""
    conn.execute("SAVEPOINT atomic")
    try:
        yield
    except BaseException:
        # SQLite can end the whole transaction by itself on some errors, such as a full
        # disk, and then there is no savepoint left to go back to.
        if conn.in_transaction:
            conn.execute("ROLLBACK TO atomic")
            conn.execute("RELEASE atomic")
        raise
    conn.execute("RELEASE atomic")
    conn.commit()


def apply_delta(
    state: sqlite3.Connection, delta: sqlite3.Connection, *, after_seq: int = 0
) -> int:
    """Apply the rows of a delta that have a `seq` above `after_seq` to the state, in
    `seq` order, and return the highest `seq` applied.

    `state` and `delta` are connections to the two databases, and the delta is only
    read. The caller keeps the number that this returns (as the state's `applied_seq`),
    and passes it as `after_seq` the next time, so that each row is applied once. The
    number is `after_seq` when the delta has no row above it. A release record counts
    as every row does, so a delta that ends with one is applied through it.

    A delta that has rows, all of them below `after_seq`, is refused. It was not opened
    with a `first_seq` that goes on from what the state has applied, so none of its rows
    would ever be applied, and returning `after_seq` would hide that. A delta with no
    rows, or whose highest `seq` is `after_seq`, has nothing to apply and is not an
    error.

    Each row changes the state as follows. Every event and row is about a project, a
    file or a version at an incarnation, and the state must hold it.

    - `created` adds the project: active, with `last_serial` 0, not swept, and first
      seen at the event's `observed_at`, exactly.
    - `reregistered` gives a removed project the event's incarnation, which must be
      above the stored one. A project in any other status is refused: it is the same
      project, and a quarantined one keeps its files. The project is active and not
      swept, and first seen at the event's `observed_at`, exactly: it is a new project,
      and it was seen when it came back. Its `last_serial` is left for the serial row
      that follows.
    - `removed` sets the status `removed` and deletes the incarnation's files.
      `quarantined` sets `quarantined` and keeps the files, `unquarantined` sets
      `active`, and `status_changed` sets the status in the event's `detail`.
    - `file_removed` deletes the file. `yanked` marks it yanked, with the reason in
      `detail`, and `unyanked` clears the flag and the reason.
    - A `files` row adds the file, with its yank, to `files_current`.
    - A `versions` row adds the version to `versions_seen`, or if it is there, keeps the
      higher `record`: a record is never taken away.
    - A `serials` row sets the project's `last_serial`, to a lower number as well. A
      `swept` row sets `swept` to 1 and the status to the one in the row. A `cursors`
      row sets `meta.cursor`, to the text of the serial, and a row with no serial is
      refused.
    - A `release_records` row sets the version's `record` to 1. The version must be
      in `versions_seen` already, from this delta or an earlier one.

    The changes are made in one transaction, which is committed before the function
    returns, or none is made: an error undoes what the call did to the state. Raises
    ValueError for an event or row about a project, file or version that the state does
    not hold, for an event of a kind that is not known, for two rows with one `seq`,
    whose order would be open, and for a delta numbered below `after_seq`. Raises
    sqlite3.IntegrityError for a `created` event for a project that is there, for a
    `files` row for a file that is there, and for a `cursors` row with no serial. The
    error for a row says which row it was.
    """
    top = _highest_seq(delta)
    if top is not None and top < after_seq:
        raise ValueError(
            f"the highest seq of the delta is {top}, which is below after_seq "
            f"{after_seq}: its rows were not numbered from what the state has applied, "
            "so none of them would ever be applied"
        )
    highest = after_seq
    with _atomic(state):
        for seq, table, row in _rows_after(delta, after_seq):
            if seq == highest:
                raise ValueError(
                    f"the delta has two rows with seq {seq}, so their order is not set"
                )
            try:
                _APPLY[table][1](state, row)
            except Exception as exc:
                exc.add_note(f"while applying row {seq} of the {table} table")
                raise
            highest = seq
    return highest


def state_rows(state: sqlite3.Connection) -> dict[str, list[tuple]]:
    """Every row of the four tables of the state, each table's rows sorted.

    The tables are in the order of the schema. The rows are all held in memory, so this
    is for tests and for small states. `diff_states` compares large ones.
    """
    return {
        table: sorted(tuple(row) for row in state.execute(f"SELECT * FROM {table}"))
        for table in _STATE_TABLES
    }


def diff_states(a: sqlite3.Connection, b: Path) -> list[str]:
    """The names of the state tables in which `a` and the state database at `b` differ,
    sorted. An empty list means that the two are equal.

    The comparison is in SQL and reads no row into Python, so it works for states of
    millions of rows, where `state_rows` would need gigabytes. What it needs of `a`,
    and what it raises, is told by `obskit.sqlite.diff_tables`, which it calls.
    """
    return diff_tables(a, b, _STATE_TABLES)
