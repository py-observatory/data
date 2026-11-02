"""The state of the stream's advisories, the delta that records each change to it, and
how a delta applies to the state.

The stream is event sourced on the kit's delta (`obskit.delta`). A run writes what it
learns into a delta database, as rows numbered by `seq`, and applies them to the state
database, so the state is only what the deltas say, and a month's base and its deltas
rebuild it.

The state has two tables:

- `meta` holds the `cursor` (the generation of the last index that was read),
  `bootstrap_at` (when the state was first built) and `applied_seq` (the highest `seq`
  that was applied). A delta sets only the cursor. The run stores the other two.
- `advisories` has one row for each advisory that the stream has seen. Its `status` is
  `active` while OSV's index lists it and `removed` once it does not. `first_seen` is
  when the stream first saw it, which for the advisories of the first run is the time
  of that run. The rest is its last version: every column of the version but the id,
  which is the key, and the `seq` of the row of the delta that holds it.

A delta has one table for each kind of row, each keyed by `seq`, and every row has the
time of its run, `observed_at`. `versions` holds each copy of a record that a run read,
as `records.parse_record` makes it, so a delta can hold several versions of one
advisory. `events` holds the removals and returns of advisories, and `cursors` the
generation of each index that a run read.
"""

import sqlite3

from obskit.delta import DeltaWriter, apply_rows
from osv_pypi.records import VERSION_COLUMNS

# The columns of a version that the state keeps of an advisory: all of them but the
# id, which is the advisory's key.
_KEPT = tuple(column for column in VERSION_COLUMNS if column != "id")

# What the state requires of a version: one of the two kinds, the time it was modified
# with its key, and the PyPI entries that it affects. Each other column may be null.
_REQUIRED = {
    "kind": "TEXT NOT NULL CHECK (kind IN ('malware', 'vulnerability'))",
    "modified": "TEXT NOT NULL",
    "modified_key": "TEXT NOT NULL",
    "affected": "TEXT NOT NULL",
}
_ADVISORY_VERSION = ",\n    ".join(
    f"{column} {_REQUIRED[column]}" if column in _REQUIRED else column
    for column in _KEPT
)

# The tables that hold versions are made from VERSION_COLUMNS, as the rows are.
STATE_SCHEMA = f"""
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE advisories (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('active', 'removed')),
    first_seen TEXT NOT NULL,
    seq INTEGER NOT NULL,
    {_ADVISORY_VERSION}
) WITHOUT ROWID;
"""

DELTA_SCHEMA = f"""
CREATE TABLE versions (
    seq INTEGER PRIMARY KEY,
    observed_at TEXT NOT NULL,
    {", ".join(VERSION_COLUMNS)}
);

CREATE TABLE events (
    seq INTEGER PRIMARY KEY,
    id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('removed', 'returned')),
    observed_at TEXT NOT NULL
);

CREATE TABLE cursors (
    seq INTEGER PRIMARY KEY,
    generation INTEGER NOT NULL,
    observed_at TEXT NOT NULL
);
"""

STATE_TABLES = ("meta", "advisories")

_INSERT_VERSION = (
    f"INSERT INTO versions (seq, observed_at, {', '.join(VERSION_COLUMNS)}) "
    f"VALUES (?, ?, {', '.join('?' * len(VERSION_COLUMNS))})"
)
_INSERT_EVENT = "INSERT INTO events (seq, id, kind, observed_at) VALUES (?, ?, ?, ?)"
_INSERT_CURSOR = "INSERT INTO cursors (seq, generation, observed_at) VALUES (?, ?, ?)"
_INSERT_ADVISORY = (
    f"INSERT INTO advisories (id, status, first_seen, seq, {', '.join(_KEPT)}) "
    f"VALUES (?, 'active', ?, ?, {', '.join('?' * len(_KEPT))})"
)
_UPDATE_ADVISORY = (
    f"UPDATE advisories SET seq = ?, {', '.join(f'{column} = ?' for column in _KEPT)} "
    "WHERE id = ?"
)


def meta(state: sqlite3.Connection, key: str) -> str | None:
    """The value of `key` in the state's meta table, or None when it has none."""
    row = state.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return None if row is None else row[0]


def _apply_version(state: sqlite3.Connection, row: tuple) -> None:
    seq, observed_at, *values = row
    version = dict(zip(VERSION_COLUMNS, values, strict=True))
    advisory_id = version["id"]
    kept = [version[column] for column in _KEPT]
    held = state.execute(
        "SELECT status, modified_key FROM advisories WHERE id = ?", (advisory_id,)
    ).fetchone()
    if held is None:
        state.execute(_INSERT_ADVISORY, (advisory_id, observed_at, seq, *kept))
        return
    status, modified_key = held
    if status != "active":
        raise ValueError(
            f"the state holds advisory {advisory_id!r} as {status}, and a version "
            "applies only to an active advisory, so a return must come first"
        )
    if version["modified_key"] < modified_key:
        raise ValueError(
            f"the version of advisory {advisory_id!r} was modified at "
            f"{version['modified_key']}, before the version that the state holds, "
            f"which was modified at {modified_key}: a version never goes back"
        )
    state.execute(_UPDATE_ADVISORY, (seq, *kept, advisory_id))


def _set_status(
    state: sqlite3.Connection, advisory_id: str, was: str, now: str
) -> None:
    """Give the advisory, which the state must hold with the status `was`, the status
    `now`."""
    cursor = state.execute(
        "UPDATE advisories SET status = ? WHERE id = ? AND status = ?",
        (now, advisory_id, was),
    )
    if cursor.rowcount != 1:
        raise ValueError(f"the state has no {was} advisory {advisory_id!r}")


def _apply_event(state: sqlite3.Connection, row: tuple) -> None:
    _, advisory_id, kind = row
    match kind:
        case "removed":
            _set_status(state, advisory_id, "active", "removed")
        case "returned":
            _set_status(state, advisory_id, "removed", "active")
        case _:
            raise ValueError(f"the delta has an event of an unknown kind, {kind!r}")


def _apply_cursor(state: sqlite3.Connection, row: tuple) -> None:
    _, generation = row
    held = meta(state, "cursor")
    if held is not None and generation < int(held):
        raise ValueError(
            f"the cursor would go back from generation {held} to {generation}: an "
            "index with a lower generation is a stale copy"
        )
    # The generation goes in as it is. `meta.value` is TEXT, so the number is stored as
    # its text.
    state.execute(
        "INSERT INTO meta (key, value) VALUES ('cursor', ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (generation,),
    )


# For each table of a delta, the columns that `apply_delta` reads, `seq` first, and the
# function that applies a row of them.
_APPLY = {
    "versions": (f"seq, observed_at, {', '.join(VERSION_COLUMNS)}", _apply_version),
    "events": ("seq, id, kind", _apply_event),
    "cursors": ("seq, generation", _apply_cursor),
}
# The tables of a delta are those that `apply_delta` reads, so the numbering of a
# `Delta` counts every table whose rows are applied.
_DELTA_TABLES = tuple(_APPLY)


class Delta(DeltaWriter):
    """The writer of one delta database of the stream. Each row it writes takes the
    next `seq`, and the time of the run as its `observed_at`.

    Rows are numbered as `obskit.delta.DeltaWriter` numbers them: from `first_seq`, and
    on after the highest `seq` that the delta holds already, in any of its tables. A
    row that is refused takes no `seq`. A row goes into the open transaction of `conn`,
    and the caller commits it before anything else reads the delta.
    """

    def __init__(
        self, conn: sqlite3.Connection, observed_at: str, *, first_seq: int = 1
    ) -> None:
        super().__init__(conn, observed_at, _DELTA_TABLES, first_seq=first_seq)

    def version(self, row: dict) -> None:
        """Record a version of an advisory, as `records.parse_record` makes it.

        Its keys must be exactly VERSION_COLUMNS. A row with another key is refused and
        not cut down to the columns, because a key that is left out is data that is
        lost. Raises ValueError for a key that is missing or not a column.
        """
        if row.keys() != set(VERSION_COLUMNS):
            missing = [column for column in VERSION_COLUMNS if column not in row]
            extra = sorted(row.keys() - set(VERSION_COLUMNS))
            raise ValueError(
                "a version row must have the keys of VERSION_COLUMNS and no others "
                f"(missing: {', '.join(missing) or 'none'}; "
                f"not columns: {', '.join(extra) or 'none'})"
            )
        values = tuple(row[column] for column in VERSION_COLUMNS)
        self._write(_INSERT_VERSION, (self.observed_at, *values))

    def removed(self, advisory_id: str) -> None:
        """Record that OSV's index no longer lists an advisory."""
        self._write(_INSERT_EVENT, (advisory_id, "removed", self.observed_at))

    def returned(self, advisory_id: str) -> None:
        """Record that OSV's index lists a removed advisory again."""
        self._write(_INSERT_EVENT, (advisory_id, "returned", self.observed_at))

    def cursor(self, generation: int) -> None:
        """Record the generation of the index that the run read."""
        self._write(_INSERT_CURSOR, (generation, self.observed_at))


def apply_delta(
    state: sqlite3.Connection, delta: sqlite3.Connection, *, after_seq: int = 0
) -> int:
    """Apply the rows of a delta that have a `seq` above `after_seq` to the state, in
    `seq` order, and return the highest `seq` applied.

    This is `obskit.delta.apply_rows` with the stream's rows: they go in one savepoint,
    which is committed before it returns or undone whole by an error, and two rows
    with one `seq`, or a delta numbered below `after_seq`, raise ValueError. The error
    for a row says which row it was.

    Each row changes the state as follows, and a row that does not fit it raises
    ValueError, as an event of a kind that is not known does.

    - A `versions` row of an advisory that the state lacks adds it: active, first seen
      at the row's `observed_at`, with every column of the version and the row's `seq`.
      For an active advisory, it sets every column of the version and the `seq`. It
      does not fit a removed advisory, which only a return makes active, or a
      `modified_key` below the stored one: a version never goes back.
    - A `removed` event sets an active advisory removed, and a `returned` event sets a
      removed one active. Neither fits an advisory with another status, or one that the
      state lacks.
    - A `cursors` row sets `meta.cursor` to the text of its generation. It does not fit
      a generation below the stored cursor, since an index with a lower generation is a
      stale copy.
    """
    return apply_rows(state, delta, _APPLY, after_seq=after_seq)


def apply(state: sqlite3.Connection, delta: sqlite3.Connection) -> None:
    """Apply the rows of `delta` above the state's applied_seq to the state, store the
    highest seq applied as its applied_seq, and commit, as every run and every replay
    does. This is the stream's `Store.apply`.

    Raises ValueError when the state has no applied_seq, which a bootstrap writes, and
    as `apply_delta` does.
    """
    applied = meta(state, "applied_seq")
    if applied is None:
        raise ValueError(
            "the state has no applied_seq in meta, so no bootstrap built it"
        )
    top = apply_delta(state, delta, after_seq=int(applied))
    state.execute("UPDATE meta SET value = ? WHERE key = 'applied_seq'", (str(top),))
    state.commit()
