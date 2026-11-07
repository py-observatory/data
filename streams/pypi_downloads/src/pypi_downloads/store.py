"""The state of the stream's projects and days, the delta that records each change to
it, and how a delta applies to the state.

The stream is event sourced on the kit's delta (`obskit.delta`). The state has:

- `meta`: `applied_seq` (the highest `seq` applied), `bootstrap_at` and `launch` (the
  first day counted by the stream, YYYY-MM-DD).
- `projects`: each project's id and canonical name. The first day that a project had
  a download is not kept here: the backfill walks backward, so the day whose count
  first named a project is seldom its first, and the table is append-only once
  published. It is the earliest `day` of the project's `counts`.
- `days`: one row for each day counted, with what the counter reported.
- `breaks`: the days where the series changes meaning.
- `spend`: the newest budget notification of each billing month.

Days are stored as the number of days since 1970-01-01.

A delta has the same tables, each keyed by `seq` with the time of its run, plus
`counts`: each project's downloads for each day. The state holds no counts, and the
delta's `counts` rows are keyed by project and day, not `seq`: a `seq` column would
cost about 35% more per day. So `Delta` never sees `counts`, `insert_counts` writes
them, and `apply` leaves them alone. The month merge keys on the primary key, so a day
counted twice in one month fails it loudly instead of doubling.
"""

import sqlite3
from collections.abc import Iterable
from datetime import date, timedelta

from obskit.delta import DeltaWriter, apply_rows

STATE_SCHEMA = """
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE projects (
    project_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE days (
    day INTEGER PRIMARY KEY,
    rows_bigquery INTEGER NOT NULL,
    rows_counted INTEGER NOT NULL,
    partition_modified TEXT NOT NULL,
    projects INTEGER NOT NULL,
    downloads INTEGER NOT NULL,
    mirror_downloads INTEGER NOT NULL,
    null_project_rows INTEGER NOT NULL,
    read_bytes INTEGER NOT NULL,
    execution TEXT,
    vcpu_seconds REAL,
    gib_seconds REAL,
    counted_at TEXT NOT NULL,
    seq INTEGER NOT NULL
);

CREATE TABLE breaks (
    day INTEGER PRIMARY KEY,
    note TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE spend (
    month TEXT PRIMARY KEY,
    cost REAL NOT NULL,
    budget REAL NOT NULL,
    notified_at TEXT NOT NULL
) WITHOUT ROWID;
"""

DELTA_SCHEMA = """
CREATE TABLE projects (
    seq INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE days (
    seq INTEGER PRIMARY KEY,
    day INTEGER NOT NULL,
    rows_bigquery INTEGER NOT NULL,
    rows_counted INTEGER NOT NULL,
    partition_modified TEXT NOT NULL,
    projects INTEGER NOT NULL,
    downloads INTEGER NOT NULL,
    mirror_downloads INTEGER NOT NULL,
    null_project_rows INTEGER NOT NULL,
    read_bytes INTEGER NOT NULL,
    execution TEXT,
    vcpu_seconds REAL,
    gib_seconds REAL,
    counted_at TEXT NOT NULL,
    observed_at TEXT NOT NULL
);

CREATE TABLE breaks (
    seq INTEGER PRIMARY KEY,
    day INTEGER NOT NULL,
    note TEXT NOT NULL
);

CREATE TABLE spend (
    seq INTEGER PRIMARY KEY,
    month TEXT NOT NULL,
    cost REAL NOT NULL,
    budget REAL NOT NULL,
    notified_at TEXT NOT NULL
);

CREATE TABLE counts (
    project_id INTEGER NOT NULL,
    day INTEGER NOT NULL,
    downloads INTEGER NOT NULL,
    mirror_downloads INTEGER NOT NULL,
    PRIMARY KEY (project_id, day)
) WITHOUT ROWID;
"""

STATE_TABLES = ("meta", "projects", "days", "breaks", "spend")

# The columns of a day, in the order of the state's `days` table (and the delta's, which
# has observed_at where the state has seq).
_DAY_COLUMNS = (
    "day",
    "rows_bigquery",
    "rows_counted",
    "partition_modified",
    "projects",
    "downloads",
    "mirror_downloads",
    "null_project_rows",
    "read_bytes",
    "execution",
    "vcpu_seconds",
    "gib_seconds",
    "counted_at",
)

_INSERT_PROJECT = "INSERT INTO projects (seq, project_id, name) VALUES (?, ?, ?)"
_INSERT_DAY = (
    f"INSERT INTO days (seq, {', '.join(_DAY_COLUMNS)}, observed_at) "
    f"VALUES (?, {', '.join('?' * (len(_DAY_COLUMNS) + 1))})"
)
_INSERT_BREAK = "INSERT INTO breaks (seq, day, note) VALUES (?, ?, ?)"
_INSERT_SPEND = (
    "INSERT INTO spend (seq, month, cost, budget, notified_at) VALUES (?, ?, ?, ?, ?)"
)
_INSERT_COUNT = (
    "INSERT INTO counts (project_id, day, downloads, mirror_downloads) "
    "VALUES (?, ?, ?, ?)"
)

_EPOCH = date(1970, 1, 1)


def day_number(d: date) -> int:
    """The number of days from 1970-01-01 to `d`."""
    return (d - _EPOCH).days


def day_date(n: int) -> date:
    """The date that is `n` days after 1970-01-01."""
    return _EPOCH + timedelta(days=n)


def meta(state: sqlite3.Connection, key: str) -> str | None:
    """The value of `key` in the state's meta table, or None when it has none."""
    row = state.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return None if row is None else row[0]


def _apply_project(state: sqlite3.Connection, row: tuple) -> None:
    state.execute("INSERT INTO projects (project_id, name) VALUES (?, ?)", row[1:])


def _apply_day(state: sqlite3.Connection, row: tuple) -> None:
    seq, *columns = row
    state.execute(
        f"INSERT INTO days ({', '.join(_DAY_COLUMNS)}, seq) "
        f"VALUES ({', '.join('?' * (len(_DAY_COLUMNS) + 1))})",
        (*columns, seq),
    )


def _apply_break(state: sqlite3.Connection, row: tuple) -> None:
    state.execute("INSERT INTO breaks (day, note) VALUES (?, ?)", row[1:])


def _apply_spend(state: sqlite3.Connection, row: tuple) -> None:
    # A newer notice for a month replaces the one held, and an older one is dropped:
    # Pub/Sub can deliver an older notice after a newer one, and its lower cost would
    # lift a stop. The times are Pub/Sub's, in UTC, so they compare as text to the
    # second.
    state.execute(
        "INSERT INTO spend (month, cost, budget, notified_at) VALUES (?, ?, ?, ?) "
        "ON CONFLICT (month) DO UPDATE SET cost = excluded.cost, "
        "budget = excluded.budget, notified_at = excluded.notified_at "
        "WHERE excluded.notified_at > spend.notified_at",
        row[1:],
    )


# For each table of a delta that is applied, the columns that are read, `seq` first, and
# the function that applies a row of them. `counts` is not here: the state holds none.
_APPLY = {
    "projects": ("seq, project_id, name", _apply_project),
    "days": (f"seq, {', '.join(_DAY_COLUMNS)}", _apply_day),
    "breaks": ("seq, day, note", _apply_break),
    "spend": ("seq, month, cost, budget, notified_at", _apply_spend),
}


class Delta(DeltaWriter):
    """The writer of one delta database of the stream. Each row it writes takes the next
    `seq`, and the time of the run as its `observed_at`.

    It counts the tables that are keyed by `seq` and never `counts`, which has no `seq`.
    A row goes into the open transaction of `conn`, and the caller commits it.
    """

    def __init__(
        self, conn: sqlite3.Connection, observed_at: str, *, first_seq: int = 1
    ) -> None:
        super().__init__(
            conn,
            observed_at,
            ("projects", "days", "breaks", "spend"),
            first_seq=first_seq,
        )

    def project(self, project_id: int, name: str) -> None:
        """Record a project, by its id and its canonical name."""
        self._write(_INSERT_PROJECT, (project_id, name))

    def day(self, **columns) -> None:
        """Record a counted day. The keys must be exactly the columns of a day (`day`,
        `rows_bigquery`, and so on up to `counted_at`), with `execution`, `vcpu_seconds`
        and `gib_seconds` given as None for a day that came from the bucket. Raises
        ValueError for a key that is missing or not a column."""
        if columns.keys() != set(_DAY_COLUMNS):
            missing = [c for c in _DAY_COLUMNS if c not in columns]
            extra = sorted(columns.keys() - set(_DAY_COLUMNS))
            raise ValueError(
                "a day row must have the columns of a day and no others "
                f"(missing: {', '.join(missing) or 'none'}; "
                f"not columns: {', '.join(extra) or 'none'})"
            )
        values = tuple(columns[c] for c in _DAY_COLUMNS)
        self._write(_INSERT_DAY, (*values, self.observed_at))

    def series_break(self, day: int, note: str) -> None:
        """Record a day where the series changes meaning."""
        self._write(_INSERT_BREAK, (day, note))

    def spend(self, month: str, cost: float, budget: float, notified_at: str) -> None:
        """Record the budget notice for a billing month."""
        self._write(_INSERT_SPEND, (month, cost, budget, notified_at))


def insert_counts(
    conn: sqlite3.Connection, rows: Iterable[tuple[int, int, int, int]]
) -> int:
    """Insert `(project_id, day, downloads, mirror_downloads)` rows into the delta's
    `counts` and return how many went in. Raises sqlite3.IntegrityError for a project
    and day that are there already. The rows join the open transaction of `conn`."""
    inserted = 0

    def counted(source: Iterable[tuple[int, int, int, int]]):
        nonlocal inserted
        for row in source:
            inserted += 1
            yield row

    conn.executemany(_INSERT_COUNT, counted(rows))
    return inserted


def apply(state: sqlite3.Connection, delta: sqlite3.Connection) -> None:
    """Apply the rows of `delta` above the state's applied_seq to the state, store the
    highest seq of its seq-keyed tables as the applied_seq, and commit, as every run and
    every replay does. This is the stream's `Store.apply`. It never reads `counts`.

    A `projects`, `days` or `breaks` row whose key is in the state already raises
    sqlite3.IntegrityError, and a `spend` row replaces the month's only when it was
    notified later. Raises ValueError when the state has no applied_seq, which a
    bootstrap writes, and as `apply_rows` does.
    """
    applied = meta(state, "applied_seq")
    if applied is None:
        raise ValueError(
            "the state has no applied_seq in meta, so no bootstrap built it"
        )
    top = apply_rows(state, delta, _APPLY, after_seq=int(applied))
    state.execute("UPDATE meta SET value = ? WHERE key = 'applied_seq'", (str(top),))
    state.commit()
