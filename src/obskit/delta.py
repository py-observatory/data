"""The numbering of a delta's rows, and the loop that applies them to the state in
order, which the store of each event-sourced stream builds on.

Such a stream writes everything that a run learns into a delta database, as rows
numbered by `seq`, and then applies those rows to its state database. The state is only
what the deltas say. To restore a day, start from the state as it stood at the start of
the month and apply that month's deltas again, in order. That rebuilds the state only if
applying a delta is exact, so the rows are applied in `seq` order, and the function that
applies a row takes everything from the row and reads neither the clock nor anything
else. A delta that reaches the state in one pass, or in several (the runs of a day each
apply what the runs before them left), gives the same state.

`seq` is one sequence for all of a stream's deltas, and not one for each delta. A new
delta is opened with a first `seq` that is one above the highest that the state has
applied, so the deltas of a month never repeat a `seq` and can be merged into one
database by key.

What is the stream's own stays in its store: its schemas, a `Delta` that subclasses
`DeltaWriter` with a writer for each kind of row, and for each table of its delta the
columns that `apply_rows` reads, `seq` first, and the function that applies a row of
them.
"""

import heapq
import sqlite3
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import contextmanager
from operator import itemgetter


def highest_seq(conn: sqlite3.Connection, tables: Iterable[str]) -> int | None:
    """The highest `seq` in any of `tables`, the tables of the delta, or None when they
    have no row."""
    highests = " UNION ALL ".join(
        f"SELECT max(seq) AS seq FROM {table}" for table in tables
    )
    (highest,) = conn.execute(f"SELECT max(seq) FROM ({highests})").fetchone()
    return highest


class DeltaWriter:
    """The writer of one delta database. Each row it writes has the next `seq`.

    `conn` is the connection to the delta, from `open_db`. `observed_at` is the time of
    the run, and every row that has a time takes it. `tables` are the tables of the
    delta, every one of them: a table that was left out would not be counted, and a
    delta that is opened again could repeat a `seq` of its rows. A row goes into the
    open transaction of the connection. This class does not commit, so the caller
    commits `conn` before anything else reads the delta.

    Numbering starts at `first_seq` and goes on after the highest `seq` that the delta
    holds already, in any table. A delta that is opened again carries on from its last
    row, and a delta with no rows starts at `first_seq`. A row that is refused takes no
    `seq`. Raises ValueError for a `first_seq` below 1: a state that has applied nothing
    has applied through 0 and applies the rows above that, so a row numbered 0 would
    never be applied.

    Opening one can read the whole delta to find its highest `seq`, since a table that
    is not keyed by `seq` is read in full, so a run opens one and keeps it.

    A stream's `Delta` subclasses this with a writer for each kind of row, which writes
    the row with `_write`.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        observed_at: str,
        tables: Iterable[str],
        *,
        first_seq: int = 1,
    ) -> None:
        if first_seq < 1:
            raise ValueError(f"first_seq must be at least 1, not {first_seq}")
        self._conn = conn
        self._observed_at = observed_at
        highest = highest_seq(conn, tables)
        self._next_seq = first_seq if highest is None else max(first_seq, highest + 1)

    @property
    def observed_at(self) -> str:
        """The time of the run, which every row with a time takes. A value that a stream
        makes with the time of the run is made with it too, so that the value and the
        rows around it give one time."""
        return self._observed_at

    def _write(self, statement: str, values: tuple) -> None:
        """Write one row with `statement`, whose first mark is the `seq` and the rest
        `values`. The `seq` is used up only once the row is in, so a row that the
        database refuses takes none."""
        self._conn.execute(statement, (self._next_seq, *values))
        self._next_seq += 1


def rows_after(
    delta: sqlite3.Connection, columns: Mapping[str, str], after_seq: int
) -> Iterator[tuple[int, str, tuple]]:
    """The rows of every table of `columns` with a `seq` above `after_seq`, as `(seq,
    table, row)` in `seq` order. `columns` gives the columns of each table of the delta
    that are read, `seq` first. Each table is read in its own order, and the merge reads
    one row of each at a time, so its memory does not grow with the delta."""

    def table_rows(table: str, columns: str) -> Iterator[tuple[int, str, tuple]]:
        cursor = delta.execute(
            f"SELECT {columns} FROM {table} WHERE seq > ? ORDER BY seq", (after_seq,)
        )
        return ((row[0], table, row) for row in cursor)

    streams = [table_rows(table, read) for table, read in columns.items()]
    return heapq.merge(*streams, key=itemgetter(0))


@contextmanager
def atomic(conn: sqlite3.Connection) -> Generator[None]:
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


def apply_rows(
    state: sqlite3.Connection,
    delta: sqlite3.Connection,
    appliers: Mapping[str, tuple[str, Callable[[sqlite3.Connection, tuple], None]]],
    *,
    after_seq: int = 0,
) -> int:
    """Apply the rows of a delta that have a `seq` above `after_seq` to the state, in
    `seq` order, and return the highest `seq` applied.

    `state` and `delta` are connections to the two databases, and the delta is only
    read. `appliers` gives, for each table of the delta, the columns that are read,
    `seq` first, and the function that applies a row of them to the state. It names
    every table of the delta: a table that it leaves out is neither applied nor counted.

    The caller keeps the number that this returns (as the state's `applied_seq`), and
    passes it as `after_seq` the next time, so that each row is applied once. The number
    is `after_seq` when the delta has no row above it. A row whose function changes
    nothing counts too, so a delta that ends with one is applied through it.

    A delta that has rows, all of them below `after_seq`, is refused. It was not opened
    with a `first_seq` that goes on from what the state has applied, so none of its rows
    would ever be applied, and returning `after_seq` would hide that. A delta with no
    rows, or whose highest `seq` is `after_seq`, has nothing to apply and is not an
    error.

    The changes are made in one transaction, which is committed before the function
    returns, or none is made: an error undoes what the call did to the state. Raises
    ValueError for two rows with one `seq`, whose order would be open, and for a delta
    numbered below `after_seq`, and raises what a function raises for a row that does
    not fit. The error for a row says which row it was.
    """
    top = highest_seq(delta, appliers.keys())
    if top is not None and top < after_seq:
        raise ValueError(
            f"the highest seq of the delta is {top}, which is below after_seq "
            f"{after_seq}: its rows were not numbered from what the state has applied, "
            "so none of them would ever be applied"
        )
    columns = {table: read for table, (read, _) in appliers.items()}
    highest = after_seq
    with atomic(state):
        for seq, table, row in rows_after(delta, columns, after_seq):
            if seq == highest:
                raise ValueError(
                    f"the delta has two rows with seq {seq}, so their order is not set"
                )
            try:
                appliers[table][1](state, row)
            except Exception as exc:
                exc.add_note(f"while applying row {seq} of the {table} table")
                raise
            highest = seq
    return highest
