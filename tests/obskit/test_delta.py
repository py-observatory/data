import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

import pytest

from obskit.delta import DeltaWriter, apply_rows, atomic, highest_seq, rows_after
from obskit.sqlite import open_db

OBSERVED_AT = "2026-11-01T06:00:00Z"

# The databases of a toy stream. Its state holds items, each a key and a value. A delta
# has two tables, numbered by `seq` together as every stream's are: `puts`, each of which
# sets an item, and `drops`, each of which deletes one. `puts` is keyed by its item, so a
# delta puts an item once and its `seq` is a column apart from its key, as in a table
# that is keyed by what it is about. `drops` is keyed by its `seq`.
STATE_SCHEMA = """
CREATE TABLE items (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
"""
DELTA_SCHEMA = """
CREATE TABLE puts (
    seq INTEGER NOT NULL,
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE drops (seq INTEGER PRIMARY KEY, key TEXT NOT NULL);
"""
TABLES = ("puts", "drops")
# The items of the state that a delta is applied to.
BASE = {"b": "1", "c": "2"}


def apply_put(state: sqlite3.Connection, row: tuple) -> None:
    _, key, value = row
    state.execute(
        "INSERT INTO items (key, value) VALUES (?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def apply_drop(state: sqlite3.Connection, row: tuple) -> None:
    _, key = row
    if state.execute("DELETE FROM items WHERE key = ?", (key,)).rowcount != 1:
        raise ValueError(f"the state has no item {key!r}")


# For each table of the delta, the columns that are read, `seq` first, and the function
# that applies a row of them.
APPLIERS = {
    "puts": ("seq, key, value", apply_put),
    "drops": ("seq, key", apply_drop),
}


class ToyDelta(DeltaWriter):
    """The toy stream's delta, with a writer for each kind of row, as a stream's own
    `Delta` has."""

    def __init__(self, conn: sqlite3.Connection, *, first_seq: int = 1) -> None:
        super().__init__(conn, OBSERVED_AT, TABLES, first_seq=first_seq)

    def put(self, key: str, value: str) -> None:
        self._write("INSERT INTO puts (seq, key, value) VALUES (?, ?, ?)", (key, value))

    def drop(self, key: str) -> None:
        self._write("INSERT INTO drops (seq, key) VALUES (?, ?)", (key,))


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


def new_state(tmp_path: Path) -> sqlite3.Connection:
    """A new state database that holds the items of BASE, committed."""
    state = opened(open_db(tmp_path / "state.db", STATE_SCHEMA, create=True))
    state.executemany("INSERT INTO items (key, value) VALUES (?, ?)", BASE.items())
    state.commit()
    return state


def new_delta(
    tmp_path: Path, name: str = "delta.db", **options: int
) -> tuple[sqlite3.Connection, ToyDelta]:
    conn = opened(open_db(tmp_path / name, DELTA_SCHEMA, create=True))
    return conn, ToyDelta(conn, **options)


def apply_toy(
    state: sqlite3.Connection, delta: sqlite3.Connection, **options: int
) -> int:
    return apply_rows(state, delta, APPLIERS, **options)


def items_of(state: sqlite3.Connection) -> dict[str, str]:
    return dict(state.execute("SELECT key, value FROM items"))


def seqs_of(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    """The `seq` of every row of the delta, with its table, in `seq` order."""
    return sorted(
        (seq, table)
        for table in TABLES
        for (seq,) in conn.execute(f"SELECT seq FROM {table}")
    )


# ---- numbering


def test_seq_is_global_and_increasing(tmp_path):
    conn, delta = new_delta(tmp_path)
    # Rows in an order that mixes the tables.
    delta.put("a", "1")
    delta.drop("b")
    delta.drop("c")
    delta.put("d", "4")
    conn.commit()
    # One sequence, with no number used twice and none left out, that follows the order
    # of the calls and not the order of the tables.
    assert seqs_of(conn) == [(1, "puts"), (2, "drops"), (3, "drops"), (4, "puts")]


def test_first_seq_continues_numbering(tmp_path):
    path = tmp_path / "delta.db"
    # A delta that starts at 101 numbers its first row 101.
    with closing(open_db(path, DELTA_SCHEMA, create=True)) as conn:
        delta = ToyDelta(conn, first_seq=101)
        delta.put("a", "1")
        delta.drop("b")
        conn.commit()
        assert seqs_of(conn) == [(101, "puts"), (102, "drops")]
    # Opened again with the default start, it goes on after its highest seq.
    with closing(open_db(path, DELTA_SCHEMA)) as conn:
        ToyDelta(conn, first_seq=1).put("c", "3")
        conn.commit()
        assert seqs_of(conn) == [(101, "puts"), (102, "drops"), (103, "puts")]
    # A start that is past its highest seq is kept.
    with closing(open_db(path, DELTA_SCHEMA)) as conn:
        ToyDelta(conn, first_seq=500).drop("c")
        conn.commit()
        assert seqs_of(conn)[-1] == (500, "drops")


@pytest.mark.parametrize(
    "write",
    [
        pytest.param(lambda d: d.put("a", "1"), id="puts"),
        pytest.param(lambda d: d.drop("a"), id="drops"),
    ],
)
def test_the_highest_seq_may_be_in_any_table(tmp_path, write):
    # Whichever table holds the highest seq, a delta that is opened again numbers its
    # next row after it. A table that was left out of the count would repeat a seq.
    path = tmp_path / "delta.db"
    with closing(open_db(path, DELTA_SCHEMA, create=True)) as conn:
        assert highest_seq(conn, TABLES) is None
        write(ToyDelta(conn, first_seq=40))
        conn.commit()
        assert highest_seq(conn, TABLES) == 40
    with closing(open_db(path, DELTA_SCHEMA)) as conn:
        ToyDelta(conn).put("z", "9")
        conn.commit()
        found = conn.execute("SELECT seq FROM puts WHERE key = 'z'")
        assert found.fetchall() == [(41,)]


@pytest.mark.parametrize("first_seq", [0, -1])
def test_first_seq_must_be_positive(tmp_path, first_seq):
    # A state starts with nothing applied, which is 0, and applies the rows above that.
    # A row numbered 0 would never be applied.
    conn = opened(open_db(tmp_path / "delta.db", DELTA_SCHEMA, create=True))
    with pytest.raises(ValueError, match="first_seq"):
        ToyDelta(conn, first_seq=first_seq)


def test_a_row_that_is_refused_takes_no_seq(tmp_path):
    conn, delta = new_delta(tmp_path)
    delta.put("a", "1")
    with pytest.raises(sqlite3.IntegrityError):
        delta.put("a", "2")  # a delta puts an item once
    delta.drop("b")
    assert seqs_of(conn) == [(1, "puts"), (2, "drops")]


def test_a_delta_tells_the_time_of_its_run(tmp_path):
    # A stream takes the time of a row from the delta, so the rows of one run give one
    # time, and nothing can change it under them.
    _, delta = new_delta(tmp_path)
    assert delta.observed_at == OBSERVED_AT
    with pytest.raises(AttributeError):
        delta.observed_at = "2026-11-01T07:00:00Z"


# ---- the merge of the tables in seq order


def test_rows_after_merges_the_tables_in_seq_order(tmp_path):
    conn, delta = new_delta(tmp_path)
    delta.drop("b")  # 1
    delta.put("a", "1")  # 2
    delta.put("e", "5")  # 3
    delta.drop("a")  # 4
    conn.commit()
    columns = {"puts": "seq, key, value", "drops": "seq, key"}
    assert list(rows_after(conn, columns, 0)) == [
        (1, "drops", (1, "b")),
        (2, "puts", (2, "a", "1")),
        (3, "puts", (3, "e", "5")),
        (4, "drops", (4, "a")),
    ]
    # Only the rows above `after_seq`, each with the columns that were asked for.
    assert list(rows_after(conn, {"puts": "seq, value", "drops": "seq"}, 2)) == [
        (3, "puts", (3, "5")),
        (4, "drops", (4,)),
    ]


def test_rows_apply_in_seq_order_across_tables(tmp_path):
    state = new_state(tmp_path)
    conn, delta = new_delta(tmp_path)
    # An item that is put and then dropped is gone. Applied by table, with the drops
    # first, the drop would find no item.
    delta.put("a", "1")
    delta.drop("a")
    # An item that is put after it was dropped is there. Applied by table, with the puts
    # first, the drop would delete it.
    delta.drop("b")
    delta.put("b", "3")
    conn.commit()
    # The number returned is the highest seq applied.
    assert apply_toy(state, conn) == 4
    assert items_of(state) == {"b": "3", "c": "2"}


# ---- what is applied, and what it returns


def test_apply_rows_with_nothing_to_apply_returns_after_seq(tmp_path):
    state = new_state(tmp_path)
    conn, delta = new_delta(tmp_path)
    # A delta with no rows has nothing to apply, whatever the state has applied.
    assert apply_toy(state, conn) == 0
    assert apply_toy(state, conn, after_seq=7) == 7
    delta.put("a", "1")  # 1
    delta.drop("b")  # 2
    conn.commit()
    # The state has applied through the last row of the delta, as it has after the
    # last pass of a day, so nothing is applied and nothing is lost.
    assert apply_toy(state, conn, after_seq=2) == 2
    assert items_of(state) == BASE


@pytest.mark.parametrize("after_seq", [3, 50])
def test_a_delta_numbered_below_after_seq_is_refused(tmp_path, after_seq):
    # A delta that has rows, all of them below what the state has applied, was not
    # numbered from there. None of its rows would ever be applied, and returning
    # `after_seq` would let that pass for a delta with nothing in it.
    state = new_state(tmp_path)
    conn, delta = new_delta(tmp_path)
    delta.put("a", "1")  # 1
    delta.drop("b")  # 2
    conn.commit()
    with pytest.raises(ValueError, match="below after_seq") as caught:
        apply_toy(state, conn, after_seq=after_seq)
    assert f"is 2, which is below after_seq {after_seq}" in str(caught.value)
    assert items_of(state) == BASE
    assert not state.in_transaction


@pytest.mark.parametrize(
    ("after_seq", "items"),
    [
        pytest.param(0, {"a": "1", "c": "3"}, id="after 0"),
        pytest.param(1, {"c": "3"}, id="after 1"),
        pytest.param(2, {"b": "1", "c": "3"}, id="after 2"),
        pytest.param(3, BASE, id="after 3"),
    ],
)
def test_apply_rows_applies_only_the_rows_above_after_seq(tmp_path, after_seq, items):
    state = new_state(tmp_path)
    conn, delta = new_delta(tmp_path)
    delta.put("a", "1")  # 1
    delta.drop("b")  # 2
    delta.put("c", "3")  # 3
    conn.commit()
    assert apply_toy(state, conn, after_seq=after_seq) == max(after_seq, 3)
    assert items_of(state) == items


def test_a_row_that_changes_nothing_counts_toward_the_seq_applied(tmp_path):
    # A table can hold facts that the state does not keep, read for their `seq` alone
    # and applied by doing nothing. Its rows still count: a delta that ends with one
    # has been applied through it, and a delta of such rows alone is numbered by them.
    def keep(state: sqlite3.Connection, row: tuple) -> None:
        """A drop is kept as a fact, and changes nothing."""

    appliers = {"puts": APPLIERS["puts"], "drops": ("seq", keep)}
    state = new_state(tmp_path)
    conn, delta = new_delta(tmp_path)
    delta.put("a", "1")  # 1
    delta.drop("b")  # 2
    conn.commit()
    assert apply_rows(state, conn, appliers) == 2
    assert items_of(state) == {**BASE, "a": "1"}

    conn, delta = new_delta(tmp_path, "drops.db", first_seq=4)
    delta.drop("c")  # 4
    conn.commit()
    assert apply_rows(state, conn, appliers, after_seq=4) == 4
    with pytest.raises(ValueError, match="below after_seq"):
        apply_rows(state, conn, appliers, after_seq=5)


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param(
            [
                "INSERT INTO puts VALUES (3, 'a', '1')",
                "INSERT INTO drops VALUES (3, 'b')",
            ],
            id="in two tables",
        ),
        pytest.param(
            [
                "INSERT INTO puts VALUES (3, 'a', '1')",
                "INSERT INTO puts VALUES (3, 'd', '4')",
            ],
            id="in one table",
        ),
    ],
)
def test_two_rows_with_one_seq_are_refused(tmp_path, rows):
    # Order is by `seq`. Two rows that share one leave the order open, so the delta is
    # refused and not applied in some order of the tables or of the rows.
    state = new_state(tmp_path)
    conn, _ = new_delta(tmp_path)
    for row in rows:
        conn.execute(row)
    conn.commit()
    with pytest.raises(ValueError, match="two rows with seq 3"):
        apply_toy(state, conn)
    assert items_of(state) == BASE
    assert not state.in_transaction


def test_apply_rows_only_reads_the_delta(tmp_path):
    # A sealed delta can be opened read-only, and a replay must work from one.
    state = new_state(tmp_path)
    conn, delta = new_delta(tmp_path)
    delta.put("a", "1")
    conn.commit()
    conn.close()
    uri = f"{(tmp_path / 'delta.db').resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as readonly:
        assert apply_toy(state, readonly) == 1
    assert items_of(state) == {**BASE, "a": "1"}


# ---- one savepoint: all or nothing, and committed


def test_apply_rows_is_all_or_nothing(tmp_path):
    state = new_state(tmp_path)
    conn, delta = new_delta(tmp_path)
    delta.put("a", "1")
    delta.drop("b")
    delta.drop("ghost")  # the state has no item `ghost`
    conn.commit()
    with pytest.raises(ValueError, match="ghost") as caught:
        apply_toy(state, conn)
    # The error says which row it was.
    assert caught.value.__notes__ == ["while applying row 3 of the drops table"]
    # Nothing that came before the bad row was kept, and no transaction is left open.
    assert items_of(state) == BASE
    assert not state.in_transaction

    # The state can be used again, and the same delta applies once the bad row is gone.
    conn.execute("DELETE FROM drops WHERE key = 'ghost'")
    conn.commit()
    assert apply_toy(state, conn) == 2
    assert items_of(state) == {"a": "1", "c": "2"}


def test_a_failed_apply_leaves_the_callers_own_write_alone(tmp_path):
    # Only what the call did is undone. A write that the caller made before it, and has
    # not committed, is still there and is the caller's to commit or roll back.
    state = new_state(tmp_path)
    state.execute("INSERT INTO items VALUES ('note', 'before')")
    conn, delta = new_delta(tmp_path)
    delta.put("a", "1")
    delta.drop("ghost")
    conn.commit()
    with pytest.raises(ValueError, match="ghost"):
        apply_toy(state, conn)
    assert state.in_transaction
    assert items_of(state) == {**BASE, "note": "before"}
    state.rollback()
    assert items_of(state) == BASE


def test_apply_rows_commits_the_state(tmp_path):
    state = new_state(tmp_path)
    conn, delta = new_delta(tmp_path)
    delta.put("a", "1")
    conn.commit()
    apply_toy(state, conn)
    assert not state.in_transaction
    # Another connection to the file sees the change, so it is on disk.
    with closing(open_db(tmp_path / "state.db", STATE_SCHEMA)) as other:
        assert items_of(other) == {**BASE, "a": "1"}


def test_apply_rows_commits_inside_a_transaction_that_is_open(tmp_path):
    # A caller can have a write that is not committed yet. The state is committed when
    # `apply_rows` returns, that write with it.
    state = new_state(tmp_path)
    state.execute("INSERT INTO items VALUES ('note', 'before')")
    assert state.in_transaction
    conn, delta = new_delta(tmp_path)
    delta.put("a", "1")
    conn.commit()
    apply_toy(state, conn)
    assert not state.in_transaction
    with closing(open_db(tmp_path / "state.db", STATE_SCHEMA)) as other:
        assert items_of(other) == {**BASE, "note": "before", "a": "1"}


def test_atomic_lets_the_error_through_when_sqlite_has_ended_the_transaction(tmp_path):
    # SQLite ends the whole transaction by itself on some errors, such as a full disk,
    # and then there is no savepoint left to go back to. The error of the block is the
    # one that comes out, and not one about the savepoint, and nothing is kept.
    state = new_state(tmp_path)
    with pytest.raises(RuntimeError, match="the disk is full"), atomic(state):
        state.execute("INSERT INTO items VALUES ('a', '1')")
        state.execute("ROLLBACK")  # as SQLite does by itself
        raise RuntimeError("the disk is full")
    assert not state.in_transaction
    assert items_of(state) == BASE
