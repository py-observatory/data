import json
import sqlite3
import tomllib
from collections.abc import Callable, Iterator
from contextlib import closing
from pathlib import Path

import pytest

from obskit.sqlite import open_db
from osv_pypi.records import VERSION_COLUMNS, parse_record
from osv_pypi.store import (
    DELTA_SCHEMA,
    STATE_SCHEMA,
    STATE_TABLES,
    Delta,
    apply,
    apply_delta,
)

FIXTURES = Path(__file__).parent / "fixtures"
STREAM_TOML = Path(__file__).parents[1] / "stream.toml"
GHSA = "GHSA-4m7q-9xv2-hc3p"
MAL = "MAL-2026-40017"
PYSEC = "PYSEC-2019-311"
# An advisory that no state of these tests holds.
UNKNOWN = "GHSA-0000-0000-0000"
# The times of three runs, a day apart. The first built the state.
FIRST_RUN = "2026-11-01T06:00:00Z"
SECOND_RUN = "2026-11-02T06:00:00Z"
THIRD_RUN = "2026-11-03T06:00:00Z"
# A time after the fixture's, for a later version of the GHSA advisory.
LATER = "2025-06-01T08:00:00Z"

type Writer = Callable[[Delta], None]


def version(name: str, **edits: object) -> dict:
    """The version row of the record in fixtures/ called `name`, with the keys of
    `edits` set in the record first, as `records.parse_record` makes it."""
    record = json.loads((FIXTURES / f"{name}.json").read_bytes())
    record.update(edits)
    return parse_record(json.dumps(record).encode(), advisory_id=record["id"])


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
    """A new state, as the first run starts it: nothing applied, and no advisory."""
    state = opened(open_db(tmp_path / "state.db", STATE_SCHEMA, create=True))
    state.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        [("applied_seq", "0"), ("bootstrap_at", FIRST_RUN)],
    )
    state.commit()
    return state


def new_delta(
    tmp_path: Path, name: str = "delta.db", observed_at: str = FIRST_RUN, **options: int
) -> tuple[sqlite3.Connection, Delta]:
    conn = opened(open_db(tmp_path / name, DELTA_SCHEMA, create=True))
    return conn, Delta(conn, observed_at, **options)


def meta(state: sqlite3.Connection) -> dict[str, str]:
    return dict(state.execute("SELECT key, value FROM meta"))


def run_delta(
    state: sqlite3.Connection, tmp_path: Path, observed_at: str
) -> tuple[sqlite3.Connection, Delta]:
    """A new delta for a run at `observed_at`, numbered on from what the state has
    applied, as a run opens one."""
    name = f"run-{len(list(tmp_path.glob('run-*.db')))}.db"
    first_seq = int(meta(state)["applied_seq"]) + 1
    return new_delta(tmp_path, name, observed_at, first_seq=first_seq)


def run(
    state: sqlite3.Connection, tmp_path: Path, observed_at: str, *writes: Writer
) -> sqlite3.Connection:
    """A run at `observed_at`: write a row with each of `writes` into its delta, and
    apply the delta to the state with `apply`. Returns the delta."""
    conn, delta = run_delta(state, tmp_path, observed_at)
    for write in writes:
        write(delta)
    conn.commit()
    apply(state, conn)
    return conn


def refused(
    state: sqlite3.Connection,
    tmp_path: Path,
    observed_at: str,
    write: Writer,
    match: str,
) -> None:
    """A run at `observed_at` that writes one row with `write`, which `apply` refuses
    with a ValueError that matches `match`. The state is as it was."""
    before = state_rows(state)
    conn, delta = run_delta(state, tmp_path, observed_at)
    write(delta)
    conn.commit()
    with pytest.raises(ValueError, match=match):
        apply(state, conn)
    assert state_rows(state) == before
    assert not state.in_transaction


def advisory(state: sqlite3.Connection, advisory_id: str) -> dict:
    """The state's row of the advisory, by column."""
    cursor = state.execute("SELECT * FROM advisories WHERE id = ?", (advisory_id,))
    columns = [column for column, *_ in cursor.description]
    (row,) = cursor.fetchall()
    return dict(zip(columns, row, strict=True))


def state_rows(state: sqlite3.Connection) -> dict[str, list[tuple]]:
    """Every row of each table of the state, sorted."""
    return {
        table: sorted(state.execute(f"SELECT * FROM {table}")) for table in STATE_TABLES
    }


# ---- the schemas


def columns_of(conn: sqlite3.Connection, table: str) -> list[tuple[str, bool]]:
    """Each column of `table` in order, with whether it must have a value."""
    info = conn.execute(f"PRAGMA table_info({table})")
    return [(name, bool(notnull)) for _, name, _, notnull, _, _ in info]


def test_the_tables_are_those_of_stream_toml_and_hold_the_version_columns():
    config = tomllib.loads(STREAM_TOML.read_text(encoding="utf-8"))
    with closing(sqlite3.connect(":memory:")) as scratch:
        scratch.executescript(STATE_SCHEMA)
        scratch.executescript(DELTA_SCHEMA)
        made = scratch.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        # The publish gates read the history class of each table from stream.toml.
        assert sorted(name for (name,) in made) == sorted(config["tables"])
        # An advisory keeps every column of its last version but the id, which is its
        # key. A version has a kind, the time it was modified with its key, and the
        # PyPI entries that it affects, and the rest may be null.
        required = {"kind", "modified", "modified_key", "affected"}
        assert columns_of(scratch, "advisories") == [
            ("id", True),
            ("status", True),
            ("first_seen", True),
            ("seq", True),
            *((column, column in required) for column in VERSION_COLUMNS[1:]),
        ]
        versions = [name for name, _ in columns_of(scratch, "versions")]
        assert versions == ["seq", "observed_at", *VERSION_COLUMNS]
        # A status, a kind of advisory and a kind of event is one of those it can be.
        for statement in [
            "INSERT INTO advisories (id, status, first_seen, seq, kind, modified, "
            "modified_key, affected) VALUES ('X', 'gone', 't', 1, 'malware', 't', "
            "'t', '[]')",
            "INSERT INTO advisories (id, status, first_seen, seq, kind, modified, "
            "modified_key, affected) VALUES ('X', 'active', 't', 1, 'other', 't', "
            "'t', '[]')",
            "INSERT INTO events VALUES (1, 'X', 'deleted', 't')",
        ]:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
                scratch.execute(statement)


# ---- versions


def test_new_then_changed_version(tmp_path):
    state = new_state(tmp_path)
    first = version("ghsa")
    delta = run(state, tmp_path, FIRST_RUN, lambda d: d.version(first))
    # The delta holds the version, observed at the time of its run.
    values = tuple(first[column] for column in VERSION_COLUMNS)
    assert delta.execute("SELECT * FROM versions").fetchall() == [
        (1, FIRST_RUN, *values)
    ]
    # A new advisory is active, first seen when its first version was observed, and
    # holds that version, with the seq of the row that holds it in the delta.
    assert advisory(state, GHSA) == {
        **first,
        "status": "active",
        "first_seen": FIRST_RUN,
        "seq": 1,
    }

    # A later version sets every column, one that it no longer has among them, and its
    # seq. The advisory keeps its first sighting.
    later = version("ghsa", modified=LATER, severity=None, withdrawn=LATER)
    assert first["severity"] is not None
    run(state, tmp_path, SECOND_RUN, lambda d: d.version(later))
    assert advisory(state, GHSA) == {
        **later,
        "status": "active",
        "first_seen": FIRST_RUN,
        "seq": 2,
    }
    # Each run stored the seq that it applied through, and committed.
    assert meta(state) == {"applied_seq": "2", "bootstrap_at": FIRST_RUN}
    assert not state.in_transaction


def test_version_for_removed_refused(tmp_path):
    # A removed advisory takes no version until a return makes it active again, and a
    # run writes the return first when the index lists it again. A version without one
    # means that the delta and the state disagree.
    state = new_state(tmp_path)
    run(
        state,
        tmp_path,
        FIRST_RUN,
        lambda d: d.version(version("ghsa")),
        lambda d: d.removed(GHSA),
    )
    later = version("ghsa", modified=LATER)
    refused(
        state,
        tmp_path,
        SECOND_RUN,
        lambda d: d.version(later),
        f"the state holds advisory '{GHSA}' as removed",
    )


def test_modified_going_back_refused(tmp_path):
    # A version older than the one that the state holds is a stale copy, and applying
    # it would put the old version back.
    state = new_state(tmp_path)
    held = version("ghsa")
    run(state, tmp_path, FIRST_RUN, lambda d: d.version(held))
    # The times compare by their keys. As text, a whole second sorts after its own
    # fractions, so this earlier time would pass for a later one.
    older = version("ghsa", modified="2025-03-04T17:20:11Z")
    assert older["modified"] > held["modified"]
    refused(
        state,
        tmp_path,
        SECOND_RUN,
        lambda d: d.version(older),
        "before the version that the state holds",
    )
    # A version with the same time does not go back, and it applies.
    again = version("ghsa", summary="example-archive writes outside its directory")
    run(state, tmp_path, SECOND_RUN, lambda d: d.version(again))
    assert advisory(state, GHSA)["summary"] == again["summary"]


# ---- removals and returns


def test_removed_then_returned(tmp_path):
    state = new_state(tmp_path)
    run(
        state,
        tmp_path,
        FIRST_RUN,
        lambda d: d.version(version("ghsa")),  # 1
        lambda d: d.version(version("pysec")),  # 2
    )
    held = advisory(state, GHSA)
    # A return needs an advisory that was removed.
    refused(
        state,
        tmp_path,
        SECOND_RUN,
        lambda d: d.returned(GHSA),
        f"the state has no removed advisory '{GHSA}'",
    )

    # A removal sets the status alone, and the advisory keeps its last version.
    run(state, tmp_path, SECOND_RUN, lambda d: d.removed(GHSA))  # 3
    assert advisory(state, GHSA) == {**held, "status": "removed"}
    # A removal needs an advisory that is active.
    refused(
        state,
        tmp_path,
        THIRD_RUN,
        lambda d: d.removed(GHSA),
        f"the state has no active advisory '{GHSA}'",
    )

    # A return makes it active again, so a version that follows the return applies.
    later = version("ghsa", modified=LATER)
    run(
        state,
        tmp_path,
        THIRD_RUN,
        lambda d: d.returned(GHSA),  # 4
        lambda d: d.version(later),  # 5
    )
    assert advisory(state, GHSA) == {
        **later,
        "status": "active",
        "first_seen": FIRST_RUN,
        "seq": 5,
    }
    # Neither event applies to an advisory that the state lacks.
    for write, match in [
        (lambda d: d.removed(UNKNOWN), "no active advisory"),
        (lambda d: d.returned(UNKNOWN), "no removed advisory"),
    ]:
        refused(state, tmp_path, THIRD_RUN, write, match)
    # The other advisory was left alone.
    assert advisory(state, PYSEC)["status"] == "active"


# ---- the cursor


def test_cursor_going_back_refused(tmp_path):
    # The cursor is the generation of the index that was read, which only grows, so an
    # index with a lower one is a stale copy.
    state = new_state(tmp_path)
    run(state, tmp_path, FIRST_RUN, lambda d: d.cursor(999))
    # The first cursor is added, as text, as meta holds every value.
    assert meta(state)["cursor"] == "999"
    # A run that reads the same index again writes the same generation, and a newer
    # index moves the cursor on. Generations compare as numbers: as text, 1000 would
    # sort before 999, and 998 after 1000.
    run(
        state,
        tmp_path,
        SECOND_RUN,
        lambda d: d.cursor(999),
        lambda d: d.cursor(1000),
    )
    assert meta(state)["cursor"] == "1000"
    refused(
        state,
        tmp_path,
        THIRD_RUN,
        lambda d: d.cursor(998),
        "from generation 1000 to 998",
    )


# ---- the rules of every delta


def test_two_rows_one_seq_refused(tmp_path):
    # Order is by seq, so two rows that share one leave it open. The delta is refused,
    # and the first of the two, which was applied, is undone.
    state = new_state(tmp_path)
    before = state_rows(state)
    conn, delta = new_delta(tmp_path, first_seq=3)
    delta.version(version("ghsa"))  # 3
    conn.execute(
        "INSERT INTO cursors (seq, generation, observed_at) VALUES (3, 999, ?)",
        (FIRST_RUN,),
    )
    conn.commit()
    with pytest.raises(ValueError, match="two rows with seq 3"):
        apply_delta(state, conn)
    assert state_rows(state) == before
    assert not state.in_transaction


def test_delta_below_after_seq_refused(tmp_path):
    # A delta whose rows are all below what the state has applied was not numbered on
    # from it, so none of its rows would ever be applied.
    state = new_state(tmp_path)
    before = state_rows(state)
    conn, delta = new_delta(tmp_path)
    delta.version(version("ghsa"))  # 1
    delta.cursor(999)  # 2
    conn.commit()
    with pytest.raises(ValueError, match="is 2, which is below after_seq 3"):
        apply_delta(state, conn, after_seq=3)
    assert state_rows(state) == before
    # A delta whose highest seq is after_seq has nothing left to apply, and nor has an
    # empty one. Neither is an error.
    assert apply_delta(state, conn, after_seq=2) == 2
    empty, _ = new_delta(tmp_path, "empty.db")
    assert apply_delta(state, empty, after_seq=7) == 7
    assert state_rows(state) == before


def test_a_failed_row_undoes_the_whole_apply(tmp_path):
    state = new_state(tmp_path)
    before = state_rows(state)
    conn, delta = new_delta(tmp_path)
    delta.version(version("ghsa"))  # 1
    delta.version(version("mal"))  # 2
    delta.cursor(999)  # 3
    delta.removed(PYSEC)  # 4: the state has no such advisory
    conn.commit()
    with pytest.raises(ValueError, match=PYSEC) as caught:
        apply(state, conn)
    # The error says which row it was.
    assert caught.value.__notes__ == ["while applying row 4 of the events table"]
    # Nothing that came before the bad row was kept, the applied_seq among it, and no
    # transaction is left open.
    assert state_rows(state) == before
    assert not state.in_transaction


@pytest.mark.parametrize(
    "write",
    [
        pytest.param(lambda d: d.version(version("ghsa")), id="versions"),
        pytest.param(lambda d: d.removed(GHSA), id="events"),
        pytest.param(lambda d: d.cursor(999), id="cursors"),
    ],
)
def test_delta_numbering_continues_after_reopen(tmp_path, write):
    # The runs of a day add to one open delta, and each opens it again. Whichever table
    # holds its highest seq, the next row is numbered after it, though the run starts
    # from what the state has applied, which can be lower.
    path = tmp_path / "delta.db"
    with closing(open_db(path, DELTA_SCHEMA, create=True)) as conn:
        write(Delta(conn, FIRST_RUN, first_seq=40))
        conn.commit()
    with closing(open_db(path, DELTA_SCHEMA)) as conn:
        Delta(conn, FIRST_RUN).returned(MAL)
        conn.commit()
        found = conn.execute("SELECT seq FROM events WHERE id = ?", (MAL,))
        assert found.fetchall() == [(41,)]


def test_a_version_needs_exactly_the_version_columns(tmp_path):
    # A row with a key that is not a column is refused, and not cut down to the
    # columns, and so is a row that lacks one: a key that is left out is data lost.
    conn, delta = new_delta(tmp_path)
    row = version("ghsa")
    lacking = {key: value for key, value in row.items() if key != "summary"}
    more = {**row, "details": "the text of the details, which is never kept"}
    with pytest.raises(ValueError, match="missing: summary; not columns: none"):
        delta.version(lacking)
    with pytest.raises(ValueError, match="missing: none; not columns: details"):
        delta.version(more)
    # Neither was written, and neither took a seq.
    delta.cursor(999)
    conn.commit()
    assert conn.execute("SELECT count(*) FROM versions").fetchone() == (0,)
    assert conn.execute("SELECT seq FROM cursors").fetchall() == [(1,)]


def test_apply_needs_the_applied_seq_that_a_bootstrap_writes(tmp_path):
    # Without it, which rows the state has applied is not known.
    state = opened(open_db(tmp_path / "state.db", STATE_SCHEMA, create=True))
    conn, delta = new_delta(tmp_path)
    delta.cursor(999)
    conn.commit()
    with pytest.raises(ValueError, match="no applied_seq in meta"):
        apply(state, conn)
    assert meta(state) == {}
