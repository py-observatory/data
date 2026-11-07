import sqlite3
from datetime import date

import pytest
from pypi_downloads.config import load
from pypi_downloads.store import (
    DELTA_SCHEMA,
    STATE_SCHEMA,
    STATE_TABLES,
    Delta,
    apply,
    day_date,
    day_number,
    insert_counts,
    meta,
)

from obskit.cycle import Store
from obskit.sqlite import open_db

NOW = "2026-11-06T04:00:00Z"


def day_columns(day, **over):
    row = {
        "day": day,
        "rows_bigquery": 10,
        "rows_counted": 10,
        "partition_modified": NOW,
        "projects": 2,
        "downloads": 30,
        "mirror_downloads": 3,
        "null_project_rows": 0,
        "read_bytes": 1000,
        "execution": "e1",
        "vcpu_seconds": 1.5,
        "gib_seconds": 0.5,
        "counted_at": NOW,
    }
    return row | over


@pytest.fixture
def state(tmp_path):
    conn = open_db(tmp_path / "state.db", STATE_SCHEMA, create=True)
    conn.execute("INSERT INTO meta VALUES ('applied_seq', '0')")
    conn.commit()
    return conn


@pytest.fixture
def delta_conn(tmp_path):
    return open_db(tmp_path / "delta.db", DELTA_SCHEMA, create=True)


def test_store_names_every_state_table():
    c = load()
    Store(
        stream="pypi-downloads",
        state_schema=STATE_SCHEMA,
        delta_schema=DELTA_SCHEMA,
        state_tables=STATE_TABLES,
        classes=c.tables,
        licences=c.upstreams,
        apply=apply,
    )


def test_apply_adds_projects_days_breaks_and_upserts_spend(state, delta_conn):
    d = Delta(delta_conn, NOW)
    d.project(1, "requests")
    d.project(2, "numpy")
    d.day(**day_columns(100))
    d.series_break(100, "a break")
    d.spend("2026-11", 0.0, 1.0, "2026-11-05T00:00:00Z")
    d.spend("2026-11", 0.5, 1.0, "2026-11-06T00:00:00Z")
    delta_conn.commit()
    apply(state, delta_conn)
    assert state.execute("SELECT * FROM projects ORDER BY project_id").fetchall() == [
        (1, "requests"),
        (2, "numpy"),
    ]
    day = state.execute("SELECT day, execution, seq FROM days").fetchall()
    assert day == [(100, "e1", 3)]
    assert state.execute("SELECT * FROM breaks").fetchall() == [(100, "a break")]
    assert state.execute("SELECT * FROM spend").fetchall() == [
        ("2026-11", 0.5, 1.0, "2026-11-06T00:00:00Z")
    ]
    assert meta(state, "applied_seq") == "6"


def test_an_older_notice_never_replaces_a_newer_one(state, delta_conn):
    # Pub/Sub can deliver an older notice after a newer one, in the same run or a
    # later one. The older notice's lower cost must not lift a stop.
    d = Delta(delta_conn, NOW)
    d.spend("2026-11", 0.5, 1.0, "2026-11-06T00:00:00Z")
    d.spend("2026-11", 0.0, 1.0, "2026-11-05T00:00:00Z")
    delta_conn.commit()
    apply(state, delta_conn)
    newest = [("2026-11", 0.5, 1.0, "2026-11-06T00:00:00Z")]
    assert state.execute("SELECT * FROM spend").fetchall() == newest

    Delta(delta_conn, NOW).spend("2026-11", 0.0, 1.0, "2026-11-05T12:00:00Z")
    delta_conn.commit()
    apply(state, delta_conn)
    assert state.execute("SELECT * FROM spend").fetchall() == newest
    assert meta(state, "applied_seq") == "3"


def test_the_newest_clearance_of_a_month_wins(state, delta_conn):
    d = Delta(delta_conn, NOW)
    d.clearance("2026-11", 0.25, "a rounding charge")
    d.clearance("2026-10", 0.01, "a test run")
    d.clearance("2026-11", 0.4, "the same charge, billed in full")
    delta_conn.commit()
    apply(state, delta_conn)
    assert state.execute("SELECT * FROM clearances ORDER BY month").fetchall() == [
        ("2026-10", 0.01, "a test run", NOW),
        ("2026-11", 0.4, "the same charge, billed in full", NOW),
    ]

    # A later run's clearance replaces the month's, with that run's time.
    later = "2026-11-07T04:00:00Z"
    Delta(delta_conn, later).clearance("2026-11", 0.5, "looked again")
    delta_conn.commit()
    apply(state, delta_conn)
    assert state.execute("SELECT * FROM clearances ORDER BY month").fetchall() == [
        ("2026-10", 0.01, "a test run", NOW),
        ("2026-11", 0.5, "looked again", later),
    ]
    assert meta(state, "applied_seq") == "4"


def test_apply_never_reads_counts(state, delta_conn):
    d = Delta(delta_conn, NOW)
    d.project(1, "a")
    d.project(2, "b")
    delta_conn.commit()
    assert insert_counts(delta_conn, [(1, 1, 5, 0), (2, 1, 6, 1), (1, 2, 7, 0)]) == 3
    delta_conn.commit()
    apply(state, delta_conn)
    assert meta(state, "applied_seq") == "2"
    tables = {r[0] for r in state.execute("SELECT name FROM sqlite_master")}
    assert "counts" not in tables


def test_apply_twice_applies_each_row_once(state, delta_conn):
    d = Delta(delta_conn, NOW)
    d.project(1, "a")
    delta_conn.commit()
    apply(state, delta_conn)
    apply(state, delta_conn)
    assert state.execute("SELECT count(*) FROM projects").fetchone() == (1,)
    assert meta(state, "applied_seq") == "1"


def test_a_project_id_taken_twice_is_refused(state, delta_conn):
    d = Delta(delta_conn, NOW)
    d.project(1, "a")
    d.project(1, "b")
    delta_conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        apply(state, delta_conn)
    assert state.execute("SELECT count(*) FROM projects").fetchone() == (0,)
    assert meta(state, "applied_seq") == "0"


def test_a_day_or_break_taken_twice_is_refused(state, delta_conn):
    d = Delta(delta_conn, NOW)
    d.day(**day_columns(100))
    d.day(**day_columns(100))
    delta_conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        apply(state, delta_conn)


def test_counts_key_refuses_a_day_twice(delta_conn):
    insert_counts(delta_conn, [(1, 5, 10, 0)])
    with pytest.raises(sqlite3.IntegrityError):
        insert_counts(delta_conn, [(1, 5, 10, 0)])


def test_a_day_row_with_a_wrong_key_is_refused(delta_conn):
    d = Delta(delta_conn, NOW)
    with pytest.raises(ValueError, match="missing: counted_at"):
        d.day(**{k: v for k, v in day_columns(1).items() if k != "counted_at"})


def test_a_delta_goes_on_after_its_highest_seq_and_ignores_counts(delta_conn):
    d = Delta(delta_conn, NOW)
    d.project(1, "a")
    insert_counts(delta_conn, [(1, 1, 5, 0)])
    delta_conn.commit()
    Delta(delta_conn, NOW).project(2, "b")
    assert delta_conn.execute("SELECT seq FROM projects ORDER BY seq").fetchall() == [
        (1,),
        (2,),
    ]


def test_apply_without_applied_seq_is_refused(tmp_path, delta_conn):
    bare = open_db(tmp_path / "bare.db", STATE_SCHEMA, create=True)
    with pytest.raises(ValueError, match="applied_seq"):
        apply(bare, delta_conn)


def test_day_numbers():
    assert day_number(date(2026, 11, 4)) == 20761
    assert day_date(20761) == date(2026, 11, 4)
