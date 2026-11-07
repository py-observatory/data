import ast
import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path

import pytest
from downloads_run_helpers import counts_file
from pypi_downloads import counts as counts_module
from pypi_downloads.counts import COUNTS_SCHEMA, CountsError, read_counts, write_day
from pypi_downloads.store import DELTA_SCHEMA, STATE_SCHEMA, Delta, day_number

from obskit.sqlite import open_db

DAY = date(2026, 11, 4)
NOW = "2026-11-06T04:00:00Z"
COUNTER_OUTPUT = Path(__file__).parents[1] / "counter" / "src" / "counter" / "output.py"


@pytest.fixture
def work(tmp_path):
    folder = tmp_path / "work"
    folder.mkdir()
    return folder


@pytest.fixture
def state(tmp_path):
    conn = open_db(tmp_path / "state.db", STATE_SCHEMA, create=True)
    conn.execute("INSERT INTO meta VALUES ('applied_seq', '0')")
    conn.commit()
    return conn


@pytest.fixture
def delta_conn(tmp_path):
    return open_db(tmp_path / "delta.db", DELTA_SCHEMA, create=True)


def counted_of(counts, work, **file_options):
    summary, db = counts_file(DAY, counts, **file_options)
    return read_counts(summary, db, DAY, work)


def test_counts_schema_matches_the_counters():
    tree = ast.parse(COUNTER_OUTPUT.read_text())
    (theirs,) = [
        node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and [t.id for t in node.targets if isinstance(t, ast.Name)] == ["COUNTS_SCHEMA"]
    ]
    assert COUNTS_SCHEMA == theirs


def test_read_counts_returns_the_day(work):
    counts = {"requests": (10, 2), "numpy": (5, 0)}
    got = counted_of(counts, work, null_project_rows=3)
    assert got.day == DAY
    assert got.counts == counts
    assert got.rows_counted == got.rows_bigquery == 18
    assert got.null_project_rows == 3
    assert got.partition_modified == "2026-11-04T05:00:00Z"
    assert got.read_bytes == 1000
    assert got.renamed == 0
    assert list(work.iterdir()) == []


def test_summary_that_is_not_an_object_is_refused(work):
    _, db = counts_file(DAY, {"a": (1, 0)})
    for bad in (b"not json", b"[1]", b"null"):
        with pytest.raises(CountsError, match="summary"):
            read_counts(bad, db, DAY, work)


def test_summary_without_a_key_is_refused(work):
    summary, db = counts_file(DAY, {"a": (1, 0)})
    parsed = json.loads(summary)
    for key in parsed:
        broken = {k: v for k, v in parsed.items() if k != key}
        with pytest.raises(CountsError, match=key):
            read_counts(json.dumps(broken).encode(), db, DAY, work)


@pytest.mark.parametrize(
    ("file_options", "reason"),
    [
        ({"db_sha256": "0" * 64}, "db_sha256"),
        ({"db_bytes": 1}, "says 1 bytes"),
        ({"meta": {"day": "2026-11-05"}}, "meta day"),
        ({"rows_bigquery": 99}, "rows_counted .* is not rows_bigquery"),
        (
            {"rows_counted": 6, "rows_bigquery": 6},
            "plus null_project_rows 0 is not rows_counted 6",
        ),
        ({"counts": {"": (1, 0)}}, "empty"),
        ({"counts": {"a" * 257: (1, 0)}}, "over 256"),
        ({"counts": {"a\x00b": (1, 0)}}, "not printable"),
    ],
    ids=[
        "digest",
        "size",
        "meta-day",
        "rows",
        "totals",
        "empty",
        "long",
        "unprintable",
    ],
)
def test_counts_file_refusals(work, file_options, reason):
    options = dict(file_options)
    counts = options.pop("counts", {"requests": (3, 0), "numpy": (2, 0)})
    summary, db = counts_file(DAY, counts, **options)
    with pytest.raises(CountsError, match=reason):
        read_counts(summary, db, DAY, work)
    assert list(work.iterdir()) == []


def sqlite_bytes(tmp_path, schema, rows):
    path = tmp_path / "raw.db"
    conn = sqlite3.connect(path)
    conn.executescript(schema)
    conn.executemany("INSERT INTO meta VALUES (?, ?)", [("day", DAY.isoformat())])
    for row in rows:
        conn.execute("INSERT INTO counts VALUES (?, ?, ?)", row)
    conn.commit()
    conn.close()
    return path.read_bytes()


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (("a", 0, 0), "below 1"),
        (("a", -1, 0), "below 1"),
        (("a", 2, 3), "outside 0..downloads"),
        (("a", 2, -1), "outside 0..downloads"),
        (("a", 2.5, 0), "downloads count that is not an integer"),
        (("a", 3, 1.5), "mirror count that is not an integer"),
        ((b"blob", 3, 0), "name is not text"),
    ],
)
def test_counts_out_of_range_are_refused(tmp_path, work, row, reason):
    # The counter's CHECKs would stop these rows, so the table is made without them.
    loose = COUNTS_SCHEMA.replace("CHECK (downloads > 0)", "").replace(
        "CHECK (mirror_downloads BETWEEN 0 AND downloads)", ""
    )
    raw = sqlite_bytes(tmp_path, loose, [row])
    summary, db = counts_file(DAY, {}, raw_db=raw, null_project_rows=0)
    parsed = json.loads(summary)
    parsed |= {"rows_counted": 3, "rows_bigquery": 3}
    with pytest.raises(CountsError, match=reason):
        read_counts(json.dumps(parsed).encode(), db, DAY, work)


def test_a_summary_for_another_day_is_refused(work):
    summary, db = counts_file(date(2026, 11, 5), {"a": (1, 0)})
    with pytest.raises(CountsError, match="2026-11-05, not 2026-11-04"):
        read_counts(summary, db, DAY, work)


def test_a_database_over_the_cap_is_refused(work, monkeypatch):
    monkeypatch.setattr(counts_module, "MAX_DB_BYTES", 1000)
    summary, db = counts_file(DAY, {f"p{i}": (1, 0) for i in range(2000)})
    with pytest.raises(CountsError, match="decompressed"):
        read_counts(summary, db, DAY, work)
    assert list(work.iterdir()) == []


def test_a_file_that_is_not_zstd_or_not_a_counts_database_is_refused(work, tmp_path):
    garbage = b"garbage" * 10
    summary, _ = counts_file(DAY, {"a": (1, 0)}, raw_db=b"x")
    parsed = json.loads(summary) | {
        "db_bytes": len(garbage),
        "db_sha256": hashlib.sha256(garbage).hexdigest(),
    }
    with pytest.raises(CountsError, match="zstd"):
        read_counts(json.dumps(parsed).encode(), garbage, DAY, work)
    summary, db = counts_file(DAY, {"a": (1, 0)}, raw_db=b"not a database at all")
    with pytest.raises(CountsError, match="opened"):
        read_counts(summary, db, DAY, work)


def test_a_name_that_is_not_canonical_is_merged(work):
    got = counted_of({"Foo_Bar": (5, 1), "foo-bar": (2, 0)}, work)
    assert got.counts == {"foo-bar": (7, 1)}
    assert got.renamed == 1


def counted_for(counts):
    rows = sum(d for d, _ in counts.values())
    return counts_module.Counted(
        day=DAY,
        rows_bigquery=rows,
        rows_counted=rows,
        partition_modified="2026-11-04T05:00:00Z",
        read_bytes=1000,
        null_project_rows=0,
        counts=counts,
        renamed=0,
    )


def test_write_day_gives_new_names_the_next_ids_in_name_order(state, delta_conn):
    state.execute("INSERT INTO projects VALUES (7, 'old', 100)")
    state.commit()
    counted = counted_for({"zebra": (1, 0), "apple": (2, 1), "old": (3, 0)})
    new = write_day(
        state,
        delta_conn,
        Delta(delta_conn, NOW),
        counted,
        execution=None,
        counted_at=NOW,
    )
    assert new == 2
    assert delta_conn.execute(
        "SELECT project_id, name, first_day FROM projects ORDER BY seq"
    ).fetchall() == [(8, "apple", day_number(DAY)), (9, "zebra", day_number(DAY))]
    assert delta_conn.execute(
        "SELECT * FROM counts ORDER BY project_id"
    ).fetchall() == [
        (7, day_number(DAY), 3, 0),
        (8, day_number(DAY), 2, 1),
        (9, day_number(DAY), 1, 0),
    ]


def test_write_day_reuses_known_ids(state, delta_conn):
    state.execute("INSERT INTO projects VALUES (1, 'requests', 50)")
    state.commit()
    delta = Delta(delta_conn, NOW)
    write_day(
        state,
        delta_conn,
        delta,
        counted_for({"requests": (1, 0)}),
        execution=None,
        counted_at=NOW,
    )
    # The next day of the same run: the state has not applied the first one's names.
    second = counted_for({"requests": (4, 0), "brand-new": (1, 0)})
    object.__setattr__(second, "day", date(2026, 11, 5))
    assert (
        write_day(state, delta_conn, delta, second, execution=None, counted_at=NOW) == 1
    )
    assert delta_conn.execute("SELECT project_id, name FROM projects").fetchall() == [
        (2, "brand-new")
    ]
    day5 = day_number(date(2026, 11, 5))
    assert delta_conn.execute(
        "SELECT project_id, downloads FROM counts WHERE day = ? ORDER BY 1", (day5,)
    ).fetchall() == [(1, 4), (2, 1)]


def test_write_day_writes_the_days_row_with_its_execution(state, delta_conn):
    counted = counted_for({"a": (3, 1), "b": (2, 0)})
    run = {"name": "executions/e1", "vcpu_seconds": 12.5, "gib_seconds": 6.25}
    write_day(
        state,
        delta_conn,
        Delta(delta_conn, NOW),
        counted,
        execution=run,
        counted_at=NOW,
    )
    row = delta_conn.execute(
        "SELECT day, rows_bigquery, rows_counted, partition_modified, projects, "
        "downloads, mirror_downloads, null_project_rows, read_bytes, execution, "
        "vcpu_seconds, gib_seconds, counted_at, observed_at FROM days"
    ).fetchone()
    assert row == (
        day_number(DAY),
        5,
        5,
        "2026-11-04T05:00:00Z",
        2,
        5,
        1,
        0,
        1000,
        "executions/e1",
        12.5,
        6.25,
        NOW,
        NOW,
    )


def test_write_day_without_an_execution_leaves_its_columns_null(state, delta_conn):
    write_day(
        state,
        delta_conn,
        Delta(delta_conn, NOW),
        counted_for({"a": (1, 0)}),
        execution=None,
        counted_at=NOW,
    )
    assert delta_conn.execute(
        "SELECT execution, vcpu_seconds, gib_seconds FROM days"
    ).fetchone() == (None, None, None)


def test_a_day_that_fails_partway_leaves_nothing_in_the_delta(state, delta_conn):
    delta = Delta(delta_conn, NOW)
    counted = counted_for({"a": (1, 0), "b": (1, 0)})
    write_day(state, delta_conn, delta, counted, execution=None, counted_at=NOW)
    before = {
        t: delta_conn.execute(f"SELECT * FROM {t}").fetchall()
        for t in ("projects", "days", "counts")
    }
    # The same day again: the first counts row is a duplicate key, after projects and
    # nothing else are written.
    again = counted_for({"a": (1, 0), "c": (1, 0)})
    with pytest.raises(sqlite3.IntegrityError):
        write_day(state, delta_conn, delta, again, execution=None, counted_at=NOW)
    after = {
        t: delta_conn.execute(f"SELECT * FROM {t}").fetchall()
        for t in ("projects", "days", "counts")
    }
    assert after == before
