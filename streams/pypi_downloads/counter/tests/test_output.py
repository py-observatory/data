import compression.zstd
import hashlib
import sqlite3
from contextlib import closing

import pyarrow as pa
import pytest

from counter.output import COUNTS_SCHEMA, compress, summary, write_counts

META = {
    "day": "2026-11-04",
    "rows_counted": "5",
    "rows_bigquery": "6",
    "partition_modified": "2026-11-05T01:02:03Z",
    "read_bytes": "1234",
    "null_project_rows": "1",
    "started_at": "2026-11-06T10:00:00Z",
    "finished_at": "2026-11-06T10:05:00Z",
    "streams": "4",
    "workers": "2",
}


def make_table(rows):
    return pa.table(
        {
            "project": [r[0] for r in rows],
            "downloads": pa.array([r[1] for r in rows], pa.int64()),
            "mirror_downloads": pa.array([r[2] for r in rows], pa.int64()),
        }
    )


def test_write_counts_and_summary(tmp_path):
    path = tmp_path / "2026-11-04.db"
    table = make_table([("boto3", 1, 0), ("numpy", 3, 1)])
    write_counts(table, META, path)
    with closing(sqlite3.connect(path)) as db:
        assert dict(db.execute("SELECT key, value FROM meta")) == META
        assert db.execute("SELECT project, downloads, mirror_downloads FROM counts "
                          "ORDER BY project").fetchall() == [("boto3", 1, 0), ("numpy", 3, 1)]
    zst, size, digest = compress(path)
    info = summary(META, size, digest)
    assert set(info) == {"day", "db_bytes", "db_sha256", "rows_counted", "rows_bigquery",
                         "partition_modified", "read_bytes", "null_project_rows"}
    assert info["day"] == "2026-11-04" and info["db_bytes"] == size
    assert info["db_sha256"] == digest


def test_summary_numbers_are_integers():
    info = summary(META, 10, "ab")
    assert info["rows_counted"] == 5 and info["rows_bigquery"] == 6
    assert info["read_bytes"] == 1234 and info["null_project_rows"] == 1
    assert info["partition_modified"] == "2026-11-05T01:02:03Z"


def test_the_database_has_the_counts_file_tables(tmp_path):
    path = tmp_path / "d.db"
    write_counts(make_table([("a", 1, 0)]), META, path)
    with closing(sqlite3.connect(path)) as db:
        names = {row[0] for row in
                 db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert names == {"meta", "counts"}
    assert "WITHOUT ROWID" in COUNTS_SCHEMA


def test_compress_round_trips_with_the_standard_library(tmp_path):
    path = tmp_path / "d.db"
    write_counts(make_table([("a", 1, 0), ("b", 2, 2)]), META, path)
    zst, size, digest = compress(path)
    assert zst == tmp_path / "d.db.zst"
    data = zst.read_bytes()
    assert compression.zstd.decompress(data) == path.read_bytes()
    assert size == len(data)
    assert digest == hashlib.sha256(data).hexdigest()


def test_write_counts_refuses_a_mirror_count_above_downloads(tmp_path):
    with pytest.raises(sqlite3.IntegrityError):
        write_counts(make_table([("a", 1, 2)]), META, tmp_path / "d.db")


def test_write_counts_refuses_a_zero_download_count(tmp_path):
    with pytest.raises(sqlite3.IntegrityError):
        write_counts(make_table([("a", 0, 0)]), META, tmp_path / "d.db")


def test_write_counts_replaces_a_half_written_file(tmp_path):
    path = tmp_path / "d.db"
    path.write_bytes(b"junk")
    write_counts(make_table([("a", 1, 0)]), META, path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT count(*) FROM counts").fetchone() == (1,)
