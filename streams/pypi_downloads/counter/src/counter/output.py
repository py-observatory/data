"""The counter's output for one day: a SQLite database and its summary."""

import compression.zstd
import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

import pyarrow as pa

# The stream keeps a copy of this text and checks it against this file, so it stays
# one plain string literal.
COUNTS_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE counts (
    project TEXT PRIMARY KEY,
    downloads INTEGER NOT NULL CHECK (downloads > 0),
    mirror_downloads INTEGER NOT NULL CHECK (mirror_downloads BETWEEN 0 AND downloads)
) WITHOUT ROWID;
"""

ZSTD_LEVEL = 19


def write_counts(table: pa.Table, meta: dict[str, str], path: Path) -> None:
    """Write the day's database, uncompressed, replacing any half-written file."""
    path.unlink(missing_ok=True)
    with closing(sqlite3.connect(path)) as db:
        db.executescript(COUNTS_SCHEMA)
        with db:
            db.executemany("INSERT INTO meta VALUES (?, ?)", meta.items())
            db.executemany(
                "INSERT INTO counts VALUES (?, ?, ?)",
                zip(
                    table.column("project").to_pylist(),
                    table.column("downloads").to_pylist(),
                    table.column("mirror_downloads").to_pylist(),
                ),
            )


def compress(path: Path) -> tuple[Path, int, str]:
    """Compress the database next to itself; return the file, its size and sha256."""
    packed = compression.zstd.compress(path.read_bytes(), level=ZSTD_LEVEL)
    target = path.with_suffix(".db.zst")
    target.write_bytes(packed)
    return target, len(packed), hashlib.sha256(packed).hexdigest()


def summary(meta: dict[str, str], size: int, sha256: str) -> dict:
    """The JSON that marks a finished day; ``size`` and ``sha256`` are of the compressed file."""
    return {
        "day": meta["day"],
        "db_bytes": size,
        "db_sha256": sha256,
        "rows_counted": int(meta["rows_counted"]),
        "rows_bigquery": int(meta["rows_bigquery"]),
        "partition_modified": meta["partition_modified"],
        "read_bytes": int(meta["read_bytes"]),
        "null_project_rows": int(meta["null_project_rows"]),
    }
