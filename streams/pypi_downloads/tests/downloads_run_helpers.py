"""Builders of the stream's test fixtures. The counts file builder writes the counter's
layout from the stream's own copy of its schema, because the tests cannot import the
counter."""

import hashlib
import json
import sqlite3
from compression import zstd
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from pypi_downloads.counts import COUNTS_SCHEMA


def counts_file(
    day: date,
    counts: dict[str, tuple[int, int]],
    *,
    null_project_rows: int = 0,
    summary: dict | bytes | None = None,
    raw_db: bytes | None = None,
    meta: dict[str, str] | None = None,
    **overrides,
) -> tuple[bytes, bytes]:
    """The summary JSON and the compressed database of one counted day, as the counter
    writes them. `counts` maps each project name to `(downloads, mirror_downloads)`.

    Rows are written as given, so a name need not be canonical. Each refusal test breaks
    one thing: `overrides` replace keys of the summary, `meta` replaces keys of the
    database's meta table, `summary` replaces the whole summary (a dict is encoded, bytes
    are used as they are), and `raw_db` replaces the database's bytes before they are
    compressed (the summary's digest and size then describe the replacement).
    """
    rows = sum(d for d, _ in counts.values()) + null_project_rows
    values = {
        "day": day.isoformat(),
        "rows_counted": rows,
        "rows_bigquery": rows,
        "partition_modified": f"{day.isoformat()}T05:00:00Z",
        "read_bytes": 1000,
        "null_project_rows": null_project_rows,
    }
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "day.db"
        if raw_db is None:
            conn = sqlite3.connect(path)
            conn.executescript(COUNTS_SCHEMA)
            stored = {k: str(v) for k, v in values.items()} | {
                "started_at": "2026-11-05T00:00:00Z",
                "finished_at": "2026-11-05T00:10:00Z",
                "streams": "4",
                "workers": "2",
            }
            conn.executemany(
                "INSERT INTO meta VALUES (?, ?)", (stored | (meta or {})).items()
            )
            conn.executemany(
                "INSERT INTO counts VALUES (?, ?, ?)",
                [(name, d, m) for name, (d, m) in counts.items()],
            )
            conn.commit()
            conn.close()
            raw_db = path.read_bytes()
    compressed = zstd.compress(raw_db)
    if summary is None:
        summary = (
            values
            | {
                "db_bytes": len(compressed),
                "db_sha256": hashlib.sha256(compressed).hexdigest(),
            }
            | overrides
        )
    if isinstance(summary, dict):
        summary = json.dumps(summary).encode()
    return summary, compressed
