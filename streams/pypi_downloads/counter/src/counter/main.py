"""One run counts one day of PyPI downloads and hands the result to the bucket.

The day is read through the BigQuery Storage Read API, which scans no SQL and reads
only that day's partition. The count is then checked against the partition's own row
count, taken from the same snapshot, and only a day that agrees is uploaded.
"""

import json
import multiprocessing
import os
import re
import tempfile
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timedelta, UTC
from pathlib import Path

import pyarrow as pa

from counter.output import compress, summary, write_counts
from counter.tally import Tally, merge

TABLE = "projects/bigquery-public-data/datasets/pypi/tables/file_downloads"
PARTITIONS_SQL = (
    "SELECT total_rows, FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', last_modified_time) "
    "FROM `bigquery-public-data.pypi.INFORMATION_SCHEMA.PARTITIONS` "
    "WHERE table_name = 'file_downloads' AND partition_id = @partition"
)

# Each stream is a unit of work for a worker, so more streams than workers keeps
# the workers evenly busy when some streams hold more rows than others.
STREAMS_PER_WORKER = 4
ATTEMPTS = 3
UPLOAD_RETRY_SECONDS = 300


class SnapshotMoved(RuntimeError):
    """The partition changed while it was being read."""


class RowsMismatch(RuntimeError):
    """The rows counted are not the rows the partition holds."""


def check_rows(counted: int, before: tuple[int, str], after: tuple[int, str]) -> None:
    """Refuse a count taken while the partition moved, or one that is not complete."""
    if before != after:
        raise SnapshotMoved(f"partition moved during the read: {before} then {after}")
    if counted != before[0]:
        raise RowsMismatch(f"counted {counted} rows, the partition holds {before[0]}")


_client = None


def _open_client() -> None:
    """Pool initializer: one Read API client per worker, and one Arrow thread."""
    global _client
    from google.cloud import bigquery_storage_v1

    pa.set_cpu_count(1)
    _client = bigquery_storage_v1.BigQueryReadClient()


def _read_stream(job: tuple[str, bytes]) -> tuple[pa.Table, int, int]:
    """Tally one stream's batches; return its table, rows and null-project rows."""
    stream_name, serialized_schema = job
    schema = pa.ipc.read_schema(pa.py_buffer(serialized_schema))
    tally = Tally()
    for response in _client.read_rows(stream_name):
        batch = pa.ipc.read_record_batch(
            pa.py_buffer(response.arrow_record_batch.serialized_record_batch), schema
        )
        tally.add(batch)
    return tally.table(), tally.rows, tally.null_project_rows


def count_day(day: date, *, project: str, workers: int) -> tuple[pa.Table, dict[str, str]]:
    """Count one day's downloads per project; return the table and what to record."""
    from google.cloud import bigquery_storage_v1
    from google.cloud.bigquery_storage_v1 import types

    options = types.ReadSession.TableReadOptions(
        selected_fields=["project", "details.installer.name"],
        row_restriction=f"timestamp >= '{day}' AND timestamp < '{day + timedelta(days=1)}'",
        arrow_serialization_options=types.ArrowSerializationOptions(
            buffer_compression=types.ArrowSerializationOptions.CompressionCodec.LZ4_FRAME
        ),
    )
    session = bigquery_storage_v1.BigQueryReadClient().create_read_session(
        parent=f"projects/{project}",
        read_session=types.ReadSession(
            table=TABLE, data_format=types.DataFormat.ARROW, read_options=options
        ),
        max_stream_count=workers * STREAMS_PER_WORKER,
    )
    serialized_schema = session.arrow_schema.serialized_schema
    jobs = [(stream.name, serialized_schema) for stream in session.streams]
    # Spawn, not fork: gRPC clients do not survive a fork.
    with ProcessPoolExecutor(
        workers,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=_open_client,
    ) as pool:
        results = list(pool.map(_read_stream, jobs))
    table = merge(result[0] for result in results)
    meta = {
        "rows_counted": str(sum(result[1] for result in results)),
        "null_project_rows": str(sum(result[2] for result in results)),
        "read_bytes": str(session.estimated_total_bytes_scanned),
        "streams": str(len(jobs)),
        "workers": str(workers),
    }
    return table, meta


def read_partition(day: date, project: str) -> tuple[int, str]:
    """The partition's total rows and last-modified time, by a metadata-only query."""
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    session = AuthorizedSession(google.auth.default()[0])
    reply = session.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries",
        json={
            "query": PARTITIONS_SQL,
            "useLegacySql": False,
            # The snapshot check compares two reads, so neither may come from cache.
            "useQueryCache": False,
            "location": "US",
            "parameterMode": "NAMED",
            "queryParameters": [
                {
                    "name": "partition",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": day.strftime("%Y%m%d")},
                }
            ],
        },
        timeout=120,
    )
    reply.raise_for_status()
    body = reply.json()
    if not body.get("jobComplete") or not body.get("rows"):
        raise RuntimeError(f"no partition row for {day}: {json.dumps(body)[:300]}")
    cells = body["rows"][0]["f"]
    return int(cells[0]["v"]), cells[1]["v"]


def upload(bucket: str, project: str, name: str, path: Path) -> None:
    """Upload one file. Overwriting the same object is safe, so a transient failure retries."""
    from google.cloud import storage
    from google.cloud.storage.retry import DEFAULT_RETRY

    # By default the library retries an upload only when it carries a generation
    # precondition; a failed upload here would waste the whole day's count.
    blob = storage.Client(project=project).bucket(bucket).blob(name)
    blob.upload_from_filename(str(path), retry=DEFAULT_RETRY.with_timeout(UPLOAD_RETRY_SECONDS))


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _say(**line) -> None:
    print(json.dumps(line), flush=True)


def _read_env() -> tuple[date, str, str, int]:
    """The run's settings; raises ValueError naming the first one that is wrong."""
    day_text = os.environ.get("DAY", "")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_text):
        raise ValueError("DAY")
    try:
        day = date.fromisoformat(day_text)
    except ValueError:
        raise ValueError("DAY") from None
    project, bucket = os.environ.get("PROJECT", ""), os.environ.get("BUCKET", "")
    if not project:
        raise ValueError("PROJECT")
    if not bucket:
        raise ValueError("BUCKET")
    workers_text = os.environ.get("WORKERS", "")
    if not (workers_text.isascii() and workers_text.isdigit() and int(workers_text) > 0):
        raise ValueError("WORKERS")
    return day, project, bucket, int(workers_text)


def _count_checked(day: date, project: str, workers: int):
    """Count the day until the snapshot holds still, at most ATTEMPTS times."""
    for attempt in range(1, ATTEMPTS + 1):
        started_at = _now()
        before = read_partition(day, project)
        table, meta = count_day(day, project=project, workers=workers)
        after = read_partition(day, project)
        try:
            check_rows(int(meta["rows_counted"]), before, after)
        except SnapshotMoved:
            _say(outcome="snapshot_moved_retry", day=str(day), attempt=attempt)
            continue
        meta = {
            "day": str(day),
            "rows_bigquery": str(before[0]),
            "partition_modified": before[1],
            "started_at": started_at,
            "finished_at": _now(),
            **meta,
        }
        return table, meta
    raise SnapshotMoved(f"the partition moved in all {ATTEMPTS} attempts")


def main() -> int:
    try:
        day, project, bucket, workers = _read_env()
    except ValueError as bad:
        _say(outcome="bad_env", variable=str(bad))
        return 2
    try:
        table, meta = _count_checked(day, project, workers)
        with tempfile.TemporaryDirectory() as work:
            database = Path(work) / f"{day}.db"
            write_counts(table, meta, database)
            packed, size, sha256 = compress(database)
            marker = Path(work) / f"{day}.json"
            marker.write_text(json.dumps(summary(meta, size, sha256)))
            # The summary goes last: it is what marks the day as finished.
            upload(bucket, project, f"counts/{day}.db.zst", packed)
            upload(bucket, project, f"counts/{day}.json", marker)
    except SnapshotMoved as moved:
        _say(outcome="snapshot_moved", day=str(day), detail=str(moved))
        return 1
    except RowsMismatch as mismatch:
        _say(outcome="rows_mismatch", day=str(day), detail=str(mismatch))
        return 1
    except Exception as failed:
        _say(outcome="failed", day=str(day), error=type(failed).__name__, detail=str(failed)[:500])
        return 1
    _say(outcome="counted", day=str(day), rows=int(meta["rows_counted"]), db_bytes=size)
    return 0
