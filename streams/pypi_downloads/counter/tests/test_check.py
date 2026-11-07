import json
import pyarrow as pa
import pytest

from counter import main as job
from counter.main import RowsMismatch, SnapshotMoved, check_rows

SNAPSHOT = (6_332_914_314, "2026-11-05T00:36:50Z")


def test_check_rows_passes_when_the_snapshot_held_and_the_count_matches():
    check_rows(6_332_914_314, SNAPSHOT, SNAPSHOT)


def test_check_rows_refuses_a_moved_snapshot():
    with pytest.raises(SnapshotMoved):
        check_rows(6_332_914_314, SNAPSHOT, (6_332_914_315, "2026-11-05T00:36:50Z"))
    with pytest.raises(SnapshotMoved):
        check_rows(6_332_914_314, SNAPSHOT, (6_332_914_314, "2026-11-05T09:00:00Z"))


def test_check_rows_refuses_a_count_that_differs():
    with pytest.raises(RowsMismatch) as refused:
        check_rows(6_332_914_313, SNAPSHOT, SNAPSHOT)
    assert "6332914313" in str(refused.value)
    assert "6332914314" in str(refused.value)


def fake_count_day(rows):
    def count_day(day, *, project, workers):
        table = pa.table(
            {
                "project": ["a", "b"],
                "downloads": pa.array([3, 2], pa.int64()),
                "mirror_downloads": pa.array([1, 0], pa.int64()),
            }
        )
        meta = {
            "rows_counted": str(rows),
            "null_project_rows": "0",
            "read_bytes": "1234",
            "streams": "8",
            "workers": str(workers),
        }
        return table, meta

    return count_day


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DAY", "2026-11-04")
    monkeypatch.setenv("PROJECT", "py-observatory")
    monkeypatch.setenv("BUCKET", "py-observatory-counts")
    monkeypatch.setenv("WORKERS", "2")


def stand_in_google(monkeypatch, partitions, rows=5):
    """Replace everything that reaches Google; return the list of uploaded names."""
    uploaded = []
    reads = iter(partitions)
    monkeypatch.setattr(job, "count_day", fake_count_day(rows))
    monkeypatch.setattr(job, "read_partition", lambda day, project: next(reads))
    monkeypatch.setattr(
        job, "upload", lambda bucket, project, name, path: uploaded.append(name)
    )
    return uploaded


def test_main_uploads_the_database_before_the_summary(monkeypatch, env, capsys):
    uploaded = stand_in_google(monkeypatch, [(5, "2026-11-05T00:36:50Z")] * 2)
    assert job.main() == 0
    assert uploaded == ["counts/2026-11-04.db.zst", "counts/2026-11-04.json"]
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["outcome"] == "counted"


def test_main_counts_again_when_the_snapshot_moved_once(monkeypatch, env):
    uploaded = stand_in_google(
        monkeypatch,
        [(4, "2026-11-05T00:00:00Z"), (5, "2026-11-05T00:36:50Z")]
        + [(5, "2026-11-05T00:36:50Z")] * 2,
    )
    assert job.main() == 0
    assert len(uploaded) == 2


def test_main_uploads_nothing_after_three_moved_snapshots(monkeypatch, env, capsys):
    moving = [(n, "2026-11-05T00:00:00Z") for n in range(1, 7)]
    uploaded = stand_in_google(monkeypatch, moving)
    assert job.main() == 1
    assert uploaded == []
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["outcome"] == "snapshot_moved"


def test_main_uploads_nothing_when_the_count_differs(monkeypatch, env):
    uploaded = stand_in_google(monkeypatch, [(6, "2026-11-05T00:36:50Z")] * 2)
    assert job.main() == 1
    assert uploaded == []


def test_main_uploads_nothing_when_the_database_cannot_be_written(monkeypatch, env):
    uploaded = stand_in_google(monkeypatch, [(5, "2026-11-05T00:36:50Z")] * 2)

    def refuse(table, meta, path):
        path.write_bytes(b"half")
        raise ValueError("CHECK constraint failed")

    monkeypatch.setattr(job, "write_counts", refuse)
    monkeypatch.setattr(job, "compress", lambda path: pytest.fail("compressed a partial database"))
    assert job.main() == 1
    assert uploaded == []


@pytest.mark.parametrize(
    "name, value",
    [
        ("DAY", ""),
        ("DAY", "2026-13-01"),
        ("DAY", "20261104"),
        ("PROJECT", ""),
        ("BUCKET", ""),
        ("WORKERS", "0"),
        ("WORKERS", "two"),
    ],
)
def test_main_names_a_bad_environment_variable(monkeypatch, env, capsys, name, value):
    monkeypatch.setenv(name, value)
    monkeypatch.setattr(job, "count_day", lambda *a, **k: pytest.fail("counted"))
    assert job.main() == 2
    line = json.loads(capsys.readouterr().out)
    assert line["variable"] == name


def test_main_names_a_missing_environment_variable(monkeypatch, env, capsys):
    monkeypatch.delenv("DAY")
    assert job.main() == 2
    assert json.loads(capsys.readouterr().out)["variable"] == "DAY"



PARTITION_ROW = {"jobComplete": True, "rows": [{"f": [{"v": "5"}, {"v": "2026-11-05T00:36:50Z"}]}]}


def stub_partition_session(monkeypatch, answers):
    """Stand in for Google's session: each post takes the next of `answers`, an
    exception to raise or a reply body to give. Returns the request bodies posted."""
    import google.auth
    import google.auth.transport.requests as transport

    posted = []
    left = iter(answers)

    class Reply:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self.body

    class Session:
        def __init__(self, credentials):
            pass

        def post(self, url, json, timeout):
            posted.append(json)
            answer = next(left)
            if isinstance(answer, Exception):
                raise answer
            return Reply(answer)

    monkeypatch.setattr(google.auth, "default", lambda: (object(), "p"))
    monkeypatch.setattr(transport, "AuthorizedSession", Session)
    return posted


def test_the_partition_read_bypasses_the_query_cache(monkeypatch):
    from datetime import date

    posted = stub_partition_session(monkeypatch, [PARTITION_ROW])
    assert job.read_partition(date(2026, 11, 4), "py-observatory") == (5, "2026-11-05T00:36:50Z")
    (body,) = posted
    assert body["useQueryCache"] is False
    assert body["timeoutMs"] == 60000
    assert body["queryParameters"][0]["parameterValue"] == {"value": "20261104"}


@pytest.mark.parametrize(
    "hiccup",
    [ConnectionError("connection reset"), {"jobComplete": False}],
    ids=["exception", "unfinished"],
)
def test_a_partition_read_that_fails_once_is_tried_again(monkeypatch, capsys, hiccup):
    from datetime import date

    waits = []
    posted = stub_partition_session(monkeypatch, [hiccup, PARTITION_ROW])
    got = job.read_partition(date(2026, 11, 4), "py-observatory", sleep=waits.append)
    assert got == (5, "2026-11-05T00:36:50Z")
    assert len(posted) == 2
    assert waits == [job.PARTITION_RETRY_SECONDS]
    assert json.loads(capsys.readouterr().out)["outcome"] == "partition_read_retry"


@pytest.mark.parametrize(
    "hiccup, error",
    [
        (ConnectionError("connection reset"), ConnectionError),
        ({"jobComplete": False}, job.QueryUnfinished),
    ],
    ids=["exception", "unfinished"],
)
def test_a_partition_read_that_fails_twice_fails(monkeypatch, hiccup, error):
    from datetime import date

    waits = []
    posted = stub_partition_session(monkeypatch, [hiccup, hiccup])
    with pytest.raises(error):
        job.read_partition(date(2026, 11, 4), "py-observatory", sleep=waits.append)
    assert len(posted) == 2
    assert waits == [job.PARTITION_RETRY_SECONDS]


def test_a_partition_with_no_row_is_not_read_again(monkeypatch):
    from datetime import date

    waits = []
    posted = stub_partition_session(monkeypatch, [{"jobComplete": True, "rows": []}])
    with pytest.raises(RuntimeError, match="no partition row"):
        job.read_partition(date(2026, 11, 4), "py-observatory", sleep=waits.append)
    assert len(posted) == 1
    assert waits == []


def test_each_upload_carries_a_bounded_retry(monkeypatch, tmp_path):
    from google.cloud import storage

    calls = []

    class Blob:
        def upload_from_filename(self, filename, **kwargs):
            calls.append((filename, kwargs))

    class Bucket:
        def blob(self, name):
            return Blob()

    class Client:
        def __init__(self, project):
            pass

        def bucket(self, name):
            return Bucket()

    monkeypatch.setattr(storage, "Client", Client)
    job.upload("b", "p", "counts/x.json", tmp_path / "x.json")
    (_, kwargs), = calls
    assert kwargs["retry"].timeout == job.UPLOAD_RETRY_SECONDS
