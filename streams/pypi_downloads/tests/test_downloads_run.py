import dataclasses
import json
import re
import sqlite3
import tomllib
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from downloads_run_helpers import (
    BASE_DECEMBER,
    BASE_NOVEMBER,
    CONFIG,
    CURRENT,
    DECEMBER,
    DELTA,
    MANIFEST,
    MANIFEST_NOVEMBER,
    NOVEMBER,
    OPENED,
    PART_NOVEMBER,
    READ_BYTES,
    SERVING,
    STATE,
    Stream,
    by_month,
    close_connections,  # autouse: it closes what each test of this module opened
    counted_days,
    counts_rows,
    execution,
    folded_days,
    found_on,
    project_ids,
    rows,
    rows_of,
    sealed_name,
    serving_tables,
)
from pypi_downloads import run as run_module
from pypi_downloads import serving as serving_module
from pypi_downloads.config import STREAM_TOML
from pypi_downloads.counts import write_day
from pypi_downloads.plan import BACKFILL_SHARE, FIRST_DAY, FREE, billing_month
from pypi_downloads.run import (
    BREAKS,
    STORE,
    BootstrapError,
    RunReport,
    counter_template,
)
from pypi_downloads.serving import SERVING_DB, TABLES, ServingMismatch, check, rebuild
from pypi_downloads.store import STATE_SCHEMA, STATE_TABLES, day_number

from obskit import cycle
from obskit.gcp import GoogleError
from obskit.github import GitHubError
from obskit.manifest import TableInfo, Watermark
from obskit.parts import coarsest_parts
from obskit.release import fetch_assets
from obskit.sqlite import diff_tables, open_db

# Most tests start with a bootstrap at this time, whose newest due day is the launch.
BOOTSTRAP_AT = "2026-11-11T09:00:00Z"
LAUNCH = date(2026, 11, 9)
# Each execution of the fake counter takes 600 seconds at 4 vCPU and 2 GiB.
VCPU_SECONDS = 2400.0
GIB_SECONDS = 1200.0
# The workflow names the image by the commit, as a tag.
IMAGE = "us-central1-docker.pkg.dev/py-observatory/counter/counter:" + "a" * 40


@pytest.fixture(autouse=True)
def no_sqlite_tmpdir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start each test with no SQLITE_TMPDIR, and put back what was there when it ends.
    `main` sets it for the rest of the process, which would carry it into later
    tests."""
    monkeypatch.setenv("SQLITE_TMPDIR", "")  # so that the original is put back
    monkeypatch.delenv("SQLITE_TMPDIR")


def days_after_bootstrap(stream: Stream) -> list[str]:
    """The days that executions were started for after the bootstrap's."""
    return stream.started[1:]


def printed_report(capsys: pytest.CaptureFixture) -> dict:
    """The report that `main` printed last."""
    return json.loads(capsys.readouterr().out.splitlines()[-1])


def listed_partitions(sql: str) -> list[str]:
    """The partition ids that a query of the partition sizes names."""
    return re.findall(r"'([0-9]{8})'", sql)


# ---- the bootstrap


def test_bootstrap_counts_the_launch_day_and_publishes_first(tmp_path):
    stream = Stream(tmp_path)

    report = stream.bootstrap(BOOTSTRAP_AT)

    # The newest day that is due on 11-11 is 11-09, which the counter is run for.
    assert stream.started == ["2026-11-09"]
    (run,) = stream.cloud.executions
    # The current release holds the state, the open delta, the serving database and
    # the manifest. The month's release holds the state as the month began, which is
    # the same bytes.
    assert stream.assets(CURRENT) == sorted([STATE, DELTA, SERVING, MANIFEST])
    assert stream.assets(NOVEMBER) == [BASE_NOVEMBER]
    base_bytes = stream.github.asset_bytes(NOVEMBER, BASE_NOVEMBER)
    assert base_bytes == stream.github.asset_bytes(CURRENT, STATE)
    state = stream.open(CURRENT, STATE)
    assert dict(state.execute("SELECT key, value FROM meta")) == {
        "applied_seq": "6",
        "bootstrap_at": BOOTSTRAP_AT,
        "launch": "2026-11-09",
    }
    assert state.execute("SELECT day, note FROM breaks ORDER BY day").fetchall() == [
        (day_number(day), note) for day, note in BREAKS
    ]
    # The non-canonical name is stored canonical, and the new names take ids in name
    # order.
    assert state.execute("SELECT * FROM projects ORDER BY project_id").fetchall() == [
        (1, "new-20261109"),
        (2, "numpy"),
        (3, "requests"),
    ]
    day = state.execute(
        "SELECT day, rows_bigquery, projects, downloads, mirror_downloads, read_bytes, "
        "execution, vcpu_seconds, gib_seconds, counted_at FROM days"
    ).fetchall()
    assert day == [
        (
            day_number(LAUNCH),
            rows_of(LAUNCH),
            3,
            rows_of(LAUNCH),
            10,
            READ_BYTES,
            run["name"],
            VCPU_SECONDS,
            GIB_SECONDS,
            BOOTSTRAP_AT,
        )
    ]
    delta = stream.open(CURRENT, DELTA)
    assert counts_rows(delta) == [
        ("2026-11-09", 1, 1, 0),
        ("2026-11-09", 2, 500 + LAUNCH.toordinal() % 1000, 0),
        ("2026-11-09", 3, 1000 + LAUNCH.toordinal() % 1000, 10),
    ]

    manifest = stream.manifest()
    state_entry, delta_entry, serving_entry, base_entry = manifest.assets
    assert (delta_entry.name, delta_entry.period) == (DELTA, "2026-11-11")
    assert serving_entry.name == SERVING
    assert delta_entry.tables["counts"] == TableInfo(3, "append")
    assert base_entry == dataclasses.replace(
        state_entry, name=BASE_NOVEMBER, tag=NOVEMBER, period="2026-11", closed=True
    )
    assert manifest.series_breaks == [
        {"day": "2018-07-26", "note": "Earlier days are undercounted."},
        {
            "day": "2026-08-24",
            "note": "PyPI stopped counting PEP 658 metadata fetches, and counts "
            "fell 35-37%.",
        },
    ]
    assert manifest.data_through == Watermark("2026-11-09", "day")
    assert manifest.cursor is None
    assert manifest.coverage == {
        "launch": "2026-11-09",
        "first_day": "2026-11-09",
        "last_day": "2026-11-09",
        "days": 1,
        "forward_missing": 0,
        "backfill_left": (LAUNCH - FIRST_DAY).days,
        "projects": 3,
        "window_days": 1,
        "top_year": None,
        "top_set": 0,
    }
    assert manifest.complete is False
    assert report == RunReport(
        counted=["2026-11-09"],
        reused=[],
        failed=[],
        held=None,
        held_kind=None,
        notices_quiet=True,
        drift=[],
        drift_unknown=False,
        new_projects=3,
        renamed=1,
        usage={
            "vcpu_seconds": VCPU_SECONDS,
            "gib_seconds": GIB_SECONDS,
            "read_bytes": READ_BYTES,
        },
        gate_failures=[],
        serving="rebuilt",
        serving_reason="the first run has no serving database to update",
    )


def test_a_bootstrap_whose_launch_day_fails_publishes_nothing(tmp_path):
    stream = Stream(tmp_path)
    stream.fail = {LAUNCH}

    with pytest.raises(BootstrapError, match="2026-11-09"):
        stream.bootstrap(BOOTSTRAP_AT)

    assert stream.github.release(CURRENT) is None
    assert stream.github.release(NOVEMBER) is None


def test_a_bootstrap_held_by_the_budget_starts_no_execution(tmp_path):
    stream = Stream(tmp_path)
    stream.notify(0.01, at="2026-11-11T08:00:00Z")

    with pytest.raises(BootstrapError, match=r"\$0.01 spent"):
        stream.bootstrap(BOOTSTRAP_AT)

    assert stream.started == []
    assert stream.github.release(CURRENT) is None
    # Nothing was published to hold the spend, so the notification is not
    # acknowledged, and Pub/Sub delivers it again to a rerun before the budget's next.
    assert stream.cloud.acked == []

    with pytest.raises(BootstrapError, match=r"\$0.01 spent"):
        stream.bootstrap("2026-11-11T09:30:00Z")

    assert stream.started == []
    assert stream.cloud.acked == []

    # A person who has looked clears the spend with a reason, and the bootstrap
    # counts. The clearance is published with the first state.
    report = stream.run(
        "2026-11-11T10:00:00Z", bootstrap=True, override_reason="a test charge"
    )

    assert (report.counted, report.held) == (["2026-11-09"], None)
    state = stream.open(CURRENT, STATE)
    assert state.execute("SELECT * FROM clearances").fetchall() == [
        ("2026-11", 0.01, "a test charge", "2026-11-11T10:00:00Z")
    ]
    assert stream.cloud.acked == ["ack-3"]


# ---- the runs after it


def test_later_run_counts_forward_days_before_backfill(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.0, at="2026-11-13T08:00:00Z")

    # Four executions fit in the time: the days after launch first, oldest first, and
    # then the backfill, newest first.
    report = stream.run("2026-11-13T09:00:00Z", max_seconds=4 * 600 - 1)

    started = ["2026-11-10", "2026-11-11", "2026-11-08", "2026-11-07"]
    assert days_after_bootstrap(stream) == started
    assert report.counted == started
    assert (report.reused, report.failed, report.drift) == ([], [], [])
    assert (report.held, report.held_kind) == (None, None)
    assert (report.new_projects, report.renamed) == (4, 4)
    # Every execution so far started this billing month, and every day was counted
    # in it.
    assert report.usage == {
        "vcpu_seconds": 5 * VCPU_SECONDS,
        "gib_seconds": 5 * GIB_SECONDS,
        "read_bytes": 5 * READ_BYTES,
    }
    state = stream.open(CURRENT, STATE)
    assert counted_days(state) == [
        "2026-11-07",
        "2026-11-08",
        "2026-11-09",
        "2026-11-10",
        "2026-11-11",
    ]
    names = [row[0] for row in state.execute("SELECT execution FROM days ORDER BY seq")]
    assert names == [run["name"] for run in stream.cloud.executions]
    assert {day for day, *_ in counts_rows(stream.open(CURRENT, DELTA))} == {
        "2026-11-07",
        "2026-11-08",
        "2026-11-10",
        "2026-11-11",
    }
    manifest = stream.manifest()
    assert manifest.data_through == Watermark("2026-11-11", "day")
    assert manifest.coverage == {
        "launch": "2026-11-09",
        "first_day": "2026-11-07",
        "last_day": "2026-11-11",
        "days": 5,
        "forward_missing": 0,
        "backfill_left": (date(2026, 11, 7) - FIRST_DAY).days,
        "projects": 7,
        "window_days": 5,
        "top_year": None,
        "top_set": 0,
    }
    assert manifest.complete is False
    # The notification is kept in the state, and acknowledged once the state that
    # holds it is published. The series breaks go on.
    assert state.execute("SELECT * FROM spend").fetchall() == [
        ("2026-11", 0.0, 1.0, "2026-11-13T08:00:00Z")
    ]
    assert stream.cloud.acked == ["ack-1"]
    assert len(manifest.series_breaks) == 2


def test_a_day_in_the_bucket_is_not_counted_again(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    # The run of 11-12 gives up waiting for the execution of 11-10, which goes on and
    # writes the day's files.
    stream.hang.add(date(2026, 11, 10))

    first = stream.run("2026-11-12T09:00:00Z")

    assert (first.counted, first.failed) == ([], ["2026-11-10"])
    assert days_after_bootstrap(stream) == ["2026-11-10"]
    assert counted_days(stream.open(CURRENT, STATE)) == ["2026-11-09"]

    # The next run takes the day from the bucket, and starts no execution for it.
    second = stream.run("2026-11-12T15:00:00Z")

    assert days_after_bootstrap(stream) == ["2026-11-10"]
    assert (second.counted, second.reused, second.failed) == ([], ["2026-11-10"], [])
    state = stream.open(CURRENT, STATE)
    assert state.execute(
        "SELECT day, rows_bigquery, execution, vcpu_seconds, gib_seconds FROM days "
        "WHERE day = ?",
        (day_number(date(2026, 11, 10)),),
    ).fetchall() == [
        (day_number(date(2026, 11, 10)), rows_of(date(2026, 11, 10)), None, None, None)
    ]
    assert stream.manifest().data_through == Watermark("2026-11-10", "day")


def test_a_failed_execution_stops_the_run_and_exits_2(tmp_path, monkeypatch, capsys):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.fail = {date(2026, 11, 11)}

    assert stream.main(monkeypatch, "2026-11-14T09:00:00Z", "run") == 2

    # 11-12 is due as well, and is not started once 11-11 has failed. The day that
    # was counted is published.
    assert days_after_bootstrap(stream) == ["2026-11-10", "2026-11-11"]
    out, err = capsys.readouterr()
    report = json.loads(out)
    assert (report["counted"], report["failed"]) == (["2026-11-10"], ["2026-11-11"])
    assert "2026-11-11 failed: " in err
    state = stream.open(CURRENT, STATE)
    assert counted_days(state) == ["2026-11-09", "2026-11-10"]
    manifest = stream.manifest()
    assert manifest.data_through == Watermark("2026-11-10", "day")
    assert manifest.coverage["forward_missing"] == 2


def test_backfill_stops_at_the_budget_line_and_exits_0(tmp_path, monkeypatch, capsys):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    at = "2026-11-12T09:00:00Z"
    now = datetime.fromisoformat(at)
    start, end = billing_month(now)
    line = BACKFILL_SHARE * FREE.vcpu_seconds * ((now - start) / (end - start))
    # An earlier execution leaves the month 1,200 vCPU-seconds below the line once the
    # bootstrap's and the next forward day's executions are counted, so one backfill
    # day starts and the next is held.
    earlier = line - 2 * VCPU_SECONDS - 1200
    stream.cloud.executions.append(
        execution(datetime(2026, 11, 5, tzinfo=UTC), earlier / CONFIG.cpu)
    )
    stream.notify(0.0, at="2026-11-12T08:00:00Z")

    assert stream.main(monkeypatch, at, "run") == 0

    assert days_after_bootstrap(stream) == ["2026-11-10", "2026-11-08"]
    report = printed_report(capsys)
    assert report["counted"] == ["2026-11-10", "2026-11-08"]
    assert report["held_kind"] == "line"
    assert report["held"].startswith("vCPU-seconds")
    assert report["usage"]["vcpu_seconds"] == pytest.approx(line + 1200, abs=CONFIG.cpu)


def test_backfill_without_a_notification_exits_2(tmp_path, monkeypatch, capsys):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)

    # The day after launch needs no notification. The backfill does. No notification
    # of any month has come, so the feed may be broken, and the run exits 2.
    assert stream.main(monkeypatch, "2026-11-12T09:00:00Z", "run") == 2

    assert days_after_bootstrap(stream) == ["2026-11-10"]
    report = printed_report(capsys)
    assert (report["counted"], report["held_kind"]) == (["2026-11-10"], "notice")
    assert report["notices_quiet"] is True


def test_a_month_before_its_first_notification_holds_the_backfill_and_exits_0(
    tmp_path, monkeypatch, capsys
):
    stream = Stream(tmp_path)
    # The billing month began at 07:00 UTC. October's notifications still come, and
    # November's first has not.
    stream.bootstrap("2026-11-01T09:00:00Z")
    stream.notify(0.0, at="2026-11-01T10:00:00Z", month="2026-10")

    assert stream.main(monkeypatch, "2026-11-01T12:00:00Z", "run") == 0

    # The launch day, 10-30, is the newest that is due, so only the backfill is left,
    # and it waits for November's notification.
    assert days_after_bootstrap(stream) == []
    report = printed_report(capsys)
    assert report["held_kind"] == "notice"
    assert report["held"] == "no budget notification for 2026-11 in the last 48 hours"
    assert report["notices_quiet"] is False


def test_an_override_clears_a_spend_hold_until_the_cost_rises(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.25, at="2026-11-12T08:00:00Z")

    held = stream.run("2026-11-12T09:00:00Z")

    assert (held.counted, held.held_kind) == ([], "spend")

    # A person looks at the billing report and clears the spend with a reason. The
    # run records a clearance of the month's cost, and counts.
    cleared = stream.run(
        "2026-11-12T15:00:00Z", override_reason="a rounding charge", max_seconds=1
    )

    assert (cleared.counted, cleared.held) == (["2026-11-10"], None)
    state = stream.open(CURRENT, STATE)
    assert state.execute("SELECT * FROM clearances").fetchall() == [
        ("2026-11", 0.25, "a rounding charge", "2026-11-12T15:00:00Z")
    ]

    # The clearance is published, so the next run counts with no reason given.
    after = stream.run("2026-11-12T21:00:00Z", max_seconds=1)

    assert (after.counted, after.held) == (["2026-11-08"], None)

    # A later notification shows more spent in the month: the clearance does not
    # cover it, and the run holds again.
    stream.notify(0.4, at="2026-11-13T08:00:00Z")

    again = stream.run("2026-11-13T09:00:00Z")

    assert (again.counted, again.held_kind) == ([], "spend")
    assert again.held == (
        "the budget shows $0.4 spent in 2026-11, above the $0.25 cleared"
    )
    assert days_after_bootstrap(stream) == ["2026-11-10", "2026-11-08"]


def test_a_spend_holds_into_the_next_billing_month(tmp_path, monkeypatch, capsys):
    stream = Stream(tmp_path)
    stream.bootstrap("2026-11-28T09:00:00Z")
    stream.notify(0.25, at="2026-11-29T08:00:00Z")

    assert stream.main(monkeypatch, "2026-11-29T09:00:00Z", "run") == 2

    # December has begun in California, and November's spend still holds every day.
    assert stream.main(monkeypatch, "2026-12-01T09:00:00Z", "run") == 2

    assert days_after_bootstrap(stream) == []
    report = printed_report(capsys)
    assert (report["counted"], report["held_kind"]) == ([], "spend")
    assert report["held"] == "the budget shows $0.25 spent in 2026-11"


def test_usage_is_measured_again_at_the_time_of_measuring(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.0, at="2026-11-12T08:00:00Z")
    # An execution started as the run began, by hand, and is still running.
    running = execution(datetime(2026, 11, 12, 9, tzinfo=UTC), 0)
    del running["completionTime"]
    stream.cloud.executions.append(running)

    report = stream.run("2026-11-12T09:00:00Z", max_seconds=1)

    # Once 11-10 is counted, 600 seconds into the run, the running execution has used
    # 600 seconds too.
    assert report.counted == ["2026-11-10"]
    assert report.usage["vcpu_seconds"] == 3 * VCPU_SECONDS
    assert report.usage["gib_seconds"] == 3 * GIB_SECONDS


def test_the_stop_keeps_room_for_the_next_execution(tmp_path, monkeypatch, capsys):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.0, at="2026-11-12T08:00:00Z")
    # The month is 100 vCPU-seconds short of the reserve of a whole execution, at the
    # job's limits for its whole timeout, from the stop.
    reserve = CONFIG.cpu * CONFIG.task_timeout_seconds
    used = 0.98 * FREE.vcpu_seconds - reserve + 100
    stream.cloud.executions.append(
        execution(datetime(2026, 11, 5, tzinfo=UTC), (used - VCPU_SECONDS) / CONFIG.cpu)
    )

    assert stream.main(monkeypatch, "2026-11-12T09:00:00Z", "run") == 2

    assert days_after_bootstrap(stream) == []
    report = printed_report(capsys)
    assert (report["counted"], report["held_kind"]) == ([], "stop")
    assert "reserved for the next execution" in report["held"]


def test_spend_above_zero_counts_nothing_and_exits_2(tmp_path, monkeypatch, capsys):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.25, at="2026-11-12T08:00:00Z")

    assert stream.main(monkeypatch, "2026-11-12T09:00:00Z", "run") == 2

    assert days_after_bootstrap(stream) == []
    report = printed_report(capsys)
    assert (report["counted"], report["held_kind"]) == ([], "spend")
    assert report["held"] == "the budget shows $0.25 spent in 2026-11"
    # The spend is published with the state, so a later run holds too, with no new
    # notification.
    state = stream.open(CURRENT, STATE)
    assert state.execute("SELECT * FROM spend").fetchall() == [
        ("2026-11", 0.25, 1.0, "2026-11-12T08:00:00Z")
    ]
    assert stream.manifest().coverage["forward_missing"] == 1

    assert stream.main(monkeypatch, "2026-11-12T15:00:00Z", "run") == 2

    assert days_after_bootstrap(stream) == []
    assert printed_report(capsys)["held_kind"] == "spend"
    assert stream.manifest().generated_at == "2026-11-12T15:00:00Z"


def test_a_run_that_fails_after_the_pull_acknowledges_nothing(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.25, at="2026-11-12T08:00:00Z")
    # The run fails after it has pulled the notification: Cloud Run refuses the list
    # of the job's executions.
    stream.cloud.fail_next(403, url_contains="/executions")

    with pytest.raises(GoogleError, match="HTTP 403"):
        stream.run("2026-11-12T09:00:00Z")

    assert stream.manifest().generated_at == BOOTSTRAP_AT
    assert stream.cloud.acked == []

    # The next run is given the notification again, and holds for the spend, but its
    # publish fails as the manifest goes up. That acknowledges nothing either.
    stream.github.fail("POST", "name=swap-next-manifest.json", 422)

    with pytest.raises(GitHubError):
        stream.run("2026-11-12T12:00:00Z")

    assert stream.manifest().generated_at == BOOTSTRAP_AT
    assert stream.cloud.acked == []

    # The run after it is given the notification once more, and holds for the spend.
    # It acknowledges the notification once its publish holds the spend.
    report = stream.run("2026-11-12T15:00:00Z")

    assert (report.counted, report.held_kind) == ([], "spend")
    assert days_after_bootstrap(stream) == []
    assert stream.cloud.acked == ["ack-3"]
    state = stream.open(CURRENT, STATE)
    assert state.execute("SELECT * FROM spend").fetchall() == [
        ("2026-11", 0.25, 1.0, "2026-11-12T08:00:00Z")
    ]

    # Acknowledged, it is not given again, and the state still holds the spend.
    report = stream.run("2026-11-12T21:00:00Z")

    assert (report.counted, report.held_kind) == ([], "spend")
    assert stream.cloud.acked == ["ack-3"]


def test_a_day_whose_partition_moved_is_reported(tmp_path, monkeypatch, capsys):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.0, at="2026-11-12T08:00:00Z")
    # 11-10 after launch, then 11-08 before it.
    stream.run("2026-11-12T09:00:00Z", max_seconds=2 * 600 - 1)
    # The partition of the launch day has grown since it was counted.
    stream.cloud.partitions["20261109"] = (rows_of(LAUNCH) + 3, "2026-11-12T10:00:00Z")
    stream.cloud.queries.clear()
    stream.notify(0.0, at="2026-11-13T08:00:00Z")

    assert (
        stream.main(monkeypatch, "2026-11-13T09:00:00Z", "run", "--max-seconds", "1")
        == 2
    )

    report = printed_report(capsys)
    assert report["drift"] == ["2026-11-09"]
    assert (report["counted"], report["failed"], report["held"]) == (
        ["2026-11-11"],
        [],
        None,
    )
    # One query of the partition sizes, which names the counted days at or after
    # launch, and not the backfill.
    (sql,) = stream.cloud.queries
    assert "INFORMATION_SCHEMA.PARTITIONS" in sql
    assert sorted(listed_partitions(sql)) == ["20261109", "20261110"]

    # Only the newest DRIFT_DAYS days are read.
    monkeypatch.setattr(run_module, "DRIFT_DAYS", 2)
    stream.cloud.queries.clear()

    report = stream.run("2026-11-13T15:00:00Z", max_seconds=0)

    (sql,) = stream.cloud.queries
    assert sorted(listed_partitions(sql)) == ["20261110", "20261111"]
    assert report.drift == []


def test_a_drift_check_that_fails_is_reported_and_the_run_goes_on(
    tmp_path, monkeypatch, capsys
):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.0, at="2026-11-12T08:00:00Z")
    stream.cloud.fail_next(403, url_contains="bigquery")

    assert (
        stream.main(monkeypatch, "2026-11-12T09:00:00Z", "run", "--max-seconds", "1")
        == 2
    )

    out, err = capsys.readouterr()
    report = json.loads(out)
    assert (report["drift"], report["drift_unknown"]) == ([], True)
    assert "the drift check failed: " in err
    assert (report["counted"], report["failed"], report["held"]) == (
        ["2026-11-10"],
        [],
        None,
    )
    assert stream.manifest().generated_at == "2026-11-12T09:00:00Z"
    # The notification is acknowledged after the publish, as on any run.
    assert stream.cloud.acked == ["ack-1"]


def test_the_time_budget_stops_starting_days(tmp_path, monkeypatch, capsys):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.execution_seconds = 1000

    # Five days are due. The run starts one at 0, 1,000 and 2,000 seconds, and none
    # once 2,500 have passed.
    code = stream.main(
        monkeypatch, "2026-11-16T09:00:00Z", "run", "--max-seconds", "2500"
    )

    assert code == 0
    assert days_after_bootstrap(stream) == ["2026-11-10", "2026-11-11", "2026-11-12"]
    report = printed_report(capsys)
    assert report["counted"] == ["2026-11-10", "2026-11-11", "2026-11-12"]
    assert (report["held"], report["held_kind"]) == (None, None)
    manifest = stream.manifest()
    assert manifest.data_through == Watermark("2026-11-12", "day")
    assert manifest.coverage["forward_missing"] == 2


# ---- the month close


def test_a_month_close_merges_counts_into_the_part(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap("2026-11-28T09:00:00Z")
    stream.run("2026-11-29T09:00:00Z")
    last_state = stream.github.asset_bytes(CURRENT, STATE)

    # The first run of December closes November, and then counts the days now due.
    report = stream.run("2026-12-01T09:00:00Z")

    assert report.counted == ["2026-11-28", "2026-11-29"]
    days = ["2026-11-28", "2026-11-29"]
    month_assets = [BASE_NOVEMBER, *map(sealed_name, days), PART_NOVEMBER]
    assert stream.assets(NOVEMBER) == sorted([*month_assets, MANIFEST_NOVEMBER])
    assert stream.assets(DECEMBER) == [BASE_DECEMBER]
    assert stream.github.asset_bytes(DECEMBER, BASE_DECEMBER) == last_state
    # Each sealed delta holds the counts of the day that its run counted: the launch
    # day, 11-26, and then 11-27. The part holds every one of them.
    sealed = [counts_rows(stream.open(NOVEMBER, sealed_name(day))) for day in days]
    assert [{row[0] for row in rows} for rows in sealed] == [
        {"2026-11-26"},
        {"2026-11-27"},
    ]
    part = stream.open(NOVEMBER, PART_NOVEMBER)
    assert counts_rows(part) == sorted(row for rows in sealed for row in rows)
    # Before it built the part, the run rebuilt November from its base copy and its
    # sealed deltas, and found the state that November ended with. A restore from the
    # releases alone finds it too.
    entries = stream.entries()
    asked = [entries[BASE_NOVEMBER], *(entries[sealed_name(day)] for day in days)]
    base_db, *delta_dbs = fetch_assets(stream.gh, asked, stream.folder("restore"))
    cycle.replay(STORE, base_db, delta_dbs)
    ended = stream.expand(DECEMBER, BASE_DECEMBER)
    with closing(open_db(base_db, STATE_SCHEMA)) as rebuilt:
        assert diff_tables(rebuilt, ended, STATE_TABLES) == []
    # December's open delta holds the days counted in December.
    assert {row[0] for row in counts_rows(stream.open(CURRENT, DELTA))} == set(days)
    assert entries[PART_NOVEMBER].tables["counts"] == TableInfo(6, "append")
    assert stream.manifest().data_through == Watermark("2026-11-29", "day")


# ---- the serving database


def scripted_runs(stream: Stream, times: list[str]) -> dict[str, RunReport]:
    """A run at each of `times`, UTC and from 09:00 on, in order, with a budget
    notification of $0 for the run's billing month an hour before it, so that the
    backfill goes on below the budget line. Each run starts at most two days. Returns
    the reports by time."""
    reports = {}
    for at in times:
        before = datetime.fromisoformat(at) - timedelta(hours=1)
        stream.notify(0.0, at=f"{before:%Y-%m-%dT%H:%M:%SZ}", month=at[:7])
        reports[at] = stream.run(at, max_seconds=2 * 600 - 1)
    return reports


def rebuilt_from_releases(stream: Stream, today: date) -> sqlite3.Connection:
    """A serving database rebuilt from the published state and every published part
    that holds counts, the open delta included, as a restore from the releases alone
    makes it on `today`, opened."""
    parts = coarsest_parts(stream.manifest().assets, frozenset({"counts"}))
    paths = fetch_assets(stream.gh, parts, stream.folder("parts"))
    path = stream.folder("rebuilt") / SERVING_DB
    rebuild(path, stream.open(CURRENT, STATE), lambda: iter(paths), today=today)
    conn = sqlite3.connect(path)
    OPENED.append(conn)
    return conn


def test_the_bootstrap_publishes_a_serving_database(tmp_path):
    stream = Stream(tmp_path)

    report = stream.bootstrap(BOOTSTRAP_AT)

    # The bootstrap builds it from its open delta, which holds the launch day.
    assert (report.serving, report.serving_reason) == (
        "rebuilt",
        "the first run has no serving database to update",
    )
    entry = stream.entries()[SERVING]
    assert (entry.tag, entry.role, entry.period, entry.closed) == (
        CURRENT,
        "public",
        None,
        False,
    )
    assert {table: info.history_class for table, info in entry.tables.items()} == (
        dict.fromkeys(TABLES, "derived")
    )
    db = stream.open(CURRENT, SERVING)
    state = stream.open(CURRENT, STATE)
    found = found_on(state, ["2026-11-09"])
    assert rows(db, "folded") == [(day_number(LAUNCH), 6)]
    assert rows(db, "months") == [(202611, 1, rows_of(LAUNCH), 10)]
    assert rows(db, "monthly") == by_month(found)
    assert rows(db, "recent") == found
    # No month has closed since launch, so there is no top set and no daily count.
    assert rows(db, "top_set") == rows(db, "daily") == []
    check(db, state)


def test_each_run_folds_the_days_it_counts(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.0, at="2026-11-13T08:00:00Z")

    first = stream.run("2026-11-13T09:00:00Z", max_seconds=4 * 600 - 1)

    assert (first.serving, first.serving_reason) == ("updated", None)
    assert folded_days(stream.open(CURRENT, SERVING)) == [
        "2026-11-07",
        "2026-11-08",
        "2026-11-09",
        "2026-11-10",
        "2026-11-11",
    ]

    second = stream.run("2026-11-14T09:00:00Z", max_seconds=1)

    assert second.counted == ["2026-11-12"]
    assert (second.serving, second.serving_reason) == ("updated", None)
    db = stream.open(CURRENT, SERVING)
    state = stream.open(CURRENT, STATE)
    days = counted_days(state)
    assert (
        days
        == folded_days(db)
        == [
            "2026-11-07",
            "2026-11-08",
            "2026-11-09",
            "2026-11-10",
            "2026-11-11",
            "2026-11-12",
        ]
    )
    assert (
        rows(db, "folded")
        == state.execute("SELECT day, seq FROM days ORDER BY day").fetchall()
    )
    found = found_on(state, days)
    downloads = sum(row[2] for row in found)
    mirrors = sum(row[3] for row in found)
    assert rows(db, "months") == [(202611, 6, downloads, mirrors)]
    assert rows(db, "monthly") == by_month(found)
    # The six days are all within 30 of the newest.
    assert rows(db, "recent") == found
    assert rows(db, "top_set") == rows(db, "daily") == []
    check(db, state)
    coverage = stream.manifest().coverage
    assert (coverage["window_days"], coverage["top_year"], coverage["top_set"]) == (
        6,
        None,
        0,
    )


def test_a_month_close_adds_the_top_set(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap("2026-11-28T09:00:00Z")
    stream.run("2026-11-29T09:00:00Z")

    report = stream.run("2026-12-01T09:00:00Z")

    assert report.counted == ["2026-11-28", "2026-11-29"]
    # November's close makes 2026's first members, who keep every day of 2026, and the
    # days already folded have no daily counts: the database is rebuilt.
    assert report.serving == "rebuilt"
    assert (report.serving_reason or "").startswith(
        "the close of 2026-11 makes the first members of 2026's top set"
    )
    db = stream.open(CURRENT, SERVING)
    state = stream.open(CURRENT, STATE)
    ids = project_ids(state)
    # November is ranked by downloads less mirror downloads over the days counted
    # before December began, 11-26 and 11-27, ties by name. The projects first seen on
    # 11-28 and 11-29 were counted after it closed.
    members = ["requests", "numpy", "new-20261126", "new-20261127"]
    ranked = db.execute("SELECT * FROM top_set ORDER BY rank").fetchall()
    assert ranked == [
        (2026, ids[name], name, 202611, rank) for rank, name in enumerate(members, 1)
    ]
    # They entered at 2026's first close, so their daily counts are kept for every
    # day of 2026, those counted after the close included.
    kept = {ids[name] for name in members}
    found = found_on(state, counted_days(state))
    assert rows(db, "daily") == [row for row in found if row[0] in kept]
    assert {day for _, day, *_ in rows(db, "daily")} == {
        day_number(date(2026, 11, day)) for day in (26, 27, 28, 29)
    }
    check(db, state)
    coverage = stream.manifest().coverage
    assert (coverage["top_year"], coverage["top_set"]) == (2026, 4)


def test_serving_that_lags_the_state_is_rebuilt(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    behind = stream.expand(CURRENT, SERVING)
    stream.run("2026-11-12T09:00:00Z")
    # The current release is given back the bootstrap's serving database, which lacks
    # 11-10. The counts of 11-10 are in the open delta of 11-12, which the next run
    # seals, and those of the launch day in the sealed delta of 11-11.
    stream.publish_serving(behind)

    report = stream.run("2026-11-13T09:00:00Z")

    assert report.counted == ["2026-11-11"]
    assert (report.serving, report.serving_reason) == (
        "rebuilt",
        "the delta holds 0 counts of 2026-11-10, where the state counted 3 projects "
        "that day",
    )
    db = stream.open(CURRENT, SERVING)
    state = stream.open(CURRENT, STATE)
    check(db, state)
    assert folded_days(db) == ["2026-11-09", "2026-11-10", "2026-11-11"]
    assert rows(db, "recent") == found_on(state, counted_days(state))


def test_serving_that_fails_its_check_after_the_update_is_rebuilt(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    tampered = stream.expand(CURRENT, SERVING)
    with closing(sqlite3.connect(tampered)) as conn, conn:
        conn.execute(
            "UPDATE monthly SET downloads = downloads + 1 WHERE project_id = 1"
        )
    stream.publish_serving(tampered)

    report = stream.run("2026-11-12T09:00:00Z")

    # The update folds 11-10 into it, and its check finds November's sums wrong.
    assert (report.serving, report.serving_reason) == (
        "rebuilt",
        "monthly does not sum to months in 2026-11",
    )
    db = stream.open(CURRENT, SERVING)
    state = stream.open(CURRENT, STATE)
    check(db, state)
    assert rows(db, "monthly") == by_month(found_on(state, counted_days(state)))


def test_a_run_without_a_prior_serving_database_rebuilds_it(tmp_path, monkeypatch):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.run("2026-11-12T09:00:00Z")
    # The current release as the stream published it before it kept a serving
    # database.
    stream.publish_serving(None)
    stream.notify(0.0, at="2026-11-14T08:00:00Z")
    fetched: list[str] = []
    folders: list[Path] = []

    def fetch_one_at_a_time(gh, assets, workdir, **options):
        # Each part is fetched into a new, empty directory of its own, once the one
        # before has been deleted.
        assert list(workdir.iterdir()) == []
        assert not any(folder.exists() for folder in folders)
        folders.append(workdir)
        fetched.extend(asset.name for asset in assets)
        return fetch_assets(gh, assets, workdir, **options)

    monkeypatch.setattr(run_module, "fetch_assets", fetch_one_at_a_time)

    report = stream.run("2026-11-14T09:00:00Z", max_seconds=3 * 600 - 1)

    assert report.counted == ["2026-11-11", "2026-11-12", "2026-11-08"]
    assert (report.serving, report.serving_reason) == (
        "rebuilt",
        f"the prior manifest lists no {SERVING}",
    )
    # The sealed delta of 11-11, which holds the launch day, is read from November's
    # release on each of the rebuild's two passes, and deleted after each. The sealed
    # delta of 11-12, which the rollover made and which holds 11-10, and the open delta
    # are read where they are.
    assert fetched == [sealed_name("2026-11-11")] * 2
    assert len(set(folders)) == 2
    assert not any(folder.exists() for folder in folders)
    db = stream.open(CURRENT, SERVING)
    state = stream.open(CURRENT, STATE)
    check(db, state)
    assert folded_days(db) == counted_days(state)
    assert len(counted_days(state)) == 5
    assert stream.entries()[SERVING].tables["folded"] == TableInfo(5, "derived")


def test_rebuild_from_the_releases_equals_the_runs(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap("2026-11-25T09:00:00Z")
    first = date(2026, 11, 26)
    times = [f"{first + timedelta(days=n)}T09:00:00Z" for n in range(39)]
    # A second run on one day, which adds to that day's open delta.
    times.insert(times.index("2026-12-10T09:00:00Z") + 1, "2026-12-10T15:00:00Z")

    reports = scripted_runs(stream, times)

    assert times[-1] == "2027-01-03T09:00:00Z"
    # Only 2026's first close rebuilds. Every other run updates the database, the run
    # of 2027-01-01, which closes December into 2027's set, included.
    assert {at for at, report in reports.items() if report.serving != "updated"} == {
        "2026-12-01T09:00:00Z"
    }
    published = stream.open(CURRENT, SERVING)
    state = stream.open(CURRENT, STATE)
    check(published, state)
    rebuilt = rebuilt_from_releases(stream, date(2027, 1, 3))
    assert serving_tables(rebuilt) == serving_tables(published)
    # What was compared holds both closes and the daily counts of both years: of the
    # days of 2026 counted after its first close, by the backfill among them, and of
    # 2027's first day.
    entries = set(published.execute("SELECT DISTINCT year, entered FROM top_set"))
    assert entries == {(2026, 202611), (2027, 202612)}
    late = {
        day
        for day, counted_at in state.execute("SELECT day, counted_at FROM days")
        if counted_at >= "2026-12-01" and day < day_number(date(2026, 11, 1))
    }
    daily = {day for (day,) in published.execute("SELECT DISTINCT day FROM daily")}
    assert late and late <= daily
    assert day_number(date(2027, 1, 1)) in daily
    # Every day from launch on is folded, so the window holds 30, up to 2027-01-01.
    (members,) = published.execute(
        "SELECT count(*) FROM top_set WHERE year = 2027"
    ).fetchone()
    coverage = stream.manifest().coverage
    assert (coverage["window_days"], coverage["top_year"], coverage["top_set"]) == (
        30,
        2027,
        members,
    )


def test_a_run_after_a_gap_closes_each_month_it_missed(tmp_path):
    stream = Stream(tmp_path)
    stream.bootstrap("2026-11-28T09:00:00Z")
    scripted_runs(
        stream,
        [f"2026-{day}T09:00:00Z" for day in ("11-29", "12-01", "12-04", "12-05")],
    )

    # No run from 12-05 to 2027-02-03. The rollover closes December, the open delta's
    # month, and the run closes January as well, both before it folds the days it
    # counts.
    report = stream.run("2027-02-03T09:00:00Z")

    assert report.counted[0] == "2026-12-04"
    assert (report.serving, report.serving_reason) == ("updated", None)
    published = stream.open(CURRENT, SERVING)
    check(published, stream.open(CURRENT, STATE))
    rebuilt = rebuilt_from_releases(stream, date(2027, 2, 3))
    assert serving_tables(rebuilt) == serving_tables(published)
    # December's close ranks the days of December counted before 2027 began into
    # 2027's set. January had no run, so its close adds no one.
    entries = set(published.execute("SELECT DISTINCT year, entered FROM top_set"))
    assert entries == {(2026, 202611), (2027, 202612)}


def test_a_serving_database_that_fails_its_check_stops_the_run_before_it_publishes(
    tmp_path, monkeypatch
):
    stream = Stream(tmp_path)
    stream.bootstrap(BOOTSTRAP_AT)
    stream.notify(0.0, at="2026-11-12T08:00:00Z")
    published = stream.github.asset_bytes(CURRENT, MANIFEST)
    rebuilt: list[str] = []

    def miscounting(state, conn, delta, counted, **options) -> int:
        # Each project's count of a day written is one more than the day's row sums.
        new = write_day(state, conn, delta, counted, **options)
        conn.execute(
            "UPDATE counts SET downloads = downloads + 1 WHERE day = ?",
            (day_number(counted.day),),
        )
        return new

    def noted_rebuild(path, state, sources, *, today):
        rebuilt.append(path.name)
        rebuild(path, state, sources, today=today)

    monkeypatch.setattr(run_module, "write_day", miscounting)
    monkeypatch.setattr(serving_module, "rebuild", noted_rebuild)

    # The updated database disagrees with the state, and so does the one rebuilt from
    # the same counts.
    with pytest.raises(
        ServingMismatch, match="months disagrees with the state's days in 2026-11"
    ):
        stream.run("2026-11-12T09:00:00Z")

    assert rebuilt == [SERVING_DB]
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == published
    assert stream.manifest().generated_at == BOOTSTRAP_AT
    assert stream.cloud.acked == []


# ---- the counter's job


def test_counter_template_matches_stream_toml():
    raw = tomllib.loads(STREAM_TOML.read_text(encoding="utf-8"))
    gcp, counter = raw["gcp"], raw["counter"]

    template = counter_template(CONFIG, IMAGE)

    assert template == {
        "taskCount": 1,
        "template": {
            "containers": [
                {
                    "image": IMAGE,
                    "resources": {
                        "limits": {
                            "cpu": str(counter["cpu"]),
                            "memory": f"{counter['memory_gib']}Gi",
                        }
                    },
                    "env": [
                        {"name": "PROJECT", "value": gcp["project"]},
                        {"name": "BUCKET", "value": gcp["bucket"]},
                        {"name": "WORKERS", "value": str(counter["workers"])},
                    ],
                }
            ],
            "timeout": f"{counter['task_timeout_seconds']}s",
            "maxRetries": 0,
            "serviceAccount": gcp["counter_service_account"],
        },
    }


def test_deploy_signs_in_as_the_deployer_and_deploys_the_job(tmp_path, monkeypatch):
    stream = Stream(tmp_path)

    assert (
        stream.main(monkeypatch, "2026-11-11T09:00:00Z", "deploy", "--image", IMAGE)
        == 0
    )

    assert stream.signed_in_as == [CONFIG.deployer]
    assert stream.cloud.jobs == {CONFIG.job: counter_template(CONFIG, IMAGE)}


def test_main_runs_as_the_tick_s_service_account(tmp_path, monkeypatch, capsys):
    stream = Stream(tmp_path)

    assert stream.main(monkeypatch, BOOTSTRAP_AT, "run", "--bootstrap") == 0

    assert stream.signed_in_as == [CONFIG.federation.service_account]
    assert printed_report(capsys)["counted"] == ["2026-11-09"]
    assert stream.manifest().data_through == Watermark("2026-11-09", "day")
    # Each run works in a directory of its own in ./work, which is removed.
    assert list((tmp_path / "work").iterdir()) == []
