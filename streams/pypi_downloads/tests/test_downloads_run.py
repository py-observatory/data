import dataclasses
import json
import re
import tomllib
from contextlib import closing
from datetime import UTC, date, datetime

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
    PART_NOVEMBER,
    READ_BYTES,
    STATE,
    Stream,
    close_connections,  # autouse: it closes what each test of this module opened
    counted_days,
    counts_rows,
    execution,
    rows_of,
    sealed_name,
)
from pypi_downloads import run as run_module
from pypi_downloads.config import STREAM_TOML
from pypi_downloads.plan import BACKFILL_SHARE, FIRST_DAY, FREE, billing_month
from pypi_downloads.run import (
    BREAKS,
    STORE,
    BootstrapError,
    RunReport,
    counter_template,
)
from pypi_downloads.store import STATE_SCHEMA, STATE_TABLES, day_number

from obskit import cycle
from obskit.gcp import GoogleError
from obskit.github import GitHubError
from obskit.manifest import TableInfo, Watermark
from obskit.release import fetch_assets
from obskit.sqlite import diff_tables, open_db

# Most tests start with a bootstrap at this time, whose newest due day is the launch.
BOOTSTRAP_AT = "2026-11-11T09:00:00Z"
LAUNCH = date(2026, 11, 9)
# Each execution of the fake counter takes 600 seconds at 4 vCPU and 2 GiB.
VCPU_SECONDS = 2400.0
GIB_SECONDS = 1200.0
IMAGE = "us-central1-docker.pkg.dev/py-observatory/counter/counter@sha256:" + "a" * 64


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
    # The current release holds the state, the open delta and the manifest. The
    # month's release holds the state as the month began, which is the same bytes.
    assert stream.assets(CURRENT) == sorted([STATE, DELTA, MANIFEST])
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
    # The non-canonical name is stored canonical, and each project is first seen on
    # the launch day.
    assert state.execute("SELECT * FROM projects ORDER BY project_id").fetchall() == [
        (1, "new-20261109", day_number(LAUNCH)),
        (2, "numpy", day_number(LAUNCH)),
        (3, "requests", day_number(LAUNCH)),
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
    state_entry, delta_entry, base_entry = manifest.assets
    assert (delta_entry.name, delta_entry.period) == (DELTA, "2026-11-11")
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
    }
    assert manifest.complete is False
    assert report == RunReport(
        counted=["2026-11-09"],
        reused=[],
        failed=[],
        held=None,
        held_kind=None,
        drift=[],
        new_projects=3,
        renamed=1,
        usage={
            "vcpu_seconds": VCPU_SECONDS,
            "gib_seconds": GIB_SECONDS,
            "read_bytes": READ_BYTES,
        },
        gate_failures=[],
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

    # The day after launch needs no notification. The backfill does.
    assert stream.main(monkeypatch, "2026-11-12T09:00:00Z", "run") == 2

    assert days_after_bootstrap(stream) == ["2026-11-10"]
    report = printed_report(capsys)
    assert (report["counted"], report["held_kind"]) == (["2026-11-10"], "notice")


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
    # The run fails after it has pulled the notification: BigQuery refuses the query
    # of the partition sizes.
    stream.cloud.fail_next(403, url_contains="bigquery")

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
