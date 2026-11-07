"""The pypi-downloads run: which days it counts, how it has the counter count them, what
keeps it inside Google Cloud's free tier, and how it publishes.

`run_once` is one run, on the kit's day and month cycle (`obskit.cycle`). It reads the
prior state from `pypi-downloads-current`, which fails closed: a state that cannot be
read ends the run, and only a bootstrap starts from nothing.

A day is counted by the counter, a Cloud Run job that reads the day's partition of
PyPI's downloads table and leaves a counts file for it in the bucket. The run starts
one execution at a time and waits for it, checks the file (`counts.read_counts`) and
writes the day to the open delta, which it commits and applies at once, so that a later
failure in the run still leaves whole files to publish. A day whose file is in the
bucket already, because an execution outlived the run that started it, is read from
there and never counted again. A day fails when its execution does not succeed, Google
refuses a call, the wait runs out or its file is not one that the counter writes, and
the run then starts no more days.

The first run, the bootstrap, counts one day, the newest that is due, which is the
launch day, and writes the known breaks in the series. Every later run counts the days
from launch on that are due and not yet counted, oldest first, and then the backfill:
the days before launch, newest first, back to the first day of PyPI's table. Before
each day it asks the budget guard (`plan.refusal`). No counting starts once the budget
shows any spend above $0, in any billing month, that a person has not cleared, or once
the next execution could take a measure of the free tier to 98%, and the backfill
starts only below the budget line and with a recent budget notification. A run also
stops starting days once its time is up.

A person clears a spend by giving the run a reason (`--override-reason`), once they have
looked at the billing report. The run then records a clearance of each month's cost as
it stands, and counts. A clearance covers only that cost, so more spend in the same
month holds the stream again.

The budget's notifications are acknowledged only once a publish holds their spend. A
run that publishes nothing leaves them to Pub/Sub, which delivers them again to the next
run, so a spend above $0 is never forgotten.

Each later run reads the sizes of the partitions of the newest days counted from launch
on, and names each day whose partition has changed since it was counted. Late rows are
detected, not recounted. A run whose read of the sizes fails says that the drift is
unknown, and goes on.

`main` exits 2 when a day failed, a partition moved or the drift is unknown, or the
guard held the run for spend or for the 98% stop, after the publish, so that the
workflow opens its failure issue. A backfill held for want of a notification exits 2
only when no notification of any month has come for NOTICE_HOURS, which is when the
feed is broken: a billing month that has begun without its first is not. A backfill
held at the budget line is the normal end of a run. Every other error propagates, and a
run that fails publishes nothing to the current release.
"""

import argparse
import json
import sqlite3
import sys
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from itertools import chain
from pathlib import Path

from obskit import cycle
from obskit.gates import GateFailure
from obskit.gcp import Credentials, Federation, Google, GoogleError
from obskit.github import GitHub
from obskit.manifest import Asset, Manifest, Watermark, instant
from obskit.release import fetch_prior
from obskit.runs import github_from_env, run_directory
from obskit.sqlite import open_db
from pypi_downloads import STREAM
from pypi_downloads.config import Config, load
from pypi_downloads.counts import Counted, CountsError, read_counts, write_day
from pypi_downloads.plan import (
    FIRST_DAY,
    Refusal,
    Spend,
    Usage,
    backfill_days,
    billing_month,
    execution_cost,
    execution_usage,
    forward_days,
    latest_due,
    notices_quiet,
    refusal,
    spend_from,
    uncleared,
)
from pypi_downloads.store import (
    DELTA_SCHEMA,
    STATE_SCHEMA,
    STATE_TABLES,
    Delta,
    apply,
    day_date,
    day_number,
    meta,
)

# The days where the series changes meaning, which the bootstrap writes to the state and
# to the first manifest.
BREAKS = [
    (date(2018, 7, 26), "Earlier days are undercounted."),
    (
        date(2026, 8, 24),
        "PyPI stopped counting PEP 658 metadata fetches, and counts fell 35-37%.",
    ),
]
# How many of the newest days counted from launch on each run checks for late rows.
DRIFT_DAYS = 10

# The most that a run reads of a day's summary, which is a few hundred bytes, and of its
# database, which is about 20 MB.
SUMMARY_MAX_BYTES = 65_536
DATABASE_MAX_BYTES = 268_435_456
# How long a run waits for an execution beyond the task's own timeout, for Cloud Run to
# start it and to say that it has finished.
WAIT_MARGIN_SECONDS = 300
# The read bytes that the guard keeps for the next execution until a day has been
# counted to say how much one reads.
RESERVE_READ_BYTES = 2 * 2**40
# The kinds of refusal that make `main` exit 2. A refusal for want of a notification
# does too when no notification of any month is recent (see `_unwell`), and a refusal at
# the budget line is the normal end of a run's backfill.
FAILING_HOLDS = ("spend", "stop")

# The sizes of the partitions that a run checks. It reads table metadata and scans no
# table data, so it is free.
_PARTITIONS_SQL = (
    "SELECT partition_id, total_rows "
    "FROM `bigquery-public-data.pypi.INFORMATION_SCHEMA.PARTITIONS` "
    "WHERE table_name = 'file_downloads' AND partition_id IN ({})"
)
_DAY = timedelta(days=1)


def _store() -> cycle.Store:
    """The stream as the kit's cycle sees it: its id, its schemas, the tables of its
    state and how a delta applies to it, from store.py, and the history class of each
    table and the licences of its upstreams, from its stream.toml. The Store checks as
    it is made that STATE_TABLES names every table of the state."""
    config = load()
    return cycle.Store(
        stream=STREAM,
        state_schema=STATE_SCHEMA,
        delta_schema=DELTA_SCHEMA,
        state_tables=STATE_TABLES,
        classes=config.tables,
        licences=config.upstreams,
        apply=apply,
    )


# Made as the module is imported, so a state schema that STATE_TABLES does not match
# stops the stream before any run starts.
STORE = _store()


class BootstrapError(RuntimeError):
    """The bootstrap could not count its launch day, so it published nothing."""


@dataclass(frozen=True)
class RunReport:
    """What a run did.

    Days are YYYY-MM-DD. `counted` names the days that executions of this run counted,
    `reused` those read from the bucket, where an execution of an earlier run left
    them, and `failed` the day that failed, after which the run started no more.
    `held` is why the budget guard stopped the run from starting a day, and `held_kind`
    the kind of that refusal (see `plan.Refusal`). Both are None when the guard let
    every day start. `notices_quiet` is whether no budget notification of any month
    has come in the last NOTICE_HOURS (see `plan.notices_quiet`). `drift` names the
    days whose partition has changed since they were counted, and `drift_unknown` is
    whether the read of the partitions failed, which leaves `drift` empty.
    `new_projects` counts the projects first seen in the days written, and `renamed`
    the names in their files that were not canonical. `usage` is the free tier used in
    the billing month, as last measured: vCPU- and GiB-seconds of the counter's
    executions, and the bytes read for the days counted. `gate_failures` lists the
    failures that a person overrode, and is empty when no gate failed.
    """

    counted: list[str]
    reused: list[str]
    failed: list[str]
    held: str | None
    held_kind: str | None
    notices_quiet: bool
    drift: list[str]
    drift_unknown: bool
    new_projects: int
    renamed: int
    usage: dict
    gate_failures: list[GateFailure]


@dataclass(frozen=True)
class _Resume:
    """Where the prior state stands, for a run that goes on from it.

    `carried` is the entries of the state and of the open delta in the prior manifest.
    `applied` is the highest `seq` that the state has applied, and `launch` the first
    day that the stream counted.
    """

    carried: tuple[Asset, Asset]
    applied: int
    launch: date


@dataclass(frozen=True)
class _Ensured:
    """What became of a day: "counted" by an execution of this run, whose name and
    cost are `execution`; "reused" from the bucket; or "failed". `counted` is the
    checked file of a day that did not fail."""

    outcome: str
    counted: Counted | None = None
    execution: dict | None = None


@dataclass
class _Days:
    """The days that a run has taken so far, by what became of them, and what their
    files held."""

    counted: list[str] = field(default_factory=list)
    reused: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    new_projects: int = 0
    renamed: int = 0


def _required_meta(state: sqlite3.Connection, key: str) -> str:
    """The value of `key` in the state's meta table. Raises ValueError when there is
    none: the bootstrap writes every key that a later run reads."""
    value = meta(state, key)
    if value is None:
        raise ValueError(f"the state has no {key} in meta, so no bootstrap built it")
    return value


def _resume(prior: Manifest, workdir: Path) -> _Resume:
    """Check the state that `fetch_prior` left in `workdir` for the prior manifest
    `prior`, and read where it stands.

    Raises PriorStateError unless `prior` lists the state and the open delta (see
    `cycle.carried`). Both databases are opened without making them (`open_db` raises
    FileNotFoundError or ValueError for one that is missing or not of its schema), so a
    state that is not there is an error and never an empty one. Raises ValueError when
    the state lacks its applied_seq or its launch.
    """
    carried = cycle.carried(STORE, prior)
    with (
        closing(open_db(workdir / STORE.state_db, STATE_SCHEMA)) as state,
        closing(open_db(workdir / STORE.open_delta_db, DELTA_SCHEMA)),
    ):
        applied = int(_required_meta(state, "applied_seq"))
        launch = date.fromisoformat(_required_meta(state, "launch"))
    return _Resume(carried, applied, launch)


def _commit_and_apply(state: sqlite3.Connection, conn: sqlite3.Connection) -> None:
    """Commit what the run has written to its open delta, `conn`, and apply the rows
    that the state has not applied with the stream's apply, which commits the state.
    Both files are then whole on disk for the publish to compress."""
    conn.commit()
    STORE.apply(state, conn)


def _failed(day: date, why: str) -> _Ensured:
    """A day that failed, with why written to stderr for the run's log."""
    print(f"{day} failed: {why}", file=sys.stderr)
    return _Ensured("failed")


def _read(
    google: Google, config: Config, day: date, summary: bytes, workdir: Path
) -> Counted:
    """The checked counts file of `day`, whose summary is `summary`, with its database
    read from the bucket. Raises CountsError for a file that is not one the counter
    writes, the database missing included."""
    db = google.get_object(
        config.bucket, f"counts/{day}.db.zst", max_bytes=DATABASE_MAX_BYTES
    )
    if db is None:
        raise CountsError(f"the bucket holds the summary of {day} and not its database")
    return read_counts(summary, db, day, workdir)


def _ensure(
    google: Google, config: Config, day: date, workdir: Path, now: datetime
) -> _Ensured:
    """Have the counts file of `day` in the bucket, and read it.

    A file whose summary is there already is "reused". Otherwise an execution of the
    counter is started for the day and waited for, up to the task's timeout and
    WAIT_MARGIN_SECONDS, and the day is "counted" when the execution succeeded, with
    the execution's name and its vCPU- and GiB-seconds. The day "failed" when the
    execution did not succeed, when a call to Google is refused or the wait runs out,
    and when its file is not one that the counter writes (CountsError). Why goes to
    stderr. An execution that the wait gave up on may still finish and leave its file,
    which the next run reuses. Nothing is written to the stream's databases here.
    """
    summary_name = f"counts/{day}.json"
    try:
        summary = google.get_object(
            config.bucket, summary_name, max_bytes=SUMMARY_MAX_BYTES
        )
        if summary is not None:
            return _Ensured("reused", _read(google, config, day, summary, workdir))
        operation = google.run_job(
            config.project, config.region, config.job, {"DAY": day.isoformat()}
        )
        execution = google.wait(
            operation,
            timeout_seconds=config.task_timeout_seconds + WAIT_MARGIN_SECONDS,
        )
        if execution.get("succeededCount") != 1:
            return _failed(
                day, f"the execution {execution.get('name')} did not succeed"
            )
        summary = google.get_object(
            config.bucket, summary_name, max_bytes=SUMMARY_MAX_BYTES
        )
        if summary is None:
            return _failed(day, "the execution succeeded and left no summary")
        vcpu_seconds, gib_seconds = execution_cost(execution, now)
        cost = {
            "name": execution["name"],
            "vcpu_seconds": vcpu_seconds,
            "gib_seconds": gib_seconds,
        }
        return _Ensured("counted", _read(google, config, day, summary, workdir), cost)
    except (GoogleError, CountsError) as error:
        return _failed(day, str(error))


def _take(
    google: Google,
    config: Config,
    state: sqlite3.Connection,
    conn: sqlite3.Connection,
    delta: Delta,
    day: date,
    *,
    workdir: Path,
    now: datetime,
    moment: str,
    days: _Days,
) -> str:
    """Ensure `day` (see `_ensure`), write a day that did not fail to the open delta,
    and commit and apply it. Notes the day in `days`, and returns what became of it."""
    ensured = _ensure(google, config, day, workdir, now)
    if ensured.outcome == "failed" or ensured.counted is None:
        days.failed.append(day.isoformat())
        return "failed"
    days.new_projects += write_day(
        state,
        conn,
        delta,
        ensured.counted,
        execution=ensured.execution,
        counted_at=moment,
    )
    days.renamed += ensured.counted.renamed
    _commit_and_apply(state, conn)
    taken = days.counted if ensured.outcome == "counted" else days.reused
    taken.append(day.isoformat())
    return ensured.outcome


def _record_spend(google: Google, config: Config, delta: Delta) -> list[str]:
    """Pull the budget's notifications without acknowledging them, write the newest of
    each billing month to the open delta, and return the ackIds of every one.

    The run acknowledges them once its publish holds their spend (`_acknowledge`). A
    run that publishes nothing acknowledges nothing, so Pub/Sub delivers them again to
    the next run, and a spend above $0 is never forgotten. The state keeps the newest
    of each month whichever way round they come (see `store.apply`)."""
    messages = google.pull(config.project, config.subscription, acknowledge=False)
    for spend in spend_from(messages):
        delta.spend(spend.month, spend.cost, spend.budget, spend.notified_at)
    return [message["ackId"] for message in messages]


def _acknowledge(google: Google, config: Config, ack_ids: list[str]) -> None:
    """Acknowledge the notifications that the run pulled, once the state that holds
    their spend is published."""
    google.acknowledge(config.project, config.subscription, ack_ids)


def _spends(state: sqlite3.Connection) -> list[Spend]:
    """The newest budget notification of each billing month in the state."""
    rows = state.execute("SELECT month, cost, budget, notified_at FROM spend")
    return [Spend(*row) for row in rows]


def _cleared(state: sqlite3.Connection) -> dict[str, float]:
    """The cost that a person cleared for each billing month in the state."""
    return dict(state.execute("SELECT month, cost FROM clearances"))


def _clear_spend(
    state: sqlite3.Connection, conn: sqlite3.Connection, delta: Delta, reason: str
) -> None:
    """Record a clearance, for `reason`, of each month whose spend would hold the run,
    at the month's cost as the state holds it, and commit and apply them. A person
    gives the reason once they have looked at the billing report. Each month cleared is
    written to stderr for the run's log."""
    for spend in uncleared(_spends(state), _cleared(state)):
        delta.clearance(spend.month, spend.cost, reason)
        print(f"cleared ${spend.cost} spent in {spend.month}", file=sys.stderr)
    _commit_and_apply(state, conn)


def _wall(now: datetime, clock: Callable[[], float], started: float) -> datetime:
    """The time at this moment. `now` is the time at which `clock` read `started`, as
    the run began, and what `clock` has run since is added to it. Usage measured at
    `now` alone would count nothing of an execution still running since then."""
    return now + timedelta(seconds=clock() - started)


def _usage(
    google: Google, config: Config, state: sqlite3.Connection, now: datetime
) -> Usage:
    """What the billing month of `now` has used of the free tier: the vCPU- and
    GiB-seconds of the counter's executions in it, as Cloud Run lists them, and the
    bytes read for the days counted in it."""
    start, _ = billing_month(now)
    executions = google.executions(config.project, config.region, config.job)
    (read,) = state.execute(
        "SELECT coalesce(sum(read_bytes), 0) FROM days WHERE counted_at >= ?",
        (instant(start),),
    ).fetchone()
    return replace(execution_usage(executions, start, now), read_bytes=read)


def _reserve(config: Config, state: sqlite3.Connection) -> Usage:
    """The most that the next execution can use: its cpu and its memory for the whole
    task timeout, and the bytes of the largest read of a day counted so far, or
    RESERVE_READ_BYTES before any."""
    (largest,) = state.execute("SELECT max(read_bytes) FROM days").fetchone()
    return Usage(
        config.cpu * config.task_timeout_seconds,
        config.memory_gib * config.task_timeout_seconds,
        RESERVE_READ_BYTES if largest is None else largest,
    )


def _held(
    usage: Usage,
    config: Config,
    state: sqlite3.Connection,
    now: datetime,
    *,
    backfill: bool,
) -> Refusal | None:
    """Why the guard refuses to start the next day, a backfill day when `backfill`, or
    None to go on (see `plan.refusal`)."""
    return refusal(
        usage,
        _spends(state),
        _cleared(state),
        now,
        backfill=backfill,
        reserve=_reserve(config, state),
    )


def _drift(
    google: Google, config: Config, state: sqlite3.Connection, launch: date
) -> list[str]:
    """The days among the newest DRIFT_DAYS counted from `launch` on whose partition
    has changed since they were counted: its total_rows is not the rows_bigquery that
    the day was counted with, or the partition is gone. One query of the partitions'
    metadata reads them all."""
    rows = state.execute(
        "SELECT day, rows_bigquery FROM days WHERE day >= ? ORDER BY day DESC LIMIT ?",
        (day_number(launch), DRIFT_DAYS),
    ).fetchall()
    if not rows:
        return []
    stored = {f"{day_date(day):%Y%m%d}": (day_date(day), count) for day, count in rows}
    listed = ", ".join(f"'{partition}'" for partition in stored)
    found = {
        partition: total
        for partition, total, *_ in google.query(
            config.project, _PARTITIONS_SQL.format(listed)
        )
    }
    return sorted(
        day.isoformat()
        for partition, (day, count) in stored.items()
        if found.get(partition) is None or int(found[partition]) != count
    )


def _drift_or_unknown(
    google: Google, config: Config, state: sqlite3.Connection, launch: date
) -> list[str] | None:
    """The days whose partition moved (see `_drift`), or None when Google refuses the
    read of the partitions. Why goes to stderr. A drift check that fails says nothing of
    the days to count, so the run goes on, and reports the drift as unknown."""
    try:
        return _drift(google, config, state, launch)
    except GoogleError as error:
        print(f"the drift check failed: {error}", file=sys.stderr)
        return None


def _counted(state: sqlite3.Connection) -> set[date]:
    """The days that the state has counted."""
    return {day_date(day) for (day,) in state.execute("SELECT day FROM days")}


def _run_end(counted: set[date], start: date, step: timedelta) -> date:
    """The last day of the unbroken run of counted days that goes from `start` in
    steps of `step`, or `start` when the next day is not counted."""
    day = start
    while day + step in counted:
        day += step
    return day


def _summary(
    state: sqlite3.Connection, launch: date, today: date
) -> tuple[Watermark, dict, bool]:
    """What the manifest says of the state: how far the data reaches, what it covers
    and whether it is complete.

    The data reaches the last day of the unbroken run of counted days from launch on.
    The coverage gives the launch day, the first day of the unbroken run of counted
    days that ends at it, the newest day counted, the number of days counted, the days
    from launch on that are due and not counted, the days before the first day that
    are left to the backfill, and the projects. The data is complete when the backfill
    has reached the first day of PyPI's table and no day after launch is missing.
    """
    counted = _counted(state)
    first = _run_end(counted, launch, -_DAY)
    missing = len(forward_days(counted, launch, today))
    (projects,) = state.execute("SELECT count(*) FROM projects").fetchone()
    coverage = {
        "launch": launch.isoformat(),
        "first_day": first.isoformat(),
        "last_day": max(counted, default=launch).isoformat(),
        "days": len(counted),
        "forward_missing": missing,
        "backfill_left": (first - FIRST_DAY).days,
        "projects": projects,
    }
    through = Watermark(_run_end(counted, launch, _DAY).isoformat(), "day")
    return through, coverage, first == FIRST_DAY and missing == 0


def _report(
    days: _Days,
    *,
    held: Refusal | None,
    quiet: bool,
    drift: list[str] | None,
    usage: Usage,
    failures: list[GateFailure],
) -> RunReport:
    return RunReport(
        counted=days.counted,
        reused=days.reused,
        failed=days.failed,
        held=None if held is None else held.reason,
        held_kind=None if held is None else held.kind,
        notices_quiet=quiet,
        drift=[] if drift is None else drift,
        drift_unknown=drift is None,
        new_projects=days.new_projects,
        renamed=days.renamed,
        usage=asdict(usage),
        gate_failures=failures,
    )


def _bootstrap(
    gh: GitHub,
    google: Google,
    workdir: Path,
    *,
    config: Config,
    now: datetime,
    moment: str,
    today: date,
    override_reason: str | None,
    clock: Callable[[], float],
    started: float,
) -> RunReport:
    """Build the stream's first state from its launch day, the newest day that is due
    on `today`, and publish it.

    The state starts with nothing applied, `moment` as its bootstrap_at and the launch
    day, and the open delta with the known breaks in the series and the budget's
    notifications, and with `override_reason`, a clearance of any spend that would hold
    it (see `_clear_spend`). The launch day is ensured (see `_ensure`) once the guard
    allows it, as for a day after launch, and written, and the usage is measured again
    at the time then (see `_wall`). The state is copied as the base of today's month,
    and published with the open delta and a manifest that lists the breaks. Only then
    are the notifications acknowledged.

    Raises BootstrapError, and publishes nothing, when the guard refuses the launch
    day or the day fails: a stream with no day counted has nothing to publish. The
    notifications are then not acknowledged, so a rerun is given them again.
    """
    launch = latest_due(today)
    days = _Days()
    with (
        closing(open_db(workdir / STORE.state_db, STATE_SCHEMA, create=True)) as state,
        closing(
            open_db(workdir / STORE.open_delta_db, DELTA_SCHEMA, create=True)
        ) as conn,
    ):
        state.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [
                ("applied_seq", "0"),
                ("bootstrap_at", moment),
                ("launch", launch.isoformat()),
            ],
        )
        state.commit()
        delta = Delta(conn, moment, first_seq=1)
        for day, note in BREAKS:
            delta.series_break(day_number(day), note)
        pulled = _record_spend(google, config, delta)
        _commit_and_apply(state, conn)
        if override_reason:
            _clear_spend(state, conn, delta, override_reason)
        quiet = notices_quiet(_spends(state), now)
        usage = _usage(google, config, state, now)
        held = _held(usage, config, state, now, backfill=False)
        if held is not None:
            raise BootstrapError(
                f"the budget guard holds the launch day {launch}: {held.reason}"
            )
        outcome = _take(
            google,
            config,
            state,
            conn,
            delta,
            launch,
            workdir=workdir,
            now=now,
            moment=moment,
            days=days,
        )
        if outcome == "failed":
            raise BootstrapError(f"the launch day {launch} failed")
        if outcome == "counted":
            usage = _usage(google, config, state, _wall(now, clock, started))
        through, coverage, complete = _summary(state, launch, today)
    cycle.publish_first(
        gh,
        STORE,
        workdir,
        today=today,
        moment=moment,
        data_through=through,
        cursor=None,
        coverage=coverage,
        complete=complete,
        series_breaks=[{"day": day.isoformat(), "note": note} for day, note in BREAKS],
    )
    _acknowledge(google, config, pulled)
    return _report(days, held=None, quiet=quiet, drift=[], usage=usage, failures=[])


def run_once(
    gh: GitHub,
    google: Google,
    workdir: Path,
    *,
    config: Config,
    now: datetime,
    bootstrap: bool,
    override_reason: str | None = None,
    max_seconds: float = 3600,
    clock: Callable[[], float] = time.monotonic,
) -> RunReport:
    """One run of the stream at `now`, in the empty directory `workdir`.

    It reads the prior state (`fetch_prior`), which only `bootstrap` may find missing,
    and then a bootstrap builds and publishes the first state (see `_bootstrap`).
    Otherwise the prior manifest must list the state and the open delta
    (PriorStateError). The run rolls over (`cycle.roll_over`), writes the budget's
    notifications to the delta, without acknowledging them, and with `override_reason`
    a clearance of any spend that would hold it (see `_clear_spend`), and applies it.
    It names the days whose partition moved, or that the drift is unknown when the
    read fails, and measures what the billing month has used.

    Then it takes the days from launch on that are due and not counted, oldest first,
    and the backfill, newest first, one at a time (see `_take`). Before each it asks
    the guard (`plan.refusal`), and it measures the usage again after each that an
    execution counted, at the time then (see `_wall`). It stops at a refusal, at a day
    that failed, and once `max_seconds` from its start on `clock` have passed. It then
    publishes (`cycle.publish`): the manifest is checked with the gates, and the state,
    the open delta and the manifest are swapped into the current release. Only then are
    the notifications acknowledged.

    Every error other than a failed day propagates, and an error before the publish
    publishes nothing to the current release and acknowledges no notification, so the
    next run is given them again. What a rollover already added to a month's release
    stays, as in `obskit.cycle`.
    """
    started = clock()
    deadline = started + max_seconds
    moment = instant(now)
    today = now.astimezone(UTC).date()
    prior = fetch_prior(gh, STREAM, workdir, bootstrap=bootstrap)
    # fetch_prior returns None only for a first publish that it has confirmed: with
    # bootstrap, when GitHub answers 404 for the current release.
    if prior is None:
        return _bootstrap(
            gh,
            google,
            workdir,
            config=config,
            now=now,
            moment=moment,
            today=today,
            override_reason=override_reason,
            clock=clock,
            started=started,
        )
    resume = _resume(prior, workdir)
    launch = resume.launch
    added = cycle.roll_over(gh, STORE, workdir, prior, resume.carried, today)
    days = _Days()
    held: Refusal | None = None
    with (
        closing(open_db(workdir / STORE.state_db, STATE_SCHEMA)) as state,
        closing(open_db(workdir / STORE.open_delta_db, DELTA_SCHEMA)) as conn,
    ):
        # A new day's delta is numbered on from what the state has applied, and
        # today's goes on after its own last row.
        delta = Delta(conn, moment, first_seq=resume.applied + 1)
        pulled = _record_spend(google, config, delta)
        _commit_and_apply(state, conn)
        if override_reason:
            _clear_spend(state, conn, delta, override_reason)
        quiet = notices_quiet(_spends(state), now)
        drift = _drift_or_unknown(google, config, state, launch)
        usage = _usage(google, config, state, now)
        counted = _counted(state)
        due = chain(
            ((day, False) for day in forward_days(counted, launch, today)),
            ((day, True) for day in backfill_days(counted, launch)),
        )
        for day, backfill in due:
            if clock() >= deadline:
                break
            held = _held(usage, config, state, now, backfill=backfill)
            if held is not None:
                break
            outcome = _take(
                google,
                config,
                state,
                conn,
                delta,
                day,
                workdir=workdir,
                now=now,
                moment=moment,
                days=days,
            )
            if outcome == "failed":
                break
            if outcome == "counted":
                usage = _usage(google, config, state, _wall(now, clock, started))
        through, coverage, complete = _summary(state, launch, today)
    failures = cycle.publish(
        gh,
        STORE,
        workdir,
        prior,
        added,
        today=today,
        moment=moment,
        data_through=through,
        cursor=None,
        coverage=coverage,
        complete=complete,
        allowed_shrink={},
        extra=[],
        override_reason=override_reason,
    )
    _acknowledge(google, config, pulled)
    return _report(
        days, held=held, quiet=quiet, drift=drift, usage=usage, failures=failures
    )


def counter_template(config: Config, image: str) -> dict:
    """The counter's Cloud Run job, as `Google.deploy_job` takes it: one task that runs
    `image` at the counter's cpu and memory, with the project, the bucket and the
    number of workers in its environment, for at most the task timeout, as the
    counter's service account, and never retried. Each run of the job adds the DAY to
    count."""
    container = {
        "image": image,
        "resources": {
            "limits": {"cpu": str(config.cpu), "memory": f"{config.memory_gib}Gi"}
        },
        "env": [
            {"name": "PROJECT", "value": config.project},
            {"name": "BUCKET", "value": config.bucket},
            {"name": "WORKERS", "value": str(config.workers)},
        ],
    }
    return {
        "taskCount": 1,
        "template": {
            "containers": [container],
            "timeout": f"{config.task_timeout_seconds}s",
            "maxRetries": 0,
            "serviceAccount": config.counter_service_account,
        },
    }


def _google(federation: Federation) -> Google:
    """The Google client of a process, signed in through `federation`, which renews its
    token as it runs out and when Google refuses it."""
    credentials = Credentials(federation)
    return Google(credentials.token, renew=credentials.invalidate)


def _unwell(report: RunReport) -> bool:
    """Whether the workflow should open its failure issue for `report`: a day failed, a
    partition moved or the drift is unknown, or the guard held the run for spend or the
    stop, or for want of a notification while no notification of any month is recent.
    A month that has begun without its first notification is not a failure, nor is the
    budget line."""
    if report.failed or report.drift or report.drift_unknown:
        return True
    if report.held_kind == "notice":
        return report.notices_quiet
    return report.held_kind in FAILING_HOLDS


def main(argv: list[str] | None = None) -> int:
    """Run the stream from the command line: `python -m pypi_downloads run`, or
    `python -m pypi_downloads deploy --image IMAGE`.

    `run` signs in to Google Cloud as the stream's service account, through the
    workflow's OIDC token. It needs GITHUB_TOKEN, and publishes to GITHUB_REPOSITORY,
    or to py-observatory/data when that is not set (see obskit.runs). It works in a new
    directory in the folder that `--workdir` names (./work by default), which is
    removed afterwards, and SQLite's temporary files go in that folder too, unless
    SQLITE_TMPDIR is set. It prints the report as JSON, and returns 2 when the report
    calls for the workflow's failure issue (see `_unwell`), and 0 otherwise. Either
    comes after the publish. `--override-reason` also clears a spend that holds the run
    (see `run_once`).

    `deploy` signs in as the deployer, and creates or updates the counter's job to run
    IMAGE (see `counter_template`). It returns 0.

    Every error propagates.
    """
    parser = argparse.ArgumentParser(
        prog="python -m pypi_downloads", description="The pypi-downloads stream."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser(
        "run",
        help="count the days that are due while the budget allows, and publish them",
    )
    run.add_argument(
        "--bootstrap",
        action="store_true",
        help="count the newest day that is due as the launch day and publish the "
        "first state: only for a stream that has never published",
    )
    run.add_argument(
        "--override-reason",
        default="",
        metavar="TEXT",
        help="publish although a gate failed (except a failed month close), and clear "
        "any spend above $0 that holds the run, for this reason, which the manifest "
        "or the state logs",
    )
    run.add_argument(
        "--max-seconds",
        type=float,
        default=3600,
        help="start no day after this many seconds (default: 3600)",
    )
    run.add_argument(
        "--workdir",
        type=Path,
        default=Path("work"),
        metavar="PATH",
        help="the folder to work in; each run makes a directory of its own there and "
        "removes it (default: work)",
    )
    deploy = commands.add_parser(
        "deploy", help="create or update the counter's Cloud Run job"
    )
    deploy.add_argument(
        "--image",
        required=True,
        help="the counter's image, named by a tag (the deploy workflow passes the "
        "image repository and the commit, as REPOSITORY:COMMIT)",
    )
    args = parser.parse_args(argv)
    config = load()
    if args.command == "deploy":
        deployer = replace(config.federation, service_account=config.deployer)
        _google(deployer).deploy_job(
            config.project,
            config.region,
            config.job,
            counter_template(config, args.image),
        )
        return 0
    gh = github_from_env()
    google = _google(config.federation)
    with run_directory(args.workdir) as workdir:
        report = run_once(
            gh,
            google,
            workdir,
            config=config,
            now=datetime.now(UTC),
            bootstrap=args.bootstrap,
            override_reason=args.override_reason or None,
            max_seconds=args.max_seconds,
        )
    print(json.dumps(asdict(report)))
    return 2 if _unwell(report) else 0
