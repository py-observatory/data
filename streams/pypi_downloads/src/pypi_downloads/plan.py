"""Which days are due, and the budget guard that keeps the stream inside Google Cloud's
free tier. Everything here is pure: the clock, the usage and the spend come in as
parameters, so the guard can be tested at any instant."""

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

FIRST_DAY = date(2016, 1, 22)
"""The first day in the table."""
WAIT_DAYS = 2
"""A day D is counted on D+2 UTC or later."""
BACKFILL_SHARE = 0.8
"""Backfill stays under this share of the free tier, spread evenly over the month."""
STOP_SHARE = 0.98
"""No counting starts once any measure reaches this share of the free tier."""
NOTICE_HOURS = 48
"""Backfill needs a budget notification for the month newer than this."""
PACIFIC = ZoneInfo("America/Los_Angeles")
"""Google's free tier resets with the billing month, which follows this zone."""


@dataclass(frozen=True)
class Usage:
    """What a billing month has used of the three measured parts of the free tier."""

    vcpu_seconds: float
    gib_seconds: float
    read_bytes: int


FREE = Usage(240_000, 450_000, 300 * 2**40)
"""The monthly free tier: Cloud Run jobs' vCPU- and GiB-seconds, and Storage Read API
bytes. The 1 TiB of SQL is not here because the stream scans no table data."""


@dataclass(frozen=True)
class Spend:
    """The budget's latest report of a billing month's cost."""

    month: str
    cost: float
    budget: float
    notified_at: str


@dataclass(frozen=True)
class Refusal:
    """Why the guard says no. `kind` is "spend" (money was spent), "stop" (a measure is
    at the stop share), "notice" (backfill has no recent budget notification) or "line"
    (backfill is at the budget line)."""

    kind: str
    reason: str


def latest_due(today: date) -> date:
    """The newest day that may be counted on `today`."""
    return today - timedelta(days=WAIT_DAYS)


def forward_days(counted: set[date], launch: date, today: date) -> list[date]:
    """The uncounted days from launch to the newest due day, oldest first."""
    last = latest_due(today)
    days = (launch + timedelta(days=n) for n in range((last - launch).days + 1))
    return [day for day in days if day not in counted]


def backfill_days(counted: set[date], launch: date) -> Iterator[date]:
    """The uncounted days before launch, newest first, down to the first day."""
    day = launch - timedelta(days=1)
    while day >= FIRST_DAY:
        if day not in counted:
            yield day
        day -= timedelta(days=1)


def billing_month(now: datetime) -> tuple[datetime, datetime]:
    """The start and end, in UTC, of the Pacific calendar month holding `now`. The two
    instants are real, so the month's length follows its days and any clock change."""
    local = now.astimezone(PACIFIC)
    start = datetime(local.year, local.month, 1, tzinfo=PACIFIC)
    following = datetime(
        local.year + local.month // 12, local.month % 12 + 1, 1, tzinfo=PACIFIC
    )
    return start.astimezone(UTC), following.astimezone(UTC)


_MEMORY_UNITS_GIB = {"Ki": 2**-20, "Mi": 2**-10, "Gi": 1.0, "Ti": 2**10}


def _vcpus(cpu: str) -> float:
    """Cloud Run's cpu limit: whole or fractional ("4", "0.5"), or millicores ("4000m")."""
    if cpu.endswith("m"):
        return int(cpu[:-1]) / 1000
    return float(cpu)


def _gib(memory: str) -> float:
    """Cloud Run's memory limit in GiB ("2Gi", "512Mi")."""
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(Ki|Mi|Gi|Ti)", memory)
    if match is None:
        raise ValueError(f"unknown memory limit {memory!r}")
    return float(match[1]) * _MEMORY_UNITS_GIB[match[2]]


def _time(text: str) -> datetime:
    """An RFC 3339 time from Cloud Run, with fractional seconds and a Z."""
    return datetime.fromisoformat(text)


def execution_cost(
    execution: dict, now: datetime, *, since: datetime | None = None
) -> tuple[float, float]:
    """The vCPU-seconds and GiB-seconds of one Cloud Run execution at its own limits,
    over `max(startTime, since)` to `completionTime or now`. `since` is the start of a
    billing month: Google bills the part of an execution after that instant to the new
    month's free tier."""
    limits = execution["template"]["containers"][0]["resources"]["limits"]
    start = _time(execution["startTime"])
    if since is not None:
        start = max(start, since)
    end = _time(execution["completionTime"]) if execution.get("completionTime") else now
    seconds = max((end - start).total_seconds(), 0.0)
    return _vcpus(limits["cpu"]) * seconds, _gib(limits["memory"]) * seconds


def execution_usage(executions: list[dict], start: datetime, now: datetime) -> Usage:
    """The compute used from `start` on by executions that were running then or began
    after, finished or still running. One that began before `start` counts only its part
    after it, and one that finished before `start` counts nothing. An execution with no
    start time has not used any yet. Read bytes are counted elsewhere, from the days
    counted."""
    vcpu = gib = 0.0
    for execution in executions:
        if "startTime" not in execution:
            continue
        end = (
            _time(execution["completionTime"])
            if execution.get("completionTime")
            else now
        )
        if end <= start:
            continue
        cost = execution_cost(execution, now, since=start)
        vcpu += cost[0]
        gib += cost[1]
    return Usage(vcpu, gib, 0)


def spend_from(messages: list[dict]) -> list[Spend]:
    """One Spend per billing month from budget notifications, the newest of each. The
    month is the first seven characters of `costIntervalStart`, which is the start of
    the Pacific billing month."""
    newest: dict[str, tuple[datetime, Spend]] = {}
    for message in messages:
        published = _time(message["publishTime"])
        month = message["costIntervalStart"][:7]
        if month not in newest or published > newest[month][0]:
            spend = Spend(
                month,
                message["costAmount"],
                message["budgetAmount"],
                message["publishTime"],
            )
            newest[month] = (published, spend)
    return [spend for _, spend in newest.values()]


def refusal(
    usage: Usage, spend: Spend | None, now: datetime, *, backfill: bool, reserve: Usage
) -> Refusal | None:
    """None to go on, or why counting must not start. `spend` is the newest report in
    hand, of any month; only one for the billing month of `now` counts. The caller
    passes `reserve`, the worst case of the execution about to start (its cpu times the
    task timeout, its memory times the task timeout, and the largest read bytes of a
    counted day), and the stop share applies to `usage + reserve`. The budget line
    applies to `usage` alone."""
    start, end = billing_month(now)
    month = now.astimezone(PACIFIC).strftime("%Y-%m")
    current = spend if spend is not None and spend.month == month else None
    if current is not None and current.cost > 0:
        return Refusal(
            "spend", f"the budget shows ${current.cost:.2f} spent in {month}"
        )
    for (name, used, free), (_, extra, _) in zip(_measures(usage), _measures(reserve)):
        if used + extra >= STOP_SHARE * free:
            return Refusal(
                "stop",
                f"{name} would reach {(used + extra) / free:.1%} of the free tier "
                f"({used:,.0f} used, {extra:,.0f} reserved for the next execution)",
            )
    if not backfill:
        return None
    if current is None or now - _time(current.notified_at) >= timedelta(
        hours=NOTICE_HOURS
    ):
        return Refusal(
            "notice",
            f"no budget notification for {month} in the last {NOTICE_HOURS} hours",
        )
    elapsed = (now - start) / (end - start)
    for name, used, free in _measures(usage):
        line = BACKFILL_SHARE * free * elapsed
        if used >= line:
            return Refusal(
                "line", f"{name} ({used:,.0f}) is at the budget line ({line:,.0f})"
            )
    return None


def _measures(usage: Usage) -> list[tuple[str, float, float]]:
    """Each measure with its name and what the free tier gives of it."""
    return [
        ("vCPU-seconds", usage.vcpu_seconds, FREE.vcpu_seconds),
        ("GiB-seconds", usage.gib_seconds, FREE.gib_seconds),
        ("read bytes", usage.read_bytes, FREE.read_bytes),
    ]
