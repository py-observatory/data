from datetime import UTC, date, datetime, timedelta

import pytest

from pypi_downloads.plan import (
    FIRST_DAY,
    FREE,
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

NOW = datetime(2026, 11, 11, 7, 0, tzinfo=UTC)  # ten days into a 30-day Pacific month
NONE = Usage(0, 0, 0)
FRESH = Spend("2026-11", 0.0, 1.0, "2026-11-11T05:00:00.000Z")


def execution(start, completion=None, cpu="4", memory="2Gi"):
    ex = {
        "name": "e",
        "startTime": start,
        "template": {
            "containers": [{"resources": {"limits": {"cpu": cpu, "memory": memory}}}]
        },
    }
    if completion:
        ex["completionTime"] = completion
    return ex


def message(month_start, cost, published, budget=1.0):
    return {
        "budgetDisplayName": "$1 Monthly Budget Alert",
        "costAmount": cost,
        "costIntervalStart": month_start,
        "budgetAmount": budget,
        "budgetAmountType": "SPECIFIED_AMOUNT",
        "currencyCode": "USD",
        "publishTime": published,
    }


def test_latest_due_is_two_days_back():
    assert latest_due(date(2026, 11, 11)) == date(2026, 11, 9)


def test_forward_days_wait_two_days():
    assert forward_days(set(), date(2026, 11, 9), date(2026, 11, 11)) == [
        date(2026, 11, 9)
    ]
    assert forward_days(set(), date(2026, 11, 9), date(2026, 11, 10)) == []


def test_forward_days_fill_gaps_oldest_first():
    counted = {date(2026, 11, 9), date(2026, 11, 11)}
    assert forward_days(counted, date(2026, 11, 8), date(2026, 11, 13)) == [
        date(2026, 11, 8),
        date(2026, 11, 10),
    ]


def test_backfill_walks_back_from_launch_and_stops_at_the_first_day():
    days = backfill_days({date(2026, 11, 8)}, date(2026, 11, 9))
    assert next(days) == date(2026, 11, 7)
    assert next(days) == date(2026, 11, 6)
    everything = list(backfill_days(set(), date(2016, 1, 25)))
    assert everything == [date(2016, 1, 24), date(2016, 1, 23), FIRST_DAY]
    assert list(backfill_days(set(), FIRST_DAY)) == []


def test_the_billing_month_is_pacific():
    start, end = billing_month(datetime(2026, 12, 1, 3, 0, tzinfo=UTC))
    assert start == datetime(2026, 11, 1, 7, 0, tzinfo=UTC)  # PDT ended 2026-11-01
    assert end == datetime(2026, 12, 1, 8, 0, tzinfo=UTC)
    start, end = billing_month(datetime(2026, 12, 1, 8, 0, tzinfo=UTC))
    assert start == datetime(2026, 12, 1, 8, 0, tzinfo=UTC)
    assert end == datetime(2027, 1, 1, 8, 0, tzinfo=UTC)


def test_the_billing_month_changes_length_with_dst():
    start, end = billing_month(datetime(2026, 3, 15, tzinfo=UTC))
    assert end - start == timedelta(days=31) - timedelta(hours=1)
    start, end = billing_month(datetime(2027, 2, 15, tzinfo=UTC))
    assert end - start == timedelta(days=28)
    start, end = billing_month(datetime(2026, 12, 31, 23, 0, tzinfo=UTC))
    assert end == datetime(2027, 1, 1, 8, 0, tzinfo=UTC)


def test_execution_cost_of_a_finished_execution():
    ex = execution("2026-11-06T19:10:45.912029Z", "2026-11-06T19:20:45.912029Z")
    assert execution_cost(ex, NOW) == (4 * 600.0, 2 * 600.0)


def test_execution_cost_of_a_running_execution_runs_to_now():
    ex = execution("2026-11-11T06:00:00Z", cpu="4000m", memory="512Mi")
    assert execution_cost(ex, NOW) == (4 * 3600.0, 0.5 * 3600.0)


def test_execution_cost_reads_whole_and_milli_cpu_and_both_memory_units():
    done = "2026-11-06T00:01:40Z"
    start = "2026-11-06T00:00:00Z"
    assert execution_cost(execution(start, done, cpu="4", memory="2Gi"), NOW) == (
        400.0,
        200.0,
    )
    assert execution_cost(execution(start, done, cpu="4000m", memory="512Mi"), NOW) == (
        400.0,
        50.0,
    )
    assert execution_cost(execution(start, done, cpu="500m", memory="1Gi"), NOW) == (
        50.0,
        100.0,
    )


def test_execution_cost_refuses_a_unit_it_does_not_know():
    with pytest.raises(ValueError, match="memory"):
        execution_cost(
            execution("2026-11-06T00:00:00Z", "2026-11-06T00:01:00Z", memory="2T"), NOW
        )


def test_execution_usage_counts_its_own_limits_and_running_ones():
    start = datetime(2026, 11, 1, 7, 0, tzinfo=UTC)
    executions = [
        execution(
            "2026-10-31T23:00:00Z", "2026-11-01T01:00:00Z"
        ),  # last month: left out
        execution(
            "2026-11-02T00:00:00Z", "2026-11-02T00:10:00Z"
        ),  # 600 s at 4 vCPU, 2 GiB
        execution("2026-11-11T06:00:00Z", cpu="2", memory="1Gi"),  # running 3600 s
    ]
    assert execution_usage(executions, start, NOW) == Usage(
        4 * 600.0 + 2 * 3600.0, 2 * 600.0 + 1 * 3600.0, 0
    )
    assert execution_usage([], start, NOW) == Usage(0, 0, 0)


def test_spend_from_keeps_the_newest_per_month():
    spends = spend_from(
        [
            message("2026-10-01T07:00:00Z", 0.0, "2026-10-30T20:00:00.5Z"),
            message("2026-11-01T07:00:00Z", 0.0, "2026-11-06T20:00:00.123Z"),
            message(
                "2026-11-01T07:00:00Z", 0.25, "2026-11-08T20:00:00.000Z", budget=2.0
            ),
            message("2026-11-01T07:00:00Z", 0.1, "2026-11-07T20:00:00Z"),
        ]
    )
    by_month = {s.month: s for s in spends}
    assert set(by_month) == {"2026-10", "2026-11"}
    assert by_month["2026-11"] == Spend(
        "2026-11", 0.25, 2.0, "2026-11-08T20:00:00.000Z"
    )
    assert spend_from([]) == []


def test_any_spend_stops_everything():
    spent = Spend("2026-11", 0.01, 1.0, "2026-11-11T05:00:00Z")
    for backfill in (False, True):
        result = refusal(
            Usage(0, 0, 0), [spent], {}, NOW, backfill=backfill, reserve=NONE
        )
        assert result == Refusal("spend", "the budget shows $0.01 spent in 2026-11")
    # A month whose newest notification shows nothing spent holds nothing.
    nothing = [Spend("2026-10", 0.0, 1.0, "2026-10-31T05:00:00Z"), FRESH]
    assert (
        refusal(Usage(0, 0, 0), nothing, {}, NOW, backfill=True, reserve=NONE) is None
    )


def test_a_spend_holds_into_the_next_billing_month():
    # October's spend is not cleared, so it holds November's counting too, though
    # November has spent nothing.
    october = Spend("2026-10", 5.0, 1.0, "2026-10-31T05:00:00Z")
    for backfill in (False, True):
        result = refusal(
            Usage(0, 0, 0), [october, FRESH], {}, NOW, backfill=backfill, reserve=NONE
        )
        assert result == Refusal("spend", "the budget shows $5.0 spent in 2026-10")
    # Cleared, it holds nothing.
    cleared = {"2026-10": 5.0}
    assert (
        refusal(
            Usage(0, 0, 0), [october, FRESH], cleared, NOW, backfill=True, reserve=NONE
        )
        is None
    )


def test_a_cleared_month_holds_again_when_its_cost_rises():
    cleared = {"2026-11": 0.25}
    same = Spend("2026-11", 0.25, 1.0, "2026-11-11T05:00:00Z")
    assert (
        refusal(Usage(0, 0, 0), [same], cleared, NOW, backfill=True, reserve=NONE)
        is None
    )
    more = Spend("2026-11", 0.3, 1.0, "2026-11-11T06:00:00Z")
    result = refusal(Usage(0, 0, 0), [more], cleared, NOW, backfill=False, reserve=NONE)
    assert result == Refusal(
        "spend", "the budget shows $0.3 spent in 2026-11, above the $0.25 cleared"
    )
    # Another month's clearance covers nothing of this one.
    other = {"2026-10": 1.0}
    assert (
        refusal(Usage(0, 0, 0), [same], other, NOW, backfill=False, reserve=NONE).kind
        == "spend"
    )


def test_the_spend_reason_gives_the_cost_as_reported_and_every_month_held():
    small = Spend("2026-11", 0.004, 1.0, "2026-11-11T05:00:00Z")
    october = Spend("2026-10", 0.5, 1.0, "2026-10-31T05:00:00Z")
    result = refusal(
        Usage(0, 0, 0), [small, october], {}, NOW, backfill=False, reserve=NONE
    )
    assert result.reason == (
        "the budget shows $0.5 spent in 2026-10; $0.004 spent in 2026-11"
    )


def test_uncleared_names_the_months_that_hold_oldest_first():
    spends = [
        Spend("2026-11", 0.3, 1.0, "2026-11-11T05:00:00Z"),
        Spend("2026-09", 0.0, 1.0, "2026-09-30T05:00:00Z"),
        Spend("2026-10", 0.2, 1.0, "2026-10-31T05:00:00Z"),
        Spend("2026-08", 0.1, 1.0, "2026-08-31T05:00:00Z"),
    ]
    held = uncleared(spends, {"2026-08": 0.1, "2026-11": 0.25})
    assert [spend.month for spend in held] == ["2026-10", "2026-11"]
    assert uncleared([], {}) == []


def test_the_notice_is_read_in_pacific_months():
    # 03:00 UTC on 12-01 is still November in California, so November's notification
    # is this month's, and the backfill may go on. By 09:00 it is December's month.
    november = Spend("2026-11", 0.0, 1.0, "2026-12-01T02:00:00Z")
    still_november = datetime(2026, 12, 1, 3, 0, tzinfo=UTC)
    assert (
        refusal(NONE, [november], {}, still_november, backfill=True, reserve=NONE)
        is None
    )
    december = datetime(2026, 12, 1, 9, 0, tzinfo=UTC)
    assert (
        refusal(NONE, [november], {}, december, backfill=True, reserve=NONE).kind
        == "notice"
    )


def test_98_percent_stops_everything():
    for backfill in (False, True):
        for usage in (
            Usage(0.98 * FREE.vcpu_seconds, 0, 0),
            Usage(0, 0.98 * FREE.gib_seconds, 0),
            Usage(0, 0, int(0.98 * FREE.read_bytes) + 1),
        ):
            result = refusal(usage, [FRESH], {}, NOW, backfill=backfill, reserve=NONE)
            assert result.kind == "stop"
    assert (
        refusal(
            Usage(0.97 * FREE.vcpu_seconds, 0, 0),
            [FRESH],
            {},
            NOW,
            backfill=False,
            reserve=NONE,
        )
        is None
    )


def test_the_notices_are_quiet_when_none_of_any_month_is_recent():
    assert notices_quiet([], NOW) is True
    # 48 hours and a minute old.
    old = Spend("2026-11", 0.0, 1.0, "2026-11-09T06:59:00Z")
    assert notices_quiet([old], NOW) is True
    # A new billing month whose first notification has not come, while the last
    # month's still do: the feed works.
    last_month = Spend("2026-10", 0.0, 1.0, "2026-11-10T06:00:00Z")
    assert notices_quiet([old, last_month], NOW) is False
    assert notices_quiet([FRESH], NOW) is False


def test_backfill_needs_a_recent_notification():
    assert refusal(Usage(0, 0, 0), [], {}, NOW, backfill=False, reserve=NONE) is None
    result = refusal(Usage(0, 0, 0), [], {}, NOW, backfill=True, reserve=NONE)
    assert result.kind == "notice"
    assert "notification" in result.reason
    old = Spend("2026-11", 0.0, 1.0, "2026-11-09T06:59:00Z")  # 48 h 1 min old
    assert (
        refusal(Usage(0, 0, 0), [old], {}, NOW, backfill=True, reserve=NONE).kind
        == "notice"
    )
    assert refusal(Usage(0, 0, 0), [old], {}, NOW, backfill=False, reserve=NONE) is None
    last_month = Spend("2026-10", 0.0, 1.0, "2026-11-11T06:00:00Z")
    assert (
        refusal(Usage(0, 0, 0), [last_month], {}, NOW, backfill=True, reserve=NONE).kind
        == "notice"
    )
    recent = Spend("2026-11", 0.0, 1.0, "2026-11-09T07:01:00Z")
    assert (
        refusal(Usage(0, 0, 0), [recent], {}, NOW, backfill=True, reserve=NONE) is None
    )


def test_backfill_follows_the_budget_line():
    # A third of the way through the month the line is 0.8 * 240000 / 3 = 64000 vCPU-s.
    start, end = billing_month(NOW)
    third = start + (end - start) / 3
    fresh = Spend("2026-11", 0.0, 1.0, third.isoformat())
    assert (
        refusal(Usage(63_999, 0, 0), [fresh], {}, third, backfill=True, reserve=NONE)
        is None
    )
    result = refusal(
        Usage(64_000, 0, 0), [fresh], {}, third, backfill=True, reserve=NONE
    )
    assert result.kind == "line"
    assert (
        refusal(Usage(64_000, 0, 0), [fresh], {}, third, backfill=False, reserve=NONE)
        is None
    )
    gib_line = 0.8 * FREE.gib_seconds / 3
    assert (
        refusal(
            Usage(0, gib_line - 1, 0), [fresh], {}, third, backfill=True, reserve=NONE
        )
        is None
    )
    assert (
        refusal(
            Usage(0, gib_line + 1, 0), [fresh], {}, third, backfill=True, reserve=NONE
        ).kind
        == "line"
    )
    read_line = 0.8 * FREE.read_bytes / 3
    assert (
        refusal(
            Usage(0, 0, int(read_line) - 1),
            [fresh],
            {},
            third,
            backfill=True,
            reserve=NONE,
        )
        is None
    )
    assert (
        refusal(
            Usage(0, 0, int(read_line) + 1),
            [fresh],
            {},
            third,
            backfill=True,
            reserve=NONE,
        ).kind
        == "line"
    )


def test_the_budget_line_uses_the_real_length_of_the_month():
    # November 2026 has 30 days plus an hour; the share is of the real span.
    start, end = billing_month(NOW)
    half = start + (end - start) / 2
    fresh = Spend("2026-11", 0.0, 1.0, half.isoformat())
    line = 0.8 * FREE.vcpu_seconds * 0.5
    assert (
        refusal(Usage(line - 1, 0, 0), [fresh], {}, half, backfill=True, reserve=NONE)
        is None
    )
    assert (
        refusal(Usage(line, 0, 0), [fresh], {}, half, backfill=True, reserve=NONE).kind
        == "line"
    )


def test_a_running_execution_counts_only_its_part_in_the_month():
    start = datetime(2026, 11, 1, 7, 0, tzinfo=UTC)
    ex = execution("2026-11-01T06:30:00Z", "2026-11-01T07:30:00Z")
    assert execution_cost(ex, NOW, since=start) == (4 * 1800.0, 2 * 1800.0)
    assert execution_usage([ex], start, NOW) == Usage(4 * 1800.0, 2 * 1800.0, 0)
    assert execution_cost(ex, NOW) == (4 * 3600.0, 2 * 3600.0)


def test_an_execution_that_finished_before_the_month_costs_nothing():
    start = datetime(2026, 11, 1, 7, 0, tzinfo=UTC)
    ex = execution("2026-11-01T05:30:00Z", "2026-11-01T07:00:00Z")
    assert execution_usage([ex], start, NOW) == Usage(0, 0, 0)


def test_a_day_is_refused_when_its_worst_case_would_reach_the_stop_share():
    used = Usage(0.97 * FREE.vcpu_seconds, 0, 0)
    for backfill in (False, True):
        result = refusal(
            used, [FRESH], {}, NOW, backfill=backfill, reserve=Usage(7_200, 0, 0)
        )
        assert result.kind == "stop"
    fits = Usage(2_000, 0, 0)  # 232,800 + 2,000 stays under 235,200
    assert refusal(used, [FRESH], {}, NOW, backfill=False, reserve=fits) is None


def test_the_read_bytes_reserve_counts_too():
    used = Usage(0, 0, int(0.97 * FREE.read_bytes))
    big = Usage(0, 0, int(0.02 * FREE.read_bytes))
    assert refusal(used, [FRESH], {}, NOW, backfill=False, reserve=big).kind == "stop"
    small = Usage(0, 0, int(0.005 * FREE.read_bytes))
    assert refusal(used, [FRESH], {}, NOW, backfill=False, reserve=small) is None


def test_the_budget_line_ignores_the_reserve():
    start, end = billing_month(NOW)
    third = start + (end - start) / 3
    fresh = Spend("2026-11", 0.0, 1.0, third.isoformat())
    below = Usage(60_000, 0, 0)
    assert (
        refusal(below, [fresh], {}, third, backfill=True, reserve=Usage(7_200, 0, 0))
        is None
    )
