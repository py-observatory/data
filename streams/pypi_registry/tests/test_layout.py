from datetime import date

import pytest

from pypi_registry.layout import (
    OPEN_DELTA_DB,
    STATE_DB,
    Rollover,
    base_state_name,
    delta_name,
    month_manifest_name,
    month_part_name,
    plan_rollover,
)


def test_layout_names_and_rollover():
    assert STATE_DB == "pypi-registry-state.db"
    assert OPEN_DELTA_DB == "pypi-registry-delta-open.db"
    assert delta_name(date(2026, 11, 2)) == "pypi-registry-delta-2026-11-02.db"
    assert base_state_name(2026, 12) == "pypi-registry-state-base-2026-12.db"
    assert month_part_name(2026, 1) == "pypi-registry-2026-01.db"
    assert month_manifest_name(2026, 11) == "manifest-2026-11.json"

    # The first run starts the month's base and seals nothing.
    assert plan_rollover(None, date(2026, 11, 28)) == Rollover(None, None, (2026, 11))
    # Another run on the open delta's day changes nothing.
    assert plan_rollover(date(2026, 11, 2), date(2026, 11, 2)) == Rollover(
        None, None, None
    )
    # A new day seals the open delta's day, in the same month.
    assert plan_rollover(date(2026, 11, 2), date(2026, 11, 3)) == Rollover(
        date(2026, 11, 2), None, None
    )
    # A day with no run in between is not sealed: there is nothing to seal for it.
    assert plan_rollover(date(2026, 11, 2), date(2026, 11, 5)) == Rollover(
        date(2026, 11, 2), None, None
    )
    # A new month also closes the open delta's month and starts the new one's base.
    assert plan_rollover(date(2026, 11, 30), date(2026, 12, 1)) == Rollover(
        date(2026, 11, 30), (2026, 11), (2026, 12)
    )
    assert plan_rollover(date(2026, 12, 31), date(2027, 1, 1)) == Rollover(
        date(2026, 12, 31), (2026, 12), (2027, 1)
    )
    # After a gap, the month of the open delta closes, and the base is today's month.
    assert plan_rollover(date(2026, 10, 15), date(2026, 12, 2)) == Rollover(
        date(2026, 10, 15), (2026, 10), (2026, 12)
    )
    # The same day of another month is a new month, not the same day.
    assert plan_rollover(date(2026, 11, 3), date(2026, 12, 3)) == Rollover(
        date(2026, 11, 3), (2026, 11), (2026, 12)
    )


@pytest.mark.parametrize(
    ("open_day", "today"),
    [(date(2026, 11, 3), date(2026, 11, 2)), (date(2027, 1, 1), date(2026, 12, 31))],
    ids=["a day back", "a year back"],
)
def test_rollover_refuses_a_clock_that_went_back(open_day, today):
    # The open delta is from a later day than the run's clock gives. Sealing it under
    # today's name, or carrying it on, would file the rows under the wrong day.
    with pytest.raises(ValueError, match=str(open_day)):
        plan_rollover(open_day, today)
