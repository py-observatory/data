"""Where the stream's databases go, and what a run seals when a day or a month ends.

`pypi-registry-current` holds the state and the open delta, the delta of the day of
the last run, which each run of that day adds to. Each month has a create-once release,
`pypi-registry-month-YYYY-MM`, which holds a copy of the state as the month began, the
sealed delta of each day of the month that had a run, and, once the month is over, the
month's part (its deltas merged into one database) and the month's manifest. The names
here are of the databases. A release asset adds `.zst` to the name, except for the
month's manifest, which is JSON.
"""

from dataclasses import dataclass
from datetime import date

STATE_DB = "pypi-registry-state.db"
OPEN_DELTA_DB = "pypi-registry-delta-open.db"


def delta_name(day: date) -> str:
    """The sealed delta of `day`."""
    return f"pypi-registry-delta-{day:%Y-%m-%d}.db"


def base_state_name(year: int, month: int) -> str:
    """The copy of the state as the month began, which its deltas are replayed onto."""
    return f"pypi-registry-state-base-{year:04d}-{month:02d}.db"


def month_part_name(year: int, month: int) -> str:
    """The month's deltas, merged into one database once the month is over."""
    return f"pypi-registry-{year:04d}-{month:02d}.db"


def month_manifest_name(year: int, month: int) -> str:
    """The manifest that lists the month's assets once the month is over."""
    return f"manifest-{year:04d}-{month:02d}.json"


@dataclass(frozen=True)
class Rollover:
    """What a run does before it reads anything, because a day or a month has ended
    since the open delta was started.

    `seal_day` is the day whose open delta is sealed, and a new open delta is started.
    `close_month` is the (year, month) whose deltas are merged into its part, and
    `new_base` is the (year, month) whose base copy of the state is made. Each is None
    when there is nothing of that kind to do.
    """

    seal_day: date | None
    close_month: tuple[int, int] | None
    new_base: tuple[int, int] | None


def plan_rollover(open_day: date | None, today: date) -> Rollover:
    """What a run on `today` (a UTC day) does to an open delta begun on `open_day`.

    `open_day` is None for the first run, which builds the state: it starts today's
    month with a base copy. A run on the open delta's day does nothing. A run on a
    later day seals the open delta's day. If that day is in another month, the run
    also closes that month and starts today's month with a base copy. Days without a
    run have no delta, so only the open delta's day is sealed, and after a gap only
    its month is closed.

    Raises ValueError when `today` is before `open_day`: the clock went back, and the
    rows of the open delta would be filed under the wrong day.
    """
    if open_day is None:
        return Rollover(None, None, (today.year, today.month))
    if today < open_day:
        raise ValueError(
            f"the open delta is from {open_day}, which is after today, {today}: the "
            "clock went back"
        )
    if today == open_day:
        return Rollover(None, None, None)
    if (today.year, today.month) == (open_day.year, open_day.month):
        return Rollover(open_day, None, None)
    return Rollover(
        open_day, (open_day.year, open_day.month), (today.year, today.month)
    )
