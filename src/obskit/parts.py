"""Which published parts of a stream hold a table, decided from the manifest alone.

A stream keeps what it publishes at three grains: a sealed delta for each day, a month's
part once the month is over, and, later, a year's part. A closed part is final, and it
holds what the parts of its shorter periods held, so a reader that wants every row once
takes the coarsest closed part over each stretch of time. A part that is not closed can
still change, so it covers nothing.

The catalog reads a stream's parts by this rule, and a stream that rebuilds a table from
its own parts needs the same rule, so that the two never disagree about which part
stands for which days.
"""

from collections.abc import Iterable

from obskit.manifest import Asset

SQLITE = "sqlite+zstd"
# How long each shape of a period is, as obskit.cycle tells a day from the others.
YEAR = len("YYYY")
MONTH = len("YYYY-MM")
DAY = len("YYYY-MM-DD")


class PeriodlessPartError(ValueError):
    """An asset holds a wanted table but has no period, so it can be neither put in
    order nor matched with the part that replaces it. `asset` is that asset."""

    def __init__(self, asset: Asset):
        super().__init__(
            f"{asset.tag}/{asset.name} holds a table that is wanted but has no period, "
            "so where it falls in the stream's history is not known"
        )
        self.asset = asset


def period_of(asset: Asset) -> str:
    """The period of `asset`, and an empty string for an asset with none, which is no
    year, month or day."""
    return asset.period or ""


def order_key(asset: Asset) -> tuple[str, str]:
    """How assets are put in order: by period, and by name for equal periods. As the
    shorter text comes first, a year comes before its months, and a month before its
    days."""
    return period_of(asset), asset.name


def last_month(period: str) -> str:
    """The month, as YYYY-MM, in which `period` ends. A year ends in December."""
    return f"{period}-12" if len(period) == YEAR else period[:MONTH]


def coarsest_parts(assets: Iterable[Asset], tables: frozenset[str]) -> list[Asset]:
    """The sqlite assets among `assets` that hold any of `tables`, without the days that
    a closed month part covers and the months that a closed year part covers, so that
    each row is read from one part, in the order of their periods and then of their
    names.

    What a closed part covers is told from the closed parts that hold `tables` as they
    are given, and not from those that are left, so a day goes with its month, and the
    month with its year.

    Raises PeriodlessPartError, a ValueError, for a holding asset with no period.
    """
    parts = [
        a for a in assets if a.format == SQLITE and not tables.isdisjoint(a.tables)
    ]
    for part in parts:
        if part.period is None:
            raise PeriodlessPartError(part)
    closed = {period_of(part) for part in parts if part.closed}

    def covered(period: str) -> bool:
        if len(period) == DAY:
            return period[:MONTH] in closed
        if len(period) == MONTH:
            return period[:YEAR] in closed
        return False

    return sorted(
        (part for part in parts if not covered(period_of(part))), key=order_key
    )
