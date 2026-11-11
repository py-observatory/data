import pytest

from obskit.manifest import Asset, TableInfo
from obskit.parts import coarsest_parts

HELD = frozenset({"counts"})


def part(
    period: str | None,
    *,
    closed: bool = False,
    tables: tuple[str, ...] = ("counts",),
    name: str | None = None,
    format: str = "sqlite+zstd",
) -> Asset:
    """An asset of a stream's release, with a name that comes from its period."""
    name = name or f"counts-{period}.db.zst"
    return Asset(
        name=name,
        tag="stream-current",
        role="state",
        format=format,
        asset_bytes=1_000,
        asset_sha256=f"sha-{name}",
        db_bytes=8_000,
        db_sha256=f"db-{name}",
        tables={t: TableInfo(rows=1, history_class="append") for t in tables},
        period=period,
        closed=closed,
    )


def test_a_closed_month_part_replaces_its_days():
    days = [part("2026-11-01", closed=True), part("2026-11-02", closed=True)]
    month = part("2026-11", closed=True)
    open_day = part("2026-12-01")

    assert coarsest_parts([*days, month, open_day], HELD) == [month, open_day]


def test_a_month_part_that_is_not_closed_replaces_nothing():
    day = part("2026-11-01", closed=True)
    month = part("2026-11")

    assert coarsest_parts([day, month], HELD) == [month, day]


def test_a_closed_year_part_replaces_its_months():
    months = [part("2026-01", closed=True), part("2026-02", closed=True)]
    year = part("2026", closed=True)
    later = part("2027-01", closed=True)

    assert coarsest_parts([*months, year, later], HELD) == [year, later]


def test_a_day_goes_with_its_month_when_the_year_is_closed_too():
    day = part("2026-03-05", closed=True)
    month = part("2026-03", closed=True)
    year = part("2026", closed=True)

    assert coarsest_parts([day, month, year], HELD) == [year]


def test_assets_without_the_tables_are_left_out():
    held = part("2026-11-01")
    other = part("2026-11-02", tables=("projects",))
    json_asset = part("2026-11-03", format="json")

    assert coarsest_parts([other, held, json_asset], HELD) == [held]


def test_parts_come_in_period_order():
    year = part("2025", closed=True)
    month = part("2026-11", name="b.db.zst")
    same_month = part("2026-11", name="a.db.zst")
    day = part("2026-11-01")

    ordered = coarsest_parts([day, month, year, same_month], HELD)

    assert ordered == [year, same_month, month, day]


def test_a_holding_asset_without_a_period_is_refused():
    with pytest.raises(ValueError, match="no period"):
        coarsest_parts([part("2026-11-01"), part(None, name="loose.db.zst")], HELD)


def test_an_asset_without_a_period_that_holds_nothing_wanted_is_ignored():
    loose = part(None, name="state.db.zst", tables=("projects",))

    assert coarsest_parts([loose], HELD) == []
