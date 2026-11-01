from dataclasses import FrozenInstanceError, fields, replace
from hashlib import sha256

import pytest

from obskit.gates import GateFailure, check
from obskit.manifest import Asset, Manifest, TableInfo, Watermark

# The shapes of a pypi-registry manifest: the state database and the open delta in
# -current, and the closed assets that earlier runs left in the month releases.
CURRENT = "pypi-registry-current"
MONTH = "pypi-registry-month-2026-11"
STATE = "pypi-registry-state.db.zst"
OPEN_DELTA = "pypi-registry-delta-open.db.zst"

# A serial is digits only. int() alone would read an underscore, a sign and the digits
# of other scripts as numbers, and raise for any other text.
NOT_DIGITS = [
    ("100", "abc"),
    ("abc", "100"),
    ("100", ""),
    ("1_0", "100"),
    ("100", "+5"),
    ("٥", "6"),  # Arabic-Indic five
]

# The watermark that both manifests hold unless a test moves one of them.
UNMOVED = Watermark("1000", "serial")


def digest(label: str) -> str:
    return sha256(label.encode()).hexdigest()


def asset(
    name: str,
    *,
    tag: str = CURRENT,
    period: str | None = None,
    closed: bool = False,
    tables: dict[str, TableInfo] | None = None,
) -> Asset:
    """An asset whose digests come from its name, so equal arguments give equal
    assets. `rewritten` gives one that holds other bytes."""
    return Asset(
        name=name,
        tag=tag,
        role="state",
        format="sqlite+zstd",
        asset_bytes=1_048_576,
        asset_sha256=digest(name),
        db_bytes=8_388_608,
        db_sha256=digest(f"{name} unpacked"),
        tables=tables or {},
        period=period,
        closed=closed,
    )


def rewritten(old: Asset, **changes) -> Asset:
    """`old` with other bytes, and `changes` applied."""
    return replace(
        old,
        asset_sha256=digest(f"{old.name} next"),
        db_sha256=digest(f"{old.name} next unpacked"),
        **changes,
    )


SEALED = asset(
    "pypi-registry-delta-2026-11-01.db.zst", tag=MONTH, period="2026-11-01", closed=True
)
BASE = asset(
    "pypi-registry-base-2026-11.db.zst", tag=MONTH, period="2026-11", closed=True
)
STATE_DB = asset(
    STATE,
    tables={
        "meta": TableInfo(6, "snapshot"),
        "files_current": TableInfo(1000, "snapshot"),
        "files": TableInfo(1200, "append"),
        "events": TableInfo(40, "events"),
        "rollup": TableInfo(7, "derived"),
        "swept": TableInfo(0, "append"),
    },
)
DELTA_NOV_2 = asset(
    OPEN_DELTA, period="2026-11-02", tables={"files": TableInfo(120, "append")}
)


def manifest(
    assets: list[Asset],
    *,
    cursor: str | None = "1000",
    data_through: Watermark = UNMOVED,
) -> Manifest:
    """A manifest whose cursor and data_through do not move unless a test moves them."""
    return Manifest(
        stream="pypi-registry",
        producer={
            "repo": "py-observatory/data",
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "run_id": "1234567890",
            "obskit": "0.1.0",
            "python": "3.14.0",
        },
        generated_at="2026-11-03T01:02:03Z",
        complete=True,
        coverage={},
        data_through=data_through,
        cursor=cursor,
        licences=[],
        assets=assets,
    )


def holding(history_class: str, rows: int, table: str = "files") -> Manifest:
    """A manifest whose state asset holds just the one table."""
    return manifest([asset(STATE, tables={table: TableInfo(rows, history_class)})])


def rules(failures: list[GateFailure]) -> list[str]:
    return [failure.rule for failure in failures]


def test_bootstrap_has_no_failures():
    new = holding("append", 10)

    assert check(None, new) == []
    assert check(None, new, allowed_shrink={"files": 3}) == []


# The cursor


@pytest.mark.parametrize(("was", "now"), [("200", "100"), ("10", "9"), ("1000", "0")])
def test_cursor_backwards_fails(was, now):
    failures = check(manifest([], cursor=was), manifest([], cursor=now))

    assert rules(failures) == ["cursor"]
    assert f"from {was} to {now}" in failures[0].detail


@pytest.mark.parametrize(
    ("was", "now"),
    [
        ("100", "100"),
        ("100", "101"),
        ("9", "10"),  # more as a number, less as text
        (None, "5"),
        (None, None),
    ],
)
def test_cursor_not_backwards_passes(was, now):
    assert check(manifest([], cursor=was), manifest([], cursor=now)) == []


@pytest.mark.parametrize("was", ["1000", "5", "abc"])
def test_cursor_lost_fails(was):
    failures = check(manifest([], cursor=was), manifest([], cursor=None))

    assert rules(failures) == ["cursor"]
    assert "missing" in failures[0].detail
    assert was in failures[0].detail


@pytest.mark.parametrize(("was", "now"), NOT_DIGITS)
def test_cursor_that_is_not_digits_fails_instead_of_raising(was, now):
    failures = check(manifest([], cursor=was), manifest([], cursor=now))

    assert rules(failures) == ["cursor"]
    assert "digits only" in failures[0].detail


# data_through


@pytest.mark.parametrize(
    ("granularity", "was", "now"),
    [
        ("serial", "200", "100"),
        ("day", "2026-11-03", "2026-11-02"),
        ("month", "2026-11", "2026-10"),
        ("month", "2027-01", "2026-12"),
        ("instant", "2026-11-03T01:02:03Z", "2026-11-03T01:02:02Z"),
    ],
)
def test_data_through_backwards_fails(granularity, was, now):
    prior = manifest([], data_through=Watermark(was, granularity))
    new = manifest([], data_through=Watermark(now, granularity))

    failures = check(prior, new)

    assert rules(failures) == ["data_through"]
    assert f"from {was} to {now}" in failures[0].detail


@pytest.mark.parametrize(
    ("granularity", "was", "now"),
    [
        ("serial", "100", "100"),
        ("serial", "100", "101"),
        ("day", "2026-11-03", "2026-11-03"),
        ("day", "2026-11-03", "2026-11-04"),
        ("day", "2026-12-31", "2027-01-01"),
        ("month", "2026-11", "2026-11"),
        ("month", "2026-12", "2027-01"),
        ("instant", "2026-11-03T01:02:03Z", "2026-11-03T01:02:03Z"),
        ("instant", "2026-11-03T01:02:03Z", "2026-11-03T01:02:04Z"),
    ],
)
def test_data_through_not_backwards_passes(granularity, was, now):
    prior = manifest([], data_through=Watermark(was, granularity))
    new = manifest([], data_through=Watermark(now, granularity))

    assert check(prior, new) == []


def test_serial_watermark_compared_numerically():
    nine = manifest([], data_through=Watermark("9", "serial"))
    ten = manifest([], data_through=Watermark("10", "serial"))

    assert check(nine, ten) == []
    assert rules(check(ten, nine)) == ["data_through"]


@pytest.mark.parametrize(
    ("was", "now"),
    [
        (Watermark("2026-11-03", "day"), Watermark("2026-10", "month")),
        (Watermark("2026-10", "month"), Watermark("2026-11-03", "day")),
        # As text, "123456" sorts before "2026-11-03", so one direction would fail.
        (Watermark("2026-11-03", "day"), Watermark("123456", "serial")),
        (Watermark("123456", "serial"), Watermark("2026-11-03", "day")),
        (Watermark("2026-11", "month"), Watermark("2026-11-03T01:02:03Z", "instant")),
    ],
)
def test_data_through_of_different_granularities_is_not_compared(was, now):
    prior = manifest([], data_through=was)
    new = manifest([], data_through=now)

    assert check(prior, new) == []


@pytest.mark.parametrize(("was", "now"), NOT_DIGITS)
def test_serial_watermark_that_is_not_digits_fails_instead_of_raising(was, now):
    prior = manifest([], data_through=Watermark(was, "serial"))
    new = manifest([], data_through=Watermark(now, "serial"))

    failures = check(prior, new)

    assert rules(failures) == ["data_through"]
    assert "digits only" in failures[0].detail


# Closed assets


def test_closed_asset_changed_fails():
    changed = rewritten(SEALED)

    failures = check(manifest([SEALED]), manifest([changed]))

    assert rules(failures) == ["closed-kept"]
    assert SEALED.name in failures[0].detail
    assert SEALED.asset_sha256 in failures[0].detail
    assert changed.asset_sha256 in failures[0].detail


@pytest.mark.parametrize(
    "remaining",
    [
        [],
        [replace(SEALED, tag="pypi-registry-month-2026-12")],
        [replace(SEALED, name="pypi-registry-delta-2026-11-01b.db.zst")],
    ],
    ids=["gone", "under another tag", "under another name"],
)
def test_closed_asset_missing_fails(remaining):
    failures = check(manifest([SEALED]), manifest(remaining))

    assert rules(failures) == ["closed-kept"]
    assert SEALED.name in failures[0].detail
    assert SEALED.tag in failures[0].detail


def test_closed_asset_listed_as_open_fails():
    failures = check(manifest([SEALED]), manifest([replace(SEALED, closed=False)]))

    assert rules(failures) == ["closed-kept"]
    assert SEALED.name in failures[0].detail
    assert "no longer closed" in failures[0].detail


def test_closed_asset_that_changed_and_is_listed_as_open_is_one_failure():
    changed = rewritten(SEALED, closed=False)

    failures = check(manifest([SEALED]), manifest([changed]))

    assert rules(failures) == ["closed-kept"]
    assert changed.asset_sha256 in failures[0].detail
    assert "no longer closed" in failures[0].detail


def test_closed_assets_kept_and_new_ones_added_pass():
    added = asset(
        "pypi-registry-delta-2026-11-02.db.zst",
        tag=MONTH,
        period="2026-11-02",
        closed=True,
    )

    assert check(manifest([BASE, SEALED]), manifest([added, SEALED, BASE])) == []


# Rows


@pytest.mark.parametrize("history_class", ["append", "events"])
def test_append_table_shrink_fails(history_class):
    failures = check(holding(history_class, 100), holding(history_class, 99))

    assert rules(failures) == ["rows"]
    detail = failures[0].detail
    assert "files" in detail
    assert STATE in detail
    assert "from 100 rows to 99" in detail


@pytest.mark.parametrize("history_class", ["append", "events", "snapshot", "derived"])
@pytest.mark.parametrize("now", [100, 101, 5000])
def test_table_that_holds_or_grows_passes(history_class, now):
    assert check(holding(history_class, 100), holding(history_class, now)) == []


def test_snapshot_shrink_within_allowance_passes():
    prior = holding("snapshot", 1000, table="files_current")
    allowed = {"files_current": 5}

    five = holding("snapshot", 995, table="files_current")
    assert check(prior, five, allowed_shrink=allowed) == []

    six = holding("snapshot", 994, table="files_current")
    failures = check(prior, six, allowed_shrink=allowed)
    assert rules(failures) == ["rows"]
    assert "files_current" in failures[0].detail
    assert "from 1,000 rows to 994" in failures[0].detail
    assert "at most 5 may go" in failures[0].detail


@pytest.mark.parametrize(
    "allowed", [None, {}, {"meta": 50}], ids=["none", "empty", "another table"]
)
def test_snapshot_shrink_without_allowance_fails(allowed):
    failures = check(
        holding("snapshot", 1000, table="files_current"),
        holding("snapshot", 999, table="files_current"),
        allowed_shrink=allowed,
    )

    assert rules(failures) == ["rows"]


@pytest.mark.parametrize("history_class", ["append", "events"])
def test_allowance_never_covers_an_append_table(history_class):
    failures = check(
        holding(history_class, 100),
        holding(history_class, 90),
        allowed_shrink={"files": 10},
    )

    assert rules(failures) == ["rows"]


def test_derived_table_is_not_checked():
    assert check(holding("derived", 100), holding("derived", 0)) == []


def test_unknown_history_class_is_held_to_the_strictest_rule():
    # `loads` refuses such a class, so only a manifest built by hand has one.
    failures = check(
        holding("mystery", 100),
        holding("mystery", 99),
        allowed_shrink={"files": 5},
    )

    assert rules(failures) == ["rows"]


def test_table_is_judged_by_its_class_in_the_prior_manifest():
    # Calling a table derived does not excuse the rows it lost.
    assert rules(check(holding("append", 100), holding("derived", 90))) == ["rows"]


def test_table_gone_from_an_asset_counts_as_all_its_rows_lost():
    prior = manifest(
        [
            asset(
                STATE,
                tables={
                    "files": TableInfo(100, "append"),
                    "meta": TableInfo(1, "snapshot"),
                    "rollup": TableInfo(7, "derived"),
                    "swept": TableInfo(0, "append"),
                },
            )
        ]
    )
    new = manifest([asset(STATE)])

    failures = check(prior, new)

    # The derived table is not checked and the empty one had nothing to lose.
    assert rules(failures) == ["rows", "rows"]
    assert "files" in failures[0].detail
    assert "gone" in failures[0].detail
    assert "meta" in failures[1].detail
    assert "gone" in failures[1].detail


def test_table_gone_from_an_asset_may_use_the_allowance():
    prior = holding("snapshot", 3, table="files_current")
    new = manifest([asset(STATE)])

    assert check(prior, new, allowed_shrink={"files_current": 3}) == []
    assert rules(check(prior, new, allowed_shrink={"files_current": 2})) == ["rows"]


def test_table_new_to_an_asset_is_not_compared():
    new = manifest(
        [
            asset(
                STATE,
                tables={
                    "files": TableInfo(100, "append"),
                    "serials": TableInfo(1, "append"),
                },
            )
        ]
    )

    assert check(holding("append", 100), new) == []


def test_open_delta_new_day_not_compared():
    yesterday = asset(
        OPEN_DELTA,
        period="2026-11-02",
        tables={"events": TableInfo(500, "events"), "files": TableInfo(900, "append")},
    )
    today = asset(
        OPEN_DELTA,
        period="2026-11-03",
        tables={"events": TableInfo(3, "events"), "files": TableInfo(5, "append")},
    )

    assert check(manifest([yesterday]), manifest([today])) == []


def test_open_delta_of_the_same_day_is_compared():
    before = asset(
        OPEN_DELTA, period="2026-11-02", tables={"events": TableInfo(500, "events")}
    )
    after = asset(
        OPEN_DELTA, period="2026-11-02", tables={"events": TableInfo(499, "events")}
    )

    failures = check(manifest([before]), manifest([after]))

    assert rules(failures) == ["rows"]
    assert "2026-11-02" in failures[0].detail
    assert "from 500 rows to 499" in failures[0].detail


@pytest.mark.parametrize("now", [120, 121, 5000])
def test_open_delta_of_the_same_day_that_holds_or_grows_passes(now):
    after = replace(DELTA_NOV_2, tables={"files": TableInfo(now, "append")})

    assert check(manifest([DELTA_NOV_2]), manifest([after])) == []


def test_asset_new_to_the_manifest_is_not_compared():
    assert check(manifest([]), manifest([DELTA_NOV_2])) == []


def test_changed_closed_asset_is_not_also_reported_as_lost_rows():
    sealed = replace(SEALED, tables={"events": TableInfo(40, "events")})
    changed = rewritten(sealed, tables={"events": TableInfo(10, "events")})

    assert rules(check(manifest([sealed]), manifest([changed]))) == ["closed-kept"]


def test_open_asset_that_comes_back_closed_is_still_compared():
    open_delta = asset(
        OPEN_DELTA, period="2026-11-02", tables={"files": TableInfo(100, "append")}
    )
    sealed = asset(
        OPEN_DELTA,
        period="2026-11-02",
        closed=True,
        tables={"files": TableInfo(90, "append")},
    )

    assert rules(check(manifest([open_delta]), manifest([sealed]))) == ["rows"]


# An open asset that is not there

# An asset without a period, like the state database, has to come back without one.
# One that does not has lost every table.


@pytest.mark.parametrize(
    "remaining",
    [
        [],
        [replace(STATE_DB, name="pypi-registry-state-v2.db.zst")],
        [replace(STATE_DB, period="2026-11")],
        [replace(STATE_DB, tag=MONTH)],
    ],
    ids=["dropped", "renamed", "given a period", "under another tag"],
)
def test_state_asset_that_is_not_there_has_lost_every_table(remaining):
    failures = check(manifest([STATE_DB]), manifest(remaining))

    # meta, files_current, files and events. The derived table is not checked, and the
    # empty one had nothing to lose.
    assert rules(failures) == ["rows"] * 4
    for failure in failures:
        assert STATE in failure.detail
        assert "missing" in failure.detail
    assert not any("rollup" in failure.detail for failure in failures)
    assert not any("swept" in failure.detail for failure in failures)


def test_state_asset_given_a_period_says_which():
    given = replace(STATE_DB, period="2026-11")

    failures = check(manifest([STATE_DB]), manifest([given]))

    assert "the period 2026-11" in failures[0].detail


@pytest.mark.parametrize(
    "tables",
    [
        {"rollup": TableInfo(7, "derived")},
        {"swept": TableInfo(0, "append")},
        {},
    ],
    ids=["only derived tables", "only empty tables", "no tables"],
)
def test_state_asset_dropped_with_nothing_to_lose_passes(tables):
    prior = manifest([asset(STATE, tables=tables)])

    assert check(prior, manifest([])) == []


def test_dropped_asset_may_use_the_allowance_for_a_snapshot_table():
    prior = holding("snapshot", 3, table="files_current")

    assert check(prior, manifest([]), allowed_shrink={"files_current": 3}) == []
    lacking = check(prior, manifest([]), allowed_shrink={"files_current": 2})
    assert rules(lacking) == ["rows"]


# An asset with a period, like the open delta, has to come back with that period or a
# later one, whatever its tables hold.


@pytest.mark.parametrize(
    ("remaining", "said"),
    [
        ([], "missing"),
        ([replace(DELTA_NOV_2, tag=MONTH)], "missing"),
        ([replace(DELTA_NOV_2, name="pypi-registry-delta-open-v2.db.zst")], "missing"),
        ([replace(DELTA_NOV_2, period="2026-11-01")], "went back"),
        ([replace(DELTA_NOV_2, period=None)], "no period"),
    ],
    ids=["dropped", "under another tag", "renamed", "earlier period", "no period"],
)
def test_open_delta_that_is_missing_or_goes_back_fails(remaining, said):
    failures = check(manifest([DELTA_NOV_2]), manifest(remaining))

    assert rules(failures) == ["rows"]
    assert OPEN_DELTA in failures[0].detail
    assert said in failures[0].detail


def test_open_delta_that_is_missing_or_goes_back_fails_whatever_it_holds():
    bare = replace(DELTA_NOV_2, tables={"rollup": TableInfo(7, "derived")})

    assert rules(check(manifest([bare]), manifest([]))) == ["rows"]
    earlier = replace(bare, period="2026-11-01")
    assert rules(check(manifest([bare]), manifest([earlier]))) == ["rows"]


@pytest.mark.parametrize(
    ("was", "now"),
    [
        ("2026-11-02", "2026-11-05"),  # days with no run
        ("2026-11-30", "2026-12-01"),
        ("2026-12-31", "2027-01-01"),
    ],
)
def test_open_delta_rolled_to_a_later_day_is_not_compared(was, now):
    before = replace(DELTA_NOV_2, period=was)
    after = replace(before, period=now, tables={"files": TableInfo(1, "append")})

    assert check(manifest([before]), manifest([after])) == []


# Together


def test_every_failure_is_reported():
    prior = manifest(
        [SEALED, asset(STATE, tables={"files": TableInfo(100, "append")})],
        cursor="200",
        data_through=Watermark("200", "serial"),
    )
    new = manifest(
        [asset(STATE, tables={"files": TableInfo(99, "append")})],
        cursor="100",
        data_through=Watermark("100", "serial"),
    )

    failures = check(prior, new)

    assert rules(failures) == ["cursor", "data_through", "closed-kept", "rows"]


def test_a_normal_registry_run_passes():
    """The next run of the registry: the cursor and data_through move on, 5 files leave
    files_current, the append tables grow, the open delta moves to a new day, and the
    sealed delta of the old day joins the closed assets. Every open asset is rewritten,
    so it has other bytes than before, as it does on every run."""
    state = asset(
        STATE,
        tables={
            "meta": TableInfo(6, "snapshot"),
            "files_current": TableInfo(1000, "snapshot"),
            "files": TableInfo(1200, "append"),
            "events": TableInfo(40, "events"),
        },
    )
    delta = asset(
        OPEN_DELTA, period="2026-11-02", tables={"files": TableInfo(120, "append")}
    )
    prior = manifest(
        [state, delta, BASE, SEALED],
        cursor="123456",
        data_through=Watermark("123456", "serial"),
    )
    next_state = rewritten(
        state,
        tables={
            "meta": TableInfo(6, "snapshot"),
            "files_current": TableInfo(995, "snapshot"),
            "files": TableInfo(1210, "append"),
            "events": TableInfo(45, "events"),
        },
    )
    next_delta = rewritten(
        delta, period="2026-11-03", tables={"files": TableInfo(4, "append")}
    )
    sealed_delta = asset(
        "pypi-registry-delta-2026-11-02.db.zst",
        tag=MONTH,
        period="2026-11-02",
        closed=True,
        tables={"files": TableInfo(120, "append")},
    )
    new = manifest(
        [next_state, next_delta, BASE, SEALED, sealed_delta],
        cursor="123470",
        data_through=Watermark("123470", "serial"),
    )

    assert check(prior, new, allowed_shrink={"files_current": 5}) == []


def test_gate_failure_is_a_pair_of_rule_and_detail():
    failure = GateFailure("cursor", "the cursor went back")

    assert [f.name for f in fields(failure)] == ["rule", "detail"]
    assert (failure.rule, failure.detail) == ("cursor", "the cursor went back")


@pytest.mark.parametrize("name", ["rule", "detail"])
def test_gate_failure_is_frozen(name):
    with pytest.raises(FrozenInstanceError):
        setattr(GateFailure("cursor", "the cursor went back"), name, "changed")
