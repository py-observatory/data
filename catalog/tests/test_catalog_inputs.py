import re
from dataclasses import dataclass, replace
from datetime import date
from hashlib import sha256

import pytest

from catalog.inputs import (
    InputError,
    asset_url,
    fingerprint,
    needed_assets,
    open_day,
    pin_assets,
    sealed_through,
)
from obskit.manifest import Asset, Manifest, TableInfo, Watermark
from obskit.tags import current_tag

REPO = "py-observatory/data"

# What catalog.toml admits of each stream.
REGISTRY_TABLES = frozenset(
    {"projects", "versions_seen", "files_current", "files", "release_records", "events"}
)
OSV_TABLES = frozenset({"advisories"})
DOWNLOADS_TABLES = frozenset(
    {"projects", "days", "breaks"}
    | {"folded", "months", "monthly", "top_set", "daily", "recent"}
)


def digest(label: str) -> str:
    return sha256(label.encode()).hexdigest()


def tables_of(names: tuple[str, ...], rows: int) -> dict[str, TableInfo]:
    return {name: TableInfo(rows, "append") for name in names}


@dataclass(frozen=True)
class Stream:
    """A stream's id and the tables of its state and of its delta. It makes the entries
    that the stream's manifest lists, named and placed as obskit.cycle.Store names and
    places them.

    The state has more rows than the base copy of the month, as it does once the month
    is a day old, so that no test can take the one for the other by its tables and rows.
    """

    id: str
    state_tables: tuple[str, ...]
    delta_tables: tuple[str, ...]

    def _month_tag(self, period: str) -> str:
        return f"{self.id}-month-{period[:7]}"

    def _entry(
        self,
        name: str,
        tag: str,
        *,
        role: str,
        tables: dict[str, TableInfo],
        period: str | None,
        closed: bool,
    ) -> Asset:
        """An entry whose digests come from its tag and name, so that no two entries of
        a manifest share bytes."""
        return Asset(
            name=name,
            tag=tag,
            role=role,
            format="sqlite+zstd",
            asset_bytes=1_000,
            asset_sha256=digest(f"{tag}/{name}"),
            db_bytes=8_000,
            db_sha256=digest(f"{tag}/{name} unpacked"),
            tables=tables,
            period=period,
            closed=closed,
        )

    def state(self) -> Asset:
        return self._entry(
            f"{self.id}-state.db.zst",
            current_tag(self.id),
            role="state",
            tables=tables_of(self.state_tables, 1_000),
            period=None,
            closed=False,
        )

    def open_delta(self, day: str) -> Asset:
        return self._entry(
            f"{self.id}-delta-open.db.zst",
            current_tag(self.id),
            role="state",
            tables=tables_of(self.delta_tables, 20),
            period=day,
            closed=False,
        )

    def base(self, month: str) -> Asset:
        """The state as `month` began."""
        return self._entry(
            f"{self.id}-state-base-{month}.db.zst",
            self._month_tag(month),
            role="public",
            tables=tables_of(self.state_tables, 900),
            period=month,
            closed=True,
        )

    def sealed(self, day: str) -> Asset:
        return self._entry(
            f"{self.id}-delta-{day}.db.zst",
            self._month_tag(day),
            role="public",
            tables=tables_of(self.delta_tables, 50),
            period=day,
            closed=True,
        )

    def part(self, month: str) -> Asset:
        """The deltas of `month`, merged into one database once the month is over."""
        return self._entry(
            f"{self.id}-{month}.db.zst",
            self._month_tag(month),
            role="public",
            tables=tables_of(self.delta_tables, 1_500),
            period=month,
            closed=True,
        )

    def year_part(self, year: str) -> Asset:
        """The month parts of `year`, merged into one database. No stream makes one yet,
        and the kit names none, so this one is named as a month part is."""
        return self._entry(
            f"{self.id}-{year}.db.zst",
            f"{self.id}-year-{year}",
            role="public",
            tables=tables_of(self.delta_tables, 18_000),
            period=year,
            closed=True,
        )

    def month_manifest(self, month: str) -> Asset:
        name = f"manifest-{month}.json"
        tag = self._month_tag(month)
        return Asset(
            name=name,
            tag=tag,
            role="public",
            format="json",
            asset_bytes=4_000,
            asset_sha256=digest(f"{tag}/{name}"),
            db_bytes=4_000,
            db_sha256=digest(f"{tag}/{name}"),
            tables={},
            period=month,
            closed=True,
        )

    def month(self, month: str, *days: int) -> list[Asset]:
        """What a release keeps of a month that is going on: the base copy, and the
        sealed delta of each of `days`."""
        return [self.base(month), *[self.sealed(f"{month}-{day:02}") for day in days]]

    def closed_month(self, month: str, *days: int) -> list[Asset]:
        """What a release keeps of a month that is over: what `month` gives, and then
        the month's part and its manifest. The sealed deltas stay beside the part."""
        return [*self.month(month, *days), self.part(month), self.month_manifest(month)]

    def manifest(self, *assets: Asset) -> Manifest:
        """The stream's manifest, which lists its state and then `assets`."""
        return Manifest(
            stream=self.id,
            producer={
                "repo": REPO,
                "commit": "",
                "run_id": "",
                "obskit": "0.1.0",
                "python": "3.14.10",
            },
            generated_at="2026-11-04T01:02:03Z",
            complete=True,
            coverage={},
            data_through=Watermark("2026-11-04T01:00:00Z", "instant"),
            cursor=None,
            licences=[],
            assets=[self.state(), *assets],
        )


# The tables of each stream's state and of its delta, as its stores make them.
REGISTRY = Stream(
    "pypi-registry",
    state_tables=("meta", "projects", "files_current", "versions_seen"),
    delta_tables=(
        "files",
        "versions",
        "release_records",
        "events",
        "serials",
        "swept",
        "cursors",
    ),
)
OSV = Stream(
    "osv-pypi",
    state_tables=("meta", "advisories"),
    delta_tables=("versions", "events", "cursors"),
)


DOWNLOADS = Stream(
    "pypi-downloads",
    state_tables=("meta", "projects", "days", "breaks", "spend", "clearances"),
    delta_tables=("projects", "days", "breaks", "spend", "clearances", "counts"),
)
SERVING_TABLES = ("folded", "months", "monthly", "top_set", "daily", "recent")


def serving_db() -> Asset:
    """The downloads stream's derived database: an asset of the current release with the
    role public, no period and derived tables, which each run makes anew."""
    return DOWNLOADS._entry(
        "pypi-downloads-serving.db.zst",
        current_tag(DOWNLOADS.id),
        role="public",
        tables={t: TableInfo(10, "derived") for t in SERVING_TABLES},
        period=None,
        closed=False,
    )


def downloads_on_2026_11_04() -> Manifest:
    return DOWNLOADS.manifest(
        DOWNLOADS.open_delta("2026-11-04"),
        serving_db(),
        *DOWNLOADS.month("2026-11", 1, 2, 3),
    )


def registry_on_2026_11_04() -> Manifest:
    """The registry early in its first month. The month's manifest is listed too,
    though the real one comes only when the month is over, so that the tests show that
    a JSON asset is never a part."""
    return REGISTRY.manifest(
        REGISTRY.open_delta("2026-11-04"),
        *REGISTRY.month("2026-11", 1, 2, 3),
        REGISTRY.month_manifest("2026-11"),
    )


def registry_on_2026_12_03() -> Manifest:
    """November is over, so its part and its manifest are listed beside its sealed
    deltas. December has begun."""
    return REGISTRY.manifest(
        REGISTRY.open_delta("2026-12-03"),
        *REGISTRY.closed_month("2026-11", 1, 2, 30),
        *REGISTRY.month("2026-12", 1, 2),
    )


def registry_on_2027_02_03() -> Manifest:
    """2026 is over and has its year part, which is listed beside the parts of its two
    months. January is over too, and February has begun."""
    return REGISTRY.manifest(
        REGISTRY.open_delta("2027-02-03"),
        REGISTRY.year_part("2026"),
        *REGISTRY.closed_month("2026-11", 1),
        *REGISTRY.closed_month("2026-12", 1),
        *REGISTRY.closed_month("2027-01", 1),
        *REGISTRY.month("2027-02", 1, 2),
    )


def registry_on_2027_11_03() -> Manifest:
    """A year on from the registry's first month, the same month has begun again. 2026
    has its year part, which is listed beside the parts of its months. The months of 2027
    between January and November are left out."""
    return REGISTRY.manifest(
        REGISTRY.open_delta("2027-11-03"),
        REGISTRY.year_part("2026"),
        *REGISTRY.closed_month("2026-11", 1, 2),
        *REGISTRY.closed_month("2026-12", 1),
        *REGISTRY.closed_month("2027-01", 1),
        *REGISTRY.month("2027-11", 1, 2),
    )


def osv_on_2026_11_04() -> Manifest:
    return OSV.manifest(
        OSV.open_delta("2026-11-04"),
        *OSV.month("2026-11", 1, 2, 3),
    )


def names(assets: list[Asset]) -> list[str]:
    return [asset.name for asset in assets]


def without(manifest: Manifest, *gone: Asset) -> Manifest:
    """`manifest` without the entries `gone`."""
    return replace(manifest, assets=[a for a in manifest.assets if a not in gone])


def with_assets(manifest: Manifest, *added: Asset) -> Manifest:
    return replace(manifest, assets=[*manifest.assets, *added])


def changed(manifest: Manifest, old: Asset, **changes) -> Manifest:
    """`manifest` with the entry `old` changed by `changes`."""
    return replace(
        manifest,
        assets=[replace(a, **changes) if a == old else a for a in manifest.assets],
    )


def test_input_error_is_a_value_error():
    # A caller that catches ValueError also catches an input that cannot be read.
    assert issubclass(InputError, ValueError)


def test_needed_assets_for_a_state_only_admission():
    # The base copy and the deltas are listed, and none is needed: advisories is a table
    # of osv's state, and osv admits no table of its delta.
    manifest = osv_on_2026_11_04()

    assert names(needed_assets(manifest, OSV_TABLES)) == ["osv-pypi-state.db.zst"]


def test_needed_assets_take_the_state_the_sealed_deltas_and_the_open_delta():
    manifest = registry_on_2026_11_04()

    # The manifest lists the open delta before the sealed deltas, as a publish does, and
    # the result puts it last: its day is the latest.
    assert names(manifest.assets[:2]) == [
        "pypi-registry-state.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]
    assert names(needed_assets(manifest, REGISTRY_TABLES)) == [
        "pypi-registry-state.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-02.db.zst",
        "pypi-registry-delta-2026-11-03.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]


def test_needed_assets_leave_out_a_state_that_no_admitted_table_asks_for():
    # Events is a table of the registry's delta, and no table of its state is admitted.
    manifest = registry_on_2026_11_04()

    assert names(needed_assets(manifest, frozenset({"events"}))) == [
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-02.db.zst",
        "pypi-registry-delta-2026-11-03.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]


def test_a_month_part_replaces_its_sealed_deltas():
    manifest = registry_on_2026_12_03()

    assert names(needed_assets(manifest, REGISTRY_TABLES)) == [
        "pypi-registry-state.db.zst",
        "pypi-registry-2026-11.db.zst",
        "pypi-registry-delta-2026-12-01.db.zst",
        "pypi-registry-delta-2026-12-02.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]


def test_a_year_part_replaces_its_month_parts():
    manifest = registry_on_2027_02_03()

    # The year part goes in for the parts of November and December, and for the sealed
    # deltas that those parts had replaced. The part of January stays, as it is of
    # another year.
    assert names(needed_assets(manifest, REGISTRY_TABLES)) == [
        "pypi-registry-state.db.zst",
        "pypi-registry-2026.db.zst",
        "pypi-registry-2027-01.db.zst",
        "pypi-registry-delta-2027-02-01.db.zst",
        "pypi-registry-delta-2027-02-02.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]


def test_a_part_replaces_only_what_its_own_period_holds():
    # The part of November 2026 holds nothing of November 2027, so the deltas of that
    # month stay, and the part of January 2027 stays beside the year part of 2026.
    manifest = registry_on_2027_11_03()

    assert names(needed_assets(manifest, REGISTRY_TABLES)) == [
        "pypi-registry-state.db.zst",
        "pypi-registry-2026.db.zst",
        "pypi-registry-2027-01.db.zst",
        "pypi-registry-delta-2027-11-01.db.zst",
        "pypi-registry-delta-2027-11-02.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]


def test_a_part_that_is_not_closed_replaces_nothing():
    # A part that is not closed can still change, so what it would replace stays.
    month_part = changed(
        registry_on_2026_12_03(), REGISTRY.part("2026-11"), closed=False
    )
    assert names(needed_assets(month_part, REGISTRY_TABLES)) == [
        "pypi-registry-state.db.zst",
        "pypi-registry-2026-11.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-02.db.zst",
        "pypi-registry-delta-2026-11-30.db.zst",
        "pypi-registry-delta-2026-12-01.db.zst",
        "pypi-registry-delta-2026-12-02.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]

    year_part = changed(
        registry_on_2027_02_03(), REGISTRY.year_part("2026"), closed=False
    )
    assert names(needed_assets(year_part, REGISTRY_TABLES)) == [
        "pypi-registry-state.db.zst",
        "pypi-registry-2026.db.zst",
        "pypi-registry-2026-11.db.zst",
        "pypi-registry-2026-12.db.zst",
        "pypi-registry-2027-01.db.zst",
        "pypi-registry-delta-2027-02-01.db.zst",
        "pypi-registry-delta-2027-02-02.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]


def test_parts_of_one_period_are_put_in_the_order_of_their_names():
    # No real manifest lists two parts for one day. The order is made sure of all the
    # same, so that the result does not depend on how the manifest lists them.
    second = replace(REGISTRY.sealed("2026-11-02"), name="a-second-delta.db.zst")
    manifest = with_assets(registry_on_2026_11_04(), second)

    assert names(needed_assets(manifest, REGISTRY_TABLES))[1:4] == [
        "pypi-registry-delta-2026-11-01.db.zst",
        "a-second-delta.db.zst",
        "pypi-registry-delta-2026-11-02.db.zst",
    ]


def test_a_json_asset_is_not_a_part_whatever_it_lists():
    # A month's manifest is JSON and lists no tables. This entry lists the delta's, and
    # has a closed day, so that only its format keeps it out.
    sealed = REGISTRY.sealed("2026-11-02")
    manifest = changed(
        registry_on_2026_11_04(),
        sealed,
        format="json",
        db_bytes=sealed.asset_bytes,
        db_sha256=sealed.asset_sha256,
    )

    assert names(needed_assets(manifest, REGISTRY_TABLES)) == [
        "pypi-registry-state.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-03.db.zst",
        "pypi-registry-delta-open.db.zst",
    ]
    assert names(pin_assets(manifest, REGISTRY_TABLES, date(2026, 11, 3))) == [
        "pypi-registry-state-base-2026-11.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-03.db.zst",
    ]


@pytest.mark.parametrize(
    "other",
    [{"role": "public"}, {"tag": "pypi-registry-month-2026-11"}],
    ids=["another role", "another release"],
)
def test_the_state_and_the_open_delta_are_state_assets_of_the_current_release(other):
    delta = REGISTRY.open_delta("2026-11-04")
    manifest = REGISTRY.manifest(delta)

    # A state that has another role, or is in another release, is no state, and an open
    # delta that has either is no open delta.
    with pytest.raises(InputError, match="lists no state"):
        needed_assets(changed(manifest, REGISTRY.state(), **other), REGISTRY_TABLES)
    assert open_day(changed(manifest, delta, **other)) is None


REFUSED = [
    pytest.param(
        without(registry_on_2026_11_04(), REGISTRY.state()),
        REGISTRY_TABLES,
        "lists no state",
        id="no state",
    ),
    pytest.param(
        registry_on_2026_11_04(),
        REGISTRY_TABLES | {"nonesuch"},
        "nonesuch",
        id="a table that no asset holds",
    ),
    pytest.param(
        changed(
            registry_on_2026_11_04(),
            REGISTRY.sealed("2026-11-02"),
            period=None,
            closed=False,
        ),
        REGISTRY_TABLES,
        re.escape("pypi-registry-delta-2026-11-02.db.zst"),
        id="a delta part with no period",
    ),
    pytest.param(
        with_assets(
            registry_on_2026_11_04(),
            replace(REGISTRY.state(), name="pypi-registry-state-again.db.zst"),
        ),
        REGISTRY_TABLES,
        "more than one state",
        id="two states",
    ),
]


@pytest.mark.parametrize(("manifest", "tables", "reason"), REFUSED)
def test_needed_assets_refuse(manifest, tables, reason):
    # Each case is refused for the reason that it names.
    with pytest.raises(InputError, match=reason):
        needed_assets(manifest, tables)


def test_fingerprint_follows_the_bytes_and_not_the_order():
    state = REGISTRY.state()
    sealed = REGISTRY.sealed("2026-11-01")
    open_delta = REGISTRY.open_delta("2026-11-04")
    inputs = [state, sealed, open_delta]

    # The sha256 of the sorted lines `tag/name:asset_sha256`, joined with newlines.
    lines = sorted(f"{a.tag}/{a.name}:{a.asset_sha256}" for a in inputs)
    assert fingerprint(inputs) == sha256("\n".join(lines).encode()).hexdigest()
    assert fingerprint([open_delta, state, sealed]) == fingerprint(inputs)
    # Other bytes under the same name are another input, and so are the same bytes under
    # another name or in another release.
    rebuilt = replace(state, asset_sha256=digest("rebuilt"))
    renamed = replace(state, name="pypi-registry-state-2.db.zst")
    moved = replace(state, tag="pypi-registry-month-2026-11")
    for other in (rebuilt, renamed, moved):
        assert fingerprint([other, sealed, open_delta]) != fingerprint(inputs)
    # Nothing but the bytes of the file counts with its name and release: not the sizes
    # and tables that the entry gives.
    rewritten = replace(state, asset_bytes=1, db_bytes=1, tables={})
    assert fingerprint([rewritten, sealed, open_delta]) == fingerprint(inputs)


# The sealed deltas of the month are listed, and none stands in for the open delta: it
# is the open delta that says which day the stream has reached.
NO_OPEN_DELTA = without(registry_on_2026_11_04(), REGISTRY.open_delta("2026-11-04"))


def test_open_day_is_the_day_the_open_delta_was_opened_for():
    assert open_day(REGISTRY.manifest(REGISTRY.open_delta("2026-11-05"))) == date(
        2026, 11, 5
    )
    assert open_day(NO_OPEN_DELTA) is None


@pytest.mark.parametrize(
    ("manifest", "sealed"),
    [
        (REGISTRY.manifest(REGISTRY.open_delta("2026-11-05")), date(2026, 11, 4)),
        # The day before is in the month, the year or the leap day before.
        (REGISTRY.manifest(REGISTRY.open_delta("2026-12-01")), date(2026, 11, 30)),
        (REGISTRY.manifest(REGISTRY.open_delta("2027-01-01")), date(2026, 12, 31)),
        (REGISTRY.manifest(REGISTRY.open_delta("2028-03-01")), date(2028, 2, 29)),
        (NO_OPEN_DELTA, None),
    ],
    ids=["an open day", "a month back", "a year back", "a leap day", "no open delta"],
)
def test_sealed_through_is_the_day_before_the_open_delta(manifest, sealed):
    assert sealed_through(manifest) == sealed


def test_two_open_deltas_are_refused():
    manifest = with_assets(
        REGISTRY.manifest(REGISTRY.open_delta("2026-11-04")),
        replace(REGISTRY.open_delta("2026-11-05"), name="pypi-registry-delta-2.db.zst"),
    )

    for read in (open_day, sealed_through):
        with pytest.raises(InputError, match="more than one open delta"):
            read(manifest)


def test_pin_assets_name_what_rebuilds_the_day():
    # The base copy of November, and the sealed deltas of the 1st and the 2nd. The
    # delta of the 3rd is after the day.
    assert names(
        pin_assets(registry_on_2026_11_04(), REGISTRY_TABLES, date(2026, 11, 2))
    ) == [
        "pypi-registry-state-base-2026-11.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-02.db.zst",
    ]

    # October has closed, so its part holds the registry's history before November. The
    # base copy of October, its sealed delta and its manifest are not named: the part
    # replaces the delta, and the base copy and the manifest hold no delta table.
    with_october = with_assets(
        registry_on_2026_11_04(), *REGISTRY.closed_month("2026-10", 31)
    )
    assert names(pin_assets(with_october, REGISTRY_TABLES, date(2026, 11, 2))) == [
        "pypi-registry-2026-10.db.zst",
        "pypi-registry-state-base-2026-11.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-02.db.zst",
    ]

    # osv admits no table of its delta, so it reads no history, and its pin names what
    # rebuilds its state and no more, though a closed October is listed.
    osv = with_assets(osv_on_2026_11_04(), *OSV.closed_month("2026-10", 31))
    assert names(pin_assets(osv, OSV_TABLES, date(2026, 11, 2))) == [
        "osv-pypi-state-base-2026-11.db.zst",
        "osv-pypi-delta-2026-11-01.db.zst",
        "osv-pypi-delta-2026-11-02.db.zst",
    ]


def test_pin_assets_name_create_once_assets_only():
    # The pin is for the day that is still open. The open delta is of that day, but each
    # run adds to it and replaces it, so a pin that named it would stop being true.
    manifest = registry_on_2026_11_04()
    pinned = pin_assets(manifest, REGISTRY_TABLES, date(2026, 11, 4))
    assert names(pinned) == [
        "pypi-registry-state-base-2026-11.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-02.db.zst",
        "pypi-registry-delta-2026-11-03.db.zst",
    ]
    assert all(a.closed and a.tag != current_tag(REGISTRY.id) for a in pinned)

    # An entry of the current release that a manifest marked closed, for a day of the
    # pin's month before the pin's day, is not named either. Nothing in the current
    # release is create-once, whatever its entry says.
    stray = replace(
        REGISTRY.open_delta("2026-11-03"),
        name="pypi-registry-stray.db.zst",
        closed=True,
    )
    assert (
        pin_assets(with_assets(manifest, stray), REGISTRY_TABLES, date(2026, 11, 4))
        == pinned
    )

    # And a sealed delta whose entry does not say that it is closed is left out too.
    reopened = changed(manifest, REGISTRY.sealed("2026-11-02"), closed=False)
    assert names(pin_assets(reopened, REGISTRY_TABLES, date(2026, 11, 4))) == [
        "pypi-registry-state-base-2026-11.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-03.db.zst",
    ]


def test_pin_assets_rebuild_a_closed_month_from_its_sealed_deltas():
    # November is over, and its part is listed. The part holds the days after the 2nd
    # as well, so a pin of the 2nd is rebuilt from the base copy and the sealed deltas.
    manifest = registry_on_2026_12_03()

    assert names(pin_assets(manifest, REGISTRY_TABLES, date(2026, 11, 2))) == [
        "pypi-registry-state-base-2026-11.db.zst",
        "pypi-registry-delta-2026-11-01.db.zst",
        "pypi-registry-delta-2026-11-02.db.zst",
    ]


def test_pin_assets_name_the_coarsest_parts_wholly_before_the_month():
    # The registry in 2028, when 2027 is over and has a year part. The pin is for a day
    # in March 2027.
    manifest = REGISTRY.manifest(
        REGISTRY.open_delta("2028-01-05"),
        REGISTRY.year_part("2026"),
        *REGISTRY.closed_month("2026-11", 1),
        *REGISTRY.closed_month("2026-12", 1),
        REGISTRY.year_part("2027"),
        *REGISTRY.closed_month("2027-01", 1),
        *REGISTRY.closed_month("2027-02", 1),
        *REGISTRY.closed_month("2027-03", 1, 14, 15, 16),
        *REGISTRY.closed_month("2027-04", 1),
    )

    # The part of 2026 stands for its months. A reader of the whole manifest takes the
    # part of 2027 for the months of 2027, but it holds March whole and the months after
    # it, so a pin of the 15th cannot name it, and names the parts of January and
    # February in its place. The parts of March and April are not named, and neither are
    # the deltas after the 15th.
    assert names(pin_assets(manifest, REGISTRY_TABLES, date(2027, 3, 15))) == [
        "pypi-registry-2026.db.zst",
        "pypi-registry-2027-01.db.zst",
        "pypi-registry-2027-02.db.zst",
        "pypi-registry-state-base-2027-03.db.zst",
        "pypi-registry-delta-2027-03-01.db.zst",
        "pypi-registry-delta-2027-03-14.db.zst",
        "pypi-registry-delta-2027-03-15.db.zst",
    ]


def test_pin_assets_take_the_deltas_of_the_month_of_the_pin_and_no_other():
    # The sealed deltas of November 2026 are before the pin's day and have its month's
    # number, but they are of another year, and the year part of 2026 stands for them.
    manifest = registry_on_2027_11_03()

    assert names(pin_assets(manifest, REGISTRY_TABLES, date(2027, 11, 2))) == [
        "pypi-registry-2026.db.zst",
        "pypi-registry-2027-01.db.zst",
        "pypi-registry-state-base-2027-11.db.zst",
        "pypi-registry-delta-2027-11-01.db.zst",
        "pypi-registry-delta-2027-11-02.db.zst",
    ]


PIN_REFUSED = [
    pytest.param(
        without(registry_on_2026_11_04(), REGISTRY.state()),
        date(2026, 11, 2),
        "lists no state",
        id="no state",
    ),
    pytest.param(
        without(registry_on_2026_11_04(), REGISTRY.base("2026-11")),
        date(2026, 11, 2),
        "no closed base copy of 2026-11",
        id="no base copy",
    ),
    pytest.param(
        registry_on_2026_11_04(),
        date(2026, 10, 31),
        "no closed base copy of 2026-10",
        id="a month that has no base copy",
    ),
    pytest.param(
        changed(registry_on_2026_11_04(), REGISTRY.base("2026-11"), closed=False),
        date(2026, 11, 2),
        "no closed base copy of 2026-11",
        id="a base copy that is not closed",
    ),
    pytest.param(
        changed(
            registry_on_2026_11_04(),
            REGISTRY.base("2026-11"),
            tables=tables_of(("meta", "projects"), 900),
        ),
        date(2026, 11, 2),
        "no closed base copy of 2026-11",
        id="a month asset with other tables than the state's",
    ),
    pytest.param(
        with_assets(
            registry_on_2026_11_04(),
            replace(REGISTRY.base("2026-11"), name="pypi-registry-base-again.db.zst"),
        ),
        date(2026, 11, 2),
        "more than one base copy of 2026-11",
        id="two base copies",
    ),
]


@pytest.mark.parametrize(("manifest", "day", "reason"), PIN_REFUSED)
def test_pin_assets_refuse(manifest, day, reason):
    with pytest.raises(InputError, match=reason):
        pin_assets(manifest, REGISTRY_TABLES, day)


def test_asset_url():
    asset = REGISTRY.sealed("2026-11-01")

    assert asset_url(REPO, asset) == (
        "https://github.com/py-observatory/data/releases/download/"
        "pypi-registry-month-2026-11/pypi-registry-delta-2026-11-01.db.zst"
    )


def test_the_choice_does_not_read_names():
    # The same manifest with every name replaced by one that says nothing. The names
    # are the stream's own, so the choice must follow tag, role, period, format, closed
    # and tables, and pick the same entries.
    manifest = registry_on_2027_02_03()
    nameless = replace(
        manifest,
        assets=[
            replace(a, name=f"asset-{i:02}") for i, a in enumerate(manifest.assets)
        ],
    )

    def places(of: Manifest, chosen: list[Asset]) -> list[int]:
        return [of.assets.index(asset) for asset in chosen]

    needed = needed_assets(manifest, REGISTRY_TABLES)
    assert len(needed) == 6
    assert places(nameless, needed_assets(nameless, REGISTRY_TABLES)) == places(
        manifest, needed
    )

    day = date(2027, 2, 2)
    pinned = pin_assets(manifest, REGISTRY_TABLES, day)
    assert len(pinned) == 5
    assert places(nameless, pin_assets(nameless, REGISTRY_TABLES, day)) == places(
        manifest, pinned
    )
    assert open_day(nameless) == open_day(manifest) == date(2027, 2, 3)


def test_a_derived_asset_holding_admitted_tables_is_needed():
    manifest = downloads_on_2026_11_04()

    # The state serves projects, days and breaks, and the derived database the rest. The
    # counts of the deltas are not admitted, so no delta is needed.
    assert names(needed_assets(manifest, DOWNLOADS_TABLES)) == [
        "pypi-downloads-state.db.zst",
        "pypi-downloads-serving.db.zst",
    ]


def test_a_derived_asset_that_holds_no_admitted_table_is_not_needed():
    manifest = downloads_on_2026_11_04()

    assert names(needed_assets(manifest, frozenset({"projects", "days"}))) == [
        "pypi-downloads-state.db.zst"
    ]


def test_no_part_is_asked_for_tables_a_derived_asset_holds():
    # Here the deltas and the state hold a table that the derived database holds too.
    # Its rows are read from the derived database alone, so neither is asked for it.
    manifest = downloads_on_2026_11_04()
    shared = {"monthly": TableInfo(5, "append")}
    for asset in [
        DOWNLOADS.open_delta("2026-11-04"),
        DOWNLOADS.sealed("2026-11-01"),
        DOWNLOADS.sealed("2026-11-02"),
        DOWNLOADS.sealed("2026-11-03"),
    ]:
        manifest = changed(manifest, asset, tables={**asset.tables, **shared})

    assert names(needed_assets(manifest, frozenset({"monthly", "daily"}))) == [
        "pypi-downloads-serving.db.zst"
    ]


def test_the_registry_and_osv_need_what_they_needed_without_a_derived_asset():
    assert names(needed_assets(osv_on_2026_11_04(), OSV_TABLES)) == [
        "osv-pypi-state.db.zst"
    ]
    assert needed_assets(registry_on_2026_11_04(), REGISTRY_TABLES)[-1].name == (
        "pypi-registry-delta-open.db.zst"
    )
