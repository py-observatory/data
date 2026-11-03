"""Which assets of a stream the catalog reads, and which a daily pin names, decided from
the stream's published manifest alone.

A release listing can show a publish half made, and the names of a stream's files are its
own business. The manifest is swapped in last, after the files that it lists, and each of
its entries gives the tag, role, period, format, closed flag and tables of an asset. So
the choice of the assets to read, and of those that a pin names, rests on those and
never on a name. A name still counts in two places: the order of assets breaks a tie of
periods by name, so that it is the same every time, and the fingerprint hashes each
asset's release and name with its sha256.

The assets of `<stream>-current` that have the role state are the state, which has no
period, and the open delta, whose period is the day of the stream's latest run. What a
stream keeps for good is closed: for each month, a copy of the state as the month began
(the base copy), the sealed delta of each day, and, once the month is over, the month's
part, which is its deltas merged into one database. A year's part, which would merge the
parts of its months, is not made yet, and the rules take it as they take a month's part.

The admitted tables that the state lacks are the delta tables, and a delta part is a
sqlite asset that holds one: the open delta, a sealed delta, a month's part or a year's
part. A base copy holds the state's tables, and a month's manifest is JSON, so neither is
a delta part.

`read_stream` fetches a stream's manifest and finds what the catalog needs of it. A
stream that cannot be read does not stop a build, which keeps that stream's tables from
the previous build, so `read_stream` gives the reason in place of the manifest.
"""

import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from catalog.config import StreamAdmission
from obskit.github import GitHub, GitHubError
from obskit.manifest import Asset, Manifest
from obskit.release import PriorStateError, fetch_manifest
from obskit.tags import current_tag

_SQLITE = "sqlite+zstd"
# How long each shape of a period is, as obskit.cycle tells a day from the others.
_YEAR = len("YYYY")
_MONTH = len("YYYY-MM")
_DAY = len("YYYY-MM-DD")


class InputError(ValueError):
    """A stream's manifest does not list what the catalog needs of it, or lists it in a
    way that leaves the catalog unable to tell which entry is meant."""


def _period(asset: Asset) -> str:
    """The period of `asset`, and an empty string for an asset with none, which is no
    year, month or day."""
    return asset.period or ""


def _order(asset: Asset) -> tuple[str, str]:
    """How the catalog puts assets in order: by period, and by name for equal periods.
    As the shorter text comes first, a year comes before its months, and a month before
    its days."""
    return _period(asset), asset.name


def _only(manifest: Manifest, found: list[Asset], what: str) -> Asset | None:
    """The only asset of `found`, or None when it is empty. Raises InputError for more
    than one, because the catalog cannot tell which of them is `what`, and reading the
    wrong one would put the wrong data in the catalog."""
    if len(found) > 1:
        listed = ", ".join(f"{asset.tag}/{asset.name}" for asset in found)
        raise InputError(
            f"the manifest of {manifest.stream} lists more than one {what}: {listed}"
        )
    return found[0] if found else None


def _in_current(manifest: Manifest) -> list[Asset]:
    """The assets of the stream's current release that have the role state."""
    tag = current_tag(manifest.stream)
    return [a for a in manifest.assets if a.tag == tag and a.role == "state"]


def _state(manifest: Manifest) -> Asset:
    """The state: the asset of the current release with the role state and no period.

    Raises InputError when there is none, or more than one. Without it the catalog
    cannot tell the stream's delta tables from its state tables, and so cannot tell a
    base copy from a delta part.
    """
    state = _only(
        manifest, [a for a in _in_current(manifest) if a.period is None], "state"
    )
    if state is None:
        raise InputError(
            f"the manifest of {manifest.stream} lists no state, which is an asset of "
            f"{current_tag(manifest.stream)} with the role state and no period"
        )
    return state


def _open_delta(manifest: Manifest) -> Asset | None:
    """The open delta: the asset of the current release with the role state and a day
    for its period, or None when there is none. Raises InputError for more than one."""
    found = [a for a in _in_current(manifest) if len(_period(a)) == _DAY]
    return _only(manifest, found, "open delta")


def _delta_parts(
    manifest: Manifest, wanted: frozenset[str], assets: list[Asset]
) -> list[Asset]:
    """The delta parts among `assets`: the sqlite assets that hold a table of `wanted`,
    which are the admitted tables that the state lacks.

    Raises InputError for a part with no period, which could be neither put in order
    nor matched with the part that replaces it.
    """
    parts = [
        a for a in assets if a.format == _SQLITE and not wanted.isdisjoint(a.tables)
    ]
    for part in parts:
        if part.period is None:
            raise InputError(
                f"{part.tag}/{part.name} in the manifest of {manifest.stream} holds a "
                "table of the delta but has no period, so where it falls in the "
                "stream's history is not known"
            )
    return parts


def _coarsest(parts: list[Asset]) -> list[Asset]:
    """`parts` without the days that a closed month part covers and the months that a
    closed year part covers, so that each row is read from one part.

    A part that is not closed can still change, so it covers nothing. What a closed part
    covers is told from the closed parts of `parts` as they are given, and not from those
    that are left, so a day goes with its month, and the month with its year.
    """
    closed = {_period(part) for part in parts if part.closed}

    def covered(period: str) -> bool:
        if len(period) == _DAY:
            return period[:_MONTH] in closed
        if len(period) == _MONTH:
            return period[:_YEAR] in closed
        return False

    return [part for part in parts if not covered(_period(part))]


def _last_month(period: str) -> str:
    """The month, as YYYY-MM, in which `period` ends. A year ends in December."""
    return f"{period}-12" if len(period) == _YEAR else period[:_MONTH]


def needed_assets(manifest: Manifest, tables: frozenset[str]) -> list[Asset]:
    """The assets that the catalog downloads to read `tables`, which are the tables that
    it admits of the stream: the state, when `tables` holds a table of it, and then the
    delta parts, in the order of their periods and then of their names.

    A delta part is a sqlite asset that holds an admitted table that the state lacks
    (see the top of this module). A closed month part stands for the sealed deltas of its
    month, and a closed year part for the parts of its months (see `_coarsest`).

    Raises InputError when the manifest lists no state, when an admitted table is held by
    none of the assets chosen, and when a delta part has no period.
    """
    state = _state(manifest)
    own = set(state.tables)
    parts = _coarsest(_delta_parts(manifest, tables - own, manifest.assets))
    needed = [*([state] if tables & own else []), *sorted(parts, key=_order)]
    held = {table for asset in needed for table in asset.tables}
    missing = sorted(tables - held)
    if missing:
        raise InputError(
            f"no asset that the catalog needs of {manifest.stream} holds "
            f"{', '.join(missing)}"
        )
    return needed


def fingerprint(assets: list[Asset]) -> str:
    """The sha256 that stands for the bytes of `assets`, so that two builds can tell by
    comparing them whether any input has changed.

    It is the sha256 of the lines `tag/name:asset_sha256`, one for each asset, sorted and
    joined with newlines. So the order of `assets` does not matter, and nor does
    anything of an entry but its bytes, its name and its release.
    """
    lines = sorted(f"{a.tag}/{a.name}:{a.asset_sha256}" for a in assets)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


@dataclass(frozen=True)
class StreamRead:
    """What the catalog read of one stream: its manifest, the assets that it needs of it
    (see `needed_assets`) and their fingerprint. When the stream could not be read,
    `error` says why, and there is no manifest, no asset and no fingerprint."""

    stream: str
    manifest: Manifest | None
    assets: list[Asset]
    fingerprint: str | None
    error: str | None


def read_stream(gh: GitHub, admission: StreamAdmission, directory: Path) -> StreamRead:
    """Read the stream that `admission` admits: fetch its manifest into `directory`,
    which must exist, and find the assets that the catalog needs of it and their
    fingerprint. No asset is downloaded.

    The catalog fails open for its inputs, so a stream that cannot be read gives a
    StreamRead whose `error` is the reason. That is any PriorStateError, which says that
    the stream has never published or that its release holds no sound manifest of its
    own, any GitHubError, a 502 that outlasted the retries among them, and any
    ValueError, such as a ManifestError for a manifest that does not read or an
    InputError for one that does not list what the catalog needs.
    """
    try:
        manifest = fetch_manifest(gh, admission.stream, directory)
        assets = needed_assets(manifest, admission.tables)
    except (PriorStateError, GitHubError, ValueError) as exc:
        return StreamRead(admission.stream, None, [], None, str(exc))
    return StreamRead(admission.stream, manifest, assets, fingerprint(assets), None)


def open_day(manifest: Manifest) -> date | None:
    """The day of the open delta, which is the day of the stream's latest run: every run
    of a day adds to the delta of that day. None when the manifest lists no open delta.

    Raises InputError when it lists more than one.
    """
    delta = _open_delta(manifest)
    return None if delta is None else date.fromisoformat(_period(delta))


def sealed_through(manifest: Manifest) -> date | None:
    """The last day that the stream has sealed: the day before the open delta's. A run
    on a later day seals the open delta and opens another, and the days of a stream only
    move forward, so nothing is added to a day before the open delta's. That day may
    have no delta of its own, as a day without a run has none. None when the manifest
    lists no open delta.

    Raises InputError when it lists more than one.
    """
    day = open_day(manifest)
    return None if day is None else day - timedelta(days=1)


def pin_assets(manifest: Manifest, tables: frozenset[str], day: date) -> list[Asset]:
    """The create-once assets that rebuild the stream's data as it stood at the end of
    `day`, in the order of their periods and then of their names: the base copy of
    `day`'s month, every sealed delta of the month on or before `day`, and, when `tables`
    holds a table of the delta, the coarsest closed delta parts of the months before it.

    The base copy and the deltas rebuild the state of `day`, as a restore does, so the
    deltas are named whatever `tables` holds. A month part of `day`'s month, and a year
    part of its year, hold days after `day` as well, so neither is named: the sealed
    deltas, which a closed month keeps, stand in for the month. The parts of earlier
    months are the coarsest that the manifest lists wholly before `day`'s month. Nothing
    of the current release is named, because each run replaces it, and a pin that named
    it would stop being true at the next run.

    Raises InputError when the manifest lists no state, whose tables tell the base copy
    from a delta part, and when it lists no closed base copy of the month, or more than
    one.
    """
    state = _state(manifest)
    own = set(state.tables)
    month = f"{day:%Y-%m}"
    created = [
        a for a in manifest.assets if a.closed and a.tag != current_tag(manifest.stream)
    ]
    base = _only(
        manifest,
        [a for a in created if a.period == month and set(a.tables) == own],
        f"base copy of {month}",
    )
    if base is None:
        raise InputError(
            f"the manifest of {manifest.stream} lists no closed base copy of {month}, "
            "which is an asset with that period and the tables of the state"
        )
    last = day.isoformat()
    sealed = [
        a
        for a in created
        if a.format == _SQLITE
        and len(_period(a)) == _DAY
        and _period(a)[:_MONTH] == month
        and _period(a) <= last
    ]
    # Parts that reach into `day`'s month or past it cannot cover what is before it, so
    # they are set aside before the coarsest of the rest are chosen. When `tables` holds
    # no table of the delta, no asset is a delta part, and so none is named.
    before = [
        part
        for part in _delta_parts(manifest, tables - own, created)
        if _last_month(_period(part)) < month
    ]
    return sorted([base, *sealed, *_coarsest(before)], key=_order)


def asset_url(repo: str, asset: Asset) -> str:
    """The address that `asset` is served from in the repository `repo`, which is written
    owner/name, by the tag of its release and its own name."""
    return f"https://github.com/{repo}/releases/download/{asset.tag}/{asset.name}"
