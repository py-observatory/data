"""The catalog's daily pin, and the month's copy of its serving database.

The pin of a day is the release `vYYYY-MM-DD`, which holds one file, release.json. For
each stream, it names by URL and sha256 the create-once assets that rebuild what the
catalog reads of the stream as it stood at the end of the day (see
`inputs.pin_assets`), and it gives the commit of the run that wrote it. Those assets are
never replaced, so the pin stays true for as long as they are kept. The pin of a day is
written once every stream has sealed the day, or once `pin_wait_hours` have passed since
the day ended, and then it marks each stream that has not sealed the day. Pins start on
the day of the catalog's first build. A release that holds release.json already is left
alone, so a rerun never writes a pin twice.

The month's copy is the release `catalog-month-YYYY-MM`. The first run of a month puts
in it the serving database of the last build of the month before, with that build's
manifest. The database is the bytes that `catalog-current` holds, as downloaded and
checked, and never compressed again: zstd can give other bytes after a library update,
and a rerun after a failed run would then find its own copy refused under its name.
"""

import json
import os
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from obskit.cycle import month_manifest_name
from obskit.github import GitHub, NotFound
from obskit.manifest import Asset, Manifest, dumps
from obskit.release import PriorStateError, add_create_once, fetch_assets
from obskit.tags import current_tag, month_tag
from obskit.zst import sha256_file

from catalog.config import Config
from catalog.inputs import (
    InputError,
    StreamRead,
    asset_url,
    pin_assets,
    sealed_through,
)
from catalog.schema import CATALOG, SERVING_DB

PIN_ASSET = "release.json"


def pin_tag(day: date) -> str:
    """The release that holds the pin of `day`."""
    return f"v{day.isoformat()}"


def due_days(first: date, last_pin: date | None, today: date) -> list[date]:
    """The days whose pins are due on `today`, in order: from the day after `last_pin`,
    or from `first`, the day of the catalog's first build, when no pin has been
    written, through the day before `today`, which has not ended."""
    start = first if last_pin is None else last_pin + timedelta(days=1)
    return [start + timedelta(days=n) for n in range((today - start).days)]


def pin_ready(
    day: date, reads: list[StreamRead], now: datetime, wait_hours: int
) -> bool:
    """Whether the pin of `day` is to be written at `now`: every stream of `reads` has
    sealed the day, or `wait_hours` have passed since the day ended at midnight UTC.

    A stream that could not be read has sealed nothing that the catalog can see, and
    nor has one whose manifest lists no open delta or more than one (see
    `inputs.sealed_through`).
    """
    ended = datetime.combine(day + timedelta(days=1), time(), tzinfo=UTC)
    if now >= ended + timedelta(hours=wait_hours):
        return True
    return all(_has_sealed(read, day) for read in reads)


def _has_sealed(read: StreamRead, day: date) -> bool:
    """Whether the stream of `read` has sealed `day`."""
    if read.manifest is None:
        return False
    try:
        sealed = sealed_through(read.manifest)
    except InputError:
        return False
    return sealed is not None and sealed >= day


def pin_document(
    day: date, reads: list[StreamRead], config: Config, repo: str, commit: str
) -> dict:
    """The release.json of the pin of `day`: `commit`, the day, the repository `repo`
    and, for each stream of `reads`, what rebuilds the tables that `config` admits of it
    at the end of the day.

    That is the stream's create-once assets that `inputs.pin_assets` names, each with
    its name, period, sha256, release and URL, and the last day that the stream has
    sealed. The stream is complete when that is the day or later. A stream that could
    not be read, or whose manifest does not list what a pin needs, such as the base
    copy of the day's month, names no asset, is not complete, and gives the reason as
    its error.
    """
    return {
        "commit": commit,
        "day": day.isoformat(),
        "repo": repo,
        "streams": {
            read.stream: _pinned(read, config.streams[read.stream].tables, day, repo)
            for read in reads
        },
    }


def _pinned(read: StreamRead, tables: frozenset[str], day: date, repo: str) -> dict:
    """What the pin of `day` says of the stream of `read`, which admits `tables`."""
    if read.manifest is None:
        return _not_pinned(None, read.error)
    sealed = None
    try:
        sealed = sealed_through(read.manifest)
        assets = pin_assets(read.manifest, tables, day)
    except InputError as exc:
        return _not_pinned(sealed, str(exc))
    return {
        "assets": [
            {
                "name": asset.name,
                "period": asset.period,
                "sha256": asset.asset_sha256,
                "tag": asset.tag,
                "url": asset_url(repo, asset),
            }
            for asset in assets
        ],
        "complete": sealed is not None and sealed >= day,
        "error": None,
        "sealed_through": _day_or_none(sealed),
    }


def _not_pinned(sealed: date | None, error: str | None) -> dict:
    """What a pin says of a stream that it cannot name assets of, for `error`, and that
    has sealed the days through `sealed`, when that is known."""
    return {
        "assets": [],
        "complete": False,
        "error": error,
        "sealed_through": _day_or_none(sealed),
    }


def _day_or_none(day: date | None) -> str | None:
    return None if day is None else day.isoformat()


def write_pin(gh: GitHub, day: date, document: dict, workdir: Path) -> bool:
    """Write the pin of `day`, whose release.json is `document`, from `workdir`.

    Returns False, having changed nothing, when its release holds release.json already:
    a pin is written once, and a rerun leaves it as it is. Otherwise it adds the file
    to the release, which is created as a prerelease that is not Latest when there is
    none, and returns True. So a release that a stopped run left without the file gets
    it. A release.json that GitHub lists without a digest is not a pin (see
    `_holds_pin`), and adding the file beside it raises CreateOnceError.
    """
    tag = pin_tag(day)
    if _holds_pin(gh, tag):
        return False
    path = workdir / PIN_ASSET
    text = json.dumps(document, sort_keys=True, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")
    add_create_once(gh, CATALOG, tag, [path])
    return True


def _holds_pin(gh: GitHub, tag: str) -> bool:
    """Whether the release `tag` exists and holds release.json, which GitHub lists with
    a digest once its upload is whole.

    One listed without a digest is an upload that stuck or failed, and it is not taken
    as a pin: `write_pin` then adds the file again, which `add_create_once` refuses
    loudly, rather than skip the day for good.
    """
    try:
        release = gh.release_by_tag(tag)
    except NotFound:
        return False
    return any(
        asset["name"] == PIN_ASSET and asset.get("digest")
        for asset in gh.assets(release)
    )


def serving_asset(prior: Manifest) -> Asset:
    """The serving database that the build of the manifest `prior` put in
    `catalog-current`. Raises PriorStateError when the manifest lists none."""
    tag = current_tag(CATALOG)
    name = f"{SERVING_DB}.zst"
    for asset in prior.assets:
        if asset.tag == tag and asset.name == name:
            return asset
    raise PriorStateError(
        f"the catalog's last manifest lists no {name} in {tag}, so the last build's "
        "serving database cannot be found"
    )


def copy_month(
    gh: GitHub, prior: Manifest, workdir: Path, now: datetime
) -> list[Asset]:
    """Copy the last build of a month that has ended into `catalog-month-YYYY-MM`, and
    return the copy's entries, which every later manifest keeps.

    `prior` is the manifest of the catalog's last build. When it was made in a month
    before the month of `now`, its serving database is downloaded from
    `catalog-current` into `workdir`, which must exist, and checked against its entry.
    The downloaded bytes go into the release of the month of `prior` as
    `catalog-YYYY-MM.db.zst`, with `prior` itself, as `dumps` writes it, as
    `manifest-YYYY-MM.json`. A rerun makes both files again with the same bytes, so a
    run that stops after adding them and runs again finds them there and adds nothing.

    It returns the two entries, closed and with the month as their period: the
    database's, which is the prior serving entry with the copy's name and release, so
    its digests and tables, and the manifest's, which is JSON, with its own digests
    and no tables. It returns [] when `prior` was made in the month of `now`, and
    makes no request.

    Every error propagates: PriorStateError when `prior` lists no serving database or
    the download does not match its entry, CreateOnceError when the month's release
    holds either file with other bytes, and any GitHubError.
    """
    built = datetime.fromisoformat(prior.generated_at)
    month = f"{built:%Y-%m}"
    if month >= f"{now.astimezone(UTC):%Y-%m}":
        return []
    serving = serving_asset(prior)
    tag = month_tag(CATALOG, built.date())
    [expanded] = fetch_assets(gh, [serving], workdir, keep_downloads=True)
    # The copy is the downloaded bytes, which move to the copy's name. The database
    # that they expand to is not needed, and would hold the disk through the build.
    expanded.unlink()
    database = workdir / f"{CATALOG}-{month}.db.zst"
    os.replace(workdir / serving.name, database)
    manifest = workdir / month_manifest_name(built.year, built.month)
    manifest.write_text(dumps(prior), encoding="utf-8")
    add_create_once(gh, CATALOG, tag, [database, manifest])
    return [
        replace(serving, name=database.name, tag=tag, period=month, closed=True),
        _json_entry(manifest, tag, month),
    ]


def _json_entry(path: Path, tag: str, period: str) -> Asset:
    """The closed entry of the JSON file `path` in the release `tag`, for `period`. A
    JSON asset repeats its own size and sha256 for the database's, and holds no
    tables."""
    size = path.stat().st_size
    digest = sha256_file(path)
    return Asset(
        name=path.name,
        tag=tag,
        role="public",
        format="json",
        asset_bytes=size,
        asset_sha256=digest,
        db_bytes=size,
        db_sha256=digest,
        tables={},
        period=period,
        closed=True,
    )
