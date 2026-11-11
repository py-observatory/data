"""The registry run: the order of its steps, the releases that it reads and writes, and
how it publishes.

`run_once` is one run. It reads the prior state from `pypi-registry-current`, which
fails closed: a state that cannot be read ends the run, and only a bootstrap starts
from nothing. It reads PyPI's Simple API root, and refuses a root whose serial is below
the stored cursor, which is a stale copy, before it writes anything. Then:

1. It rolls over when a day or a month has ended since the open delta began (see
   `layout`). The open delta is sealed into the month's release as the delta of its
   day, and a new one begins. When the month has ended, the run also rebuilds the
   month's state from its base copy and its sealed deltas, as a restore would, and
   refuses to go on unless that equals the state. Only then does it merge the deltas
   into the month's part, and copy the state as the base of the new month.
2. It processes each project that the root gives work for (`process.process_projects`),
   writing what it learns to the open delta, until its time is up. In what time is
   left, it re-reads the pages of one slice of the most downloaded projects and records
   any change of status (the status pass, `_status_pass`).
3. It applies the delta to the state with `store.apply_delta`, as a restore does.
4. It writes the manifest, checks it against the prior one with the publish gates, and
   swaps the state, the open delta and the manifest into the current release.

`sweep` is the other run. A bootstrap stores every project of the root without reading
its page, so every project starts unswept, and the daily run reads a page only when its
project changes. The sweep reads the page of each project that is not swept yet, in
full, a time budget at a time. It is run again while it reads some (`swept > 0`) and
some are left. One that reads none while some are left has met only pages that it
cannot use, which the next sweep would meet as well, so the chain ends there (see
`main`). It goes at about 0.8 requests a second (SWEEP_INTERVAL), which spreads the
pages of close to a million projects over about two weeks of sweeps. It takes the same
steps as `run_once`, except that it reads no root: it goes on from the state as it is,
rolls over, reads the unswept projects (`_sweep_projects`), and then applies the delta,
writes the manifest, checks the gates and publishes in the same way (`_resume` and
`_publish` are the start and the end of both). With no root it has no serial to record,
so it writes no serial and no cursor, and its manifest keeps the `data_through`, the
`cursor` and the backlog of the run before it: the coverage's `remaining` is what the
last daily run left to the next one, and its `unswept` is the sweep's progress. It
writes no event either: it reads a page as it is now, and nothing says when a file was
added or a status changed, so none of the files that it records is exact.

`records` is the third run, the records pass. The bootstrap and the sweep store the
versions of the releases from before the bootstrap without their records, so most
projects lack the record of the release that they are at, which holds what a project
says of itself: its summary, its licence and its requirements. The records pass reads
that record, from the JSON API, for each project that may lack it, in name order and a
time budget at a time, at the sweep's pace (`_record_projects`). It writes the record
alone, and applying it marks the version as recorded (see `store.apply_delta`). A reply
that makes no record is counted as refused and passed over, so one release that the
parser cannot read does not stop the pass. Like the sweep, it reads no root and keeps the prior manifest's `data_through`, `cursor` and
backlog. Its coverage says where it stopped (`records_after`) and how many projects are
left after that (`records_left`), so that the next records run goes on from there, and
every other run carries both on as they were.

The status pass is the daily run's. Archiving a project moves no serial, so the root
never shows it. The downloads stream publishes the set of the most downloaded projects
in its serving database (`top_names`), and each daily run re-reads the pages of one of
SLICES slices of it (`in_slot`). The coverage's `status_slice` is the slice that the next
pass reads: a daily run that reads its slice to the end moves it on, and every other
run, the sweep and the records run among them, carries it on as it was.

A manifest says that the data is `complete` when neither is left: no project to the next
daily run, and none unswept. That holds for every run, so the bootstrap, which has read
no page, publishes a manifest that is not complete.

The assets of a month's release are never replaced. The sealed delta and the base copy
are the files that the last run published, byte for byte: compressing again gives the
same bytes only with the same zstd and SQLite, which can change between two attempts.
The month's part is built afresh from the month's deltas, and the month's manifest
takes nothing from the run but the part's entry. So a run that stops after a rollover
and runs again adds the same files only when it has the same zstd and SQLite. With
another build its part is other bytes, and it stops with CreateOnceError for a person
to look at.

Errors are not caught, and a run that fails publishes nothing to the current release.

The gates hold back a publish that has less history than the last one, and one that
removes more than MAX_REMOVALS projects at once: a removal is for good, and a name that
comes back is a new project. A held publish goes out only when a person gives a reason,
and the manifest keeps a log of each such override. A month that its base copy and its
deltas do not rebuild is not held in that way: no reason overrides it, because a run
that went on would publish over a month that its releases cannot restore.
"""

import argparse
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
import tomllib
from collections import Counter
from collections.abc import Callable
from compression.zstd import ZstdError
from contextlib import closing
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

from obskit.gates import GateError, GateFailure, check
from obskit.github import GitHub, GitHubError
from obskit.http import Http
from obskit.manifest import (
    Asset,
    Manifest,
    ManifestError,
    Watermark,
    current_asset,
    dumps,
    instant,
    loads,
    producer,
)
from obskit.release import (
    PriorStateError,
    add_create_once,
    fetch_assets,
    fetch_manifest,
    fetch_prior,
    prepare_asset,
    publish_current,
)
from obskit.sqlite import consolidate
from obskit.tags import current_tag, month_tag
from obskit.zst import sha256_file
from pypi_registry import STREAM
from pypi_registry.layout import (
    OPEN_DELTA_DB,
    STATE_DB,
    base_state_name,
    delta_name,
    month_manifest_name,
    month_part_name,
    plan_rollover,
)
from pypi_registry.process import (
    INACTIVE,
    Context,
    ProcessReport,
    bootstrap_at,
    get_page,
    get_root,
    process_projects,
    process_status,
    process_unswept,
    record_current,
)
from pypi_registry.store import (
    DELTA_SCHEMA,
    STATE_SCHEMA,
    Delta,
    apply_delta,
    diff_states,
    open_db,
)

# More `removed` events than this in one run hold the publish for a person to look at.
# A removal is for good, and a name that comes back is a new project, so a root that
# lost its names and a burst of 404s must not be published unread.
MAX_REMOVALS = 1_000

# What `main` returns for a sweep or a records run that is to be run again, so that the
# workflow that runs it can dispatch the next. Any other non-zero code is a failure.
RUN_AGAIN = 3

# The seconds between two requests of a sweep: about 0.8 a second. A sweep reads a page
# for each project on PyPI, close to a million, and at this pace the sweeps take about
# two weeks, where the kit's default of a request every 0.25 s would take a few days.
# The records pass reads about as many records, and goes at the same pace. The daily run
# keeps the kit's default.
SWEEP_INTERVAL = 1.25

# The repository that `main` publishes to when GITHUB_REPOSITORY does not say.
_REPOSITORY = "py-observatory/data"
_STREAM_TOML = Path(__file__).resolve().parents[2] / "stream.toml"
# The assets that carry the state and the open delta in the current release.
_STATE_ASSET = f"{STATE_DB}.zst"
_DELTA_ASSET = f"{OPEN_DELTA_DB}.zst"
# The condition on a project's status that picks the projects that are neither removed
# nor quarantined: the ones that the sweep reads and that the coverage counts. Its marks
# are filled with INACTIVE.
_ACTIVE = f"status NOT IN ({', '.join('?' * len(INACTIVE))})"
# The keys of the coverage that give the records pass's progress: the name of the last
# project that a records run came to, null before any records run and after one that
# came to the end of the list, and how many projects that may lack a record come after
# it.
_RECORDS_PROGRESS = ("records_after", "records_left")
# The slices of the most downloaded projects that the status pass reads, one in each
# daily run that reads its slice to the end. GitHub fires two or three of the tick's
# four scheduled runs a day, so each project is read again about once a week.
SLICES = 16
# The stream that publishes the set of the most downloaded projects, and the asset of
# its current release that holds the set, in its table top_set.
_DOWNLOADS = "pypi-downloads"
_SERVING_ASSET = "pypi-downloads-serving.db.zst"
# The names of the newest year's set, in name order.
_TOP_NAMES = """
SELECT name FROM top_set WHERE year = (SELECT max(year) FROM top_set) ORDER BY name
"""
# What makes the set unreadable, so that the status pass skips: a release that does not
# hold what its manifest lists, a failed download, a file that matches its digest but
# will not expand (cut short, which is an EOFError, or not zstd at all), a database
# whose set cannot be read, and a full disk. The catalog stages a stream on the same
# terms.
_TOP_NAMES_ERRORS = (
    PriorStateError,
    GitHubError,
    ZstdError,
    EOFError,
    ValueError,
    OSError,
)


class StaleRootError(RuntimeError):
    """The Simple API root is older than the state: its serial is below the cursor."""


@dataclass(frozen=True)
class StatusReport:
    """What the status pass of a daily run did.

    `slot` is the slice of the most downloaded projects that it read (see `in_slot`).
    `members` counts the projects of the set in that slice that the state holds and
    that are neither removed nor quarantined, `read` the pages of them that it asked
    for, and `changed` the `status_changed` events that it wrote. `skipped` says why
    the pass read nothing when the set could not be read, and is None otherwise.
    """

    slot: int
    members: int
    read: int
    changed: int
    skipped: str | None


@dataclass(frozen=True)
class RecordsReport:
    """What a records run did.

    `processed` counts the projects that it came to before its deadline. Of those,
    `recorded` got the record of their current release, `missing` are those whose
    record PyPI does not have (404 or 410), and `refused` those whose reply made no
    record that could be kept, for which nothing was written. The rest had nothing to
    ask for. `left` counts the projects after the last one that it came to that may
    still lack a record, and is 0 when it came to the end of the list.
    """

    processed: int
    recorded: int
    missing: int
    refused: int
    left: int


@dataclass(frozen=True)
class RunReport:
    """What a run did.

    The counts of projects and versions are those of `ProcessReport`, and a bootstrap,
    which processes no project, has none. A sweep counts only `versions_fetched`,
    `swept` and `remaining`: it does not sort projects into new, changed, vanished and
    returned, and its `remaining` is the number of projects that are still unswept. That
    is not the `remaining` of the manifest's coverage, which is always what the last
    daily run left to the next one. `requests` counts the requests to PyPI, the retries
    among them. `gate_failures` lists the failures that a person overrode, and is empty
    when no gate failed. `status` is what the status pass did, and is None for a
    bootstrap and a sweep, which make none.
    """

    new: int
    changed: int
    vanished: int
    returned: int
    versions_fetched: int
    requests: int
    swept: int
    remaining: int
    gate_failures: list[GateFailure]
    status: StatusReport | None = None


def _stream_toml() -> dict:
    """The stream's stream.toml: its upstreams and the history class of each table."""
    return tomllib.loads(_STREAM_TOML.read_text(encoding="utf-8"))


def _meta(state: sqlite3.Connection, key: str) -> str:
    """The value of `key` in the state's meta table. Raises ValueError when there is
    none: a bootstrap writes every key."""
    row = state.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    if row is None:
        raise ValueError(f"the state has no {key} in meta, so no bootstrap built it")
    return row[0]


def _apply(state: sqlite3.Connection, delta: sqlite3.Connection) -> None:
    """Apply the rows of `delta` above the state's applied_seq to the state, and store
    the highest seq applied as its applied_seq, as every run and every replay does."""
    applied = int(_meta(state, "applied_seq"))
    top = apply_delta(state, delta, after_seq=applied)
    state.execute("UPDATE meta SET value = ? WHERE key = 'applied_seq'", (str(top),))
    state.commit()


def replay(base: Path, deltas: list[Path]) -> None:
    """Apply each delta database of `deltas`, in order, to the state database at
    `base`, as the runs applied them, and leave `base` as the result.

    This rebuilds a day's state from the copy of the state as its month began and the
    month's deltas up to that day, in day order. A run checks each month that it
    ends this way. Raises as `store.apply_delta` does for a row that does not fit.
    """
    with closing(open_db(base, STATE_SCHEMA)) as state:
        for path in deltas:
            with closing(open_db(path, DELTA_SCHEMA)) as delta:
                _apply(state, delta)


def _carried(prior: Manifest) -> tuple[Asset, Asset]:
    """The entries of the state and of the open delta in the prior manifest.

    Raises PriorStateError unless the manifest lists both as state assets of the
    current release, which `fetch_prior` then downloaded, and gives the open delta its
    day. Otherwise the run would start from no state at all, or from an empty one.
    """
    tag = current_tag(STREAM)
    listed = {
        asset.name: asset
        for asset in prior.assets
        if asset.tag == tag and asset.role == "state"
    }
    missing = [name for name in (_STATE_ASSET, _DELTA_ASSET) if name not in listed]
    if missing:
        raise PriorStateError(
            f"the prior manifest lists no {' and no '.join(missing)} as a state asset "
            f"of {tag}, so there is no state to go on from"
        )
    delta = listed[_DELTA_ASSET]
    if delta.period is None or len(delta.period) != len("YYYY-MM-DD"):
        raise PriorStateError(
            f"the prior manifest gives {_DELTA_ASSET} the period {delta.period}, which "
            "is not a day, so the day that it is open for is not known"
        )
    return listed[_STATE_ASSET], delta


@dataclass(frozen=True)
class _Resume:
    """Where the prior state stands, for a run that goes on from it.

    `carried` is the entries of the state and of the open delta in the prior manifest.
    `cursor` is the serial of the root that the state last read, and `applied` is the
    highest `seq` that the state has applied.
    """

    carried: tuple[Asset, Asset]
    cursor: int
    applied: int


def _resume(prior: Manifest, workdir: Path) -> _Resume:
    """Check the state that `fetch_prior` left in `workdir` for the prior manifest
    `prior`, and read where it stands. Every run that goes on from a published state
    starts here, the sweep as well as the daily run.

    Raises PriorStateError unless `prior` lists the state and the open delta (see
    `_carried`). Both databases are opened without making them (`open_db` raises
    FileNotFoundError or ValueError for one that is missing or not of its schema), so a
    state that is not there is an error and never an empty one. Raises ValueError when
    the state lacks its cursor or its applied_seq.
    """
    carried = _carried(prior)
    with (
        closing(open_db(workdir / STATE_DB, STATE_SCHEMA)) as state,
        closing(open_db(workdir / OPEN_DELTA_DB, DELTA_SCHEMA)),
    ):
        cursor = int(_meta(state, "cursor"))
        applied = int(_meta(state, "applied_seq"))
    return _Resume(carried, cursor, applied)


def _backlog(prior: Manifest) -> int:
    """The number of projects that the last daily run left to the next one, which the
    coverage of the prior manifest gives as `remaining`. A sweep reads no root, so it
    has no such backlog of its own, and its manifest carries this one on as it is.

    Raises PriorStateError when the coverage does not give a count, because the sweep
    would otherwise publish a manifest that says nothing of the backlog, or one that it
    made up.
    """
    remaining = prior.coverage.get("remaining")
    if type(remaining) is not int:  # a bool is not a count, and nor is a missing value
        shown = "nothing" if remaining is None else f"a {type(remaining).__name__}"
        raise PriorStateError(
            "the coverage of the prior manifest must give `remaining` as a count, and "
            f"gives {shown}, so the backlog of the daily run is not known"
        )
    return remaining


def _reuse(workdir: Path, entry: Asset, name: str) -> Path:
    """Copy the file of `entry` in `workdir` to `workdir/name`, and return the copy.

    The copy goes into a create-once release for good, under an entry that says it is
    the entry's bytes, so it raises RuntimeError unless it is."""
    source = workdir / entry.name
    copy = workdir / name
    shutil.copyfile(source, copy)
    digest = sha256_file(copy)
    if digest != entry.asset_sha256:
        raise RuntimeError(
            f"{source} has the sha256 {digest}, but its entry gives "
            f"{entry.asset_sha256}"
        )
    return copy


def _seal(workdir: Path, delta: Asset, day: date) -> tuple[Path, Path, Asset]:
    """Seal the prior open delta, whose entry is `delta`, as the delta of `day`.

    Returns the file to add to the month's release, the database that it expands to,
    and its entry. The file is the one that `fetch_prior` downloaded and checked, and
    the entry keeps the prior entry's sizes, digests and tables. The open delta's
    database moves to the sealed name, which frees its own name for the next one.
    """
    name = delta_name(day)
    packed = _reuse(workdir, delta, f"{name}.zst")
    expanded = workdir / name
    os.replace(workdir / OPEN_DELTA_DB, expanded)
    sealed = replace(
        delta,
        name=packed.name,
        tag=month_tag(STREAM, day),
        role="public",
        period=day.isoformat(),
        closed=True,
    )
    return packed, expanded, sealed


def _base_copy(
    workdir: Path, state: Asset, month: tuple[int, int]
) -> tuple[Path, Asset]:
    """The copy of the state as `month` begins, from the state's own file, whose entry
    is `state`, as its file and its entry."""
    year, number = month
    packed = _reuse(workdir, state, f"{base_state_name(year, number)}.zst")
    base = replace(
        state,
        name=packed.name,
        tag=month_tag(STREAM, date(year, number, 1)),
        period=f"{year:04d}-{number:02d}",
        closed=True,
    )
    return packed, base


def _is_sealed_delta(asset: Asset) -> bool:
    """Whether `asset` is the sealed delta of the day that its period gives."""
    if asset.period is None or len(asset.period) != len("YYYY-MM-DD"):
        return False
    return asset.name == f"{delta_name(date.fromisoformat(asset.period))}.zst"


def _month_manifest(
    workdir: Path, prior: Manifest, month: tuple[int, int], assets: list[Asset]
) -> tuple[Path, Asset]:
    """The manifest of a month that has ended, as its file and its entry.

    It is the prior manifest, which describes the state as the month ended, listing
    `assets` in place of its own. It takes nothing from the run but the entry of the
    month's part, so a run that stops after adding it and runs again makes the same
    bytes when it builds the same part. Raises ManifestError unless it reads back as
    itself, as the strict reader reads it.
    """
    year, number = month
    manifest = replace(prior, assets=assets)
    text = dumps(manifest)
    if loads(text) != manifest:
        raise ManifestError(
            f"the manifest of {year:04d}-{number:02d} reads back from its JSON as "
            "another manifest"
        )
    path = workdir / month_manifest_name(year, number)
    path.write_text(text, encoding="utf-8")
    size = path.stat().st_size
    digest = sha256_file(path)
    entry = Asset(
        name=path.name,
        tag=month_tag(STREAM, date(year, number, 1)),
        role="public",
        format="json",
        asset_bytes=size,
        asset_sha256=digest,
        db_bytes=size,
        db_sha256=digest,
        tables={},
        period=f"{year:04d}-{number:02d}",
        closed=True,
    )
    return path, entry


def _log_free_disk(workdir: Path, when: str) -> None:
    """Write to stderr, on one line, how much space is free on the disk that holds
    `workdir`, so that the log of a run shows what a month close needs of it."""
    free = shutil.disk_usage(workdir).free
    print(f"{when}: {free / 1e9:.1f} GB free on {workdir}", file=sys.stderr)


def _close_month(
    gh: GitHub,
    workdir: Path,
    prior: Manifest,
    month: tuple[int, int],
    last: tuple[Path, Asset],
) -> list[tuple[Path, Asset]]:
    """Close `month`, whose last delta was just sealed from the prior open delta, as
    `last`: its database and its entry.

    The month's state is rebuilt from its base copy and every sealed delta of the
    month, in day order, as a restore would, and compared with the state that the
    runs left. Only when they are equal are the deltas merged into the month's part,
    and the month's manifest made. Returns the part and the manifest, each as its file
    and its entry, to add to the month's release. Nothing is uploaded here.

    A month of deltas takes much of a runner's disk, so each file goes as soon as it
    has served: the rebuilt base once it checks, and each delta, `last`'s database
    included, once the part holds its rows. A retry fetches them again. The space
    free on the disk is logged to stderr as the close starts and as it ends.

    Raises PriorStateError when the prior manifest lists no base copy of the month,
    or a copy does not download as its entry says, and GateError when the rebuilt
    state differs from the state. No override reason passes that failure: going on
    would publish over a month that its releases cannot restore.
    """
    year, number = month
    period = f"{year:04d}-{number:02d}"
    _log_free_disk(workdir, f"month close {period} starts")
    tag = month_tag(STREAM, date(year, number, 1))
    held = {asset.name: asset for asset in prior.assets if asset.tag == tag}
    base = held.get(f"{base_state_name(year, number)}.zst")
    if base is None:
        raise PriorStateError(
            f"the prior manifest lists no {base_state_name(year, number)}.zst in "
            f"{tag}, so the month cannot be rebuilt to check it"
        )
    days = sorted(
        (asset for asset in held.values() if _is_sealed_delta(asset)),
        key=lambda asset: asset.period or "",
    )
    base_db, *day_dbs = fetch_assets(gh, [base, *days], workdir)
    last_db, sealed = last
    deltas = [*day_dbs, last_db]
    replay(base_db, deltas)
    with closing(open_db(base_db, STATE_SCHEMA)) as rebuilt:
        differ = diff_states(rebuilt, workdir / STATE_DB)
    if differ:
        detail = (
            f"{period} rebuilt from {base.name} and its {len(deltas)} sealed deltas "
            f"differs from the state in {', '.join(differ)}. This cannot be "
            "overridden, because going on would publish over a month that its "
            "releases cannot restore"
        )
        raise GateError([GateFailure("replay", detail)])
    base_db.unlink()
    part_db = workdir / month_part_name(year, number)
    consolidate(deltas, part_db, remove_inputs=True)
    part = prepare_asset(
        part_db,
        workdir,
        tag=tag,
        role="public",
        classes=_stream_toml()["tables"],
        period=period,
        closed=True,
    )
    month_assets = [base, *days, sealed, part[1]]
    manifest = _month_manifest(workdir, prior, month, month_assets)
    _log_free_disk(workdir, f"month close {period} ends")
    return [part, manifest]


def _add_create_once(gh: GitHub, added: list[tuple[Path, Asset]]) -> None:
    """Add each file of `added` to the release that its entry names, in order."""
    by_tag: dict[str, list[Path]] = {}
    for path, asset in added:
        by_tag.setdefault(asset.tag, []).append(path)
    for tag, files in by_tag.items():
        add_create_once(gh, STREAM, tag, files)


def _roll_over(
    gh: GitHub,
    workdir: Path,
    prior: Manifest,
    carried: tuple[Asset, Asset],
    today: date,
) -> list[Asset]:
    """Do what the end of a day or a month asks for, before the run reads a project.
    Returns the entries of the assets that it added to the month releases.

    `carried` is the prior entries of the state and the open delta, whose files
    `fetch_prior` left in `workdir`. A new day seals the open delta and begins a new
    one in its place. A new month closes the month that ended, and copies the state
    as the base of the new one. Everything is checked and built before the first file
    is added, and a file that a release holds already, with the same bytes, is kept.
    """
    state, delta = carried
    plan = plan_rollover(date.fromisoformat(delta.period or ""), today)
    added: list[tuple[Path, Asset]] = []
    if plan.seal_day is not None:
        packed, expanded, sealed = _seal(workdir, delta, plan.seal_day)
        added.append((packed, sealed))
        if plan.close_month is not None:
            added += _close_month(
                gh, workdir, prior, plan.close_month, (expanded, sealed)
            )
        open_db(workdir / OPEN_DELTA_DB, DELTA_SCHEMA, create=True).close()
    if plan.new_base is not None:
        added.append(_base_copy(workdir, state, plan.new_base))
    _add_create_once(gh, added)
    return [asset for _, asset in added]


def _removals(delta: sqlite3.Connection, *, after_seq: int) -> int:
    """The `removed` events that the run wrote: those above `after_seq`."""
    (count,) = delta.execute(
        "SELECT count(*) FROM events WHERE kind = 'removed' AND seq > ?", (after_seq,)
    ).fetchone()
    return count


def _status_slice(coverage: dict) -> int:
    """The slice that the next status pass reads, as `coverage` gives it as
    `status_slice`. It is the first when the coverage gives none, as before the first
    status pass, or gives a value that is not a slice."""
    value = coverage.get("status_slice")
    return value if type(value) is int and 0 <= value < SLICES else 0


def _progress(prior: Manifest | None) -> dict:
    """Where the records pass and the status pass stand, as the coverage of the prior
    manifest gives it, for a run that carries it on. A records key that the coverage
    lacks, as a manifest from before any records run does, is null: nothing is known of
    what is left. The status pass's slice is the first when the coverage gives none
    (see `_status_slice`)."""
    coverage = prior.coverage if prior else {}
    return {
        **{key: coverage.get(key) for key in _RECORDS_PROGRESS},
        "status_slice": _status_slice(coverage),
    }


def _coverage(state: sqlite3.Connection, remaining: int, progress: dict) -> dict:
    """What the state covers. `projects` counts the projects that are neither removed
    nor quarantined, which are the ones that the sweep reads, and `swept` and
    `unswept` split them by whether their page has been read in full. `remaining`
    counts the projects that the last daily run left to the next one, which a sweep
    carries on, and `bootstrap_at` is when the state was first built. `progress` is
    where the records pass and the status pass stand: the records pass's place and what
    it has left after it, which a records run sets, and the slice that the next status
    pass reads, which a daily run sets. Every other run carries each on (see
    `_progress`)."""
    projects, swept = state.execute(
        f"SELECT count(*), coalesce(sum(swept), 0) FROM projects WHERE {_ACTIVE}",
        INACTIVE,
    ).fetchone()
    return {
        "projects": projects,
        "swept": swept,
        "unswept": projects - swept,
        "remaining": remaining,
        "bootstrap_at": _meta(state, "bootstrap_at"),
        **progress,
    }


def _apply_run(
    state: sqlite3.Connection,
    conn: sqlite3.Connection,
    *,
    applied: int,
    remaining: int,
    progress: dict,
) -> tuple[int, dict]:
    """Commit what a run wrote to its open delta, `conn`, and apply it to `state`.

    `applied` is the highest `seq` that the state had applied when the run began, so
    the rows above it are the run's. Returns the number of `removed` events among them,
    and what the state covers once they are applied (see `_coverage`), where `remaining`
    is the projects that the last daily run left to the next one: this run's own for a
    daily run, and the prior manifest's for a sweep and a records run. `progress` is
    where the records pass and the status pass stand: a records run gives its own place
    in the records pass, a daily run its own slice of the status pass, and each gives
    the prior manifest's for the other.
    """
    conn.commit()
    removals = _removals(conn, after_seq=applied)
    _apply(state, conn)
    return removals, _coverage(state, remaining, progress)


def _prepare_current(workdir: Path, today: date) -> tuple[list[Path], list[Asset]]:
    """Compress the state and the open delta, which is today's, for the current
    release. Returns their files and their entries."""
    classes = _stream_toml()["tables"]
    tag = current_tag(STREAM)
    state = prepare_asset(
        workdir / STATE_DB, workdir, tag=tag, role="state", classes=classes
    )
    delta = prepare_asset(
        workdir / OPEN_DELTA_DB,
        workdir,
        tag=tag,
        role="state",
        classes=classes,
        period=today.isoformat(),
    )
    return [state[0], delta[0]], [state[1], delta[1]]


def _manifest(
    repo: str,
    prior: Manifest | None,
    assets: list[Asset],
    *,
    data_through: Watermark,
    cursor: str | None,
    coverage: dict,
    moment: str,
) -> Manifest:
    """The manifest of a publish made at `moment`. `data_through` and `cursor` say how
    far the data reaches and which root was read last. A run that read a root gives its
    serial for both, and one that read none, like the sweep, gives those of the prior
    manifest. The data is complete when no project is left to the next daily run
    (`remaining`) and none is left unswept (`unswept`), whichever run it is. The logs of
    series breaks, corrections and overrides go on from the prior manifest."""
    return Manifest(
        stream=STREAM,
        producer=producer(repo),
        generated_at=moment,
        complete=coverage["remaining"] == 0 and coverage["unswept"] == 0,
        coverage=coverage,
        data_through=data_through,
        cursor=cursor,
        licences=_stream_toml()["upstreams"],
        assets=assets,
        series_breaks=list(prior.series_breaks) if prior else [],
        corrections=list(prior.corrections) if prior else [],
        overrides=list(prior.overrides) if prior else [],
    )


def _gate(
    prior: Manifest,
    manifest: Manifest,
    *,
    files_removed: int,
    removals: int,
    override_reason: str | None,
    moment: str,
) -> tuple[Manifest, list[GateFailure]]:
    """Check `manifest` against `prior` with the publish gates, and with the limit on
    removals. Returns the manifest to publish and the failures.

    `files_current` may shrink by the `files_removed` that the run's removals explain.
    With no failure, the manifest is returned as it is. A failure raises GateError,
    unless `override_reason` gives a person's reason to publish anyway: then the
    manifest's overrides log the time, the reason and the failures.
    """
    failures = check(prior, manifest, allowed_shrink={"files_current": files_removed})
    if removals > MAX_REMOVALS:
        detail = f"{removals:,} removed events in one run (limit {MAX_REMOVALS:,})"
        failures.append(GateFailure("removals", detail))
    if not failures:
        return manifest, []
    if not override_reason:
        raise GateError(failures)
    entry = {
        "at": moment,
        "reason": override_reason,
        "failures": [asdict(failure) for failure in failures],
    }
    return replace(manifest, overrides=[*manifest.overrides, entry]), failures


def _publish(
    gh: GitHub,
    workdir: Path,
    prior: Manifest,
    added: list[Asset],
    *,
    today: date,
    moment: str,
    data_through: Watermark,
    cursor: str | None,
    coverage: dict,
    files_removed: int,
    removals: int,
    override_reason: str | None,
) -> list[GateFailure]:
    """Publish the state that a run left in `workdir`: the end of every run that goes
    on from a published state, the sweep's as well as the daily run's.

    It compresses the state and the open delta, writes the manifest (see `_manifest`)
    from them, the closed assets of `prior` and the `added` entries of the rollover,
    checks it against `prior` (see `_gate`), and swaps the files and the manifest into
    the current release. Returns the failures that `override_reason` overrode, and
    raises GateError for any that it did not, before anything is published.
    """
    files, current = _prepare_current(workdir, today)
    closed = [asset for asset in prior.assets if asset.closed]
    manifest = _manifest(
        gh.repo,
        prior,
        [*current, *closed, *added],
        data_through=data_through,
        cursor=cursor,
        coverage=coverage,
        moment=moment,
    )
    manifest, failures = _gate(
        prior,
        manifest,
        files_removed=files_removed,
        removals=removals,
        override_reason=override_reason,
        moment=moment,
    )
    publish_current(gh, STREAM, files, manifest, bootstrap=False)
    return failures


def _report(
    processed: ProcessReport,
    *,
    requests: int,
    failures: list[GateFailure],
    status: StatusReport | None = None,
) -> RunReport:
    """The report of a run that processed projects: the counts of `processed`, the
    `requests` that it made, the gate `failures` that were overridden and what its
    status pass did, when it made one."""
    return RunReport(
        new=processed.new,
        changed=processed.changed,
        vanished=processed.vanished,
        returned=processed.returned,
        versions_fetched=processed.versions_fetched,
        requests=requests,
        swept=processed.swept,
        remaining=processed.remaining,
        gate_failures=failures,
        status=status,
    )


def _bootstrap(
    gh: GitHub, http: Http, workdir: Path, *, moment: str, today: date, requests: int
) -> RunReport:
    """Build the state from the root, as the stream's first state, and publish it.

    Every project of the root is in it, at incarnation 1 and the root's serial,
    active, not swept, and first seen at `moment`, which is not when it first
    appeared. The state's cursor is the root's serial, and nothing has been applied.
    No event is written, and the open delta is empty. The state is copied as the base
    of today's month, and then published with the open delta and the manifest.
    `requests` is the count of requests to PyPI when the run began.
    """
    root = get_root(http)
    with closing(open_db(workdir / STATE_DB, STATE_SCHEMA, create=True)) as state:
        state.executemany(
            "INSERT INTO projects (name, incarnation, last_serial, status, swept, "
            "first_seen, first_seen_exact) VALUES (?, 1, ?, 'active', 0, ?, 0)",
            [(name, serial, moment) for name, serial in root.projects.items()],
        )
        meta = [
            ("applied_seq", "0"),
            ("bootstrap_at", moment),
            ("cursor", str(root.serial)),
        ]
        state.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta)
        state.commit()
        coverage = _coverage(state, remaining=0, progress=_progress(None))
    open_db(workdir / OPEN_DELTA_DB, DELTA_SCHEMA, create=True).close()
    files, current = _prepare_current(workdir, today)
    # The first run starts today's month with a base copy, as plan_rollover says.
    base = _base_copy(workdir, current[0], (today.year, today.month))
    _add_create_once(gh, [base])
    # A first publish has nothing to fall short of, so no gate can fail.
    manifest = _manifest(
        gh.repo,
        None,
        [*current, base[1]],
        data_through=Watermark(str(root.serial), "serial"),
        cursor=str(root.serial),
        coverage=coverage,
        moment=moment,
    )
    publish_current(gh, STREAM, files, manifest, bootstrap=True)
    return RunReport(
        new=0,
        changed=0,
        vanished=0,
        returned=0,
        versions_fetched=0,
        requests=http.requests - requests,
        swept=0,
        remaining=0,
        gate_failures=[],
    )


# ---- the status pass


def in_slot(name: str, slot: int) -> bool:
    """Whether the project `name` is in the slice `slot` of the status pass: the first
    eight bytes of the sha256 of its name, as a number, modulo SLICES. The hash spreads
    the names evenly over the slices, and a name keeps its slice from run to run and
    from one year's set to the next."""
    return int.from_bytes(sha256(name.encode()).digest()[:8], "big") % SLICES == slot


def _read_top_names(path: Path) -> list[str]:
    """The names of the newest year's set in the serving database at `path`, in name
    order. The database is data from outside, so it is opened read-only and read with
    plain SQL. Raises ValueError for a file that SQLite cannot read, for one with no
    table of the sets, and for a name that is not text."""
    try:
        with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as db:
            names = [name for (name,) in db.execute(_TOP_NAMES)]
    except sqlite3.Error as error:
        raise ValueError(
            f"{path.name} cannot be read for the most downloaded projects: {error}"
        ) from error
    if not all(isinstance(name, str) for name in names):
        raise ValueError(f"the top_set of {path.name} holds a name that is not text")
    return names


def top_names(gh: GitHub, workdir: Path) -> list[str]:
    """The names of the newest year's set of the most downloaded projects, which the
    downloads stream publishes in its serving database in `pypi-downloads-current`.
    The set is empty until that stream's first month close.

    The manifest and the database go to a new directory in `workdir`, which is removed
    before this returns, and the database is checked against its entry (see
    `fetch_assets`). Raises PriorStateError when the stream has not published, when its
    manifest lists no serving database, and when the file does not match its entry,
    GitHubError for a request that failed, ZstdError or EOFError for a file that matches
    its entry but does not expand, OSError when the file cannot be written, and
    ValueError for a manifest that cannot be read and a database whose set cannot be
    read (see `_read_top_names`).
    """
    folder = Path(tempfile.mkdtemp(prefix="downloads-", dir=workdir))
    try:
        manifest = fetch_manifest(gh, _DOWNLOADS, folder)
        asset = current_asset(manifest, _SERVING_ASSET)
        if asset is None:
            raise PriorStateError(
                f"the manifest of {_DOWNLOADS} lists no {_SERVING_ASSET} in "
                f"{current_tag(_DOWNLOADS)}"
            )
        (path,) = fetch_assets(gh, [asset], folder)
        return _read_top_names(path)
    finally:
        shutil.rmtree(folder)


def _read_by_run(conn: sqlite3.Connection, *, after_seq: int) -> set[str]:
    """The projects that the run has written a serial or an event for, in the rows of
    the open delta `conn` above `after_seq`: it has read their pages already."""
    rows = conn.execute(
        "SELECT name FROM serials WHERE seq > ? UNION SELECT name FROM events "
        "WHERE seq > ?",
        (after_seq, after_seq),
    )
    return {name for (name,) in rows}


def _slice_members(
    state: sqlite3.Connection, names: list[str], slot: int
) -> list[tuple[str, int, int, str, bool]]:
    """The projects of `names` in the slice `slot` that the state holds and that are
    neither removed nor quarantined, in name order, each with its incarnation, serial,
    status and whether a run has read its page in full. A name that the state does not
    hold is left out."""
    members = []
    for name in sorted(names):
        if not in_slot(name, slot):
            continue
        row = state.execute(
            "SELECT incarnation, last_serial, status, swept FROM projects "
            "WHERE name = ?",
            (name,),
        ).fetchone()
        if row is not None and row[2] not in INACTIVE:
            incarnation, serial, status, swept = row
            members.append((name, incarnation, serial, status, bool(swept)))
    return members


def _status_pass(
    gh: GitHub,
    workdir: Path,
    ctx: Context,
    *,
    slot: int,
    read_by_run: set[str],
    deadline: float,
    clock: Callable[[], float],
) -> tuple[StatusReport, bool]:
    """Re-read the pages of the projects of the slice `slot` of the most downloaded
    projects (see `top_names` and `_slice_members`), one at a time until `clock()`
    reaches `deadline`, and write a `status_changed` event for each whose status has
    changed (see `process.process_status`). Returns the report, and whether the pass
    read the slice to its end.

    Archiving a project moves no serial, so the root never shows it, and this pass is
    what sees a project become archived or stop being archived. It compares a page only
    when nothing else could have changed: the rest is left to the daily run, which reads
    the files and the serial with the status. So it leaves a page that is missing (404
    or 410), a page whose serial is not the stored one (a newer page, which the root
    will show, or an old copy), and a quarantined page, which the daily run probes when
    the root stops listing the project. It asks for no page of a project in
    `read_by_run`, which the run has read already, nor of one that no run has read in
    full: its status was never read, so a change of it cannot be told, and the sweep
    records it with the rest of the page.

    A set that cannot be read skips the pass, and the run goes on without it: the
    report says why, and the slice is read by the next run. A page that fails or does
    not parse raises, as in the rest of the run.
    """
    try:
        names = top_names(gh, workdir)
    except _TOP_NAMES_ERRORS as error:
        report = StatusReport(
            slot=slot, members=0, read=0, changed=0, skipped=str(error)
        )
        return report, False
    members = _slice_members(ctx.state, names, slot)
    read = changed = 0
    finished = True
    for name, incarnation, serial, status, swept in members:
        if name in read_by_run or not swept:
            continue
        if clock() >= deadline:
            finished = False
            break
        _, page = get_page(ctx.http, name)
        read += 1
        if page is None or page.serial != serial or page.status == "quarantined":
            continue
        if process_status(ctx, name, incarnation, status, page):
            changed += 1
    report = StatusReport(
        slot=slot, members=len(members), read=read, changed=changed, skipped=None
    )
    return report, finished


def run_once(
    gh: GitHub,
    http: Http,
    workdir: Path,
    *,
    now: datetime,
    bootstrap: bool,
    override_reason: str | None = None,
    max_seconds: float = 18_000,
    clock: Callable[[], float] = time.monotonic,
) -> RunReport:
    """One run of the stream at `now`, in the empty directory `workdir`.

    It reads the prior state (`fetch_prior`), which only `bootstrap` may find missing,
    and then a bootstrap builds and publishes the first state. Otherwise the prior
    manifest must list the state and the open delta (PriorStateError). The run reads
    the root, and raises StaleRootError, before it writes anything, for a root whose
    serial is below the state's cursor. It rolls over (see `_roll_over`), processes
    the projects that the root gives work for until `max_seconds` from its start on
    `clock` have passed, re-reads the status of one slice of the most downloaded
    projects in what is left of that time (see `_status_pass`), writes the root's
    serial to the delta as the cursor, and applies the delta to the state. It then
    writes the manifest, checks it with the gates (see `_gate`), and swaps the state,
    the open delta and the manifest into the current release. The manifest carries the
    records pass's progress on from the prior one (see `records`).

    The slice is the one that the prior manifest's coverage gives as `status_slice`,
    and the manifest gives the next one when the pass read it to its end. A pass that
    skips, because the set of the most downloaded projects cannot be read, or that
    stops at the deadline, leaves the slice for the next run.

    Every error propagates but a set that cannot be read, and an error before the
    publish publishes nothing to the current release. What a rollover already added to
    a month's release stays. The next run adds the same files when it builds the same
    month part, which takes the same zstd and SQLite, and otherwise stops with
    CreateOnceError.
    """
    deadline = clock() + max_seconds
    requests = http.requests
    moment = instant(now)
    today = now.astimezone(UTC).date()
    prior = fetch_prior(gh, STREAM, workdir, bootstrap=bootstrap)
    if prior is None:
        return _bootstrap(
            gh, http, workdir, moment=moment, today=today, requests=requests
        )
    resume = _resume(prior, workdir)
    root = get_root(http)
    if root.serial < resume.cursor:
        raise StaleRootError(
            f"the root's serial {root.serial} is below the stored cursor "
            f"{resume.cursor}, so it is a stale copy, and nothing was written"
        )
    added = _roll_over(gh, workdir, prior, resume.carried, today)
    with (
        closing(open_db(workdir / STATE_DB, STATE_SCHEMA)) as state,
        closing(open_db(workdir / OPEN_DELTA_DB, DELTA_SCHEMA)) as conn,
    ):
        # A new day's delta is numbered on from what the state has applied, and
        # today's goes on after its own last row.
        delta = Delta(conn, moment, first_seq=resume.applied + 1)
        processed = process_projects(
            state, delta, http, root, deadline=deadline, clock=clock
        )
        progress = _progress(prior)
        slot = progress["status_slice"]
        status, finished = _status_pass(
            gh,
            workdir,
            Context(state, delta, http, bootstrap_at(state)),
            slot=slot,
            read_by_run=_read_by_run(conn, after_seq=resume.applied),
            deadline=deadline,
            clock=clock,
        )
        if finished:
            progress["status_slice"] = (slot + 1) % SLICES
        delta.cursor(root.serial, moment)
        removals, coverage = _apply_run(
            state,
            conn,
            applied=resume.applied,
            remaining=processed.remaining,
            progress=progress,
        )
    failures = _publish(
        gh,
        workdir,
        prior,
        added,
        today=today,
        moment=moment,
        data_through=Watermark(str(root.serial), "serial"),
        cursor=str(root.serial),
        coverage=coverage,
        files_removed=processed.files_removed,
        removals=removals,
        override_reason=override_reason,
    )
    return _report(
        processed, requests=http.requests - requests, failures=failures, status=status
    )


def _sweep_projects(
    state: sqlite3.Connection,
    delta: Delta,
    http: Http,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> ProcessReport:
    """Read the page of each project that is not swept yet, in full, until `clock()`
    reaches `deadline`, writing what it finds to `delta`.

    The projects are the ones that are neither removed nor quarantined and that no run
    has read in full, taken by name. Each page goes through `process.process_unswept`,
    which writes the files that the state lacks, the versions, the release records of
    the versions with a file from after the bootstrap, and a swept row that carries the
    status of the page. That is all that is written. There is no event, because the
    page is read as it is now, and nothing says when it changed. No file is exact: the
    sweep comes to a project when its turn is, so it was not there to see a file when
    it was uploaded, whether that was before the bootstrap or after. There is no serial,
    because no root is read: a project keeps the serial that it has, so a daily run that
    later finds it changed reads its page again, and writes only what the page has
    gained since.

    A page that cannot be used is skipped, with nothing written, and its project stays
    unswept:

    - a page that is not there (404 or 410). The daily run probes the project when the
      root stops listing it, and records its removal then;
    - a quarantined page, which lists no files and so says nothing of the ones that the
      state holds. The daily run probes the project in the same way, and records the
      quarantine;
    - a page whose serial is below the stored one, which is an old copy of the page.

    The next sweep asks for each of them again. A project is processed whole or not at
    all: the deadline is checked between projects. The state is read and never written,
    and the delta is not committed. `remaining` counts the projects that are still
    unswept, those that the deadline cut off and those that were skipped.
    """
    ctx = Context(state, delta, http, bootstrap_at(state))
    unswept = state.execute(
        "SELECT name, incarnation, last_serial FROM projects "
        f"WHERE swept = 0 AND {_ACTIVE} ORDER BY name",
        INACTIVE,
    ).fetchall()
    for name, incarnation, stored_serial in unswept:
        if clock() >= deadline:
            break
        _, page = get_page(http, name)
        if page is None or page.status == "quarantined" or page.serial < stored_serial:
            continue
        process_unswept(ctx, name, incarnation, page, exact=False)
    return ProcessReport(
        new=0,
        changed=0,
        vanished=0,
        returned=0,
        versions_fetched=ctx.versions_fetched,
        swept=ctx.swept,
        remaining=len(unswept) - ctx.swept,
        files_removed=0,
    )


def sweep(
    gh: GitHub,
    http: Http,
    workdir: Path,
    *,
    now: datetime,
    max_seconds: float = 16_200,
    clock: Callable[[], float] = time.monotonic,
) -> RunReport:
    """One sweep of the stream at `now`, in the empty directory `workdir`.

    It goes on from the last publish as a run does that is not a bootstrap, and reads no
    root, so it takes the state as it is. The stream must have published already, and
    its manifest must list the state and the open delta (PriorStateError): the sweep
    never builds a first state. It rolls over (see `_roll_over`), reads the page of each
    project that is not swept yet until `max_seconds` from its start on `clock` have
    passed (see `_sweep_projects`), and applies the delta to the state. It then writes
    the manifest, checks it with the gates, and swaps the state, the open delta and the
    manifest into the current release, as `run_once` does. The manifest keeps the
    `data_through`, the `cursor` and the backlog of the prior one, because no root was
    read: the coverage's `remaining` stays what the last daily run left to the next one
    (`_backlog`), and its `unswept` is how many projects the sweep has left. The data is
    complete when both are 0. The records pass's progress and the status pass's slice
    are carried on as well.

    The report's `swept` is the number of projects that this sweep read, and its
    `remaining` is the number that are still unswept. Each sweep reads what it has the
    time for, and the next goes on, so the sweep is run again while `remaining` is not
    0 (see `main`).

    A gate failure raises GateError. The sweep takes no override reason: a held publish
    is overridden by the daily run, which does. Every other error propagates as well,
    and an error before the publish publishes nothing to the current release, so the
    work of that sweep is lost, and the next one does it again.
    """
    deadline = clock() + max_seconds
    requests = http.requests
    moment = instant(now)
    today = now.astimezone(UTC).date()
    prior = fetch_prior(gh, STREAM, workdir, bootstrap=False)
    if prior is None:
        raise PriorStateError(
            f"{gh.repo} has no published state to sweep: a stream's first state is the "
            "bootstrap of the daily run"
        )
    resume = _resume(prior, workdir)
    backlog = _backlog(prior)
    added = _roll_over(gh, workdir, prior, resume.carried, today)
    with (
        closing(open_db(workdir / STATE_DB, STATE_SCHEMA)) as state,
        closing(open_db(workdir / OPEN_DELTA_DB, DELTA_SCHEMA)) as conn,
    ):
        delta = Delta(conn, moment, first_seq=resume.applied + 1)
        processed = _sweep_projects(state, delta, http, deadline=deadline, clock=clock)
        removals, coverage = _apply_run(
            state,
            conn,
            applied=resume.applied,
            remaining=backlog,
            progress=_progress(prior),
        )
    failures = _publish(
        gh,
        workdir,
        prior,
        added,
        today=today,
        moment=moment,
        data_through=prior.data_through,
        cursor=prior.cursor,
        coverage=coverage,
        files_removed=processed.files_removed,
        removals=removals,
        override_reason=None,
    )
    return _report(processed, requests=http.requests - requests, failures=failures)


# The projects that the records pass comes to, in name order: those that are neither
# removed nor quarantined, whose page a run has read in full, and that hold a version of
# their incarnation without a record. Only the names after the pass's place are taken,
# and every name when it has none. The marks are filled with INACTIVE, then the place
# twice.
_LACKING_RECORDS = f"""
SELECT name, incarnation FROM projects
WHERE swept = 1 AND {_ACTIVE} AND (? IS NULL OR name > ?)
AND EXISTS (
    SELECT 1 FROM versions_seen
    WHERE versions_seen.name = projects.name
    AND versions_seen.incarnation = projects.incarnation
    AND versions_seen.record = 0
)
ORDER BY name
"""


def _record_projects(
    state: sqlite3.Connection,
    delta: Delta,
    http: Http,
    *,
    after: str | None,
    deadline: float,
    clock: Callable[[], float],
) -> tuple[RecordsReport, str | None]:
    """Come to each project that may lack the record of its current release, in name
    order after `after`, until `clock()` reaches `deadline`, and write the record when
    it does lack it (see `process.record_current`). Returns the report, and the name of
    the last project that it came to, or None when it came to none.

    A project is a candidate while any version of it lacks a record, and most keep an
    old version without one for good, so the pass comes to most projects only to find
    nothing to ask for, which costs no request. A project whose reply is refused is
    processed too: nothing is written for it, and the place moves past it. A project is
    processed whole or not at all: the deadline is checked between projects. The state
    is read and never written, and the delta is not committed.
    """
    ctx = Context(state, delta, http, bootstrap_at(state))
    lacking = state.execute(_LACKING_RECORDS, (*INACTIVE, after, after)).fetchall()
    outcomes: Counter[str] = Counter()
    last = None
    for name, incarnation in lacking:
        if clock() >= deadline:
            break
        outcomes[record_current(ctx, name, incarnation)] += 1
        last = name
    processed = outcomes.total()
    report = RecordsReport(
        processed=processed,
        recorded=outcomes["recorded"],
        missing=outcomes["missing"],
        refused=outcomes["refused"],
        left=len(lacking) - processed,
    )
    return report, last


def records(
    gh: GitHub,
    workdir: Path,
    *,
    max_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    http: Http | None = None,
    now: datetime | None = None,
) -> RecordsReport:
    """One run of the records pass, in the empty directory `workdir`, at `now`, or at
    the present when it is not given. It fetches with `http`, and when that is not given
    with a client paced as the sweep's is (SWEEP_INTERVAL).

    It goes on from the last publish as the sweep does, and reads no root. The stream
    must have published already, and its manifest must list the state and the open
    delta (PriorStateError). It rolls over (see `_roll_over`), comes to each project
    that may lack the record of its current release, in name order after the place
    where the last records run stopped, until `max_seconds` from its start on `clock`
    have passed (see `_record_projects`), and applies the delta. It then publishes as
    the sweep does, and its manifest keeps the `data_through`, the `cursor` and the
    backlog of the prior one, because no root was read, and the status pass's slice.

    Its coverage gives the pass's place, `records_after`: the last project that it came
    to, or null when it came to the end of the list, so that the next records run starts
    again from the first name. Until then a project whose record PyPI does not have, or
    whose reply the run refused, is not asked for again. `records_left` is the report's
    `left`. Every other run carries both on from the prior manifest.

    A gate failure raises GateError. The run takes no override reason, as the sweep
    takes none. Every other error propagates as well, a failed request among them, and
    an error before the publish publishes nothing to the current release, so the next
    run does that work again. A reply that makes no record is not an error here: it is
    counted as `refused` (see `process.record_current`).
    """
    deadline = clock() + max_seconds
    if http is None:
        http = Http(interval=SWEEP_INTERVAL)
    if now is None:
        now = datetime.now(UTC)
    moment = instant(now)
    today = now.astimezone(UTC).date()
    prior = fetch_prior(gh, STREAM, workdir, bootstrap=False)
    if prior is None:
        raise PriorStateError(
            f"{gh.repo} has no published state to read records for: a stream's first "
            "state is the bootstrap of the daily run"
        )
    resume = _resume(prior, workdir)
    backlog = _backlog(prior)
    progress = _progress(prior)
    after = progress["records_after"]
    added = _roll_over(gh, workdir, prior, resume.carried, today)
    with (
        closing(open_db(workdir / STATE_DB, STATE_SCHEMA)) as state,
        closing(open_db(workdir / OPEN_DELTA_DB, DELTA_SCHEMA)) as conn,
    ):
        delta = Delta(conn, moment, first_seq=resume.applied + 1)
        report, last = _record_projects(
            state, delta, http, after=after, deadline=deadline, clock=clock
        )
        # The end of the list leaves no place, so that the next run starts again from
        # the first name. A run that came to no project keeps the place it had.
        if not report.left:
            place = None
        else:
            place = last if last is not None else after
        removals, coverage = _apply_run(
            state,
            conn,
            applied=resume.applied,
            remaining=backlog,
            progress={**progress, "records_after": place, "records_left": report.left},
        )
    _publish(
        gh,
        workdir,
        prior,
        added,
        today=today,
        moment=moment,
        data_through=prior.data_through,
        cursor=prior.cursor,
        coverage=coverage,
        files_removed=0,
        removals=removals,
        override_reason=None,
    )
    return report


def main(argv: list[str] | None = None) -> int:
    """Run the stream from the command line: `python -m pypi_registry run`, `python -m
    pypi_registry sweep` or `python -m pypi_registry records`.

    It needs GITHUB_TOKEN, and publishes to GITHUB_REPOSITORY, or to
    py-observatory/data when that is not set. It makes one GitHub client, whose
    writes are paced, for every call of the run, and works in a new directory in the
    folder that `--workdir` names (./work by default), which is removed afterwards.
    SQLite's temporary files go in that folder too, unless SQLITE_TMPDIR is set. It
    prints the report as JSON. Every error propagates. A sweep and a records run wait
    SWEEP_INTERVAL seconds between requests, and a run waits for as long as the kit's
    `Http` does by default.

    `run` returns 0. `sweep` returns 3, when it has swept some projects and others
    are left, to say that it is to be run again, and 0 otherwise. A sweep that reads
    nothing while projects are left has met only pages that it cannot use, which a
    sweep that followed at once would meet as well, so it returns 0 and ends the chain
    that runs it again. `records` returns 3 in the same way, when it came to some
    projects and others are left, and 0 otherwise: one that came to none made no
    progress, so it ends the chain too.
    """
    parser = argparse.ArgumentParser(
        prog="python -m pypi_registry", description="The pypi-registry stream."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser(
        "run", help="read what changed on PyPI since the last run, and publish it"
    )
    run.add_argument(
        "--bootstrap",
        action="store_true",
        help="build the first state: only for a stream that has never published",
    )
    run.add_argument(
        "--override-reason",
        default="",
        help="publish although a gate failed (except a failed month close), for this "
        "reason, which the manifest logs",
    )
    run.add_argument(
        "--max-seconds",
        type=float,
        default=18_000,
        help="stop starting projects after this many seconds (default: 18000)",
    )
    sweeping = commands.add_parser(
        "sweep",
        help="read the page of each project that no run has read in full, and publish "
        "what they hold",
    )
    sweeping.add_argument(
        "--max-seconds",
        type=float,
        default=16_200,
        help="stop starting projects after this many seconds (default: 16200)",
    )
    recording = commands.add_parser(
        "records",
        help="read the record of each project's current release that the registry "
        "lacks, and publish them",
    )
    recording.add_argument(
        "--max-seconds",
        type=float,
        default=16_200,
        help="stop starting projects after this many seconds (default: 16200)",
    )
    for command in (run, sweeping, recording):
        command.add_argument(
            "--workdir",
            type=Path,
            default=Path("work"),
            metavar="PATH",
            help="the folder to work in, on a disk with room for a month close; each "
            "run makes a directory of its own there and removes it (default: work)",
        )
    args = parser.parse_args(argv)
    gh = GitHub(
        os.environ.get("GITHUB_REPOSITORY", _REPOSITORY), os.environ["GITHUB_TOKEN"]
    )
    work = args.workdir.absolute()
    work.mkdir(exist_ok=True)
    # SQLite writes its temporary files, VACUUM's copy of a whole database among them,
    # to SQLITE_TMPDIR, and otherwise to /var/tmp or /tmp, which may be on another disk.
    # Set before any SQLite work, it keeps them on the work's disk, and so in the free
    # disk that a month close logs. A folder that is set already is kept.
    os.environ.setdefault("SQLITE_TMPDIR", str(work.resolve()))
    report: RunReport | RecordsReport
    with tempfile.TemporaryDirectory(prefix="run-", dir=work) as workdir:
        if args.command == "sweep":
            report = sweep(
                gh,
                Http(interval=SWEEP_INTERVAL),
                Path(workdir),
                now=datetime.now(UTC),
                max_seconds=args.max_seconds,
            )
            again = bool(report.swept and report.remaining)
        elif args.command == "records":
            report = records(gh, Path(workdir), max_seconds=args.max_seconds)
            again = bool(report.processed and report.left)
        else:
            report = run_once(
                gh,
                Http(),
                Path(workdir),
                now=datetime.now(UTC),
                bootstrap=args.bootstrap,
                override_reason=args.override_reason or None,
                max_seconds=args.max_seconds,
            )
            again = False
    print(json.dumps(asdict(report)))
    return RUN_AGAIN if again else 0
