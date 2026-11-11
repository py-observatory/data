"""The day and month release cycle of an event-sourced stream, which a `Store` fits to
the stream's own databases.

Such a stream writes what each run learns into a delta database, and applies the delta
to its state database, so the state is only what the deltas say. `<stream>-current`
holds the state and the open delta, the delta of the day of the last run, which each
run of that day adds to. Each month has a create-once release, `<stream>-month-YYYY-MM`,
which holds a copy of the state as the month began, the sealed delta of each day of the
month that had a run, and, once the month is over, the month's part (its deltas merged
into one database) and the month's manifest. The names here are of the databases. A
release asset adds `.zst` to the name, except for the month's manifest, which is JSON.

A run rolls over (`roll_over`) when a day or a month has ended since the open delta
began. The open delta is sealed into the month's release as the delta of its day, and
a new one begins. When the month has ended, the run also rebuilds the month's state
from its base copy and its sealed deltas, as a restore would, and refuses to go on
unless that equals the state. Only then does it merge the deltas into the month's part,
and copy the state as the base of the new month.

The assets of a month's release are never replaced. The sealed delta and the base copy
are the files that the last run published, byte for byte: compressing again gives the
same bytes only with the same zstd and SQLite, which can change between two attempts.
The month's part is built afresh from the month's deltas, and the month's manifest
takes nothing from the run but the part's entry. So a run that stops after a rollover
and runs again adds the same files only when it has the same zstd and SQLite. With
another build its part is other bytes, and it stops with CreateOnceError for a person
to look at.

Once the run has written what it learned to the open delta and applied it, it
publishes (`publish`). The state and the open delta are compressed, and the manifest
lists them with every closed asset of the month releases. The gates check it against
the last manifest, and then the files and the manifest are swapped into
`<stream>-current`. A stream's first run publishes with `publish_first` instead, which
begins the month with a base copy and has no manifest to check against.

The gates hold back a publish that has less history than the last one, and a stream
can hold one back for reasons of its own, like a limit on removals. A held publish goes
out only when a person gives a reason, and the manifest keeps a log of each such
override. A month that its base copy and its deltas do not rebuild stops the run with
GateError as well, but no override reason passes that: a run that went on would publish
over a month that its releases cannot restore.
"""

import os
import shutil
import sqlite3
import sys
from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import asdict, dataclass, replace
from datetime import date
from pathlib import Path

from obskit.gates import GateError, GateFailure, check
from obskit.github import GitHub
from obskit.manifest import (
    Asset,
    Manifest,
    ManifestError,
    Watermark,
    dumps,
    loads,
    producer,
)
from obskit.release import (
    PriorStateError,
    add_create_once,
    fetch_assets,
    prepare_asset,
    publish_current,
)
from obskit.sqlite import consolidate, diff_tables, open_db, table_counts
from obskit.tags import current_tag, month_tag
from obskit.zst import sha256_file


def _schema_tables(schema: str) -> set[str]:
    """The names of the tables that `schema` makes, leaving out those that SQLite keeps
    under its reserved prefix `sqlite_`, like the sqlite_sequence of an AUTOINCREMENT
    key."""
    with closing(sqlite3.connect(":memory:")) as scratch:
        scratch.executescript(schema)
        rows = scratch.execute(
            "SELECT name FROM sqlite_master "
            r"WHERE type = 'table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\'"
        )
        return {name for (name,) in rows}


@dataclass(frozen=True)
class Store:
    """What the cycle needs of a stream: its id, its schemas, the tables of the state
    that the replay check compares, the history class of each table and the licences
    of its upstreams (both from its stream.toml), and how a delta applies to its state.

    The names of the stream's databases are made from its id, so a copy of a Store
    with another id names all of them for that stream.

    A Store is checked as it is made: `state_tables` must name every table that the
    state schema makes, each once, or it raises ValueError. The replay check of a
    month close compares only those, so a table left out would pass a month close
    without being compared.
    """

    stream: str  # the stream id, e.g. "osv-pypi"
    state_schema: str  # SQL that makes the state database
    delta_schema: str  # SQL that makes a delta database
    state_tables: tuple[str, ...]  # every table of the state, for the replay check
    classes: dict[str, str]  # history class of every table (stream.toml [tables])
    licences: list[dict]  # stream.toml [[upstreams]]
    # apply(state, delta): apply the delta's rows above the state's applied seq, store
    # the new applied seq in the state, and commit, as every run and every replay does.
    apply: Callable[[sqlite3.Connection, sqlite3.Connection], None]

    def __post_init__(self) -> None:
        """Raise ValueError unless `state_tables` names every table of the state
        schema, each once (see the class)."""
        made = _schema_tables(self.state_schema)
        named = set(self.state_tables)
        problems = []
        lacking = sorted(made - named)
        if lacking:
            problems.append(f"it leaves out {', '.join(lacking)}")
        unknown = sorted(named - made)
        if unknown:
            problems.append(f"the state schema makes no {', '.join(unknown)}")
        repeated = sorted(
            {name for name in self.state_tables if self.state_tables.count(name) > 1}
        )
        if repeated:
            problems.append(f"it names {', '.join(repeated)} more than once")
        if problems:
            raise ValueError(
                f"the state_tables of the stream {self.stream} must name every table "
                "of its state schema, each once, because a month close compares only "
                f"those: {'; '.join(problems)}"
            )

    @property
    def state_db(self) -> str:
        """The state, which the current release holds."""
        return f"{self.stream}-state.db"

    @property
    def open_delta_db(self) -> str:
        """The open delta, which the current release holds with the state."""
        return f"{self.stream}-delta-open.db"

    def delta_name(self, day: date) -> str:
        """The sealed delta of `day`."""
        return f"{self.stream}-delta-{day:%Y-%m-%d}.db"

    def base_state_name(self, year: int, month: int) -> str:
        """The copy of the state as the month began, which its deltas are replayed
        onto."""
        return f"{self.stream}-state-base-{year:04d}-{month:02d}.db"

    def month_part_name(self, year: int, month: int) -> str:
        """The month's deltas, merged into one database once the month is over."""
        return f"{self.stream}-{year:04d}-{month:02d}.db"


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


def replay(store: Store, base: Path, deltas: list[Path]) -> None:
    """Apply each delta database of `deltas`, in order, to the state database at
    `base`, as the runs applied them, and leave `base` as the result.

    This rebuilds a day's state from the copy of the state as its month began and the
    month's deltas up to that day, in day order. A run checks each month that it
    ends this way. Raises as `store.apply` does for a row that does not fit.
    """
    with closing(open_db(base, store.state_schema)) as state:
        for path in deltas:
            with closing(open_db(path, store.delta_schema)) as delta:
                store.apply(state, delta)


def carried(store: Store, prior: Manifest) -> tuple[Asset, Asset]:
    """The entries of the state and of the open delta in the prior manifest.

    Raises PriorStateError unless the manifest lists both as state assets of the
    current release, which `fetch_prior` then downloaded, and gives the open delta its
    day. Otherwise the run would start from no state at all, or from an empty one.
    """
    tag = current_tag(store.stream)
    listed = {
        asset.name: asset
        for asset in prior.assets
        if asset.tag == tag and asset.role == "state"
    }
    state_asset = f"{store.state_db}.zst"
    delta_asset = f"{store.open_delta_db}.zst"
    missing = [name for name in (state_asset, delta_asset) if name not in listed]
    if missing:
        raise PriorStateError(
            f"the prior manifest lists no {' and no '.join(missing)} as a state asset "
            f"of {tag}, so there is no state to go on from"
        )
    delta = listed[delta_asset]
    if delta.period is None or len(delta.period) != len("YYYY-MM-DD"):
        raise PriorStateError(
            f"the prior manifest gives {delta_asset} the period {delta.period}, which "
            "is not a day, so the day that it is open for is not known"
        )
    return listed[state_asset], delta


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


def _seal(
    store: Store, workdir: Path, delta: Asset, day: date
) -> tuple[Path, Path, Asset]:
    """Seal the prior open delta, whose entry is `delta`, as the delta of `day`.

    Returns the file to add to the month's release, the database that it expands to,
    and its entry. The file is the one that `fetch_prior` downloaded and checked, and
    the entry keeps the prior entry's sizes, digests and tables. The open delta's
    database moves to the sealed name, which frees its own name for the next one.
    """
    name = store.delta_name(day)
    packed = _reuse(workdir, delta, f"{name}.zst")
    expanded = workdir / name
    os.replace(workdir / store.open_delta_db, expanded)
    sealed = replace(
        delta,
        name=packed.name,
        tag=month_tag(store.stream, day),
        role="public",
        period=day.isoformat(),
        closed=True,
    )
    return packed, expanded, sealed


def _base_copy(
    store: Store, workdir: Path, state: Asset, month: tuple[int, int]
) -> tuple[Path, Asset]:
    """The copy of the state as `month` begins, from the state's own file, whose entry
    is `state`, as its file and its entry."""
    year, number = month
    packed = _reuse(workdir, state, f"{store.base_state_name(year, number)}.zst")
    base = replace(
        state,
        name=packed.name,
        tag=month_tag(store.stream, date(year, number, 1)),
        period=f"{year:04d}-{number:02d}",
        closed=True,
    )
    return packed, base


def _is_sealed_delta(store: Store, asset: Asset) -> bool:
    """Whether `asset` is the sealed delta of the day that its period gives."""
    if asset.period is None or len(asset.period) != len("YYYY-MM-DD"):
        return False
    return asset.name == f"{store.delta_name(date.fromisoformat(asset.period))}.zst"


def _month_manifest(
    store: Store,
    workdir: Path,
    prior: Manifest,
    month: tuple[int, int],
    assets: list[Asset],
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
        tag=month_tag(store.stream, date(year, number, 1)),
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
    store: Store,
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
    tag = month_tag(store.stream, date(year, number, 1))
    held = {asset.name: asset for asset in prior.assets if asset.tag == tag}
    base = held.get(f"{store.base_state_name(year, number)}.zst")
    if base is None:
        raise PriorStateError(
            f"the prior manifest lists no {store.base_state_name(year, number)}.zst in "
            f"{tag}, so the month cannot be rebuilt to check it"
        )
    days = sorted(
        (asset for asset in held.values() if _is_sealed_delta(store, asset)),
        key=lambda asset: asset.period or "",
    )
    base_db, *day_dbs = fetch_assets(gh, [base, *days], workdir)
    last_db, sealed = last
    deltas = [*day_dbs, last_db]
    replay(store, base_db, deltas)
    with closing(open_db(base_db, store.state_schema)) as rebuilt:
        differ = diff_tables(rebuilt, workdir / store.state_db, store.state_tables)
    if differ:
        detail = (
            f"{period} rebuilt from {base.name} and its {len(deltas)} sealed deltas "
            f"differs from the state in {', '.join(differ)}. This cannot be "
            "overridden, because going on would publish over a month that its "
            "releases cannot restore"
        )
        raise GateError([GateFailure("replay", detail)])
    base_db.unlink()
    part_db = workdir / store.month_part_name(year, number)
    consolidate(deltas, part_db, remove_inputs=True)
    part = prepare_asset(
        part_db,
        workdir,
        tag=tag,
        role="public",
        classes=store.classes,
        period=period,
        closed=True,
    )
    month_assets = [base, *days, sealed, part[1]]
    manifest = _month_manifest(store, workdir, prior, month, month_assets)
    _log_free_disk(workdir, f"month close {period} ends")
    return [part, manifest]


def _add_create_once(gh: GitHub, store: Store, added: list[tuple[Path, Asset]]) -> None:
    """Add each file of `added` to the release that its entry names, in order."""
    by_tag: dict[str, list[Path]] = {}
    for path, asset in added:
        by_tag.setdefault(asset.tag, []).append(path)
    for tag, files in by_tag.items():
        add_create_once(gh, store.stream, tag, files)


def roll_over(
    gh: GitHub,
    store: Store,
    workdir: Path,
    prior: Manifest,
    carried: tuple[Asset, Asset],
    today: date,
) -> list[Asset]:
    """Do what the end of a day or a month asks for, before the run processes anything.
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
        packed, expanded, sealed = _seal(store, workdir, delta, plan.seal_day)
        added.append((packed, sealed))
        if plan.close_month is not None:
            added += _close_month(
                gh, store, workdir, prior, plan.close_month, (expanded, sealed)
            )
        open_db(workdir / store.open_delta_db, store.delta_schema, create=True).close()
    if plan.new_base is not None:
        added.append(_base_copy(store, workdir, state, plan.new_base))
    _add_create_once(gh, store, added)
    return [asset for _, asset in added]


def _require_derived(store: Store, derived: Sequence[Path]) -> None:
    """Raise ValueError unless every table of each database in `derived` is of class
    derived. The gates skip derived tables, so a table of another class kept there
    would lose rows unchecked."""
    for db in derived:
        wrong = [
            table for table in table_counts(db) if store.classes.get(table) != "derived"
        ]
        if wrong:
            raise ValueError(
                f"{db} has tables that are not of class derived: {', '.join(wrong)}"
            )


def prepare_current(
    store: Store, workdir: Path, today: date, derived: Sequence[Path] = ()
) -> tuple[list[Path], list[Asset]]:
    """Compress the state and the open delta, which is today's, for the current
    release. Returns their files and their entries.

    Each database in `derived` follows them, as a public asset of the current release
    with no period that is not closed. Its tables must all be of class derived
    (ValueError otherwise, before anything is compressed), because it is not part of
    the state that a replay rebuilds and no gate holds it to its rows.
    """
    _require_derived(store, derived)
    tag = current_tag(store.stream)
    state = prepare_asset(
        workdir / store.state_db, workdir, tag=tag, role="state", classes=store.classes
    )
    delta = prepare_asset(
        workdir / store.open_delta_db,
        workdir,
        tag=tag,
        role="state",
        classes=store.classes,
        period=today.isoformat(),
    )
    extra = [
        prepare_asset(db, workdir, tag=tag, role="public", classes=store.classes)
        for db in derived
    ]
    return (
        [state[0], delta[0], *(path for path, _ in extra)],
        [state[1], delta[1], *(asset for _, asset in extra)],
    )


def build_manifest(
    store: Store,
    repo: str,
    prior: Manifest | None,
    assets: list[Asset],
    *,
    data_through: Watermark,
    cursor: str | None,
    coverage: dict,
    complete: bool,
    moment: str,
) -> Manifest:
    """The manifest of a publish to `repo` made at `moment`, which lists `assets`.

    `data_through` and `cursor` say how far the data reaches and what the stream read
    last. A run that read nothing new upstream, like the registry's sweep, gives those
    of the prior manifest. `coverage` and `complete` say how much the data covers and
    whether it is whole, which only the stream can judge. The logs of series breaks,
    corrections and overrides go on from the prior manifest, and start empty without
    one.
    """
    return Manifest(
        stream=store.stream,
        producer=producer(repo),
        generated_at=moment,
        complete=complete,
        coverage=coverage,
        data_through=data_through,
        cursor=cursor,
        licences=store.licences,
        assets=assets,
        series_breaks=list(prior.series_breaks) if prior else [],
        corrections=list(prior.corrections) if prior else [],
        overrides=list(prior.overrides) if prior else [],
    )


def gate(
    prior: Manifest,
    manifest: Manifest,
    *,
    allowed_shrink: dict[str, int],
    extra: list[GateFailure],
    override_reason: str | None,
    moment: str,
) -> tuple[Manifest, list[GateFailure]]:
    """Check `manifest` against `prior` with the publish gates, and add `extra`, the
    failures of the stream's own checks, like a limit on removals, after theirs.
    Returns the manifest to publish and the failures.

    A snapshot table may shrink by the rows that `allowed_shrink` gives it, which the
    run's upstream removals explain. With no failure, the manifest is returned as it
    is. A failure raises GateError, unless `override_reason` gives a person's reason to
    publish anyway: then the manifest's overrides log the time, the reason and the
    failures.
    """
    failures = [*check(prior, manifest, allowed_shrink=allowed_shrink), *extra]
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


def publish(
    gh: GitHub,
    store: Store,
    workdir: Path,
    prior: Manifest,
    added: list[Asset],
    *,
    today: date,
    moment: str,
    data_through: Watermark,
    cursor: str | None,
    coverage: dict,
    complete: bool,
    allowed_shrink: dict[str, int],
    extra: list[GateFailure],
    override_reason: str | None,
    derived: Sequence[Path] = (),
) -> list[GateFailure]:
    """Publish the state that a run left in `workdir`: the end of every run that goes
    on from a published state.

    It compresses the state and the open delta, and the databases of `derived` tables
    (see `prepare_current`), writes the manifest (see
    `build_manifest`) from them, the closed assets of `prior` and the `added` entries
    of the rollover, checks it against `prior` (see `gate`), and swaps the files and
    the manifest into the current release. Returns the failures that `override_reason`
    overrode, and raises GateError for any that it did not, before anything is
    published.
    """
    files, current = prepare_current(store, workdir, today, derived)
    closed = [asset for asset in prior.assets if asset.closed]
    manifest = build_manifest(
        store,
        gh.repo,
        prior,
        [*current, *closed, *added],
        data_through=data_through,
        cursor=cursor,
        coverage=coverage,
        complete=complete,
        moment=moment,
    )
    manifest, failures = gate(
        prior,
        manifest,
        allowed_shrink=allowed_shrink,
        extra=extra,
        override_reason=override_reason,
        moment=moment,
    )
    publish_current(gh, store.stream, files, manifest, bootstrap=False)
    return failures


def publish_first(
    gh: GitHub,
    store: Store,
    workdir: Path,
    *,
    today: date,
    moment: str,
    data_through: Watermark,
    cursor: str | None,
    coverage: dict,
    complete: bool,
    series_breaks: list[dict] | None = None,
    derived: Sequence[Path] = (),
) -> None:
    """Publish a stream's first state, which its first run left in `workdir` with the
    open delta.

    Both are compressed, with the databases of `derived` tables (see
    `prepare_current`), and the state is copied as the base of today's month and
    added to the month's release. Then both are swapped into the current release,
    which is created, with a manifest that has no prior (see `build_manifest`).
    `series_breaks` starts the manifest's log of series breaks, for a stream that
    knows of breaks in its upstream before its first run. Every later manifest goes
    on from it.

    It does not check that the stream has never published: only a first run calls it,
    once `fetch_prior` with `bootstrap` has found no current release. The base copy is
    added before `publish_current` refuses a current release that holds a state
    already (PriorStateError).
    """
    files, current = prepare_current(store, workdir, today, derived)
    # The first run starts today's month with a base copy, as plan_rollover says.
    base = _base_copy(store, workdir, current[0], (today.year, today.month))
    _add_create_once(gh, store, [base])
    # A first publish has nothing to fall short of, so no gate can fail.
    manifest = build_manifest(
        store,
        gh.repo,
        None,
        [*current, base[1]],
        data_through=data_through,
        cursor=cursor,
        coverage=coverage,
        complete=complete,
        moment=moment,
    )
    manifest = replace(manifest, series_breaks=list(series_breaks or []))
    publish_current(gh, store.stream, files, manifest, bootstrap=True)
