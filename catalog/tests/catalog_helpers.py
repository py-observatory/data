"""Tiny but real releases of the streams that the catalog reads, published on the kit's
FakeGitHub for the catalog's tests.

`publish_registry` and `publish_osv` publish a stream as its runs leave it, so that what
a test gives the catalog to read is shaped as the live streams' releases are. Each
database is made with the stream's own schema, from rows given as dicts per table, and
each file goes up through the kit's publishing: `prepare_asset` describes it,
`add_create_once` adds it to its month's release, and `publish_current` swaps the state
and the open delta into `<stream>-current`, with the manifest last.

The releases are laid out as obskit.cycle lays them out:

- `<stream>-current` holds the state, which has no period, and the open delta, whose
  period is its day, both with the role state. Its manifest.json lists them and every
  closed asset of the month releases.
- `<stream>-month-YYYY-MM` holds the month's base copy, which is a copy of the state:
  its entry is the state's with its own name and release, the month as its period, and
  closed, so its role stays state. It holds the sealed delta of each day, with the role
  public, the day as its period, and closed. Once the month is over, it also holds the
  month's part, public and closed with the month as its period, and the month's
  manifest.

Each call is a publish of the stream, and a later call is the stream's next publish. It
keeps every closed asset that the last one listed, as the kit's gates hold a stream to.
A month's base copy is made once, from the state of the first call that reaches the
month, as a run makes it when the month begins. A sealed delta or a part that is given
again must hold the same rows: a create-once asset is never replaced, and other bytes
under its name raise CreateOnceError.

Each call makes requests of its own, so a test that looks at the requests that the
catalog makes clears the fake's calls once it has published.
"""

import shutil
import tempfile
import tomllib
from collections.abc import Callable, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

import osv_pypi
import pypi_downloads
import pypi_registry
from obskit.cycle import month_manifest_name
from obskit.github import GitHub, NotFound
from obskit.manifest import Asset, Manifest, Watermark, dumps, producer
from obskit.release import (
    PriorStateError,
    add_create_once,
    fetch_manifest,
    prepare_asset,
    publish_current,
)
from obskit.sqlite import open_db
from obskit.tags import current_tag, month_tag
from obskit.zst import sha256_file
from osv_pypi import store as osv_store
from osv_pypi.index import instant_key
from pypi_downloads import serving as downloads_serving
from pypi_downloads import store as downloads_store
from pypi_registry import store as registry_store

REPO = "py-observatory/data"
TOKEN = "t0ken"
ROOT = Path(__file__).resolve().parents[2]

# The rows of a database, by table. Each row maps its columns to their values.
Rows = Mapping[str, Sequence[Mapping[str, object]]]

# The time that a row takes for a column that must have one, when a test leaves it out.
TIME = "2026-10-01T00:00:00Z"

_SQLITE = "sqlite+zstd"


def no_sleep(seconds: float) -> None:
    """GitHub's retries and pacing wait for nothing in a test."""


@dataclass(frozen=True)
class _Stream:
    """A stream as these helpers publish it: its id, the schemas of its state and of its
    deltas, how far a publish on a day says its data reaches, and the plain value of
    each column that a row must have, by table.

    A row takes that value for each such column that the test leaves out, so that a test
    names only what it is about. The columns that say which row it is, such as the name
    of a project or of a file, have none, and a test always names them.
    """

    id: str
    state_schema: str
    delta_schema: str
    data_through: Callable[[str], Watermark]
    defaults: dict[str, dict[str, object]]


def _registry_data_through(open_day: str) -> Watermark:
    """The registry's data reaches the serial of the last root that it read. Here the
    serial is the digits of the open day, so that it moves on with the days, as the
    serial does with each day's runs."""
    return Watermark(open_day.replace("-", ""), "serial")


def _osv_data_through(open_day: str) -> Watermark:
    """osv's data reaches the newest time in OSV's index, which is before the run. Here
    it is the start of the open day, when the run is."""
    return Watermark(f"{open_day}T00:00:00Z", "instant")


# The first incarnation of a project is 1. A project is swept, its page read in full, as
# every project that has a release is.
_REGISTRY = _Stream(
    id=pypi_registry.STREAM,
    state_schema=registry_store.STATE_SCHEMA,
    delta_schema=registry_store.DELTA_SCHEMA,
    data_through=_registry_data_through,
    defaults={
        "meta": {"value": ""},
        "projects": {
            "incarnation": 1,
            "last_serial": 0,
            "status": "active",
            "swept": 1,
            "first_seen": TIME,
            "first_seen_exact": 0,
        },
        "files_current": {"incarnation": 1, "yanked": 0},
        "versions_seen": {"incarnation": 1, "record": 0},
        "files": {"incarnation": 1},
        "versions": {"incarnation": 1},
        "release_records": {"incarnation": 1},
    },
)
_OSV = _Stream(
    id=osv_pypi.STREAM,
    state_schema=osv_store.STATE_SCHEMA,
    delta_schema=osv_store.DELTA_SCHEMA,
    data_through=_osv_data_through,
    defaults={
        "meta": {"value": ""},
        "advisories": {
            "status": "active",
            "first_seen": TIME,
            "seq": 0,
            "kind": "vulnerability",
            "modified": TIME,
            "modified_key": instant_key(TIME),
            "affected": "[]",
        },
    },
)

# The downloads stream's days are numbered from 1970-01-01, and the time that the counter
# wrote them is `TIME` unless a test says. The state's `seq` says which delta wrote a day,
# so a test names it.
_DOWNLOADS = _Stream(
    id=pypi_downloads.STREAM,
    state_schema=downloads_store.STATE_SCHEMA,
    delta_schema=downloads_store.DELTA_SCHEMA,
    data_through=lambda open_day: Watermark(open_day, "day"),
    defaults={
        "meta": {"value": ""},
        "days": {
            "rows_bigquery": 0,
            "rows_counted": 0,
            "partition_modified": TIME,
            "projects": 0,
            "downloads": 0,
            "mirror_downloads": 0,
            "null_project_rows": 0,
            "read_bytes": 0,
            "counted_at": TIME,
            "observed_at": TIME,
        },
        "breaks": {"note": ""},
    },
)


def publish_registry(
    gh: GitHub,
    *,
    state: Rows,
    open_day: str,
    open_delta: Rows,
    sealed: Mapping[str, Rows] | None = None,
    parts: Mapping[str, Rows] | None = None,
    complete: bool = True,
) -> Manifest:
    """Publish pypi-registry as its run on `open_day` leaves it, and return the live
    manifest.

    `state` holds the rows of the state's tables (meta, projects, files_current and
    versions_seen), and `open_delta` those of the open delta of `open_day`, given as
    YYYY-MM-DD. `sealed` gives the rows of the sealed delta of each day, by its day, and
    `parts` those of the part of each month that is over, by its month, as YYYY-MM.
    Each delta holds the delta's tables: files, versions, release_records, events,
    serials, swept and cursors. A row of a delta that leaves out its `seq` is numbered
    as `_first_seq` says.

    The manifest says that the publish was made at the start of `open_day`, and that the
    data reaches the serial of that day (see `_registry_data_through`). Its licences are
    the upstreams that the stream's stream.toml names, as a run gives them. The catalog
    reads nothing more of the registry's manifest than those, the assets and whether the
    data is complete, so the coverage is empty and there is no cursor. The data is
    complete unless `complete` says otherwise, as the registry's is not until its sweep
    has read every project.
    """
    return _publish(
        gh,
        _REGISTRY,
        state,
        open_day,
        open_delta,
        sealed or {},
        parts or {},
        complete=complete,
    )


def publish_osv(
    gh: GitHub,
    *,
    advisories: Sequence[Mapping[str, object]],
    open_day: str,
    generated_at: str | None = None,
    sealed: Mapping[str, Rows] | None = None,
    parts: Mapping[str, Rows] | None = None,
    complete: bool = True,
) -> Manifest:
    """Publish osv-pypi as its run on `open_day`, given as YYYY-MM-DD, leaves it, and
    return the live manifest.

    `advisories` are the rows of the state's advisories table. The open delta is empty,
    because the catalog reads only the state of osv. `sealed` and `parts` give the rows
    of sealed deltas and of month parts, as `publish_registry`'s do, for a test of what
    a daily pin names, which is every sealed delta of its month whatever the catalog
    reads. The manifest is made as `publish_registry`'s is, but the data reaches the
    start of `open_day`, as an instant (see `_osv_data_through`). `generated_at`, when
    given, is the time that the manifest says the publish was made, in place of the
    start of `open_day`, for a test of a stream whose publish is older than its last.
    `complete` is as `publish_registry`'s.
    """
    return _publish(
        gh,
        _OSV,
        {"advisories": advisories},
        open_day,
        {},
        sealed or {},
        parts or {},
        generated_at=generated_at,
        complete=complete,
    )


def publish_downloads(
    gh: GitHub,
    *,
    state: Rows,
    serving: Rows,
    open_day: str,
    open_delta: Rows,
    coverage: Mapping[str, object] | None = None,
    sealed: Mapping[str, Rows] | None = None,
    complete: bool = False,
) -> Manifest:
    """Publish pypi-downloads as its run on `open_day`, given as YYYY-MM-DD, leaves it,
    and return the live manifest.

    `state` holds the rows of the state's tables (meta, projects, days, breaks, spend and
    clearances), and `open_delta` those of the open delta. `serving` holds the rows of
    the serving database's tables (folded, months, monthly, top_set, daily and recent),
    which goes up beside the state as the stream's derived asset: `pypi-downloads-
    serving.db.zst` in the current release, with the role public and no period. A row of
    the state's days leaves out nothing but what `_DOWNLOADS` gives it, so it names its
    `seq`. `sealed` gives the rows of the sealed delta of each day, as
    `publish_registry`'s does. The data reaches the start of `open_day`, as a day. The
    manifest's coverage is `coverage`, which the catalog copies into its own tables, or
    empty. The stream's data is not complete, as it is not until its backfill has
    counted every day.
    """
    return _publish(
        gh,
        _DOWNLOADS,
        state,
        open_day,
        open_delta,
        sealed or {},
        {},
        complete=complete,
        serving=serving,
        coverage=coverage,
    )


def _publish(
    gh: GitHub,
    stream: _Stream,
    state: Rows,
    open_day: str,
    open_delta: Rows,
    sealed: Mapping[str, Rows],
    parts: Mapping[str, Rows],
    *,
    generated_at: str | None = None,
    complete: bool = True,
    serving: Rows | None = None,
    coverage: Mapping[str, object] | None = None,
) -> Manifest:
    """Publish `stream` as `publish_registry` says, and return the manifest. Its
    `generated_at` is the start of `open_day` unless `generated_at` gives another, it
    says that the data is complete unless `complete` is False, and its coverage is
    `coverage`, or empty.

    The files go up as a run puts them up: first those of the month releases, each to
    the release that its entry names, and then the state and the open delta, which
    `publish_current` swaps into the current release with the manifest last. A first
    publish creates the current release. With `serving`, the rows of the downloads
    stream's serving database, that database goes up beside them as a derived asset.
    """
    prior = _live_manifest(gh, stream.id)
    carried = [asset for asset in prior.assets if asset.closed] if prior else []
    toml = _stream_toml(stream.id)
    manifest = Manifest(
        stream=stream.id,
        producer=producer(gh.repo),
        generated_at=generated_at or f"{open_day}T00:00:00Z",
        complete=complete,
        coverage=dict(coverage or {}),
        data_through=stream.data_through(open_day),
        cursor=None,
        licences=toml["upstreams"],
        assets=[],
    )
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        classes = toml["tables"]
        tag = current_tag(stream.id)
        state_file, state_entry = prepare_asset(
            _database(
                work / f"{stream.id}-state.db",
                stream.state_schema,
                state,
                stream.defaults,
            ),
            work,
            tag=tag,
            role="state",
            classes=classes,
        )
        delta_file, delta_entry = prepare_asset(
            _delta(work / f"{stream.id}-delta-open.db", stream, open_delta, open_day),
            work,
            tag=tag,
            role="state",
            classes=classes,
            period=open_day,
        )
        current = [(state_file, state_entry), (delta_file, delta_entry)]
        if serving is not None:
            current.append(
                prepare_asset(
                    _database(
                        work / downloads_serving.SERVING_DB,
                        downloads_serving.SCHEMA,
                        serving,
                        {},
                    ),
                    work,
                    tag=tag,
                    role="public",
                    classes=classes,
                )
            )
        made = _month_files(
            stream,
            work,
            classes,
            (state_file, state_entry),
            open_day,
            sealed,
            parts,
            carried,
        )
        made += _month_manifests(stream, work, manifest, carried, made, parts)
        _add_create_once(gh, stream.id, made)
        held = {(asset.tag, asset.name) for asset in carried}
        added = [entry for _, entry in made if (entry.tag, entry.name) not in held]
        manifest = replace(
            manifest, assets=[*(entry for _, entry in current), *carried, *added]
        )
        publish_current(
            gh,
            stream.id,
            [file for file, _ in current],
            manifest,
            bootstrap=prior is None,
        )
    return manifest


def _live_manifest(gh: GitHub, stream: str) -> Manifest | None:
    """The manifest of the stream's last publish, or None when it has not published,
    which GitHub says with a 404 for `<stream>-current`."""
    with tempfile.TemporaryDirectory() as tmp:
        try:
            return fetch_manifest(gh, stream, Path(tmp))
        except PriorStateError as exc:
            if isinstance(exc.__cause__, NotFound):
                return None
            raise


def _stream_toml(stream: str) -> dict:
    """The stream.toml of `stream`, which gives the history class of each of its tables
    and its upstreams with their licences."""
    path = ROOT / "streams" / stream.replace("-", "_") / "stream.toml"
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _month_files(
    stream: _Stream,
    work: Path,
    classes: dict[str, str],
    state: tuple[Path, Asset],
    open_day: str,
    sealed: Mapping[str, Rows],
    parts: Mapping[str, Rows],
    carried: list[Asset],
) -> list[tuple[Path, Asset]]:
    """The files for the stream's month releases, each with its entry, month by month in
    order: the base copy of each month that the open day, a sealed day or a part falls
    in, unless `carried` lists it already, then each sealed delta of the month, by day,
    and then the month's part.

    The databases are named as obskit.cycle.Store names them, and an asset adds `.zst`.
    """
    held = {(asset.tag, asset.name) for asset in carried}
    months = sorted({open_day[:7], *(day[:7] for day in sealed), *parts})
    made: list[tuple[Path, Asset]] = []
    for month in months:
        tag = month_tag(stream.id, date.fromisoformat(f"{month}-01"))
        base = f"{stream.id}-state-base-{month}.db.zst"
        if (tag, base) not in held:
            made.append(_base_copy(work, state, base, tag, month))
        for day in sorted(day for day in sealed if day[:7] == month):
            db = _delta(work / f"{stream.id}-delta-{day}.db", stream, sealed[day], day)
            made.append(
                prepare_asset(
                    db,
                    work,
                    tag=tag,
                    role="public",
                    classes=classes,
                    period=day,
                    closed=True,
                )
            )
        if month in parts:
            db = _delta(work / f"{stream.id}-{month}.db", stream, parts[month], month)
            made.append(
                prepare_asset(
                    db,
                    work,
                    tag=tag,
                    role="public",
                    classes=classes,
                    period=month,
                    closed=True,
                )
            )
    return made


def _base_copy(
    work: Path, state: tuple[Path, Asset], name: str, tag: str, month: str
) -> tuple[Path, Asset]:
    """The copy of the state as `month` begins, from the state's own file, as its file
    `name` in `work` and its entry: the state's, with that name, the month's release
    `tag`, the month as its period, and closed."""
    state_file, state_entry = state
    copy = work / name
    shutil.copyfile(state_file, copy)
    entry = replace(state_entry, name=name, tag=tag, period=month, closed=True)
    return copy, entry


def _month_manifests(
    stream: _Stream,
    work: Path,
    manifest: Manifest,
    carried: list[Asset],
    made: list[tuple[Path, Asset]],
    parts: Mapping[str, Rows],
) -> list[tuple[Path, Asset]]:
    """The manifest of each month of `parts` that `carried` does not list one for, as its
    file and its entry.

    As at a month's close, it is the stream's manifest listing the month's base copy,
    its sealed deltas by day, and its part in place of its own assets. Those are taken
    from `carried` and from `made`, the files that the call adds.
    """
    held = {(asset.tag, asset.name) for asset in carried}
    listed = [*carried, *(entry for _, entry in made)]
    files: list[tuple[Path, Asset]] = []
    for month in sorted(parts):
        year, number = (int(field) for field in month.split("-"))
        name = month_manifest_name(year, number)
        tag = month_tag(stream.id, date(year, number, 1))
        if (tag, name) in held:
            continue
        databases = {
            asset.name: asset
            for asset in listed
            if asset.tag == tag and asset.format == _SQLITE
        }
        base = databases.pop(f"{stream.id}-state-base-{month}.db.zst")
        part = databases.pop(f"{stream.id}-{month}.db.zst")
        days = sorted(databases.values(), key=lambda asset: asset.period or "")
        path = work / name
        text = dumps(replace(manifest, assets=[base, *days, part]))
        path.write_text(text, encoding="utf-8")
        size = path.stat().st_size
        digest = sha256_file(path)
        entry = Asset(
            name=name,
            tag=tag,
            role="public",
            format="json",
            asset_bytes=size,
            asset_sha256=digest,
            db_bytes=size,
            db_sha256=digest,
            tables={},
            period=month,
            closed=True,
        )
        files.append((path, entry))
    return files


def _add_create_once(gh: GitHub, stream: str, made: list[tuple[Path, Asset]]) -> None:
    """Add each file of `made` to the release that its entry names, in order. A file
    that a release holds already with the same bytes is left as it is."""
    by_tag: dict[str, list[Path]] = {}
    for path, entry in made:
        by_tag.setdefault(entry.tag, []).append(path)
    for tag, files in by_tag.items():
        add_create_once(gh, stream, tag, files)


def _first_seq(period: str) -> int:
    """The number from which the rows of a delta of `period` that leave out their `seq`
    are numbered: the digits of the period, made eight long with zeros, and then four
    more zeros.

    A stream numbers the rows of all its deltas in one sequence, so no two of its deltas
    share a seq, and a later delta has the higher. These numbers do the same, for deltas
    of up to 9,999 rows: the part of 2026-11 counts on from 202611000000, before the
    delta of 2026-11-01, which counts on from 202611010000. A delta gets the same
    numbers each time that it is made, so a sealed delta made again from the same rows
    has the same bytes.
    """
    return int(period.replace("-", "").ljust(8, "0")) * 10_000


def _delta(path: Path, stream: _Stream, rows: Rows, period: str) -> Path:
    """Make the delta of `period` at `path`, holding `rows`."""
    return _database(
        path,
        stream.delta_schema,
        rows,
        stream.defaults,
        first_seq=_first_seq(period),
    )


def _database(
    path: Path,
    schema: str,
    rows: Rows,
    defaults: dict[str, dict[str, object]],
    *,
    first_seq: int | None = None,
) -> Path:
    """Make the database `path` from `schema`, holding `rows`, and return its path.

    A row takes its table's value in `defaults` for each column that it leaves out. With
    `first_seq`, which a delta has, a row that leaves out its `seq` takes `first_seq`
    plus its place among the rows of the database, from 1, in the order given.
    """
    with closing(open_db(path, schema, create=True)) as con:
        listed = [(table, row) for table, many in rows.items() for row in many]
        for place, (table, row) in enumerate(listed, start=1):
            have = {r[1] for r in con.execute(f"PRAGMA table_info({_quote(table)})")}
            values = {**defaults.get(table, {}), **row}
            # A default is for the columns that the schema has: one table of a state
            # and its delta may differ in them, as the downloads days do in `observed_at`.
            values = {c: v for c, v in values.items() if c in have or c in row}
            if first_seq is not None and "seq" in have:
                values.setdefault("seq", first_seq + place)
            columns = ", ".join(_quote(column) for column in values)
            marks = ", ".join("?" * len(values))
            con.execute(
                f"INSERT INTO {_quote(table)} ({columns}) VALUES ({marks})",
                list(values.values()),
            )
        con.commit()
    return path


def _quote(name: str) -> str:
    """`name` as SQL writes a name, so that no name is read as SQL."""
    return '"' + name.replace('"', '""') + '"'
