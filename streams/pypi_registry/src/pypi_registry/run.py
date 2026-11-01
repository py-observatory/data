"""The registry run: what a run reads from PyPI, what it writes, and how it publishes.

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
2. It processes each project that the root gives work for (`process_projects`),
   writing what it learns to the open delta, until its time is up.
3. It applies the delta to the state with `store.apply_delta`, as a restore does.
4. It writes the manifest, checks it against the prior one with the publish gates, and
   swaps the state, the open delta and the manifest into the current release.

The assets of a month's release are never replaced, so a rollover adds only what the
published state alone decides. The sealed delta and the base copy are the files that
the last run published, byte for byte: compressing again gives the same bytes only
with the same zstd and SQLite, which can change between two attempts. The month's
manifest takes nothing from the run. So a run that stops after a rollover, and runs
again, adds the same files, which are kept as they are.

`process_projects` reads the state, fetches pages and release records, and writes what
it learns to the open delta as numbered rows. It never writes the state: the run
applies the delta afterwards, as a replay of the month's deltas does. Every row of a
delta is replayed into the state and published in a monthly part that is never
replaced, so a row written in error is there for good. Each rule below keeps one such
row out:

- A page fetch that finds no page (404 or 410) for a project that the root lists writes
  nothing, not even the project's serial, and the next run fetches the page again. The
  fault may pass, and a project that is really gone leaves the root and is probed then.
- A page whose serial is below the root's serial for the project is a stale copy, from
  before the change that the root announced, and it is skipped in the same way. So is a
  page below the stored serial, which is older than what the state holds: a diff of it
  would record the files that it does not list yet as removed. A probe of a name that
  the root does not list skips such a page too, as it may show a quarantine that has
  ended.
- A quarantined page lists no files, so it is never diffed, and the stored files are
  kept.
- A file, version or release record that the state holds is never written again. A
  delta holds each once, and a month's deltas are merged by key, which refuses a key
  that comes twice.
- A page that is read in full ends with a swept row, so the sweep does not read it
  again and write its files a second time.

A project is processed whole or not at all: the time budget is checked between
projects, never inside one. Errors are not caught. A reply of the wrong shape raises
ValueError and a failed request raises UpstreamError, and either one ends the run,
which then publishes nothing to the current release.

The gates hold back a publish that has less history than the last one, and one that
removes more than MAX_REMOVALS projects at once: a removal is for good, and a name that
comes back is a new project. A held publish goes out only when a person gives a reason,
and the manifest keeps a log of each such override.
"""

import argparse
import json
import os
import platform
import shutil
import sqlite3
import tempfile
import time
import tomllib
from collections.abc import Callable
from contextlib import closing
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path

import obskit
from obskit.gates import GateFailure, check
from obskit.github import GitHub
from obskit.http import Http, HttpResponse, UpstreamError
from obskit.manifest import Asset, Manifest, ManifestError, Watermark, dumps, loads
from obskit.release import (
    PriorStateError,
    add_create_once,
    fetch_assets,
    fetch_prior,
    prepare_asset,
    publish_current,
)
from obskit.sqlite import consolidate
from obskit.tags import current_tag, month_tag
from obskit.zst import sha256_file
from pypi_registry import STREAM
from pypi_registry.diff import Event, classify_probe, diff_files, diff_root
from pypi_registry.layout import (
    OPEN_DELTA_DB,
    STATE_DB,
    base_state_name,
    delta_name,
    month_manifest_name,
    month_part_name,
    plan_rollover,
)
from pypi_registry.names import canonical_name, version_from_filename
from pypi_registry.records import release_record
from pypi_registry.simple import (
    ACCEPT,
    ROOT_URL,
    ProjectPage,
    Root,
    parse_project_page,
    parse_root,
    project_url,
    version_url,
)
from pypi_registry.store import (
    DELTA_SCHEMA,
    STATE_SCHEMA,
    Delta,
    apply_delta,
    diff_states,
    open_db,
)

# The stored statuses that take a project out of the root's diff, as diff_root's
# `inactive` map: such a project cannot change or vanish, only return.
_INACTIVE = ("removed", "quarantined")
# The replies that mean there is no page: 404 for now, and 410 for good.
_GONE = (404, 410)
# How much of an odd value an error message shows.
_SHOWN = 60


@dataclass(frozen=True)
class ProcessReport:
    """What `process_projects` did.

    `new`, `changed`, `vanished` and `returned` count the projects of each group that
    were processed. `vanished` also counts the quarantined names that are not listed,
    which are probed with the vanished ones. `remaining` counts the projects that were
    not reached before the deadline, and the ones skipped for a missing or stale page,
    so the five add up to the projects that the root gave work for.

    `versions_fetched` counts the release records asked for, a 404 included. `swept`
    counts the swept rows written. `files_removed` is the shrink of `files_current`
    that the publish gates allow: the `file_removed` events, and the files that the
    `removed` events delete when the delta is applied.
    """

    new: int
    changed: int
    vanished: int
    returned: int
    versions_fetched: int
    swept: int
    remaining: int
    files_removed: int


@dataclass
class _Context:
    """What the processing of every project shares: the state that it reads, the delta
    that it writes, the client that it fetches with and the moment the state was first
    built, with the counts that end up in the report."""

    state: sqlite3.Connection
    delta: Delta
    http: Http
    bootstrap_at: datetime
    versions_fetched: int = 0
    swept: int = 0
    files_removed: int = 0


@dataclass(frozen=True)
class _Stored:
    """A project as the state holds it."""

    incarnation: int
    last_serial: int
    status: str
    swept: bool


def _moment(text: str, what: str) -> datetime:
    """The moment that an ISO 8601 `text` gives, in UTC when it names no zone. Times are
    compared as moments and not as text: a time on the whole second has no fraction,
    and "." sorts before "Z"."""
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(
            f"{what} must be an ISO 8601 time, not {text[:_SHOWN]!r}"
        ) from None
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _uploaded_after(upload_time: str | None, moment: datetime) -> bool:
    """Whether a file was uploaded after `moment`. A file with no upload time was not:
    it cannot be shown to be."""
    if upload_time is None:
        return False
    return _moment(upload_time, "a file's upload-time") > moment


def _meta(state: sqlite3.Connection, key: str) -> str:
    """The value of `key` in the state's meta table. Raises ValueError when there is
    none: a bootstrap writes every key."""
    row = state.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    if row is None:
        raise ValueError(f"the state has no {key} in meta, so no bootstrap built it")
    return row[0]


def _bootstrap_at(state: sqlite3.Connection) -> datetime:
    """When the state was first built: files uploaded after it were seen as they came."""
    return _moment(_meta(state, "bootstrap_at"), "the state's bootstrap_at")


def _stored(state: sqlite3.Connection, name: str) -> _Stored:
    incarnation, last_serial, status, swept = state.execute(
        "SELECT incarnation, last_serial, status, swept FROM projects WHERE name = ?",
        (name,),
    ).fetchone()
    return _Stored(incarnation, last_serial, status, bool(swept))


def _stored_files(
    state: sqlite3.Connection, name: str, incarnation: int
) -> dict[str, tuple[bool, str | None]]:
    """The files of an incarnation that the state holds, as `diff_files` takes them."""
    rows = state.execute(
        "SELECT filename, yanked, yanked_reason FROM files_current "
        "WHERE name = ? AND incarnation = ?",
        (name, incarnation),
    )
    return {filename: (bool(yanked), reason) for filename, yanked, reason in rows}


def _seen_versions(state: sqlite3.Connection, name: str, incarnation: int) -> set[str]:
    rows = state.execute(
        "SELECT version FROM versions_seen WHERE name = ? AND incarnation = ?",
        (name, incarnation),
    )
    return {version for (version,) in rows}


def _get(http: Http, url: str, *, accept: str | None = None) -> HttpResponse:
    """GET `url`: a 200, or a 404 or 410 for a page that is not there. A 304 is refused,
    as no ETag was sent, so it answers nothing that was asked."""
    response = http.get(url, accept=accept)
    if response.status == 304:
        message = f"GET {url} returned HTTP 304, though no ETag was sent"
        raise UpstreamError(message, url, response.status)
    return response


def _get_page(http: Http, name: str) -> tuple[int, ProjectPage | None]:
    """The HTTP status of a project's Simple API page, and the page when it is there.
    Raises ValueError for a page that does not parse, or that is another project's."""
    response = _get(http, project_url(name), accept=ACCEPT)
    if response.status in _GONE:
        return response.status, None
    page = parse_project_page(response.body)
    if page.name != name:
        raise ValueError(f"the page of {name!r} is the page of {page.name[:_SHOWN]!r}")
    return response.status, page


def _fresh_page(http: Http, name: str, *, floor: int) -> ProjectPage | None:
    """The page of a project that the root lists, or None when there is none to use:
    no page (404 or 410), or a stale copy whose serial is below `floor`."""
    _, page = _get_page(http, name)
    if page is None or page.serial < floor:
        return None
    return page


def _json_object(body: bytes, what: str) -> dict:
    """The JSON object in `body`. Raises ValueError for a body that is not JSON, not an
    object, or nested too deeply to read."""
    try:
        document = json.loads(body)
    except RecursionError:
        raise ValueError(f"{what} is nested too deeply to read") from None
    except ValueError as exc:  # a JSONDecodeError, or a UnicodeDecodeError
        raise ValueError(f"{what} is not JSON: {exc}") from None
    if not isinstance(document, dict):
        raise ValueError(f"{what} must be a JSON object")
    return document


def _record_version(ctx: _Context, name: str, incarnation: int, version: str) -> None:
    """Write a version that the state has not seen, with its release record.

    The record comes from the JSON API, asked for by the canonical version, which PyPI
    resolves to the release. The version is written with record=True and the record,
    or with record=False when the API has no page for it (404 or 410). A reply about
    another project or another version is refused: the record would be kept for good
    under this project, and keyed by a version that the state may hold already.
    Raises ValueError for a reply that does not make a record.
    """
    url = version_url(name, version)
    response = _get(ctx.http, url)
    ctx.versions_fetched += 1
    if response.status in _GONE:
        ctx.delta.version(name, incarnation, version, record=False)
        return
    document = _json_object(response.body, f"the JSON of {name} {version}")
    row = release_record(
        name,
        incarnation,
        document.get("info"),
        document.get("urls"),
        observed_at=ctx.delta.observed_at,
    )
    if canonical_name(row["name_raw"]) != name:
        raise ValueError(
            f"{url} is a release of {row['name_raw'][:_SHOWN]!r}, not of {name!r}"
        )
    if row["version"] != version:
        raise ValueError(
            f"{url} is the release {row['version_raw'][:_SHOWN]!r}, not {version!r}"
        )
    ctx.delta.version(name, incarnation, version, record=True)
    ctx.delta.record(row)


def _unseen(versions: list[str], seen: set[str]) -> list[str]:
    """The versions of a page that are not in `seen`, each once, in the page's order."""
    return [version for version in dict.fromkeys(versions) if version not in seen]


def _process_unswept(
    ctx: _Context, name: str, incarnation: int, page: ProjectPage
) -> None:
    """Read the page of a project in full, as the sweep does, and end with a swept row.

    Each file that the state does not hold is written, in filename order. It is exact
    only when it was uploaded after the bootstrap: a file from before then was there
    before the project was first read. A version that the state has not seen gets its
    record when a file of it was uploaded after the bootstrap, and record=False
    otherwise, as the history fill holds the releases from before. The swept row
    carries the page's status, and no event is written.

    Raises ValueError for a quarantined page, before anything is written. Such a page
    lists no files, so the caller records the quarantine instead.
    """
    if page.status == "quarantined":
        raise ValueError(
            f"the page of {name!r} is quarantined and lists no files, so it is not "
            "read in full: the caller records the quarantine"
        )
    held = _stored_files(ctx.state, name, incarnation)
    by_name = {info.filename: info for info in page.files}
    recent = set()  # the versions with a file uploaded after the bootstrap
    for filename in sorted(by_name):
        info = by_name[filename]
        after = _uploaded_after(info.upload_time, ctx.bootstrap_at)
        if after and info.version is not None:
            recent.add(info.version)
        if filename not in held:
            ctx.delta.file(name, incarnation, info, exact=after)
    for version in _unseen(page.versions, _seen_versions(ctx.state, name, incarnation)):
        if version in recent:
            _record_version(ctx, name, incarnation, version)
        else:
            ctx.delta.version(name, incarnation, version, record=False)
    ctx.delta.swept(name, incarnation, page.status)
    ctx.swept += 1


def _process_changed(
    ctx: _Context, name: str, incarnation: int, status: str, page: ProjectPage
) -> None:
    """Record how the page of a swept project differs from the state.

    `status` is the project's status as the rows written so far leave it. A page with
    another status writes `status_changed`. A file that the state lacks is written as
    exact, since it is seen as it comes, and a stored file that the page lacks is a
    `file_removed` event. A file whose yank or reason changed is a `yanked` or
    `unyanked` event. Each version that the state has not seen gets its record.

    Raises ValueError for a quarantined page, before anything is written: it lists no
    files, and a diff of it would remove every stored one.
    """
    changes = diff_files(_stored_files(ctx.state, name, incarnation), page)
    if page.status != status:
        ctx.delta.event(Event(name, incarnation, "status_changed", detail=page.status))
    for info in changes.added:
        ctx.delta.file(name, incarnation, info, exact=True)
    for filename in changes.removed:
        version = version_from_filename(filename)
        ctx.delta.event(Event(name, incarnation, "file_removed", version, filename))
        ctx.files_removed += 1
    for filename, yanked, reason in changes.yank_changes:
        kind = "yanked" if yanked else "unyanked"
        version = version_from_filename(filename)
        ctx.delta.event(Event(name, incarnation, kind, version, filename, reason))
    for version in _unseen(page.versions, _seen_versions(ctx.state, name, incarnation)):
        _record_version(ctx, name, incarnation, version)


def _process_new(
    ctx: _Context,
    name: str,
    incarnation: int,
    page: ProjectPage,
    *,
    reregistered: bool = False,
) -> None:
    """Start a project, and read its page in full.

    A new name gets `created`. A removed name that is registered again gets
    `reregistered` with its new incarnation, and no `created`, which the state refuses
    for a name it holds. A quarantined page then gets a `quarantined` event and a swept
    row with its status, and no files or records. Any other page is read in full.
    """
    ctx.delta.event(
        Event(name, incarnation, "reregistered" if reregistered else "created")
    )
    if page.status == "quarantined":
        ctx.delta.event(Event(name, incarnation, "quarantined"))
        ctx.delta.swept(name, incarnation, page.status)
        ctx.swept += 1
    else:
        _process_unswept(ctx, name, incarnation, page)


def _probe(ctx: _Context, name: str, root: Root) -> bool:
    """Probe a stored name that the root does not list: a vanished project, or a
    quarantined one, which PyPI may delete while it is out of the root.

    No page (404 or 410) writes `removed`, and the files that it deletes count toward
    the allowed shrink. A quarantined page writes `quarantined` for a project that is
    not quarantined yet. Anything else writes nothing, and the name is probed again at
    the next run. A probe writes no serial: the root gives none.

    A page below the stored serial is an old copy, which may still show a quarantine
    that has ended. It writes nothing, and the probe counts as skipped, so the name is
    probed again at the next run.
    """
    stored = _stored(ctx.state, name)
    status, page = _get_page(ctx.http, name)
    if page is not None and page.serial < stored.last_serial:
        return False
    found = classify_probe(status, page)
    if found == "removed":
        (files,) = ctx.state.execute(
            "SELECT count(*) FROM files_current WHERE name = ? AND incarnation = ?",
            (name, stored.incarnation),
        ).fetchone()
        ctx.files_removed += files
        ctx.delta.event(Event(name, stored.incarnation, "removed"))
    elif found == "quarantined" and stored.status != "quarantined":
        ctx.delta.event(Event(name, stored.incarnation, "quarantined"))
    return True


def _process_listed(
    ctx: _Context, name: str, stored: _Stored, status: str, page: ProjectPage
) -> None:
    """Process the page of a listed project that the state holds at its incarnation:
    read in full if it is not swept yet, and diffed if it is. `status` is the
    project's status as the rows written so far leave it."""
    if stored.swept:
        _process_changed(ctx, name, stored.incarnation, status, page)
    else:
        _process_unswept(ctx, name, stored.incarnation, page)


def _returned(ctx: _Context, name: str, root: Root) -> bool:
    """Process a name that is listed again: a removed name that was registered again
    starts a new incarnation, and a quarantined one keeps its incarnation if its page
    is no longer quarantined. A page that is still quarantined writes only the serial,
    so a quarantined project that PyPI keeps in the root writes no events."""
    stored = _stored(ctx.state, name)
    listed = root.projects[name]
    if stored.status == "removed":
        # The stored serial is the old incarnation's, so only the root's counts.
        page = _fresh_page(ctx.http, name, floor=listed)
        if page is None:
            return False
        _process_new(ctx, name, stored.incarnation + 1, page, reregistered=True)
    else:
        page = _fresh_page(ctx.http, name, floor=max(listed, stored.last_serial))
        if page is None:
            return False
        if page.status != "quarantined":
            ctx.delta.event(Event(name, stored.incarnation, "unquarantined"))
            _process_listed(ctx, name, stored, "active", page)
    ctx.delta.serial(name, page.serial)
    return True


def _new(ctx: _Context, name: str, root: Root) -> bool:
    """Process a name that the state does not hold, at incarnation 1."""
    page = _fresh_page(ctx.http, name, floor=root.projects[name])
    if page is None:
        return False
    _process_new(ctx, name, 1, page)
    ctx.delta.serial(name, page.serial)
    return True


def _changed(ctx: _Context, name: str, root: Root) -> bool:
    """Process an active project that the root lists with another serial. A page that
    is quarantined now writes `quarantined`, and the stored files are kept."""
    stored = _stored(ctx.state, name)
    page = _fresh_page(
        ctx.http, name, floor=max(root.projects[name], stored.last_serial)
    )
    if page is None:
        return False
    if page.status == "quarantined":
        ctx.delta.event(Event(name, stored.incarnation, "quarantined"))
    else:
        _process_listed(ctx, name, stored, stored.status, page)
    ctx.delta.serial(name, page.serial)
    return True


def _projects_by_status(
    state: sqlite3.Connection,
) -> tuple[dict[str, int], dict[str, str]]:
    """The stored projects as `diff_root` takes them: the serial of each active one,
    and the status of each removed or quarantined one."""
    active: dict[str, int] = {}
    inactive: dict[str, str] = {}
    for name, serial, status in state.execute(
        "SELECT name, last_serial, status FROM projects"
    ):
        if status in _INACTIVE:
            inactive[name] = status
        else:
            active[name] = serial
    return active, inactive


# The groups of projects in the order they are processed, each with its handler. A
# handler returns False for a project that it skipped, with nothing written.
_GROUPS: tuple[tuple[str, Callable[[_Context, str, Root], bool]], ...] = (
    ("vanished", _probe),
    ("returned", _returned),
    ("new", _new),
    ("changed", _changed),
)


def process_projects(
    state: sqlite3.Connection,
    delta: Delta,
    http: Http,
    root: Root,
    *,
    deadline: float,
    clock: Callable[[], float] = time.monotonic,
) -> ProcessReport:
    """Process each project that `root` gives work for, writing what is found to
    `delta`, until `clock()` reaches `deadline`.

    The state is read and never written, and the delta is not committed: the run
    applies it afterwards. `diff_root` sorts the root against the state. The groups go
    in this order, each by name: the vanished names, probed with the quarantined names
    that the root does not list; then the returned names, the new ones and the changed
    ones. The deadline is checked before each project, and a project that is begun is
    finished.

    Each processed project that the root lists ends with a serial row, at the serial
    of its page. A project that is not reached gets no row and keeps its old serial, so
    the next run finds it again, and so does one whose page is missing (404 or 410) or
    stale. Each of these counts in `remaining`.
    """
    ctx = _Context(state, delta, http, _bootstrap_at(state))
    active, inactive = _projects_by_status(state)
    diff = diff_root(active, inactive, root)
    held = [
        name
        for name, status in inactive.items()
        if status == "quarantined" and name not in root.projects
    ]
    names = {
        "vanished": sorted([*diff.vanished, *held]),
        "returned": diff.returned,
        "new": diff.new,
        "changed": diff.changed,
    }
    work = [
        (group, handler, name) for group, handler in _GROUPS for name in names[group]
    ]
    done = dict.fromkeys(names, 0)
    remaining = 0
    for index, (group, handler, name) in enumerate(work):
        if clock() >= deadline:
            remaining += len(work) - index
            break
        if handler(ctx, name, root):
            done[group] += 1
        else:
            remaining += 1
    return ProcessReport(
        new=done["new"],
        changed=done["changed"],
        vanished=done["vanished"],
        returned=done["returned"],
        versions_fetched=ctx.versions_fetched,
        swept=ctx.swept,
        remaining=remaining,
        files_removed=ctx.files_removed,
    )


# ---- the run around the processing

# More `removed` events than this in one run hold the publish for a person to look at.
# A removal is for good, and a name that comes back is a new project, so a root that
# lost its names and a burst of 404s must not be published unread.
MAX_REMOVALS = 1_000

# The repository that `main` publishes to when GITHUB_REPOSITORY does not say.
_REPOSITORY = "py-observatory/data"
_STREAM_TOML = Path(__file__).resolve().parents[2] / "stream.toml"
# The assets that carry the state and the open delta in the current release.
_STATE_ASSET = f"{STATE_DB}.zst"
_DELTA_ASSET = f"{OPEN_DELTA_DB}.zst"


class StaleRootError(RuntimeError):
    """The Simple API root is older than the state: its serial is below the cursor."""


class GateError(RuntimeError):
    """What the run would publish fails a check, so nothing was published to the
    current release. `failures` lists each failure."""

    def __init__(self, failures: list[GateFailure]) -> None:
        self.failures = list(failures)
        listed = "; ".join(f"{failure.rule}: {failure.detail}" for failure in failures)
        super().__init__(f"nothing was published, because the run fails: {listed}")


@dataclass(frozen=True)
class RunReport:
    """What a run did.

    The counts of projects and versions are those of `ProcessReport`, and a bootstrap,
    which processes no project, has none. `requests` counts the requests to PyPI, the
    retries among them. `gate_failures` lists the failures that a person overrode, and
    is empty when no gate failed.
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


def _instant(now: datetime) -> str:
    """`now` as a manifest writes a time, in UTC to the second. Raises ValueError for a
    time that names no zone, which could be any of them."""
    if now.tzinfo is None:
        raise ValueError(f"the time of a run must name its zone, and {now} does not")
    return now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stream_toml() -> dict:
    """The stream's stream.toml: its upstreams and the history class of each table."""
    return tomllib.loads(_STREAM_TOML.read_text(encoding="utf-8"))


def _fetch_root(http: Http) -> Root:
    """The Simple API root. Any reply but a 200 is a failed request (UpstreamError),
    and a root that does not parse raises ValueError."""
    response = _get(http, ROOT_URL, accept=ACCEPT)
    if response.status != 200:
        message = f"GET {ROOT_URL} returned HTTP {response.status}"
        raise UpstreamError(message, ROOT_URL, response.status)
    return parse_root(response.body)


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
    `assets` in place of its own. It takes nothing from the run, so a run that adds
    it to the release, stops, and runs again makes the same bytes. Raises
    ManifestError unless it reads back as itself, as the strict reader reads it.
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

    Raises PriorStateError when the prior manifest lists no base copy of the month,
    or a copy does not download as its entry says, and GateError when the rebuilt
    state differs from the state.
    """
    year, number = month
    period = f"{year:04d}-{number:02d}"
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
            f"differs from the state in {', '.join(differ)}"
        )
        raise GateError([GateFailure("replay", detail)])
    part_db = workdir / month_part_name(year, number)
    consolidate(deltas, part_db)
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
    return [part, _month_manifest(workdir, prior, month, month_assets)]


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


def _coverage(state: sqlite3.Connection, remaining: int) -> dict:
    """What the state covers. `projects` counts the projects that are neither removed
    nor quarantined, which are the ones that the sweep reads, and `swept` and
    `unswept` split them by whether their page has been read in full. `remaining`
    counts the projects that the run left to the next one, and `bootstrap_at` is when
    the state was first built."""
    marks = ", ".join("?" * len(_INACTIVE))
    projects, swept = state.execute(
        "SELECT count(*), coalesce(sum(swept), 0) FROM projects "
        f"WHERE status NOT IN ({marks})",
        _INACTIVE,
    ).fetchone()
    return {
        "projects": projects,
        "swept": swept,
        "unswept": projects - swept,
        "remaining": remaining,
        "bootstrap_at": _meta(state, "bootstrap_at"),
    }


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


def _producer(repo: str) -> dict[str, str]:
    """Who made a publish: the repository published to, which `main` takes from
    GITHUB_REPOSITORY, the commit and the workflow run (GITHUB_SHA and GITHUB_RUN_ID),
    and the versions of obskit and Python. The commit and the run are empty outside a
    workflow."""
    return {
        "repo": repo,
        "commit": os.environ.get("GITHUB_SHA", ""),
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "obskit": obskit.__version__,
        "python": platform.python_version(),
    }


def _manifest(
    repo: str,
    prior: Manifest | None,
    assets: list[Asset],
    *,
    serial: int,
    coverage: dict,
    moment: str,
) -> Manifest:
    """The manifest of a publish made at `moment` from the root at `serial`. The data
    reaches as far as that root, which is the cursor too. The run is complete when it
    left no project to the next one. The logs of series breaks, corrections and
    overrides go on from the prior manifest."""
    return Manifest(
        stream=STREAM,
        producer=_producer(repo),
        generated_at=moment,
        complete=coverage["remaining"] == 0,
        coverage=coverage,
        data_through=Watermark(str(serial), "serial"),
        cursor=str(serial),
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
    root = _fetch_root(http)
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
        coverage = _coverage(state, remaining=0)
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
        serial=root.serial,
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
    `clock` have passed, writes the root's serial to the delta as the cursor, and
    applies the delta to the state. It then writes the manifest, checks it with the
    gates (see `_gate`), and swaps the state, the open delta and the manifest into the
    current release.

    Every error propagates, and an error before the publish publishes nothing to the
    current release. What a rollover already added to a month's release stays, as it
    is made from the published state alone, and the next run adds the same files.
    """
    deadline = clock() + max_seconds
    requests = http.requests
    moment = _instant(now)
    today = now.astimezone(UTC).date()
    prior = fetch_prior(gh, STREAM, workdir, bootstrap=bootstrap)
    if prior is None:
        return _bootstrap(
            gh, http, workdir, moment=moment, today=today, requests=requests
        )
    carried = _carried(prior)
    state_db, delta_db = workdir / STATE_DB, workdir / OPEN_DELTA_DB
    with (
        closing(open_db(state_db, STATE_SCHEMA)) as state,
        closing(open_db(delta_db, DELTA_SCHEMA)),
    ):
        cursor = int(_meta(state, "cursor"))
        applied = int(_meta(state, "applied_seq"))
    root = _fetch_root(http)
    if root.serial < cursor:
        raise StaleRootError(
            f"the root's serial {root.serial} is below the stored cursor {cursor}, so "
            "it is a stale copy, and nothing was written"
        )
    added = _roll_over(gh, workdir, prior, carried, today)
    with (
        closing(open_db(state_db, STATE_SCHEMA)) as state,
        closing(open_db(delta_db, DELTA_SCHEMA)) as conn,
    ):
        # A new day's delta is numbered on from what the state has applied, and
        # today's goes on after its own last row.
        delta = Delta(conn, moment, first_seq=applied + 1)
        processed = process_projects(
            state, delta, http, root, deadline=deadline, clock=clock
        )
        delta.cursor(root.serial, moment)
        conn.commit()
        removals = _removals(conn, after_seq=applied)
        _apply(state, conn)
        coverage = _coverage(state, processed.remaining)
    files, current = _prepare_current(workdir, today)
    closed = [asset for asset in prior.assets if asset.closed]
    manifest = _manifest(
        gh.repo,
        prior,
        [*current, *closed, *added],
        serial=root.serial,
        coverage=coverage,
        moment=moment,
    )
    manifest, failures = _gate(
        prior,
        manifest,
        files_removed=processed.files_removed,
        removals=removals,
        override_reason=override_reason,
        moment=moment,
    )
    publish_current(gh, STREAM, files, manifest, bootstrap=False)
    return RunReport(
        new=processed.new,
        changed=processed.changed,
        vanished=processed.vanished,
        returned=processed.returned,
        versions_fetched=processed.versions_fetched,
        requests=http.requests - requests,
        swept=processed.swept,
        remaining=processed.remaining,
        gate_failures=failures,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the stream from the command line: `python -m pypi_registry run`.

    It needs GITHUB_TOKEN, and publishes to GITHUB_REPOSITORY, or to
    py-observatory/data when that is not set. It makes one GitHub client, whose
    writes are paced, for every call of the run, and works in a new directory under
    ./work, which is removed afterwards. It prints the report as JSON and returns 0.
    Every error propagates.
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
        help="publish although a gate failed, for this reason, which the manifest logs",
    )
    run.add_argument(
        "--max-seconds",
        type=float,
        default=18_000,
        help="stop starting projects after this many seconds (default: 18000)",
    )
    args = parser.parse_args(argv)
    gh = GitHub(
        os.environ.get("GITHUB_REPOSITORY", _REPOSITORY), os.environ["GITHUB_TOKEN"]
    )
    work = Path.cwd() / "work"
    work.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="run-", dir=work) as workdir:
        report = run_once(
            gh,
            Http(),
            Path(workdir),
            now=datetime.now(UTC),
            bootstrap=args.bootstrap,
            override_reason=args.override_reason or None,
            max_seconds=args.max_seconds,
        )
    print(json.dumps(asdict(report)))
    return 0
