"""Per-project processing: what a run reads from PyPI, and the rows it writes to the
delta.

A run reads PyPI's Simple API root (`get_root`), compares it with the state, and
processes each project that the comparison names. `process_projects` is that step. It
reads the state, fetches pages and release records, and writes what it learns to the
open delta as numbered rows. It never writes the state: the run applies the delta
afterwards, with `store.apply_delta`, as a replay of the month's deltas does.
`process_new`, `process_changed` and `process_unswept` are the steps for one project,
and are public so that the sweep can share them. `record_current` is the step of the
records pass, which reads the record of a project's current release when the state
holds that version without one. `process_status` is the step of the status pass, which
records a change of a project's status, and `process_changed` takes it too.

Every row of a delta is replayed into the state and published in a monthly part that is
never replaced, so a row written in error is there for good. Each rule below keeps one
such row out:

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
  that comes twice. So the records pass writes a record alone, for a version that the
  state holds without one, and never the version again.
- A page that is read in full ends with a swept row, so the sweep does not read it
  again and write its files a second time.

A project is processed whole or not at all: the time budget is checked between
projects, never inside one. Errors are not caught. A reply of the wrong shape raises
ValueError and a failed request raises UpstreamError, and either one ends the run,
which then publishes nothing to the current release. The records pass alone writes
nothing for a release record that it refuses and goes on (see `record_current`).
"""

import json
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from obskit.http import Http, HttpResponse, UpstreamError
from pypi_registry.diff import Event, classify_probe, diff_files, diff_root
from pypi_registry.names import canonical_name, latest_version, version_from_filename
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
from pypi_registry.store import Delta

# The stored statuses that take a project out of the root's diff, as diff_root's
# `inactive` map: such a project cannot change or vanish, only return.
INACTIVE = ("removed", "quarantined")
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
class Context:
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


def bootstrap_at(state: sqlite3.Connection) -> datetime:
    """When the state was first built: files uploaded after it were seen as they came."""
    row = state.execute("SELECT value FROM meta WHERE key = 'bootstrap_at'").fetchone()
    if row is None:
        raise ValueError(
            "the state has no bootstrap_at in meta, so no bootstrap built it"
        )
    return _moment(row[0], "the state's bootstrap_at")


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


def get_root(http: Http) -> Root:
    """The Simple API root. Any reply but a 200 is a failed request (UpstreamError),
    and a root that does not parse raises ValueError."""
    response = _get(http, ROOT_URL, accept=ACCEPT)
    if response.status != 200:
        message = f"GET {ROOT_URL} returned HTTP {response.status}"
        raise UpstreamError(message, ROOT_URL, response.status)
    return parse_root(response.body)


def get_page(http: Http, name: str) -> tuple[int, ProjectPage | None]:
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
    _, page = get_page(http, name)
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


def _fetch_record(
    ctx: Context, name: str, incarnation: int, version: str
) -> dict | None:
    """The release record of a version, or None when the JSON API has no page for it
    (404 or 410).

    The record is asked for by the canonical version, which PyPI resolves to the
    release. A reply about another project or another version is refused: the record
    would be kept for good under this project, and keyed by a version that the state
    may hold already. Raises ValueError for a reply that does not make a record.
    """
    url = version_url(name, version)
    response = _get(ctx.http, url)
    ctx.versions_fetched += 1
    if response.status in _GONE:
        return None
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
    return row


def _record_version(ctx: Context, name: str, incarnation: int, version: str) -> None:
    """Write a version that the state has not seen, with its release record (see
    `_fetch_record`): with record=True and the record, or with record=False when the
    API has no page for it. The version comes first, because applying a record marks
    a version that the state must hold."""
    row = _fetch_record(ctx, name, incarnation, version)
    ctx.delta.version(name, incarnation, version, record=row is not None)
    if row is not None:
        ctx.delta.record(row)


def current_version(
    state: sqlite3.Connection, name: str, incarnation: int
) -> str | None:
    """The version that a project is at: the latest (see `names.latest_version`) of
    the versions that its files in the state carry, leaving out the yanked files, or
    None when it has no such file. A file whose name carries no version that can be
    read is left out. The versions are canonical, as the state keeps them."""
    rows = state.execute(
        "SELECT filename FROM files_current "
        "WHERE name = ? AND incarnation = ? AND yanked = 0",
        (name, incarnation),
    )
    versions = (version_from_filename(filename) for (filename,) in rows)
    return latest_version(version for version in versions if version is not None)


def record_current(ctx: Context, name: str, incarnation: int) -> str:
    """Write the release record of a project's current release (see
    `current_version`) when the state holds that version without one, and say what
    came of it.

    "recorded": the record was written, and nothing else. The state holds the version
    already, so writing it again would repeat a key of the month's deltas, and applying
    the record marks the version as recorded. "missing": the JSON API has no page for
    the release (404 or 410), and nothing was written. "refused": the reply makes no
    record that can be kept (`_fetch_record` raised ValueError), and nothing was
    written. "had": nothing was asked for, because the version has a record, or the
    project has no current release, or the state has not seen the version, whose record
    could not be applied.

    This is the records pass's step, and only that pass goes on past a reply that it
    refuses: it reads the releases from before the bootstrap, close to a million of
    them, and one that the parser cannot read would otherwise stop it at the same name
    in every run. A failed request still raises UpstreamError, as it may pass.
    """
    version = current_version(ctx.state, name, incarnation)
    if version is None:
        return "had"
    seen = ctx.state.execute(
        "SELECT record FROM versions_seen "
        "WHERE name = ? AND incarnation = ? AND version = ?",
        (name, incarnation, version),
    ).fetchone()
    if seen is None or seen[0]:
        return "had"
    try:
        row = _fetch_record(ctx, name, incarnation, version)
    except ValueError:
        return "refused"
    if row is None:
        return "missing"
    ctx.delta.record(row)
    return "recorded"


def _unseen(versions: list[str], seen: set[str]) -> list[str]:
    """The versions of a page that are not in `seen`, each once, in the page's order."""
    return [version for version in dict.fromkeys(versions) if version not in seen]


def process_unswept(
    ctx: Context,
    name: str,
    incarnation: int,
    page: ProjectPage,
    *,
    exact: bool = True,
) -> None:
    """Read the page of a project in full, as the sweep does, and end with a swept row.

    Each file that the state does not hold is written, in filename order. It is exact
    only when it was uploaded after the bootstrap, and `exact` is true: a file from
    before the bootstrap was there before the project was first read. The daily run
    leaves `exact` true, because it reads a project as it changes and sees a file about
    when it appears. The sweep passes false, because it comes to a project when its turn
    is, so a file that it finds was not seen when it was uploaded, whenever that was,
    and none of its files is exact. A version that the state has not seen gets its
    record when a file of it was uploaded after the bootstrap, and record=False
    otherwise, as the history fill holds the releases from before. That does not depend
    on `exact`. The swept row carries the page's status, and no event is written.

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
            ctx.delta.file(name, incarnation, info, exact=after and exact)
    for version in _unseen(page.versions, _seen_versions(ctx.state, name, incarnation)):
        if version in recent:
            _record_version(ctx, name, incarnation, version)
        else:
            ctx.delta.version(name, incarnation, version, record=False)
    ctx.delta.swept(name, incarnation, page.status)
    ctx.swept += 1


def process_status(
    ctx: Context, name: str, incarnation: int, status: str, page: ProjectPage
) -> bool:
    """Write `status_changed`, with the page's status as its detail, when the page of a
    swept project shows another status than `status`, and say whether it did.

    `status` is the project's status as the rows written so far leave it. This is the
    status step of `process_changed`, and all that the run's status pass writes for a
    page that it re-reads: the pass reads projects whose serial has not moved, because
    archiving a project moves no serial, so nothing else would see it.
    """
    if page.status == status:
        return False
    ctx.delta.event(Event(name, incarnation, "status_changed", detail=page.status))
    return True


def process_changed(
    ctx: Context, name: str, incarnation: int, status: str, page: ProjectPage
) -> None:
    """Record how the page of a swept project differs from the state.

    `status` is the project's status as the rows written so far leave it. A page with
    another status writes `status_changed` (see `process_status`). A file that the
    state lacks is written as exact, since it is seen as it comes, and a stored file
    that the page lacks is a `file_removed` event. A file whose yank or reason changed
    is a `yanked` or `unyanked` event. Each version that the state has not seen gets
    its record.

    Raises ValueError for a quarantined page, before anything is written: it lists no
    files, and a diff of it would remove every stored one.
    """
    changes = diff_files(_stored_files(ctx.state, name, incarnation), page)
    process_status(ctx, name, incarnation, status, page)
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


def process_new(
    ctx: Context,
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
        process_unswept(ctx, name, incarnation, page)


def _probe(ctx: Context, name: str, root: Root) -> bool:
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
    status, page = get_page(ctx.http, name)
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
    ctx: Context, name: str, stored: _Stored, status: str, page: ProjectPage
) -> None:
    """Process the page of a listed project that the state holds at its incarnation:
    read in full if it is not swept yet, and diffed if it is. `status` is the
    project's status as the rows written so far leave it."""
    if stored.swept:
        process_changed(ctx, name, stored.incarnation, status, page)
    else:
        process_unswept(ctx, name, stored.incarnation, page)


def _returned(ctx: Context, name: str, root: Root) -> bool:
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
        process_new(ctx, name, stored.incarnation + 1, page, reregistered=True)
    else:
        page = _fresh_page(ctx.http, name, floor=max(listed, stored.last_serial))
        if page is None:
            return False
        if page.status != "quarantined":
            ctx.delta.event(Event(name, stored.incarnation, "unquarantined"))
            _process_listed(ctx, name, stored, "active", page)
    ctx.delta.serial(name, page.serial)
    return True


def _new(ctx: Context, name: str, root: Root) -> bool:
    """Process a name that the state does not hold, at incarnation 1."""
    page = _fresh_page(ctx.http, name, floor=root.projects[name])
    if page is None:
        return False
    process_new(ctx, name, 1, page)
    ctx.delta.serial(name, page.serial)
    return True


def _changed(ctx: Context, name: str, root: Root) -> bool:
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
        if status in INACTIVE:
            inactive[name] = status
        else:
            active[name] = serial
    return active, inactive


# The groups of projects in the order they are processed, each with its handler. A
# handler returns False for a project that it skipped, with nothing written.
_GROUPS: tuple[tuple[str, Callable[[Context, str, Root], bool]], ...] = (
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
    ctx = Context(state, delta, http, bootstrap_at(state))
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
