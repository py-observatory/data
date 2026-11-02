"""What a run takes from OSV's index: the advisories whose entry moved since the state
last saw them, each written to the open delta.

A run reads OSV's index (`index.get_index`) and compares it with the state. An advisory
that the state lacks, or that the index lists with a later time than the version that
the state holds, has its record fetched, and the record becomes a version row. An
active advisory that the index no longer lists is removed, and a removed one that it
lists again returns. `process_index` is that step. It reads the state and writes the
open delta, and never writes the state: the run applies the delta afterwards, with
`store.apply`, as a replay of the month's deltas does. `process_dump` comes before it
once, at the bootstrap, and takes the records of OSV's full dump instead of fetching
them one by one.

Every row of a delta is applied to the state and published in a monthly part that is
never replaced, so a row written in error stays for good. `store.apply_delta` refuses a
version that goes back and a version of a removed advisory, and the refusal fails the
whole run. Each rule below keeps such a row out:

- A record whose time is below the one that the index gives is a stale copy, from
  before the change that the index announced, and so is one that is no later than the
  version that the state holds. Neither writes anything, and the advisory is left for
  the next run, which fetches it again.
- A record that is not there (404 or 410) writes nothing, not even a removal, and is
  fetched again at the next run. The index lists it, so the fault may pass. Only the
  index says that an advisory is gone.
- A removed advisory that the index lists again gets its return before its version, so
  that it is active when the version applies.

A record that does not parse is skipped and named by its id, and the others go on. OSV
gathers advisories from several databases, and its schema keeps changing, so one record
that the stream cannot read yet should not hold back every other. It is fetched again
at every run, and comes in once the stream can read it.

Removals and returns need no request, so all of them are written, whatever the time.
The time budget is checked before each fetch, and the advisories that are not fetched by
the deadline are left for the next run. A request that fails is Http's UpstreamError,
and it is not caught: the run fails and publishes nothing.
"""

import sqlite3
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from obskit.emails import without_addresses
from obskit.http import Http, UpstreamError
from osv_pypi.index import Index, record_url
from osv_pypi.records import parse_record
from osv_pypi.store import Delta

# The replies that mean that OSV has no record: 404 for now, and 410 for good.
_GONE = (404, 410)


@dataclass(frozen=True)
class ProcessReport:
    """What `process_index` did.

    `new` and `changed` count the version rows written for the advisories of each
    group, and `removed` and `returned` count the events written. The others count the
    advisories that are left for the next run: a `stale` copy, a `missing` record (404
    or 410), a record not fetched by the deadline (`deferred`), and the ids of the
    records that did not parse (`unparsed`).
    """

    new: int
    changed: int
    removed: int
    returned: int
    stale: int
    missing: int
    deferred: int
    unparsed: list[str]

    @property
    def remaining(self) -> int:
        """The advisories whose record the index called for and that are still to be
        written."""
        return self.stale + self.missing + self.deferred + len(self.unparsed)


@dataclass(frozen=True)
class _Fetch:
    """An advisory whose record is to be fetched: its id, the group whose count its
    version row goes to ("new" or "changed"), the key of the time that the index gives,
    and the `modified_key` that the state holds, or None for an advisory that it
    lacks."""

    advisory_id: str
    group: str
    listed_key: str
    held_key: str | None


def _held(state: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    """Each advisory that the state holds, with its status and the `modified_key` of
    its version."""
    rows = state.execute("SELECT id, status, modified_key FROM advisories")
    return {advisory_id: (status, key) for advisory_id, status, key in rows}


def _write_events(
    delta: Delta, held: dict[str, tuple[str, str]], listed: dict[str, str]
) -> tuple[int, int]:
    """Write a removal for each active advisory that the index does not list, and a
    return for each removed one that it lists, in id order. Returns how many removals
    and how many returns were written."""
    removed = returned = 0
    for advisory_id in sorted(held):
        status, _ = held[advisory_id]
        if status == "active" and advisory_id not in listed:
            delta.removed(advisory_id)
            removed += 1
        elif status == "removed" and advisory_id in listed:
            delta.returned(advisory_id)
            returned += 1
    return removed, returned


def _fetches(held: dict[str, tuple[str, str]], listed: dict[str, str]) -> list[_Fetch]:
    """The advisories whose record is to be fetched, in id order: each that the state
    lacks, which is new, and each that the index lists with a key above the stored
    one, which is changed. A returned advisory is changed when its key is above the
    stored one: its return makes it active, and its entry has moved."""
    fetches = []
    for advisory_id in sorted(listed):
        listed_key = listed[advisory_id]
        if advisory_id not in held:
            fetches.append(_Fetch(advisory_id, "new", listed_key, None))
            continue
        _, held_key = held[advisory_id]
        if listed_key > held_key:
            fetches.append(_Fetch(advisory_id, "changed", listed_key, held_key))
    return fetches


def _fetch_record(http: Http, advisory_id: str) -> bytes | None:
    """The bytes of an advisory's record as OSV serves it, or None when OSV has none
    (404 or 410).

    Http returns a 200, a 304, a 404 or a 410, and raises UpstreamError for any other
    reply. A 304 answers an ETag, and none was sent, so it is not the record, and it
    does not say that the record is missing either: it raises UpstreamError too.
    """
    url = record_url(advisory_id)
    reply = http.get(url)
    if reply.status in _GONE:
        return None
    if reply.status != 200:
        message = f"GET {url} returned HTTP {reply.status}, though no ETag was sent"
        raise UpstreamError(message, url, reply.status)
    return reply.body


def _is_stale(record_key: str, fetch: _Fetch) -> bool:
    """Whether a record whose `modified_key` is `record_key` is a stale copy: older than
    the time that the index gives, or no later than the version that the state holds.
    Keys compare as the times do.

    A record is fetched only when the index gives a time above the stored one, so a
    copy that is not older than the index's is later than the stored version too. The
    second test stays all the same, beside the write that it guards. The state refuses
    a version that goes back, and the refusal fails the whole run. A copy at the stored
    time would only repeat the version that the state holds.
    """
    if record_key < fetch.listed_key:
        return True
    return fetch.held_key is not None and record_key <= fetch.held_key


def _log_refusal(advisory_id: str, error: ValueError) -> None:
    """Say on stderr why the record of `advisory_id` was refused, so that whoever reads
    the run's log sees what to fix.

    A refusal shows part of the bad value, which can be an email address, and the log
    of a public repository is public. So every address is taken out first, and when
    that cannot be done, the reason is not shown.
    """
    reason = without_addresses(str(error))
    if reason is None:
        reason = "the reason holds an email address, so it is not shown"
    print(f"skipped {advisory_id}: {reason}", file=sys.stderr)


def _fetch_version(delta: Delta, http: Http, fetch: _Fetch) -> str:
    """Fetch the record of an advisory and write it as a version row, unless it is
    missing, does not parse or is stale. Returns the count of the report that this goes
    to: the advisory's group when the row is written, and otherwise "missing",
    "unparsed" or "stale"."""
    body = _fetch_record(http, fetch.advisory_id)
    if body is None:
        return "missing"
    try:
        row = parse_record(body, advisory_id=fetch.advisory_id)
    except ValueError as error:
        _log_refusal(fetch.advisory_id, error)
        return "unparsed"
    if _is_stale(row["modified_key"], fetch):
        return "stale"
    delta.version(row)
    return fetch.group


def process_dump(
    delta: Delta, records: Iterable[tuple[str, bytes]], index: Index
) -> int:
    """Write a version row for each record of the dump that the index lists and that
    parses, in the dump's order, and return how many were written.

    `records` are the id and the bytes of each record, as `dump.read_dump` yields them.
    A record that the index does not list is skipped: the index says which advisories
    OSV has, and the dump is a copy from another moment. A copy that is older than the
    time that the index gives is written all the same, as a version that the stream
    saw, and `process_index` then fetches the newer one. A record that does not parse
    is skipped, so the state lacks it, and `process_index` fetches it as a new one and
    names it if it still does not parse.

    Only the parse is guarded. A ValueError from `records` means that `read_dump`
    refused the whole dump, and it is not caught: the run fails. Catching it would take
    a refused dump for one record that did not parse.
    """
    written = 0
    for advisory_id, body in records:
        if advisory_id not in index.modified:
            continue
        try:
            row = parse_record(body, advisory_id=advisory_id)
        except ValueError:
            continue
        delta.version(row)
        written += 1
    return written


def process_index(
    state: sqlite3.Connection,
    delta: Delta,
    http: Http,
    index: Index,
    *,
    deadline: float,
    clock: Callable[[], float] = time.monotonic,
) -> ProcessReport:
    """Compare `index` with the state, write what changed to `delta`, and fetch the
    records that it calls for until `clock()` reaches `deadline`.

    The state is read and never written, and the delta is not committed: the run
    applies it afterwards. First, in id order, each active advisory that the index does
    not list gets a removal, and each removed one that it lists gets a return. These
    need no request, so every one of them is written, whatever the time. Then records
    are fetched, in id order:

    - the record of each advisory that the state lacks, which is new
    - the record of each that the index lists with a key above the stored
      `modified_key`, which is changed. A returned advisory is among these when its key
      is above the stored one, as its return makes it active.

    The deadline is checked before each fetch. A record that parses and is not stale is
    written as a version row. A stale copy, a missing record (404 or 410), a record
    that does not parse and one that is not fetched by the deadline write nothing, and
    count in `remaining`, so the next run finds the advisory again. A record that does
    not parse is named in `unparsed` by its id, and why it was refused goes to stderr
    without any email address.
    """
    held = _held(state)
    removed, returned = _write_events(delta, held, index.modified)
    fetches = _fetches(held, index.modified)
    counted: Counter[str] = Counter()
    unparsed: list[str] = []
    deferred = 0
    for position, fetch in enumerate(fetches):
        if clock() >= deadline:
            deferred = len(fetches) - position
            break
        outcome = _fetch_version(delta, http, fetch)
        if outcome == "unparsed":
            unparsed.append(fetch.advisory_id)
        else:
            counted[outcome] += 1
    return ProcessReport(
        new=counted["new"],
        changed=counted["changed"],
        removed=removed,
        returned=returned,
        stale=counted["stale"],
        missing=counted["missing"],
        deferred=deferred,
        unparsed=unparsed,
    )
