"""The osv-pypi run: the order of its steps, what it reads, and how it publishes.

`run_once` is one run, on the kit's day and month cycle (`obskit.cycle`). It reads the
prior state from `osv-pypi-current`, which fails closed: a state that cannot be read
ends the run, and only a bootstrap starts from nothing.

The first run, the bootstrap, builds the state from OSV's index and its full dump. It
writes a version of each record of the dump that the index lists and that parses into
the first open delta, and applies it, so the part of the first month holds the first
version that the stream saw of every advisory. Then it compares the index with that
state, which fetches one at a time what the dump lacked: a record that the dump does
not hold or that did not parse, and the newer version of one whose copy in the dump is
older than the index says. It publishes with `cycle.publish_first`.

Every later run reads the index, and refuses one whose generation is below the stored
cursor, which is a stale copy, before it writes anything. Then it rolls over
(`cycle.roll_over`), compares the index with the state (`process.process_index`),
writes the index's generation to the delta as the cursor, applies the delta to the
state, and publishes with `cycle.publish`, which checks the gates first.

Each manifest says that the data goes through the newest time in the index, to the
second, and gives the index's generation as the cursor. Its coverage counts the active
advisories, the malware reports and the withdrawn advisories among them, the removed
advisories, and the advisories that the run left to the next one (`remaining`): a stale
copy, a missing record, a record that did not parse and one that was not fetched in
time. The data is complete when none is left.

A removal stays in the month parts for good, and an index that was cut short would
remove every advisory that it lost. So more than MAX_REMOVALS removals in one run hold
the publish until a person gives a reason to override the limit. A month that its base
copy and its deltas do not rebuild stops the run as well, and no reason overrides that
(see `obskit.cycle`).

A record that does not parse is skipped and named by its id, and the run publishes the
rest, because OSV gathers advisories from several databases and its schema keeps
changing. `main` then exits 2, after the publish, so that the workflow opens its
failure issue, and the record is tried again at every run. Every other error
propagates, and a run that fails publishes nothing to the current release.
"""

import argparse
import json
import os
import sqlite3
import tempfile
import time
import tomllib
from collections.abc import Callable
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from obskit import cycle
from obskit.gates import GateFailure
from obskit.github import GitHub
from obskit.http import Http
from obskit.manifest import Asset, Manifest, Watermark, instant
from obskit.release import fetch_prior
from obskit.sqlite import open_db
from osv_pypi import STREAM
from osv_pypi.dump import get_dump, read_dump
from osv_pypi.index import Index, get_index, seconds
from osv_pypi.process import ProcessReport, process_dump, process_index
from osv_pypi.store import (
    DELTA_SCHEMA,
    STATE_SCHEMA,
    STATE_TABLES,
    Delta,
    apply,
    meta,
)

# More `removed` events than this in one run hold the publish for a person to look at.
# A removal stays in the month parts for good, and an index that was cut short would
# remove every advisory that it lost.
MAX_REMOVALS = 100

# The repository that `main` publishes to when GITHUB_REPOSITORY does not say.
_REPOSITORY = "py-observatory/data"
_STREAM_TOML = Path(__file__).resolve().parents[2] / "stream.toml"


def _store() -> cycle.Store:
    """The stream as the kit's cycle sees it: its id, its schemas, the tables of its
    state and how a delta applies to it, from store.py, and the history class of each
    table and the licences of its upstreams, from its stream.toml. The Store checks as
    it is made that STATE_TABLES names every table of the state."""
    config = tomllib.loads(_STREAM_TOML.read_text(encoding="utf-8"))
    return cycle.Store(
        stream=STREAM,
        state_schema=STATE_SCHEMA,
        delta_schema=DELTA_SCHEMA,
        state_tables=STATE_TABLES,
        classes=config["tables"],
        licences=config["upstreams"],
        apply=apply,
    )


# Made as the module is imported, so a state schema that STATE_TABLES does not match
# stops the stream before any run starts.
STORE = _store()


class StaleIndexError(RuntimeError):
    """OSV's index is older than the state: its generation is below the cursor."""


@dataclass(frozen=True)
class RunReport:
    """What a run did.

    The counts are those of `process.ProcessReport`. `new` and `changed` count the
    version rows written for the advisories that the state lacked and for those whose
    index entry moved, so a bootstrap counts each record of the dump as new. `removed`
    and `returned` count the events written, and `remaining` the advisories left to the
    next run. `requests` counts the requests to OSV, the retries among them.

    `unparsed` names each record that did not parse by its id, and by nothing else: the
    report goes to the log of a public repository, and a refusal shows part of the bad
    value. `gate_failures` lists the failures that a person overrode, and is empty when
    no gate failed.
    """

    new: int
    changed: int
    removed: int
    returned: int
    remaining: int
    requests: int
    unparsed: list[str]
    gate_failures: list[GateFailure]


@dataclass(frozen=True)
class _Resume:
    """Where the prior state stands, for a run that goes on from it.

    `carried` is the entries of the state and of the open delta in the prior manifest.
    `cursor` is the generation of the index that the state read last, and `applied` is
    the highest `seq` that the state has applied.
    """

    carried: tuple[Asset, Asset]
    cursor: int
    applied: int


def _required_meta(state: sqlite3.Connection, key: str) -> str:
    """The value of `key` in the state's meta table. Raises ValueError when there is
    none: the bootstrap writes every key that a later run reads."""
    value = meta(state, key)
    if value is None:
        raise ValueError(f"the state has no {key} in meta, so no bootstrap built it")
    return value


def _resume(prior: Manifest, workdir: Path) -> _Resume:
    """Check the state that `fetch_prior` left in `workdir` for the prior manifest
    `prior`, and read where it stands.

    Raises PriorStateError unless `prior` lists the state and the open delta (see
    `cycle.carried`). Both databases are opened without making them (`open_db` raises
    FileNotFoundError or ValueError for one that is missing or not of its schema), so a
    state that is not there is an error and never an empty one. Raises ValueError when
    the state lacks its cursor or its applied_seq.
    """
    carried = cycle.carried(STORE, prior)
    with (
        closing(open_db(workdir / STORE.state_db, STATE_SCHEMA)) as state,
        closing(open_db(workdir / STORE.open_delta_db, DELTA_SCHEMA)),
    ):
        cursor = int(_required_meta(state, "cursor"))
        applied = int(_required_meta(state, "applied_seq"))
    return _Resume(carried, cursor, applied)


def _commit_and_apply(state: sqlite3.Connection, conn: sqlite3.Connection) -> None:
    """Commit what the run has written to its open delta, `conn`, and apply the rows
    that the state has not applied with the stream's apply, which commits the state.
    Both files are then whole on disk for the publish to compress."""
    conn.commit()
    STORE.apply(state, conn)


def _coverage(state: sqlite3.Connection, remaining: int) -> dict:
    """What the state covers: the advisories that the index lists (`advisories`), the
    malware reports and the withdrawn advisories among them, and the advisories that it
    no longer lists (`removed`). `remaining` counts the advisories that the run left to
    the next one."""
    active, malware, withdrawn, removed = state.execute(
        "SELECT count(*) FILTER (WHERE status = 'active'), "
        "count(*) FILTER (WHERE status = 'active' AND kind = 'malware'), "
        "count(*) FILTER (WHERE status = 'active' AND withdrawn IS NOT NULL), "
        "count(*) FILTER (WHERE status = 'removed') "
        "FROM advisories"
    ).fetchone()
    return {
        "advisories": active,
        "malware": malware,
        "withdrawn": withdrawn,
        "removed": removed,
        "remaining": remaining,
    }


def _data_through(index: Index) -> Watermark:
    """How far the data reaches: the newest time in the index, to the second. The index
    always lists an advisory, since `get_index` refuses an empty one."""
    return Watermark(seconds(max(index.modified.values())), "instant")


def _removal_limit(removed: int) -> list[GateFailure]:
    """The failure that holds the publish when the run wrote more than MAX_REMOVALS
    `removed` events, or none."""
    if removed <= MAX_REMOVALS:
        return []
    detail = f"{removed:,} removed events in one run (limit {MAX_REMOVALS:,})"
    return [GateFailure("removals", detail)]


def _report(
    processed: ProcessReport,
    *,
    dumped: int = 0,
    requests: int,
    failures: list[GateFailure],
) -> RunReport:
    """The report of a run whose comparison of the index with the state gave
    `processed`. `dumped` is the number of versions that a bootstrap wrote from the
    dump, which are all new, `requests` the number of requests to OSV, and `failures`
    the gate failures that were overridden."""
    return RunReport(
        new=dumped + processed.new,
        changed=processed.changed,
        removed=processed.removed,
        returned=processed.returned,
        remaining=processed.remaining,
        requests=requests,
        unparsed=processed.unparsed,
        gate_failures=failures,
    )


def _bootstrap(
    gh: GitHub,
    http: Http,
    workdir: Path,
    *,
    moment: str,
    today: date,
    deadline: float,
    clock: Callable[[], float],
    requests: int,
) -> RunReport:
    """Build the stream's first state from OSV's index and its full dump, and publish
    it.

    The state starts with nothing applied and `moment` as its bootstrap_at, and the open
    delta starts empty. Each record of the dump that the index lists and that parses is
    written to the delta as a version, and the delta is applied, so each such advisory
    is active and first seen at `moment`, which is not when it first appeared. The
    index is then compared with that state, which fetches what the dump lacked until
    `deadline` on `clock`. The index's generation is written as the cursor, and the
    delta is applied again. The state is copied as the base of today's month, and
    published with the open delta and a manifest that says what a later run's says.
    `requests` is the count of requests to OSV when the run began.
    """
    index = get_index(http)
    dump = get_dump(http)
    with (
        closing(open_db(workdir / STORE.state_db, STATE_SCHEMA, create=True)) as state,
        closing(
            open_db(workdir / STORE.open_delta_db, DELTA_SCHEMA, create=True)
        ) as conn,
    ):
        state.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [("applied_seq", "0"), ("bootstrap_at", moment)],
        )
        state.commit()
        delta = Delta(conn, moment, first_seq=1)
        dumped = process_dump(delta, read_dump(dump), index)
        # The comparison takes the state to hold every row of the delta, so the
        # dump's rows are applied before it.
        _commit_and_apply(state, conn)
        processed = process_index(
            state, delta, http, index, deadline=deadline, clock=clock
        )
        delta.cursor(index.generation)
        _commit_and_apply(state, conn)
        coverage = _coverage(state, processed.remaining)
    cycle.publish_first(
        gh,
        STORE,
        workdir,
        today=today,
        moment=moment,
        data_through=_data_through(index),
        cursor=str(index.generation),
        coverage=coverage,
        complete=processed.remaining == 0,
    )
    return _report(
        processed, dumped=dumped, requests=http.requests - requests, failures=[]
    )


def run_once(
    gh: GitHub,
    http: Http,
    workdir: Path,
    *,
    now: datetime,
    bootstrap: bool,
    override_reason: str | None = None,
    max_seconds: float = 1200,
    clock: Callable[[], float] = time.monotonic,
) -> RunReport:
    """One run of the stream at `now`, in the empty directory `workdir`.

    It reads the prior state (`fetch_prior`), which only `bootstrap` may find missing,
    and then a bootstrap builds and publishes the first state (see `_bootstrap`).
    Otherwise the prior manifest must list the state and the open delta
    (PriorStateError). The run reads OSV's index, and raises StaleIndexError, before it
    writes anything, for an index whose generation is below the state's cursor. It
    rolls over (`cycle.roll_over`), compares the index with the state and fetches the
    records that it calls for until `max_seconds` from its start on `clock` have passed
    (`process.process_index`), writes the index's generation to the delta as the cursor,
    and applies the delta to the state. It then publishes (`cycle.publish`): the
    manifest is checked with the gates and with the limit on removals, and the state,
    the open delta and the manifest are swapped into the current release.

    A record that does not parse is skipped and named in the report, and the rest are
    published. Every error propagates, and an error before the publish publishes
    nothing to the current release. What a rollover already added to a month's release
    stays. The next run adds the same files when it builds the same month part, which
    takes the same zstd and SQLite, and otherwise stops with CreateOnceError.
    """
    deadline = clock() + max_seconds
    requests = http.requests
    moment = instant(now)
    today = now.astimezone(UTC).date()
    prior = fetch_prior(gh, STREAM, workdir, bootstrap=bootstrap)
    # fetch_prior returns None only for a first publish that it has confirmed: with
    # bootstrap, when GitHub answers 404 for the current release.
    if prior is None:
        return _bootstrap(
            gh,
            http,
            workdir,
            moment=moment,
            today=today,
            deadline=deadline,
            clock=clock,
            requests=requests,
        )
    resume = _resume(prior, workdir)
    index = get_index(http)
    if index.generation < resume.cursor:
        raise StaleIndexError(
            f"the index's generation {index.generation} is below the stored cursor "
            f"{resume.cursor}, so it is a stale copy, and nothing was written"
        )
    added = cycle.roll_over(gh, STORE, workdir, prior, resume.carried, today)
    with (
        closing(open_db(workdir / STORE.state_db, STATE_SCHEMA)) as state,
        closing(open_db(workdir / STORE.open_delta_db, DELTA_SCHEMA)) as conn,
    ):
        # A new day's delta is numbered on from what the state has applied, and
        # today's goes on after its own last row.
        delta = Delta(conn, moment, first_seq=resume.applied + 1)
        processed = process_index(
            state, delta, http, index, deadline=deadline, clock=clock
        )
        delta.cursor(index.generation)
        _commit_and_apply(state, conn)
        coverage = _coverage(state, processed.remaining)
    failures = cycle.publish(
        gh,
        STORE,
        workdir,
        prior,
        added,
        today=today,
        moment=moment,
        data_through=_data_through(index),
        cursor=str(index.generation),
        coverage=coverage,
        complete=processed.remaining == 0,
        allowed_shrink={},
        extra=_removal_limit(processed.removed),
        override_reason=override_reason,
    )
    return _report(processed, requests=http.requests - requests, failures=failures)


def main(argv: list[str] | None = None) -> int:
    """Run the stream from the command line: `python -m osv_pypi run`.

    It needs GITHUB_TOKEN, and publishes to GITHUB_REPOSITORY, or to
    py-observatory/data when that is not set. It makes one GitHub client, whose
    writes are paced, for every call of the run, and works in a new directory in the
    folder that `--workdir` names (./work by default), which is removed afterwards.
    SQLite's temporary files go in that folder too, unless SQLITE_TMPDIR is set. It
    prints the report as JSON. Every error propagates.

    It returns 0, or 2 when a record did not parse, so that the workflow opens its
    failure issue. Either comes after the publish, so the records that did parse are
    published all the same.
    """
    parser = argparse.ArgumentParser(
        prog="python -m osv_pypi", description="The osv-pypi stream."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser(
        "run",
        help="read what changed in OSV's index since the last run, and publish it",
    )
    run.add_argument(
        "--bootstrap",
        action="store_true",
        help="build the first state from OSV's full dump: only for a stream that has "
        "never published",
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
        default=1200,
        help="stop fetching records after this many seconds (default: 1200)",
    )
    run.add_argument(
        "--workdir",
        type=Path,
        default=Path("work"),
        metavar="PATH",
        help="the folder to work in; each run makes a directory of its own there and "
        "removes it (default: work)",
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
    return 2 if report.unparsed else 0
