"""The catalog's run: read the streams' releases, judge whether a build is due, and
publish the serving database as `catalog-current`.

`run_once` is one run. It reads the catalog's own last publish from `catalog-current`,
which fails closed: a manifest that cannot be read ends the run, and only a bootstrap
starts from nothing. It then reads the manifest of each stream that catalog.toml admits,
which fails open: a stream that cannot be read is named in the report, and its tables
are carried over from the last build.

A stream's freshness is judged by how long its `data_through` has stood still. The
catalog keeps, in its coverage, when it first saw each stream's value, and a stream
whose value is older than its `stale_after_hours` is stale.

The first run of a month copies the last build of the month before into
`catalog-month-YYYY-MM` before anything else is written (see `pins.copy_month`), and a
failed copy stops the run.

A run builds on a bootstrap, and when it is the first run of a month. It builds when
the assets that it reads of a stream have changed since the last build, or when the
last build's manifest no longer says how a stream stands: a stream that it carried can
be read again, one that it read cannot, or one has turned stale or fresh. That waits
until `min_gap_minutes` have passed since the last build, and a build is made anyway
once the last build is `max_gap_hours` old. Every other run builds nothing. A build
stages each stream that was read, carries each stream that could not be read or staged
from the last build's database, and builds the serving database. Its manifest says
which streams were carried and which are stale, and the data is complete only when none
is. The kit's gates check the manifest against the last one, so a build from older
inputs than the last build's waits for a person's reason. Then it is swapped into
`catalog-current`, which is the only release that GitHub shows as Latest.

Every run writes the daily pins that are due and ready (see `pins`). They depend on the
streams' manifests alone, so a build writes them before it publishes, and its manifest
records the last of them. A run that builds nothing writes them too, and the next build
records them.

A first build needs every stream, so on a bootstrap a stream that cannot be read or
staged stops the run. `main` exits 2, after the publish, when a stream was carried,
stale or unreadable, so that the workflow opens its failure issue.
"""

import argparse
import json
import os
import shutil
import sqlite3
import warnings
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from obskit.cycle import gate
from obskit.github import GitHub, GitHubError
from obskit.manifest import Asset, Manifest, Watermark, instant, producer
from obskit.release import (
    PriorStateError,
    fetch_assets,
    fetch_prior,
    prepare_asset,
    publish_current,
)
from obskit.runs import github_from_env, run_directory
from obskit.sqlite import table_counts
from obskit.tags import current_tag

from catalog.config import Config, StreamAdmission, load_config
from catalog.inputs import StreamRead, read_stream
from catalog.pins import (
    copy_month,
    due_days,
    pin_document,
    pin_ready,
    pin_tag,
    serving_asset,
    write_pin,
)
from catalog.schema import CATALOG, SERVING_DB
from catalog.serving import (
    BUDGET_BYTES,
    CarryError,
    StreamInput,
    build_serving,
    over_budget,
)
from catalog.stage import STAGERS, OsvStage, RegistryStage

# The reasons for which a run builds. Every other reason builds nothing.
_BUILDS = frozenset({"bootstrap", "month began", "inputs changed", "heartbeat"})

# What makes a stream that was read fail to stage, and so be carried: a release that
# does not hold what its manifest lists, a failed download, a database that is not of
# its stream's schema, a record or an event in two parts, a part that lacks a column,
# and a full disk.
_STAGE_ERRORS = (PriorStateError, GitHubError, ValueError, sqlite3.Error, OSError)


@dataclass(frozen=True)
class RunReport:
    """What a run did.

    `built` says whether it built and published, and `reason` why or why not:
    bootstrap, month began (the first run of a month), inputs changed, heartbeat, too
    soon (an input changed, and the last build is less than min_gap_minutes old) or
    unchanged. `carried` names the streams whose tables the build took from the last
    build, `stale` those whose data_through has stood still for too long, and
    `unreadable` those whose release could not be read, each in the order of
    catalog.toml. `pins` names the releases of the daily pins that the run wrote, in
    day order, and `month_copy` the release of the month's copy that it made, or None.
    `unparsed_requirements` counts the requirements in the registry's records that PEP
    508 refused.
    """

    built: bool
    reason: str
    carried: list[str]
    stale: list[str]
    unreadable: list[str]
    pins: list[str]
    month_copy: str | None
    unparsed_requirements: int


def _time(text: str) -> datetime:
    """A time as a manifest writes it (see obskit.manifest.instant), read back."""
    return datetime.fromisoformat(text)


def _read_streams(gh: GitHub, config: Config, workdir: Path) -> dict[str, StreamRead]:
    """Read each admitted stream's manifest into `workdir/<stream>`, by stream and in
    the order of catalog.toml. A stream that cannot be read gives its reason."""
    reads = {}
    for stream, admission in config.streams.items():
        directory = workdir / stream
        directory.mkdir()
        reads[stream] = read_stream(gh, admission, directory)
    return reads


def _seen(read: StreamRead, last: dict[str, dict], now: datetime) -> dict:
    """What the run knows of a stream, as its coverage gives it, but for `stale`.

    `last` is the coverage of each stream in the last build. A stream that was read
    gives its manifest's values. Its data_through_since is the last build's while its
    data_through is unchanged, and `now` once it has changed, so it says when the
    catalog first saw the value. A stream that could not be read keeps the last
    build's values (see `_carried`).
    """
    if read.manifest is None or read.fingerprint is None:
        return _carried(read.stream, last, read.error or "")
    data_through = asdict(read.manifest.data_through)
    before = last.get(read.stream)
    if before is not None and before["data_through"] == data_through:
        since = before["data_through_since"]
    else:
        since = instant(now)
    return {
        "carried": False,
        "data_through": data_through,
        "data_through_since": since,
        "error": None,
        "fingerprint": read.fingerprint,
        "generated_at": read.manifest.generated_at,
    }


def _carried(stream: str, last: dict[str, dict], error: str) -> dict:
    """The coverage of a stream whose tables are carried from the last build: what the
    last build read of it, and `error`, which says why it could not be read or staged.

    Raises CarryError when the last build lacks the stream, and so always on a
    bootstrap, which has no last build: there is nothing to carry.
    """
    before = last.get(stream)
    if before is None:
        raise CarryError(
            f"{stream} could not be read or staged, and no earlier build of the "
            f"catalog holds it to carry: {error}"
        )
    return {
        "carried": True,
        "data_through": before["data_through"],
        "data_through_since": before["data_through_since"],
        "error": error,
        "fingerprint": before["fingerprint"],
        "generated_at": before["generated_at"],
    }


def _is_stale(seen: dict, admission: StreamAdmission, now: datetime) -> bool:
    """Whether the stream's data_through has stood still for longer than its
    stale_after_hours."""
    standing = now - _time(seen["data_through_since"])
    return standing > timedelta(hours=admission.stale_after_hours)


def _judged(seen: dict[str, dict], config: Config, now: datetime) -> dict[str, dict]:
    """Each stream's coverage: what the run knows of it, and whether it is stale."""
    return {
        stream: {**known, "stale": _is_stale(known, config.streams[stream], now)}
        for stream, known in seen.items()
    }


def _stale(streams: dict[str, dict]) -> list[str]:
    """The streams that their coverage, `streams`, calls stale."""
    return [stream for stream, coverage in streams.items() if coverage["stale"]]


def _changed(read: StreamRead, known: dict, last: dict[str, dict]) -> bool:
    """Whether the last build does not hold the stream as the run finds it, which
    `known`, its coverage, gives: the last build lacks it, carried it and it can be read
    now, read it and it cannot be read now, judged it stale or fresh otherwise, or read
    other assets of it.

    A stream that the last build carried has changed even when its fingerprint is the
    one recorded, which is that of its last good input. The tables would be the same,
    but the last build's manifest says that the stream could not be read, and would go
    on saying so after the stream recovered. In the same way, a stream that cannot be
    read, or has turned stale, changes what the manifest says of it, though no asset
    that the build reads has changed.
    """
    before = last.get(read.stream)
    if before is None:
        return True
    unreadable = read.error is not None
    if before["carried"] != unreadable or before["stale"] != known["stale"]:
        return True
    return not unreadable and before["fingerprint"] != read.fingerprint


def _decide(
    prior: Manifest | None,
    reads: dict[str, StreamRead],
    known: dict[str, dict],
    now: datetime,
    config: Config,
    *,
    month_began: bool,
) -> str:
    """Why the run builds, or why it does not, judged in this order: a bootstrap; the
    first run of a month, which `month_began` says, as it has made the month's copy;
    an input that changed (see `_changed`), once min_gap_minutes have passed since the
    last build, and "too soon" before that; a last build that is max_gap_hours old (the
    heartbeat); and otherwise "unchanged". `known` is the coverage of each stream as
    the run finds it. The catalog publishes only when it builds, so the last publish is
    the last build."""
    if prior is None:
        return "bootstrap"
    if month_began:
        return "month began"
    since_build = now - _time(prior.generated_at)
    last = prior.coverage["streams"]
    if any(_changed(read, known[stream], last) for stream, read in reads.items()):
        if since_build >= timedelta(minutes=config.min_gap_minutes):
            return "inputs changed"
        return "too soon"
    if since_build >= timedelta(hours=config.max_gap_hours):
        return "heartbeat"
    return "unchanged"


def _stage(
    gh: GitHub, read: StreamRead, directory: Path, *, bootstrap: bool
) -> tuple[RegistryStage | OsvStage | None, str | None]:
    """Stage the stream of `read` in `directory`. Returns the stage, or None and the
    reason that there is none: the stream could not be read, or could not be staged.

    A stream that fails to stage is carried, so its directory is deleted at once, and
    its files do not hold the disk through the build. On a bootstrap, which has nothing
    to carry, the error is raised.
    """
    if read.error is not None:
        return None, read.error
    try:
        return STAGERS[read.stream](gh, read.assets, directory), None
    except _STAGE_ERRORS as exc:
        if bootstrap:
            raise
        shutil.rmtree(directory)
        return None, str(exc)


def _inputs(
    gh: GitHub,
    workdir: Path,
    prior: Manifest | None,
    reads: dict[str, StreamRead],
    seen: dict[str, dict],
    *,
    bootstrap: bool,
) -> tuple[list[StreamInput], dict[str, dict]]:
    """Stage each stream that was read, in `workdir/<stream>`, and return the build's
    inputs, with what the run knows of each stream once it is staged or carried (see
    `_seen` and `_carried`).

    There is exactly one input for each admitted stream, in the order of catalog.toml:
    the build carries a stream whose input has no stage, and would drop one that had
    no input at all. A carried stream keeps the licences that the last build listed for
    it, as it keeps that build's attribution.
    """
    last = prior.coverage["streams"] if prior else {}
    inputs: list[StreamInput] = []
    known: dict[str, dict] = {}
    for stream, read in reads.items():
        staged, error = _stage(gh, read, workdir / stream, bootstrap=bootstrap)
        if staged is None or read.manifest is None:
            known[stream] = _carried(stream, last, error or "")
            licences = _licences_of(prior, stream)
        else:
            known[stream] = seen[stream]
            licences = read.manifest.licences
        inputs.append(_input(stream, known[stream], licences, staged))
    return inputs, known


def _licences_of(prior: Manifest | None, stream: str) -> list[dict]:
    """The licences of `stream` that the last build listed, for a stream that is
    carried from it, as its attribution is."""
    listed = prior.licences if prior else []
    return [licence for licence in listed if licence.get("stream") == stream]


def _input(
    stream: str,
    known: dict,
    licences: list[dict],
    staged: RegistryStage | OsvStage | None,
) -> StreamInput:
    """What the build has of `stream`, from `known`, what the run knows of it (see
    `_seen` and `_carried`)."""
    return StreamInput(
        stream=stream,
        generated_at=known["generated_at"],
        data_through=Watermark(**known["data_through"]),
        fingerprint=known["fingerprint"],
        licences=licences,
        staged=staged,
    )


def _previous_build(gh: GitHub, prior: Manifest, workdir: Path) -> Path:
    """Download the last build's serving database into `workdir/previous`, for the
    streams that are carried from it, and return the database."""
    directory = workdir / "previous"
    directory.mkdir()
    [previous] = fetch_assets(gh, [serving_asset(prior)], directory)
    return previous


def _pack(db: Path, workdir: Path) -> tuple[Path, Asset, bool]:
    """Compress the serving database `db` into `workdir` for `catalog-current`, every
    table of it derived, since each build makes it afresh. Returns the file, its entry
    and whether it is over the budget, which warns."""
    classes = {table: "derived" for table in table_counts(db)}
    packed, entry = prepare_asset(
        db, workdir, tag=current_tag(CATALOG), role="public", classes=classes
    )
    over = over_budget(packed)
    if over:
        warnings.warn(
            f"{packed.name} is {packed.stat().st_size:,} bytes, over the budget of "
            f"{BUDGET_BYTES:,}",
            stacklevel=3,
        )
    return packed, entry, over


def _copy_month(
    gh: GitHub, prior: Manifest | None, workdir: Path, now: datetime
) -> list[Asset]:
    """Make the month's copy when a month has begun since the last build (see
    `copy_month`), in `workdir/month`, so that its download never meets the new
    build's database in `workdir`. Returns its entries, or [] when there is none to
    make, as on a bootstrap, which has no last build."""
    if prior is None:
        return []
    directory = workdir / "month"
    directory.mkdir()
    return copy_month(gh, prior, directory, now)


def _write_pins(
    gh: GitHub,
    workdir: Path,
    reads: dict[str, StreamRead],
    now: datetime,
    config: Config,
    first_build: str,
    last_pin: str | None,
) -> tuple[list[str], str | None]:
    """Write the pins that are due (see `due_days`), in day order, from `workdir`, and
    stop at the first day whose pin is not ready (see `pin_ready`). Each pin gives the
    commit that the run's code is at, which a workflow sets as GITHUB_SHA, and none
    outside one.

    Returns the releases of the pins written and the last day written or found, which
    is `last_pin` when there is none. Days are YYYY-MM-DD, as the coverage gives them.
    A pin that its release holds already is left alone (see `write_pin`), and its day
    still counts as the last pin found, so the build after a run that wrote pins and
    built nothing records them.
    """
    listed = list(reads.values())
    commit = os.environ.get("GITHUB_SHA", "")
    found = None if last_pin is None else date.fromisoformat(last_pin)
    today = now.astimezone(UTC).date()
    written = []
    for day in due_days(date.fromisoformat(first_build), found, today):
        if not pin_ready(day, listed, now, config.pin_wait_hours):
            break
        document = pin_document(day, listed, config, gh.repo, commit)
        if write_pin(gh, day, document, workdir):
            written.append(pin_tag(day))
        found = day
    return written, None if found is None else found.isoformat()


def _licences(inputs: list[StreamInput]) -> list[dict]:
    """The licences of every input, each with its stream, sorted by stream and then by
    name, and each listed once."""
    once: dict[str, dict] = {}
    for item in inputs:
        for licence in item.licences:
            entry = {**licence, "stream": item.stream}
            once[json.dumps(entry, sort_keys=True)] = entry
    return sorted(once.values(), key=lambda entry: (entry["stream"], entry["name"]))


def run_once(
    gh: GitHub,
    workdir: Path,
    now: datetime,
    *,
    config: Config,
    bootstrap: bool,
    override_reason: str | None,
) -> RunReport:
    """One run of the catalog at `now`, in the empty directory `workdir`, over the
    streams that `config` admits.

    It reads the catalog's last publish (`fetch_prior`), which only `bootstrap` may find
    missing, and each stream's manifest, and judges each stream's freshness. When a
    month has begun since the last build, it copies that build into the month's
    release (see `copy_month`). Then it decides whether to build (see `_decide`), and
    a run that does not build writes the daily pins that are due (see `_write_pins`)
    and returns there.

    A build stages each stream that was read, and carries each one that could not be
    read or staged from the last build's serving database, which it downloads for
    them. It builds the serving database, which has one input for each admitted
    stream, carried or not, and compresses it. It writes the daily pins that are due.
    Its manifest says how far the oldest input reaches, says of each stream whether it
    was carried and whether it is stale, records the last pin, and lists the month's
    copy beside the last build's closed assets. Without `bootstrap`, the manifest is
    checked against the last one with the kit's gates, which raise GateError, before
    anything is published, unless `override_reason` gives a person's reason. Then the
    database and the manifest are swapped into `catalog-current`, which a bootstrap
    creates as the Latest release.

    On a bootstrap, a stream that cannot be read raises CarryError, and one that cannot
    be staged raises its error, because a first build needs every stream. Every other
    error propagates, a failed month's copy included, and a run that fails publishes
    nothing.
    """
    moment = instant(now)
    prior_dir = workdir / "prior"
    prior_dir.mkdir()
    prior = fetch_prior(gh, CATALOG, prior_dir, bootstrap=bootstrap)
    last = prior.coverage["streams"] if prior else {}
    reads = _read_streams(gh, config, workdir)
    seen = {stream: _seen(read, last, now) for stream, read in reads.items()}
    known = _judged(seen, config, now)
    unreadable = [stream for stream, read in reads.items() if read.error is not None]
    month_copy = _copy_month(gh, prior, workdir, now)
    reason = _decide(prior, reads, known, now, config, month_began=bool(month_copy))
    first_build = (
        prior.coverage["first_build"] if prior else f"{now.astimezone(UTC):%Y-%m-%d}"
    )
    last_pin = prior.coverage["last_pin"] if prior else None
    if reason not in _BUILDS:
        pins, _ = _write_pins(gh, workdir, reads, now, config, first_build, last_pin)
        return RunReport(
            built=False,
            reason=reason,
            carried=[],
            stale=_stale(known),
            unreadable=unreadable,
            pins=pins,
            month_copy=None,
            unparsed_requirements=0,
        )

    inputs, seen = _inputs(gh, workdir, prior, reads, seen, bootstrap=bootstrap)
    carrying = any(item.staged is None for item in inputs)
    previous = _previous_build(gh, prior, workdir) if prior and carrying else None
    db = workdir / SERVING_DB
    built = build_serving(db, inputs, previous)
    packed, entry, over = _pack(db, workdir)
    # The pins are written before the manifest is made, so that it records them.
    pins, last_pin = _write_pins(gh, workdir, reads, now, config, first_build, last_pin)
    streams = _judged(seen, config, now)
    stale = _stale(streams)
    closed = [asset for asset in prior.assets if asset.closed] if prior else []
    held = {(asset.tag, asset.name) for asset in closed}
    copied = [asset for asset in month_copy if (asset.tag, asset.name) not in held]
    manifest = Manifest(
        stream=CATALOG,
        producer=producer(gh.repo),
        generated_at=moment,
        complete=not (built.carried or stale or unreadable),
        coverage={
            "first_build": first_build,
            "last_pin": last_pin,
            "over_budget": over,
            "streams": streams,
        },
        # Every generated_at is written to the second in UTC, as obskit.manifest
        # checks, so the oldest is the least as text.
        data_through=Watermark(min(item.generated_at for item in inputs), "instant"),
        cursor=None,
        licences=_licences(inputs),
        assets=[entry, *closed, *copied],
        series_breaks=list(prior.series_breaks) if prior else [],
        corrections=list(prior.corrections) if prior else [],
        overrides=list(prior.overrides) if prior else [],
    )
    if prior is not None:
        manifest, _ = gate(
            prior,
            manifest,
            allowed_shrink={},
            extra=[],
            override_reason=override_reason,
            moment=moment,
        )
    publish_current(gh, CATALOG, [packed], manifest, bootstrap=bootstrap, latest=True)
    return RunReport(
        built=True,
        reason=reason,
        carried=built.carried,
        stale=stale,
        unreadable=unreadable,
        pins=pins,
        month_copy=month_copy[0].tag if month_copy else None,
        unparsed_requirements=built.unparsed_requirements,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the catalog from the command line: `python -m catalog run`.

    It needs GITHUB_TOKEN, and publishes to GITHUB_REPOSITORY, or to
    py-observatory/data when that is not set (see obskit.runs). It works in a new
    directory in the folder that `--workdir` names (./work by default), which is
    removed afterwards, and SQLite's temporary files go in that folder too, unless
    SQLITE_TMPDIR is set. It prints the report as JSON. Every error propagates.

    It returns 2 when a stream was carried, is stale or could not be read, so that the
    workflow opens its failure issue, and 0 otherwise. A run that builds returns
    either after its publish.
    """
    parser = argparse.ArgumentParser(
        prog="python -m catalog",
        description="The catalog: the streams' releases, read together into one "
        "serving database.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser(
        "run",
        help="read the streams' releases, and build and publish catalog-current when "
        "a build is due",
    )
    run.add_argument(
        "--bootstrap",
        action="store_true",
        help="make the first build: only for a catalog that has never published, and "
        "every stream must be read",
    )
    run.add_argument(
        "--override-reason",
        default="",
        metavar="TEXT",
        help="publish although a gate failed, for this reason, which the manifest logs",
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
    config = load_config()
    gh = github_from_env()
    with run_directory(args.workdir) as workdir:
        report = run_once(
            gh,
            workdir,
            datetime.now(UTC),
            config=config,
            bootstrap=args.bootstrap,
            override_reason=args.override_reason or None,
        )
    print(json.dumps(asdict(report)))
    return 2 if report.carried or report.stale or report.unreadable else 0
