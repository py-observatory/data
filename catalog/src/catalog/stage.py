"""Each stream's admitted tables, downloaded from its release and staged for a build.

A stage holds what the builders read of a stream. osv's admitted table is a table of its
state, so its stage is its state as downloaded. Three of the registry's admitted tables
are tables of its state and three are tables of its deltas, which are large: their files
alone are tens of millions of rows. So `stage_registry` reads the parts one at a time,
keeps of each only what a build needs, and deletes each before it downloads the next, so
that the disk never holds the files twice. It sums each release's files into one row,
joined with the state's files_current for what each file is now, and copies the release
records and the events as they are.

A downloaded database is data from outside. Each is opened with obskit.sqlite.open_db
against its stream's schema, which refuses one that lacks a table, and the stage reads
the registry's through read-only attachments. Nothing in them is run.
"""

import shutil
import sqlite3
import sys
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import osv_pypi
import pypi_registry
from obskit.github import GitHub
from obskit.manifest import Asset
from obskit.release import fetch_assets
from obskit.sqlite import open_db
from osv_pypi import store as osv_store
from pypi_registry import store as registry_store

# The tables that the stage copies from each of the registry's parts, as they are.
_COPIED = ("release_records", "events")
_STAGE_DB = "stage.db"


@dataclass(frozen=True)
class RegistryStage:
    """The registry, staged: its state, as downloaded, which the builders read, and the
    stage, which holds what they take from its deltas (see STAGE_SCHEMA)."""

    state: Path
    stage: Path


@dataclass(frozen=True)
class OsvStage:
    """osv, staged: its state, as downloaded, which holds its advisories."""

    state: Path


def _registry_tables(tables: tuple[str, ...]) -> str:
    """The statements that make `tables` as the registry's delta schema makes them, read
    back from a database made with that schema. A table made with them has the
    registry's columns in its order, each with the registry's type or with none."""
    with closing(sqlite3.connect(":memory:")) as scratch:
        scratch.executescript(registry_store.DELTA_SCHEMA)
        made = dict(
            scratch.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table'")
        )
    return "".join(f"\n{made[table]};\n" for table in tables)


# What a build takes from the registry's deltas.
#
# release_files has one row for each release whose files have a version. `uploaded` is
# the earliest upload time of its files, as the registry writes it. `files_ever` counts
# the files that the deltas have seen, `files_now` those that the state still has, and
# `files_yanked` those of them that the state has yanked. `yanked_reason` is the reason
# that the first part to give one gives.
#
# release_records and events are as the registry's delta makes them, so that each is
# copied as it is.
STAGE_SCHEMA = """
CREATE TABLE release_files (
    name,
    incarnation,
    version,
    uploaded,
    files_ever INTEGER NOT NULL,
    files_now INTEGER NOT NULL,
    files_yanked INTEGER NOT NULL,
    yanked_reason,
    PRIMARY KEY (name, incarnation, version)
) WITHOUT ROWID;
""" + _registry_tables(_COPIED)

# The files of one part that have a version, summed by release and added to what the
# parts before it gave. What a file is now comes from the state's files_current: a file
# that it lacks has been removed, and counts in files_ever alone. PyPI yanks a release
# whole, so the files of a release that one part holds have one reason.
#
# min() of two values is NULL when either is NULL, so the earliest upload goes through
# coalesce, which keeps a time when only one of the two has one.
_SUM_FILES = """
INSERT INTO release_files (
    name, incarnation, version, uploaded,
    files_ever, files_now, files_yanked, yanked_reason
)
SELECT
    f.name,
    f.incarnation,
    f.version,
    min(f.upload_time),
    count(*),
    count(c.filename),
    count(*) FILTER (WHERE c.yanked),
    min(c.yanked_reason)
FROM part.files AS f
LEFT JOIN state.files_current AS c
    ON c.name = f.name AND c.incarnation = f.incarnation AND c.filename = f.filename
WHERE f.version IS NOT NULL
GROUP BY f.name, f.incarnation, f.version
ON CONFLICT (name, incarnation, version) DO UPDATE SET
    uploaded = coalesce(min(uploaded, excluded.uploaded), uploaded, excluded.uploaded),
    files_ever = files_ever + excluded.files_ever,
    files_now = files_now + excluded.files_now,
    files_yanked = files_yanked + excluded.files_yanked,
    yanked_reason = coalesce(yanked_reason, excluded.yanked_reason)
"""


def stage_registry(gh: GitHub, assets: list[Asset], workdir: Path) -> RegistryStage:
    """Download the registry's `assets` into `workdir`, which must exist, and stage
    them there.

    `assets` are as `needed_assets` gives them: the state, and then the delta parts in
    the order of their periods. The state comes first, and stays for the builders. Then
    each part in turn is downloaded, its files are summed into release_files, its
    release records and its events are copied, and it is deleted before the next one
    is downloaded, so that the disk holds one part at a time. A part is staged whole or
    not at all.

    Raises sqlite3.IntegrityError when two parts hold one release record or one event,
    which a build would otherwise count twice, ValueError for a database that lacks a
    table of the registry's schema, and as `fetch_assets` raises. The free disk is
    written to stderr as the staging starts and as it ends, as the kit's month close
    writes it.
    """
    _log_free_disk(workdir, f"staging {pypi_registry.STREAM} starts")
    state_asset, *parts = assets
    [state] = fetch_assets(gh, [state_asset], workdir)
    open_db(state, registry_store.STATE_SCHEMA).close()
    stage = workdir / _STAGE_DB
    with closing(open_db(stage, STAGE_SCHEMA, create=True)) as con:
        attach_read_only(con, state, "state")
        for part in parts:
            _stage_part(gh, con, part, workdir)
        con.execute("DETACH DATABASE state")
    _log_free_disk(workdir, f"staging {pypi_registry.STREAM} ends")
    return RegistryStage(state=state, stage=stage)


def _stage_part(
    gh: GitHub, con: sqlite3.Connection, asset: Asset, workdir: Path
) -> None:
    """Download the part `asset` into `workdir`, add what a build takes of it to the
    stage that `con` is open on, with the state attached as `state`, and delete it."""
    [part] = fetch_assets(gh, [asset], workdir)
    open_db(part, registry_store.DELTA_SCHEMA).close()
    attach_read_only(con, part, "part")
    con.execute(_SUM_FILES)
    for table in _COPIED:
        _copy(con, table)
    # The part cannot be detached while the transaction that read it is open.
    con.commit()
    con.execute("DETACH DATABASE part")
    part.unlink()


def _copy(con: sqlite3.Connection, table: str) -> None:
    """Copy the rows of `table` from the attached part into the stage with a plain
    INSERT, so that a row whose key the stage holds already raises
    sqlite3.IntegrityError. The columns are named as the stage has them, so a part that
    has them in another order is read right, and one that lacks one is refused."""
    info = con.execute(f"PRAGMA main.table_info({table})")
    columns = ", ".join(row[1] for row in info)
    con.execute(
        f"INSERT INTO main.{table} ({columns}) SELECT {columns} FROM part.{table}"
    )


def attach_read_only(con: sqlite3.Connection, path: Path, alias: str) -> None:
    """Attach the database at `path` to `con` as `alias`, read-only, so that reading it
    cannot write to it. `con` must open URIs, as the one that `open_db` gives does, and
    `alias` is written into the statement as it is, so it must be a plain name."""
    uri = f"{path.resolve(strict=True).as_uri()}?mode=ro"
    con.execute(f"ATTACH DATABASE ? AS {alias}", (uri,))


def _log_free_disk(workdir: Path, when: str) -> None:
    """Write to stderr, on one line, how much space is free on the disk that holds
    `workdir`, so that the log of a run shows what staging needs of it."""
    free = shutil.disk_usage(workdir).free
    print(f"{when}: {free / 1e9:.1f} GB free on {workdir}", file=sys.stderr)


def stage_osv(gh: GitHub, assets: list[Asset], workdir: Path) -> OsvStage:
    """Download osv's state into `workdir`, which must exist, as its stage.

    `assets` are as `needed_assets` gives them, the state first. The catalog admits only
    a table of osv's state, so the state is all that is downloaded. Raises ValueError
    for a state that lacks a table of osv's schema, and as `fetch_assets` raises.
    """
    [state] = fetch_assets(gh, assets[:1], workdir)
    open_db(state, osv_store.STATE_SCHEMA).close()
    return OsvStage(state=state)


# The tables that each stage reads of its stream. They must be the tables that
# catalog.toml admits of it, and a test holds them to that, so that a change to what is
# admitted comes with a change to the stage that reads it.
READS: dict[str, frozenset[str]] = {
    pypi_registry.STREAM: frozenset(
        {
            "projects",
            "versions_seen",
            "files_current",
            "files",
            "release_records",
            "events",
        }
    ),
    osv_pypi.STREAM: frozenset({"advisories"}),
}

# How each stream is staged, by its id.
STAGERS: dict[str, Callable[[GitHub, list[Asset], Path], RegistryStage | OsvStage]] = {
    pypi_registry.STREAM: stage_registry,
    osv_pypi.STREAM: stage_osv,
}
