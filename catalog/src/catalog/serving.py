"""The serving database, made whole from what each stream gave.

A stream whose input was staged is built from it. A stream whose input could not be read
is carried: its tables, and its `input` and `attribution` rows, are copied from the
previous build, so that the database stays whole and says, in `input.carried`, which
stream it could not refresh. The joint tables are never carried. `affected` and the
search index are made again from whatever the stream tables hold, carried or fresh, so a
new advisory meets the releases of a carried registry, and a new release meets the
advisories of a carried osv.
"""

import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import osv_pypi
import pypi_registry
from obskit.manifest import Watermark
from obskit.sqlite import open_db, table_counts

from catalog.advisories import build_advisories, expand_affected
from catalog.records import build_records
from catalog.registry import build_registry
from catalog.schema import SERVING_SCHEMA, SOURCED
from catalog.stage import OsvStage, RegistryStage, attach_read_only

# The most that the compressed serving database may be before a run warns that it is
# over budget. The build still publishes.
BUDGET_BYTES = 200_000_000


@dataclass(frozen=True)
class StreamInput:
    """What the build has of one stream: its manifest's time, the watermark of its data,
    the fingerprint of the assets read, and its licences as its manifest lists them.
    `staged` is what the builders read, or None when the stream could not be read and
    its tables are to be carried over from the previous build."""

    stream: str
    generated_at: str
    data_through: Watermark
    fingerprint: str
    licences: list[dict]
    staged: RegistryStage | OsvStage | None


@dataclass(frozen=True)
class BuildReport:
    """What a build did: the streams it carried, the requirements that PEP 508 refused
    in the registry's records, and the rows of each table."""

    carried: list[str]
    unparsed_requirements: int
    tables: dict[str, int]


class CarryError(RuntimeError):
    """A stream could not be read, and the previous build has nothing to carry."""


def _build_registry(con: sqlite3.Connection, stage: RegistryStage) -> int:
    """The registry's tables, and the number of requirements that PEP 508 refused."""
    build_registry(con, stage)
    return build_records(con, stage)


def _build_osv(con: sqlite3.Connection, stage: OsvStage) -> int:
    """osv's tables. No requirement is read, so none is refused."""
    build_advisories(con, stage)
    return 0


# How each stream's tables are built from its stage. Each returns the requirements that
# PEP 508 refused.
BUILDERS: dict[str, Callable[..., int]] = {
    pypi_registry.STREAM: _build_registry,
    osv_pypi.STREAM: _build_osv,
}

_INSERT_INPUT = """
INSERT INTO main.input (stream, generated_at, data_through, granularity, fingerprint, carried)
VALUES (?, ?, ?, ?, ?, 0)
"""

_INSERT_ATTRIBUTION = """
INSERT INTO main.attribution (stream, upstream, url, licence, terms)
VALUES (?, ?, ?, ?, ?)
"""

_PREVIOUS_INPUT = "SELECT 1 FROM previous.input WHERE stream = ?"

_CARRY_INPUT = """
INSERT INTO main.input (stream, generated_at, data_through, granularity, fingerprint, carried)
SELECT stream, generated_at, data_through, granularity, fingerprint, 1
FROM previous.input WHERE stream = ?
"""

_CARRY_ATTRIBUTION = """
INSERT INTO main.attribution (stream, upstream, url, licence, terms)
SELECT stream, upstream, url, licence, terms
FROM previous.attribution WHERE stream = ?
"""


def build_serving(
    out: Path, inputs: list[StreamInput], previous: Path | None
) -> BuildReport:
    """Make the serving database at `out`, which must not exist, from `inputs`.

    An input with a stage is built from it. One without is carried from the database at
    `previous`: its tables with their ids, its `input` row marked carried, and its
    attribution. Raises CarryError when there is such an input and `previous` is None or
    has no `input` row for its stream. `out` is removed when the build fails, so that no
    half-made database is left to be taken for a build.
    """
    try:
        return _build(out, inputs, previous)
    except BaseException:
        out.unlink(missing_ok=True)
        raise


def _build(out: Path, inputs: list[StreamInput], previous: Path | None) -> BuildReport:
    carried: list[str] = []
    refused = 0
    with closing(open_db(out, SERVING_SCHEMA, create=True)) as con:
        for item in inputs:
            if item.staged is None:
                _carry(con, item.stream, previous)
                carried.append(item.stream)
            else:
                refused += BUILDERS[item.stream](con, item.staged)
                _write_input(con, item)
        expand_affected(con)
        con.execute("INSERT INTO search(search) VALUES ('rebuild')")
        con.commit()
        con.execute("VACUUM")
    return BuildReport(
        carried=carried, unparsed_requirements=refused, tables=table_counts(out)
    )


def _write_input(con: sqlite3.Connection, item: StreamInput) -> None:
    """Record what the build read of a stream, and the upstreams of its licences."""
    con.execute(
        _INSERT_INPUT,
        (
            item.stream,
            item.generated_at,
            item.data_through.value,
            item.data_through.granularity,
            item.fingerprint,
        ),
    )
    con.executemany(
        _INSERT_ATTRIBUTION,
        (
            (
                item.stream,
                licence["name"],
                licence.get("url"),
                licence.get("licence"),
                licence.get("terms"),
            )
            for licence in item.licences
        ),
    )
    con.commit()


def _carry(con: sqlite3.Connection, stream: str, previous: Path | None) -> None:
    """Copy the tables of `stream`, with its `input` and `attribution` rows, from the
    database at `previous`. The ids come with the rows, because both databases are made
    from one schema and the tables that link by id are carried together."""
    if previous is None:
        raise CarryError(f"{stream} could not be read, and there is no previous build")
    attach_read_only(con, previous, "previous")
    if con.execute(_PREVIOUS_INPUT, (stream,)).fetchone() is None:
        raise CarryError(f"{stream} could not be read, and the previous build lacks it")
    for table in SOURCED[stream]:
        con.execute(f"INSERT INTO main.{table} SELECT * FROM previous.{table}")
    con.execute(_CARRY_INPUT, (stream,))
    con.execute(_CARRY_ATTRIBUTION, (stream,))
    # A database cannot be detached while the transaction that read it is open.
    con.commit()
    con.execute("DETACH DATABASE previous")


def over_budget(packed: Path) -> bool:
    """Whether the compressed database at `packed` is larger than the budget."""
    return packed.stat().st_size > BUDGET_BYTES
