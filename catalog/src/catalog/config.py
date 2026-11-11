"""What the catalog reads: the streams it admits, the tables of each, and its limits.

`catalog.toml` is the list. A stream is read only if it is in it, and only the tables
listed for that stream are read, so a new stream or a new table reaches the catalog by a
change to that file. `load_config` refuses a file that is not in the shape it expects,
and names the key at fault, because a limit that was misspelled and then ignored would
leave the catalog running on a limit that nobody chose.
"""

import tomllib
from dataclasses import dataclass
from pathlib import Path

from catalog.schema import CATALOG
from obskit.tags import check_tag, current_tag

# Next to the package's pyproject.toml, as a stream keeps its stream.toml.
CONFIG_PATH = Path(__file__).resolve().parents[2] / "catalog.toml"

_BUILD_KEYS = (
    "min_gap_minutes",
    "max_gap_hours",
    "pin_wait_hours",
    "download_daily_years",
)
_STREAM_KEYS = ("tables", "stale_after_hours")


@dataclass(frozen=True)
class StreamAdmission:
    """What the catalog reads of one stream: the tables it takes from the stream's
    `-current` release, and how many hours the stream's `data_through` may stand still
    before the stream counts as stale."""

    stream: str
    tables: frozenset[str]
    stale_after_hours: int


@dataclass(frozen=True)
class Config:
    """The admitted streams, by id and in the order of the file, and the limits of a
    build. A build waits `min_gap_minutes` after the last one, and one is made anyway
    once the last is `max_gap_hours` old. A day's pin waits `pin_wait_hours` after the
    day for a stream to seal it. A build keeps the daily downloads of the newest
    `download_daily_years` years: the year of the newest day counted, and the years
    before it."""

    streams: dict[str, StreamAdmission]
    min_gap_minutes: int
    max_gap_hours: int
    pin_wait_hours: int
    download_daily_years: int


def load_config(path: Path = CONFIG_PATH) -> Config:
    """Read the admission list at `path`.

    The file has a `build` table with the four limits and a `streams` table with one
    table for each stream, which gives that stream's `tables` and `stale_after_hours`,
    and it has no other key. A limit is a whole number of at least 1, and
    `min_gap_minutes` is shorter than `max_gap_hours`. A stream's id must be one that
    the kit's tags accept, and it cannot be the catalog's own. A stream lists at least
    one table, and none twice.

    Raises ValueError for anything else, and names the key as the file spells it.
    """
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    _check_keys(raw, "", ("build", "streams"))

    build = _table(raw["build"], "build")
    _check_keys(build, "build", _BUILD_KEYS)
    limits = {key: _limit(build, "build", key) for key in _BUILD_KEYS}
    longest = limits["max_gap_hours"] * 60
    if limits["min_gap_minutes"] >= longest:
        raise ValueError(
            f"build.min_gap_minutes ({limits['min_gap_minutes']}) must be less than "
            f"build.max_gap_hours ({limits['max_gap_hours']}), which is {longest} minutes"
        )

    streams = {
        stream: _admission(stream, table)
        for stream, table in _table(raw["streams"], "streams").items()
    }
    return Config(streams=streams, **limits)


def _admission(stream: str, table: object) -> StreamAdmission:
    """The stream `stream`, from `table`, which is what follows `[streams.<stream>]`."""
    where = f"streams.{stream}"
    if stream == CATALOG:
        raise ValueError(f"{where}: the catalog does not read itself")
    # A TagError, which is a ValueError, says that no tag could have this id.
    check_tag(stream, current_tag(stream))

    table = _table(table, where)
    _check_keys(table, where, _STREAM_KEYS)
    return StreamAdmission(
        stream=stream,
        tables=_table_names(table["tables"], f"{where}.tables"),
        stale_after_hours=_limit(table, where, "stale_after_hours"),
    )


def _table_names(value: object, where: str) -> frozenset[str]:
    """The tables listed at `where`: names in a list, at least one, and none twice. A
    string is refused, because it would be read as its letters."""
    if not isinstance(value, list) or not all(isinstance(name, str) for name in value):
        raise ValueError(f"{where} must be a list of table names")
    if not value:
        raise ValueError(f"{where} lists no tables")
    seen: set[str] = set()
    for name in value:
        if name in seen:
            raise ValueError(f"{where} lists {name!r} twice")
        seen.add(name)
    return frozenset(seen)


def _limit(table: dict, where: str, key: str) -> int:
    """The limit at `key` of `table`: a whole number of at least 1."""
    value = table[key]
    # TOML's `true` loads as a bool, which Python counts as an int, and no limit is a flag.
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(
            f"{_spell(where, key)} must be a whole number of at least 1, not {value!r}"
        )
    return value


def _table(value: object, where: str) -> dict:
    """`value`, if the file wrote a table at `where`."""
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a table")
    return value


def _check_keys(table: dict, where: str, keys: tuple[str, ...]) -> None:
    """Raise ValueError for a key of `table` that is not in `keys`, and then for one of
    `keys` that `table` lacks. The unknown key is named first, because it is often the
    missing one, misspelled."""
    for key in table:
        if key not in keys:
            raise ValueError(f"unknown key {_spell(where, key)}")
    for key in keys:
        if key not in table:
            raise ValueError(f"missing key {_spell(where, key)}")


def _spell(where: str, key: str) -> str:
    """A key as an error names it: the table it is in, then its own name, as in
    `build.max_gap_hours`. The top level of the file has no table."""
    return f"{where}.{key}" if where else key
