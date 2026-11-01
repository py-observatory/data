"""The manifest.json each stream publishes beside its data (schema version 1).

It lists every asset with its digests and row counts, and records how far the data
reaches. A stream trusts the last manifest it published, so `loads` is strict and
raises ManifestError for an unknown key, a missing key or a bad value. One allowance:
a key whose field has a default may be left out and takes that default, so a field
added later with a default does not break the manifests already published.
`schema_version` is always required, because it says what the rest of the text means.
`dumps` writes every key.
"""

import json
import re
from collections.abc import Callable, Sequence
from dataclasses import MISSING, asdict, dataclass, field, fields
from datetime import date, datetime
from functools import partial
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

SCHEMA_VERSION = 1

_HISTORY_CLASSES = ("append", "events", "snapshot", "derived")
_ROLES = ("public", "state")
_FORMATS = ("sqlite+zstd", "json")
_PRODUCER_KEYS = ("repo", "commit", "run_id", "obskit", "python")

# The digits are `[0-9]` and not `\d`, which also matches the digits of other scripts.
_SERIAL_RE = re.compile(r"[0-9]+")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_DATE_RE = re.compile(r"([0-9]{4})(?:-([0-9]{2})(?:-([0-9]{2}))?)?")


class ManifestError(ValueError):
    """The text is not a valid manifest."""


@dataclass(frozen=True)
class Watermark:
    """How far the data reaches. The granularity is serial, day, month or instant."""

    value: str
    granularity: str


@dataclass(frozen=True)
class TableInfo:
    rows: int
    history_class: str  # append, events, snapshot or derived


@dataclass(frozen=True)
class Asset:
    name: str
    tag: str
    role: str  # public or state
    format: str  # sqlite+zstd or json
    asset_bytes: int  # size and digest of the file as uploaded
    asset_sha256: str
    db_bytes: int  # size and digest of the database inside; a json asset repeats them
    db_sha256: str
    tables: dict[str, TableInfo]
    period: str | None = None  # YYYY-MM-DD, YYYY-MM or YYYY
    closed: bool = False  # a closed asset needs a period


@dataclass(frozen=True)
class Manifest:
    stream: str
    producer: dict  # repo, commit, run_id, obskit and python, all strings
    generated_at: str  # YYYY-MM-DDTHH:MM:SSZ
    complete: bool
    coverage: dict
    data_through: Watermark
    cursor: str | None
    licences: list[dict]
    assets: list[Asset]
    series_breaks: list[dict] = field(default_factory=list)
    corrections: list[dict] = field(default_factory=list)
    overrides: list[dict] = field(default_factory=list)
    schema_version: int = SCHEMA_VERSION  # always written, and required when reading


def dumps(m: Manifest) -> str:
    """The manifest as JSON with sorted keys, a 2-space indent and a trailing newline,
    so equal manifests give identical text."""
    return json.dumps(asdict(m), sort_keys=True, indent=2) + "\n"


def _bad(where: str, key: str, expected: str, value: object) -> ManifestError:
    return ManifestError(f"{where}.{key} must be {expected}, not {value!r}")


def _with_keys(
    value: object, where: str, required: Sequence[str], optional: Sequence[str] = ()
) -> dict:
    """`value` as a dict that has every key of `required` and no key beyond those and
    `optional`."""
    if not isinstance(value, dict):
        raise ManifestError(f"{where} must be an object, not {value!r}")
    missing = [name for name in required if name not in value]
    if missing:
        raise ManifestError(f"{where} is missing {', '.join(missing)}")
    unknown = sorted(set(value) - set(required) - set(optional))
    if unknown:
        raise ManifestError(f"{where} has unknown keys: {', '.join(unknown)}")
    return value


def _build[T: DataclassInstance](
    cls: type[T], value: object, where: str, also_required: tuple[str, ...] = ()
) -> T:
    """An instance of the dataclass `cls`, read from the JSON object `value`.

    The keys of the fields that have no default must be there, and so must those in
    `also_required`. A field whose key is left out takes its default, as the dataclass
    gives it. The reader for each field comes from `_READERS`. A field without one is
    an error, whatever the text holds, so a field added to a dataclass cannot be
    dropped without notice.
    """
    readers = _READERS[cls]
    all_fields = fields(cls)
    for f in all_fields:
        if f.name not in readers:
            raise TypeError(f"{cls.__name__}.{f.name} has no reader in obskit.manifest")
    required = [
        f.name
        for f in all_fields
        if (f.default is MISSING and f.default_factory is MISSING)
        or f.name in also_required
    ]
    optional = [f.name for f in all_fields if f.name not in required]
    raw = _with_keys(value, where, required, optional)
    return cls(
        **{
            f.name: readers[f.name](raw, f.name, where)
            for f in all_fields
            if f.name in raw
        }
    )


def _is_timestamp(value: str) -> bool:
    if not _TIMESTAMP_RE.fullmatch(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:  # the right shape for a time that does not exist, like month 13
        return False
    return True


def _date_parts(value: str) -> int:
    """How many of year, month and day `value` has (1 to 3) when it is a real date in
    one of the shapes YYYY, YYYY-MM and YYYY-MM-DD, and 0 when it is not."""
    match = _DATE_RE.fullmatch(value)
    if match is None:
        return 0
    year, month, day = match.groups()
    try:
        date(int(year), int(month or 1), int(day or 1))
    except ValueError:  # the right shape for a date that does not exist, like Feb 30
        return 0
    return sum(part is not None for part in (year, month, day))


# What a watermark value looks like at each granularity: how an error says so, and a
# test for a value. A day, month or instant must be a real date or time.
_WATERMARK_VALUES: dict[str, tuple[str, Callable[[str], bool]]] = {
    "serial": ("digits only", lambda value: _SERIAL_RE.fullmatch(value) is not None),
    "day": ("YYYY-MM-DD", lambda value: _date_parts(value) == 3),
    "month": ("YYYY-MM", lambda value: _date_parts(value) == 2),
    "instant": ("YYYY-MM-DDTHH:MM:SSZ", _is_timestamp),
}
_GRANULARITIES = tuple(_WATERMARK_VALUES)


# The readers below take the dict that holds the value, the key to read and the path
# of that dict for error messages. `_build` calls one only for a key that is there.


def _string(raw: dict, key: str, where: str) -> str:
    value = raw[key]
    if not isinstance(value, str):
        raise _bad(where, key, "a string", value)
    return value


def _optional_string(raw: dict, key: str, where: str) -> str | None:
    value = raw[key]
    if value is not None and not isinstance(value, str):
        raise _bad(where, key, "a string or null", value)
    return value


def _count(raw: dict, key: str, where: str) -> int:
    value = raw[key]
    # `type`, not isinstance: JSON true reads as a Python bool, and a bool is an int.
    if type(value) is not int or value < 0:
        raise _bad(where, key, "a whole number from 0 up", value)
    return value


def _flag(raw: dict, key: str, where: str) -> bool:
    value = raw[key]
    if not isinstance(value, bool):
        raise _bad(where, key, "true or false", value)
    return value


def _choice(raw: dict, key: str, where: str, allowed: tuple[str, ...]) -> str:
    value = raw[key]
    if not (isinstance(value, str) and value in allowed):
        raise _bad(where, key, f"one of {', '.join(allowed)}", value)
    return value


def _sha256(raw: dict, key: str, where: str) -> str:
    value = raw[key]
    if not (isinstance(value, str) and _SHA256_RE.fullmatch(value)):
        raise _bad(where, key, "64 lowercase hex characters", value)
    return value


def _timestamp(raw: dict, key: str, where: str) -> str:
    value = raw[key]
    if not (isinstance(value, str) and _is_timestamp(value)):
        raise _bad(where, key, "a UTC time like 2026-11-03T01:02:03Z", value)
    return value


def _period(raw: dict, key: str, where: str) -> str | None:
    value = raw[key]
    if value is not None and not (isinstance(value, str) and _date_parts(value)):
        raise _bad(where, key, "YYYY-MM-DD, YYYY-MM, YYYY or null", value)
    return value


def _schema_version(raw: dict, key: str, where: str) -> int:
    value = raw[key]
    # `type` and not ==: True and 1.0 both equal 1.
    if type(value) is not int or value != SCHEMA_VERSION:
        raise ManifestError(
            f"{where}.{key} is {value!r}: "
            f"this reader understands only schema version {SCHEMA_VERSION}"
        )
    return value


def _mapping(raw: dict, key: str, where: str) -> dict:
    value = raw[key]
    if not isinstance(value, dict):
        raise _bad(where, key, "an object", value)
    return value


def _list(raw: dict, key: str, where: str) -> list:
    value = raw[key]
    if not isinstance(value, list):
        raise _bad(where, key, "a list", value)
    return value


def _objects(raw: dict, key: str, where: str) -> list[dict]:
    items = _list(raw, key, where)
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise _bad(where, f"{key}[{i}]", "an object", item)
    return items


def _producer(raw: dict, key: str, where: str) -> dict:
    here = f"{where}.{key}"
    producer = _with_keys(raw[key], here, _PRODUCER_KEYS)
    for name in _PRODUCER_KEYS:
        _string(producer, name, here)
    return producer


def _watermark(raw: dict, key: str, where: str) -> Watermark:
    here = f"{where}.{key}"
    mark = _build(Watermark, raw[key], here)
    shape, fits = _WATERMARK_VALUES[mark.granularity]
    if not fits(mark.value):
        expected = f"{shape} when the granularity is {mark.granularity}"
        raise _bad(here, "value", expected, mark.value)
    return mark


def _tables(raw: dict, key: str, where: str) -> dict[str, TableInfo]:
    return {
        name: _build(TableInfo, value, f"{where}.{key}.{name}")
        for name, value in _mapping(raw, key, where).items()
    }


def _asset(value: object, where: str) -> Asset:
    asset = _build(Asset, value, where)
    if asset.closed and asset.period is None:
        raise ManifestError(f"{where} is closed, so it needs a period")
    if asset.format == "json" and (asset.db_bytes, asset.db_sha256) != (
        asset.asset_bytes,
        asset.asset_sha256,
    ):
        raise ManifestError(
            f"{where} is a json asset, so db_bytes and db_sha256 must equal "
            "asset_bytes and asset_sha256"
        )
    return asset


def _assets(raw: dict, key: str, where: str) -> list[Asset]:
    """The assets, each listed once: a release holds one asset of a name, so a second
    entry for it leaves a reader unable to tell which one is meant."""
    items = _list(raw, key, where)
    assets = [_asset(item, f"{where}.{key}[{i}]") for i, item in enumerate(items)]
    seen: set[tuple[str, str]] = set()
    for i, asset in enumerate(assets):
        if (asset.tag, asset.name) in seen:
            raise ManifestError(
                f"{where}.{key}[{i}] lists {asset.name} of the release {asset.tag} "
                "again"
            )
        seen.add((asset.tag, asset.name))
    return assets


# The reader of each field of each dataclass. A new field needs its entry here, and
# a field with a default may then be left out of the JSON.
_Reader = Callable[[dict, str, str], object]
_READERS: dict[type, dict[str, _Reader]] = {
    Watermark: {
        "value": _string,
        "granularity": partial(_choice, allowed=_GRANULARITIES),
    },
    TableInfo: {
        "rows": _count,
        "history_class": partial(_choice, allowed=_HISTORY_CLASSES),
    },
    Asset: {
        "name": _string,
        "tag": _string,
        "role": partial(_choice, allowed=_ROLES),
        "format": partial(_choice, allowed=_FORMATS),
        "asset_bytes": _count,
        "asset_sha256": _sha256,
        "db_bytes": _count,
        "db_sha256": _sha256,
        "tables": _tables,
        "period": _period,
        "closed": _flag,
    },
    Manifest: {
        "stream": _string,
        "producer": _producer,
        "generated_at": _timestamp,
        "complete": _flag,
        "coverage": _mapping,
        "data_through": _watermark,
        "cursor": _optional_string,
        "licences": _objects,
        "assets": _assets,
        "series_breaks": _objects,
        "corrections": _objects,
        "overrides": _objects,
        "schema_version": _schema_version,
    },
}


def loads(text: str) -> Manifest:
    """Parse and validate the text of a manifest.

    Raises ManifestError for text that is not JSON, a schema_version other than 1, an
    unknown key at any level, a missing key (one whose field has no default, and
    schema_version), and any value that breaks the format's rules: the JSON type of
    each field, the allowed roles, formats, granularities and history classes,
    lowercase 64-digit sha256 values, real dates and times in the stated shapes, a
    watermark value that fits its granularity, a period on every closed asset, a
    json asset that repeats its digests, and no two assets with one tag and name.
    """
    try:
        doc = json.loads(text)
    except ValueError as exc:
        raise ManifestError(f"the text could not be parsed as JSON: {exc}") from exc
    # Read the version before the keys: a later schema may name its fields differently.
    if isinstance(doc, dict) and "schema_version" in doc:
        _schema_version(doc, "schema_version", "manifest")
    return _build(Manifest, doc, "manifest", also_required=("schema_version",))
