"""The manifest.json each stream publishes beside its data (schema version 1).

It lists every asset with its digests and row counts, and records how far the data
reaches. A stream trusts the last manifest it published, so `loads` is strict: it
accepts exactly what `dumps` writes, and raises ManifestError for anything else.
"""

import json
import re
from dataclasses import asdict, dataclass, field, fields
from datetime import date, datetime

SCHEMA_VERSION = 1

_GRANULARITIES = ("serial", "day", "month", "instant")
_HISTORY_CLASSES = ("append", "events", "snapshot", "derived")
_ROLES = ("public", "state")
_FORMATS = ("sqlite+zstd", "json")
_PRODUCER_KEYS = ("repo", "commit", "run_id", "obskit", "python")

# The digits are `[0-9]` and not `\d`, which also matches the digits of other scripts.
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_PERIOD_RE = re.compile(r"([0-9]{4})(?:-([0-9]{2})(?:-([0-9]{2}))?)?")


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
    schema_version: int = SCHEMA_VERSION


def dumps(m: Manifest) -> str:
    """The manifest as JSON with sorted keys, a 2-space indent and a trailing newline,
    so equal manifests give identical text."""
    return json.dumps(asdict(m), sort_keys=True, indent=2) + "\n"


def _names(cls: type) -> tuple[str, ...]:
    return tuple(f.name for f in fields(cls))


def _bad(where: str, key: str, expected: str, value: object) -> ManifestError:
    return ManifestError(f"{where}.{key} must be {expected}, not {value!r}")


def _exact(value: object, where: str, names: tuple[str, ...]) -> dict:
    """`value` as a dict that has exactly the keys `names`."""
    if not isinstance(value, dict):
        raise ManifestError(f"{where} must be an object, not {value!r}")
    missing = [name for name in names if name not in value]
    if missing:
        raise ManifestError(f"{where} is missing {', '.join(missing)}")
    unknown = sorted(set(value) - set(names))
    if unknown:
        raise ManifestError(f"{where} has unknown keys: {', '.join(unknown)}")
    return value


def _is_timestamp(value: str) -> bool:
    if not _TIMESTAMP_RE.fullmatch(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:  # the right shape for a time that does not exist, like month 13
        return False
    return True


def _is_period(value: str) -> bool:
    match = _PERIOD_RE.fullmatch(value)
    if match is None:
        return False
    year, month, day = match.groups()
    try:
        date(int(year), int(month or 1), int(day or 1))
    except ValueError:  # the right shape for a date that does not exist, like Feb 30
        return False
    return True


# The readers below take the dict that holds the value, the key to read and the path
# of that dict for error messages. They run only after `_exact` has found the key.


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


def _choice(raw: dict, key: str, allowed: tuple[str, ...], where: str) -> str:
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
    if value is not None and not (isinstance(value, str) and _is_period(value)):
        raise _bad(where, key, "YYYY-MM-DD, YYYY-MM, YYYY or null", value)
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


def _producer(raw: dict, where: str) -> dict:
    here = f"{where}.producer"
    producer = _exact(raw["producer"], here, _PRODUCER_KEYS)
    for key in _PRODUCER_KEYS:
        _string(producer, key, here)
    return producer


def _watermark(raw: dict, where: str) -> Watermark:
    here = f"{where}.data_through"
    mark = _exact(raw["data_through"], here, _names(Watermark))
    return Watermark(
        value=_string(mark, "value", here),
        granularity=_choice(mark, "granularity", _GRANULARITIES, here),
    )


def _tables(raw: dict, where: str) -> dict[str, TableInfo]:
    tables = {}
    for name, value in _mapping(raw, "tables", where).items():
        here = f"{where}.tables.{name}"
        info = _exact(value, here, _names(TableInfo))
        tables[name] = TableInfo(
            rows=_count(info, "rows", here),
            history_class=_choice(info, "history_class", _HISTORY_CLASSES, here),
        )
    return tables


def _asset(value: object, where: str) -> Asset:
    raw = _exact(value, where, _names(Asset))
    asset = Asset(
        name=_string(raw, "name", where),
        tag=_string(raw, "tag", where),
        role=_choice(raw, "role", _ROLES, where),
        format=_choice(raw, "format", _FORMATS, where),
        asset_bytes=_count(raw, "asset_bytes", where),
        asset_sha256=_sha256(raw, "asset_sha256", where),
        db_bytes=_count(raw, "db_bytes", where),
        db_sha256=_sha256(raw, "db_sha256", where),
        tables=_tables(raw, where),
        period=_period(raw, "period", where),
        closed=_flag(raw, "closed", where),
    )
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


def _assets(raw: dict, where: str) -> list[Asset]:
    items = _list(raw, "assets", where)
    return [_asset(item, f"{where}.assets[{i}]") for i, item in enumerate(items)]


def loads(text: str) -> Manifest:
    """Parse and validate the text of a manifest.

    Raises ManifestError for text that is not JSON, a schema_version other than 1, a
    missing or unknown key at any level, and any value that breaks the format's rules:
    the JSON type of each field, the allowed roles, formats, granularities and history
    classes, lowercase 64-digit sha256 values, real dates and times in the stated
    shapes, a period on every closed asset, and a json asset that repeats its digests.
    """
    try:
        doc = json.loads(text)
    except ValueError as exc:
        raise ManifestError(f"the text could not be parsed as JSON: {exc}") from exc
    # Read the version before the keys: a later schema may name its fields differently.
    if isinstance(doc, dict) and "schema_version" in doc:
        version = doc["schema_version"]
        if type(version) is not int or version != SCHEMA_VERSION:
            raise ManifestError(
                f"unsupported schema_version {version!r}, "
                f"this reader understands {SCHEMA_VERSION}"
            )
    where = "manifest"
    raw = _exact(doc, where, _names(Manifest))
    return Manifest(
        stream=_string(raw, "stream", where),
        producer=_producer(raw, where),
        generated_at=_timestamp(raw, "generated_at", where),
        complete=_flag(raw, "complete", where),
        coverage=_mapping(raw, "coverage", where),
        data_through=_watermark(raw, where),
        cursor=_optional_string(raw, "cursor", where),
        licences=_objects(raw, "licences", where),
        assets=_assets(raw, where),
        series_breaks=_objects(raw, "series_breaks", where),
        corrections=_objects(raw, "corrections", where),
        overrides=_objects(raw, "overrides", where),
        schema_version=raw["schema_version"],
    )
