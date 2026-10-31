import json
from dataclasses import FrozenInstanceError, fields, replace
from hashlib import sha256

import pytest

from obskit.manifest import (
    Asset,
    Manifest,
    ManifestError,
    TableInfo,
    Watermark,
    dumps,
    loads,
)

STATE = ("assets", 0)
DAY = ("assets", 1)


def digest(label: str) -> str:
    return sha256(label.encode()).hexdigest()


def state_asset() -> Asset:
    return Asset(
        name="state.db.zst",
        tag="example-current",
        role="state",
        format="sqlite+zstd",
        asset_bytes=1_048_576,
        asset_sha256=digest("state.db.zst"),
        db_bytes=8_388_608,
        db_sha256=digest("state.db"),
        tables={
            "projects": TableInfo(1200, "snapshot"),
            "serials": TableInfo(1, "derived"),
        },
    )


def day_asset() -> Asset:
    return Asset(
        name="day-2026-11-02.db.zst",
        tag="example-month-2026-11",
        role="public",
        format="sqlite+zstd",
        asset_bytes=204_800,
        asset_sha256=digest("day-2026-11-02.db.zst"),
        db_bytes=1_572_864,
        db_sha256=digest("day-2026-11-02.db"),
        tables={"events": TableInfo(40, "events"), "files": TableInfo(120, "append")},
        period="2026-11-02",
        closed=True,
    )


def json_asset() -> Asset:
    return Asset(
        name="summary.json",
        tag="example-current",
        role="public",
        format="json",
        asset_bytes=512,
        asset_sha256=digest("summary.json"),
        db_bytes=512,
        db_sha256=digest("summary.json"),
        tables={},
    )


def required() -> dict:
    """The fields of a manifest that have no default."""
    return {
        "stream": "example",
        "producer": {
            "repo": "py-observatory/data",
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "run_id": "1234567890",
            "obskit": "0.1.0",
            "python": "3.14.0",
        },
        "generated_at": "2026-11-03T01:02:03Z",
        "complete": True,
        "coverage": {"days": 2},
        "data_through": Watermark("123456", "serial"),
        "cursor": "123456",
        "licences": [{"scope": "data", "spdx": "CC0-1.0"}],
        "assets": [state_asset(), day_asset()],
    }


def sample() -> Manifest:
    """A manifest with one state asset, one closed public asset, and every optional
    list filled."""
    return Manifest(
        **required(),
        series_breaks=[{"at": "2026-11-01", "note": "new upstream field"}],
        corrections=[
            {"replaces": "day-2026-11-01.db.zst", "with": "day-2026-11-01b.db.zst"}
        ],
        overrides=[
            {
                "at": "2026-11-03T00:00:00Z",
                "reason": "reviewed by Zoë",
                "failures": ["a table shrank"],
            }
        ],
    )


def minimal() -> Manifest:
    """The smallest manifest: nulls, empty lists, one open asset."""
    return Manifest(
        stream="example",
        producer={
            "repo": "py-observatory/data",
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "run_id": "1234567890",
            "obskit": "0.1.0",
            "python": "3.14.0",
        },
        generated_at="2026-11-03T01:02:03Z",
        complete=False,
        coverage={},
        data_through=Watermark("2026-11", "month"),
        cursor=None,
        licences=[],
        assets=[
            Asset(
                name="owners.json",
                tag="example-current",
                role="public",
                format="json",
                asset_bytes=512,
                asset_sha256="a" * 64,
                db_bytes=512,
                db_sha256="a" * 64,
                tables={"owners": TableInfo(2, "snapshot")},
            )
        ],
    )


def at(doc: dict, path: tuple):
    """The node of `doc` that `path` leads to."""
    node = doc
    for step in path:
        node = node[step]
    return node


def wire() -> dict:
    """The sample manifest as the plain dict that `dumps` writes."""
    return json.loads(dumps(sample()))


def with_value(path: tuple, value: object) -> str:
    """The sample manifest as JSON text, with the value at `path` replaced."""
    doc = wire()
    at(doc, path[:-1])[path[-1]] = value
    return json.dumps(doc)


def without(path: tuple) -> str:
    """The sample manifest as JSON text, minus the key at `path`."""
    doc = wire()
    del at(doc, path[:-1])[path[-1]]
    return json.dumps(doc)


def with_assets(*assets: Asset) -> Manifest:
    return replace(sample(), assets=list(assets))


def test_round_trip():
    m = sample()
    assert [(a.role, a.closed, a.period) for a in m.assets] == [
        ("state", False, None),
        ("public", True, "2026-11-02"),
    ]

    assert loads(dumps(m)) == m


def test_round_trip_with_nulls_and_empty_lists():
    m = minimal()

    assert loads(dumps(m)) == m


def test_dumps_is_stable():
    first, second = sample(), sample()
    assert first is not second

    assert dumps(first) == dumps(second)
    assert dumps(first).endswith("\n")
    assert not dumps(first).endswith("\n\n")


def test_dumps_ignores_insertion_order():
    m = sample()
    shuffled = replace(
        m,
        producer=dict(reversed(m.producer.items())),
        assets=[replace(a, tables=dict(reversed(a.tables.items()))) for a in m.assets],
    )

    assert dumps(shuffled) == dumps(m)


def test_dumps_is_ascii():
    # Writing the text to a file then needs no choice of encoding.
    assert not sample().overrides[0]["reason"].isascii()
    assert dumps(sample()).isascii()


def test_dumps_wire_format():
    # The key names are the contract with everything that reads manifest.json.
    text = dumps(minimal())

    assert json.loads(text) == {
        "assets": [
            {
                "asset_bytes": 512,
                "asset_sha256": "a" * 64,
                "closed": False,
                "db_bytes": 512,
                "db_sha256": "a" * 64,
                "format": "json",
                "name": "owners.json",
                "period": None,
                "role": "public",
                "tables": {"owners": {"history_class": "snapshot", "rows": 2}},
                "tag": "example-current",
            }
        ],
        "complete": False,
        "corrections": [],
        "coverage": {},
        "cursor": None,
        "data_through": {"granularity": "month", "value": "2026-11"},
        "generated_at": "2026-11-03T01:02:03Z",
        "licences": [],
        "overrides": [],
        "producer": {
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "obskit": "0.1.0",
            "python": "3.14.0",
            "repo": "py-observatory/data",
            "run_id": "1234567890",
        },
        "schema_version": 1,
        "series_breaks": [],
        "stream": "example",
    }
    assert text.startswith('{\n  "assets": [\n    {\n      "asset_bytes": 512,\n')
    assert text == json.dumps(json.loads(text), sort_keys=True, indent=2) + "\n"


def test_manifest_error_is_a_value_error():
    assert issubclass(ManifestError, ValueError)


@pytest.mark.parametrize(
    "value",
    [Watermark("1", "serial"), TableInfo(1, "append"), state_asset(), sample()],
    ids=["Watermark", "TableInfo", "Asset", "Manifest"],
)
def test_values_are_frozen(value):
    with pytest.raises(FrozenInstanceError):
        setattr(value, fields(value)[0].name, None)


def test_optional_fields_have_defaults():
    one, two = Manifest(**required()), Manifest(**required())

    assert (one.series_breaks, one.corrections, one.overrides) == ([], [], [])
    assert one.schema_version == 1
    assert one.overrides is not two.overrides
    assert (json_asset().period, json_asset().closed) == (None, False)


def test_rejects_missing_field():
    with pytest.raises(ManifestError, match="complete"):
        loads(without(("complete",)))


# The objects whose keys the format fixes. Each key must be there, and no other may be.
FIXED_OBJECTS = {
    "manifest": (),
    "producer": ("producer",),
    "data_through": ("data_through",),
    "asset": STATE,
    "table": STATE + ("tables", "projects"),
}
EVERY_KEY = [
    pytest.param(path, key, id=f"{name}-{key}")
    for name, path in FIXED_OBJECTS.items()
    for key in at(wire(), path)
]


@pytest.mark.parametrize(("path", "key"), EVERY_KEY)
def test_rejects_every_missing_key(path, key):
    with pytest.raises(ManifestError, match=f"missing.*{key}"):
        loads(without(path + (key,)))


@pytest.mark.parametrize("path", list(FIXED_OBJECTS.values()), ids=list(FIXED_OBJECTS))
def test_rejects_unknown_keys(path):
    text = with_value(path + ("surprise",), 1)

    with pytest.raises(ManifestError, match="unknown.*surprise"):
        loads(text)


def test_rejects_unknown_history_class():
    with pytest.raises(ManifestError, match="history_class"):
        loads(with_value(STATE + ("tables", "projects", "history_class"), "weird"))


BAD_SHAS = [
    pytest.param(digest("x").upper(), id="uppercase"),
    pytest.param(digest("x")[:63], id="63 characters"),
    pytest.param(digest("x") + "0", id="65 characters"),
    pytest.param("g" * 64, id="not hex"),
    pytest.param(digest("x") + "\n", id="trailing newline"),
    pytest.param("sha256:" + digest("x"), id="prefixed"),
    pytest.param("", id="empty"),
    pytest.param(None, id="null"),
    pytest.param(123, id="number"),
]


@pytest.mark.parametrize("field", ["asset_sha256", "db_sha256"])
@pytest.mark.parametrize("sha", BAD_SHAS)
def test_rejects_bad_sha(field, sha):
    with pytest.raises(ManifestError, match=field):
        loads(with_value(DAY + (field,), sha))


def test_closed_asset_needs_period():
    with pytest.raises(ManifestError, match="period"):
        loads(with_value(DAY + ("period",), None))


def test_an_open_asset_may_carry_a_period():
    m = with_assets(replace(state_asset(), period="2026-11-03"))

    assert loads(dumps(m)) == m


def test_rejects_schema_version_2():
    with pytest.raises(ManifestError, match="schema_version"):
        loads(with_value(("schema_version",), 2))


# True and 1.0 both equal 1 in Python, so only a check on the type keeps them out.
@pytest.mark.parametrize("version", [0, 3, True, 1.0, "1", None, [1]])
def test_rejects_every_schema_version_but_1(version):
    with pytest.raises(ManifestError, match="schema_version"):
        loads(with_value(("schema_version",), version))


def test_a_later_schema_is_reported_as_one():
    # A later version may rename fields, so the version is read before the keys.
    with pytest.raises(ManifestError, match="schema_version"):
        loads('{"schema_version": 2, "assets": "in some new shape"}')


def bad(path: tuple, value: object):
    return pytest.param(path, value, id=f"{'.'.join(map(str, path))}={value!r}")


BAD_VALUES = [
    # the wrong JSON type
    bad(("stream",), 7),
    bad(("stream",), None),
    bad(("complete",), 1),
    bad(("complete",), "true"),
    bad(("cursor",), 123456),
    bad(("coverage",), []),
    bad(("licences",), {}),
    bad(("licences", 0), "CC0-1.0"),
    bad(("assets",), {}),
    bad(("assets", 0), 7),
    bad(("series_breaks",), "none"),
    bad(("corrections", 0), []),
    bad(("overrides", 0), None),
    bad(("producer",), []),
    bad(("producer", "run_id"), 1234567890),
    bad(("producer", "repo"), None),
    bad(("data_through",), "123456"),
    bad(("data_through", "value"), 123456),
    bad(("assets", 0, "name"), None),
    bad(("assets", 0, "tag"), 5),
    bad(("assets", 0, "tables"), []),
    bad(("assets", 0, "tables", "projects"), 5),
    bad(("assets", 0, "closed"), 0),
    bad(("assets", 0, "period"), 2026),
    # a value outside the allowed set
    bad(("assets", 0, "role"), "private"),
    bad(("assets", 0, "format"), "parquet"),
    bad(("assets", 0, "format"), "JSON"),
    bad(("data_through", "granularity"), "week"),
    # a count is a whole number from zero up, and JSON true is not one
    bad(("assets", 0, "asset_bytes"), True),
    bad(("assets", 0, "asset_bytes"), -1),
    bad(("assets", 0, "asset_bytes"), 1.5),
    bad(("assets", 0, "asset_bytes"), 1024.0),
    bad(("assets", 0, "db_bytes"), "9"),
    bad(("assets", 0, "tables", "projects", "rows"), -1),
    bad(("assets", 0, "tables", "projects", "rows"), None),
]


@pytest.mark.parametrize(("path", "value"), BAD_VALUES)
def test_rejects_a_value_the_format_rules_out(path, value):
    key = next(step for step in reversed(path) if isinstance(step, str))

    with pytest.raises(ManifestError, match=key):
        loads(with_value(path, value))


@pytest.mark.parametrize("granularity", ["serial", "day", "month", "instant"])
def test_accepts_every_granularity(granularity):
    m = replace(sample(), data_through=Watermark("2026-11", granularity))

    assert loads(dumps(m)) == m


@pytest.mark.parametrize("history_class", ["append", "events", "snapshot", "derived"])
def test_accepts_every_history_class(history_class):
    asset = replace(state_asset(), tables={"t": TableInfo(0, history_class)})

    assert loads(dumps(with_assets(asset))) == with_assets(asset)


def test_json_asset_round_trips():
    m = with_assets(json_asset())

    assert loads(dumps(m)) == m


@pytest.mark.parametrize(
    ("field", "value"),
    [("db_bytes", 513), ("db_sha256", digest("other"))],
    ids=["db_bytes", "db_sha256"],
)
def test_json_asset_must_repeat_its_digests(field, value):
    m = with_assets(replace(json_asset(), **{field: value}))

    with pytest.raises(ManifestError, match="json"):
        loads(dumps(m))


@pytest.mark.parametrize("period", ["2026-11-02", "2026-11", "2026", "2028-02-29"])
def test_accepts_every_period_shape(period):
    m = with_assets(replace(day_asset(), period=period))

    assert loads(dumps(m)) == m


BAD_PERIODS = [
    pytest.param("2026-13-01", id="month 13"),
    pytest.param("2026-00-10", id="month 0"),
    pytest.param("2026-02-30", id="february 30"),
    pytest.param("2027-02-29", id="leap day in a common year"),
    pytest.param("2026-11-00", id="day 0"),
    pytest.param("2026-13", id="month 13 alone"),
    pytest.param("0000", id="year 0"),
    pytest.param("26-11-02", id="two digit year"),
    pytest.param("2026-1-2", id="unpadded"),
    pytest.param("20261102", id="no dashes"),
    pytest.param("2026/11/02", id="slashes"),
    pytest.param("2026-W44", id="iso week"),
    pytest.param("2026-11-02T00:00:00Z", id="timestamp"),
    pytest.param("2026-11-02\n", id="trailing newline"),
    pytest.param("\u0662\u0660\u0662\u0666-\u0661\u0661", id="arabic-indic digits"),
    pytest.param("", id="empty"),
]


@pytest.mark.parametrize("period", BAD_PERIODS)
def test_rejects_bad_period(period):
    with pytest.raises(ManifestError, match="period"):
        loads(with_value(DAY + ("period",), period))


@pytest.mark.parametrize(
    "stamp", ["2026-11-03T01:02:03Z", "2028-02-29T23:59:59Z", "2026-01-01T00:00:00Z"]
)
def test_accepts_a_real_utc_time(stamp):
    m = replace(sample(), generated_at=stamp)

    assert loads(dumps(m)) == m


BAD_TIMESTAMPS = [
    pytest.param("2026-11-03", id="date only"),
    pytest.param("2026-11-03 01:02:03Z", id="space separator"),
    pytest.param("2026-11-03T01:02:03", id="no Z"),
    pytest.param("2026-11-03T01:02:03+00:00", id="offset"),
    pytest.param("2026-11-03T01:02:03.500Z", id="fraction"),
    pytest.param("2026-11-03T01:02:03z", id="lowercase z"),
    pytest.param("2026-11-03T1:2:3Z", id="unpadded"),
    pytest.param("2026-13-03T01:02:03Z", id="month 13"),
    pytest.param("2026-11-31T01:02:03Z", id="november 31"),
    pytest.param("2026-11-03T24:00:00Z", id="hour 24"),
    pytest.param("2026-11-03T01:60:00Z", id="minute 60"),
    pytest.param("2026-11-03T01:02:60Z", id="second 60"),
    pytest.param("2026-11-03T01:02:03Z\n", id="trailing newline"),
    pytest.param(
        "\u0662\u0660\u0662\u0666-\u0661\u0661-\u0660\u0663"
        "T\u0660\u0661:\u0660\u0662:\u0660\u0663Z",
        id="arabic-indic digits",
    ),
    pytest.param("", id="empty"),
    pytest.param(None, id="null"),
    pytest.param(1762131723, id="epoch number"),
]


@pytest.mark.parametrize("stamp", BAD_TIMESTAMPS)
def test_rejects_bad_generated_at(stamp):
    with pytest.raises(ManifestError, match="generated_at"):
        loads(with_value(("generated_at",), stamp))


NOT_MANIFESTS = [
    pytest.param("", id="empty"),
    pytest.param("not json", id="not json"),
    pytest.param('{"schema_version": 1', id="truncated"),
    pytest.param("\ufeff{}", id="byte order mark"),
    pytest.param("null", id="null"),
    pytest.param("[]", id="list"),
    pytest.param('"manifest"', id="string"),
    pytest.param("42", id="number"),
    pytest.param("{}", id="empty object"),
    pytest.param('{"schema_version": 1}', id="version only"),
    pytest.param('{"schema_version": ' + "9" * 5000 + "}", id="too many digits"),
]


@pytest.mark.parametrize("text", NOT_MANIFESTS)
def test_rejects_text_that_is_not_a_manifest(text):
    with pytest.raises(ManifestError):
        loads(text)
