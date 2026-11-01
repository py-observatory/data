from datetime import date

import pytest

from obskit.tags import TagError, check_tag, current_tag, month_tag, year_tag

STREAM = "pypi-registry"


def accepted(stream: str, tag: str) -> bool:
    """Whether check_tag accepts `tag` as a tag of `stream`."""
    try:
        check_tag(stream, tag)
    except TagError:
        return False
    return True


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("pypi-registry-month-2026-11", True),
        ("pypi-registry-month-2026-01", True),
        ("pypi-registry-month-2026-12", True),
        ("pypi-registry-current", True),
        ("pypi-registry-year-2026", True),
        ("pypi-registry-month-2026-13", False),
        ("pypi-registry-month-2026-00", False),
        ("pypi-registry-month-2026-1", False),
        ("pypi-registry-month-26-11", False),
        ("pypi-registry-month-2026-11-02", False),
        ("pypi-registry-2026-11", False),
        ("pypi-registry-year-26", False),
        ("pypi-registry-year-2026-11", False),
        ("pypi-registry-Current", False),
        ("pypi-registry-current\n", False),
        # Digits of another script: fullwidth ２０２６ and Arabic-Indic ١١.
        ("pypi-registry-year-２０２６", False),
        ("pypi-registry-month-2026-١١", False),
        ("pypi-registry", False),
        ("", False),
        # A tag shape with no stream at all.
        ("current", False),
        ("month-2026-11", False),
        ("year-2026", False),
        # The tags of other streams, including streams whose ids share a prefix.
        ("other-current", False),
        ("catalog-current", False),
        ("pypi-current", False),
        ("pypi-registry-extra-current", False),
        ("v2026-11-02", False),
    ],
)
def test_tag_shapes(tag, expected):
    assert accepted(STREAM, tag) is expected


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("catalog-current", True),
        ("catalog-month-2026-11", True),
        ("v2026-11-02", True),
        ("v2026-13-02", False),  # no such month
        ("v2026-02-30", False),  # no such day
        ("v2026-11-2", False),
        ("v20261102", False),
        ("V2026-11-02", False),
        ("v2026-11-02\n", False),
        ("catalog-v2026-11-02", False),
        ("pypi-registry-current", False),
    ],
)
def test_catalog_tags(tag, expected):
    assert accepted("catalog", tag) is expected


@pytest.mark.parametrize(
    ("stream", "expected"),
    [
        ("pypi-registry", True),
        ("catalog", True),
        ("kit-smoke", True),
        ("a1-b2-c3", True),
        ("x", True),
        ("", False),
        ("PyPI", False),
        ("pypi_registry", False),
        ("pypi.registry", False),
        ("pypi registry", False),
        ("-pypi", False),
        ("pypi-", False),
        ("pypi--registry", False),
        ("pypi\n", False),
        ("pypì", False),  # a letter outside a-z
    ],
)
def test_stream_id_shape(stream, expected):
    # The tag would fit any stream, so only the stream id can make check_tag refuse.
    assert accepted(stream, current_tag(stream)) is expected


def test_tag_builders():
    assert current_tag(STREAM) == "pypi-registry-current"
    assert month_tag(STREAM, date(2026, 11, 30)) == "pypi-registry-month-2026-11"
    assert month_tag(STREAM, date(2027, 1, 1)) == "pypi-registry-month-2027-01"
    assert year_tag(STREAM, 2026) == "pypi-registry-year-2026"
    for tag in (
        current_tag(STREAM),
        month_tag(STREAM, date(2027, 1, 1)),
        year_tag(STREAM, 2026),
    ):
        check_tag(STREAM, tag)


def test_tag_error_is_a_value_error():
    with pytest.raises(ValueError, match="other-current"):
        check_tag(STREAM, "other-current")
