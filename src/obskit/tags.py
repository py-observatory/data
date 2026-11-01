"""Release tag names. Every release has one of a few fixed shapes, and the kit refuses
any other shape, and any tag of another stream."""

import re
from datetime import date

# The digits are `[0-9]` and not `\d`, which also matches the digits of other scripts.
_STREAM = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
# What follows `<stream>-` in each tag of a stream.
_CURRENT_OR_MONTH = r"current|month-[0-9]{4}-(?:0[1-9]|1[0-2])"
_STREAM_TAG = re.compile(rf"{_CURRENT_OR_MONTH}|year-[0-9]{{4}}")
# The catalog has no year release. It writes a daily pin, `vYYYY-MM-DD`, instead.
_CATALOG = "catalog"
_CATALOG_TAG = re.compile(_CURRENT_OR_MONTH)
_PIN = re.compile(r"v[0-9]{4}-[0-9]{2}-[0-9]{2}")


class TagError(ValueError):
    """A stream id or a release tag that is not one of the allowed shapes."""


def current_tag(stream: str) -> str:
    """The release that each publish of `stream` swaps its latest files into."""
    return f"{stream}-current"


def month_tag(stream: str, day: date) -> str:
    """The create-once release of the month that `day` falls in."""
    return f"{stream}-month-{day:%Y-%m}"


def year_tag(stream: str, year: int) -> str:
    """The create-once release of `year`."""
    return f"{stream}-year-{year:04d}"


def _is_pin(tag: str) -> bool:
    """Whether `tag` is `vYYYY-MM-DD` for a day that exists."""
    if not _PIN.fullmatch(tag):
        return False
    try:
        date.fromisoformat(tag[1:])
    except ValueError:  # the right shape for a day that does not exist, like Feb 30
        return False
    return True


def check_tag(stream: str, tag: str) -> None:
    """Raise TagError unless `stream` is a stream id and `tag` is one of its tags.

    A stream id is lowercase letters and digits, in words joined by single hyphens.
    The tags of a stream are `<stream>-current`, `<stream>-month-YYYY-MM` and
    `<stream>-year-YYYY`. The catalog has no year release: its tags are
    `catalog-current`, `catalog-month-YYYY-MM` and its daily pins, `vYYYY-MM-DD`.
    """
    if not _STREAM.fullmatch(stream):
        raise TagError(
            f"{stream!r} is not a stream id: it must be lowercase letters and digits, "
            "in words joined by single hyphens"
        )
    catalog = stream == _CATALOG
    shapes = _CATALOG_TAG if catalog else _STREAM_TAG
    prefix = f"{stream}-"
    if tag.startswith(prefix) and shapes.fullmatch(tag.removeprefix(prefix)):
        return
    if catalog and _is_pin(tag):
        return
    if catalog:
        expected = "catalog-current, catalog-month-YYYY-MM or vYYYY-MM-DD"
    else:
        expected = f"{stream}-current, {stream}-month-YYYY-MM or {stream}-year-YYYY"
    raise TagError(f"{tag!r} is not a tag of the stream {stream}: expected {expected}")
