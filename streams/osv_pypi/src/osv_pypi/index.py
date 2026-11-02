"""OSV's index of changed advisories, and the addresses in OSV's bucket.

OSV.dev keeps every advisory that affects a PyPI project in a public bucket. Its file
`modified_id.csv` lists them, one line `<time>,<id>` for each, newest first. The time is
when the advisory last changed, in RFC 3339 UTC with as many fraction digits as it
needs, none to nine. Google Cloud Storage numbers each version of a file, and the
`x-goog-generation` header of a reply says which one was served. That number only grows,
so it is the stream's cursor: it names the copy of the index that was read.

The index is data from outside. It is read as text and never run, and a body of any
other shape raises ValueError, because a guess at a bad line would be stored as a fact.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote

from obskit.http import Http
from obskit.show import show

BASE = "https://osv-vulnerabilities.storage.googleapis.com/PyPI/"
INDEX_URL = BASE + "modified_id.csv"  # every advisory, with the time it last changed
DUMP_URL = BASE + "all.zip"  # the record of every advisory, in one zip

# An id is capitals and digits, then one or more parts after hyphens, as in
# GHSA-mjcr-rqjg-rhg3, PYSEC-2022-123 and MAL-2026-18435. The letters are ASCII: the
# pattern has no `re.IGNORECASE`, and no `\w`, which lets other scripts in.
ADVISORY_ID = re.compile(r"[A-Z][A-Z0-9]*(?:-[A-Za-z0-9]+)+")
_MAX_ID_LENGTH = 64

# The real index is about a megabyte, so a body over this is not a copy of it.
_MAX_INDEX_BYTES = 64 * 1024 * 1024

# A time as the index writes it: the date, the time of day, and up to nine fraction
# digits. The digits are `[0-9]` and not `\d`, which also matches the digits of other
# scripts.
_INSTANT = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]{1,9}))?Z"
)
# The length of `YYYY-MM-DDTHH:MM:SS`, the part of a time before its fraction.
_TO_THE_SECOND = 19

# A generation is ASCII digits and nothing else. `str.isdigit` is also true of "²",
# which `int` cannot read, and `int` itself reads digits of other scripts, signs,
# blanks and underscores.
_DIGITS = re.compile(r"[0-9]+")


def record_url(advisory_id: str) -> str:
    """The address of one advisory's record. An id that `is_advisory_id` accepts has
    nothing in it to quote. Any other text is quoted whole, so it cannot change the
    path."""
    return BASE + quote(advisory_id, safe="") + ".json"


def is_advisory_id(text: str) -> bool:
    """Whether `text` is an advisory id: all of it matches ADVISORY_ID, and it is at
    most 64 characters. The index and the dump both ask this, so the rule lives in one
    place. The length is checked first, so a long value is never run through the
    pattern."""
    return len(text) <= _MAX_ID_LENGTH and ADVISORY_ID.fullmatch(text) is not None


def instant_key(text: str) -> str:
    """The key that puts a time of the index in order: `text` with its fraction padded
    to nine digits, so that 2022-03-21T20:04:49Z is 2022-03-21T20:04:49.000000000Z.

    As plain text a whole second sorts after its own fractions, `...:49Z` after
    `...:49.5Z`, because `Z` comes after the point. Every key has nine digits, so keys
    compare as the times do.

    `text` is RFC 3339 in UTC, `YYYY-MM-DDTHH:MM:SS[.f{1,9}]Z`. Anything else raises
    ValueError: another zone, a date or a time that does not exist (February 30,
    hour 24) and second 60.
    """
    found = _INSTANT.fullmatch(text)
    if found is None:
        raise ValueError(
            f"{show(text)} is not an RFC 3339 UTC time like 2022-03-21T20:04:49.5Z"
        )
    *fields, fraction = found.groups()
    try:
        # This only checks the values. The constructor refuses a month 13, a
        # February 30, an hour 24 and a second 60.
        datetime(*map(int, fields))
    except ValueError as exc:
        raise ValueError(f"{show(text)} is not a time that exists: {exc}") from exc
    return f"{text[:_TO_THE_SECOND]}.{(fraction or '').ljust(9, '0')}Z"


def seconds(key: str) -> str:
    """A key as the manifest writes an instant: to the second, with no fraction, as in
    2026-11-01T09:45:03Z. The fraction is cut and never rounded, so the time that the
    data goes through is never ahead of the data."""
    return key[:_TO_THE_SECOND] + "Z"


@dataclass(frozen=True)
class Index:
    """The index as `INDEX_URL` lists it, with the generation of the copy that was
    read."""

    modified: dict[str, str]  # advisory id -> the `instant_key` of its last change
    generation: int  # `x-goog-generation` of the file: the stream's cursor


def _read_line(number: int, line: str) -> tuple[str, str]:
    """The id and the key of one line of the index, `<time>,<id>`. `number` is the
    line's place in the index, counted from 1, and is there for the error."""
    time_text, comma, advisory_id = line.partition(",")
    if not comma or "," in advisory_id:
        raise ValueError(
            f"line {number} of the index must be a time, a comma and an id, "
            f"not {show(line)}"
        )
    if not is_advisory_id(advisory_id):
        raise ValueError(
            f"line {number} of the index has {show(advisory_id)}, "
            "which is not an advisory id"
        )
    try:
        return advisory_id, instant_key(time_text)
    except ValueError as exc:
        raise ValueError(f"line {number} of the index: {exc}") from exc


def parse_index(body: bytes, generation: int) -> Index:
    """The advisories of an index, each with the key of when it last changed.
    `generation` is the generation of the copy that `body` came from.

    `body` is strict UTF-8: a line `<time>,<id>` for each advisory, each line ended by a
    line feed, the last one's optional. Raises ValueError for a body of over 64 MiB,
    for bytes that are not UTF-8 and for a carriage return anywhere. An index with no
    line is refused as well, because OSV lists thousands of advisories and an empty
    one is a broken copy.

    A line is refused, with its number, when it is not one time, a comma and one id (a
    blank line is not), when its id is not one that `is_advisory_id` accepts, and when
    its time is not one that `instant_key` accepts. So is an id that is listed twice:
    the index lists each advisory once, and two times for one would leave the reader to
    choose.
    """
    if len(body) > _MAX_INDEX_BYTES:
        raise ValueError(
            f"the index is {len(body):,} bytes, over the limit of {_MAX_INDEX_BYTES:,}"
        )
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"the index is not UTF-8: {exc}") from exc
    if "\r" in text:
        raise ValueError(
            "the index has a carriage return, and its lines end in a line feed alone"
        )
    text = text.removesuffix("\n")  # the last line's line feed is optional
    if not text:
        raise ValueError("the index is empty")
    modified: dict[str, str] = {}
    for number, line in enumerate(text.split("\n"), start=1):
        advisory_id, key = _read_line(number, line)
        if advisory_id in modified:
            raise ValueError(f"line {number} of the index lists {advisory_id} again")
        modified[advisory_id] = key
    return Index(modified, generation)


def get_index(http: Http) -> Index:
    """The index as OSV serves it now, with the generation of that copy.

    Raises ValueError for any reply but a 200 (Http returns a 404 and a 410 and does
    not raise, and a 304 cannot come, because no ETag goes out), for an
    `x-goog-generation` header that is missing or is not all digits, and for a body
    that `parse_index` refuses. The header is checked before the body is parsed: a copy
    that cannot be named cannot be the cursor. A request that fails is Http's
    UpstreamError, and it is not caught here.
    """
    reply = http.get(INDEX_URL)
    if reply.status != 200:
        raise ValueError(f"GET {INDEX_URL} returned HTTP {reply.status}, not 200")
    generation = reply.headers.get("x-goog-generation")
    if generation is None:
        raise ValueError(
            f"the reply to GET {INDEX_URL} has no x-goog-generation header"
        )
    if not _DIGITS.fullmatch(generation):
        raise ValueError(
            f"the x-goog-generation of the reply to GET {INDEX_URL} must be all "
            f"digits, not {show(generation)}"
        )
    return parse_index(reply.body, int(generation))
