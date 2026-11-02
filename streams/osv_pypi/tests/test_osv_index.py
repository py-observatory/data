import re
from dataclasses import FrozenInstanceError

import pytest

from obskit.http import Http, UpstreamError
from obskit.testing import FakeHttp
from osv_pypi.index import (
    BASE,
    DUMP_URL,
    INDEX_URL,
    Index,
    get_index,
    instant_key,
    is_advisory_id,
    parse_index,
    record_url,
    seconds,
)

MIB = 1024 * 1024
GENERATION = 1793526521815797
# Lines as the index has them: one time with nine fraction digits, one with none.
NEW_LINE = b"2026-11-01T09:45:03.305960502Z,MAL-2026-18435\n"
OLD_LINE = b"2022-03-21T20:04:49Z,GHSA-mjcr-rqjg-rhg3\n"


# ---- the addresses


def test_urls():
    assert BASE == "https://osv-vulnerabilities.storage.googleapis.com/PyPI/"
    assert INDEX_URL == (
        "https://osv-vulnerabilities.storage.googleapis.com/PyPI/modified_id.csv"
    )
    assert DUMP_URL == (
        "https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip"
    )


def test_record_url_quotes_the_id():
    assert record_url("GHSA-mjcr-rqjg-rhg3") == (
        "https://osv-vulnerabilities.storage.googleapis.com/PyPI/"
        "GHSA-mjcr-rqjg-rhg3.json"
    )
    # Text that is no advisory id still cannot change the path.
    assert record_url("a/b?c#d e") == BASE + "a%2Fb%3Fc%23d%20e.json"
    assert record_url("../x") == BASE + "..%2Fx.json"


# ---- advisory ids


@pytest.mark.parametrize(
    "text",
    [
        "GHSA-mjcr-rqjg-rhg3",
        "PYSEC-2022-123",
        "MAL-2026-18435",
        "CVE-2022-12345",
        "OSV-2020-1",
        "A-B",
        "A1-2",
        "A-" + "b" * 62,  # 64 characters, the longest that is allowed
    ],
)
def test_is_advisory_id_accepts(text):
    assert is_advisory_id(text)


@pytest.mark.parametrize(
    "text",
    [
        "",
        "GHSA",  # no hyphen
        "GHSA-",
        "GHSA--mjcr",
        "-GHSA-mjcr",
        "ghsa-mjcr-rqjg-rhg3",  # the prefix is capitals
        "1GHSA-mjcr",
        "GHSA-mjcr_rqjg",
        "GHSA-mjcr:rqjg",
        "GHSA mjcr-rqjg",
        "GHSA-mjcr-rqjg-rhg3 ",
        "GHSA-mjcr-rqjg-rhg3\n",  # a pattern that ends in `$` lets this through
        "GHSA-mjcr-é",
        "GHSA-٣٣",  # Arabic-Indic digits are not 0 to 9
        "A-" + "b" * 63,  # 65 characters
    ],
)
def test_is_advisory_id_refuses(text):
    assert not is_advisory_id(text)


# ---- times


def test_instant_keys_sort_across_precisions():
    assert instant_key("2022-03-21T20:04:49Z") == "2022-03-21T20:04:49.000000000Z"
    assert instant_key("2026-11-01T09:45:03.305960502Z") == (
        "2026-11-01T09:45:03.305960502Z"
    )
    assert instant_key("2022-03-21T20:04:49Z") < instant_key("2022-03-21T20:04:49.5Z")


def test_instant_keys_put_every_precision_in_time_order():
    in_order = [
        "2022-03-21T20:04:48.999999999Z",
        "2022-03-21T20:04:49Z",
        "2022-03-21T20:04:49.000000001Z",
        "2022-03-21T20:04:49.5Z",
        "2022-03-21T20:04:49.51Z",
        "2022-03-21T20:04:50Z",
    ]
    # As plain text the whole second sorts after its own fractions, which is why the
    # index needs keys.
    assert sorted(in_order) != in_order
    assert sorted(reversed(in_order), key=instant_key) == in_order


@pytest.mark.parametrize(
    ("text", "key"),
    [
        ("2022-03-21T20:04:49.1Z", "2022-03-21T20:04:49.100000000Z"),
        ("2022-03-21T20:04:49.12Z", "2022-03-21T20:04:49.120000000Z"),
        ("2022-03-21T20:04:49.123456789Z", "2022-03-21T20:04:49.123456789Z"),
        ("2024-02-29T23:59:59Z", "2024-02-29T23:59:59.000000000Z"),  # a leap day
        ("0001-01-01T00:00:00Z", "0001-01-01T00:00:00.000000000Z"),
        ("9999-12-31T23:59:59.999999999Z", "9999-12-31T23:59:59.999999999Z"),
    ],
)
def test_instant_key_pads_the_fraction_to_nine_digits(text, key):
    assert instant_key(text) == key


@pytest.mark.parametrize(
    "text",
    [
        "",
        "yesterday",
        "2022-03-21",
        "2022-03-21T20:04:49",  # no zone
        "2022-03-21T20:04:49+00:00",  # another zone, even one that is UTC
        "2022-03-21T20:04:49-05:00",
        "2022-03-21T20:04:49z",  # the letters are capitals
        "2022-03-21t20:04:49Z",
        "2022-03-21 20:04:49Z",
        "2022-03-21T20:04Z",  # no seconds
        "2022-3-21T20:04:49Z",  # fields of two digits
        "2022-03-21T20:04:49.Z",  # a point and no digits
        "2022-03-21T20:04:49.1234567890Z",  # ten digits
        " 2022-03-21T20:04:49Z",
        "2022-03-21T20:04:49Z ",
        "2022-03-21T20:04:49Z\n",
        "2022-02-30T20:04:49Z",  # a date that does not exist
        "2023-02-29T20:04:49Z",  # 2023 is no leap year
        "2022-00-10T20:04:49Z",
        "2022-13-01T20:04:49Z",
        "0000-01-01T00:00:00Z",
        "2022-03-21T24:00:00Z",
        "2022-03-21T20:60:49Z",
        "2022-03-21T20:04:60Z",  # second 60
        "٢٠٢٢-03-21T20:04:49Z",  # Arabic-Indic digits
        "２０２２-03-21T20:04:49Z",  # fullwidth digits
    ],
)
def test_instant_key_refuses(text):
    with pytest.raises(ValueError):
        instant_key(text)


def test_instant_key_errors_name_the_time():
    for text in ("2022-03-21T20:04:49+00:00", "2022-02-30T20:04:49Z"):
        with pytest.raises(ValueError, match=re.escape(text)):
            instant_key(text)


def test_seconds_cuts_a_key_to_the_second():
    assert seconds(instant_key("2026-11-01T09:45:03.305960502Z")) == (
        "2026-11-01T09:45:03Z"
    )
    assert seconds(instant_key("2022-03-21T20:04:49Z")) == "2022-03-21T20:04:49Z"
    # Cut and never rounded, so that the date the data goes through is not ahead of it.
    assert seconds(instant_key("2022-03-21T20:04:49.999999999Z")) == (
        "2022-03-21T20:04:49Z"
    )


# ---- the index


def test_parse_index_reads_real_lines():
    body = (
        b"2026-11-01T09:45:03.305960502Z,MAL-2026-18435\n"
        b"2022-03-21T20:04:49Z,GHSA-mjcr-rqjg-rhg3\n"
    )
    index = parse_index(body, 1793526521815797)
    assert index.modified["GHSA-mjcr-rqjg-rhg3"] == "2022-03-21T20:04:49.000000000Z"
    assert index.modified["MAL-2026-18435"] == "2026-11-01T09:45:03.305960502Z"
    assert len(index.modified) == 2
    assert index.generation == 1793526521815797


def test_parse_index_final_line_feed_is_optional():
    with_it = parse_index(NEW_LINE + OLD_LINE, GENERATION)
    without_it = parse_index(NEW_LINE + OLD_LINE.removesuffix(b"\n"), GENERATION)
    assert with_it == without_it
    assert len(with_it.modified) == 2


def test_parse_index_tells_the_newest_across_precisions():
    body = (
        b"2022-03-21T20:04:49Z,GHSA-aaaa-bbbb-cccc\n"
        b"2022-03-21T20:04:49.5Z,GHSA-dddd-eeee-ffff\n"
        b"2022-03-21T20:04:48.999999999Z,GHSA-gggg-hhhh-iiii\n"
    )
    index = parse_index(body, GENERATION)
    newest = max(index.modified, key=lambda advisory: index.modified[advisory])
    assert newest == "GHSA-dddd-eeee-ffff"
    assert seconds(index.modified[newest]) == "2022-03-21T20:04:49Z"


def test_the_index_is_frozen():
    index = parse_index(OLD_LINE, GENERATION)
    with pytest.raises(FrozenInstanceError):
        setattr(index, "generation", 2)


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        pytest.param(b"", "empty", id="an empty index"),
        pytest.param(b"\n", "empty", id="only a line feed"),
        pytest.param(
            OLD_LINE + b"\n", r"line 2\b.*comma", id="a blank line at the end"
        ),
        pytest.param(
            b"\n" + OLD_LINE, r"line 1\b.*comma", id="a blank line at the start"
        ),
        pytest.param(
            NEW_LINE + b"\n" + OLD_LINE, r"line 2\b.*comma", id="a blank line between"
        ),
        pytest.param(
            b"2022-03-21T20:04:49Z GHSA-mjcr-rqjg-rhg3\n", "comma", id="no comma"
        ),
        pytest.param(
            b"2022-03-21T20:04:49Z,GHSA-mjcr-rqjg-rhg3,x\n", "comma", id="two commas"
        ),
        pytest.param(
            b"2022-03-21T20:04:49Z,GHSA-mjcr-rqjg-rhg3,\n",
            "comma",
            id="a comma at the end",
        ),
        pytest.param(
            NEW_LINE + b"2022-03-21T20:04:49Z,not an id\n",
            r"line 2\b.*advisory id",
            id="a bad id",
        ),
        pytest.param(b"2022-03-21T20:04:49Z,\n", "advisory id", id="no id"),
        pytest.param(
            b"2022-03-21T20:04:49Z,A-" + b"b" * 63 + b"\n",
            "advisory id",
            id="an id of 65 characters",
        ),
        pytest.param(
            b"yesterday,GHSA-mjcr-rqjg-rhg3\n", "RFC 3339", id="a time that is no time"
        ),
        pytest.param(
            NEW_LINE + b"2022-03-21T20:04:49+00:00,GHSA-mjcr-rqjg-rhg3\n",
            r"line 2\b.*RFC 3339",
            id="another zone",
        ),
        pytest.param(
            b"2022-02-30T20:04:49Z,GHSA-mjcr-rqjg-rhg3\n",
            "exist",
            id="a date that does not exist",
        ),
        pytest.param(
            b"2022-03-21T20:04:60Z,GHSA-mjcr-rqjg-rhg3\n", "exist", id="second 60"
        ),
        pytest.param(
            OLD_LINE + b"2023-01-01T00:00:00Z,GHSA-mjcr-rqjg-rhg3\n",
            r"line 2\b.*again",
            id="a repeated id at another time",
        ),
        pytest.param(OLD_LINE + OLD_LINE, r"line 2\b.*again", id="the same line twice"),
        pytest.param(
            OLD_LINE.replace(b"\n", b"\r\n"), "carriage return", id="a CRLF line end"
        ),
        pytest.param(
            OLD_LINE.replace(b"\n", b"\r"), "carriage return", id="a CR line end"
        ),
        pytest.param(
            b"2022-03-21T20:04:49Z,GHSA-\xff\n",
            "not UTF-8",
            id="bytes that are not UTF-8",
        ),
    ],
)
def test_parse_index_refuses(body, reason):
    with pytest.raises(ValueError, match=reason):
        parse_index(body, GENERATION)


def test_parse_index_refuses_a_body_over_64_mib():
    # It lives apart from the cases above so that 64 MiB is not held for the whole run.
    # A body of exactly 64 MiB passes the size check and is refused for what it holds,
    # which is no line at all.
    with pytest.raises(ValueError, match="comma"):
        parse_index(b"x" * (64 * MIB), GENERATION)
    with pytest.raises(ValueError, match="over the limit"):
        parse_index(b"x" * (64 * MIB + 1), GENERATION)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"x" * 100_000, id="a long line with no comma"),
        pytest.param(b"2022-03-21T20:04:49Z,A-" + b"b" * 100_000, id="a long id"),
        pytest.param(
            b"2022-" + b"0" * 100_000 + b",GHSA-mjcr-rqjg-rhg3", id="a long time"
        ),
    ],
)
def test_parse_index_errors_stay_short(body):
    # The index is data from outside, and an error goes to a log and to an issue.
    with pytest.raises(ValueError) as caught:
        parse_index(body, GENERATION)
    assert len(str(caught.value)) < 300


# ---- reading the index from OSV


@pytest.fixture
def fake() -> FakeHttp:
    return FakeHttp()


@pytest.fixture
def http(fake: FakeHttp) -> Http:
    return Http(transport=fake, sleep=lambda seconds: None)


def serve(
    fake: FakeHttp,
    *,
    status: int = 200,
    body: bytes = NEW_LINE + OLD_LINE,
    generation: str | None = str(GENERATION),
) -> None:
    """Put the index at INDEX_URL, with `generation` in its header or, for None, with
    no such header."""
    headers = {} if generation is None else {"x-goog-generation": generation}
    fake.add(INDEX_URL, status=status, body=body, headers=headers)


def test_get_index_reads_the_index_and_its_generation(fake, http):
    serve(fake)

    index = get_index(http)

    assert index == Index(
        {
            "MAL-2026-18435": "2026-11-01T09:45:03.305960502Z",
            "GHSA-mjcr-rqjg-rhg3": "2022-03-21T20:04:49.000000000Z",
        },
        GENERATION,
    )
    assert [url for url, _ in fake.requests] == [INDEX_URL]


@pytest.mark.parametrize(
    "generation",
    [
        None,
        "",
        "abc",
        "12ab",
        "-1",
        "+1",
        "1.5",
        "1_000",
        " 12",
        "12 ",
        "1, 2",
        "١٢",  # Arabic-Indic digits: int() reads them, and they are not 0 to 9
        "²",  # str.isdigit() is true of this, and int() cannot read it
        "1" * 100_000 + "x",
    ],
)
def test_get_index_needs_a_generation(fake, http, generation):
    # The body is one that parse_index refuses. The error is the header's, so the
    # header was checked before anything was parsed.
    serve(fake, body=b"", generation=generation)

    with pytest.raises(ValueError, match="x-goog-generation") as caught:
        get_index(http)

    assert len(str(caught.value)) < 300


@pytest.mark.parametrize("status", [304, 404, 410])
def test_get_index_must_be_a_200(fake, http, status):
    # Its header and its body are good, so only the status is wrong.
    serve(fake, status=status)

    with pytest.raises(ValueError, match=f"HTTP {status}"):
        get_index(http)


def test_get_index_leaves_a_failed_request_to_http(fake, http):
    serve(fake, status=403)

    with pytest.raises(UpstreamError) as caught:
        get_index(http)

    assert caught.value.status == 403


def test_get_index_refuses_a_body_that_does_not_parse(fake, http):
    serve(fake, body=b"")

    with pytest.raises(ValueError, match="empty"):
        get_index(http)
