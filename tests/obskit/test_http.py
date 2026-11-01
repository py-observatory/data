import gzip
import json
import subprocess
import sys
import urllib.request
from email.message import Message

import pytest

from obskit.http import USER_AGENT, Http, HttpResponse, UpstreamError
from obskit.testing import FakeHttp

SIMPLE_ROOT = "https://pypi.org/simple/"
PAGE = "https://pypi.org/simple/example/"
VERSION_JSON = "https://pypi.org/pypi/example/1.0/json"
OTHER_HOST = "https://files.pythonhosted.org/example"
SIMPLE_JSON = "application/vnd.pypi.simple.v1+json"
# A gzip stream that stops after its 10-byte head.
GZIP_HEAD = bytes.fromhex("1f8b0800 00000000 00ff")


class Clock:
    """A clock that moves only when `sleep` is called, so no test waits for real."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class Dropping:
    """A transport that fails like a dropped connection for its first `drops`
    requests, and then passes every request on to `inner`."""

    def __init__(self, inner: FakeHttp, drops: int) -> None:
        self.inner = inner
        self.drops = drops

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        if self.drops:
            self.drops -= 1
            raise ConnectionResetError("connection reset by peer")
        return self.inner.request(method, url, headers, body)


class Timed:
    """A transport whose requests take `took` seconds on `clock`. It records the time
    at which each request began."""

    def __init__(self, inner: FakeHttp, clock: Clock, took: float) -> None:
        self.inner = inner
        self.clock = clock
        self.took = took
        self.starts: list[float] = []

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        self.starts.append(self.clock.now)
        self.clock.now += self.took
        return self.inner.request(method, url, headers, body)


@pytest.fixture
def fake() -> FakeHttp:
    return FakeHttp()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def http(fake: FakeHttp, clock: Clock) -> Http:
    return Http(transport=fake, sleep=clock.sleep, clock=clock)


# --- What a request carries ---


def test_sends_user_agent_without_email(http, fake):
    fake.add(PAGE, body="{}")

    http.get(PAGE)

    [(url, headers)] = fake.requests
    assert url == PAGE
    assert headers["User-Agent"] == USER_AGENT
    assert "@" not in headers["User-Agent"]
    assert not any("@" in value for value in headers.values())


def test_asks_for_gzip_and_sends_accept_and_if_none_match_only_when_given(http, fake):
    fake.add(PAGE, body="{}")

    http.get(PAGE)
    http.get(PAGE, accept=SIMPLE_JSON, etag='W/"abc"')

    bare, full = [headers for _, headers in fake.requests]
    assert bare == {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
    assert full == {**bare, "Accept": SIMPLE_JSON, "If-None-Match": 'W/"abc"'}


def test_etag_sends_if_none_match_and_returns_304(http, fake, clock):
    fake.add(PAGE, status=304, headers={"ETag": '"abc"'})

    response = http.get(PAGE, etag='"abc"')

    assert fake.requests[0][1]["If-None-Match"] == '"abc"'
    assert response == HttpResponse(304, {"etag": '"abc"'}, b"")
    assert clock.sleeps == []


# --- Replies that are not failures ---


def test_returns_404_without_raising(http, fake, clock):
    fake.add(PAGE, status=404, body={"message": "Not Found"})

    response = http.get(PAGE)

    assert response == HttpResponse(404, {}, b'{"message": "Not Found"}')
    assert clock.sleeps == []
    assert http.requests == 1


@pytest.mark.parametrize("status", [200, 304, 404, 410])
def test_these_statuses_are_returned_and_never_raised(http, fake, clock, status):
    headers = {"ETag": '"v1"', "X-PyPI-Last-Serial": "42"}
    fake.add(PAGE, status=status, body="text", headers=headers)

    response = http.get(PAGE)

    expected = {"etag": '"v1"', "x-pypi-last-serial": "42"}
    assert response == HttpResponse(status, expected, b"text")
    assert clock.sleeps == []
    assert http.requests == 1


# --- Gzip ---


def test_gzip_body_is_decoded(http, fake):
    original = '{"info": {"summary": "naïve"}}'.encode()
    headers = {"Content-Encoding": "gzip", "ETag": '"v1"'}
    fake.add(VERSION_JSON, body=gzip.compress(original), headers=headers)

    response = http.get(VERSION_JSON)

    assert response.body == original
    assert response.headers["etag"] == '"v1"'


@pytest.mark.parametrize("encoding", ["gzip", "GZIP", " gzip ", "x-gzip"])
def test_gzip_is_recognised_however_the_server_spells_it(http, fake, encoding):
    fake.add(
        PAGE, body=gzip.compress(b"payload"), headers={"Content-Encoding": encoding}
    )

    assert http.get(PAGE).body == b"payload"


def test_a_body_is_left_alone_unless_the_reply_says_it_is_gzip(http, fake):
    packed = gzip.compress(b"payload")  # a .gz file that is served as it is
    fake.add(PAGE, body=packed, headers={"Content-Type": "application/gzip"})

    assert http.get(PAGE).body == packed


def test_a_gzip_404_is_decoded_too(http, fake):
    body = gzip.compress(b'{"message": "Not Found"}')
    fake.add(PAGE, status=404, body=body, headers={"Content-Encoding": "gzip"})

    response = http.get(PAGE)

    assert (response.status, response.body) == (404, b'{"message": "Not Found"}')


def test_an_empty_gzip_encoded_reply_has_an_empty_body(http, fake):
    fake.add(PAGE, status=304, headers={"Content-Encoding": "gzip"})

    assert http.get(PAGE) == HttpResponse(304, {"content-encoding": "gzip"}, b"")


@pytest.mark.parametrize(
    "body",
    [b"not gzip at all", GZIP_HEAD, GZIP_HEAD + b"\xff\xff\xff\xff"],
    ids=["not gzip", "cut short", "garbled"],
)
def test_a_body_that_is_not_valid_gzip_is_a_failed_attempt(http, fake, clock, body):
    # The three ways that gzip.decompress fails are BadGzipFile, which is an OSError,
    # EOFError and zlib.error. A body damaged on its way is retried like a dropped
    # connection and then fails with the one error, whichever of them it was.
    fake.add(PAGE, body=body, headers={"Content-Encoding": "gzip"})

    with pytest.raises(UpstreamError) as caught:
        http.get(PAGE)

    assert caught.value.status is None
    assert caught.value.url == PAGE
    assert clock.sleeps == [2, 4, 8, 16]
    assert http.requests == 5


def test_a_damaged_body_that_is_resent_intact_succeeds(http, fake, clock):
    fake.add(
        PAGE, body=b"not gzip at all", headers={"Content-Encoding": "gzip"}, times=1
    )
    fake.add(PAGE, body=gzip.compress(b"payload"), headers={"Content-Encoding": "gzip"})

    assert http.get(PAGE).body == b"payload"
    assert clock.sleeps == [2]


# --- Retries ---


def test_retries_503_with_retry_after(http, fake, clock):
    fake.add(PAGE, status=503, headers={"Retry-After": "5"}, times=1)
    fake.add(PAGE, body="ok")

    response = http.get(PAGE)

    assert clock.sleeps == [5]
    assert (response.status, response.body) == (200, b"ok")
    assert http.requests == 2


def test_raises_after_retries(http, fake, clock):
    fake.add(PAGE, status=503)

    with pytest.raises(UpstreamError) as caught:
        http.get(PAGE)

    assert clock.sleeps == [2, 4, 8, 16]
    assert caught.value.status == 503
    assert caught.value.url == PAGE
    assert http.requests == len(fake.requests) == 5


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504, 599])
def test_a_status_that_may_pass_is_retried(http, fake, clock, status):
    fake.add(PAGE, status=status, times=2)
    fake.add(PAGE, body="ok")

    assert http.get(PAGE).status == 200
    assert clock.sleeps == [2, 4]
    assert http.requests == 3


@pytest.mark.parametrize(
    "status",
    [201, 204, 206, 301, 302, 308, 400, 401, 403, 405, 408, 409, 418, 451, 499, 600],
)
def test_any_other_status_raises_at_once(http, fake, clock, status):
    fake.add(PAGE, status=status)

    with pytest.raises(UpstreamError) as caught:
        http.get(PAGE)

    assert caught.value.status == status
    assert clock.sleeps == []
    assert http.requests == 1


@pytest.mark.parametrize(
    ("value", "slept"), [("7", 7), (" 3 ", 3), ("300", 300), ("900", 300)]
)
def test_retry_after_is_honoured_and_capped(http, fake, clock, value, slept):
    fake.add(PAGE, status=429, headers={"Retry-After": value}, times=1)
    fake.add(PAGE, body="ok")

    http.get(PAGE)

    assert clock.sleeps == [slept]


@pytest.mark.parametrize(
    "value", ["Wed, 21 Oct 2026 07:28:00 GMT", "soon", "-5", "1.5", ""]
)
def test_an_unusable_retry_after_falls_back_to_the_backoff(http, fake, clock, value):
    fake.add(PAGE, status=503, headers={"Retry-After": value}, times=1)
    fake.add(PAGE, body="ok")

    http.get(PAGE)

    assert clock.sleeps == [2]


def test_each_wait_is_the_servers_or_else_the_next_in_the_backoff(http, fake, clock):
    for headers in ({"Retry-After": "5"}, {}, {"Retry-After": "7"}, {}):
        fake.add(PAGE, status=503, headers=headers, times=1)
    fake.add(PAGE, body="ok")

    http.get(PAGE)

    assert clock.sleeps == [5, 4, 7, 16]


def test_network_error_is_retried(fake, clock):
    fake.add(PAGE, body="ok")
    http = Http(transport=Dropping(fake, drops=2), sleep=clock.sleep, clock=clock)

    assert http.get(PAGE).body == b"ok"
    assert clock.sleeps == [2, 4]
    assert http.requests == 3


def test_network_error_exhausted_raises_upstream_error(fake, clock):
    http = Http(transport=Dropping(fake, drops=5), sleep=clock.sleep, clock=clock)

    with pytest.raises(UpstreamError) as caught:
        http.get(PAGE)

    assert caught.value.status is None
    assert caught.value.url == PAGE
    assert isinstance(caught.value.__cause__, ConnectionResetError)
    assert "connection reset by peer" in str(caught.value)
    assert clock.sleeps == [2, 4, 8, 16]
    assert http.requests == 5


def test_the_fifth_attempt_is_the_last_and_can_still_succeed(fake, clock):
    # Four failures use up four retries. The fifth attempt gets no retry of its own,
    # but a good reply is still returned and is not read as a failure.
    fake.add(PAGE, body="ok")
    http = Http(transport=Dropping(fake, drops=4), sleep=clock.sleep, clock=clock)

    assert http.get(PAGE).body == b"ok"
    assert clock.sleeps == [2, 4, 8, 16]


def test_errors_say_which_request_failed(http, fake):
    fake.add(PAGE, status=403)

    with pytest.raises(UpstreamError) as caught:
        http.get(PAGE)

    message = str(caught.value)
    assert "GET" in message
    assert PAGE in message
    assert "HTTP 403" in message
    assert isinstance(caught.value, RuntimeError)


@pytest.mark.parametrize(
    ("headers", "asked"),
    [({"Retry-After": "12"}, 12), ({"Retry-After": "900"}, 300), ({}, None)],
)
def test_the_error_keeps_the_wait_that_the_server_asked_for(http, fake, headers, asked):
    fake.add(PAGE, status=429, headers=headers)

    with pytest.raises(UpstreamError) as caught:
        http.get(PAGE)

    assert caught.value.retry_after == asked


def test_requests_counts_every_attempt_that_went_out(http, fake):
    fake.add(PAGE, status=503, times=2)
    fake.add(PAGE, body="ok")
    assert http.requests == 0

    http.get(PAGE)
    http.get(PAGE)

    assert http.requests == 4
    assert http.requests == len(fake.requests)


# --- Pacing ---


def test_paces_requests_per_host(http, fake, clock):
    fake.add(SIMPLE_ROOT, body="{}")
    fake.add(VERSION_JSON, body="{}")
    fake.add(OTHER_HOST, body="x")

    http.get(SIMPLE_ROOT)
    assert clock.sleeps == [], "the first request to a host starts at once"

    http.get(VERSION_JSON)  # the JSON API is on the same host as the Simple API
    assert clock.sleeps == [0.25]

    http.get(OTHER_HOST)
    assert clock.sleeps == [0.25], "another host has its own pace"


def test_a_host_is_the_same_host_whatever_its_case_port_or_scheme(http, clock):
    http.get("https://PyPI.org/a")
    http.get("https://pypi.org:443/b")
    http.get("http://pypi.org/c")

    assert clock.sleeps == [0.25, 0.25]


def test_the_interval_runs_from_the_start_of_the_last_request(fake, clock):
    timed = Timed(fake, clock, took=0.125)
    http = Http(transport=timed, sleep=clock.sleep, clock=clock)

    for _ in range(40):
        http.get(PAGE)

    # The interval runs from the start of the last request and not from its end, so
    # a request that took 0.125 s is followed by a wait of the other 0.125 s.
    gaps = [later - earlier for earlier, later in zip(timed.starts, timed.starts[1:])]
    assert gaps == [0.25] * 39
    assert clock.sleeps == [0.125] * 39


def test_no_wait_is_left_when_the_interval_has_passed(http, clock):
    http.get(PAGE)
    clock.now += 0.25

    http.get(PAGE)

    assert clock.sleeps == []


def test_only_what_is_left_of_the_interval_is_waited(http, clock):
    http.get(PAGE)
    clock.now += 0.0625

    http.get(PAGE)

    assert clock.sleeps == [0.1875]


@pytest.mark.parametrize(("interval", "slept"), [(0.25, [0.25]), (1.5, [1.5]), (0, [])])
def test_the_interval_is_a_setting(fake, clock, interval, slept):
    http = Http(interval=interval, transport=fake, sleep=clock.sleep, clock=clock)

    http.get(PAGE)
    http.get(PAGE)

    assert clock.sleeps == slept


def test_a_retry_is_paced_like_any_other_request(http, fake, clock):
    # Retry-After: 0 asks for no wait, but the retry is a request to the same host.
    fake.add(PAGE, status=429, headers={"Retry-After": "0"}, times=1)
    fake.add(PAGE, body="ok")

    http.get(PAGE)

    assert clock.sleeps == [0.25]


def test_a_retry_waits_for_the_interval_when_that_is_longer_than_its_own_wait(
    fake, clock
):
    http = Http(interval=10, transport=fake, sleep=clock.sleep, clock=clock)
    fake.add(PAGE, status=429, headers={"Retry-After": "5"}, times=1)
    fake.add(PAGE, body="ok")

    http.get(PAGE)

    assert clock.sleeps == [10], "one wait, and it is the longer of the two"


def test_the_wait_before_a_retry_is_not_followed_by_another_for_the_pace(fake):
    # The clock never moves, as when a test gives `sleep` a stand-in that records and
    # does not wait. A retry's wait and the pace's wait are then one wait and not two.
    sleeps: list[float] = []
    http = Http(transport=fake, sleep=sleeps.append, clock=lambda: 0.0)
    fake.add(PAGE, status=503)

    with pytest.raises(UpstreamError):
        http.get(PAGE)

    assert sleeps == [2, 4, 8, 16]


def test_pacing_with_a_stand_in_sleep_and_the_real_clock_does_not_hang(fake):
    http = Http(transport=fake, sleep=lambda seconds: None)

    for _ in range(200):
        http.get(PAGE)

    assert http.requests == 200


# --- The real transport ---


def headers_of(**fields: str) -> Message:
    message = Message()
    for name, value in fields.items():
        message[name.replace("_", "-")] = value
    return message


class Reply:
    """What urlopen returns: a context manager with `status`, `headers` and `read`."""

    def __init__(self, status: int, headers: Message, body: bytes) -> None:
        self.status = status
        self.headers = headers
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def read(self) -> bytes:
        return self.body


def test_http_defaults_to_the_urllib_transport(monkeypatch, clock):
    seen = []

    def urlopen(request, timeout):
        seen.append(request)
        reply_headers = headers_of(Content_Encoding="gzip", ETag='"v1"')
        return Reply(200, reply_headers, gzip.compress(b'{"ok": true}'))

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)

    response = Http(sleep=clock.sleep, clock=clock).get(
        PAGE, accept=SIMPLE_JSON, etag='"v0"'
    )

    assert response == HttpResponse(
        200, {"content-encoding": "gzip", "etag": '"v1"'}, b'{"ok": true}'
    )
    [request] = seen
    assert request.full_url == PAGE
    assert request.get_method() == "GET"
    assert request.get_header("User-agent") == USER_AGENT
    assert request.get_header("Accept-encoding") == "gzip"
    assert request.get_header("Accept") == SIMPLE_JSON
    assert request.get_header("If-none-match") == '"v0"'


# --- Importing: github.py imports USER_AGENT from http.py ---


def run_python(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=60
    )


@pytest.mark.parametrize(
    "first, second",
    [("http", "github"), ("github", "http")],
    ids=["http first", "github first"],
)
def test_http_and_github_import_in_either_order(first, second):
    # A fresh interpreter for each order, because the import system remembers a
    # module that it has loaded and a cycle only fails for whichever imports first.
    result = run_python(f"import obskit.{first}\nimport obskit.{second}")

    assert result.returncode == 0, result.stderr


def test_importing_http_does_not_load_github():
    # What keeps the two modules from importing each other: http.py names Transport
    # only for the type checker, and imports UrllibTransport when it needs one.
    result = run_python(
        "import sys\nimport obskit.http\nassert 'obskit.github' not in sys.modules"
    )

    assert result.returncode == 0, result.stderr


# --- FakeHttp itself ---


def get(fake: FakeHttp, url: str) -> tuple[int, dict[str, str], bytes]:
    return fake.request("GET", url, {}, None)


def test_fake_serves_the_replies_of_a_url_in_the_order_added(fake):
    fake.add(PAGE, status=503, times=2)
    fake.add(PAGE, status=429, times=1)
    fake.add(PAGE, body="done")

    statuses = [get(fake, PAGE)[0] for _ in range(6)]

    assert statuses == [503, 503, 429, 200, 200, 200]


def test_fake_reply_without_times_serves_for_ever_and_hides_the_later_ones(fake):
    fake.add(PAGE, body="first")
    fake.add(PAGE, body="second")

    bodies = [get(fake, PAGE)[2] for _ in range(4)]

    assert bodies == [b"first"] * 4


def test_fake_urls_have_their_own_replies(fake):
    fake.add(PAGE, body="page")
    fake.add(VERSION_JSON, status=410)

    assert get(fake, VERSION_JSON)[0] == 410
    assert get(fake, PAGE)[2] == b"page"


def test_fake_url_with_no_reply_left_is_a_404_with_no_body(fake):
    fake.add(PAGE, body="once", times=1)

    assert get(fake, PAGE) == (200, {}, b"once")
    assert get(fake, PAGE) == (404, {}, b"")
    assert get(fake, SIMPLE_ROOT) == (404, {}, b"")
    assert get(fake, PAGE + "?x=1") == (404, {}, b""), "a URL matches as a whole"


def test_fake_encodes_the_body_of_a_reply(fake):
    fake.add("https://x.test/dict", body={"name": "naïve", "n": [1, 2]})
    fake.add("https://x.test/text", body="naïve")
    fake.add("https://x.test/bytes", body=b"\xff\x00")
    fake.add("https://x.test/none")

    dict_body = get(fake, "https://x.test/dict")[2]
    assert json.loads(dict_body) == {"name": "naïve", "n": [1, 2]}
    assert get(fake, "https://x.test/text")[2] == "naïve".encode("utf-8")
    assert get(fake, "https://x.test/bytes")[2] == b"\xff\x00"
    assert get(fake, "https://x.test/none")[2] == b""


def test_fake_lowercases_header_names_as_a_transport_does(fake):
    fake.add(PAGE, headers={"ETag": '"v1"', "Retry-After": "5"})

    _, headers, _ = get(fake, PAGE)

    assert headers == {"etag": '"v1"', "retry-after": "5"}


def test_fake_replies_do_not_share_their_headers(fake):
    fake.add(PAGE, headers={"ETag": '"v1"'})

    get(fake, PAGE)[1]["etag"] = "changed"

    assert get(fake, PAGE)[1] == {"etag": '"v1"'}


def test_fake_records_the_url_and_headers_of_every_request(fake):
    fake.add(PAGE, body="x", times=1)
    sent = {"User-Agent": "test"}

    fake.request("GET", PAGE, sent, None)
    fake.request("GET", SIMPLE_ROOT, {"Accept": "a/b"}, None)  # answered 404
    sent["User-Agent"] = "changed after the request"

    assert fake.requests == [
        (PAGE, {"User-Agent": "test"}),
        (SIMPLE_ROOT, {"Accept": "a/b"}),
    ]


@pytest.mark.parametrize("times", [0, -1])
def test_fake_reply_needs_at_least_one_turn(fake, times):
    with pytest.raises(ValueError):
        fake.add(PAGE, times=times)
