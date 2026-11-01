import hashlib
import http.client
import io
import urllib.error
import urllib.request
from email.message import Message
from pathlib import Path

import pytest

from obskit.github import Fatal, GitHub, NotFound, Transient, UrllibTransport
from obskit.http import USER_AGENT
from obskit.testing import FakeGitHub

REPO = "py-observatory/data"
TOKEN = "t0ken"
TAG = "example-current"
RELEASES_URL = f"https://api.github.com/repos/{REPO}/releases"
# What an asset really is: binary, not text. A zstd magic number, then every byte value.
BINARY = bytes.fromhex("28b52ffd") + bytes(range(256))


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


class Canned:
    """A transport that gives every request the same reply and counts the requests."""

    def __init__(
        self, status: int, headers: dict[str, str] | None = None, body: bytes = b""
    ) -> None:
        self.reply = (status, headers or {}, body)
        self.requests = 0

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        self.requests += 1
        return self.reply


class Dropping:
    """A transport that fails like a dropped connection for its first `drops`
    requests, and then passes every request on to `inner`."""

    def __init__(self, inner: FakeGitHub, drops: int) -> None:
        self.inner = inner
        self.drops = drops

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        if self.drops:
            self.drops -= 1
            raise ConnectionResetError("connection reset by peer")
        return self.inner.request(method, url, headers, body)


@pytest.fixture
def fake() -> FakeGitHub:
    return FakeGitHub()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def gh(fake: FakeGitHub, clock: Clock) -> GitHub:
    return GitHub(REPO, TOKEN, transport=fake, sleep=clock.sleep, clock=clock)


def client(transport, clock: Clock) -> GitHub:
    return GitHub(REPO, TOKEN, transport=transport, sleep=clock.sleep, clock=clock)


def write(tmp_path: Path, name: str, data: bytes) -> Path:
    path = tmp_path / name
    path.write_bytes(data)
    return path


def asset_names(fake: FakeGitHub, tag: str = TAG) -> list[str]:
    release = fake.release(tag)
    assert release is not None
    return [asset["name"] for asset in release["assets"]]


# --- Errors are classified by status ---


def test_release_by_tag_404_raises_not_found(gh, fake, clock):
    with pytest.raises(NotFound) as caught:
        gh.release_by_tag(TAG)

    assert caught.value.status == 404
    assert clock.sleeps == [], "a 404 is never retried"
    assert len(fake.calls) == 1


def test_release_by_tag_returns_the_release(gh):
    created = gh.create_release(TAG)

    release = gh.release_by_tag(TAG)

    assert release["id"] == created["id"]
    assert release["tag_name"] == TAG


def test_a_tag_is_quoted_in_the_url(gh, fake):
    gh.create_release("odd tag/#1")

    assert gh.release_by_tag("odd tag/#1")["tag_name"] == "odd tag/#1"

    assert fake.calls[-1][1].endswith("/releases/tags/odd%20tag%2F%231")


def test_502_twice_then_ok(gh, fake, clock):
    gh.create_release(TAG)
    fake.fail("GET", "/releases/tags/", 502, times=2)

    release = gh.release_by_tag(TAG)

    assert release["tag_name"] == TAG
    assert clock.sleeps == [2, 4]
    assert [method for method, _, _ in fake.calls].count("GET") == 3


def test_502_exhausted_raises_transient(gh, fake, clock):
    # No release exists, so a 404 is what the seventh request would get. The six
    # that fail with 502 must never be read as "not found".
    fake.fail("GET", "/releases/tags/", 502, times=6)

    with pytest.raises(Transient) as caught:
        gh.release_by_tag(TAG)

    assert caught.value.status == 502
    assert clock.sleeps == [2, 4, 8, 16, 32]
    assert len(fake.calls) == 6
    assert TOKEN not in str(caught.value)


@pytest.mark.parametrize(
    ("headers", "slept"),
    [({"x-ratelimit-remaining": "0"}, 2), ({"retry-after": "3"}, 3)],
    ids=["primary limit", "retry-after"],
)
def test_403_rate_limited_is_transient(gh, fake, clock, headers, slept):
    gh.create_release(TAG)
    fake.fail("GET", "/releases/tags/", 403, headers=headers)

    assert gh.release_by_tag(TAG)["tag_name"] == TAG
    assert clock.sleeps == [slept]

    fake.fail("GET", "/releases/tags/", 403, times=6, headers=headers)
    with pytest.raises(Transient) as caught:
        gh.release_by_tag(TAG)
    assert caught.value.status == 403


@pytest.mark.parametrize(
    "headers", [{}, {"x-ratelimit-remaining": "57"}], ids=["bare", "quota left"]
)
def test_403_plain_is_fatal(gh, fake, clock, headers):
    fake.fail("GET", "/releases/tags/", 403, times=6, headers=headers)

    with pytest.raises(Fatal) as caught:
        gh.release_by_tag(TAG)

    assert caught.value.status == 403
    assert clock.sleeps == []
    assert len(fake.calls) == 1


@pytest.mark.parametrize(("value", "slept"), [("7", 7), ("300", 300), ("900", 300)])
def test_retry_after_is_honoured_and_capped(gh, fake, clock, value, slept):
    gh.create_release(TAG)
    fake.fail("GET", "/releases/tags/", 429, headers={"retry-after": value})

    gh.release_by_tag(TAG)

    assert clock.sleeps == [slept]


@pytest.mark.parametrize(
    "value", ["Wed, 21 Oct 2026 07:28:00 GMT", "soon", "-5", "1.5", ""]
)
def test_unusable_retry_after_falls_back_to_the_backoff(gh, fake, clock, value):
    gh.create_release(TAG)
    fake.fail("GET", "/releases/tags/", 503, headers={"retry-after": value})

    gh.release_by_tag(TAG)

    assert clock.sleeps == [2]


@pytest.mark.parametrize(
    ("status", "kind"),
    [
        (404, NotFound),
        *[(status, Transient) for status in (408, 429, 500, 502, 503, 504, 599)],
        *[(status, Fatal) for status in (400, 401, 403, 409, 410, 422, 451)],
        (301, Fatal),
        (304, Fatal),
    ],
)
def test_each_status_has_one_class(clock, status, kind):
    canned = Canned(status)

    with pytest.raises(kind) as caught:
        client(canned, clock).release_by_tag(TAG)

    assert type(caught.value) is kind
    assert caught.value.status == status
    assert canned.requests == (6 if kind is Transient else 1)


@pytest.mark.parametrize(
    ("status", "message", "kind"),
    [
        (502, "Not Found", Transient),
        (404, "Bad Gateway", NotFound),
        (403, "API rate limit exceeded", Fatal),
    ],
)
def test_message_text_never_decides_the_class(clock, status, message, kind):
    canned = Canned(status, body=f'{{"message": "{message}"}}'.encode())

    with pytest.raises(kind):
        client(canned, clock).release_by_tag(TAG)


def test_network_error_is_retried(fake, clock):
    client(fake, clock).create_release(TAG)
    gh = client(Dropping(fake, drops=2), clock)

    assert gh.release_by_tag(TAG)["tag_name"] == TAG
    assert clock.sleeps == [2, 4]


def test_network_error_exhausted_raises_transient(fake, clock):
    gh = client(Dropping(fake, drops=6), clock)

    with pytest.raises(Transient) as caught:
        gh.release_by_tag(TAG)

    assert caught.value.status is None
    assert isinstance(caught.value.__cause__, ConnectionResetError)
    assert "connection reset by peer" in str(caught.value)
    assert TOKEN not in str(caught.value)
    assert clock.sleeps == [2, 4, 8, 16, 32]


def test_errors_say_which_request_failed_and_never_show_the_token(gh, tmp_path):
    release = gh.create_release(TAG)
    path = write(tmp_path, "a", b"x")
    gh.upload(release, "a.zst", path)

    with pytest.raises(Fatal) as caught:
        gh.upload(release, "a.zst", path)

    message = str(caught.value)
    assert "POST" in message
    assert "/assets?name=a.zst" in message
    assert "HTTP 422" in message
    assert "already_exists" in message, "GitHub's own explanation is kept"
    assert TOKEN not in message
    assert TOKEN not in repr(gh)


@pytest.mark.parametrize(
    ("body", "call"),
    [
        (b"<html>captive portal</html>", lambda gh: gh.release_by_tag(TAG)),
        (b"[]", lambda gh: gh.release_by_tag(TAG)),
        (b"{}", lambda gh: gh.assets({"id": 1})),
        (b"", lambda gh: gh.create_release(TAG)),
    ],
    ids=["not JSON", "array for object", "object for array", "empty"],
)
def test_a_success_that_is_not_the_expected_json_is_fatal(clock, body, call):
    canned = Canned(200, body=body)

    with pytest.raises(Fatal):
        call(client(canned, clock))

    assert canned.requests == 1


# --- Releases and assets ---


def test_create_release_is_a_prerelease_and_not_latest_by_default(gh, fake):
    created = gh.create_release("example-month-2026-11")

    assert created["prerelease"] is True
    release = fake.release("example-month-2026-11")
    assert release["prerelease"] is True
    assert release["make_latest"] == "false"


def test_create_release_can_be_latest(gh, fake):
    created = gh.create_release("catalog-current", prerelease=False, latest=True)

    assert created["prerelease"] is False
    assert fake.release("catalog-current")["make_latest"] == "true"


def test_a_second_release_for_a_tag_is_fatal_422(gh, fake):
    first = gh.create_release(TAG)

    with pytest.raises(Fatal) as caught:
        gh.create_release(TAG)

    assert caught.value.status == 422
    assert fake.release(TAG)["id"] == first["id"]


def test_upload_lists_digest(gh, fake, tmp_path):
    data = BINARY
    release = gh.create_release(TAG)

    asset = gh.upload(release, "state.db.zst", write(tmp_path, "state.db.zst", data))

    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    assert asset["name"] == "state.db.zst"
    assert asset["size"] == len(data)
    assert asset["digest"] == digest
    assert asset["browser_download_url"] == (
        f"https://github.com/{REPO}/releases/download/{TAG}/state.db.zst"
    )
    assert gh.assets(release) == [asset]
    assert fake.asset_bytes(TAG, "state.db.zst") == data


def test_upload_goes_to_the_uploads_host_as_octet_stream(gh, fake, tmp_path):
    release = gh.create_release(TAG)

    gh.upload(release, "a b.zst", write(tmp_path, "src", b"x"))

    method, url, headers = fake.calls[-1]
    assert method == "POST"
    assert url == (
        f"https://uploads.github.com/repos/{REPO}/releases/{release['id']}"
        "/assets?name=a%20b.zst"
    )
    assert headers["Content-Type"] == "application/octet-stream"
    assert asset_names(fake) == ["a b.zst"]


def test_duplicate_upload_is_fatal_422(gh, fake, clock, tmp_path):
    release = gh.create_release(TAG)
    gh.upload(release, "a.zst", write(tmp_path, "first", b"first"))

    with pytest.raises(Fatal) as caught:
        gh.upload(release, "a.zst", write(tmp_path, "second", b"second"))

    assert caught.value.status == 422
    assert clock.sleeps == []
    assert fake.asset_bytes(TAG, "a.zst") == b"first", "the original is untouched"


def test_rename_and_delete(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    nxt = gh.upload(release, "swap-next-a", write(tmp_path, "a", b"aaa"))
    other = gh.upload(release, "b", write(tmp_path, "b", b"bbb"))

    renamed = gh.rename(nxt, "a")

    assert renamed["id"] == nxt["id"]
    assert renamed["name"] == "a"
    assert renamed["browser_download_url"].endswith(f"/{TAG}/a")
    assert [asset["name"] for asset in gh.assets(release)] == ["a", "b"]
    assert fake.asset_bytes(TAG, "a") == b"aaa"

    with pytest.raises(Fatal) as caught:
        gh.rename(other, "a")
    assert caught.value.status == 422
    assert asset_names(fake) == ["a", "b"]

    gh.delete(other)
    assert asset_names(fake) == ["a"]
    with pytest.raises(NotFound):
        gh.delete(other)


@pytest.mark.parametrize(("count", "pages"), [(0, 1), (99, 1), (100, 2), (250, 3)])
def test_assets_are_read_a_page_at_a_time(gh, fake, tmp_path, count, pages):
    release = gh.create_release(TAG)
    path = write(tmp_path, "x", b"x")
    for i in range(count):
        gh.upload(release, f"asset-{i:03}", path)
    fake.calls.clear()

    found = gh.assets(release)

    assert [asset["name"] for asset in found] == [f"asset-{i:03}" for i in range(count)]
    assert [url.rsplit("?", 1)[1] for _, url, _ in fake.calls] == [
        f"per_page=100&page={page}" for page in range(1, pages + 1)
    ]


def test_a_failure_while_paging_is_raised_and_not_a_short_list(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    path = write(tmp_path, "x", b"x")
    for i in range(150):
        gh.upload(release, f"asset-{i:03}", path)
    fake.fail("GET", "page=2", 500, times=6)

    with pytest.raises(Transient):
        gh.assets(release)


def test_download_has_no_authorization_header(gh, fake, tmp_path):
    data = BINARY
    release = gh.create_release(TAG)
    asset = gh.upload(release, "state.db.zst", write(tmp_path, "src", data))
    dst = tmp_path / "out.zst"

    gh.download(asset, dst)

    assert dst.read_bytes() == data
    method, url, headers = fake.calls[-1]
    assert (method, url) == ("GET", asset["browser_download_url"])
    assert "authorization" not in {name.lower() for name in headers}
    assert headers["User-Agent"] == USER_AGENT


def test_download_of_a_missing_asset_is_not_found_and_writes_nothing(gh, tmp_path):
    gh.create_release(TAG)
    url = f"https://github.com/{REPO}/releases/download/{TAG}/nope"
    missing = {"browser_download_url": url}
    dst = tmp_path / "out"

    with pytest.raises(NotFound):
        gh.download(missing, dst)

    assert not dst.exists()


def test_a_failed_download_writes_nothing(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    asset = gh.upload(release, "a", write(tmp_path, "src", b"x"))
    fake.fail("GET", "/releases/download/", 502, times=6)
    dst = tmp_path / "out"

    with pytest.raises(Transient):
        gh.download(asset, dst)

    assert not dst.exists()


def test_api_requests_carry_the_documented_headers(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    gh.release_by_tag(TAG)
    asset = gh.upload(release, "a", write(tmp_path, "a", b"x"))
    gh.assets(release)
    asset = gh.rename(asset, "b")
    gh.delete(asset)

    assert [method for method, _, _ in fake.calls] == [
        "POST",
        "GET",
        "POST",
        "GET",
        "PATCH",
        "DELETE",
    ]
    for _, _, headers in fake.calls:
        assert headers["Authorization"] == f"Bearer {TOKEN}"
        assert headers["Accept"] == "application/vnd.github+json"
        assert headers["X-GitHub-Api-Version"] == "2022-11-28"
        assert headers["User-Agent"] == USER_AGENT
    assert [headers.get("Content-Type") for _, _, headers in fake.calls] == [
        "application/json",
        None,
        "application/octet-stream",
        None,
        "application/json",
        None,
    ]


def test_user_agent_is_the_project_constant():
    assert USER_AGENT == "py-observatory/0.1 (+https://github.com/py-observatory/data)"
    assert "@" not in USER_AGENT


# --- Write pacing ---


def test_write_pacing_81st_call_waits(gh, clock):
    for i in range(80):
        gh.create_release(f"t{i}")
    assert clock.sleeps == []

    gh.create_release("t80")

    assert clock.sleeps == [60]


def test_write_pacing_501st_call_waits_for_the_hour(gh, clock):
    for i in range(500):
        gh.create_release(f"t{i}")
    # 80 a minute: a minute's wait before each of the calls 81, 161, ... 481.
    assert clock.sleeps == [60] * 6

    gh.create_release("t500")

    # The first call was at 0 s and the 500th at 360 s, so the 501st waits for 3600 s.
    assert clock.sleeps == [60] * 6 + [3600 - 360]


def test_no_window_holds_more_writes_than_the_limits(fake, clock):
    # Each request takes a quarter of a second. Any 81 writes in a row must then span
    # at least a minute, and any 501 in a row at least an hour.
    took = 0.25
    starts: list[float] = []

    class Timed:
        def request(self, method, url, headers, body):
            starts.append(clock.now)
            clock.now += took
            return fake.request(method, url, headers, body)

    gh = client(Timed(), clock)
    for i in range(1200):
        gh.create_release(f"t{i}")

    assert all(starts[i + 80] - starts[i] >= 60 for i in range(len(starts) - 80))
    assert all(starts[i + 500] - starts[i] >= 3600 for i in range(len(starts) - 500))
    # Nor does it wait for nothing: every write starts as early as those limits allow.
    earliest: list[float] = []
    for i in range(1200):
        start = earliest[-1] + took if earliest else 0.0
        if i >= 80:
            start = max(start, earliest[i - 80] + 60)
        if i >= 500:
            start = max(start, earliest[i - 500] + 3600)
        earliest.append(start)
    assert starts == earliest


def test_reads_are_not_paced(gh, clock):
    gh.create_release(TAG)
    for _ in range(200):
        gh.release_by_tag(TAG)
    for i in range(79):
        gh.create_release(f"t{i}")  # the 80th write

    assert clock.sleeps == []


def test_every_attempt_of_a_write_is_paced(gh, fake, clock):
    for i in range(79):
        gh.create_release(f"t{i}")
    fake.fail("POST", "/releases", 502)

    gh.create_release("last")  # the 80th write fails, so its retry is the 81st

    assert clock.sleeps == [2, 58]


def test_writes_with_a_stubbed_sleep_and_the_real_clock_do_not_hang(fake):
    gh = GitHub(REPO, TOKEN, transport=fake, sleep=lambda seconds: None)

    for i in range(600):
        gh.create_release(f"t{i}")

    assert fake.release("t599") is not None


# --- UrllibTransport, with urlopen replaced: tests never touch the network ---


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


def test_urllib_transport_returns_status_lowercase_headers_and_body(monkeypatch):
    seen = {}

    def urlopen(request, timeout):
        seen["request"], seen["timeout"] = request, timeout
        reply_headers = headers_of(
            Content_Type="application/json", X_RateLimit_Used="3"
        )
        return Reply(201, reply_headers, b'{"id": 1}')

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)

    reply = UrllibTransport().request(
        "POST", "https://example.test/x?y=1", {"Accept": "a/b"}, b"payload"
    )

    assert reply == (
        201,
        {"content-type": "application/json", "x-ratelimit-used": "3"},
        b'{"id": 1}',
    )
    request = seen["request"]
    assert request.get_method() == "POST"
    assert request.full_url == "https://example.test/x?y=1"
    assert request.data == b"payload"
    assert request.get_header("Accept") == "a/b"
    assert seen["timeout"] == 60


def test_urllib_transport_returns_an_http_error_instead_of_raising(monkeypatch):
    def urlopen(request, timeout):
        raise urllib.error.HTTPError(
            request.full_url,
            502,
            "Bad Gateway",
            headers_of(Retry_After="7"),
            io.BytesIO(b"upstream down"),
        )

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)

    reply = UrllibTransport().request("GET", "https://example.test/x", {}, None)

    assert reply == (502, {"retry-after": "7"}, b"upstream down")


class Truncated(Reply):
    def read(self) -> bytes:
        raise http.client.IncompleteRead(b"par", 10)


@pytest.mark.parametrize(
    "failure",
    [
        urllib.error.URLError("connection refused"),
        TimeoutError("timed out"),
        http.client.IncompleteRead(b"par", 10),
        http.client.BadStatusLine("garbage"),
        Truncated(200, Message(), b""),
    ],
    ids=["URLError", "timeout", "IncompleteRead", "BadStatusLine", "truncated body"],
)
def test_urllib_transport_network_failures_are_os_errors(monkeypatch, failure):
    def urlopen(request, timeout):
        if isinstance(failure, BaseException):
            raise failure
        return failure

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)

    with pytest.raises(OSError):
        UrllibTransport().request("GET", "https://example.test/x", {}, None)


def test_github_defaults_to_the_urllib_transport(monkeypatch, clock):
    seen = []

    def urlopen(request, timeout):
        seen.append(request)
        return Reply(200, headers_of(), b'{"id": 7, "tag_name": "example-current"}')

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)

    release = GitHub(REPO, TOKEN, sleep=clock.sleep, clock=clock).release_by_tag(TAG)

    assert release["id"] == 7
    [request] = seen
    assert request.full_url == f"{RELEASES_URL}/tags/{TAG}"
    assert request.get_header("Authorization") == f"Bearer {TOKEN}"


# --- FakeGitHub itself ---


def test_fake_release_and_asset_bytes(gh, fake, tmp_path):
    assert fake.release(TAG) is None
    release = gh.create_release(TAG)
    gh.upload(release, "a", write(tmp_path, "a", b"abc"))

    seen = fake.release(TAG)

    assert seen["id"] == release["id"]
    assert seen["tag_name"] == TAG
    assert [asset["name"] for asset in seen["assets"]] == ["a"]
    assert fake.asset_bytes(TAG, "a") == b"abc"
    with pytest.raises(KeyError):
        fake.asset_bytes(TAG, "missing")
    with pytest.raises(KeyError):
        fake.asset_bytes("no-such-tag", "a")


def test_fake_release_objects_do_not_embed_assets(gh, tmp_path):
    release = gh.create_release(TAG)
    gh.upload(release, "a", write(tmp_path, "a", b"abc"))

    assert "assets" not in gh.release_by_tag(TAG), "code must read the assets endpoint"


def test_fake_calls_record_every_attempt_in_order(gh, fake):
    gh.create_release(TAG)
    fake.fail("GET", "/releases/tags/", 502)

    gh.release_by_tag(TAG)

    tag_url = f"{RELEASES_URL}/tags/{TAG}"
    assert [(method, url) for method, url, _ in fake.calls] == [
        ("POST", RELEASES_URL),
        ("GET", tag_url),
        ("GET", tag_url),  # the retry after the 502
    ]


def test_fake_fail_matches_by_method_and_url_substring_and_runs_out():
    fake = FakeGitHub()
    fake.fail("GET", "/releases/tags/", 502, times=2, headers={"Retry-After": "9"})
    tag_url = f"{RELEASES_URL}/tags/x"

    assert fake.request("DELETE", f"{RELEASES_URL}/assets/1", {}, None)[0] == 404
    assert fake.request("GET", f"{RELEASES_URL}/1/assets", {}, None)[0] == 404
    for _ in range(2):
        status, headers, _ = fake.request("GET", tag_url, {}, None)
        assert (status, headers["retry-after"]) == (502, "9"), "names are lowercased"
    assert fake.request("GET", tag_url, {}, None)[0] == 404


@pytest.mark.parametrize(
    "fields",
    [
        '{"tag_name": "x", "make_latest": false}',
        '{"tag_name": "x", "make_latest": "yes"}',
        '{"tag_name": "x", "prerelease": "true"}',
        '{"make_latest": "false"}',
        '{"tag_name": ""}',
    ],
    ids=[
        "boolean make_latest",
        "bad make_latest",
        "string prerelease",
        "no tag",
        "empty tag",
    ],
)
def test_fake_refuses_a_release_request_that_github_would_refuse(fake, fields):
    status, _, _ = fake.request("POST", RELEASES_URL, {}, fields.encode())

    assert status == 422
    assert fake.release("x") is None


def test_fake_makes_a_release_latest_unless_told_otherwise(fake):
    # GitHub's default, which is why the client always sends make_latest.
    status, _, _ = fake.request("POST", RELEASES_URL, {}, b'{"tag_name": "x"}')

    assert status == 201
    assert fake.release("x")["make_latest"] == "true"
    assert fake.release("x")["prerelease"] is False


def test_fake_refuses_a_body_that_is_not_a_json_object(fake):
    for body in (b"not json", b"[]", None):
        assert fake.request("POST", RELEASES_URL, {}, body)[0] == 400


def test_fake_fail_does_not_carry_out_the_request(fake):
    fake.fail("POST", "/releases", 500)

    status, _, _ = fake.request("POST", RELEASES_URL, {}, b'{"tag_name": "x"}')

    assert status == 500
    assert fake.release("x") is None, "the failed request changed nothing"


def test_fail_aims_at_the_rename_of_one_asset_by_name(gh, fake, tmp_path):
    fake.fail("PATCH", "name=swap-next-b", 422)  # before the asset exists
    release = gh.create_release(TAG)
    first = gh.upload(release, "swap-next-a", write(tmp_path, "a", b"a"))
    second = gh.upload(release, "swap-next-b", write(tmp_path, "b", b"b"))

    gh.rename(first, "a")  # another asset is not affected
    with pytest.raises(Fatal) as caught:
        gh.rename(second, "b")
    assert caught.value.status == 422
    assert asset_names(fake) == ["a", "swap-next-b"]

    gh.rename(second, "b")  # the injected failure was used up
    assert asset_names(fake) == ["a", "b"]


def test_fail_name_is_the_name_at_the_time_of_the_request(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    state = gh.upload(release, "state.db.zst", write(tmp_path, "s", b"old"))
    nxt = gh.upload(release, "swap-next-state.db.zst", write(tmp_path, "n", b"new"))
    fake.fail("PATCH", "name=state.db.zst", 422)

    # `name=state.db.zst` matches the asset called exactly that. The asset called
    # `swap-next-state.db.zst` does not match, though its name ends the same way.
    gh.rename(nxt, "final.db.zst")
    with pytest.raises(Fatal):
        gh.rename(state, "swap-prev-state.db.zst")
    gh.rename(state, "swap-prev-state.db.zst")

    assert asset_names(fake) == ["swap-prev-state.db.zst", "final.db.zst"]
    assert fake.asset_bytes(TAG, "final.db.zst") == b"new"
    assert fake.asset_bytes(TAG, "swap-prev-state.db.zst") == b"old"


def test_fail_aims_at_one_asset_by_id(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    first = gh.upload(release, "a", write(tmp_path, "a", b"a"))
    second = gh.upload(release, "b", write(tmp_path, "b", b"b"))
    fake.fail("DELETE", f"/releases/assets/{second['id']}", 500, times=6)

    gh.delete(first)
    with pytest.raises(Transient):
        gh.delete(second)

    assert asset_names(fake) == ["b"]


@pytest.mark.parametrize(
    "url",
    [
        "https://api.github.com/rate_limit",
        "https://api.github.com/repos/someone/else/releases/tags/x",
        "https://example.test/repos/py-observatory/data/releases/tags/x",
    ],
    ids=["unknown path", "other repository", "other host"],
)
def test_fake_refuses_routes_it_does_not_serve(fake, url):
    # A 404 is what the kit reads as "no release", so no unserved route may give one.
    with pytest.raises(NotImplementedError):
        fake.request("GET", url, {}, None)


def test_fake_fail_needs_at_least_one_turn(fake):
    with pytest.raises(ValueError):
        fake.fail("GET", "/releases/", 500, times=0)


def test_fake_serves_only_its_own_repository(fake, clock):
    other = GitHub(
        "someone/else", TOKEN, transport=fake, sleep=clock.sleep, clock=clock
    )

    with pytest.raises(NotImplementedError):
        other.release_by_tag(TAG)
