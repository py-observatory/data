import hashlib
import http.client
import io
import json
import urllib.error
import urllib.request
from email.message import Message
from pathlib import Path

import pytest

from obskit.github import (
    Fatal,
    GitHub,
    NotFound,
    Transient,
    UrllibTransport,
    _AuthorizationStaysOnHost,
)
from obskit.http import USER_AGENT
from obskit.testing import FakeGitHub

REPO = "py-observatory/data"
TOKEN = "t0ken"
TAG = "example-current"
RELEASES_URL = f"https://api.github.com/repos/{REPO}/releases"
ASSETS_URL = f"{RELEASES_URL}/assets"
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
    assert asset["url"] == f"{ASSETS_URL}/{asset['id']}"
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


def test_download_asks_the_api_for_the_asset_by_its_id(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    asset = gh.upload(release, "state.db.zst", write(tmp_path, "src", BINARY))
    dst = tmp_path / "out.zst"
    fake.calls.clear()

    gh.download(asset, dst)

    assert dst.read_bytes() == BINARY
    [(method, url, headers)] = fake.calls
    assert (method, url) == ("GET", f"{ASSETS_URL}/{asset['id']}")
    assert url == asset["url"]
    assert headers["Accept"] == "application/octet-stream"
    assert headers["Authorization"] == f"Bearer {TOKEN}"
    assert headers["X-GitHub-Api-Version"] == "2022-11-28"
    assert headers["User-Agent"] == USER_AGENT


def test_download_never_goes_through_the_name_of_the_asset(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    asset = gh.upload(release, "state.db.zst", write(tmp_path, "src", BINARY))
    # A trap: a name-based URL that the kit must not request. The fake serves none.
    trapped = {**asset, "browser_download_url": "https://github.com/trap/is/set"}
    fake.calls.clear()

    gh.download(trapped, tmp_path / "out")

    assert [url for _, url, _ in fake.calls] == [asset["url"]]
    assert (tmp_path / "out").read_bytes() == BINARY


@pytest.mark.parametrize("listed", ["before", "after"])
def test_a_download_after_a_rename_returns_the_bytes_of_its_own_asset(
    gh, tmp_path, listed
):
    release = gh.create_release(TAG)
    live = gh.upload(release, "state.db.zst", write(tmp_path, "live", b"live bytes"))
    nxt = gh.upload(release, "swap-next-state.db.zst", write(tmp_path, "nxt", b"next"))
    # What a publish does: the live asset moves aside and the next one takes its name.
    gh.rename(live, "swap-prev-state.db.zst")
    gh.rename(nxt, "state.db.zst")
    if listed == "after":
        by_id = {asset["id"]: asset for asset in gh.assets(release)}
        live, nxt = by_id[live["id"]], by_id[nxt["id"]]

    # Listed before the renames, `live` still says that its asset is called
    # state.db.zst, a name that another asset holds now. Its bytes are its own all the
    # same, and so are those of `nxt`.
    gh.download(live, tmp_path / "from-live")
    gh.download(nxt, tmp_path / "from-next")

    assert (tmp_path / "from-live").read_bytes() == b"live bytes"
    assert (tmp_path / "from-next").read_bytes() == b"next"


def test_download_of_a_missing_asset_is_not_found_and_writes_nothing(gh, tmp_path):
    release = gh.create_release(TAG)
    asset = gh.upload(release, "a", write(tmp_path, "src", b"x"))
    gh.delete(asset)
    dst = tmp_path / "out"

    with pytest.raises(NotFound):
        gh.download(asset, dst)

    assert not dst.exists()


def test_a_failed_download_writes_nothing(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    asset = gh.upload(release, "a", write(tmp_path, "src", b"x"))
    fake.fail("GET", f"/releases/assets/{asset['id']}", 502, times=6)
    dst = tmp_path / "out"

    with pytest.raises(Transient):
        gh.download(asset, dst)

    assert not dst.exists()


def test_a_download_is_retried_from_the_api_url(gh, fake, clock, tmp_path):
    release = gh.create_release(TAG)
    asset = gh.upload(release, "a", write(tmp_path, "src", b"x"))
    fake.fail("GET", f"/releases/assets/{asset['id']}", 502, times=2)
    fake.calls.clear()

    gh.download(asset, tmp_path / "out")

    assert (tmp_path / "out").read_bytes() == b"x"
    assert clock.sleeps == [2, 4]
    assert [url for _, url, _ in fake.calls] == [asset["url"]] * 3


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


def patch_open(monkeypatch, urlopen) -> None:
    """Make every opener's `open` call `urlopen(request, timeout)`. The urlopen function
    hands its request to an opener, so this stands in for it as well, and no test
    touches the network."""

    def open_(self, request, data=None, timeout=None):
        return urlopen(request, timeout)

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", open_)


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

    patch_open(monkeypatch, urlopen)

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

    patch_open(monkeypatch, urlopen)

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

    patch_open(monkeypatch, urlopen)

    with pytest.raises(OSError):
        UrllibTransport().request("GET", "https://example.test/x", {}, None)


def test_github_defaults_to_the_urllib_transport(monkeypatch, clock):
    seen = []

    def urlopen(request, timeout):
        seen.append(request)
        return Reply(200, headers_of(), b'{"id": 7, "tag_name": "example-current"}')

    patch_open(monkeypatch, urlopen)

    release = GitHub(REPO, TOKEN, sleep=clock.sleep, clock=clock).release_by_tag(TAG)

    assert release["id"] == 7
    [request] = seen
    assert request.full_url == f"{RELEASES_URL}/tags/{TAG}"
    assert request.get_header("Authorization") == f"Bearer {TOKEN}"


# A socket timeout bounds a whole sendall(). So a body that goes out in one send gives a
# whole upload one timeout to finish in, and not just each stall. The body therefore
# goes to urllib as a file, which http.client sends in blocks, and urllib then needs the
# Content-Length stated: without it, it falls back to chunked transfer encoding.


def stub_urlopen(monkeypatch) -> list:
    """Replace urlopen with a stub that records each Request and replies 201."""
    requests = []

    def urlopen(request, timeout):
        requests.append(request)
        return Reply(201, headers_of(), b"{}")

    patch_open(monkeypatch, urlopen)
    return requests


def test_urllib_transport_hands_a_body_to_urllib_as_a_file_with_a_content_length(
    monkeypatch,
):
    body = BINARY * 100
    given = {"Content-Type": "application/octet-stream", "Accept": "a/b"}
    requests = stub_urlopen(monkeypatch)

    UrllibTransport().request("POST", "https://example.test/up", given, body)

    [request] = requests
    assert not isinstance(request.data, bytes)
    assert request.data.read() == body, "a file, and its bytes are the original"
    assert request.get_header("Content-length") == str(len(body))
    assert not request.has_header("Transfer-encoding")
    assert request.get_header("Content-type") == "application/octet-stream"
    assert request.get_header("Accept") == "a/b"
    assert given == {"Content-Type": "application/octet-stream", "Accept": "a/b"}


def test_urllib_transport_gives_every_call_its_own_stream_of_the_whole_body(
    monkeypatch,
):
    body = BINARY * 100
    requests = stub_urlopen(monkeypatch)
    transport = UrllibTransport()

    for _ in range(3):  # what the retries do with one body
        transport.request("POST", "https://example.test/up", {}, body)

    streams = [request.data for request in requests]
    assert len({id(stream) for stream in streams}) == 3
    assert [stream.read() for stream in streams] == [body] * 3


@pytest.mark.parametrize("body", [None, b""], ids=["no body", "empty body"])
def test_urllib_transport_leaves_a_missing_or_empty_body_alone(monkeypatch, body):
    requests = stub_urlopen(monkeypatch)

    UrllibTransport().request("POST", "https://example.test/x", {}, body)

    [request] = requests
    assert request.data == body, "bytes or None, not a file"
    assert not request.has_header("Content-length"), "urllib states 0 itself"


class FakeSocket:
    """A socket that records what it is sent and replays a canned reply."""

    def __init__(self, reply: bytes) -> None:
        self.reply = reply
        self.sent: list[bytes] = []

    def sendall(self, data) -> None:
        self.sent.append(bytes(data))

    def makefile(self, mode: str = "rb", buffering=None):
        return io.BytesIO(self.reply)

    def close(self) -> None:
        pass


def test_urllib_transport_sends_a_body_in_blocks_and_not_in_one_send(monkeypatch):
    # The real urllib and http.client, over a socket that only records what is sent.
    body = BINARY * 4096  # about 1 MiB
    sock = FakeSocket(b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\n{}")
    timeouts = []

    def connect(self):
        timeouts.append(self.timeout)
        self.sock = sock

    monkeypatch.setattr(http.client.HTTPConnection, "connect", connect)
    given = {"Content-Type": "application/octet-stream"}

    for attempt in (1, 2):  # a retry must send the whole body again
        sock.sent.clear()

        reply = UrllibTransport().request(
            "POST", "http://example.test/upload", given, body
        )

        assert reply == (201, {"content-length": "2"}, b"{}"), attempt
        head, _, sent_body = b"".join(sock.sent).partition(b"\r\n\r\n")
        lines = head.decode("latin-1").lower().split("\r\n")
        assert f"content-length: {len(body)}" in lines
        assert not any(line.startswith("transfer-encoding") for line in lines)
        assert sent_body == body
        assert max(len(chunk) for chunk in sock.sent) <= 64 * 1024
        assert len(sock.sent) > 16
    assert timeouts == [60, 60]


# --- A redirect: the token goes only where the request was meant to go ---
#
# The API answers a download with a redirect to a signed URL on a storage host. urllib
# copies every header but the content headers to the request that it follows, so the
# token would reach that host, which has no use for it. These tests build urllib's own
# Request objects and call the handler as urllib does, and touch no network.

STORAGE = "https://release-assets.githubusercontent.com/blob/1/2?sig=abc%3D&se=3"


def asset_request() -> urllib.request.Request:
    """The request of a download: the API URL of an asset, with the headers it sends."""
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/octet-stream",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": USER_AGENT,
    }
    return urllib.request.Request(f"{ASSETS_URL}/7", headers=headers, method="GET")


def redirect(
    request: urllib.request.Request, location: str, code: int = 302
) -> urllib.request.Request:
    """The request that urllib sends next when the reply to `request` is a redirect of
    the given `code` to `location`."""
    handler = _AuthorizationStaysOnHost()
    followed = handler.redirect_request(
        request, io.BytesIO(), code, "Found", http.client.HTTPMessage(), location
    )
    assert followed is not None
    return followed


def header_names(request: urllib.request.Request) -> set[str]:
    return {name.lower() for name in request.headers}


@pytest.mark.parametrize(
    "location",
    [
        STORAGE,
        "https://api.github.com.example.test/repos/x",
        "https://github.com/py-observatory/data",
        "https://api.github.com:8443/repos/x",
        "http://api.github.com/repos/x",
        "https://api.github.com@example.test/repos/x",
    ],
    ids=[
        "storage host",
        "a host that begins with its name",
        "its parent domain",
        "another port",
        "plain http",
        "a name that is only a user",
    ],
)
def test_a_redirect_to_another_host_drops_the_authorization_header(location):
    followed = redirect(asset_request(), location)

    assert followed.full_url == location
    assert followed.get_method() == "GET"
    assert "authorization" not in header_names(followed)
    # The other headers go on: the new host is told what is asked for, and by whom.
    assert followed.get_header("Accept") == "application/octet-stream"
    assert followed.get_header("X-github-api-version") == "2022-11-28"
    assert followed.get_header("User-agent") == USER_AGENT


@pytest.mark.parametrize(
    "location",
    [
        f"{RELEASES_URL}/assets/7?again=1",
        "https://api.github.com/repositories/9/releases/assets/7",
        "https://API.GitHub.com/repos/x",
    ],
    ids=["another query", "another path", "host in capitals"],
)
def test_a_redirect_within_the_host_keeps_the_authorization_header(location):
    followed = redirect(asset_request(), location)

    assert followed.full_url == location
    assert followed.get_header("Authorization") == f"Bearer {TOKEN}"
    assert followed.get_header("Accept") == "application/octet-stream"
    assert followed.get_header("User-agent") == USER_AGENT


@pytest.mark.parametrize(
    "spelling", ["Authorization", "authorization", "AUTHORIZATION", "aUtHoRiZaTiOn"]
)
def test_the_authorization_header_is_dropped_in_any_spelling(monkeypatch, spelling):
    # urllib's Request capitalizes each header name that it is given, so the name
    # reaches the handler as "Authorization" whatever it was. The handler does not rely
    # on that, and here Request keeps the names as they were written.
    monkeypatch.setattr(
        urllib.request.Request,
        "add_header",
        lambda self, key, val: self.headers.update({key: val}),
    )
    headers = {"Accept": "a/b", spelling: f"Bearer {TOKEN}"}
    request = urllib.request.Request(f"{ASSETS_URL}/7", headers=headers)
    assert spelling in request.headers

    followed = redirect(request, STORAGE)

    assert spelling in request.headers, "the request that was redirected is as it was"
    assert "authorization" not in header_names(followed)
    assert followed.get_header("Accept") == "a/b"


def test_a_redirect_leaves_the_request_that_was_redirected_as_it_was():
    request = asset_request()

    redirect(request, STORAGE)

    assert request.get_header("Authorization") == f"Bearer {TOKEN}"


def test_the_header_does_not_come_back_with_a_redirect_to_the_first_host():
    elsewhere = redirect(asset_request(), STORAGE)

    back = redirect(elsewhere, f"{ASSETS_URL}/7")

    assert "authorization" not in header_names(back)


def sent_to(sock: FakeSocket) -> tuple[str, dict[str, str]]:
    """The request line, and the headers (names in lowercase), that `sock` was sent."""
    head = b"".join(sock.sent).partition(b"\r\n\r\n")[0].decode("latin-1")
    line, *fields = head.split("\r\n")
    headers = {}
    for field in fields:
        name, _, value = field.partition(": ")
        headers[name.lower()] = value
    return line, headers


def test_a_download_follows_the_redirect_without_the_token_leaving_the_host(
    monkeypatch, clock, tmp_path
):
    # The real urllib and http.client, over sockets that only record what is sent. The
    # API redirects, as GitHub does, to a signed URL on another host.
    sockets = {
        "api.test": FakeSocket(
            b"HTTP/1.1 302 Found\r\n"
            b"Location: http://storage.test/blob?sig=abc%3D\r\n"
            b"Content-Length: 0\r\n\r\n"
        ),
        "storage.test": FakeSocket(
            b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%b" % (len(BINARY), BINARY)
        ),
    }

    def connect(self):
        self.sock = sockets[self.host]

    monkeypatch.setattr(http.client.HTTPConnection, "connect", connect)
    asset = {"id": 7, "url": "http://api.test/repos/x/releases/assets/7"}
    gh = GitHub(REPO, TOKEN, sleep=clock.sleep, clock=clock)

    gh.download(asset, tmp_path / "out")

    assert (tmp_path / "out").read_bytes() == BINARY
    line, headers = sent_to(sockets["api.test"])
    assert line == "GET /repos/x/releases/assets/7 HTTP/1.1"
    assert headers["authorization"] == f"Bearer {TOKEN}"
    assert headers["accept"] == "application/octet-stream"
    line, headers = sent_to(sockets["storage.test"])
    assert line == "GET /blob?sig=abc%3D HTTP/1.1"
    assert "authorization" not in headers, "the token never reaches the storage host"
    assert headers["accept"] == "application/octet-stream"
    assert headers["user-agent"] == USER_AGENT


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


def test_fake_serves_an_asset_by_id_as_its_bytes_or_as_its_json(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    asset = gh.upload(release, "a", write(tmp_path, "a", BINARY))
    raw = {"Accept": "application/octet-stream"}

    status, headers, body = fake.request("GET", asset["url"], raw, None)

    assert (status, body) == (200, BINARY)
    assert headers["content-type"] == "application/octet-stream"
    assert headers["content-length"] == str(len(BINARY))
    for given in ({"Accept": "application/vnd.github+json"}, {}):
        status, headers, body = fake.request("GET", asset["url"], given, None)
        assert status == 200, given
        assert headers["content-type"].startswith("application/json"), given
        assert json.loads(body) == asset, given


@pytest.mark.parametrize("name", ["Accept", "accept", "ACCEPT"])
def test_fake_reads_the_accept_header_in_any_case(gh, fake, tmp_path, name):
    release = gh.create_release(TAG)
    asset = gh.upload(release, "a", write(tmp_path, "a", BINARY))

    _, _, body = fake.request(
        "GET", asset["url"], {name: "application/octet-stream"}, None
    )

    assert body == BINARY


def test_fake_answers_404_for_an_asset_id_that_no_asset_has(gh, fake, tmp_path):
    release = gh.create_release(TAG)
    gone = gh.upload(release, "a", write(tmp_path, "a", b"x"))
    gh.delete(gone)

    for accept in ("application/octet-stream", "application/vnd.github+json"):
        for url in (gone["url"], f"{ASSETS_URL}/999999"):
            status, _, _ = fake.request("GET", url, {"Accept": accept}, None)
            assert status == 404, (accept, url)


@pytest.mark.parametrize(
    "url",
    [
        "https://api.github.com/rate_limit",
        "https://api.github.com/repos/someone/else/releases/tags/x",
        "https://example.test/repos/py-observatory/data/releases/tags/x",
        f"https://github.com/{REPO}/releases/download/{TAG}/a",
    ],
    ids=["unknown path", "other repository", "other host", "download by name"],
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
