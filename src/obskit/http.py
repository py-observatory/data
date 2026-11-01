"""Upstream HTTP: how a stream reads PyPI, and any other source that it does not own.

Requests go out one at a time. A request starts at least `interval` seconds after the
last request to the same host, a retry included, and carries the project's User-Agent
(its repository, never an email address) and an ask for gzip. A 429, a 5xx and a
network failure are retried with a wait, and then fail. A request that fails is an
UpstreamError, classified by HTTP status and never by message text. A 200, 304, 404 and
410 are not failures: they are returned, for the caller to read.
"""

import gzip
import re
import time
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

if TYPE_CHECKING:
    # github.py imports USER_AGENT from this module, so this module may not import
    # github.py while it loads: each would wait for the other. Transport is named only
    # in an annotation, and UrllibTransport is imported when an Http needs one. The
    # annotation is quoted because Python 3.14 evaluates annotations when they are
    # read, and inspect.signature(Http) would raise NameError for a bare name that
    # exists only for the type checker.
    from obskit.github import Transport

# No email address: the project's public repository is how to reach its owner.
USER_AGENT = "py-observatory/0.1 (+https://github.com/py-observatory/data)"

# The statuses that are replies and not failures: the page, "not modified" for an ETag
# that still matches, and a page that is gone (404 for now, 410 for good). What each
# means is for the caller to say.
_RETURNED = frozenset({200, 304, 404, 410})

# The wait before each retry of a failure that may pass, so five attempts in all. A
# `retry-after` header replaces the wait, up to _MAX_RETRY_AFTER seconds.
_BACKOFF = (2, 4, 8, 16)
_MAX_RETRY_AFTER = 300

# `retry-after` holds a count of seconds. The HTTP-date form counts as no header.
_SECONDS = re.compile(r"[0-9]+")


@dataclass(frozen=True)
class HttpResponse:
    """A reply that is not a failure. Header names are lowercase and the values are the
    server's, so `content-encoding` still says gzip when `body` has been unzipped."""

    status: int
    headers: dict[str, str]
    body: bytes


class UpstreamError(RuntimeError):
    """An upstream request failed: its status is one that is never retried, or the
    retries ran out.

    `url` is the URL that was requested. `status` is the HTTP status that caused the
    failure. It is None when there was no usable reply: the network failed, or the body
    was damaged on its way. `retry_after` is the wait in seconds that the reply asked
    for (at most 300), or None.
    """

    def __init__(
        self,
        message: str,
        url: str,
        status: int | None = None,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status = status
        self.retry_after = retry_after


def _gunzipped(body: bytes, headers: dict[str, str]) -> bytes:
    """`body` unzipped when the reply says that it is gzip, and as it is otherwise.

    A body that does not unzip is an OSError, as a dropped connection is. gzip fails with
    BadGzipFile (an OSError already), with EOFError for a stream that stops short and
    with zlib.error for a damaged one, and each means that the body did not arrive
    intact.
    """
    if headers.get("content-encoding", "").strip().lower() not in ("gzip", "x-gzip"):
        return body
    try:
        return gzip.decompress(body)
    except (EOFError, zlib.error) as exc:
        raise OSError(f"the gzip body does not decompress: {exc}") from exc


def _retry_after(headers: dict[str, str]) -> int | None:
    """The wait in seconds that a `retry-after` header asks for, at most 5 minutes, or
    None when there is no usable header."""
    value = headers.get("retry-after", "").strip()
    return min(int(value), _MAX_RETRY_AFTER) if _SECONDS.fullmatch(value) else None


def _may_pass(error: UpstreamError) -> bool:
    """Whether a failure can pass on its own, judged by its status alone: no reply at
    all, a 429 or a 5xx."""
    status = error.status
    return status is None or status == 429 or 500 <= status <= 599


class Http:
    """Reads URLs one at a time, politely.

    `get` raises an UpstreamError when a request fails. `sleep` and `clock` are what the
    retries and the pacing use, so a test can give them stand-ins. `requests` counts the
    requests that have gone out, the retries among them.
    """

    def __init__(
        self,
        *,
        interval: float = 0.25,
        transport: "Transport | None" = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if transport is None:
            from obskit.github import UrllibTransport  # not at the top: see Transport

            transport = UrllibTransport()
        self.requests = 0
        self._interval = interval
        self._transport = transport
        self._sleep = sleep
        self._clock = clock
        # When the latest request to each host began, on the injected clock.
        self._started: dict[str, float] = {}

    def get(
        self, url: str, *, accept: str | None = None, etag: str | None = None
    ) -> HttpResponse:
        """GET `url` and return the reply, with its body unzipped if it came gzipped.

        `accept` and `etag` go out as `Accept` and `If-None-Match` when they are given.
        `etag` is the ETag of a copy that the caller holds, and a 304 says that the
        copy is current.

        A 200, 304, 404 or 410 is returned and never raised. A 429, a 5xx and an OSError
        (a network failure, or a gzip body that does not unzip) are retried 4 times,
        after the wait that the reply asked for in `retry-after` (at most 300 seconds)
        or else after 2, 4, 8 and 16 seconds, and then raise. Any other status raises at
        once. A request that fails raises an UpstreamError, and only that.
        """
        headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
        if accept is not None:
            headers["Accept"] = accept
        if etag is not None:
            headers["If-None-Match"] = etag
        delay = 0
        for backoff in _BACKOFF:
            try:
                return self._attempt(url, headers, delay)
            except UpstreamError as exc:
                if not _may_pass(exc):
                    raise
                delay = backoff if exc.retry_after is None else exc.retry_after
        return self._attempt(url, headers, delay)

    def _attempt(self, url: str, headers: dict[str, str], delay: float) -> HttpResponse:
        """One request, begun `delay` seconds from now or later if the pacing needs it:
        the reply when it is one of _RETURNED, or else the error that it amounts to."""
        self._pace(urlsplit(url).hostname or "", delay)
        self.requests += 1
        try:
            status, reply_headers, body = self._transport.request(
                "GET", url, headers, None
            )
            if status in _RETURNED:
                return HttpResponse(
                    status, reply_headers, _gunzipped(body, reply_headers)
                )
        except OSError as exc:
            raise UpstreamError(f"GET {url} failed: {exc}", url) from exc
        message = f"GET {url} returned HTTP {status}"
        raise UpstreamError(message, url, status, _retry_after(reply_headers))

    def _pace(self, host: str, delay: float) -> None:
        """Wait until a request to `host` may begin, and note when it does. It may begin
        `delay` seconds from now, and `interval` seconds after the last request to
        `host` began, whichever is later. The two waits run together, as one sleep.

        That is a single sleep and not a loop on the clock, so a `sleep` that does not
        move the clock cannot make this wait for ever."""
        now = self._clock()
        wait = delay
        if host in self._started:
            wait = max(wait, self._interval - (now - self._started[host]))
        if wait > 0:
            self._sleep(wait)
        self._started[host] = self._clock()
