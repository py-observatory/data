"""A client for the GitHub releases REST API.

Every failure is classified by HTTP status alone, never by message text: a transient
502 once read as "release not found" let a run publish over good data. A 404 is
NotFound. A status that can pass on its own, and any network failure, is Transient and
is retried with backoff. Every other failure is Fatal. No method turns an error into an
empty result.
"""

import io
import json
import re
import time
import urllib.error
import urllib.request
from collections import deque
from collections.abc import Callable
from email.message import Message
from http.client import HTTPException, HTTPMessage
from pathlib import Path
from typing import IO, Protocol
from urllib.parse import quote, urlsplit

from obskit.http import USER_AGENT

_API = "https://api.github.com"
_UPLOADS = "https://uploads.github.com"
_API_VERSION = "2022-11-28"
_PER_PAGE = 100
# urllib's limit, in seconds, on each blocking socket call. A body goes out in blocks
# (see UrllibTransport.request), so this bounds a stall and not a whole upload.
_TIMEOUT = 60

# The wait before each retry of a Transient failure, so six attempts in all. A
# `retry-after` header replaces the wait, up to _MAX_RETRY_AFTER seconds.
_BACKOFF = (2, 4, 8, 16, 32)
_MAX_RETRY_AFTER = 300

# GitHub limits the writes of one token. Stay at or below both limits, each given as
# (calls, seconds): at most 80 calls in any 60 s, and 500 in any 3,600 s.
_WRITE_METHODS = frozenset({"POST", "PATCH", "DELETE"})
_WRITE_LIMITS = ((80, 60), (500, 3600))

# `retry-after` holds a count of seconds. GitHub does not send the HTTP-date form.
_SECONDS = re.compile(r"[0-9]+")


class GitHubError(Exception):
    """A GitHub request failed.

    `status` is the HTTP status that caused it. It is None when there was no HTTP
    reply (the network failed) and when the reply was not the JSON it should be.
    """

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class NotFound(GitHubError):
    """HTTP 404: GitHub has no such release or asset. It is never retried."""


class Transient(GitHubError):
    """A failure that may pass on its own: HTTP 408, 429 or 5xx, a 403 that is a rate
    limit, or a network error. The client raises it only after its retries ran out.

    `retry_after` is the wait in seconds that the server asked for (at most 300), or
    None.
    """

    def __init__(
        self, message: str, status: int | None = None, retry_after: int | None = None
    ) -> None:
        super().__init__(message, status)
        self.retry_after = retry_after


class Fatal(GitHubError):
    """Any other failure. The same request will fail again, so it is never retried."""


class Transport(Protocol):
    """Makes one HTTP request. A reply with any status is returned, with lowercase
    header names. A network failure raises OSError."""

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]: ...


def _lowercase(headers: Message) -> dict[str, str]:
    return {name.lower(): value for name, value in headers.items()}


def _origin(url: str) -> tuple[str, str]:
    """The scheme and the authority of `url` (its host, its port if it has one, and any
    user info), as written and in lowercase. Two URLs with the same origin go to the
    same host, and what is meant for one may be sent to the other."""
    parts = urlsplit(url)
    return parts.scheme.lower(), parts.netloc.lower()


class _AuthorizationStaysOnHost(urllib.request.HTTPRedirectHandler):
    """Follows a redirect as urllib does, and keeps the `Authorization` header only when
    the redirect stays on its host.

    urllib copies every header but the content headers to the request that it follows.
    GitHub answers a download with a redirect to a signed URL on a storage host, and the
    token must not go there: the URL is signed already, the token is not that host's to
    have, and a storage host may refuse a signed URL that comes with an Authorization
    header besides. The header is dropped, in whatever case it is spelled, unless the
    new URL has the same origin (scheme, host and port) as the request that was
    redirected. Once dropped it is not put back. Every other header is kept.
    """

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> urllib.request.Request | None:
        followed = super().redirect_request(req, fp, code, msg, headers, newurl)
        if followed is not None and _origin(followed.full_url) != _origin(req.full_url):
            for name in [n for n in followed.headers if n.lower() == "authorization"]:
                del followed.headers[name]
        return followed


class UrllibTransport:
    """The transport that talks to the network, with urllib. It returns the status of an
    HTTP error and does not raise, and it turns every other failure into OSError.

    It follows redirects, and the `Authorization` header goes only to the host that the
    request was made to: a redirect to another host does not carry it (see
    _AuthorizationStaysOnHost).

    A body goes out in blocks, so the timeout bounds a stall and not a whole upload."""

    def __init__(self) -> None:
        self._opener = urllib.request.build_opener(_AuthorizationStaysOnHost)

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        data: bytes | io.BytesIO | None = body
        if body:
            # http.client sends a bytes body in a single `sendall`, and a socket timeout
            # bounds the whole call: an upload would have to finish within _TIMEOUT. It
            # sends a file in blocks of 8 KiB, so each block has its own timeout. urllib
            # cannot size a file and would fall back to chunked encoding, so the length
            # is stated. The stream is new on every call, because each retry sends the
            # whole body again. BytesIO shares the bytes and does not copy them.
            data = io.BytesIO(body)
            headers = {**headers, "Content-Length": str(len(body))}
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            try:
                with self._opener.open(req, timeout=_TIMEOUT) as reply:
                    return reply.status, _lowercase(reply.headers), reply.read()
            except urllib.error.HTTPError as err:
                with err:
                    return err.code, _lowercase(err.headers), err.read()
        except HTTPException as exc:
            # A garbled or cut-short reply is no OSError by itself, and must be one.
            raise OSError(f"{method} {url} failed: {exc!r}") from exc


def _retry_after(headers: dict[str, str]) -> int | None:
    """The wait in seconds that a `retry-after` header asks for, at most 5 minutes, or
    None when there is no usable header."""
    value = headers.get("retry-after", "").strip()
    return min(int(value), _MAX_RETRY_AFTER) if _SECONDS.fullmatch(value) else None


def _error(
    method: str, url: str, status: int, headers: dict[str, str], body: bytes
) -> GitHubError:
    """The error for a reply that is not a success, chosen by status and headers only.
    The text of the reply goes into the message and decides nothing."""
    message = f"{method} {url} returned HTTP {status}"
    text = body[:200].decode("utf-8", "replace").strip()
    if text:
        message += f": {text}"
    if status == 404:
        return NotFound(message, status)
    rate_limited = status == 403 and (
        headers.get("x-ratelimit-remaining", "").strip() == "0"
        or "retry-after" in headers
    )
    if status in (408, 429) or 500 <= status <= 599 or rate_limited:
        return Transient(message, status, _retry_after(headers))
    return Fatal(message, status)


def _decode[T: (dict, list)](data: bytes, kind: type[T], where: str) -> T:
    """The JSON in the reply `data`: an object (dict) or an array (list), as `kind`."""
    try:
        value = json.loads(data)
    except ValueError as exc:
        raise Fatal(f"{where} replied with something that is not JSON: {exc}") from exc
    if not isinstance(value, kind):
        shape = "an object" if kind is dict else "an array"
        raise Fatal(f"{where} replied with JSON that is not {shape}")
    return value


class GitHub:
    """The releases of one repository, through the REST API.

    A failed call raises a GitHubError: NotFound for a 404, Transient when a transient
    failure outlasted the retries, and Fatal for anything else. Writes (POST, PATCH
    and DELETE) are paced to GitHub's limits, and `sleep` and `clock` are what the
    retries and the pacing use, so a test can give them stand-ins.
    """

    def __init__(
        self,
        repo: str,
        token: str,
        *,
        transport: Transport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.repo = repo
        self._api = f"{_API}/repos/{repo}"
        self._uploads = f"{_UPLOADS}/repos/{repo}"
        self._token = token
        self._transport = transport if transport is not None else UrllibTransport()
        self._sleep = sleep
        self._clock = clock
        # When each of the latest writes began, as far back as the longest limit looks.
        self._writes: deque[float] = deque(maxlen=max(n for n, _ in _WRITE_LIMITS))

    def release_by_tag(self, tag: str) -> dict:
        """The release of `tag`. NotFound means GitHub answered 404, and only that."""
        url = f"{self._api}/releases/tags/{quote(tag, safe='')}"
        return self._json("GET", url, dict)

    def create_release(
        self, tag: str, *, prerelease: bool = True, latest: bool = False
    ) -> dict:
        """Create the release of `tag`. It is a prerelease and not Latest unless asked.
        `make_latest` is always sent, because GitHub's default is to make it Latest."""
        fields = {
            "tag_name": tag,
            "prerelease": prerelease,
            "make_latest": "true" if latest else "false",
        }
        body = json.dumps(fields).encode()
        return self._json(
            "POST", f"{self._api}/releases", dict, body, "application/json"
        )

    def assets(self, release: dict) -> list[dict]:
        """Every asset of `release`, read a page of 100 at a time."""
        url = f"{self._api}/releases/{release['id']}/assets"
        found: list[dict] = []
        page = 1
        while True:
            batch = self._json("GET", f"{url}?per_page={_PER_PAGE}&page={page}", list)
            found += batch
            if len(batch) < _PER_PAGE:
                return found
            page += 1

    def upload(self, release: dict, name: str, path: Path) -> dict:
        """Upload the file at `path` as the asset `name` of `release`. GitHub refuses
        (HTTP 422, so Fatal) a name that the release already holds."""
        assets = f"{self._uploads}/releases/{release['id']}/assets"
        url = f"{assets}?name={quote(name, safe='')}"
        return self._json(
            "POST", url, dict, path.read_bytes(), "application/octet-stream"
        )

    def rename(self, asset: dict, name: str) -> dict:
        """Give `asset` the name `name`. GitHub refuses (HTTP 422, so Fatal) a name that
        another asset of its release already holds."""
        url = f"{self._api}/releases/assets/{asset['id']}"
        body = json.dumps({"name": name}).encode()
        return self._json("PATCH", url, dict, body, "application/json")

    def delete(self, asset: dict) -> None:
        """Delete `asset`. An asset that is already gone is NotFound."""
        url = f"{self._api}/releases/assets/{asset['id']}"
        self._send("DELETE", url, self._headers())

    def download(self, asset: dict, dst: Path) -> None:
        """Download `asset` into `dst`, which is written only once the whole body has
        arrived.

        The asset is asked for by its id: a GET of its API URL, `asset["url"]`, with
        `Accept: application/octet-stream` and the headers of every API request, the
        token included. GitHub redirects that request to a signed URL for the bytes of
        exactly that asset, and the transport follows the redirect without the token
        (see UrllibTransport). So the bytes are those of the asset that was listed,
        whatever name it holds by now.

        The `browser_download_url` is never used. It asks for a name, and a publish
        moves names from one asset to another by renaming: for a moment after a rename,
        GitHub can still resolve the name to the asset that held it before.
        """
        headers = {**self._headers(), "Accept": "application/octet-stream"}
        dst.write_bytes(self._send("GET", asset["url"], headers))

    def _headers(self, content_type: str | None = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": _API_VERSION,
            "User-Agent": USER_AGENT,
        }
        if content_type is not None:
            headers["Content-Type"] = content_type
        return headers

    def _json[T: (dict, list)](
        self,
        method: str,
        url: str,
        kind: type[T],
        body: bytes | None = None,
        content_type: str | None = None,
    ) -> T:
        """Send a request to the API and return the JSON of its reply."""
        reply = self._send(method, url, self._headers(content_type), body)
        return _decode(reply, kind, f"{method} {url}")

    def _send(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None = None
    ) -> bytes:
        """Send a request and return the body of its 2xx reply.

        A Transient failure is retried after each wait in _BACKOFF, or after the wait
        that the server asked for, and is raised when the last attempt fails as well.
        Any other failure is raised at once.
        """
        for backoff in _BACKOFF:
            try:
                return self._attempt(method, url, headers, body)
            except Transient as exc:
                self._sleep(backoff if exc.retry_after is None else exc.retry_after)
        return self._attempt(method, url, headers, body)

    def _attempt(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> bytes:
        """One request: the body of a 2xx reply, or else the error it amounts to."""
        if method in _WRITE_METHODS:
            self._pace_write()
        try:
            status, reply_headers, data = self._transport.request(
                method, url, headers, body
            )
        except OSError as exc:
            raise Transient(f"{method} {url} failed: {exc}") from exc
        if 200 <= status < 300:
            return data
        raise _error(method, url, status, reply_headers, data)

    def _pace_write(self) -> None:
        """Wait, if need be, until a write keeps both limits, and then count it. Every
        attempt counts, a retry included. The count is of times on the injected clock,
        so a stand-in `sleep` that does not move the clock cannot make this loop."""
        now = self._clock()
        ready = now
        for calls, seconds in _WRITE_LIMITS:
            if len(self._writes) >= calls:
                ready = max(ready, self._writes[-calls] + seconds)
        if ready > now:
            self._sleep(ready - now)
        self._writes.append(max(ready, self._clock()))
