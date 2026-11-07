"""Fakes for tests, and texts for tests to share. FakeGitHub is the GitHub releases API,
FakeHttp is a set of web pages that obskit.http reads, and FakeGoogle is the Google
Cloud APIs, all held in memory. The
lists at the end are the texts that the tests of obskit.emails share with the tests of
each stream that uses it."""

import base64
import itertools
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from hashlib import sha256
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, quote, unquote, urlsplit

_Reply = tuple[int, dict[str, str], bytes]

_MAKE_LATEST = ("true", "false", "legacy")
_MAX_PER_PAGE = 100


def _reply(status: int, payload: object = None) -> _Reply:
    """A reply whose body is `payload` as JSON, or has no body for None."""
    if payload is None:
        return status, {}, b""
    headers = {"content-type": "application/json; charset=utf-8"}
    return status, headers, json.dumps(payload).encode()


def _problem(status: int, message: str, *errors: dict[str, str]) -> _Reply:
    """An error reply shaped like GitHub's: a message and the errors behind it."""
    payload: dict[str, object] = {"message": message}
    if errors:
        payload["errors"] = list(errors)
    return _reply(status, payload)


def _already_exists(resource: str, name: str) -> _Reply:
    error = {"resource": resource, "code": "already_exists", "field": name}
    return _problem(422, "Validation Failed", error)


def _json_object(body: bytes | None) -> dict | None:
    """The JSON object in a request body, or None when the body is anything else."""
    try:
        value = json.loads(body or b"")
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


@dataclass
class _Asset:
    id: int
    name: str
    data: bytes

    @property
    def digest(self) -> str:
        return "sha256:" + sha256(self.data).hexdigest()


@dataclass
class _Release:
    id: int
    tag: str
    prerelease: bool
    make_latest: str
    assets: list[_Asset] = field(default_factory=list)


@dataclass
class _Failure:
    method: str
    needle: str
    status: int
    headers: dict[str, str]
    remaining: int  # requests that this failure will still answer


@dataclass
class _Request:
    """A request that matched a route: what the route's pattern captured from the
    path, the query string, the body, and the headers as the client sent them."""

    found: re.Match[str]
    query: dict[str, list[str]]
    body: bytes | None
    headers: dict[str, str]


def _wants_bytes(headers: dict[str, str]) -> bool:
    """Whether a request asks for an asset's bytes, which GitHub gives for an `Accept`
    of `application/octet-stream` and for nothing else."""
    accept = {name.lower(): value for name, value in headers.items()}.get("accept", "")
    return accept.strip().lower() == "application/octet-stream"


class FakeGitHub:
    """The part of the GitHub releases REST API that `obskit.github.GitHub` uses, held
    in memory, as a Transport.

    It answers as GitHub does where the kit depends on it:

    - A tag with no release is a 404. A second release for a tag is a 422.
    - Assets are listed a page at a time (`per_page`, `page`). Each has an `id`, a
      `name`, a `size`, a `digest` ("sha256:<hex>"), a `url` (its API URL, which holds
      its id) and a `browser_download_url`.
    - An asset is read by its id, at its `url`. With `Accept: application/octet-stream`
      the reply is its bytes, and otherwise its JSON. An id that no asset holds, that
      of a deleted asset included, is a 404.
    - The `browser_download_url` is listed, as GitHub lists it, and is not served: the
      kit must never download through a name. A rename moves a name from one asset to
      another, and GitHub can go on resolving the name to the asset that held it a
      moment before. A request for that URL raises NotImplementedError, as any route
      that the fake lacks does.
    - An upload, or a rename, to a name that the release already holds is a 422.
    - The release objects it returns hold no `assets` list, so that code must read the
      assets endpoint. `release()` is how a test looks inside.

    A request that it has no route for raises NotImplementedError. It never answers
    that with a 404, because a 404 is what the kit reads as "no release".
    """

    def __init__(self, repo: str = "py-observatory/data") -> None:
        self.repo = repo
        # (method, url, headers) of every request, in order, failed ones included.
        self.calls: list[tuple[str, str, dict[str, str]]] = []
        self._releases: dict[str, _Release] = {}
        self._failures: list[_Failure] = []
        self._ids = itertools.count(1)  # one count for releases and assets alike

        api = f"/repos/{re.escape(repo)}/releases"
        collection = re.compile(api)
        by_tag = re.compile(rf"{api}/tags/(?P<tag>.+)")
        assets = re.compile(rf"{api}/(?P<release>[0-9]+)/assets")
        asset = re.compile(rf"{api}/assets/(?P<asset>[0-9]+)")
        self._asset_path = asset
        # Each route: the method, the host, a pattern for the whole path, and a handler.
        self._routes: list[
            tuple[str, str, re.Pattern[str], Callable[[_Request], _Reply]]
        ] = [
            ("GET", "api.github.com", by_tag, self._get),
            ("POST", "api.github.com", collection, self._create),
            ("GET", "api.github.com", assets, self._list),
            ("POST", "uploads.github.com", assets, self._upload),
            ("GET", "api.github.com", asset, self._read),
            ("PATCH", "api.github.com", asset, self._rename),
            ("DELETE", "api.github.com", asset, self._delete),
        ]

    def fail(
        self,
        method: str,
        path_contains: str,
        status: int,
        *,
        times: int = 1,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Answer the next `times` requests that use `method` and whose URL contains
        `path_contains` with `status` and `headers`, without carrying them out. The
        header names are lowercased, as a Transport gives them.

        The URL of a download, a rename or a delete holds only the id of the asset. To
        aim at one asset by its name, match `name=<name>`: such a request also matches
        as if its URL ended `?name=<the asset's name at that moment>`, as an upload's
        URL does.
        `fail("PATCH", "name=swap-next-state.db.zst", 422)` fails the rename of that
        one asset, and may be set before the asset exists.
        """
        if times < 1:
            raise ValueError(f"times must be at least 1, not {times}")
        lowered = {name.lower(): value for name, value in (headers or {}).items()}
        self._failures.append(_Failure(method, path_contains, status, lowered, times))

    def release(self, tag: str) -> dict | None:
        """The release of `tag` as a test looks at it, or None. It is the object that
        the API returns plus `assets`, all of them, and `make_latest`, as the creating
        request sent it."""
        release = self._releases.get(tag)
        if release is None:
            return None
        return {
            **self._release_json(release),
            "make_latest": release.make_latest,
            "assets": [self._asset_json(release, asset) for asset in release.assets],
        }

    def asset_bytes(self, tag: str, name: str) -> bytes:
        """The bytes of the asset `name` in the release of `tag`. Raises KeyError."""
        release = self._releases.get(tag)
        for asset in release.assets if release else ():
            if asset.name == name:
                return asset.data
        raise KeyError(f"no asset {name!r} in a release of {tag!r}")

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> _Reply:
        self.calls.append((method, url, dict(headers)))
        parts = urlsplit(url)
        path = unquote(parts.path)
        injected = self._injected(method, url, path)
        if injected is not None:
            return injected
        for route_method, host, pattern, handler in self._routes:
            found = pattern.fullmatch(path)
            if found and method == route_method and parts.netloc == host:
                query = parse_qs(parts.query)
                return handler(_Request(found, query, body, headers))
        raise NotImplementedError(
            f"FakeGitHub({self.repo}) has no route for {method} {url}"
        )

    def _injected(self, method: str, url: str, path: str) -> _Reply | None:
        """The reply of the first failure from `fail` that matches this request, which
        uses up one of that failure's turns. None when no failure matches."""
        text = url
        by_id = self._asset_path.fullmatch(path)
        hit = self._asset_by_id(int(by_id["asset"])) if by_id else None
        if hit:
            text += f"?name={quote(hit[1].name, safe='')}"
        for failure in self._failures:
            if failure.method == method and failure.needle in text:
                failure.remaining -= 1
                if failure.remaining == 0:
                    self._failures.remove(failure)
                status, headers, body = _problem(
                    failure.status, f"Injected failure: HTTP {failure.status}"
                )
                return status, {**headers, **failure.headers}, body
        return None

    def _release_by_id(self, release_id: int) -> _Release | None:
        return next((r for r in self._releases.values() if r.id == release_id), None)

    def _asset_by_id(self, asset_id: int) -> tuple[_Release, _Asset] | None:
        for release in self._releases.values():
            for asset in release.assets:
                if asset.id == asset_id:
                    return release, asset
        return None

    def _release_json(self, release: _Release) -> dict:
        return {
            "id": release.id,
            "tag_name": release.tag,
            "draft": False,
            "prerelease": release.prerelease,
        }

    def _asset_json(self, release: _Release, asset: _Asset) -> dict:
        named = f"https://github.com/{self.repo}/releases/download/{release.tag}"
        by_id = f"https://api.github.com/repos/{self.repo}/releases/assets/{asset.id}"
        return {
            "id": asset.id,
            "name": asset.name,
            "size": len(asset.data),
            "digest": asset.digest,
            "url": by_id,
            "browser_download_url": f"{named}/{asset.name}",
        }

    def _get(self, req: _Request) -> _Reply:
        release = self._releases.get(req.found["tag"])
        if release is None:
            return _problem(404, "Not Found")
        return _reply(200, self._release_json(release))

    def _create(self, req: _Request) -> _Reply:
        fields = _json_object(req.body)
        if fields is None:
            return _problem(400, "Problems parsing JSON")
        tag = fields.get("tag_name")
        prerelease = fields.get("prerelease", False)
        make_latest = fields.get("make_latest", "true")  # GitHub's default
        valid = (
            isinstance(tag, str)
            and tag != ""
            and isinstance(prerelease, bool)
            and make_latest in _MAKE_LATEST
        )
        if not valid:
            return _problem(422, "Validation Failed")
        if tag in self._releases:
            return _already_exists("Release", "tag_name")
        release = _Release(next(self._ids), tag, prerelease, make_latest)
        self._releases[tag] = release
        return _reply(201, self._release_json(release))

    def _list(self, req: _Request) -> _Reply:
        release = self._release_by_id(int(req.found["release"]))
        if release is None:
            return _problem(404, "Not Found")
        per_page = int(req.query.get("per_page", ["30"])[0])
        per_page = max(1, min(per_page, _MAX_PER_PAGE))
        page = max(1, int(req.query.get("page", ["1"])[0]))
        start = (page - 1) * per_page
        listed = release.assets[start : start + per_page]
        return _reply(200, [self._asset_json(release, asset) for asset in listed])

    def _upload(self, req: _Request) -> _Reply:
        release = self._release_by_id(int(req.found["release"]))
        if release is None:
            return _problem(404, "Not Found")
        name = req.query.get("name", [""])[0]
        if not name:
            return _problem(422, "Validation Failed")
        if any(asset.name == name for asset in release.assets):
            return _already_exists("ReleaseAsset", "name")
        asset = _Asset(next(self._ids), name, req.body or b"")
        release.assets.append(asset)
        return _reply(201, self._asset_json(release, asset))

    def _read(self, req: _Request) -> _Reply:
        hit = self._asset_by_id(int(req.found["asset"]))
        if hit is None:
            return _problem(404, "Not Found")
        release, asset = hit
        if not _wants_bytes(req.headers):
            return _reply(200, self._asset_json(release, asset))
        headers = {
            "content-type": "application/octet-stream",
            "content-length": str(len(asset.data)),
        }
        return 200, headers, asset.data

    def _rename(self, req: _Request) -> _Reply:
        hit = self._asset_by_id(int(req.found["asset"]))
        if hit is None:
            return _problem(404, "Not Found")
        release, asset = hit
        fields = _json_object(req.body)
        if fields is None:
            return _problem(400, "Problems parsing JSON")
        name = fields.get("name")
        if not isinstance(name, str) or name == "":
            return _problem(422, "Validation Failed")
        if any(other.name == name for other in release.assets if other is not asset):
            return _already_exists("ReleaseAsset", "name")
        asset.name = name
        return _reply(200, self._asset_json(release, asset))

    def _delete(self, req: _Request) -> _Reply:
        hit = self._asset_by_id(int(req.found["asset"]))
        if hit is None:
            return _problem(404, "Not Found")
        release, asset = hit
        release.assets.remove(asset)
        return _reply(204)


@dataclass
class _Entry:
    status: int
    headers: dict[str, str]
    body: bytes
    remaining: int | None  # requests that this entry will still serve, None for any


def _encoded(body: bytes | str | dict) -> bytes:
    """The bytes of a reply body: a dict as JSON, a str as UTF-8, bytes as they are."""
    if isinstance(body, dict):
        return json.dumps(body).encode()
    if isinstance(body, str):
        return body.encode()
    return body


class FakeHttp:
    """The web pages that `obskit.http.Http` reads, held in memory, as a Transport.

    `add` gives a URL one more reply. The replies of one URL are served in the order
    they were added. A reply added with `times` is dropped once it has been served that
    often, and one added without it is served for ever, so it hides every reply added
    after it. A URL with no reply left is a 404 with no body, as a project that PyPI no
    longer has is. A URL matches as a whole string, query included, and the method and
    the body of a request make no difference.
    """

    def __init__(self) -> None:
        # (url, headers) of every request, in order, those answered 404 included.
        self.requests: list[tuple[str, dict[str, str]]] = []
        self._entries: dict[str, list[_Entry]] = {}

    def add(
        self,
        url: str,
        *,
        status: int = 200,
        body: bytes | str | dict = b"",
        headers: dict[str, str] | None = None,
        times: int | None = None,
    ) -> None:
        """Give `url` one more reply, served after the replies already added for it.

        A dict body is encoded as JSON and a str body as UTF-8. The header names are
        lowercased, as a Transport gives them. The reply is served `times` times, or for
        ever when `times` is None.
        """
        if times is not None and times < 1:
            raise ValueError(f"times must be at least 1, not {times}")
        lowered = {name.lower(): value for name, value in (headers or {}).items()}
        entry = _Entry(status, lowered, _encoded(body), times)
        self._entries.setdefault(url, []).append(entry)

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> _Reply:
        self.requests.append((url, dict(headers)))
        entries = self._entries.get(url)
        if not entries:
            return 404, {}, b""
        entry = entries[0]
        if entry.remaining is not None:
            entry.remaining -= 1
            if entry.remaining == 0:
                del entries[0]
        return entry.status, dict(entry.headers), entry.body


@dataclass
class _GoogleCall:
    """A request that matched a route of FakeGoogle: its URL, headers and JSON body."""

    method: str
    url: str
    headers: dict[str, str]
    body: bytes | None


class FakeGoogle:
    """The Google Cloud APIs that `obskit.gcp` calls, held in memory, as a Transport.

    The sign-in is three calls: GitHub's OIDC endpoint (at `oidc_url`), Google's STS and
    the impersonation of a service account. Each issues a token the next one must be
    given, and one that is not known is a 401. The impersonated tokens are named
    "sa-token-1", "sa-token-2" and so on, in the order issued, and each one ends
    `access_token_lifetime` seconds after it was issued, by the wall clock.

    A test reads what the sign-in sent: `last_oidc_audience`, the audience asked of
    GitHub, and `last_sts_body`, the JSON body of the latest STS request.
    `oidc_tokens_issued` counts the OIDC tokens. `fail_next` makes a request fail.

    Cloud Run, Cloud Storage, Pub/Sub and BigQuery are served from stores that a test
    fills and reads:

    - `objects` maps (bucket, name) to bytes.
    - `jobs` maps a job's name to the template it was deployed with.
    - `executions` is the list of Executions, newest last. `run_job` calls `on_run` with
      the env dict of the run (name to value), and `on_run` may add to `objects`; it
      returns the Execution to finish with, and without one a finished Execution that
      succeeded once is used. The Execution is given a `name` when it has none.
    - `operations` maps an operation's name to the operation, so a test can make one
      that never finishes, or ends in an error.
    - `messages` holds the Pub/Sub messages as JSON objects, and `pull` drains them. The
      ackIds it was sent are in `acked`, and `publish_time` is what it says it was
      published at.
    - `partitions` maps a partition id (such as "20261109") to (total_rows, modified),
      and the one BigQuery query that is served, a read of INFORMATION_SCHEMA.PARTITIONS,
      answers with a row `[partition_id, str(total_rows), modified]` for each id it
      lists that is held. The SQL of every query is in `queries`. Any other SQL raises
      NotImplementedError: nothing may scan table data.

    Routes are a table of (method, URL prefix, handler), tried in order, and `route`
    adds one. A request that no route takes raises NotImplementedError, never a 404.
    """

    def __init__(self, oidc_url: str = "https://gh.example/oidc") -> None:
        # (method, url) of every request, in order, failed ones included.
        self.requests: list[tuple[str, str]] = []
        self.oidc_tokens_issued = 0
        self.access_token_lifetime = 3600
        self.last_oidc_audience: str | None = None
        self.last_sts_body: dict | None = None
        self._failures: list[tuple[int, str]] = []
        self._federated: set[str] = set()
        self._access_tokens_issued = 0
        self.objects: dict[tuple[str, str], bytes] = {}
        self.jobs: dict[str, dict] = {}
        self.executions: list[dict] = []
        self.on_run: Callable[[dict[str, str]], dict] | None = None
        self.operations: dict[str, dict] = {}
        self.messages: list[dict] = []
        self.acked: list[str] = []
        self.publish_time = "2026-11-06T07:00:00.000Z"
        self.partitions: dict[str, tuple[int, str]] = {}
        self.queries: list[str] = []
        self._pulled = 0
        self._routes: list[tuple[str, str, Callable[[_GoogleCall], _Reply]]] = []
        self.route("GET", oidc_url, self._oidc)
        self.route("POST", "https://sts.googleapis.com/v1/token", self._sts)
        self.route(
            "POST",
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/",
            self._impersonate,
        )
        run = "https://run.googleapis.com/v2/projects/"
        self.route("POST", run, self._run_post)
        self.route("PATCH", run, self._run_patch)
        self.route("GET", run, self._run_get)
        self.route("GET", "https://storage.googleapis.com/storage/v1/b/", self._object)
        self.route("POST", "https://pubsub.googleapis.com/v1/projects/", self._pubsub)
        self.route(
            "POST", "https://bigquery.googleapis.com/bigquery/v2/projects/", self._query
        )

    def route(
        self, method: str, url_prefix: str, handler: Callable[[_GoogleCall], _Reply]
    ) -> None:
        """Answer the requests that use `method` and whose URL starts with `url_prefix`
        with `handler`, after the routes already added."""
        self._routes.append((method, url_prefix, handler))

    def fail_next(self, status: int, *, url_contains: str = "") -> None:
        """Answer the next request whose URL contains `url_contains` (any, by default)
        with `status` and a Google-shaped error, without carrying it out."""
        self._failures.append((status, url_contains))

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> _Reply:
        self.requests.append((method, url))
        for index, (status, needle) in enumerate(self._failures):
            if needle in url:
                del self._failures[index]
                return _reply(status, {"error": {"message": f"refused with {status}"}})
        call = _GoogleCall(method, url, dict(headers), body)
        for route_method, prefix, handler in self._routes:
            if method == route_method and url.startswith(prefix):
                return handler(call)
        raise NotImplementedError(f"FakeGoogle has no route for {method} {url}")

    def _oidc(self, call: _GoogleCall) -> _Reply:
        query = parse_qs(urlsplit(call.url).query)
        self.last_oidc_audience = query.get("audience", [""])[0]
        self.oidc_tokens_issued += 1
        return _reply(200, {"value": f"oidc-jwt-{self.oidc_tokens_issued}"})

    def _sts(self, call: _GoogleCall) -> _Reply:
        body = _json_object(call.body) or {}
        self.last_sts_body = body
        if not str(body.get("subjectToken", "")).startswith("oidc-jwt-"):
            return _problem(400, "the subject token is not one that was issued")
        token = f"sts-token-{body['subjectToken'].removeprefix('oidc-jwt-')}"
        self._federated.add(token)
        return _reply(200, {"access_token": token})

    def _impersonate(self, call: _GoogleCall) -> _Reply:
        bearer = call.headers.get("Authorization", "").removeprefix("Bearer ")
        if bearer not in self._federated:
            return _problem(401, "the federated token is not one that was issued")
        self._access_tokens_issued += 1
        ends = datetime.now(UTC) + timedelta(seconds=self.access_token_lifetime)
        return _reply(
            200,
            {
                "accessToken": f"sa-token-{self._access_tokens_issued}",
                "expireTime": ends.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )

    def _run_post(self, call: _GoogleCall) -> _Reply:
        path, _, query = call.url.partition("?")
        parts = path.removeprefix("https://run.googleapis.com/v2/").split("/")
        # projects/P/locations/R/jobs/J:run, or projects/P/locations/R/jobs?jobId=J
        if len(parts) == 6 and parts[4] == "jobs" and parts[5].endswith(":run"):
            return self._run(call, parts[1], parts[3], parts[5].removesuffix(":run"))
        if len(parts) == 5 and parts[4] == "jobs":
            job = parse_qs(query).get("jobId", [""])[0]
            return self._deploy(call, parts[1], parts[3], job, create=True)
        raise NotImplementedError(
            f"FakeGoogle has no route for {call.method} {call.url}"
        )

    def _run_patch(self, call: _GoogleCall) -> _Reply:
        parts = call.url.removeprefix("https://run.googleapis.com/v2/").split("/")
        if len(parts) == 6 and parts[4] == "jobs":
            return self._deploy(
                call, parts[1], parts[3], unquote(parts[5]), create=False
            )
        raise NotImplementedError(
            f"FakeGoogle has no route for {call.method} {call.url}"
        )

    def _run_get(self, call: _GoogleCall) -> _Reply:
        path, _, query = call.url.partition("?")
        parts = path.removeprefix("https://run.googleapis.com/v2/").split("/")
        if len(parts) == 6 and parts[4] == "jobs":
            job = self.jobs.get(unquote(parts[5]))
            return (
                _google_error(404, "the job is not there")
                if job is None
                else _reply(200, job)
            )
        if len(parts) == 7 and parts[6] == "executions":
            return self._execution_page(parse_qs(query).get("pageToken", [""])[0])
        if len(parts) == 6 and parts[4] == "operations":
            return self._operation(path.removeprefix("https://run.googleapis.com/v2/"))
        raise NotImplementedError(
            f"FakeGoogle has no route for {call.method} {call.url}"
        )

    def _operation(self, name: str) -> _Reply:
        operation = self.operations.get(name)
        if operation is None:
            return _google_error(404, "the operation is not there")
        return _reply(200, operation)

    def _new_operation(self, project: str, region: str, response: dict) -> _Reply:
        name = f"projects/{project}/locations/{region}/operations/op-{len(self.operations) + 1}"
        self.operations[name] = {"name": name, "done": True, "response": response}
        return _reply(200, {"name": name})

    def _run(self, call: _GoogleCall, project: str, region: str, job: str) -> _Reply:
        body = _json_object(call.body) or {}
        containers = body.get("overrides", {}).get("containerOverrides", [{}])
        env = {item["name"]: item["value"] for item in containers[0].get("env", [])}
        if self.on_run is not None:
            execution = dict(self.on_run(env))
        else:
            execution = {"succeededCount": 1, "failedCount": 0}
        number = len(self.executions) + 1
        execution.setdefault(
            "name",
            f"projects/{project}/locations/{region}/jobs/{job}/executions/{job}-{number}",
        )
        self.executions.append(execution)
        return self._new_operation(project, region, execution)

    def _deploy(
        self, call: _GoogleCall, project: str, region: str, job: str, *, create: bool
    ) -> _Reply:
        if create == (job in self.jobs):
            return _google_error(409 if create else 404, "the job is not as expected")
        self.jobs[job] = (_json_object(call.body) or {}).get("template", {})
        return self._new_operation(project, region, {"name": job})

    def _execution_page(self, token: str) -> _Reply:
        # Pages of two, so that a test sees the page tokens followed. The token is the
        # index of the page's first Execution.
        start = int(token) if token else 0
        page = self.executions[start : start + 2]
        reply: dict[str, object] = {"executions": page}
        if start + 2 < len(self.executions):
            reply["nextPageToken"] = str(start + 2)
        return _reply(200, reply)

    def _object(self, call: _GoogleCall) -> _Reply:
        path = urlsplit(call.url).path.removeprefix("/storage/v1/b/")
        bucket, _, name = path.partition("/o/")
        data = self.objects.get((unquote(bucket), unquote(name)))
        if data is None:
            return _google_error(404, "no such object")
        return 200, {"content-type": "application/octet-stream"}, data

    def _pubsub(self, call: _GoogleCall) -> _Reply:
        body = _json_object(call.body) or {}
        if call.url.endswith(":pull"):
            batch = self.messages[: body.get("maxMessages", 100)]
            del self.messages[: len(batch)]
            received = []
            for message in batch:
                self._pulled += 1
                data = base64.b64encode(json.dumps(message).encode()).decode()
                received.append(
                    {
                        "ackId": f"ack-{self._pulled}",
                        "message": {"data": data, "publishTime": self.publish_time},
                    }
                )
            return _reply(200, {"receivedMessages": received} if received else {})
        if call.url.endswith(":acknowledge"):
            self.acked.extend(body.get("ackIds", []))
            return _reply(200, {})
        raise NotImplementedError(
            f"FakeGoogle has no route for {call.method} {call.url}"
        )

    def _query(self, call: _GoogleCall) -> _Reply:
        sql = (_json_object(call.body) or {}).get("query", "")
        self.queries.append(sql)
        if "INFORMATION_SCHEMA.PARTITIONS" not in sql:
            raise NotImplementedError(
                f"FakeGoogle serves no SQL but INFORMATION_SCHEMA.PARTITIONS: {sql}"
            )
        listed = re.findall(r"'([0-9]{8})'", sql)
        rows = [
            {
                "f": [
                    {"v": pid},
                    {"v": str(self.partitions[pid][0])},
                    {"v": self.partitions[pid][1]},
                ]
            }
            for pid in listed
            if pid in self.partitions
        ]
        return _reply(200, {"jobComplete": True, "rows": rows})


def _google_error(status: int, message: str) -> _Reply:
    """An error reply shaped like Google's: an `error` with a message."""
    return _reply(status, {"error": {"message": message}})


# ---- Texts for the tests of obskit.emails, and of every stream that uses it.
#
# They are the forms of an email address that count as one, texts that look like an
# address and are not one, and texts made to be slow to scrub. They are plain lists, and
# this module imports no test framework: a test that parametrizes over HOSTILE_TEXTS
# wraps each pair in `pytest.param(text, id=name)`.

# Each of these is one whole address. The at is `@`, `%40` or `at` in a pair of
# brackets, and a dot of the domain is `.` or `dot` in a pair of brackets.
ADDRESS_FORMS = [
    "jane%40x.org",
    "JANE%40X.ORG",
    "jane [at] x [dot] org",
    "jane[at]x[dot]org",
    "jane [ at ] x [ dot ] org",
    "jane(at)x.org",
    "jane (AT) mail (DOT) x (DOT) org",
    "jane {at} x {dot} co {dot} uk",
    "jane <at> x <dot> org",
    "jane@x [dot] org",
    # White space of any kind may stand around the brackets and inside them.
    "jane [at] example\n[dot] com",
    "jane [at] example\r\n[dot] com",
    "jane\xa0[at]\xa0x\xa0[dot]\xa0org",
    "jane [\nat\n] x [\xa0dot\xa0] org",
]

# Texts with an address that a line break or no-break spaces run through, each with what
# is left of it once the address is taken out.
WHITE_SPACE_TEXTS = [
    (
        "Copyright (c) 2020 Jane Doe <jane [at] example\n[dot] com>",
        "Copyright (c) 2020 Jane Doe",
    ),
    (
        "Copyright (c) 2020 Jane Doe <jane [at] example\r\n[dot] com>",
        "Copyright (c) 2020 Jane Doe",
    ),
    ("broken, mail jane\xa0[at]\xa0x\xa0[dot]\xa0org", "broken, mail"),
]

# A `%40` with no name before it, as in the URL of a scoped npm package.
NPM_URL = "https://www.npmjs.com/package/%40scope%2Fname"

# None of these holds an address, so each is kept as it is.
NOT_ADDRESSES = [
    # Plain words cannot be told from prose.
    "jane at x dot org",
    "look at numpy dot product",
    "jane AT x DOT org",
    "Meet us [at] PyCon",  # no domain
    "x [dot] org",  # no at
    NPM_URL,
]

# Each of these can make a naive regular expression do work in proportion to the square
# of the length, or worse: 20,000 characters took one a second or more, so 50,000 take
# it several seconds. The code under test takes a few milliseconds, and the limit is a
# second.
LONG = 50_000

# Each of these is a text and the name that a test report gives it.
HOSTILE_TEXTS = [
    # Two short texts come first. A pattern that can read one of them in more than one
    # way takes seconds over it, as its work doubles or more with each repeat, so each
    # fails within seconds when its safeguard is gone. Under such a pattern some of the
    # long texts after them would never end, so these run first.
    ("a@x" + " [dot]  " * 12, "a few bracketed dots two blanks apart"),
    ("a@" + "b." * 24, "a few single letters after an at sign"),
    ("a" * LONG, "one long word"),
    (" " * LONG, "spaces"),
    ("<" + "@" * LONG, "bracket then at signs"),
    ("< " * (LONG // 2), "brackets and spaces"),
    ("a@" * (LONG // 2), "repeated at signs"),
    ("a@" + "b." * (LONG // 2), "domain of single letters"),
    ("a." * (LONG // 2) + "@", "dotted word then at sign"),
    (",".join(["a"] * (LONG // 2)), "commas"),
    ("[at]" * (LONG // 4), "bracketed ats"),
    ("a [at] " * (LONG // 7), "names and bracketed ats"),
    ("a%40" * (LONG // 4), "names and encoded ats"),
    ("a [at] " + "b [dot] " * (LONG // 8), "domain of bracketed dots"),
    ("a(at)" + "b." * (LONG // 2), "bracketed at then single letters"),
    ("[" * LONG, "opening brackets"),
    ("[ " * (LONG // 2), "opening brackets and spaces"),
    ("a [at]\n" * (LONG // 7), "names and bracketed ats, a line each"),
]
