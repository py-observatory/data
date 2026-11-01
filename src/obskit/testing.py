"""Fakes for tests. FakeGitHub is the GitHub releases API, held in memory."""

import itertools
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from hashlib import sha256
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
    path, the query string, and the body."""

    found: re.Match[str]
    query: dict[str, list[str]]
    body: bytes | None


class FakeGitHub:
    """The part of the GitHub releases REST API that `obskit.github.GitHub` uses, held
    in memory, as a Transport.

    It answers as GitHub does where the kit depends on it:

    - A tag with no release is a 404. A second release for a tag is a 422.
    - Assets are listed a page at a time (`per_page`, `page`). Each has an `id`, a
      `name`, a `size`, a `digest` ("sha256:<hex>") and a `browser_download_url`.
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
        download = re.compile(
            rf"/{re.escape(repo)}/releases/download/(?P<tag>[^/]+)/(?P<name>.+)"
        )
        self._asset_path = asset
        # Each route: the method, the host, a pattern for the whole path, and a handler.
        self._routes: list[
            tuple[str, str, re.Pattern[str], Callable[[_Request], _Reply]]
        ] = [
            ("GET", "api.github.com", by_tag, self._get),
            ("POST", "api.github.com", collection, self._create),
            ("GET", "api.github.com", assets, self._list),
            ("POST", "uploads.github.com", assets, self._upload),
            ("PATCH", "api.github.com", asset, self._rename),
            ("DELETE", "api.github.com", asset, self._delete),
            ("GET", "github.com", download, self._download),
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

        The URL of a rename or a delete holds only the id of the asset. To aim at one
        asset by its name, match `name=<name>`: such a request also matches as if its
        URL ended `?name=<the asset's name at that moment>`, as an upload's URL does.
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
                return handler(_Request(found, parse_qs(parts.query), body))
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
        url = f"https://github.com/{self.repo}/releases/download/{release.tag}"
        return {
            "id": asset.id,
            "name": asset.name,
            "size": len(asset.data),
            "digest": asset.digest,
            "browser_download_url": f"{url}/{asset.name}",
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

    def _download(self, req: _Request) -> _Reply:
        release = self._releases.get(req.found["tag"])
        for asset in release.assets if release else ():
            if asset.name == req.found["name"]:
                headers = {
                    "content-type": "application/octet-stream",
                    "content-length": str(len(asset.data)),
                }
                return 200, headers, asset.data
        return _problem(404, "Not Found")
