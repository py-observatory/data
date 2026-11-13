"""Signing in to Google Cloud from a GitHub workflow, with no key stored anywhere.

The workflow's OIDC token is exchanged at Google's Security Token Service for a federated
token, and that is used to impersonate a service account, which gives the access token
that every Google call carries. The token is kept until 5 minutes before it ends and then
exchanged again, so a run that lasts longer than the token's hour keeps working.

`Google` is the few calls that a stream makes with that token: run a Cloud Run job and
wait for it, list its executions, deploy it, read a Cloud Storage object, pull Pub/Sub
messages and acknowledge them, and run a BigQuery metadata query.

A reply that is not a success is a GoogleError. No token, and no reply body that could
hold one, is ever put in an error message.
"""

import argparse
import base64
import binascii
import json
import os
import re
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote

from obskit.github import _BACKOFF, Transport, UrllibTransport, _retry_after
from obskit.http import USER_AGENT

SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_STS_URL = "https://sts.googleapis.com/v1/token"
_IAM_URL = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts"
_JWT = "urn:ietf:params:oauth:token-type:jwt"
_ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token"
_TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
_RUN_URL = "https://run.googleapis.com/v2"
_STORAGE_URL = "https://storage.googleapis.com/storage/v1/b"
_PUBSUB_URL = "https://pubsub.googleapis.com/v1/projects"
_BIGQUERY_URL = "https://bigquery.googleapis.com/bigquery/v2/projects"
_LIFETIME = "3600s"
# A token is exchanged again once it has this many seconds left, so that a request that
# starts just before the end still finishes inside the token's life.
_MARGIN = 300
# How long deploying a job may take to finish, and how often that is looked at.
_DEPLOY_TIMEOUT = 600
# How long BigQuery may work on a query before it gives up, in milliseconds.
_QUERY_TIMEOUT_MS = 60_000

# The fractional seconds of an RFC 3339 time, which Google writes with up to 9 digits.
_FRACTION = re.compile(r"\.[0-9]+")


@dataclass(frozen=True)
class Federation:
    """Where a workflow signs in: the workload identity pool provider that admits it, and
    the service account that it then acts as."""

    project_number: str
    pool: str
    provider: str
    service_account: str

    @property
    def audience(self) -> str:
        """The provider's full name, which the STS exchange names as its audience."""
        return (
            f"//iam.googleapis.com/projects/{self.project_number}/locations/global"
            f"/workloadIdentityPools/{self.pool}/providers/{self.provider}"
        )

    @property
    def oidc_audience(self) -> str:
        """The audience that GitHub is asked to put in the OIDC token."""
        return f"https:{self.audience}"


class GoogleError(RuntimeError):
    """A sign-in call failed. `status` is the HTTP status that caused it, or None when
    there was no usable reply: the network failed, or the reply was not what it should
    be."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def _seconds_left(expire_time: str) -> float:
    """The seconds from now until the RFC 3339 time `expire_time`. It is measured against
    the wall clock, which is what Google's time is, so the injected clock of Credentials
    needs only to tick."""
    try:
        ends = datetime.fromisoformat(_FRACTION.sub("", expire_time))
    except ValueError as exc:
        raise GoogleError("the access token's expireTime is not a time") from exc
    if ends.tzinfo is None:
        raise GoogleError("the access token's expireTime has no time zone")
    return (ends - datetime.now(UTC)).total_seconds()


class Credentials:
    """The access token of a federated sign-in, renewed as it runs out.

    `environ` is where the workflow's OIDC request URL and token are read, and `clock` is
    in seconds and is what the token's life is counted on.
    """

    def __init__(
        self,
        federation: Federation,
        *,
        transport: Transport | None = None,
        environ: Mapping[str, str] = os.environ,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._federation = federation
        self._transport = transport if transport is not None else UrllibTransport()
        self._environ = environ
        self._clock = clock
        self._token: str | None = None
        self._renew_at = 0.0

    def token(self) -> str:
        """An access token that is good for at least 5 more minutes, exchanged anew when
        the one held is that close to its end."""
        if self._token is None or self._clock() >= self._renew_at:
            self._exchange()
        assert self._token is not None
        return self._token

    def invalidate(self) -> None:
        """Forget the token held, so the next `token()` exchanges anew. A caller does this
        when Google refused the token as it stood, which can happen well before its end
        (it was revoked, or the clocks differ)."""
        self._token = None

    def _exchange(self) -> None:
        federated = self._federated(self._github_token())
        token, expire_time = self._impersonated(federated)
        self._renew_at = self._clock() + _seconds_left(expire_time) - _MARGIN
        self._token = token

    def _github_token(self) -> str:
        """The OIDC token that GitHub issues to the running workflow."""
        url = self._environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")
        request_token = self._environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
        if not url or not request_token:
            raise GoogleError(
                "GitHub gave this job no OIDC token: the workflow needs the "
                "permission `id-token: write`"
            )
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}audience={quote(self._federation.oidc_audience, safe='')}"
        reply = self._call("GET", url, {"Authorization": f"bearer {request_token}"})
        return self._field(reply, "value", "GitHub's OIDC reply")

    def _federated(self, jwt: str) -> str:
        """The federated token that Google's STS gives for GitHub's OIDC token."""
        body = {
            "audience": self._federation.audience,
            "grantType": _TOKEN_EXCHANGE,
            "requestedTokenType": _ACCESS_TOKEN,
            "scope": SCOPE,
            "subjectTokenType": _JWT,
            "subjectToken": jwt,
        }
        reply = self._call("POST", _STS_URL, {}, body)
        return self._field(reply, "access_token", "the STS reply")

    def _impersonated(self, federated: str) -> tuple[str, str]:
        """The service account's access token and its expireTime."""
        url = f"{_IAM_URL}/{self._federation.service_account}:generateAccessToken"
        body = {"scope": [SCOPE], "lifetime": _LIFETIME}
        reply = self._call("POST", url, {"Authorization": f"Bearer {federated}"}, body)
        return (
            self._field(reply, "accessToken", "the impersonation reply"),
            self._field(reply, "expireTime", "the impersonation reply"),
        )

    def _call(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        payload: dict | None = None,
    ) -> dict:
        """One request, and its reply as a JSON object. It raises a GoogleError for a
        status outside 200-299, a network failure, and a body that is not an object. The
        message says which call failed, and never quotes the URL or a body: the OIDC
        request's URL holds a secret, and a reply may hold a token."""
        status, _, raw = _transmit(self._transport, method, url, headers, payload)
        return _object_reply(method, url, status, raw)

    @staticmethod
    def _field(reply: dict, name: str, what: str) -> str:
        value = reply.get(name)
        if not isinstance(value, str) or not value:
            raise GoogleError(f"{what} has no `{name}`")
        return value


def _label(method: str, url: str) -> str:
    """How an error names a call: the method and the URL without its query, which may
    hold a secret."""
    return f"{method} {url.split('?')[0]}"


def _transmit(
    transport: Transport,
    method: str,
    url: str,
    headers: dict[str, str],
    payload: dict | None,
) -> tuple[int, dict[str, str], bytes]:
    """One request, whatever its status. A network failure is a GoogleError with no
    status. Its text is left out of the message, and the transport's error is not
    chained to it, because it may quote the URL, query and all, and a traceback would
    show it."""
    sent = {"User-Agent": USER_AGENT, **headers}
    body = None
    if payload is not None:
        sent["Content-Type"] = "application/json"
        body = json.dumps(payload).encode()
    try:
        return transport.request(method, url, sent, body)
    except OSError as exc:
        raise GoogleError(
            f"{_label(method, url)} failed: {type(exc).__name__}"
        ) from None


def _refusal(method: str, url: str, status: int, raw: bytes) -> GoogleError:
    """The error for a reply that is not a success, with Google's own message."""
    return GoogleError(
        f"{_label(method, url)} returned HTTP {status}{_why(raw)}", status
    )


def _object_reply(method: str, url: str, status: int, raw: bytes) -> dict:
    """The JSON object in a reply, or the GoogleError that the reply amounts to."""
    if not 200 <= status <= 299:
        raise _refusal(method, url, status, raw)
    try:
        reply = json.loads(raw)
    except ValueError:
        reply = None
    if not isinstance(reply, dict):
        raise GoogleError(
            f"{_label(method, url)} did not reply with a JSON object", status
        )
    return reply


def _why(raw: bytes) -> str:
    """`: <message>` from the `error.message` of a Google error reply, and nothing when
    the reply has none. STS errors are shaped differently (`error_description`), and
    that is used too."""
    try:
        reply = json.loads(raw)
    except ValueError:
        return ""
    if not isinstance(reply, dict):
        return ""
    error = reply.get("error")
    message = error.get("message") if isinstance(error, dict) else None
    if message is None:
        message = reply.get("error_description")
    return f": {message}" if isinstance(message, str) and message else ""


class Google:
    """The Google Cloud calls of a stream, made with the bearer token that `token` gives.

    `token` is asked for on every request, so a Credentials renews it as it runs out.
    `sleep` and `clock` (in seconds, and only ever ticking) are for tests.

    A request that fails with 429, a 5xx status or a network error is tried again after
    2, 4, 8, 16 and 32 seconds, or after the wait that the reply's `retry-after` asks
    for, up to 5 minutes, and then fails. A 401 is tried once more at once with a fresh
    `token()`, in case the token ran out in flight. `renew`, when given, is called first
    to drop the token held (`Credentials.invalidate`), so that the fresh one is new.
    A failure to get a token is retried only when it may pass: no reply at all, 429 or
    a 5xx status. A refused sign-in is raised at once.
    """

    def __init__(
        self,
        token: Callable[[], str],
        *,
        renew: Callable[[], None] | None = None,
        transport: Transport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._token = token
        self._renew = renew
        self._transport = transport if transport is not None else UrllibTransport()
        self._sleep = sleep
        self._clock = clock

    def run_job(self, project: str, region: str, job: str, env: dict[str, str]) -> str:
        """Start an execution of a Cloud Run job with `env` added to its environment,
        and return the name of the operation that tracks it."""
        url = f"{_RUN_URL}/{_job_path(project, region, job)}:run"
        variables = [{"name": name, "value": value} for name, value in env.items()]
        body = {"overrides": {"containerOverrides": [{"env": variables}]}}
        return self._operation_name(self._json("POST", url, body))

    def wait(
        self, operation: str, *, timeout_seconds: float, poll_seconds: float = 30
    ) -> dict:
        """Wait for a Cloud Run operation and return the Execution that is its response.
        An operation that ended in an error, and one still running after
        `timeout_seconds`, raise a GoogleError."""
        url = f"{_RUN_URL}/{operation}"
        started = self._clock()
        while True:
            reply = self._json("GET", url)
            if reply.get("done"):
                return _operation_result(operation, reply)
            if self._clock() - started >= timeout_seconds:
                raise GoogleError(
                    f"{operation} was still running after {timeout_seconds:g} seconds"
                )
            self._sleep(poll_seconds)

    def executions(self, project: str, region: str, job: str) -> list[dict]:
        """Every Execution of a job, following the pages."""
        base = f"{_RUN_URL}/{_job_path(project, region, job)}/executions"
        found: list[dict] = []
        page = ""
        while True:
            url = f"{base}?pageToken={quote(page, safe='')}" if page else base
            reply = self._json("GET", url)
            found.extend(_list_of_objects(reply, "executions", _label("GET", url)))
            page = reply.get("nextPageToken") or ""
            if not page:
                return found

    def deploy_job(self, project: str, region: str, job: str, template: dict) -> None:
        """Make the job exist with `template`: create it when it is not there, and
        otherwise patch it. This returns once the change has finished."""
        parent = f"{_RUN_URL}/projects/{project}/locations/{region}/jobs"
        path = f"{parent}/{quote(job, safe='')}"
        status, _ = self._request("GET", path, accept=(404,))
        body = {"template": template}
        if status == 404:
            created = f"{parent}?jobId={quote(job, safe='')}"
            reply = self._json("POST", created, body)
        else:
            reply = self._json("PATCH", path, body)
        self.wait(self._operation_name(reply), timeout_seconds=_DEPLOY_TIMEOUT)

    def get_object(self, bucket: str, name: str, *, max_bytes: int) -> bytes | None:
        """The bytes of a Cloud Storage object, None when there is no such object, and a
        GoogleError for one longer than `max_bytes`."""
        url = f"{_STORAGE_URL}/{quote(bucket, safe='')}/o/{quote(name, safe='')}"
        status, raw = self._request("GET", f"{url}?alt=media", accept=(404,))
        if status == 404:
            return None
        if len(raw) > max_bytes:
            raise GoogleError(
                f"GET {url} is {len(raw)} bytes, over the limit of {max_bytes}"
            )
        return raw

    def pull(
        self, project: str, subscription: str, *, max_messages: int = 100
    ) -> list[dict]:
        """Every message waiting in a Pub/Sub subscription, decoded from its JSON, with
        the time Pub/Sub published it added as `publishTime`. A message that is not a
        JSON object raises a GoogleError and is left unacknowledged.

        Each batch is acknowledged once it is decoded, within the subscription's ack
        deadline, which may be as short as 10 s: Pub/Sub ignores a later
        acknowledgement and delivers the message again."""
        base = f"{_PUBSUB_URL}/{project}/subscriptions/{subscription}"
        messages: list[dict] = []
        while True:
            # Without returnImmediately an empty pull may be held open past the
            # transport's 60 s timeout. Google discourages the flag for throughput, but
            # a stream pulls a few times a day.
            body = {"maxMessages": max_messages, "returnImmediately": True}
            reply = self._json("POST", f"{base}:pull", body)
            where = f"POST {base}:pull"
            received = _list_of_objects(reply, "receivedMessages", where)
            if not received:
                return messages
            messages.extend(_decoded(item, where) for item in received)
            ack_ids = [item["ackId"] for item in received]
            self.acknowledge(project, subscription, ack_ids)

    def acknowledge(self, project: str, subscription: str, ack_ids: list[str]) -> None:
        """Acknowledge the messages of a Pub/Sub subscription that were pulled under
        `ack_ids`, so that they are not delivered again. An empty list sends nothing.
        Pub/Sub may ignore an ackId whose ack deadline has passed, and then delivers its
        message again."""
        if not ack_ids:
            return
        url = f"{_PUBSUB_URL}/{project}/subscriptions/{subscription}:acknowledge"
        self._json("POST", url, {"ackIds": list(ack_ids)})

    def query(
        self, project: str, sql: str, *, location: str = "US"
    ) -> list[list[str | None]]:
        """Run a BigQuery query, never answered from the cache, and return its rows,
        each as a list of cell values. A query that has not finished within a minute
        raises a GoogleError."""
        url = f"{_BIGQUERY_URL}/{project}/queries"
        body = {
            "query": sql,
            "useLegacySql": False,
            "location": location,
            "timeoutMs": _QUERY_TIMEOUT_MS,
            # A reply from BigQuery's cache could hide what has changed since.
            "useQueryCache": False,
        }
        reply = self._json("POST", url, body)
        if reply.get("jobComplete") is not True:
            raise GoogleError(f"POST {url} did not finish within a minute")
        rows = _list_of_objects(reply, "rows", f"POST {url}")
        try:
            return [[cell["v"] for cell in row["f"]] for row in rows]
        except (KeyError, TypeError) as exc:
            raise GoogleError(
                f"POST {url} replied with rows of an unknown shape"
            ) from exc

    def _json(self, method: str, url: str, payload: dict | None = None) -> dict:
        _, raw = self._request(method, url, payload)
        return _object_reply(method, url, 200, raw)

    def _request(
        self,
        method: str,
        url: str,
        payload: dict | None = None,
        *,
        accept: tuple[int, ...] = (),
    ) -> tuple[int, bytes]:
        """One request with its retries: the status and body of a 2xx reply, or of one
        whose status is in `accept`. Any other reply is a GoogleError."""
        retries = 0
        renewed = False
        while True:
            try:
                status, headers, raw = _transmit(
                    self._transport,
                    method,
                    url,
                    {"Authorization": f"Bearer {self._token()}"},
                    payload,
                )
            except GoogleError as exc:
                if not _may_pass(exc) or retries == len(_BACKOFF):
                    raise
                self._sleep(_BACKOFF[retries])
                retries += 1
                continue
            if status == 401 and not renewed:
                renewed = True
                if self._renew is not None:
                    self._renew()
                continue
            if 200 <= status <= 299 or status in accept:
                return status, raw
            if (status == 429 or status >= 500) and retries < len(_BACKOFF):
                wait = _retry_after(headers)
                self._sleep(_BACKOFF[retries] if wait is None else wait)
                retries += 1
                continue
            raise _refusal(method, url, status, raw)

    @staticmethod
    def _operation_name(reply: dict) -> str:
        name = reply.get("name")
        if not isinstance(name, str) or not name:
            raise GoogleError("the reply has no operation `name`")
        return name


def _may_pass(error: GoogleError) -> bool:
    """Whether trying again can help: there was no reply, or it was 429 or a 5xx."""
    return error.status is None or error.status == 429 or error.status >= 500


def _job_path(project: str, region: str, job: str) -> str:
    return f"projects/{project}/locations/{region}/jobs/{quote(job, safe='')}"


def _operation_result(operation: str, reply: dict) -> dict:
    """The Execution in a finished operation, or the error it ended with."""
    error = reply.get("error")
    if error:
        message = error.get("message") if isinstance(error, dict) else None
        detail = f": {message}" if isinstance(message, str) and message else ""
        raise GoogleError(f"{operation} ended in an error{detail}")
    response = reply.get("response")
    if not isinstance(response, dict):
        raise GoogleError(f"{operation} finished with no response")
    return response


def _list_of_objects(reply: dict, field: str, where: str) -> list[dict]:
    """The list under `field` of a reply, empty when Google left it out."""
    items = reply.get(field, [])
    if not isinstance(items, list) or not all(isinstance(i, dict) for i in items):
        raise GoogleError(f"{where} replied with an odd `{field}`")
    return items


def _decoded(item: dict, where: str) -> dict:
    """A received Pub/Sub message as the JSON object in its data plus `publishTime`."""
    message = item.get("message")
    ack_id = item.get("ackId")
    if not isinstance(message, dict) or not isinstance(ack_id, str):
        raise GoogleError(f"{where} replied with an odd message")
    try:
        data = json.loads(base64.b64decode(message.get("data", ""), validate=True))
    except (binascii.Error, ValueError) as exc:
        raise GoogleError("a Pub/Sub message does not hold JSON") from exc
    if not isinstance(data, dict):
        raise GoogleError("a Pub/Sub message does not hold a JSON object")
    return {**data, "publishTime": message.get("publishTime")}


def main(
    argv: list[str] | None = None,
    *,
    transport: Transport | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Run from the command line: `python -m obskit.gcp token ...` writes an access token
    to stdout, with no newline, for a workflow step to capture. A failure writes its
    message to stderr and returns 1. `transport` and `environ` are for tests."""
    parser = argparse.ArgumentParser(prog="python -m obskit.gcp")
    commands = parser.add_subparsers(dest="command", required=True)
    token = commands.add_parser("token", help="print an access token")
    for option in ("project-number", "pool", "provider", "service-account"):
        token.add_argument(f"--{option}", required=True)
    args = parser.parse_args(argv)
    federation = Federation(
        args.project_number, args.pool, args.provider, args.service_account
    )
    credentials = Credentials(
        federation,
        transport=transport,
        environ=os.environ if environ is None else environ,
    )
    try:
        value = credentials.token()
    except GoogleError as exc:
        print(f"obskit.gcp: {exc}", file=sys.stderr)
        return 1
    sys.stdout.write(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
