"""Signing in to Google Cloud from a GitHub workflow, with no key stored anywhere.

The workflow's OIDC token is exchanged at Google's Security Token Service for a federated
token, and that is used to impersonate a service account, which gives the access token
that every Google call carries. The token is kept until 5 minutes before it ends and then
exchanged again, so a run that lasts longer than the token's hour keeps working.

A reply that is not a success is a GoogleError. No token, and no reply body that could
hold one, is ever put in an error message.
"""

import argparse
import json
import os
import re
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote

from obskit.github import Transport, UrllibTransport
from obskit.http import USER_AGENT

SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_STS_URL = "https://sts.googleapis.com/v1/token"
_IAM_URL = "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts"
_JWT = "urn:ietf:params:oauth:token-type:jwt"
_ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token"
_TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
_LIFETIME = "3600s"
# A token is exchanged again once it has this many seconds left, so that a request that
# starts just before the end still finishes inside the token's life.
_MARGIN = 300

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
        sent = {"User-Agent": USER_AGENT, **headers}
        body = None
        if payload is not None:
            sent["Content-Type"] = "application/json"
            body = json.dumps(payload).encode()
        label = f"{method} {url.split('?')[0]}"
        try:
            status, _, raw = self._transport.request(method, url, sent, body)
        except OSError as exc:
            raise GoogleError(f"{label} failed: {type(exc).__name__}") from exc
        if not 200 <= status <= 299:
            raise GoogleError(f"{label} returned HTTP {status}{_why(raw)}", status)
        try:
            reply = json.loads(raw)
        except ValueError:
            reply = None
        if not isinstance(reply, dict):
            raise GoogleError(f"{label} did not reply with a JSON object", status)
        return reply

    @staticmethod
    def _field(reply: dict, name: str, what: str) -> str:
        value = reply.get(name)
        if not isinstance(value, str) or not value:
            raise GoogleError(f"{what} has no `{name}`")
        return value


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
