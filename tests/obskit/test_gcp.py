import pytest

from obskit import gcp
from obskit.gcp import Credentials, Federation, GoogleError, main
from obskit.http import USER_AGENT
from obskit.testing import FakeGoogle

FED = Federation(
    "893320255656",
    "github",
    "data",
    "downloads-github@py-observatory.iam.gserviceaccount.com",
)
ENV = {
    "ACTIONS_ID_TOKEN_REQUEST_URL": "https://gh.example/oidc?x=1",
    "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "req",
}
TOKEN_ARGS = [
    "token",
    "--project-number", FED.project_number,
    "--pool", FED.pool,
    "--provider", FED.provider,
    "--service-account", FED.service_account,
]  # fmt: skip


class Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def test_federation_names_its_audiences():
    assert FED.audience == (
        "//iam.googleapis.com/projects/893320255656/locations/global"
        "/workloadIdentityPools/github/providers/data"
    )
    assert FED.oidc_audience == "https:" + FED.audience


def test_token_exchanges_github_oidc_for_an_impersonated_token():
    fake = FakeGoogle()
    creds = Credentials(FED, transport=fake, environ=ENV, clock=lambda: 1000.0)
    assert creds.token() == "sa-token-1"
    methods_urls = [(m, u.split("?")[0]) for m, u in fake.requests]
    assert methods_urls == [
        ("GET", "https://gh.example/oidc"),
        ("POST", "https://sts.googleapis.com/v1/token"),
        (
            "POST",
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
            "downloads-github@py-observatory.iam.gserviceaccount.com:generateAccessToken",
        ),
    ]
    assert fake.last_oidc_audience == FED.oidc_audience
    assert fake.last_sts_body["audience"] == FED.audience
    assert (
        fake.last_sts_body["subjectTokenType"] == "urn:ietf:params:oauth:token-type:jwt"
    )


def test_every_call_carries_the_user_agent_and_the_oidc_url_keeps_its_query():
    seen = []

    class Spy(FakeGoogle):
        def request(self, method, url, headers, body):
            seen.append(headers)
            return super().request(method, url, headers, body)

    spy = Spy()
    Credentials(FED, transport=spy, environ=ENV).token()
    assert all(h["User-Agent"] == USER_AGENT for h in seen)
    assert spy.last_sts_body["scope"] == gcp.SCOPE
    assert spy.requests[0][1].startswith(
        "https://gh.example/oidc?x=1&audience=https%3A%2F%2Fiam.googleapis.com%2F"
    )


def test_token_is_cached_until_close_to_expiry():
    fake = FakeGoogle()
    clock = Clock(1000.0)
    creds = Credentials(FED, transport=fake, environ=ENV, clock=clock)
    first = creds.token()
    clock.now = 1600.0
    assert creds.token() == first
    assert fake.oidc_tokens_issued == 1
    assert len(fake.requests) == 3


def test_token_is_refreshed_before_it_runs_out():
    fake = FakeGoogle()
    clock = Clock(1000.0)
    creds = Credentials(FED, transport=fake, environ=ENV, clock=clock)
    assert creds.token() == "sa-token-1"
    clock.now = 4200.0  # 400 s left: still good
    assert creds.token() == "sa-token-1"
    clock.now = 4300.0  # inside the 300 s margin of the end at 4600
    assert creds.token() == "sa-token-2"
    assert fake.oidc_tokens_issued == 2


def test_token_without_id_token_permission_says_so():
    fake = FakeGoogle()
    with pytest.raises(GoogleError, match="id-token: write"):
        Credentials(FED, transport=fake, environ={}).token()
    assert fake.requests == []


@pytest.mark.parametrize("needle", ["gh.example", "sts.google", "iamcredentials"])
def test_a_refused_exchange_raises_with_its_status(needle):
    fake = FakeGoogle()
    fake.fail_next(403, url_contains=needle)
    with pytest.raises(GoogleError, match="refused with 403") as caught:
        Credentials(FED, transport=fake, environ=ENV).token()
    assert caught.value.status == 403


def test_a_refused_exchange_without_a_filter_fails_the_next_request():
    fake = FakeGoogle()
    fake.fail_next(403)
    with pytest.raises(GoogleError) as caught:
        Credentials(FED, transport=fake, environ=ENV).token()
    assert caught.value.status == 403


def test_a_network_failure_is_a_google_error_with_no_status():
    class Down:
        def request(self, method, url, headers, body):
            raise OSError("secret-in-the-message")

    with pytest.raises(GoogleError) as caught:
        Credentials(FED, transport=Down(), environ=ENV).token()
    assert caught.value.status is None
    assert "secret-in-the-message" not in str(caught.value)


def test_a_reply_without_a_token_is_an_error_that_quotes_no_body():
    class Odd:
        def request(self, method, url, headers, body):
            return 200, {}, b'{"value": "", "leak": "sekret"}'

    with pytest.raises(GoogleError, match="no `value`") as caught:
        Credentials(FED, transport=Odd(), environ=ENV).token()
    assert "sekret" not in str(caught.value)


def test_errors_never_carry_the_oidc_request_url_or_a_token():
    fake = FakeGoogle()
    fake.fail_next(500, url_contains="gh.example")
    with pytest.raises(GoogleError) as caught:
        Credentials(FED, transport=fake, environ=ENV).token()
    assert "x=1" not in str(caught.value)
    assert "req" not in str(caught.value).replace("returned", "")


def test_token_command_prints_only_the_token(capsys):
    assert main(TOKEN_ARGS, transport=FakeGoogle(), environ=ENV) == 0
    assert capsys.readouterr().out == "sa-token-1"


def test_token_command_reports_a_failure_on_stderr(capsys):
    assert main(TOKEN_ARGS, transport=FakeGoogle(), environ={}) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "id-token: write" in captured.err


def test_fake_google_refuses_a_federated_token_it_did_not_issue():
    fake = FakeGoogle()
    status, _, _ = fake.request(
        "POST",
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/x:generateAccessToken",
        {"Authorization": "Bearer nope"},
        b"{}",
    )
    assert status == 401


def test_fake_google_takes_added_routes_and_refuses_unknown_ones():
    fake = FakeGoogle()
    fake.route("GET", "https://run.example/", lambda call: (200, {}, b"ok"))
    assert fake.request("GET", "https://run.example/jobs", {}, None) == (200, {}, b"ok")
    with pytest.raises(NotImplementedError):
        fake.request("GET", "https://other.example/", {}, None)
