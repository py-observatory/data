import base64
import json

import pytest

from obskit import gcp
from obskit.gcp import Credentials, Federation, Google, GoogleError, main
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


# ---- Google: the calls of a stream

PROJECT, REGION, JOB = "py-observatory", "us-central1", "pypi-downloads-counter"
JOB_URL = f"https://run.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/jobs"


def make_google(fake=None, token=None, **kwargs):
    fake = fake or FakeGoogle()
    sleeps: list[float] = []
    kwargs.setdefault("sleep", sleeps.append)
    google = Google(token or (lambda: "tok"), transport=fake, **kwargs)
    return google, fake, sleeps


class Spy(FakeGoogle):
    """A FakeGoogle that keeps the headers and body of each request."""

    def __init__(self):
        super().__init__()
        self.sent = []

    def request(self, method, url, headers, body):
        self.sent.append((method, url, headers, body))
        return super().request(method, url, headers, body)


def test_run_job_sends_the_day_as_an_override():
    spy = Spy()
    google, _, _ = make_google(spy)
    operation = google.run_job(PROJECT, REGION, JOB, {"DAY": "2026-11-04"})
    method, url, headers, body = spy.sent[0]
    assert (method, url) == ("POST", f"{JOB_URL}/{JOB}:run")
    assert json.loads(body) == {
        "overrides": {
            "containerOverrides": [{"env": [{"name": "DAY", "value": "2026-11-04"}]}]
        }
    }
    assert headers["Authorization"] == "Bearer tok"
    assert headers["User-Agent"] == USER_AGENT
    assert operation.startswith(f"projects/{PROJECT}/locations/{REGION}/operations/")


def test_wait_returns_the_execution_when_the_operation_is_done():
    fake = FakeGoogle()
    seen = []

    def on_run(env):
        seen.append(env)
        return {"name": "exec-1", "succeededCount": 1, "failedCount": 0}

    fake.on_run = on_run
    google, _, _ = make_google(fake)
    operation = google.run_job(PROJECT, REGION, JOB, {"DAY": "2026-11-04"})
    execution = google.wait(operation, timeout_seconds=60)
    assert execution == {"name": "exec-1", "succeededCount": 1, "failedCount": 0}
    assert seen == [{"DAY": "2026-11-04"}]
    assert fake.executions == [execution]


def test_wait_polls_until_the_operation_is_done():
    fake = FakeGoogle()
    google, _, _ = make_google(fake)
    operation = google.run_job(PROJECT, REGION, JOB, {})
    response = fake.operations[operation]["response"]
    fake.operations[operation] = {"name": operation, "done": False}
    polls = []

    def sleep(seconds):
        polls.append(seconds)
        if len(polls) == 2:
            fake.operations[operation] = {
                "name": operation,
                "done": True,
                "response": response,
            }

    google = Google(lambda: "tok", transport=fake, sleep=sleep, clock=lambda: 0.0)
    assert google.wait(operation, timeout_seconds=600, poll_seconds=7) == response
    assert polls == [7, 7]


def test_wait_raises_for_an_operation_error():
    fake = FakeGoogle()
    google, _, _ = make_google(fake)
    operation = google.run_job(PROJECT, REGION, JOB, {})
    fake.operations[operation] = {
        "name": operation,
        "done": True,
        "error": {"code": 8, "message": "quota exceeded"},
    }
    with pytest.raises(GoogleError, match="ended in an error: quota exceeded"):
        google.wait(operation, timeout_seconds=60)


def test_wait_gives_up_after_its_timeout():
    fake = FakeGoogle()
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    google = Google(lambda: "tok", transport=fake, sleep=sleep, clock=lambda: now[0])
    operation = google.run_job(PROJECT, REGION, JOB, {})
    fake.operations[operation] = {"name": operation, "done": False}
    with pytest.raises(GoogleError, match="still running after 100 seconds"):
        google.wait(operation, timeout_seconds=100, poll_seconds=30)
    assert sleeps == [30, 30, 30, 30]


def test_executions_follow_page_tokens():
    fake = FakeGoogle()
    fake.executions = [{"name": f"e{i}"} for i in range(5)]
    google, _, _ = make_google(fake)
    assert google.executions(PROJECT, REGION, JOB) == fake.executions
    assert len(fake.requests) == 3  # pages of two


def test_deploy_job_creates_then_patches():
    spy = Spy()
    google, _, _ = make_google(spy)
    template = {"template": {"containers": [{"image": "x"}]}}
    google.deploy_job(PROJECT, REGION, JOB, template)
    assert spy.jobs == {JOB: template}
    google.deploy_job(
        PROJECT, REGION, JOB, {"template": {"containers": [{"image": "y"}]}}
    )
    calls = [(m, u, b and json.loads(b)) for m, u, _, b in spy.sent if m != "GET"]
    assert calls[0] == ("POST", f"{JOB_URL}?jobId={JOB}", {"template": template})
    assert calls[1][:2] == ("PATCH", f"{JOB_URL}/{JOB}")
    assert calls[1][2] == {"template": {"template": {"containers": [{"image": "y"}]}}}
    assert spy.jobs[JOB]["template"]["containers"][0]["image"] == "y"
    assert [m for m, *_ in calls] == ["POST", "PATCH"]
    # Each deploy waited on its operation.
    assert [m for m, u, *_ in spy.sent if "/operations/" in u] == ["GET", "GET"]


def test_get_object_returns_none_for_404_and_refuses_oversize():
    spy = Spy()
    spy.objects[("b", "counts/2026-11-04.json")] = b"12345"
    google, _, _ = make_google(spy)
    assert google.get_object("b", "counts/2026-11-04.json", max_bytes=5) == b"12345"
    assert spy.sent[0][1] == (
        "https://storage.googleapis.com/storage/v1/b/b/o/counts%2F2026-11-04.json?alt=media"
    )
    assert google.get_object("b", "counts/2026-11-05.json", max_bytes=5) is None
    with pytest.raises(GoogleError, match="over the limit of 4"):
        google.get_object("b", "counts/2026-11-04.json", max_bytes=4)


def test_pull_decodes_and_acknowledges_every_message():
    fake = FakeGoogle()
    notes = [{"costAmount": float(i), "budgetAmount": 1.0} for i in range(5)]
    fake.messages = list(notes)
    google, _, _ = make_google(fake)
    got = google.pull(PROJECT, "budget-alerts-pull", max_messages=2)
    assert got == [{**n, "publishTime": fake.publish_time} for n in notes]
    assert fake.messages == []
    assert fake.acked == [f"ack-{i}" for i in range(1, 6)]
    assert google.pull(PROJECT, "budget-alerts-pull") == []


def test_pull_refuses_a_message_that_is_not_json_and_leaves_it_unacknowledged():
    class Odd:
        def request(self, method, url, headers, body):
            if url.endswith(":pull"):
                data = base64.b64encode(b"[1]").decode()
                return (
                    200,
                    {},
                    json.dumps(
                        {
                            "receivedMessages": [
                                {"ackId": "a", "message": {"data": data}}
                            ]
                        }
                    ).encode(),
                )
            raise AssertionError(url)

    with pytest.raises(GoogleError, match="JSON object"):
        Google(lambda: "t", transport=Odd()).pull(PROJECT, "s")


def test_query_returns_rows_as_strings():
    fake = FakeGoogle()
    fake.partitions = {"20261109": (123, "2026-11-11T01:02:03Z")}
    google, _, _ = make_google(fake)
    sql = (
        "SELECT partition_id, total_rows, FORMAT_TIMESTAMP('%FT%TZ', last_modified_time) "
        "FROM `bigquery-public-data.pypi.INFORMATION_SCHEMA.PARTITIONS` "
        "WHERE table_name = 'file_downloads' AND partition_id IN ('20261109', '20261110')"
    )
    assert google.query(PROJECT, sql) == [["20261109", "123", "2026-11-11T01:02:03Z"]]
    assert fake.queries == [sql]


def test_query_sends_its_options_and_refuses_an_unfinished_job():
    sent = []

    class Slow:
        def request(self, method, url, headers, body):
            sent.append((url, json.loads(body)))
            return 200, {}, b'{"jobComplete": false}'

    with pytest.raises(GoogleError, match="did not finish"):
        Google(lambda: "t", transport=Slow()).query(PROJECT, "SELECT 1", location="EU")
    assert sent == [
        (
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries",
            {
                "query": "SELECT 1",
                "useLegacySql": False,
                "location": "EU",
                "timeoutMs": 60000,
            },
        )
    ]


def test_fake_google_runs_no_sql_that_reads_a_table():
    google, _, _ = make_google()
    with pytest.raises(NotImplementedError, match="INFORMATION_SCHEMA"):
        google.query(
            PROJECT, "SELECT * FROM `bigquery-public-data.pypi.file_downloads`"
        )


def test_transient_failures_are_retried_then_raise():
    fake = FakeGoogle()
    google, _, sleeps = make_google(fake)
    for _ in range(6):
        fake.fail_next(503)
    with pytest.raises(GoogleError, match="HTTP 503: refused with 503") as caught:
        google.executions(PROJECT, REGION, JOB)
    assert caught.value.status == 503
    assert sleeps == [2, 4, 8, 16, 32]
    # Five failures and then an answer is a success.
    del sleeps[:]
    for _ in range(5):
        fake.fail_next(503)
    assert google.executions(PROJECT, REGION, JOB) == []
    assert sleeps == [2, 4, 8, 16, 32]


def test_a_429_waits_as_long_as_retry_after_asks_up_to_five_minutes():
    replies = [(429, {"retry-after": "7"}, b""), (429, {"retry-after": "9999"}, b"")]

    class Limited:
        def request(self, method, url, headers, body):
            return replies.pop(0) if replies else (200, {}, b'{"executions": []}')

    sleeps = []
    google = Google(lambda: "t", transport=Limited(), sleep=sleeps.append)
    assert google.executions(PROJECT, REGION, JOB) == []
    assert sleeps == [7, 300]


def test_a_network_failure_is_retried_and_names_no_url_query_or_text():
    class Down:
        def request(self, method, url, headers, body):
            raise OSError("secret-in-the-message")

    sleeps = []
    google = Google(lambda: "t", transport=Down(), sleep=sleeps.append)
    with pytest.raises(GoogleError) as caught:
        google.get_object("b", "n", max_bytes=1)
    assert sleeps == [2, 4, 8, 16, 32]
    assert caught.value.status is None
    assert "secret-in-the-message" not in str(caught.value)
    assert "alt=media" not in str(caught.value)


def test_a_401_gets_a_fresh_token_once():
    tokens = iter(["old", "new", "newer", "newer"])
    spy_headers = []

    class Seen(FakeGoogle):
        def request(self, method, url, headers, body):
            spy_headers.append(headers["Authorization"])
            return super().request(method, url, headers, body)

    seen = Seen()
    seen.fail_next(401)
    google = Google(lambda: next(tokens), transport=seen, sleep=lambda s: None)
    assert google.executions(PROJECT, REGION, JOB) == []
    assert spy_headers == ["Bearer old", "Bearer new"]
    # A second 401 in a row is raised, not retried again.
    seen.fail_next(401)
    seen.fail_next(401)
    with pytest.raises(GoogleError) as caught:
        google.executions(PROJECT, REGION, JOB)
    assert caught.value.status == 401
    assert spy_headers[2:] == ["Bearer newer", "Bearer newer"]


def test_a_refusal_quotes_google_and_never_the_token_or_the_query():
    fake = FakeGoogle()
    google, _, sleeps = make_google(fake, token=lambda: "sekret-token")
    fake.fail_next(403)
    with pytest.raises(GoogleError) as caught:
        google.get_object("b", "n", max_bytes=1)
    text = str(caught.value)
    assert text == (
        "GET https://storage.googleapis.com/storage/v1/b/b/o/n returned HTTP 403: refused with 403"
    )
    assert "sekret-token" not in text
    assert sleeps == []


def test_a_credentials_backed_google_retries_a_401_with_a_new_token():
    seen = Spy()
    creds = Credentials(FED, transport=seen, environ=ENV)
    google = Google(creds.token, renew=creds.invalidate, transport=seen)
    seen.fail_next(401, url_contains="run.googleapis.com")
    assert google.executions(PROJECT, REGION, JOB) == []
    bearers = [h["Authorization"] for m, u, h, b in seen.sent if "run.googleapis" in u]
    assert bearers == ["Bearer sa-token-1", "Bearer sa-token-2"]


def test_a_refused_sign_in_is_raised_at_once():
    def refused():
        raise GoogleError("the STS exchange returned HTTP 403", 403)

    sleeps = []
    google = Google(refused, transport=FakeGoogle(), sleep=sleeps.append)
    with pytest.raises(GoogleError) as caught:
        google.executions(PROJECT, REGION, JOB)
    assert caught.value.status == 403
    assert sleeps == []


def test_a_sign_in_that_got_no_reply_is_retried():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise GoogleError("the STS exchange failed: TimeoutError")
        return "t"

    sleeps = []
    google = Google(flaky, transport=FakeGoogle(), sleep=sleeps.append)
    assert google.executions(PROJECT, REGION, JOB) == []
    assert sleeps == [2]


def test_every_pull_returns_immediately():
    spy = Spy()
    google, _, _ = make_google(spy)
    google.pull(PROJECT, "budget-alerts-pull", max_messages=7)
    bodies = [json.loads(b) for m, u, h, b in spy.sent if u.endswith(":pull")]
    assert bodies == [{"maxMessages": 7, "returnImmediately": True}]
