"""Builders of the stream's test fixtures, and the runs of the stream on a fake GitHub
and a fake Google Cloud. The counts file builder writes the counter's layout from the
stream's own copy of its schema, because the tests cannot import the counter."""

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator
from compression import zstd
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from obskit.gcp import Credentials, Federation, Google
from obskit.github import GitHub
from obskit.manifest import Asset, Manifest, instant, loads
from obskit.testing import FakeGitHub, FakeGoogle
from obskit.zst import decompress_file
from pypi_downloads import run as run_module
from pypi_downloads.config import load
from pypi_downloads.counts import COUNTS_SCHEMA
from pypi_downloads.run import RunReport, run_once
from pypi_downloads.store import day_date


def counts_file(
    day: date,
    counts: dict[str, tuple[int, int]],
    *,
    null_project_rows: int = 0,
    summary: dict | bytes | None = None,
    raw_db: bytes | None = None,
    meta: dict[str, str] | None = None,
    **overrides,
) -> tuple[bytes, bytes]:
    """The summary JSON and the compressed database of one counted day, as the counter
    writes them. `counts` maps each project name to `(downloads, mirror_downloads)`.

    Rows are written as given, so a name need not be canonical. Each refusal test breaks
    one thing: `overrides` replace keys of the summary, `meta` replaces keys of the
    database's meta table, `summary` replaces the whole summary (a dict is encoded, bytes
    are used as they are), and `raw_db` replaces the database's bytes before they are
    compressed (the summary's digest and size then describe the replacement).
    """
    rows = sum(d for d, _ in counts.values()) + null_project_rows
    values = {
        "day": day.isoformat(),
        "rows_counted": rows,
        "rows_bigquery": rows,
        "partition_modified": f"{day.isoformat()}T05:00:00Z",
        "read_bytes": 1000,
        "null_project_rows": null_project_rows,
    }
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "day.db"
        if raw_db is None:
            conn = sqlite3.connect(path)
            conn.executescript(COUNTS_SCHEMA)
            stored = {k: str(v) for k, v in values.items()} | {
                "started_at": "2026-11-05T00:00:00Z",
                "finished_at": "2026-11-05T00:10:00Z",
                "streams": "4",
                "workers": "2",
            }
            conn.executemany(
                "INSERT INTO meta VALUES (?, ?)", (stored | (meta or {})).items()
            )
            conn.executemany(
                "INSERT INTO counts VALUES (?, ?, ?)",
                [(name, d, m) for name, (d, m) in counts.items()],
            )
            conn.commit()
            conn.close()
            raw_db = path.read_bytes()
    compressed = zstd.compress(raw_db)
    if summary is None:
        summary = (
            values
            | {
                "db_bytes": len(compressed),
                "db_sha256": hashlib.sha256(compressed).hexdigest(),
            }
            | overrides
        )
    if isinstance(summary, dict):
        summary = json.dumps(summary).encode()
    return summary, compressed


# ---- the runs of the stream, on a fake GitHub and a fake Google Cloud

CONFIG = load()
REPO = "py-observatory/data"
# The releases of the stream, and the assets that they hold, by name.
CURRENT = "pypi-downloads-current"
NOVEMBER = "pypi-downloads-month-2026-11"
DECEMBER = "pypi-downloads-month-2026-12"
MANIFEST = "manifest.json"
STATE = "pypi-downloads-state.db.zst"
DELTA = "pypi-downloads-delta-open.db.zst"
BASE_NOVEMBER = "pypi-downloads-state-base-2026-11.db.zst"
BASE_DECEMBER = "pypi-downloads-state-base-2026-12.db.zst"
PART_NOVEMBER = "pypi-downloads-2026-11.db.zst"
MANIFEST_NOVEMBER = "manifest-2026-11.json"
# What the fake counter reports reading for each day, from counts_file.
READ_BYTES = 1000
# What the workflow gives a job that may ask GitHub for an OIDC token, and the address
# that FakeGoogle serves it at.
OIDC_ENV = {
    "ACTIONS_ID_TOKEN_REQUEST_URL": "https://gh.example/oidc",
    "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "request-token",
}

# The connections that the tests open. SQLite warns about a connection that is collected
# while it is open, so `close_connections` closes what a test opened when it ends. It is
# autouse, so a test module that imports it has it run around each of its tests.
OPENED: list[sqlite3.Connection] = []


@pytest.fixture(autouse=True)
def close_connections() -> Iterator[None]:
    yield
    while OPENED:
        OPENED.pop().close()


def sealed_name(day: str) -> str:
    """The asset of the sealed delta of `day`, given as YYYY-MM-DD."""
    return f"pypi-downloads-delta-{day}.db.zst"


def no_sleep(seconds: float) -> None:
    """GitHub's retries and pacing wait for nothing in a test."""


class Clock:
    """A clock that moves only when it is moved, so no test waits for real."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def downloads_of(day: date) -> dict[str, tuple[int, int]]:
    """What the fake counter finds for `day`: two projects every day, one of them under
    a name that is not canonical, and a project first downloaded that day, with counts
    that follow the day."""
    n = day.toordinal() % 1000
    return {
        "requests": (1000 + n, 10),
        "NumPy": (500 + n, 0),
        f"new-{day:%Y%m%d}": (1, 0),
    }


def rows_of(day: date) -> int:
    """The rows of the partition of `day`, which the fake counter counts in full."""
    return sum(downloads for downloads, _ in downloads_of(day).values())


def execution(start: datetime, seconds: float, *, succeeded: bool = True) -> dict:
    """A finished Execution of the counter that ran for `seconds` from `start`, at the
    job's limits, as Cloud Run lists it."""
    limits = {"cpu": str(CONFIG.cpu), "memory": f"{CONFIG.memory_gib}Gi"}
    return {
        "startTime": instant(start),
        "completionTime": instant(start + timedelta(seconds=seconds)),
        "succeededCount": 1 if succeeded else 0,
        "failedCount": 0 if succeeded else 1,
        "template": {"containers": [{"resources": {"limits": limits}}]},
    }


class Hanging:
    """FakeGoogle as a transport, on which the execution of each day of `days` never
    finishes as far as its run can tell: its operation is never done. The job goes on
    regardless and writes its files, as when a run gives up waiting or is cancelled."""

    def __init__(self, fake: FakeGoogle) -> None:
        self.fake = fake
        self.days: set[date] = set()
        self._operations: set[str] = set()

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        _, _, path = url.partition("https://run.googleapis.com/v2/")
        if method == "GET" and path in self._operations:
            self.fake.requests.append((method, url))
            not_done = {"name": path, "done": False}
            return (
                200,
                {"content-type": "application/json"},
                json.dumps(not_done).encode(),
            )
        status, reply_headers, raw = self.fake.request(method, url, headers, body)
        if method == "POST" and path.endswith(":run") and _day_run(body) in self.days:
            self._operations.add(json.loads(raw)["name"])
        return status, reply_headers, raw


def _day_run(body: bytes | None) -> date | None:
    """The DAY that a request to run the counter gives it."""
    containers = json.loads(body or b"{}")["overrides"]["containerOverrides"]
    env = {item["name"]: item["value"] for item in containers[0]["env"]}
    return date.fromisoformat(env["DAY"]) if "DAY" in env else None


class Stream:
    """The runs of the stream, on one fake GitHub and one fake Google Cloud.

    Each run has a work directory of its own, as each run has a new runner, so all that
    a run leaves to the next goes through the releases. The clients are one for the
    whole test, as `main` makes one of each for a process.

    The fake counter takes `execution_seconds` on the clock for each day, and finds
    `downloads_of(day)`: it puts the day's counts file in the bucket and the day's
    partition in BigQuery's metadata, and finishes as an Execution that succeeded. A day
    of `fail` finishes as one that failed, with no file, and a day of `hang` is never
    seen to finish (see `Hanging`). `started` lists each day that an execution was
    started for, in order.
    """

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.github = FakeGitHub(REPO)
        self.gh = GitHub(REPO, "t0ken", transport=self.github, sleep=no_sleep)
        self.cloud = FakeGoogle()
        self.cloud.on_run = self._count
        self.transport = Hanging(self.cloud)
        self.clock = Clock()
        self.google = self.google_with(lambda: "sa-token")
        self.execution_seconds = 600.0
        self.fail: set[date] = set()
        self.started: list[str] = []
        self.signed_in_as: list[str] = []
        self.at = datetime(2026, 11, 1, tzinfo=UTC)
        self.run_began = 0.0
        self.runs = 0
        self.made = 0

    @property
    def hang(self) -> set[date]:
        return self.transport.days

    def google_with(
        self, token: Callable[[], str], *, renew: Callable[[], None] | None = None
    ) -> Google:
        """A Google client on the fake, which waits on the test's clock."""
        return Google(
            token,
            renew=renew,
            transport=self.transport,
            sleep=self.clock.sleep,
            clock=self.clock,
        )

    def _count(self, env: dict[str, str]) -> dict:
        day = date.fromisoformat(env["DAY"])
        self.started.append(env["DAY"])
        start = self.at + timedelta(seconds=self.clock.now - self.run_began)
        self.clock.now += self.execution_seconds
        if day in self.fail:
            return execution(start, self.execution_seconds, succeeded=False)
        self.put_counts(day)
        return execution(start, self.execution_seconds)

    def put_counts(self, day: date) -> None:
        """Put the counts file of `day` in the bucket, as the counter does, and the
        day's partition in BigQuery's metadata."""
        summary, db = counts_file(day, downloads_of(day))
        self.cloud.objects[(CONFIG.bucket, f"counts/{day}.db.zst")] = db
        self.cloud.objects[(CONFIG.bucket, f"counts/{day}.json")] = summary
        self.cloud.partitions[f"{day:%Y%m%d}"] = (rows_of(day), f"{day}T05:00:00Z")

    def notify(self, cost: float, *, at: str, month: str = "2026-11") -> None:
        """Queue a budget notification of `month`'s cost so far, published at `at`."""
        self.cloud.messages.append(
            {
                "budgetDisplayName": "py-observatory",
                "costAmount": cost,
                "costIntervalStart": f"{month}-01T07:00:00Z",
                "budgetAmount": 1.0,
                "budgetAmountType": "SPECIFIED_AMOUNT",
                "currencyCode": "USD",
            }
        )
        self.cloud.publish_time = at

    def begin(self, at: str) -> datetime:
        """Note that a run starts at `at`, a UTC time, for the times of its
        executions. Runs are far enough apart that the lease of every notification
        pulled and not acknowledged has run out, so Pub/Sub delivers it again."""
        self.cloud.expire_leases()
        self.at = datetime.fromisoformat(at)
        self.run_began = self.clock.now
        return self.at

    def run(
        self,
        at: str,
        *,
        bootstrap: bool = False,
        override_reason: str | None = None,
        max_seconds: float = 3600,
    ) -> RunReport:
        """A run at `at`, a UTC time."""
        return run_once(
            self.gh,
            self.google,
            self.workdir(),
            config=CONFIG,
            now=self.begin(at),
            bootstrap=bootstrap,
            override_reason=override_reason,
            max_seconds=max_seconds,
            clock=self.clock,
        )

    def main(self, monkeypatch: pytest.MonkeyPatch, at: str, *argv: str) -> int:
        """`main` with `argv`, as if at `at`, on the fakes. Its sign-in goes through
        FakeGoogle as a workflow's would, and each service account that it signs in as
        is added to `signed_in_as`."""

        def credentials(federation: Federation) -> Credentials:
            self.signed_in_as.append(federation.service_account)
            return Credentials(federation, transport=self.cloud, environ=OIDC_ENV)

        # run_once as imported here, not as the module holds it: an earlier call of
        # this method has put a stand-in there.
        def run_once_at(gh, google, workdir, **options) -> RunReport:
            options.update(now=self.begin(at), clock=self.clock)
            return run_once(gh, google, workdir, **options)

        monkeypatch.setattr(run_module, "github_from_env", lambda: self.gh)
        monkeypatch.setattr(run_module, "Credentials", credentials)
        monkeypatch.setattr(run_module, "Google", self.google_with)
        monkeypatch.setattr(run_module, "run_once", run_once_at)
        monkeypatch.chdir(self.tmp_path)
        return run_module.main(list(argv))

    def bootstrap(self, at: str = "2026-11-11T09:00:00Z") -> RunReport:
        """The stream's first run, at `at`."""
        return self.run(at, bootstrap=True)

    def workdir(self) -> Path:
        """A new, empty work directory, as each run has a new runner."""
        self.runs += 1
        path = self.tmp_path / f"run-{self.runs}"
        path.mkdir()
        return path

    def folder(self, label: str) -> Path:
        self.made += 1
        path = self.tmp_path / label / str(self.made)
        path.mkdir(parents=True)
        return path

    def manifest(self, tag: str = CURRENT, name: str = MANIFEST) -> Manifest:
        return loads(self.github.asset_bytes(tag, name).decode())

    def entries(self) -> dict[str, Asset]:
        """The assets that the current manifest lists, by name."""
        return {asset.name: asset for asset in self.manifest().assets}

    def assets(self, tag: str) -> list[str]:
        """The names of the assets of the release `tag`, sorted."""
        release = self.github.release(tag)
        return sorted(asset["name"] for asset in release["assets"]) if release else []

    def expand(self, tag: str, name: str) -> Path:
        """The database of the asset `name` of `tag`, expanded into a file."""
        packed = self.folder("published") / name
        packed.write_bytes(self.github.asset_bytes(tag, name))
        db = packed.with_suffix("")
        decompress_file(packed, db)
        return db

    def open(self, tag: str, name: str) -> sqlite3.Connection:
        """The database of the asset `name` of `tag`, opened to read."""
        conn = sqlite3.connect(f"{self.expand(tag, name).as_uri()}?mode=ro", uri=True)
        OPENED.append(conn)
        return conn


def counted_days(state: sqlite3.Connection) -> list[str]:
    """The days of the state, oldest first, as YYYY-MM-DD."""
    rows = state.execute("SELECT day FROM days ORDER BY day")
    return [day_date(day).isoformat() for (day,) in rows]


def counts_rows(conn: sqlite3.Connection) -> list[tuple[str, int, int, int]]:
    """The counts of a delta or a part, each as (day, project id, downloads, mirror
    downloads), in that order."""
    rows = conn.execute(
        "SELECT day, project_id, downloads, mirror_downloads FROM counts "
        "ORDER BY day, project_id"
    )
    return [(day_date(day).isoformat(), *rest) for day, *rest in rows]
