import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from obskit import smoke
from obskit.github import GitHub, Transient
from obskit.manifest import loads
from obskit.release import PriorStateError
from obskit.testing import FakeGitHub
from obskit.zst import decompress_file

REPO = "py-observatory/data"
TOKEN = "t0ken"
TAG = "kit-smoke-current"
STATE = "kit-smoke-state.db.zst"


def no_sleep(seconds: float) -> None:
    pass


@pytest.fixture
def fake() -> FakeGitHub:
    return FakeGitHub(REPO)


@pytest.fixture
def gh(fake: FakeGitHub) -> GitHub:
    return GitHub(REPO, TOKEN, transport=fake, sleep=no_sleep)


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    path = tmp_path / "work"
    path.mkdir()
    return path


def counter_held(fake: FakeGitHub, tmp_path: Path, label: str) -> list[tuple]:
    """The rows of the counter table in the state that the release holds now."""
    packed = tmp_path / f"{label}.db.zst"
    packed.write_bytes(fake.asset_bytes(TAG, STATE))
    db = tmp_path / f"{label}.db"
    decompress_file(packed, db)
    with closing(sqlite3.connect(db)) as con:
        return con.execute("SELECT n FROM counter").fetchall()


def writes(fake: FakeGitHub) -> list[tuple[str, str]]:
    return [(method, url) for method, url, _ in fake.calls if method != "GET"]


def fresh(tmp_path: Path, label: str) -> Path:
    path = tmp_path / label
    path.mkdir()
    return path


# --- run ---


def test_smoke_round_trip_on_fake(gh, fake, tmp_path):
    assert smoke.run(gh, fresh(tmp_path, "first"), bootstrap=True) == 0
    assert counter_held(fake, tmp_path, "after-first") == [(1,)]

    assert smoke.run(gh, fresh(tmp_path, "second"), bootstrap=False) == 0
    assert counter_held(fake, tmp_path, "after-second") == [(2,)]

    assert smoke.run(gh, fresh(tmp_path, "third"), bootstrap=False) == 0
    assert counter_held(fake, tmp_path, "after-third") == [(3,)]


def test_smoke_publishes_the_kit_smoke_current_release(gh, fake, tmp_path):
    smoke.run(gh, fresh(tmp_path, "first"), bootstrap=True)

    release = fake.release(TAG)
    assert release["prerelease"] is True
    assert release["make_latest"] == "false"  # only catalog-current may be Latest
    assert sorted(asset["name"] for asset in release["assets"]) == [
        STATE,
        "manifest.json",
    ]
    manifest = loads(fake.asset_bytes(TAG, "manifest.json").decode())
    assert manifest.stream == "kit-smoke"
    assert [asset.name for asset in manifest.assets] == [STATE]
    assert manifest.assets[0].tag == TAG
    assert manifest.producer["repo"] == REPO


def test_smoke_without_bootstrap_needs_the_release(gh, fake, tmp_path):
    with pytest.raises(PriorStateError):
        smoke.run(gh, fresh(tmp_path, "first"), bootstrap=False)

    assert fake.release(TAG) is None
    assert writes(fake) == []


def test_smoke_bootstrap_is_refused_once_the_stream_has_published(gh, fake, tmp_path):
    smoke.run(gh, fresh(tmp_path, "first"), bootstrap=True)
    fake.calls.clear()

    with pytest.raises(PriorStateError):
        smoke.run(gh, fresh(tmp_path, "second"), bootstrap=True)

    assert writes(fake) == []
    assert counter_held(fake, tmp_path, "after") == [(1,)]


def test_a_502_reading_the_release_is_not_read_as_no_release(gh, fake, tmp_path):
    # Even with --bootstrap, which is what a 404 would let through.
    fake.fail("GET", f"/releases/tags/{TAG}", 502, times=6)

    with pytest.raises(Transient):
        smoke.run(gh, fresh(tmp_path, "first"), bootstrap=True)

    assert fake.release(TAG) is None
    assert writes(fake) == []


def test_smoke_fails_when_the_release_read_back_is_not_what_it_published(
    fake, gh, tmp_path, capsys
):
    """The run reads the release back from GitHub after publishing. Here another
    publisher swaps its own state in between, which a run that only checked its own
    files would not notice."""

    class Intruder:
        """The fake API, but once the run has swapped its files in, a second run of
        the smoke test publishes before the first reads the release back."""

        def __init__(self) -> None:
            self.renamed = False
            self.intruded = False

        def request(self, method, url, headers, body):
            self.renamed = self.renamed or method == "PATCH"
            reading = method == "GET" and "/releases/tags/" in url
            if self.renamed and reading and not self.intruded:
                self.intruded = True
                other = GitHub(REPO, TOKEN, transport=fake, sleep=no_sleep)
                assert smoke.run(other, fresh(tmp_path, "other"), bootstrap=False) == 0
            return fake.request(method, url, headers, body)

    smoke.run(gh, fresh(tmp_path, "first"), bootstrap=True)
    racing = Intruder()
    victim = GitHub(REPO, TOKEN, transport=racing, sleep=no_sleep)

    assert smoke.run(victim, fresh(tmp_path, "second"), bootstrap=False) == 1

    assert racing.intruded
    assert "read back" in capsys.readouterr().err


# --- main ---


class Recorder:
    """Stands in for GitHub in `main`, and for `run`: records how they are called."""

    def __init__(self) -> None:
        self.clients: list[tuple] = []
        self.runs: list[tuple] = []
        self.answer = 0

    def client(self, *args):
        self.clients.append(args)
        return ("client", len(self.clients))

    def run(self, gh, workdir: Path, *, bootstrap: bool) -> int:
        self.runs.append((gh, workdir, workdir.is_dir(), bootstrap))
        return self.answer


@pytest.fixture
def recorder(monkeypatch, tmp_path) -> Recorder:
    recorder = Recorder()
    monkeypatch.setattr(smoke, "GitHub", recorder.client)
    monkeypatch.setattr(smoke, "run", recorder.run)
    monkeypatch.setenv("GITHUB_TOKEN", TOKEN)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.chdir(tmp_path)
    return recorder


def test_main_makes_one_client_and_hands_it_to_run(recorder, monkeypatch, tmp_path):
    monkeypatch.setenv("GITHUB_REPOSITORY", "someone/else")

    assert smoke.main(["--bootstrap"]) == 0

    assert recorder.clients == [("someone/else", TOKEN)]
    [(gh, workdir, existed, bootstrap)] = recorder.runs
    assert gh == ("client", 1)
    assert bootstrap is True
    # The run's directory is new, under ./work, and is gone afterwards.
    assert existed
    assert workdir.parent == tmp_path / "work"
    assert not workdir.exists()


def test_main_bootstraps_only_when_asked_and_publishes_here_by_default(recorder):
    assert smoke.main([]) == 0

    assert recorder.clients == [("py-observatory/data", TOKEN)]
    assert [run[3] for run in recorder.runs] == [False]


def test_main_returns_what_run_returns(recorder):
    recorder.answer = 1

    assert smoke.main([]) == 1
