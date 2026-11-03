import os

import pytest

from obskit import runs
from obskit.github import GitHub
from obskit.runs import REPOSITORY, github_from_env, run_directory


@pytest.fixture(autouse=True)
def no_sqlite_tmpdir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start each test with no SQLITE_TMPDIR, and put back what was there when it ends.
    `run_directory` sets it for the rest of the process, which would carry it into
    later tests."""
    monkeypatch.setenv("SQLITE_TMPDIR", "")  # so that the original is put back
    monkeypatch.delenv("SQLITE_TMPDIR")


# --- github_from_env ---


def test_github_from_env_names_the_repository(monkeypatch):
    made: list[tuple[str, str]] = []

    class Client(GitHub):
        """The kit's client, which also records what it was made with."""

        def __init__(self, repo: str, token: str) -> None:
            super().__init__(repo, token)
            made.append((repo, token))

    monkeypatch.setattr(runs, "GitHub", Client)
    monkeypatch.setenv("GITHUB_TOKEN", "t")
    monkeypatch.setenv("GITHUB_REPOSITORY", "a/b")

    assert github_from_env().repo == "a/b"

    monkeypatch.delenv("GITHUB_REPOSITORY")

    assert github_from_env().repo == REPOSITORY
    assert REPOSITORY == "py-observatory/data"
    assert made == [("a/b", "t"), (REPOSITORY, "t")]


def test_github_from_env_needs_a_token(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    with pytest.raises(KeyError, match="GITHUB_TOKEN"):
        github_from_env()


# --- run_directory ---


def test_run_directory_is_made_in_base_and_removed(tmp_path):
    base = tmp_path / "work"

    with run_directory(base) as workdir:
        assert base.is_dir(), "the base is made when it is missing"
        assert workdir.parent == base
        assert workdir.name.startswith("run-")
        assert list(workdir.iterdir()) == [], "the directory is new"
        (workdir / "state.db").write_bytes(b"some bytes")

    assert not workdir.exists()
    assert list(base.iterdir()) == [], "only the run's own directory goes"

    with (
        pytest.raises(RuntimeError, match="the run failed"),
        run_directory(base) as failed,
    ):
        assert failed.is_dir()
        raise RuntimeError("the run failed")

    assert not failed.exists(), "it goes after an error too"
    assert list(base.iterdir()) == []


def test_run_directory_keeps_sqlite_temp_files_on_its_disk(tmp_path, monkeypatch):
    base = tmp_path / "work"

    with run_directory(base):
        assert os.environ["SQLITE_TMPDIR"] == str(base.resolve())

    # A folder that is set already is kept.
    chosen = str(tmp_path / "chosen")
    monkeypatch.setenv("SQLITE_TMPDIR", chosen)

    with run_directory(base):
        assert os.environ["SQLITE_TMPDIR"] == chosen
