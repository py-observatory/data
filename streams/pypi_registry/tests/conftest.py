"""The fixtures that every test of the stream shares."""

from collections.abc import Iterator

import pytest

from opened_connections import OPENED


@pytest.fixture(autouse=True)
def close_connections() -> Iterator[None]:
    """Close the connections that a test opened, when it ends. SQLite warns about a
    connection that is collected while it is open."""
    yield
    while OPENED:
        OPENED.pop().close()


@pytest.fixture(autouse=True)
def no_sqlite_tmpdir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start each test with no SQLITE_TMPDIR, and put back what was there when it ends.
    `main` sets it for the rest of the process, which would carry it into later
    tests."""
    monkeypatch.setenv("SQLITE_TMPDIR", "")  # so that the original is put back
    monkeypatch.delenv("SQLITE_TMPDIR")
