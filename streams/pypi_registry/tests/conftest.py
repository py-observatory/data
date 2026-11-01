"""The fixtures that every test of the stream shares."""

from collections.abc import Iterator

import pytest

from run_helpers import OPENED


@pytest.fixture(autouse=True)
def close_connections() -> Iterator[None]:
    """Close the connections that a test opened, when it ends. SQLite warns about a
    connection that is collected while it is open."""
    yield
    while OPENED:
        OPENED.pop().close()
