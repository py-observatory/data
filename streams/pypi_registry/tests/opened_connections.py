"""The connections that the tests of the stream open, and that conftest.py closes.

SQLite warns about a connection that is collected while it is open. A test that opens
one keeps it in OPENED, and the fixture in conftest.py closes all of them when the test
ends. This module imports nothing of the stream, so conftest.py, which every test of the
stream loads, does not depend on any of the stream's code importing cleanly.
"""

import sqlite3

OPENED: list[sqlite3.Connection] = []
