import ast
import sqlite3
from collections import Counter
from itertools import chain
from pathlib import Path

import pytest

from catalog.schema import JOINT, PER_STREAM, SERVING_DB, SERVING_SCHEMA, SOURCED
from obskit.sqlite import open_db

# Every table that the serving schema makes with CREATE TABLE, written out so that the
# schema is checked against a list and not against itself.
TABLES = {
    "input",
    "attribution",
    "project",
    "release",
    "change",
    "requirement_text",
    "requirement",
    "classifier",
    "project_classifier",
    "licence",
    "growth",
    "event",
    "advisory",
    "advisory_package",
    "affected",
    "download_month",
    "download_day",
    "download_last30",
    "download_top",
    "download_period",
    "download_break",
    "download_coverage",
}
# `search` is a virtual table. FTS5 keeps its index in four tables of its own, and adds
# a fifth, `search_content`, when it stores the text itself. Here the text is in `project`.
FTS_SHADOW_TABLES = {"search_data", "search_idx", "search_docsize", "search_config"}

# Every way that Python code reaches a network by itself. The catalog reaches GitHub
# through the kit's client (`obskit.github`), so that the pace and the retries that the
# kit sets apply to every call.
NETWORK_MODULES = frozenset({"urllib", "http", "socket", "ssl", "requests", "httpx"})
SOURCE = Path(__file__).resolve().parents[1] / "src"


@pytest.fixture
def serving_db(tmp_path):
    """A serving database, made from the schema as a build makes it."""
    con = open_db(tmp_path / SERVING_DB, SERVING_SCHEMA, create=True)
    yield con
    con.close()


def names(con: sqlite3.Connection, kind: str) -> set[str]:
    """The names of the tables, or of the views, that the database holds."""
    return {
        name
        for (name,) in con.execute(
            "SELECT name FROM sqlite_master WHERE type = ?", (kind,)
        )
    }


def imported_modules(source: str) -> set[str]:
    """The top-level package of each module that `source` imports, wherever in the code
    the import is. A relative import is of the catalog's own modules, so it is left out.
    Only the module that is named is read, so `from obskit import http` is an import of
    `obskit` and not of `http`."""
    modules = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules.update(alias.name.partition(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            modules.add(node.module.partition(".")[0])
    return modules


def test_serving_schema_makes_every_table(serving_db):
    assert names(serving_db, "table") == TABLES | {"search"} | FTS_SHADOW_TABLES
    assert names(serving_db, "view") == {"tombstone"}


def test_search_is_fts5_over_project(serving_db):
    serving_db.executemany(
        "INSERT INTO project (name, incarnation, status, first_seen, "
        "first_seen_exact, swept, releases, summary) "
        "VALUES (?, 1, 'active', '2026-01-01T00:00:00Z', 1, 1, 1, ?)",
        [
            ("arrayfun", "Fast arrays for science"),
            ("webframe", "A small web framework"),
        ],
    )
    serving_db.execute("INSERT INTO search(search) VALUES ('rebuild')")

    found = serving_db.execute(
        "SELECT name FROM search WHERE search MATCH 'arrays'"
    ).fetchall()

    assert found == [("arrayfun",)]


def test_every_table_has_one_owner(serving_db):
    # `search` is in none of the three: the build rebuilds it along with the joint tables.
    owned = Counter(chain(*SOURCED.values(), JOINT, PER_STREAM, ["search"]))

    assert owned == Counter(names(serving_db, "table") - FTS_SHADOW_TABLES)


def test_the_catalog_makes_no_network_calls_but_through_the_kit():
    files = sorted(SOURCE.rglob("*.py"))
    assert files, f"there is no code under {SOURCE}"

    for file in files:
        reached = imported_modules(file.read_text(encoding="utf-8")) & NETWORK_MODULES
        assert not reached, f"{file.name} imports {', '.join(sorted(reached))}"


@pytest.mark.parametrize(
    ("source", "modules"),
    [
        ("import urllib.request", {"urllib"}),
        ("from http.client import HTTPSConnection", {"http"}),
        ("import socket, ssl", {"socket", "ssl"}),
        ("import httpx as client", {"httpx"}),
        ("def fetch():\n    import requests\n", {"requests"}),
        ("from obskit.github import GitHub", {"obskit"}),
        ("from obskit import http", {"obskit"}),
        ("from .config import load_config", set()),
        ("# import httpx\ntext = 'import socket'", set()),
    ],
)
def test_imports_are_read_by_whole_module_name(source, modules):
    assert imported_modules(source) == modules
