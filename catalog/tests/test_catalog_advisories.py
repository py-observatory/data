import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from packaging.version import Version as V

from catalog.advisories import affects, build_advisories, expand_affected
from catalog.config import load_config
from catalog.inputs import needed_assets
from catalog.registry import build_registry
from catalog.schema import SERVING_DB, SERVING_SCHEMA
from catalog.stage import stage_osv, stage_registry
from catalog_helpers import REPO, TOKEN, no_sleep, publish_osv, publish_registry
from obskit.github import GitHub
from obskit.sqlite import open_db
from obskit.testing import FakeGitHub

REGISTRY = load_config().streams["pypi-registry"]
OSV = load_config().streams["osv-pypi"]


@pytest.fixture
def gh() -> GitHub:
    return GitHub(REPO, TOKEN, transport=FakeGitHub(REPO), sleep=no_sleep)


def dumps(value) -> str:
    return json.dumps(value, separators=(",", ":"))


def advisory(id: str, entries: list[dict] | None = None, **columns) -> dict:
    """A row of osv's advisories. `entries` become its `affected`, as compact JSON."""
    row = {"id": id, **columns}
    if entries is not None:
        row["affected"] = dumps(entries)
    return row


def entry(name: str, events: list[dict] | None = None, versions=(), **columns) -> dict:
    """An `affected` entry of `name`, with one ECOSYSTEM range of `events`."""
    ranges = [{"type": "ECOSYSTEM", "events": events}] if events else []
    return {"name": name, "ranges": ranges, "versions": list(versions), **columns}


def build(
    gh: GitHub,
    tmp_path: Path,
    advisories: list[dict],
    releases: dict[str, list[str]] | None = None,
) -> Path:
    """Build a serving database from a registry that has `releases`, as a project's name
    with its versions, and an osv that has `advisories`; run `build_advisories` and
    `expand_affected` on it, and return its path."""
    state = {
        "projects": [{"name": name} for name in releases or {}],
        "versions_seen": [
            {"name": name, "version": version}
            for name, versions in (releases or {}).items()
            for version in versions
        ],
    }
    registry = publish_registry(gh, state=state, open_day="2026-11-02", open_delta={})
    osv = publish_osv(gh, advisories=advisories, open_day="2026-11-02")
    work = tmp_path / "work"
    work.mkdir()
    registry_stage = stage_registry(gh, needed_assets(registry, REGISTRY.tables), work)
    osv_stage = stage_osv(gh, needed_assets(osv, OSV.tables), work)
    db = tmp_path / SERVING_DB
    with closing(open_db(db, SERVING_SCHEMA, create=True)) as con:
        build_registry(con, registry_stage)
        build_advisories(con, osv_stage)
        expand_affected(con)
        con.commit()
    return db


def select(db: Path, query: str) -> list[tuple]:
    with closing(sqlite3.connect(db)) as con:
        return con.execute(query).fetchall()


def affected(db: Path) -> list[tuple]:
    return select(
        db,
        "SELECT p.name, a.version, d.id FROM affected AS a "
        "JOIN project AS p USING (project_id) JOIN advisory AS d USING (advisory_id) "
        "ORDER BY p.name, a.version, d.id",
    )


def test_advisories_in_id_order_with_their_fields(gh, tmp_path):
    db = build(
        gh,
        tmp_path,
        [
            advisory(
                "PYSEC-2026-2",
                published="2026-10-02T00:00:00Z",
                summary="Second",
                aliases='["CVE-2026-2"]',
                related='["GHSA-x"]',
                severity='[{"type":"CVSS_V3","score":"x"}]',
                refs='[{"type":"WEB","url":"https://example.org"}]',
                database_specific='{"a":1}',
            ),
            advisory("MAL-2026-1", kind="malware", status="removed"),
        ],
    )

    assert select(
        db,
        "SELECT advisory_id, id, kind, status, published, modified, withdrawn, summary, "
        "aliases, related, severity, refs, database_specific "
        "FROM advisory ORDER BY advisory_id",
    ) == [
        (
            1,
            "MAL-2026-1",
            "malware",
            "removed",
            None,
            "2026-10-01T00:00:00Z",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            2,
            "PYSEC-2026-2",
            "vulnerability",
            "active",
            "2026-10-02T00:00:00Z",
            "2026-10-01T00:00:00Z",
            None,
            "Second",
            '["CVE-2026-2"]',
            '["GHSA-x"]',
            '[{"type":"CVSS_V3","score":"x"}]',
            '[{"type":"WEB","url":"https://example.org"}]',
            '{"a":1}',
        ),
    ]


def test_entries_for_one_project_merge(gh, tmp_path):
    first = {"type": "ECOSYSTEM", "events": [{"introduced": "0"}, {"fixed": "1.0"}]}
    second = {"type": "ECOSYSTEM", "events": [{"introduced": "2.0"}, {"fixed": "2.5"}]}
    db = build(
        gh,
        tmp_path,
        [
            advisory(
                "PYSEC-2026-1",
                [
                    {"name": "tornado", "ranges": [first], "versions": ["0.5", "1.0"]},
                    {"name": "other", "ranges": [], "versions": ["9"]},
                    {"name": "tornado", "ranges": [second], "versions": ["1.0", "2.0"]},
                ],
            )
        ],
    )

    # Names are in order of first appearance, and each version is there once.
    assert select(
        db, "SELECT advisory_id, name, ranges, versions FROM advisory_package"
    ) == [
        (1, "other", "[]", '["9"]'),
        (1, "tornado", dumps([first, second]), '["0.5","1.0","2.0"]'),
    ]


@pytest.mark.parametrize(
    ("events", "version", "expected"),
    [
        ([{"introduced": "0"}, {"fixed": "2.0"}], "1.5", True),
        ([{"introduced": "0"}, {"fixed": "2.0"}], "2.0rc1", True),
        ([{"introduced": "0"}, {"fixed": "2.0"}], "2.0", False),
        (
            [
                {"introduced": "1.0"},
                {"fixed": "1.5"},
                {"introduced": "2.0"},
                {"fixed": "2.5"},
            ],
            "1.7",
            False,
        ),
        (
            [
                {"introduced": "1.0"},
                {"fixed": "1.5"},
                {"introduced": "2.0"},
                {"fixed": "2.5"},
            ],
            "2.2",
            True,
        ),
        ([{"introduced": "0"}, {"last_affected": "1.9"}], "1.9", True),
        ([{"introduced": "0"}, {"last_affected": "1.9"}], "2.0", False),
        ([{"fixed": "2.0"}, {"introduced": "1.0"}], "1.5", True),
        ([{"introduced": "0"}], "99", True),
        ([{"introduced": "0"}, {"limit": "3.0"}], "3.1", False),
        ([{"introduced": "not.a.version!"}], "1.0", False),
    ],
)
def test_affects(events, version, expected):
    assert affects(V(version), [{"type": "ECOSYSTEM", "events": events}]) is expected


def test_affects_ignores_git_ranges():
    assert (
        affects(V("1.0"), [{"type": "GIT", "events": [{"introduced": "0"}]}]) is False
    )


def test_affected_takes_listed_versions_and_versions_in_range(gh, tmp_path):
    db = build(
        gh,
        tmp_path,
        [
            advisory(
                "PYSEC-2026-1",
                [entry("tornado", [{"introduced": "0"}], versions=["1.0"])],
            )
        ],
        {"tornado": ["1.0", "1.1"]},
    )

    assert affected(db) == [
        ("tornado", "1.0", "PYSEC-2026-1"),
        ("tornado", "1.1", "PYSEC-2026-1"),
    ]


def test_ranges_leave_out_the_versions_they_fix(gh, tmp_path):
    db = build(
        gh,
        tmp_path,
        [
            advisory(
                "PYSEC-2026-1",
                [entry("tornado", [{"introduced": "0"}, {"fixed": "1.1"}])],
            )
        ],
        {"tornado": ["1.0", "1.1"], "other": ["1.0"]},
    )

    assert affected(db) == [("tornado", "1.0", "PYSEC-2026-1")]


def test_unparseable_versions_are_affected_only_when_listed(gh, tmp_path):
    entries = [entry("pytz", [{"introduced": "0"}])]
    db = build(
        gh,
        tmp_path,
        [
            advisory("PYSEC-2026-1", entries),
            advisory("PYSEC-2026-2", [entry("pytz", versions=["2004d"])]),
        ],
        {"pytz": ["2004d"]},
    )

    assert affected(db) == [("pytz", "2004d", "PYSEC-2026-2")]


def test_listed_versions_match_in_canonical_form(gh, tmp_path):
    db = build(
        gh,
        tmp_path,
        [advisory("PYSEC-2026-1", [entry("tornado", versions=["1.0.0-RC1"])])],
        {"tornado": ["1.0.0rc1", "1.0.0"]},
    )

    assert affected(db) == [("tornado", "1.0.0rc1", "PYSEC-2026-1")]


def test_withdrawn_and_removed_advisories_affect_nothing(gh, tmp_path):
    entries = [entry("tornado", [{"introduced": "0"}])]
    db = build(
        gh,
        tmp_path,
        [
            advisory("PYSEC-2026-1", entries, withdrawn="2026-10-05T00:00:00Z"),
            advisory("PYSEC-2026-2", entries, status="removed"),
        ],
        {"tornado": ["1.0"]},
    )

    assert affected(db) == []
    assert select(db, "SELECT id FROM advisory ORDER BY id") == [
        ("PYSEC-2026-1",),
        ("PYSEC-2026-2",),
    ]


def test_an_advisory_for_an_unknown_project_keeps_its_package_row(gh, tmp_path):
    db = build(
        gh,
        tmp_path,
        [advisory("PYSEC-2026-1", [entry("nowhere", [{"introduced": "0"}])])],
        {"tornado": ["1.0"]},
    )

    assert select(db, "SELECT name FROM advisory_package") == [("nowhere",)]
    assert affected(db) == []


def test_expand_affected_returns_the_rows_it_wrote(gh, tmp_path):
    db = build(
        gh,
        tmp_path,
        [advisory("PYSEC-2026-1", [entry("tornado", [{"introduced": "0"}])])],
        {"tornado": ["1.0", "2.0"]},
    )
    with closing(sqlite3.connect(db)) as con:
        con.execute("DELETE FROM affected")
        assert expand_affected(con) == 2
