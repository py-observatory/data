"""One test for each table that catalog.toml admits: what the table becomes in the
serving database. Each stages its inputs and builds through `build_serving`, the path a
real build takes."""

import json
import sys
from datetime import datetime, timezone

import pytest

from test_catalog_serving import (
    event,
    osv_input,
    project,
    record,
    registry_input,
    select,
)

from catalog.config import load_config
from catalog.serving import build_serving
from catalog_helpers import REPO, TOKEN, no_sleep
from obskit.github import GitHub
from obskit.testing import FakeGitHub


@pytest.fixture
def gh() -> GitHub:
    return GitHub(REPO, TOKEN, transport=FakeGitHub(REPO), sleep=no_sleep)


def built(gh, tmp_path, projects, records=(), events=()):
    """Build a serving database from a registry of these rows, and return its path."""
    out = tmp_path / "out.db"
    build_serving(out, [registry_input(gh, tmp_path, projects, records, events)], None)
    return out


def unix(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp())


def test_admits_pypi_registry_projects(gh, tmp_path):
    zed = project("zed", "1.0")
    zed["projects"][0].update(
        incarnation=2, status="archived", first_seen="2026-09-01T00:00:00Z",
        first_seen_exact=1,
    )  # fmt: skip
    out = built(gh, tmp_path, [zed, project("abc", "1.0", status="removed")])

    assert select(
        out,
        "SELECT project_id, name, incarnation, status, first_seen, first_seen_exact "
        "FROM project ORDER BY project_id",
    ) == [
        (1, "abc", 1, "removed", "2026-10-01T00:00:00Z", 0),
        (2, "zed", 2, "archived", "2026-09-01T00:00:00Z", 1),
    ]


def test_admits_pypi_registry_versions_seen(gh, tmp_path):
    rows = project("foo", "1.0", "2.0")
    rows["versions_seen"][1]["record"] = 0
    out = built(gh, tmp_path, [rows], [record("foo", "1.0")])

    assert select(out, "SELECT version, has_record FROM release ORDER BY version") == [
        ("1.0", 1),
        ("2.0", 0),
    ]


def test_admits_pypi_registry_files_current(gh, tmp_path):
    # 1.0 has its one file yanked, 2.0 has two files and one is yanked, and 3.0 has lost
    # its file, so that only the delta remembers it.
    rows = project("foo", "1.0", "2.0", "3.0")
    rows["files"].append({**rows["files"][1], "filename": "foo-2.0.whl"})
    now = [
        {"name": "foo", "filename": "foo-1.0.tar.gz", "yanked": 1, "yanked_reason": "broken"},
        {"name": "foo", "filename": "foo-2.0.tar.gz", "yanked": 0},
        {"name": "foo", "filename": "foo-2.0.whl", "yanked": 1, "yanked_reason": "x"},
    ]  # fmt: skip
    rows["files_current"] = now
    out = built(gh, tmp_path, [rows])

    assert select(
        out,
        "SELECT version, files, yanked, yanked_reason FROM release ORDER BY version",
    ) == [("1.0", 1, 1, "broken"), ("2.0", 2, 0, None), ("3.0", 0, 0, None)]
    assert select(out, "SELECT releases, latest_version FROM project") == [(2, "2.0")]


def test_admits_pypi_registry_files(gh, tmp_path):
    rows = project("foo", "1.0")
    rows["files"][0]["upload_time"] = "2026-10-05T12:30:45.123456Z"
    rows["files"].append(
        {
            **rows["files"][0],
            "filename": "foo-1.0.whl",
            "upload_time": "2026-10-04T01:02:03Z",
        }
    )
    rows["files_current"].append(
        {"name": "foo", "filename": "foo-1.0.whl", "yanked": 0}
    )
    out = built(gh, tmp_path, [rows])

    earliest = unix("2026-10-04T01:02:03")
    assert select(out, "SELECT version, uploaded, files FROM release") == [
        ("1.0", earliest, 2)
    ]
    assert select(out, "SELECT first_uploaded FROM project") == [(earliest,)]
    assert select(out, "SELECT month, projects, releases FROM growth") == [
        ("2026-10", 1, 1)
    ]


def test_admits_pypi_registry_release_records(gh, tmp_path):
    out = built(
        gh,
        tmp_path,
        [project("foo", "1.0", "1.1")],
        [
            record("foo", "1.0", summary="old", license_expression="MIT",
                   requires_dist='["bar>=1"]', classifiers='["A :: B"]'),
            record("foo", "1.1", summary="new", license_expression="MIT",
                   requires_dist='["bar>=1"]', classifiers='["A :: B"]'),
        ],
    )  # fmt: skip

    assert select(out, "SELECT record_version, summary, license_expression FROM project") == [
        ("1.1", "new", "MIT")
    ]  # fmt: skip
    assert select(out, "SELECT version, field, value, compared_with FROM change"
                  " WHERE field = 'summary' ORDER BY version") == [
        ("1.0", "summary", "old", None),
        ("1.1", "summary", "new", "1.0"),
    ]  # fmt: skip
    assert select(out, "SELECT text FROM requirement_text") == [("bar>=1",)]
    assert select(out, "SELECT depends_on FROM requirement") == [("bar",)]
    assert select(out, "SELECT text, projects FROM classifier") == [("A :: B", 1)]
    assert select(out, "SELECT licence, projects FROM licence") == [("MIT", 1)]


def test_admits_pypi_registry_events(gh, tmp_path):
    out = built(
        gh,
        tmp_path,
        [project("foo", "1.0", status="removed")],
        events=[
            event("foo", "created", "2026-10-01T00:00:00Z"),
            event("foo", "removed", "2026-10-09T00:00:00Z"),
        ],
    )

    assert select(out, "SELECT name, incarnation, kind, observed_at FROM event ORDER BY seq") == [
        ("foo", 1, "created", "2026-10-01T00:00:00Z"),
        ("foo", 1, "removed", "2026-10-09T00:00:00Z"),
    ]  # fmt: skip
    assert select(out, "SELECT status_since FROM project") == [
        ("2026-10-09T00:00:00Z",)
    ]


def test_admits_osv_pypi_advisories(gh, tmp_path):
    entry = {
        "name": "foo-bar",
        "ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "0"}]}],
        "versions": ["1.0"],
    }
    out = tmp_path / "out.db"
    osv = osv_input(
        gh,
        tmp_path,
        [
            {"id": "PYSEC-2", "affected": json.dumps([entry]), "summary": "Bad"},
            {"id": "MAL-1", "kind": "malware", "status": "removed"},
        ],
    )
    build_serving(out, [osv], None)

    assert select(out, "SELECT advisory_id, id, kind, status, summary FROM advisory ORDER BY id") == [
        (1, "MAL-1", "malware", "removed", None),
        (2, "PYSEC-2", "vulnerability", "active", "Bad"),
    ]  # fmt: skip
    assert select(out, "SELECT advisory_id, name, versions FROM advisory_package") == [
        (2, "foo-bar", '["1.0"]')
    ]


def test_every_admitted_table_has_its_test():
    module = sys.modules[__name__]
    for stream, admission in load_config().streams.items():
        for table in admission.tables:
            name = f"test_admits_{stream.replace('-', '_')}_{table}"
            assert callable(getattr(module, name, None)), name
