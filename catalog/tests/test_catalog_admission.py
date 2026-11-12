"""One test for each table that catalog.toml admits: what the table becomes in the
serving database. Each stages its inputs and builds through `build_serving`, the path a
real build takes."""

import json
import sys
from datetime import datetime, timezone

import pytest

from test_catalog_downloads import counted, counts, coverage, day, member
from test_catalog_serving import (
    DAILY_YEARS,
    downloads_input,
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
    registry = registry_input(gh, tmp_path, projects, records, events)
    build_serving(out, [registry], None, daily_years=DAILY_YEARS)
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
    build_serving(out, [osv], None, daily_years=DAILY_YEARS)

    assert select(out, "SELECT advisory_id, id, kind, status, summary FROM advisory ORDER BY id") == [
        (1, "MAL-1", "malware", "removed", None),
        (2, "PYSEC-2", "vulnerability", "active", "Bad"),
    ]  # fmt: skip
    assert select(out, "SELECT advisory_id, name, versions FROM advisory_package") == [
        (2, "foo-bar", '["1.0"]')
    ]


def with_downloads(gh, tmp_path, *, state=None, serving=None, last_day="2026-11-10"):
    """Build a serving database from a registry of foo and bar and a downloads stream
    with these rows of its state and its serving database, in which foo is the stream's
    project 2 and bar its project 1, and return its path. The newest day that the
    stream counted is `last_day`, as its coverage says."""
    registry = registry_input(
        gh, tmp_path, [project("foo", "1.0"), project("bar", "1.0")], []
    )
    names = [{"project_id": 1, "name": "bar"}, {"project_id": 2, "name": "foo"}]
    downloads = downloads_input(
        gh,
        tmp_path,
        state={"projects": names, **(state or {})},
        serving=serving or {},
        coverage=coverage(
            first_day="2026-10-31",
            last_day=last_day,
            days=2,
            backfill_left=3935,
            projects=2,
            window_days=2,
        ),
    )
    out = tmp_path / "out.db"
    build_serving(out, [registry, downloads], None, daily_years=DAILY_YEARS)
    return out


def test_admits_pypi_downloads_projects(gh, tmp_path):
    # bar is the catalog's project 1 and foo its project 2, by name.
    out = with_downloads(
        gh,
        tmp_path,
        serving={"monthly": [counts(2, "month", 202611, 8, 3)]},
    )

    assert select(out, "SELECT project_id, month, downloads FROM download_month") == [
        (2, 202611, 8)
    ]
    assert select(out, "SELECT name FROM project WHERE project_id = 2") == [("foo",)]


def test_admits_pypi_downloads_days(gh, tmp_path):
    out = with_downloads(
        gh,
        tmp_path,
        state={"days": counted(day("2026-10-31"), day("2026-11-01"))},
        last_day="2026-11-01",
    )

    # The window is the 30 days up to the newest day counted.
    assert select(
        out,
        "SELECT launch, first_day, last_day, days, backfill_left, window_from, "
        "window_to, window_days FROM download_coverage",
    ) == [
        (
            "2026-11-01",
            "2026-10-31",
            "2026-11-01",
            2,
            3935,
            "2026-10-03",
            "2026-11-01",
            2,
        )
    ]


def test_admits_pypi_downloads_breaks(gh, tmp_path):
    breaks = [
        {"day": day("2018-07-26"), "note": "undercounted before"},
        {"day": day("2026-08-24"), "note": "no PEP 658 fetches"},
    ]
    out = with_downloads(gh, tmp_path, state={"breaks": breaks})

    assert select(out, "SELECT day, note FROM download_break ORDER BY day") == [
        ("2018-07-26", "undercounted before"),
        ("2026-08-24", "no PEP 658 fetches"),
    ]


def test_admits_pypi_downloads_monthly(gh, tmp_path):
    out = with_downloads(
        gh,
        tmp_path,
        serving={
            "monthly": [
                counts(1, "month", 202610, 5, 1),
                counts(2, "month", 202610, 9),
                counts(2, "month", 202611, 4, 2),
            ]
        },
    )

    assert select(out, "SELECT * FROM download_month ORDER BY project_id, month") == [
        (1, 202610, 5, 1),
        (2, 202610, 9, 0),
        (2, 202611, 4, 2),
    ]


def test_admits_pypi_downloads_months(gh, tmp_path):
    months = [{"month": 202610, "days": 1, "downloads": 14, "mirror_downloads": 1}]
    out = with_downloads(gh, tmp_path, serving={"months": months})

    # October has one of its 31 days.
    assert select(out, "SELECT * FROM download_period") == [(202610, 1, 31, 14, 1)]


def test_admits_pypi_downloads_top_set(gh, tmp_path):
    members = [member(2027, 2, "foo", 202612, 1), member(2027, 1, "bar", 202701, 9)]
    out = with_downloads(gh, tmp_path, serving={"top_set": members})

    assert select(out, "SELECT * FROM download_top ORDER BY project_id") == [
        (2027, 1, 202701, 9),
        (2027, 2, 202612, 1),
    ]


def test_admits_pypi_downloads_daily(gh, tmp_path):
    out = with_downloads(
        gh,
        tmp_path,
        state={"days": counted(day("2026-11-01"))},
        serving={"daily": [counts(2, "day", day("2026-11-01"), 6, 1)]},
        last_day="2026-11-01",
    )

    assert select(out, "SELECT * FROM download_day") == [(2, day("2026-11-01"), 6, 1)]


def test_admits_pypi_downloads_recent(gh, tmp_path):
    newest = day("2026-11-01")
    out = with_downloads(
        gh,
        tmp_path,
        state={"days": counted(newest - 1, newest)},
        serving={
            "recent": [
                counts(1, "day", newest - 1, 3),
                counts(1, "day", newest, 4),
                counts(2, "day", newest, 9, 1),
            ]
        },
        last_day="2026-11-01",
    )

    assert select(out, "SELECT * FROM download_last30 ORDER BY rank") == [
        (2, 9, 1, 1),
        (1, 7, 0, 2),
    ]


def test_every_admitted_table_has_its_test():
    module = sys.modules[__name__]
    for stream, admission in load_config().streams.items():
        for table in admission.tables:
            name = f"test_admits_{stream.replace('-', '_')}_{table}"
            assert callable(getattr(module, name, None)), name
