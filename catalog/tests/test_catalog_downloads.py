"""The download tables: what the catalog makes of the downloads stream's state and
serving database, linked to the registry's projects by name."""

from datetime import date
from pathlib import Path

import pytest

from test_catalog_serving import (
    DAILY_YEARS,
    SECOND,
    carried,
    downloads_input,
    project,
    registry_input,
    select,
)

from catalog.schema import SOURCED
from catalog.serving import build_serving
from catalog_helpers import REPO, TOKEN, no_sleep
from obskit.github import GitHub
from obskit.testing import FakeGitHub
from pypi_downloads.store import day_number

# The newest day that the stream has counted, in the tests that do not say.
NEWEST = date(2026, 11, 10)


@pytest.fixture
def gh() -> GitHub:
    return GitHub(REPO, TOKEN, transport=FakeGitHub(REPO), sleep=no_sleep)


def day(text: str) -> int:
    """The stream's number for the day `text`, YYYY-MM-DD."""
    return day_number(date.fromisoformat(text))


def counts(
    project_id: int, key: str, at: int, downloads: int, mirrors: int = 0
) -> dict:
    """A row of the stream's monthly, daily or recent counts, whose `key` (month or day)
    is `at`."""
    return {
        "project_id": project_id,
        key: at,
        "downloads": downloads,
        "mirror_downloads": mirrors,
    }


def member(year: int, project_id: int, name: str, entered: int, rank: int) -> dict:
    """A row of the stream's top_set: a project of `year`'s set, which entered it at the
    close of the month `entered` with `rank`."""
    return {
        "year": year,
        "project_id": project_id,
        "name": name,
        "entered": entered,
        "rank": rank,
    }


def coverage(**changed: object) -> dict:
    """The coverage of the downloads stream's manifest, as its run writes it, with the
    keys in `changed` changed: launched on 2026-11-01 and counted back to 2026-10-01,
    with no day missing, so 41 days up to NEWEST."""
    return {
        "launch": "2026-11-01",
        "first_day": "2026-10-01",
        "last_day": NEWEST.isoformat(),
        "days": 41,
        "forward_missing": 0,
        "backfill_left": (date(2026, 10, 1) - date(2016, 1, 22)).days,
        "projects": 3,
        "window_days": 30,
        "top_year": None,
        "top_set": 0,
        **changed,
    }


def counted(*days: int) -> list[dict]:
    """The state's days for each of `days`, which the serving database has folded."""
    return [{"day": number, "seq": seq} for seq, number in enumerate(days, start=1)]


def registry_of(gh: GitHub, tmp_path: Path, *names: str, on: str = SECOND):
    """The registry, staged, with a project of each of `names`."""
    return registry_input(
        gh, tmp_path, [project(name, "1.0") for name in names], [], day=on
    )


def build(tmp_path: Path, inputs, previous=None, *, name="out.db", years=DAILY_YEARS):
    """Build a serving database from `inputs`, and return its path."""
    out = tmp_path / name
    build_serving(out, inputs, previous, daily_years=years)
    return out


def test_monthly_totals_link_to_registry_projects_by_name(gh, tmp_path):
    # The catalog numbers its projects in name order, and the stream in the order that
    # it first met them, so the two numbers of a project differ.
    registry = registry_of(gh, tmp_path, "alpha", "beta", "gamma")
    downloads = downloads_input(
        gh,
        tmp_path,
        state={
            "projects": [
                {"project_id": 1, "name": "gamma"},
                {"project_id": 2, "name": "alpha"},
            ],
            "days": counted(day("2026-10-31"), day("2026-11-01")),
        },
        serving={
            "monthly": [
                counts(1, "month", 202610, 100, 10),
                counts(1, "month", 202611, 50, 5),
                counts(2, "month", 202611, 7),
            ]
        },
        coverage=coverage(),
    )

    out = build(tmp_path, [registry, downloads])

    assert select(out, "SELECT project_id, name FROM project ORDER BY project_id") == [
        (1, "alpha"),
        (2, "beta"),
        (3, "gamma"),
    ]
    assert select(out, "SELECT * FROM download_month ORDER BY project_id, month") == [
        (1, 202611, 7, 0),
        (3, 202610, 100, 10),
        (3, 202611, 50, 5),
    ]


def test_last30_sums_the_window_and_ranks_without_mirrors(gh, tmp_path):
    registry = registry_of(gh, tmp_path, "alpha", "beta", "gamma")
    yesterday, today = day("2026-11-09"), day("2026-11-10")
    downloads = downloads_input(
        gh,
        tmp_path,
        state={
            "projects": [
                {"project_id": 1, "name": "alpha"},
                {"project_id": 2, "name": "beta"},
                {"project_id": 3, "name": "gamma"},
            ],
            "days": counted(yesterday, today),
        },
        serving={
            # alpha has the most downloads, and most of them are mirrors'. Without
            # mirrors beta and gamma tie, and beta comes first by name.
            "recent": [
                counts(1, "day", yesterday, 10, 7),
                counts(1, "day", today, 10, 8),
                counts(2, "day", yesterday, 5),
                counts(2, "day", today, 7),
                counts(3, "day", today, 20, 8),
            ],
            "monthly": [
                counts(1, "month", 202611, 20, 15),
                counts(2, "month", 202611, 12),
                counts(3, "month", 202611, 20, 8),
            ],
        },
        coverage=coverage(),
    )

    out = build(tmp_path, [registry, downloads])

    assert select(
        out,
        "SELECT p.name, l.downloads, l.mirror_downloads, l.rank FROM download_last30 "
        "AS l JOIN project AS p USING (project_id) ORDER BY l.rank",
    ) == [("beta", 12, 0, 1), ("gamma", 20, 8, 2), ("alpha", 20, 15, 3)]


def test_daily_rows_keep_only_the_newest_years(gh, tmp_path):
    registry = registry_of(gh, tmp_path, "alpha")
    days = [day("2025-12-31"), day("2026-01-01"), day("2027-01-05")]
    downloads = downloads_input(
        gh,
        tmp_path,
        state={
            "projects": [{"project_id": 1, "name": "alpha"}],
            "days": counted(*days),
        },
        serving={
            "daily": [counts(1, "day", number, 3) for number in days],
            "monthly": [
                counts(1, "month", 202512, 3),
                counts(1, "month", 202601, 3),
                counts(1, "month", 202701, 3),
            ],
        },
        coverage=coverage(last_day="2027-01-05"),
    )

    # The newest day counted is in 2027, so two years are 2026 and 2027.
    two = build(tmp_path, [registry, downloads], years=2)
    one = build(tmp_path, [registry, downloads], name="one.db", years=1)

    assert select(two, "SELECT day FROM download_day ORDER BY day") == [
        (day("2026-01-01"),),
        (day("2027-01-05"),),
    ]
    assert select(one, "SELECT day FROM download_day") == [(day("2027-01-05"),)]


def test_partial_months_and_window_say_how_many_days_they_hold(gh, tmp_path):
    registry = registry_of(gh, tmp_path, "alpha")
    # The 30 days up to NEWEST, but for one that is not counted yet.
    newest = day_number(NEWEST)
    window = [
        number for number in range(newest - 29, newest + 1) if number != newest - 3
    ]
    downloads = downloads_input(
        gh,
        tmp_path,
        state={
            "projects": [{"project_id": 1, "name": "alpha"}],
            "days": counted(*window),
            "breaks": [{"day": day("2026-08-24"), "note": "PEP 658 fetches"}],
        },
        serving={
            # November has 20 of its 30 days, and February 2024 all of its 29.
            "months": [
                {"month": 202402, "days": 29, "downloads": 9, "mirror_downloads": 1},
                {"month": 202611, "days": 20, "downloads": 40, "mirror_downloads": 4},
            ],
            "monthly": [counts(1, "month", 202611, 40, 4)],
        },
        # As the stream writes it: counted without a gap from 2026-10-12 to launch,
        # and one day after launch missing.
        coverage=coverage(
            first_day="2026-10-12",
            days=29,
            forward_missing=1,
            backfill_left=(date(2026, 10, 12) - date(2016, 1, 22)).days,
            window_days=29,
        ),
    )

    out = build(tmp_path, [registry, downloads])

    assert select(out, "SELECT * FROM download_period ORDER BY month") == [
        (202402, 29, 29, 9, 1),
        (202611, 20, 30, 40, 4),
    ]
    assert select(out, "SELECT * FROM download_break") == [
        ("2026-08-24", "PEP 658 fetches")
    ]
    assert select(
        out,
        "SELECT launch, first_day, last_day, days, backfill_left, window_from, "
        "window_to, window_days FROM download_coverage",
    ) == [
        (
            "2026-11-01",
            "2026-10-12",
            "2026-11-10",
            29,
            (date(2026, 10, 12) - date(2016, 1, 22)).days,
            "2026-10-12",
            "2026-11-10",
            29,
        )
    ]


def test_a_stream_with_nothing_folded_has_an_empty_window_at_launch(gh, tmp_path):
    registry = registry_of(gh, tmp_path, "alpha")
    downloads = downloads_input(
        gh,
        tmp_path,
        state={"projects": []},
        serving={},
        coverage=coverage(
            first_day="2026-11-01", last_day="2026-11-01", days=0, window_days=0
        ),
    )

    out = build(tmp_path, [registry, downloads])

    assert select(
        out,
        "SELECT window_from, window_to, window_days, unmatched_projects, "
        "unmatched_downloads FROM download_coverage",
    ) == [("2026-11-01", "2026-11-01", 0, 0, 0)]


def test_a_name_the_registry_lacks_is_counted_not_dropped(gh, tmp_path):
    registry = registry_of(gh, tmp_path, "alpha")
    today = day_number(NEWEST)
    downloads = downloads_input(
        gh,
        tmp_path,
        state={
            "projects": [
                {"project_id": 1, "name": "alpha"},
                {"project_id": 2, "name": "newcomer"},
                {"project_id": 3, "name": "old-timer"},
            ],
            "days": counted(today),
        },
        serving={
            "monthly": [
                counts(1, "month", 202611, 5),
                counts(2, "month", 202611, 30, 4),
                # old-timer has downloads in October and none in the window.
                counts(3, "month", 202610, 2),
            ],
            "recent": [counts(1, "day", today, 5), counts(2, "day", today, 30, 4)],
            "top_set": [
                member(2026, 2, "newcomer", 202610, 1),
                member(2026, 1, "alpha", 202610, 2),
            ],
        },
        coverage=coverage(),
    )

    out = build(tmp_path, [registry, downloads])

    assert select(out, "SELECT project_id, month FROM download_month") == [(1, 202611)]
    assert select(out, "SELECT project_id, rank FROM download_last30") == [(1, 1)]
    assert select(out, "SELECT year, project_id, rank FROM download_top") == [
        (2026, 1, 2)
    ]
    # Two names that the registry lacks, and newcomer's 30 downloads in the window.
    assert select(
        out, "SELECT unmatched_projects, unmatched_downloads FROM download_coverage"
    ) == [(2, 30)]


def by_name(db: Path, table: str) -> list[tuple]:
    """The rows of `table` in the database `db`, each with the name of its project in
    place of its id, which a build makes afresh, in order."""
    columns = [row[1] for row in select(db, f"PRAGMA table_info({table})")]
    if "project_id" not in columns:
        return select(db, f"SELECT * FROM {table} ORDER BY 1")
    named = ", ".join("p.name" if c == "project_id" else f"t.{c}" for c in columns)
    return select(
        db,
        f"SELECT {named} FROM {table} AS t JOIN project AS p USING (project_id) "
        "ORDER BY 1, 2",
    )


def test_a_carried_downloads_stream_keeps_its_tables(gh, tmp_path):
    days = [day("2026-01-02"), day("2026-11-09"), day("2026-11-10")]
    downloads = downloads_input(
        gh,
        tmp_path,
        state={
            "projects": [
                {"project_id": 1, "name": "gamma"},
                {"project_id": 2, "name": "beta"},
                {"project_id": 3, "name": "newcomer"},
            ],
            "days": counted(*days),
            "breaks": [{"day": day("2026-08-24"), "note": "PEP 658 fetches"}],
        },
        serving={
            "monthly": [
                counts(1, "month", 202601, 4, 1),
                counts(1, "month", 202611, 9, 2),
                counts(2, "month", 202611, 6),
                counts(3, "month", 202611, 1),
            ],
            "months": [
                {"month": 202601, "days": 1, "downloads": 4, "mirror_downloads": 1},
                {"month": 202611, "days": 2, "downloads": 16, "mirror_downloads": 2},
            ],
            "recent": [
                counts(1, "day", days[1], 4, 1),
                counts(1, "day", days[2], 5, 1),
                counts(2, "day", days[2], 6),
                counts(3, "day", days[2], 1),
            ],
            "top_set": [
                member(2026, 1, "gamma", 202512, 1),
                member(2026, 2, "beta", 202610, 7),
            ],
            "daily": [
                counts(1, "day", days[0], 4, 1),
                counts(1, "day", days[1], 4, 1),
                counts(2, "day", days[2], 6),
            ],
        },
        coverage=coverage(window_days=2),
    )
    previous = build(
        tmp_path,
        [registry_of(gh, tmp_path, "beta", "gamma"), downloads],
        name="prev.db",
    )

    # The next day the registry knows alpha, which comes first by name, so this build
    # gives beta and gamma other ids than the last one did. The downloads stream cannot
    # be read.
    registry = registry_of(gh, tmp_path, "alpha", "beta", "gamma", on="2026-11-04")
    out = tmp_path / "out.db"
    report = build_serving(
        out, [registry, carried(downloads)], previous, daily_years=DAILY_YEARS
    )

    assert report.carried == ["pypi-downloads"]
    assert select(out, "SELECT project_id FROM project WHERE name = 'beta'") == [(2,)]
    assert select(previous, "SELECT project_id FROM project WHERE name = 'beta'") == [
        (1,)
    ]
    for table in SOURCED["pypi-downloads"]:
        assert by_name(out, table) == by_name(previous, table), table
        assert by_name(out, table), table
    assert select(out, "SELECT carried FROM input WHERE stream = 'pypi-downloads'") == [
        (1,)
    ]
