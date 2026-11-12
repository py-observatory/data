"""The downloads in the serving database, from the downloads stream's state and serving
database.

The stream numbers its projects in the order that it met them, and the catalog numbers
its own in name order, so the stream's rows are linked to the catalog's projects by
name, which both write as PEP 503 normalises it. The stream's state gives the name of
each of its numbers. A name that the registry does not know, such as that of a project
it has not seen yet, is left out of the tables and counted in `download_coverage`, so
that no downloads go missing without a word.

The stream's serving database is copied as it is, linked to the catalog's projects, but
for two things. The window's days are summed for each project, which `download_last30`
ranks. Only the newest years of daily rows are kept, so that the catalog stays inside
its size budget: the year of the newest day counted and the years before it, as many as
catalog.toml says. The newest day counted is the newest of the state's days, which are
exactly the days that the serving database has folded.

The work is done in SQL, with the stream's databases attached read-only, so no table of
counts is read into Python.
"""

import sqlite3
from datetime import date

import pypi_downloads
from pypi_downloads.serving import WINDOW
from pypi_downloads.store import day_date, day_number

from catalog.schema import SOURCED
from catalog.stage import DownloadsStage, attach_read_only

# Each of the stream's project numbers whose name the registry knows, with the catalog's
# number for that name.
_CREATE_LINK = """
CREATE TEMP TABLE download_link (
    stream_id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL
)
"""

_FILL_LINK = """
INSERT INTO temp.download_link (stream_id, project_id)
SELECT s.project_id, p.project_id
FROM downloads_state.projects AS s
JOIN main.project AS p ON p.name = s.name
"""

_NEWEST_DAY = "SELECT max(day) FROM downloads_state.days"

_INSERT_MONTHS = """
INSERT INTO main.download_month (project_id, month, downloads, mirror_downloads)
SELECT l.project_id, m.month, m.downloads, m.mirror_downloads
FROM downloads_serving.monthly AS m
JOIN temp.download_link AS l ON l.stream_id = m.project_id
ORDER BY l.project_id, m.month
"""

# The daily rows from the first day kept on.
_INSERT_DAYS = """
INSERT INTO main.download_day (project_id, day, downloads, mirror_downloads)
SELECT l.project_id, d.day, d.downloads, d.mirror_downloads
FROM downloads_serving.daily AS d
JOIN temp.download_link AS l ON l.stream_id = d.project_id
WHERE d.day >= ?
ORDER BY l.project_id, d.day
"""

# Each project's sums over the window, ranked by downloads without mirrors, highest
# first and ties by name.
_INSERT_LAST30 = """
INSERT INTO main.download_last30 (project_id, downloads, mirror_downloads, rank)
SELECT
    l.project_id,
    w.downloads,
    w.mirror_downloads,
    row_number() OVER (ORDER BY w.downloads - w.mirror_downloads DESC, p.name)
FROM (
    SELECT
        project_id,
        sum(downloads) AS downloads,
        sum(mirror_downloads) AS mirror_downloads
    FROM downloads_serving.recent
    GROUP BY project_id
) AS w
JOIN temp.download_link AS l ON l.stream_id = w.project_id
JOIN main.project AS p ON p.project_id = l.project_id
ORDER BY l.project_id
"""

_INSERT_TOP = """
INSERT INTO main.download_top (year, project_id, entered, rank)
SELECT t.year, l.project_id, t.entered, t.rank
FROM downloads_serving.top_set AS t
JOIN temp.download_link AS l ON l.stream_id = t.project_id
ORDER BY t.year, l.project_id
"""

# The last day of a month is the day before the first of the next, and its number is
# how many days the month has.
_INSERT_PERIODS = """
INSERT INTO main.download_period (
    month, days, days_in_month, downloads, mirror_downloads
)
SELECT
    month,
    days,
    CAST(
        strftime(
            '%d', printf('%04d-%02d-01', month / 100, month % 100), '+1 month', '-1 day'
        ) AS INTEGER
    ),
    downloads,
    mirror_downloads
FROM downloads_serving.months
ORDER BY month
"""

# A day of the stream is a number of days since 1970-01-01, and so of seconds since then
# once multiplied by the seconds of a day.
_INSERT_BREAKS = """
INSERT INTO main.download_break (day, note)
SELECT date(day * 86400, 'unixepoch'), note
FROM downloads_state.breaks
ORDER BY day
"""

# The projects with downloads whose names the registry does not know, and their
# downloads in the window. Every project that has a day in the window has a month, so
# the months find them all.
_UNMATCHED = """
SELECT
    count(*),
    coalesce(
        sum(
            (SELECT sum(r.downloads) FROM downloads_serving.recent AS r
             WHERE r.project_id = m.project_id)
        ),
        0
    )
FROM (SELECT DISTINCT project_id FROM downloads_serving.monthly) AS m
WHERE NOT EXISTS (
    SELECT 1 FROM temp.download_link AS l WHERE l.stream_id = m.project_id
)
"""

_INSERT_COVERAGE = """
INSERT INTO main.download_coverage (
    launch, first_day, last_day, days, backfill_left,
    window_from, window_to, window_days, unmatched_projects, unmatched_downloads
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def build_downloads(
    con: sqlite3.Connection,
    stage: DownloadsStage,
    *,
    coverage: dict,
    daily_years: int,
) -> None:
    """Fill the download tables of the serving database that `con` is open on, from the
    downloads stream that `stage` holds. `coverage` is the coverage of the stream's
    manifest, which `download_coverage` copies, and `daily_years` how many years of
    daily rows to keep (see the top of this module).

    The registry's `project` must be filled already, which it is when the streams are
    built in the order of catalog.toml, and the download tables must be empty. `con`
    must open URIs, as the one that `open_db` gives does, because the stream's databases
    are attached by the URIs that make them read-only.

    Everything is committed and the two databases are detached before this returns, so
    that the connection holds no transaction. An error leaves them attached, and what
    was written uncommitted, for the caller to close the connection on and discard the
    database. Raises KeyError when `coverage` lacks a key that `download_coverage`
    copies.
    """
    attach_read_only(con, stage.state, "downloads_state")
    attach_read_only(con, stage.serving, "downloads_serving")
    con.execute(_CREATE_LINK)
    con.execute(_FILL_LINK)
    (newest,) = con.execute(_NEWEST_DAY).fetchone()
    con.execute(_INSERT_MONTHS)
    con.execute(_INSERT_PERIODS)
    con.execute(_INSERT_LAST30)
    con.execute(_INSERT_TOP)
    # With no day counted there are no daily rows to keep.
    if newest is not None:
        con.execute(_INSERT_DAYS, (_first_day_kept(newest, daily_years),))
    con.execute(_INSERT_BREAKS)
    _insert_coverage(con, coverage, newest)
    con.execute("DROP TABLE temp.download_link")
    # A database cannot be detached while the transaction that read it is open.
    con.commit()
    con.execute("DETACH DATABASE downloads_serving")
    con.execute("DETACH DATABASE downloads_state")


def _first_day_kept(newest: int, daily_years: int) -> int:
    """The first day of the oldest of the `daily_years` years that end with the year of
    the day `newest`, as the stream numbers days."""
    year = day_date(newest).year - (daily_years - 1)
    return day_number(date(year, 1, 1))


def _insert_coverage(
    con: sqlite3.Connection, coverage: dict, newest: int | None
) -> None:
    """Write the one row of `download_coverage`: what `coverage` gives, the days of the
    window that ends with the day `newest`, and the downloads that the registry's names
    leave out. With no day counted, the window is the launch day, and `coverage` says
    that it has no day counted."""
    if newest is None:
        window_from = window_to = coverage["launch"]
    else:
        window_from = day_date(newest - (WINDOW - 1)).isoformat()
        window_to = day_date(newest).isoformat()
    unmatched_projects, unmatched_downloads = con.execute(_UNMATCHED).fetchone()
    con.execute(
        _INSERT_COVERAGE,
        (
            coverage["launch"],
            coverage["first_day"],
            coverage["last_day"],
            coverage["days"],
            coverage["backfill_left"],
            window_from,
            window_to,
            coverage["window_days"],
            unmatched_projects,
            unmatched_downloads,
        ),
    )


def carry_downloads(con: sqlite3.Connection) -> None:
    """Copy the download tables of the previous build, which is attached to `con` as
    `previous`, into the serving database that `con` is open on.

    A row that names a project is given this build's number for the project of the same
    name. The registry may have been read afresh while the downloads stream could not
    be, and a build numbers its projects in name order, so a project that the registry
    has found since the previous build moves the numbers of every project after it. The
    registry's `project` must be filled already, as `build_downloads` needs it to be.
    The registry never loses a project, so every name is found.
    """
    for table in SOURCED[pypi_downloads.STREAM]:
        info = con.execute(f"PRAGMA main.table_info({table})")
        columns = [row[1] for row in info]
        listed = ", ".join(columns)
        if "project_id" not in columns:
            con.execute(
                f"INSERT INTO main.{table} ({listed}) "
                f"SELECT {listed} FROM previous.{table}"
            )
            continue
        chosen = ", ".join(
            "now.project_id" if column == "project_id" else f"t.{column}"
            for column in columns
        )
        con.execute(
            f"INSERT INTO main.{table} ({listed}) "
            f"SELECT {chosen} FROM previous.{table} AS t "
            "JOIN previous.project AS was ON was.project_id = t.project_id "
            "JOIN main.project AS now ON now.name = was.name"
        )
