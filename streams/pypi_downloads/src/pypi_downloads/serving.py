"""The serving database of the stream: monthly totals for every project, the newest 30
days, the top 15,000 projects of each year and their daily counts.

The state holds no counts. They are in the deltas and the month parts, which grow to
tens of gigabytes as the backfill goes back, far more than a reader should fetch. This
database holds what the readers need of them. Months are numbered `year * 100 + month`
and days as the state numbers them. Its tables:

- `folded`: each day whose counts the other tables hold, with its `seq`. It is always
  exactly the state's `days`.
- `months`: for each month, how many of its days are folded, and their downloads and
  mirror downloads.
- `monthly`: each project's downloads and mirror downloads in each month.
- `recent`: the counts of the folded days from 29 days before the newest one on.
- `top_set`: for each year, the projects whose daily counts are kept, with the month at
  whose close each entered the year's set and its rank at that close.
- `daily`: the counts that are kept of the projects of `top_set`.

A month closes at the first instant of the month after it, and the close belongs to the
year of that instant, so December's is the next year's. Each month from the launch
month on closes. At its close the projects are ranked by their downloads less their
mirror downloads over the days of the month that were counted before it closed, highest
first and ties by name, and the first TOP that have more than none are the close's set.
Each of them that is not yet in the set of the close's year enters it, with the month
and its rank. A calendar month is ranked, rather than the newest 30 days, because
`monthly` holds its sums already and a month is what the site shows.

A project that enters keeps its daily counts for the rest of the year, from its entry
on: `daily` holds a project's counts of a day when the project is in the set of the
day's year and either entered at that year's first close or the day was counted at or
after the close at which it entered. Years before launch have no set, so their days have
monthly totals only.

A run brings the database up to date in two steps. It closes the month that has ended
since the last run (`close`), before anything counted after the month's end is folded,
so that `monthly` holds just what the ranking needs. Then it folds the days that it
counted (`fold`). Either raises NeedsRebuild when it cannot be done from what the run
holds, and `rebuild` then makes the database again from the published counts, which
gives the same tables. `check` compares the database with the state before it is
published.

The work is done in SQL, with the state, the delta or a source of counts attached to
the serving connection, so no table of counts is read into Python. A connection given
here must have no transaction open: SQLite attaches and detaches only outside one, and
what the state or the delta has not committed would not be seen.
"""

import sqlite3
from collections.abc import Callable, Generator, Iterable
from contextlib import closing, contextmanager
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from obskit.delta import atomic
from obskit.manifest import instant
from obskit.sqlite import open_db
from pypi_downloads.store import day_date, day_number, meta

SERVING_DB = "pypi-downloads-serving.db"
TABLES = ("folded", "months", "monthly", "top_set", "daily", "recent")
# How many projects each month's close ranks into its year's set.
TOP = 15_000
# How many days, up to the newest folded, `recent` holds.
WINDOW = 30

SCHEMA = """
CREATE TABLE folded (day INTEGER PRIMARY KEY, seq INTEGER NOT NULL);
CREATE TABLE months (month INTEGER PRIMARY KEY, days INTEGER NOT NULL,
    downloads INTEGER NOT NULL, mirror_downloads INTEGER NOT NULL);
CREATE TABLE monthly (project_id INTEGER NOT NULL, month INTEGER NOT NULL,
    downloads INTEGER NOT NULL, mirror_downloads INTEGER NOT NULL,
    PRIMARY KEY (project_id, month)) WITHOUT ROWID;
CREATE TABLE top_set (year INTEGER NOT NULL, project_id INTEGER NOT NULL,
    name TEXT NOT NULL, entered INTEGER NOT NULL, rank INTEGER NOT NULL,
    PRIMARY KEY (year, project_id)) WITHOUT ROWID;
CREATE TABLE daily (project_id INTEGER NOT NULL, day INTEGER NOT NULL,
    downloads INTEGER NOT NULL, mirror_downloads INTEGER NOT NULL,
    PRIMARY KEY (project_id, day)) WITHOUT ROWID;
CREATE TABLE recent (project_id INTEGER NOT NULL, day INTEGER NOT NULL,
    downloads INTEGER NOT NULL, mirror_downloads INTEGER NOT NULL,
    PRIMARY KEY (project_id, day)) WITHOUT ROWID;
"""


class NeedsRebuild(Exception):
    """The database cannot be brought up to date from what the run holds, and must be
    rebuilt from the published counts. The message says why."""


class ServingMismatch(Exception):
    """The database disagrees with the state, so it must not be published. The message
    says where."""


# ---- the temporary tables that the updates and the rebuild work with

# The days being folded, with what the rules need to know of each and its counts do not
# hold: the month and the year it falls in, and when it was counted.
_FOLD_DAYS = """
CREATE TEMP TABLE fold_days (
    day INTEGER PRIMARY KEY,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    counted_at TEXT NOT NULL
)
"""

# For each year and each month at whose close projects entered the year's set, the
# instant from which the days of those projects are kept: null for the year's first
# close, whose projects keep every day of the year.
_ENTRIES = """
CREATE TEMP TABLE entries (
    year INTEGER NOT NULL,
    entered INTEGER NOT NULL,
    kept_from TEXT,
    PRIMARY KEY (year, entered)
) WITHOUT ROWID
"""

# The months that a rebuild closes, with the instant at which each closes.
_CLOSES = """
CREATE TEMP TABLE closes (
    month INTEGER PRIMARY KEY,
    closes_at TEXT NOT NULL
)
"""

# What each project downloaded less its mirror downloads in each month that a rebuild
# closes, over the days that were counted before the month closed.
_CLOSE_SUMS = """
CREATE TEMP TABLE close_sums (
    month INTEGER NOT NULL,
    project_id INTEGER NOT NULL,
    net INTEGER NOT NULL,
    PRIMARY KEY (month, project_id)
) WITHOUT ROWID
"""

# ---- what folding days does, from the counts of the database attached as `source`
#
# Each statement takes the counts of the days in `fold_days`, the days a run folds or
# every day for a rebuild. CROSS JOIN fixes the order in which SQLite reads the tables:
# the counts once, end to end, or by project, and each small table by its key. Left to
# itself, with no statistics to go on, it may read the counts once for each day.

_ADD_MONTHLY = """
INSERT INTO main.monthly (project_id, month, downloads, mirror_downloads)
SELECT c.project_id, d.month, sum(c.downloads), sum(c.mirror_downloads)
FROM source.counts AS c CROSS JOIN temp.fold_days AS d
WHERE d.day = c.day
GROUP BY c.project_id, d.month
ON CONFLICT (project_id, month) DO UPDATE SET
    downloads = downloads + excluded.downloads,
    mirror_downloads = mirror_downloads + excluded.mirror_downloads
"""

_ADD_MONTH_SUMS = """
INSERT INTO main.months (month, days, downloads, mirror_downloads)
SELECT d.month, 0, sum(c.downloads), sum(c.mirror_downloads)
FROM source.counts AS c CROSS JOIN temp.fold_days AS d
WHERE d.day = c.day
GROUP BY d.month
ON CONFLICT (month) DO UPDATE SET
    downloads = downloads + excluded.downloads,
    mirror_downloads = mirror_downloads + excluded.mirror_downloads
"""

# A month's folded days are counted from the state's days and not from the counts, so
# that a day is counted once whichever source holds its counts.
_ADD_MONTH_DAYS = """
INSERT INTO main.months (month, days, downloads, mirror_downloads)
SELECT month, count(*), 0, 0 FROM temp.fold_days WHERE true GROUP BY month
ON CONFLICT (month) DO UPDATE SET days = days + excluded.days
"""

_ADD_RECENT = """
INSERT INTO main.recent (project_id, day, downloads, mirror_downloads)
SELECT c.project_id, c.day, c.downloads, c.mirror_downloads
FROM source.counts AS c CROSS JOIN temp.fold_days AS d
WHERE d.day = c.day AND c.day >= ?
"""

# Each project of a set is read from the counts by its key, a few thousand projects in
# place of every count.
_ADD_DAILY = """
INSERT INTO main.daily (project_id, day, downloads, mirror_downloads)
SELECT c.project_id, c.day, c.downloads, c.mirror_downloads
FROM main.top_set AS t
CROSS JOIN source.counts AS c
CROSS JOIN temp.fold_days AS d
CROSS JOIN temp.entries AS e
WHERE c.project_id = t.project_id
    AND d.day = c.day AND d.year = t.year
    AND e.year = t.year AND e.entered = t.entered
    AND (e.kept_from IS NULL OR d.counted_at >= e.kept_from)
"""

_ADD_CLOSE_SUMS = """
INSERT INTO temp.close_sums (month, project_id, net)
SELECT d.month, c.project_id, sum(c.downloads - c.mirror_downloads)
FROM source.counts AS c CROSS JOIN temp.fold_days AS d CROSS JOIN temp.closes AS k
WHERE d.day = c.day AND k.month = d.month AND d.counted_at < k.closes_at
GROUP BY d.month, c.project_id
ON CONFLICT (month, project_id) DO UPDATE SET net = net + excluded.net
"""

# ---- a month's close, from the sums of `{sums}` and the names of the state attached as
# `state`. A name is unique, so the order is the same however the rows come.

_ADD_MEMBERS = """
INSERT INTO main.top_set (year, project_id, name, entered, rank)
SELECT :year, project_id, name, :month, place FROM (
    SELECT s.project_id, p.name,
        row_number() OVER (ORDER BY s.net DESC, p.name) AS place
    FROM ({sums}) AS s JOIN state.projects AS p ON p.project_id = s.project_id
    WHERE s.net > 0
)
WHERE place <= :top
ON CONFLICT (year, project_id) DO NOTHING
"""

# The sums that a run's close ranks: the month's sums as folded so far.
_MONTHLY_NET = """
SELECT project_id, downloads - mirror_downloads AS net
FROM main.monthly WHERE month = :month
"""

# The sums that a rebuild's close ranks.
_CLOSE_NET = "SELECT project_id, net FROM temp.close_sums WHERE month = :month"


# ---- months and days


def _month(d: date) -> int:
    """The month of `d`, as `year * 100 + month`."""
    return d.year * 100 + d.month


def _month_text(month: int) -> str:
    """`month` as YYYY-MM."""
    return f"{month // 100:04d}-{month % 100:02d}"


def _day_text(day: int) -> str:
    """The day numbered `day` as YYYY-MM-DD."""
    return day_date(day).isoformat()


def _first_day_after(month: int) -> date:
    """The first day of the month after `month`."""
    year, number = divmod(month, 100)
    return date(year + number // 12, number % 12 + 1, 1)


def _closes_at(month: int) -> str:
    """The instant at which `month` closes, the first of the month after it, written as
    the state writes `counted_at`, so that the two compare as text."""
    return instant(datetime.combine(_first_day_after(month), time(), UTC))


def _close_year(month: int) -> int:
    """The year to whose set the close of `month` adds: the year of the instant at
    which it closes."""
    return _first_day_after(month).year


def _months(first: int, last: int) -> list[int]:
    """The months from `first` to `last`, both included, in order."""
    months = []
    while first <= last:
        months.append(first)
        first = _month(_first_day_after(first))
    return months


def _year_days(year: int) -> tuple[int, int]:
    """The first day of `year` and the first day of the year after, as numbers."""
    return day_number(date(year, 1, 1)), day_number(date(year + 1, 1, 1))


def _listed(items: list[str], most: int = 5) -> str:
    """`items` joined for a message, the first `most` of them and how many more."""
    shown = ", ".join(items[:most])
    return shown if len(items) <= most else f"{shown} and {len(items) - most} more"


# ---- attaching the other databases


def _file_of(conn: sqlite3.Connection) -> Path:
    """The file of the main database of `conn`, to attach to the serving connection.

    Raises ValueError when `conn` has a transaction open, since what it has not
    committed would not be seen there, and when its database is in memory, which
    another connection cannot attach."""
    if conn.in_transaction:
        raise ValueError(
            "the database has a transaction open, and what it has not committed would "
            "not be seen: commit before updating the serving database"
        )
    for _, name, file in conn.execute("PRAGMA database_list"):
        if name == "main" and file:
            return Path(file)
    raise ValueError("the database is not in a file, so it cannot be attached")


@contextmanager
def _attached(conn: sqlite3.Connection, path: Path, alias: str) -> Generator[None]:
    """Attach the database at `path` to `conn` as `alias` for the block, and detach it
    again whatever happens. Only reads go to it. Raises ValueError when `conn` has a
    transaction open, in which SQLite neither attaches nor detaches, and
    FileNotFoundError when `path` is not there, because attaching it would make an empty
    database."""
    if conn.in_transaction:
        raise ValueError(
            "the serving database has a transaction open: commit before updating it"
        )
    conn.execute(f"ATTACH DATABASE ? AS {alias}", (str(path.resolve(strict=True)),))
    try:
        yield
    finally:
        conn.execute(f"DETACH DATABASE {alias}")


# ---- the steps that the updates and the rebuild share


def _make_fold_days(
    serving: sqlite3.Connection, days: Iterable[tuple[int, str]]
) -> None:
    """Make `fold_days` from `(day, counted_at)` pairs of the state's days."""
    serving.execute(_FOLD_DAYS)
    serving.executemany(
        "INSERT INTO temp.fold_days (day, month, year, counted_at) VALUES (?, ?, ?, ?)",
        (
            (day, _month(day_date(day)), day_date(day).year, counted_at)
            for day, counted_at in days
        ),
    )


def _make_entries(serving: sqlite3.Connection) -> None:
    """Make `entries` from `top_set`. A year's first close is the earliest month in
    which any of its projects entered."""
    serving.execute(_ENTRIES)
    entries = serving.execute(
        "SELECT DISTINCT year, entered FROM top_set ORDER BY year, entered"
    ).fetchall()
    first: dict[int, int] = {}
    for year, entered in entries:
        first.setdefault(year, entered)
    serving.executemany(
        "INSERT INTO temp.entries (year, entered, kept_from) VALUES (?, ?, ?)",
        (
            (year, entered, None if entered == first[year] else _closes_at(entered))
            for year, entered in entries
        ),
    )


def _add_counts(serving: sqlite3.Connection, recent_from: int) -> None:
    """Add the counts that `source` holds of the days of `fold_days` to `monthly`, to
    the sums of `months`, and, from the day `recent_from` on, to `recent`."""
    serving.execute(_ADD_MONTHLY)
    serving.execute(_ADD_MONTH_SUMS)
    serving.execute(_ADD_RECENT, (recent_from,))


def _add_members(serving: sqlite3.Connection, sums: str, month: int) -> int:
    """Close `month` by ranking the sums that the query `sums` gives, and return how
    many projects entered its year's set. The state must be attached as `state`."""
    cursor = serving.execute(
        _ADD_MEMBERS.format(sums=sums),
        {"year": _close_year(month), "month": month, "top": TOP},
    )
    return cursor.rowcount


# ---- the updates of a run


def close(serving: sqlite3.Connection, state: sqlite3.Connection, month: int) -> int:
    """Close `month` from the sums that `monthly` holds now, and return how many
    projects entered the set of its close's year.

    `monthly` holds the month's sums over the days counted before it closed only while
    the folded days are exactly the state's days counted before then. A run closes the
    month before it folds anything it counted after the month's end, so it is. A
    database that misses one of those days, or has folded a day counted later, raises
    NeedsRebuild.

    It raises NeedsRebuild, and adds nothing, when the close would make the first
    projects of a year's set while days of that year are folded: they keep each day of
    the year, and the run does not hold the counts of the days folded before. This is
    the launch year's first close, as no day of a later year is counted before its
    first close.
    """
    year = _close_year(month)
    closes_at = _closes_at(month)
    with _attached(serving, _file_of(state), "state"), atomic(serving):
        missing, late = serving.execute(
            "SELECT count(*) - count(f.day), count(f.day) "
            "FROM state.days AS d LEFT JOIN main.folded AS f ON f.day = d.day "
            "WHERE (d.counted_at < ?) <> (f.day IS NOT NULL)",
            (closes_at,),
        ).fetchone()
        if missing or late:
            raise NeedsRebuild(
                f"{_month_text(month)} closes from the folded days, which must be "
                f"exactly the state's days counted before {closes_at}, and they are "
                f"not (counted before and not folded: {missing}; folded and counted "
                f"at or after it: {late})"
            )
        (members,) = serving.execute(
            "SELECT count(*) FROM top_set WHERE year = ?", (year,)
        ).fetchone()
        added = _add_members(serving, _MONTHLY_NET, month)
        (folded_in_year,) = serving.execute(
            "SELECT count(*) FROM folded WHERE day >= ? AND day < ?", _year_days(year)
        ).fetchone()
        if members == 0 and added and folded_in_year:
            raise NeedsRebuild(
                f"the close of {_month_text(month)} makes the first members of "
                f"{year}'s top set, who keep each day of {year}, and {folded_in_year} "
                f"days of {year} are folded already without their daily counts"
            )
    return added


def _require_counts(delta: sqlite3.Connection, projects: dict[int, int]) -> None:
    """Raise NeedsRebuild unless `delta` holds as many counts of each day of `projects`
    as the state counted projects on it. Fewer means the counts are in another delta,
    and more that the delta is not the one the state was made from."""
    held = dict(delta.execute("SELECT day, count(*) FROM counts GROUP BY day"))
    for day, wanted in projects.items():
        found = held.get(day, 0)
        if found != wanted:
            raise NeedsRebuild(
                f"the delta holds {found} counts of {_day_text(day)}, where the state "
                f"counted {wanted} projects that day"
            )


def fold(
    serving: sqlite3.Connection, state: sqlite3.Connection, delta: sqlite3.Connection
) -> list[int]:
    """Fold each day of the state that is not folded yet, from the counts of `delta`,
    and return those days in the order of their `seq`.

    Their counts go into `monthly` and `months`, into `recent` when they are in the
    window up to the newest folded day, and into `daily` for the projects whose daily
    counts of them are kept. Then `recent` keeps the window only.

    Raises NeedsRebuild, and folds nothing, when the count of a day's rows in `delta`
    is not the number of projects that the state counted that day.
    """
    folded = {day for (day,) in serving.execute("SELECT day FROM folded")}
    new = [
        (day, seq, counted_at, projects)
        for day, seq, counted_at, projects in state.execute(
            "SELECT day, seq, counted_at, projects FROM days ORDER BY seq"
        )
        if day not in folded
    ]
    if not new:
        return []
    _require_counts(delta, {day: projects for day, _, _, projects in new})
    # The days go in together, which gives what folding them one at a time in seq
    # order gives: the sums add up the same, and a day leaves the window only for a
    # newer one, so the window of the newest of them is the one that stays.
    recent_from = max(folded | {day for day, *_ in new}) - WINDOW + 1
    with _attached(serving, _file_of(delta), "source"), atomic(serving):
        serving.executemany(
            "INSERT INTO folded (day, seq) VALUES (?, ?)",
            ((day, seq) for day, seq, _, _ in new),
        )
        _make_fold_days(serving, ((day, counted_at) for day, _, counted_at, _ in new))
        _make_entries(serving)
        serving.execute(_ADD_MONTH_DAYS)
        _add_counts(serving, recent_from)
        serving.execute(_ADD_DAILY)
        serving.execute("DELETE FROM recent WHERE day < ?", (recent_from,))
        serving.execute("DROP TABLE temp.fold_days")
        serving.execute("DROP TABLE temp.entries")
    return [day for day, *_ in new]


# ---- the check


def _check_folded(serving: sqlite3.Connection, days: list[tuple]) -> None:
    held = set(serving.execute("SELECT day, seq FROM folded"))
    wanted = {(day, seq) for day, seq, *_ in days}
    if held != wanted:
        differ = sorted({day for day, _ in held ^ wanted})
        raise ServingMismatch(
            "folded is not the state's days and seqs, on "
            f"{_listed([_day_text(day) for day in differ])}"
        )


def _check_months(serving: sqlite3.Connection, days: list[tuple]) -> None:
    wanted: dict[int, tuple[int, int, int]] = {}
    for day, _, _, downloads, mirror_downloads in days:
        month = _month(day_date(day))
        folded, total, mirrors = wanted.get(month, (0, 0, 0))
        wanted[month] = (folded + 1, total + downloads, mirrors + mirror_downloads)
    held = {
        month: (folded, total, mirrors)
        for month, folded, total, mirrors in serving.execute(
            "SELECT month, days, downloads, mirror_downloads FROM months"
        )
    }
    differ = sorted(
        m for m in wanted.keys() | held.keys() if wanted.get(m) != held.get(m)
    )
    if differ:
        raise ServingMismatch(
            "months disagrees with the state's days in "
            f"{_listed([_month_text(month) for month in differ])}"
        )


def _check_monthly(serving: sqlite3.Connection) -> None:
    sums = {
        month: (total, mirrors)
        for month, total, mirrors in serving.execute(
            "SELECT month, sum(downloads), sum(mirror_downloads) FROM monthly "
            "GROUP BY month"
        )
    }
    totals = {
        month: (total, mirrors)
        for month, total, mirrors in serving.execute(
            "SELECT month, downloads, mirror_downloads FROM months"
        )
    }
    # A month with no downloads has no rows in monthly, and sums to none.
    differ = sorted(
        m for m in sums.keys() | totals.keys() if sums.get(m, (0, 0)) != totals.get(m)
    )
    if differ:
        raise ServingMismatch(
            "monthly does not sum to months in "
            f"{_listed([_month_text(month) for month in differ])}"
        )


def _check_recent(serving: sqlite3.Connection, days: list[tuple]) -> None:
    newest = max((day for day, *_ in days), default=0)
    wanted = {
        day: (projects, downloads, mirror_downloads)
        for day, _, projects, downloads, mirror_downloads in days
        if day > newest - WINDOW
    }
    held = {
        day: (rows, total, mirrors)
        for day, rows, total, mirrors in serving.execute(
            "SELECT day, count(*), sum(downloads), sum(mirror_downloads) FROM recent "
            "GROUP BY day"
        )
    }
    # A day with no projects has no rows in recent.
    differ = sorted(
        d
        for d in wanted.keys() | held.keys()
        if held.get(d, (0, 0, 0)) != wanted.get(d)
    )
    if differ:
        raise ServingMismatch(
            f"recent is not the counts of the folded days of the {WINDOW} up to the "
            f"newest, on {_listed([_day_text(day) for day in differ])}"
        )


def _check_daily(serving: sqlite3.Connection) -> None:
    first, last = serving.execute("SELECT min(day), max(day) FROM daily").fetchone()
    if first is None:
        return
    for year in range(day_date(first).year, day_date(last).year + 1):
        (outside,) = serving.execute(
            "SELECT count(*) FROM daily AS d WHERE d.day >= ? AND d.day < ? "
            "AND NOT EXISTS (SELECT 1 FROM top_set AS t "
            "WHERE t.year = ? AND t.project_id = d.project_id)",
            (*_year_days(year), year),
        ).fetchone()
        if outside:
            raise ServingMismatch(
                f"daily holds rows of {year} for projects outside its top set "
                f"({outside})"
            )
    (unfolded,) = serving.execute(
        "SELECT count(*) FROM daily WHERE day NOT IN (SELECT day FROM folded)"
    ).fetchone()
    if unfolded:
        raise ServingMismatch(
            f"daily holds rows of days that are not folded ({unfolded})"
        )


def check(serving: sqlite3.Connection, state: sqlite3.Connection) -> None:
    """Raise ServingMismatch, with where, unless the database agrees with the state:
    `folded` is the state's days and their seqs; `months` is the state's days summed by
    month, the number of days, the downloads and the mirror downloads; `monthly` sums
    to `months` in each month; `recent` holds, for each folded day of the window up to
    the newest, as many rows as the state counted projects that day, with the state's
    sums, and no other day; and each row of `daily` is of a folded day and of a project
    in the set of the day's year. Nothing is written to either database."""
    days = state.execute(
        "SELECT day, seq, projects, downloads, mirror_downloads FROM days"
    ).fetchall()
    _check_folded(serving, days)
    _check_months(serving, days)
    _check_monthly(serving)
    _check_recent(serving, days)
    _check_daily(serving)


# ---- the rebuild


def rebuild(
    path: Path,
    state: sqlite3.Connection,
    sources: Callable[[], Iterable[Path]],
    *,
    today: date,
) -> None:
    """Make the serving database at `path` from the state and the `counts` of the
    sources, as the runs would have made it, with a close for each month from the
    launch month (the state's `meta.launch`) to the month before `today`.

    `sources()` gives the path of each database whose counts are read: the month parts,
    the sealed deltas and the open delta, together holding each day of the state once,
    in any order. It is called once for each pass over them, one for the sums and one
    for the daily counts, which need the set that the sums make. Each source is
    attached in turn, its counts are added up in SQL, and it is detached before the
    next is asked for, so that the one who provides them can delete each once it is
    read. Counts of a day the state does not hold are left out.

    Raises FileExistsError when `path` exists, which is left alone, and ValueError when
    the state has no launch. A rebuild that fails removes what it made at `path`.
    """
    launch = meta(state, "launch")
    if launch is None:
        raise ValueError("the state has no launch in meta, so no bootstrap built it")
    last_closed = _month(today.replace(day=1) - timedelta(days=1))
    closes = _months(_month(date.fromisoformat(launch)), last_closed)
    days = state.execute("SELECT day, seq, counted_at FROM days").fetchall()
    state_file = _file_of(state)
    serving = open_db(path, SCHEMA, create=True)
    try:
        with closing(serving):
            _rebuild(serving, state_file, sources, days, closes)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _rebuild(
    serving: sqlite3.Connection,
    state_file: Path,
    sources: Callable[[], Iterable[Path]],
    days: list[tuple[int, int, str]],
    closes: list[int],
) -> None:
    """Fill the new database `serving` for `rebuild`: each of the state's `days`, given
    as `(day, seq, counted_at)`, is folded, and each month of `closes` closes."""
    recent_from = max((day for day, _, _ in days), default=0) - WINDOW + 1
    with atomic(serving):
        serving.executemany(
            "INSERT INTO folded (day, seq) VALUES (?, ?)",
            ((day, seq) for day, seq, _ in days),
        )
        _make_fold_days(serving, ((day, counted_at) for day, _, counted_at in days))
        serving.execute(_ADD_MONTH_DAYS)
        serving.execute(_CLOSES)
        serving.executemany(
            "INSERT INTO temp.closes (month, closes_at) VALUES (?, ?)",
            ((month, _closes_at(month)) for month in closes),
        )
        serving.execute(_CLOSE_SUMS)
    for source in sources():
        with _attached(serving, source, "source"), atomic(serving):
            _add_counts(serving, recent_from)
            serving.execute(_ADD_CLOSE_SUMS)
    # Close by close, in order, since a project already in a year's set stays as it
    # entered.
    with _attached(serving, state_file, "state"), atomic(serving):
        for month in closes:
            _add_members(serving, _CLOSE_NET, month)
    with atomic(serving):
        _make_entries(serving)
    for source in sources():
        with _attached(serving, source, "source"), atomic(serving):
            serving.execute(_ADD_DAILY)
