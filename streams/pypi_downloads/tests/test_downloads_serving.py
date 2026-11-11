import shutil
import sqlite3
from collections.abc import Callable, Iterator
from datetime import date
from pathlib import Path

import pytest
from pypi_downloads import serving
from pypi_downloads.serving import (
    SCHEMA,
    SERVING_DB,
    TABLES,
    NeedsRebuild,
    ServingMismatch,
    check,
    close,
    fold,
    rebuild,
)
from pypi_downloads.store import (
    DELTA_SCHEMA,
    STATE_SCHEMA,
    Delta,
    apply,
    day_date,
    day_number,
    insert_counts,
    meta,
)

from obskit.sqlite import open_db

LAUNCH = "2026-11-10"
# The ids are not in the order of the names, so a tie broken by name is not one broken
# by id.
IDS = {"requests": 1, "numpy": 2, "pandas": 3}

Counts = dict[str, tuple[int, int]]


def n(day: str) -> int:
    """The number of the day given as YYYY-MM-DD, as the state numbers days."""
    return day_number(date.fromisoformat(day))


def day_row(day: int, counts: Counts, counted_at: str) -> dict:
    """The `days` row of a day whose counts are `counts`, counted at `counted_at`."""
    downloads = sum(d for d, _ in counts.values())
    return {
        "day": day,
        "rows_bigquery": downloads,
        "rows_counted": downloads,
        "partition_modified": counted_at,
        "projects": len(counts),
        "downloads": downloads,
        "mirror_downloads": sum(m for _, m in counts.values()),
        "null_project_rows": 0,
        "read_bytes": 1000,
        "execution": None,
        "vcpu_seconds": None,
        "gib_seconds": None,
        "counted_at": counted_at,
    }


class Runs:
    """The stream's state with three projects, the delta of each run and a serving
    database, in a folder of their own.

    `count` is a run that counts days, as the stream's runs do: each day's counts and
    its `days` row go into the run's own delta, counted at the run's start, and the
    delta is applied to the state. The serving database starts empty, as a first update
    finds it, and the tests bring it up to date with `close` and `fold`.
    """

    def __init__(self, folder: Path, launch: str) -> None:
        folder.mkdir()
        self.folder = folder
        self.state = open_db(folder / "state.db", STATE_SCHEMA, create=True)
        self.state.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [("applied_seq", "0"), ("launch", launch)],
        )
        self.state.commit()
        self.serving = open_db(folder / SERVING_DB, SCHEMA, create=True)
        self.deltas: list[Path] = []
        self.opened: list[sqlite3.Connection] = []

    def count(self, at: str, days: dict[str, Counts]) -> sqlite3.Connection:
        """A run at `at` that counts `days`, each given as YYYY-MM-DD with each
        project's downloads and mirror downloads. Returns the run's delta, open."""
        path = self.folder / f"delta-{len(self.deltas) + 1}.db"
        conn = open_db(path, DELTA_SCHEMA, create=True)
        self.opened.append(conn)
        applied = int(meta(self.state, "applied_seq") or 0)
        delta = Delta(conn, at, first_seq=applied + 1)
        if not self.deltas:
            for name, project_id in IDS.items():
                delta.project(project_id, name)
        for day, counts in days.items():
            rows = [(IDS[name], n(day), d, m) for name, (d, m) in counts.items()]
            insert_counts(conn, rows)
            delta.day(**day_row(n(day), counts, at))
        conn.commit()
        apply(self.state, conn)
        self.deltas.append(path)
        return conn

    def rebuild(self, today: str) -> None:
        """Replace the serving database with one rebuilt from every delta, as a run
        does when it cannot update it."""
        self.serving.close()
        (self.folder / SERVING_DB).unlink()
        rebuild(
            self.folder / SERVING_DB,
            self.state,
            lambda: iter(self.deltas),
            today=date.fromisoformat(today),
        )
        self.serving = open_db(self.folder / SERVING_DB, SCHEMA)

    def rebuilt(
        self,
        today: str,
        name: str = "rebuilt.db",
        sources: Callable[[], Iterator[Path]] | None = None,
    ) -> sqlite3.Connection:
        """Another serving database, `name`, rebuilt from every delta, opened."""
        path = self.folder / name
        rebuild(
            path,
            self.state,
            sources or (lambda: iter(self.deltas)),
            today=date.fromisoformat(today),
        )
        conn = open_db(path, SCHEMA)
        self.opened.append(conn)
        return conn

    def empty(self, name: str) -> sqlite3.Connection:
        """Another serving database, `name`, with nothing folded, opened."""
        conn = open_db(self.folder / name, SCHEMA, create=True)
        self.opened.append(conn)
        return conn

    def close(self) -> None:
        for conn in (self.state, self.serving, *self.opened):
            conn.close()


@pytest.fixture
def make_runs(tmp_path: Path) -> Iterator[Callable[..., Runs]]:
    made: list[Runs] = []

    def make(launch: str = LAUNCH) -> Runs:
        runs = Runs(tmp_path / f"stream-{len(made) + 1}", launch)
        made.append(runs)
        return runs

    yield make
    for runs in made:
        runs.close()


def rows(conn: sqlite3.Connection, table: str) -> list[tuple]:
    return conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()


def tables(conn: sqlite3.Connection) -> dict[str, list[tuple]]:
    """Every row of every table of a serving database."""
    return {table: rows(conn, table) for table in TABLES}


def daily_days(conn: sqlite3.Connection, name: str) -> list[str]:
    """The days that `daily` holds for the project `name`, as YYYY-MM-DD."""
    found = conn.execute(
        "SELECT day FROM daily WHERE project_id = ? ORDER BY day", (IDS[name],)
    )
    return [day_date(day).isoformat() for (day,) in found]


class OneAtATime:
    """The sources of a rebuild as a run provides them: each pass copies each database
    into a folder of its own, yields the copy, and deletes it before it makes the next.
    A rebuild that kept one to read again, or listed them first, would find it gone.
    `passes` counts the passes."""

    def __init__(self, paths: list[Path], folder: Path) -> None:
        self.paths = paths
        self.folder = folder
        self.passes = 0

    def __call__(self) -> Iterator[Path]:
        self.passes += 1
        for path in self.paths:
            fetched = self.folder / f"pass-{self.passes}" / path.name
            fetched.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, fetched)
            yield fetched
            fetched.unlink()


# ---- folding days


def test_fold_sums_days_into_months_and_monthly(make_runs):
    runs = make_runs()
    first = runs.count(
        "2026-11-11T09:00:00Z",
        {
            "2026-11-10": {"requests": (100, 10), "numpy": (50, 0), "pandas": (5, 5)},
            "2026-10-31": {"requests": (80, 8), "numpy": (40, 0)},
        },
    )
    assert fold(runs.serving, runs.state, first) == [n("2026-11-10"), n("2026-10-31")]
    second = runs.count(
        "2026-11-12T09:00:00Z",
        {"2026-11-11": {"requests": (120, 12), "numpy": (60, 1), "pandas": (7, 0)}},
    )
    assert fold(runs.serving, runs.state, second) == [n("2026-11-11")]
    assert fold(runs.serving, runs.state, second) == []

    # The first delta holds three projects (seq 1 to 3) and then its two days.
    assert rows(runs.serving, "folded") == [
        (n("2026-10-31"), 5),
        (n("2026-11-10"), 4),
        (n("2026-11-11"), 6),
    ]
    assert rows(runs.serving, "monthly") == [
        (1, 202610, 80, 8),
        (1, 202611, 220, 22),
        (2, 202610, 40, 0),
        (2, 202611, 110, 1),
        (3, 202611, 12, 5),
    ]
    assert rows(runs.serving, "months") == [
        (202610, 1, 120, 8),
        (202611, 2, 342, 28),
    ]
    check(runs.serving, runs.state)


def test_recent_keeps_the_thirty_days_up_to_the_newest(make_runs):
    runs = make_runs(launch="2026-11-20")
    counts = {"requests": (10, 1), "numpy": (5, 0)}
    first = runs.count(
        "2026-11-21T09:00:00Z", {"2026-11-20": counts, "2026-11-01": counts}
    )
    fold(runs.serving, runs.state, first)
    recent_days = "SELECT DISTINCT day FROM recent ORDER BY day"
    assert runs.serving.execute(recent_days).fetchall() == [
        (n("2026-11-01"),),
        (n("2026-11-20"),),
    ]

    # 2026-12-01 is the newest day now, so the window starts on 2026-11-02: 2026-11-01
    # leaves it, the backfill day 2026-11-02 is in it, and 2026-10-31 never was.
    second = runs.count(
        "2026-12-02T09:00:00Z",
        {"2026-12-01": counts, "2026-11-02": counts, "2026-10-31": counts},
    )
    fold(runs.serving, runs.state, second)
    assert rows(runs.serving, "recent") == [
        (1, n("2026-11-02"), 10, 1),
        (1, n("2026-11-20"), 10, 1),
        (1, n("2026-12-01"), 10, 1),
        (2, n("2026-11-02"), 5, 0),
        (2, n("2026-11-20"), 5, 0),
        (2, n("2026-12-01"), 5, 0),
    ]
    check(runs.serving, runs.state)


def test_fold_without_the_days_counts_asks_for_a_rebuild(make_runs):
    runs = make_runs()
    counts = {"requests": (100, 10), "numpy": (50, 0), "pandas": (5, 5)}
    first = runs.count("2026-11-11T09:00:00Z", {"2026-11-10": counts})
    for claimed in (2, 4):
        runs.state.execute("UPDATE days SET projects = ?", (claimed,))
        runs.state.commit()
        with pytest.raises(
            NeedsRebuild,
            match=f"holds 3 counts of 2026-11-10, where the state counted {claimed}",
        ):
            fold(runs.serving, runs.state, first)
        assert tables(runs.serving) == {table: [] for table in TABLES}
    runs.state.execute("UPDATE days SET projects = 3")
    runs.state.commit()
    assert fold(runs.serving, runs.state, first) == [n("2026-11-10")]

    # The database misses a day whose counts are in another run's delta.
    runs.count("2026-11-13T09:00:00Z", {"2026-11-12": counts})
    third = runs.count("2026-11-14T09:00:00Z", {"2026-11-13": counts})
    before = tables(runs.serving)
    with pytest.raises(NeedsRebuild, match="holds 0 counts of 2026-11-12"):
        fold(runs.serving, runs.state, third)
    assert tables(runs.serving) == before


# ---- month closes


def test_close_ranks_the_month_by_downloads_without_mirrors(make_runs, monkeypatch):
    # requests has the most downloads, but no more than numpy once its mirror
    # downloads are left out, and numpy comes first by name. pandas has only mirror
    # downloads.
    counts = {"requests": (150, 50), "numpy": (100, 0), "pandas": (40, 40)}
    runs = make_runs(launch="2026-12-01")
    delta = runs.count("2026-12-02T09:00:00Z", {"2026-12-01": counts})
    fold(runs.serving, runs.state, delta)
    assert close(runs.serving, runs.state, 202612) == 2
    # December closes at the first instant of 2027, so its set is 2027's.
    assert rows(runs.serving, "top_set") == [
        (2027, 1, "requests", 202612, 2),
        (2027, 2, "numpy", 202612, 1),
    ]

    monkeypatch.setattr(serving, "TOP", 1)
    fewer = make_runs(launch="2026-12-01")
    delta = fewer.count("2026-12-02T09:00:00Z", {"2026-12-01": counts})
    fold(fewer.serving, fewer.state, delta)
    assert close(fewer.serving, fewer.state, 202612) == 1
    assert rows(fewer.serving, "top_set") == [(2027, 2, "numpy", 202612, 1)]


def test_close_keeps_only_days_counted_before_the_month_ended(make_runs):
    runs = make_runs(launch="2026-12-01")
    first = runs.count(
        "2026-12-02T09:00:00Z",
        {"2026-12-01": {"requests": (80, 0), "numpy": (100, 0)}},
    )
    fold(runs.serving, runs.state, first)
    # 2026-12-31 is counted once December is over, so its counts, which would put
    # requests first and pandas in the set, are not ranked.
    late = runs.count(
        "2027-01-01T09:00:00Z",
        {"2026-12-31": {"requests": (1000, 0), "numpy": (100, 0), "pandas": (500, 0)}},
    )
    assert close(runs.serving, runs.state, 202612) == 2
    assert fold(runs.serving, runs.state, late) == [n("2026-12-31")]
    expected = [(2027, 1, "requests", 202612, 2), (2027, 2, "numpy", 202612, 1)]
    assert rows(runs.serving, "top_set") == expected

    # A rebuild ranks December by the days counted before it ended as well.
    assert tables(runs.rebuilt("2027-01-01")) == tables(runs.serving)

    # A database that folded 2026-12-31 before December closed sums it in December, so
    # the close cannot rank from it.
    folded_late = runs.rebuilt("2026-12-31", name="folded-late.db")
    with pytest.raises(
        NeedsRebuild,
        match="before 2027-01-01T00:00:00Z, .* folded and counted at or after it: 1",
    ):
        close(folded_late, runs.state, 202612)
    assert rows(folded_late, "top_set") == []
    # And one that misses 2026-12-01 would rank December without it.
    lagging = runs.empty("lagging.db")
    with pytest.raises(NeedsRebuild, match="counted before and not folded: 1;"):
        close(lagging, runs.state, 202612)
    assert rows(lagging, "top_set") == []


def test_an_entrant_has_daily_rows_from_its_entry_on(make_runs):
    runs = make_runs(launch="2026-12-01")
    december = {"requests": (100, 0), "numpy": (90, 0), "pandas": (30, 30)}
    january = {"requests": (100, 0), "numpy": (90, 0), "pandas": (500, 0)}
    fold(
        runs.serving,
        runs.state,
        runs.count("2026-12-02T09:00:00Z", {"2026-12-01": december}),
    )
    delta = runs.count("2027-01-01T09:00:00Z", {"2026-12-31": december})
    assert close(runs.serving, runs.state, 202612) == 2
    fold(runs.serving, runs.state, delta)
    fold(
        runs.serving,
        runs.state,
        runs.count("2027-01-02T09:00:00Z", {"2027-01-01": january}),
    )
    # pandas enters 2027's set at January's close, so it keeps the days counted from
    # then on, and not 2027-01-01, which was counted before.
    delta = runs.count("2027-02-01T09:00:00Z", {"2027-01-31": january})
    assert close(runs.serving, runs.state, 202701) == 1
    fold(runs.serving, runs.state, delta)

    assert rows(runs.serving, "top_set") == [
        (2027, 1, "requests", 202612, 1),
        (2027, 2, "numpy", 202612, 2),
        (2027, 3, "pandas", 202701, 1),
    ]
    assert daily_days(runs.serving, "pandas") == ["2027-01-31"]
    # requests and numpy entered at 2027's first close and keep each day of 2027. 2026
    # has no set, as the stream launched in its last month, so its days have none.
    assert daily_days(runs.serving, "requests") == ["2027-01-01", "2027-01-31"]
    assert daily_days(runs.serving, "numpy") == ["2027-01-01", "2027-01-31"]
    assert rows(runs.serving, "daily")[-1] == (3, n("2027-01-31"), 500, 0)
    check(runs.serving, runs.state)


def test_the_first_members_of_a_year_have_its_earlier_days(make_runs):
    runs = make_runs()
    counts = {"requests": (100, 0), "numpy": (90, 0), "pandas": (20, 20)}
    fold(
        runs.serving,
        runs.state,
        runs.count(
            "2026-11-11T09:00:00Z", {"2026-11-10": counts, "2026-10-31": counts}
        ),
    )
    # The run that closes November rebuilds, as the update has no daily counts of the
    # days of 2026 it folded before.
    runs.count("2026-12-01T09:00:00Z", {"2026-11-30": counts, "2026-10-30": counts})
    runs.rebuild("2026-12-01")
    assert rows(runs.serving, "top_set") == [
        (2026, 1, "requests", 202611, 1),
        (2026, 2, "numpy", 202611, 2),
    ]
    every_day = ["2026-10-30", "2026-10-31", "2026-11-10", "2026-11-30"]
    assert daily_days(runs.serving, "requests") == every_day
    assert daily_days(runs.serving, "numpy") == every_day
    assert daily_days(runs.serving, "pandas") == []

    # The days of 2026 counted later are theirs too, the backfill's among them.
    fold(
        runs.serving,
        runs.state,
        runs.count(
            "2026-12-02T09:00:00Z", {"2026-12-01": counts, "2026-10-29": counts}
        ),
    )
    assert daily_days(runs.serving, "requests") == [
        "2026-10-29",
        *every_day,
        "2026-12-01",
    ]
    check(runs.serving, runs.state)


def test_a_close_that_would_need_earlier_counts_asks_for_a_rebuild(make_runs):
    runs = make_runs()
    counts = {"requests": (100, 0), "numpy": (90, 0)}
    fold(
        runs.serving,
        runs.state,
        runs.count("2026-11-11T09:00:00Z", {"2026-11-10": counts}),
    )
    runs.count("2026-12-01T09:00:00Z", {"2026-11-30": counts})
    before = tables(runs.serving)
    with pytest.raises(NeedsRebuild, match="first members of 2026's top set"):
        close(runs.serving, runs.state, 202611)
    assert tables(runs.serving) == before
    # The state is detached again, no transaction is left open, and the fold left no
    # table behind.
    attached = runs.serving.execute("PRAGMA database_list").fetchall()
    assert "state" not in [name for _, name, _ in attached]
    assert not runs.serving.in_transaction
    assert runs.serving.execute("SELECT name FROM temp.sqlite_master").fetchall() == []


# ---- the check


def folded_through_november(make_runs) -> Runs:
    """A stream that launched on 2026-11-10 and has closed November, with daily counts
    for 2026's set, by a rebuild, as its first close has them."""
    runs = make_runs()
    counts = {"requests": (100, 10), "numpy": (90, 0), "pandas": (20, 20)}
    runs.count("2026-11-11T09:00:00Z", {"2026-11-10": counts, "2026-10-31": counts})
    runs.count("2026-12-01T09:00:00Z", {"2026-11-30": counts})
    runs.rebuild("2026-12-01")
    check(runs.serving, runs.state)
    return runs


@pytest.mark.parametrize("column", ["days", "downloads", "mirror_downloads"])
def test_check_finds_a_month_that_disagrees_with_the_state(make_runs, column):
    runs = folded_through_november(make_runs)
    runs.serving.execute(
        f"UPDATE months SET {column} = {column} + 1 WHERE month = 202611"
    )
    runs.serving.commit()
    with pytest.raises(
        ServingMismatch, match="^months disagrees with the state's days in 2026-11$"
    ):
        check(runs.serving, runs.state)


@pytest.mark.parametrize(
    ("tamper", "reason"),
    [
        pytest.param(
            f"UPDATE folded SET seq = 99 WHERE day = {n('2026-11-10')}",
            "^folded is not the state's days and seqs, on 2026-11-10$",
            id="folded-seq",
        ),
        pytest.param(
            f"DELETE FROM folded WHERE day = {n('2026-10-31')}",
            "^folded is not the state's days and seqs, on 2026-10-31$",
            id="folded-day",
        ),
        pytest.param(
            "UPDATE monthly SET downloads = downloads + 1 "
            "WHERE project_id = 1 AND month = 202611",
            "^monthly does not sum to months in 2026-11$",
            id="monthly-sum",
        ),
        pytest.param(
            "INSERT INTO monthly VALUES (1, 202605, 1, 0)",
            "^monthly does not sum to months in 2026-05$",
            id="monthly-month",
        ),
        pytest.param(
            "DELETE FROM recent WHERE project_id = 3",
            "^recent .* on 2026-11-10, 2026-11-30$",
            id="recent-missing",
        ),
        pytest.param(
            f"INSERT INTO recent VALUES (4, {n('2026-11-10')}, 0, 0)",
            "^recent .* on 2026-11-10$",
            id="recent-row",
        ),
        pytest.param(
            f"INSERT INTO recent VALUES (1, {n('2026-11-01')}, 1, 0)",
            "^recent .* on 2026-11-01$",
            id="recent-unfolded",
        ),
        pytest.param(
            f"INSERT INTO recent VALUES (1, {n('2026-10-31')}, 100, 10)",
            "^recent .* on 2026-10-31$",
            id="recent-outside",
        ),
        pytest.param(
            f"INSERT INTO daily VALUES (3, {n('2026-11-10')}, 20, 20)",
            r"^daily holds rows of 2026 for projects outside its top set \(1\)$",
            id="daily-outsider",
        ),
        pytest.param(
            "UPDATE top_set SET year = 2027",
            r"^daily holds rows of 2026 for projects outside its top set \(6\)$",
            id="daily-year",
        ),
        pytest.param(
            f"INSERT INTO daily VALUES (1, {n('2027-01-05')}, 1, 0)",
            r"^daily holds rows of 2027 for projects outside its top set \(1\)$",
            id="daily-later-year",
        ),
        pytest.param(
            f"INSERT INTO daily VALUES (1, {n('2026-11-15')}, 1, 0)",
            r"^daily holds rows of days that are not folded \(1\)$",
            id="daily-unfolded",
        ),
    ],
)
def test_check_finds_a_table_that_disagrees_with_the_state(make_runs, tamper, reason):
    runs = folded_through_november(make_runs)
    runs.serving.execute(tamper)
    runs.serving.commit()
    with pytest.raises(ServingMismatch, match=reason):
        check(runs.serving, runs.state)


# ---- the update and the rebuild


def downloads_on(day: str) -> Counts:
    """What the three projects downloaded on `day`. requests leads November once mirror
    downloads are left out, numpy leads December, and pandas has only mirror downloads
    until January, when it ties requests. The last days of November and December are
    counted once their month is over, and would change its ranking if they were
    ranked."""
    if day == "2026-11-30":
        return {"requests": (100, 0), "numpy": (9000, 0), "pandas": (10, 10)}
    if day == "2026-12-31":
        return {"requests": (100, 0), "numpy": (100, 0), "pandas": (9000, 0)}
    if day >= "2027-01":
        return {"requests": (500, 0), "numpy": (400, 0), "pandas": (600, 100)}
    if day >= "2026-12":
        return {"requests": (700, 0), "numpy": (900, 50), "pandas": (40, 40)}
    return {"requests": (1000, 100), "numpy": (800, 0), "pandas": (60, 60)}


def update(runs: Runs, at: str, days: list[str], *, closes: int | None = None) -> str:
    """A run at `at` that counts `days`, forward days first and then the backfill's,
    and brings the serving database up to date as the stream does: it closes the month
    `closes`, when one ended since the last run, before it folds, and rebuilds from
    every delta when either asks for it. It checks the result, and returns whether it
    was "updated" or "rebuilt"."""
    delta = runs.count(at, {day: downloads_on(day) for day in days})
    try:
        if closes is not None:
            close(runs.serving, runs.state, closes)
        fold(runs.serving, runs.state, delta)
        outcome = "updated"
    except NeedsRebuild:
        runs.rebuild(at[: len("YYYY-MM-DD")])
        outcome = "rebuilt"
    check(runs.serving, runs.state)
    return outcome


def test_update_and_rebuild_agree_across_closes(make_runs):
    runs = make_runs()
    outcomes = [
        update(runs, "2026-11-11T09:00:00Z", ["2026-11-10"]),
        update(runs, "2026-11-12T09:00:00Z", ["2026-11-11", "2026-11-09"]),
        update(runs, "2026-11-30T09:00:00Z", ["2026-11-29", "2026-11-08"]),
        # November closes: 2026's first close, which needs the daily counts of the
        # days of 2026 folded already, and only a rebuild has those.
        update(
            runs, "2026-12-01T09:00:00Z", ["2026-11-30", "2026-10-31"], closes=202611
        ),
        update(runs, "2026-12-02T09:00:00Z", ["2026-12-01", "2026-10-30"]),
        update(runs, "2026-12-31T09:00:00Z", ["2026-12-30", "2026-10-29"]),
        # December closes: 2027's first close, before any day of 2027 is counted.
        update(
            runs, "2027-01-01T09:00:00Z", ["2026-12-31", "2026-10-28"], closes=202612
        ),
        update(runs, "2027-01-02T09:00:00Z", ["2027-01-01", "2026-10-27"]),
        # A day of 2025, a year before launch, which has no set.
        update(runs, "2027-01-31T09:00:00Z", ["2027-01-30", "2025-12-31"]),
        # January closes, and pandas enters 2027's set.
        update(
            runs, "2027-02-01T09:00:00Z", ["2027-01-31", "2026-10-26"], closes=202701
        ),
    ]
    assert outcomes == ["updated"] * 3 + ["rebuilt"] + ["updated"] * 6

    sources = OneAtATime(runs.deltas, runs.folder / "fetched")
    rebuilt = runs.rebuilt("2027-02-01", sources=sources)
    assert sources.passes == 2
    assert tables(runs.serving) == tables(rebuilt)
    check(rebuilt, runs.state)

    top = runs.serving.execute(
        "SELECT year, name, entered, rank FROM top_set ORDER BY year, rank, name"
    )
    assert top.fetchall() == [
        (2026, "requests", 202611, 1),
        (2026, "numpy", 202611, 2),
        (2027, "numpy", 202612, 1),
        (2027, "pandas", 202701, 1),
        (2027, "requests", 202612, 2),
    ]
    counted = runs.state.execute(
        "SELECT day FROM days WHERE day >= ? ORDER BY day", (n("2026-01-01"),)
    )
    since_2026 = [day_date(day).isoformat() for (day,) in counted]
    assert daily_days(runs.serving, "requests") == since_2026
    assert daily_days(runs.serving, "numpy") == since_2026
    assert daily_days(runs.serving, "pandas") == ["2027-01-31"]
    recent = runs.serving.execute("SELECT DISTINCT day FROM recent ORDER BY day")
    assert recent.fetchall() == [(n("2027-01-30"),), (n("2027-01-31"),)]
    months = runs.serving.execute("SELECT month, days FROM months ORDER BY month")
    assert months.fetchall() == [
        (202512, 1),
        (202610, 6),
        (202611, 6),
        (202612, 3),
        (202701, 3),
    ]
