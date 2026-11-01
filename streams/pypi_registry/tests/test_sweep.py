"""The sweep: the one-time read of the page of every project that no run has read in
full, a time budget at a time, and what it leaves for the daily run.

Each test starts from a stream that the bootstrap built on a fake GitHub, and sweeps it
with pages that a fake PyPI serves, as a run of the stream does.
"""

import dataclasses
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from obskit.github import GitHub
from obskit.http import Http
from obskit.manifest import Watermark
from obskit.release import PriorStateError
from obskit.sqlite import table_counts
from pypi_registry.run import RunReport, main, sweep
from pypi_registry.simple import ROOT_URL, project_url, version_url
from run_helpers import (
    CURRENT,
    DELTA,
    MANIFEST,
    NOVEMBER,
    PART_NOVEMBER,
    STATE,
    Stream,
    dist,
    files_of,
    meta_of,
    page,
    project_rows,
    restored_differences,
    sealed_name,
    versions_of,
    written,
)

# When the stream is bootstrapped, an upload before that and one after it, and the time
# of the sweeps, which are on the day of the bootstrap unless a test says otherwise.
BOOTSTRAP = "2026-11-02T06:00:00Z"
OLD = "2026-01-01T00:00:00Z"
NEW = "2026-11-02T09:00:00Z"
SWEPT_AT = "2026-11-02T12:00:00Z"
# The projects that the stream starts with, with their serials in the bootstrap's root.
PROJECTS = {"alpha": 10, "beta": 20, "gamma": 30}


def bootstrapped(tmp_path: Path, projects: dict[str, int] = PROJECTS) -> Stream:
    """A stream that the bootstrap built from a root listing `projects`. The request of
    that root is forgotten, so a test counts the requests of its sweeps alone."""
    stream = Stream(tmp_path)
    stream.run(BOOTSTRAP, 100, projects, bootstrap=True)
    stream.pypi.requests.clear()
    return stream


def requested(stream: Stream) -> list[str]:
    """The URLs that PyPI was asked for, in order."""
    return [url for url, _ in stream.pypi.requests]


def applied(stream: Stream) -> int:
    """The highest seq that the published state has applied."""
    return int(meta_of(stream.open(CURRENT, STATE))["applied_seq"])


def report_of(**counted: int) -> RunReport:
    """The report of a run that counted `counted`, and 0 of the rest."""
    zeros = {field.name: 0 for field in dataclasses.fields(RunReport)}
    return RunReport(**{**zeros, "gate_failures": [], **counted})


def test_sweep_marks_projects_swept_with_inexact_files(tmp_path):
    stream = bootstrapped(tmp_path)
    pages = [
        page(
            "alpha",
            10,
            dist("alpha-1.0.tar.gz", OLD),
            dist("alpha-1.0-py3-none-any.whl", OLD),
            versions=["1.0"],
        ),
        # beta changed after the root was read: its page has a higher serial. It has a
        # yanked file, and a status that no event ever announced.
        page(
            "beta",
            22,
            dist("beta-0.1.tar.gz", OLD, yanked="broken"),
            versions=["0.1"],
            status="archived",
        ),
        page("gamma", 30),
    ]

    report = stream.sweep(SWEPT_AT, pages=pages)

    # A project's files come first, then its versions, then its swept row, which carries
    # the status of the page. Every file is from before the bootstrap, so none is exact,
    # and no version has a record. There is no event, no serial and no cursor.
    assert written(stream.open(CURRENT, DELTA), 0) == [
        ("file", "alpha", 1, "alpha-1.0-py3-none-any.whl", 0),
        ("file", "alpha", 1, "alpha-1.0.tar.gz", 0),
        ("version", "alpha", 1, "1.0", 0),
        ("swept", "alpha", 1, "active"),
        ("file", "beta", 1, "beta-0.1.tar.gz", 0),
        ("version", "beta", 1, "0.1", 0),
        ("swept", "beta", 1, "archived"),
        ("swept", "gamma", 1, "active"),
    ]
    # No root is read, and no release record is asked for.
    assert requested(stream) == [project_url(name) for name in PROJECTS]
    assert report == report_of(requests=3, swept=3)

    state = stream.open(CURRENT, STATE)
    # Each project is swept, and beta has the status of its page. Each keeps the serial
    # that the root gave it, whatever its page said, and is still first seen at the
    # bootstrap, and not exactly: nothing says when it appeared.
    assert project_rows(state) == {
        "alpha": (1, 10, "active", 1),
        "beta": (1, 20, "archived", 1),
        "gamma": (1, 30, "active", 1),
    }
    first_seen = state.execute(
        "SELECT name, first_seen, first_seen_exact FROM projects ORDER BY name"
    )
    assert first_seen.fetchall() == [(name, BOOTSTRAP, 0) for name in PROJECTS]
    assert files_of(state) == [
        ("alpha", 1, "alpha-1.0-py3-none-any.whl", 0, None),
        ("alpha", 1, "alpha-1.0.tar.gz", 0, None),
        ("beta", 1, "beta-0.1.tar.gz", 1, "broken"),
    ]
    assert versions_of(state) == [("alpha", 1, "1.0", 0), ("beta", 1, "0.1", 0)]
    assert meta_of(state) == {
        "applied_seq": "8",
        "bootstrap_at": BOOTSTRAP,
        "cursor": "100",
    }
    manifest = stream.manifest()
    assert manifest.coverage == {
        "projects": 3,
        "swept": 3,
        "unswept": 0,
        "remaining": 0,
        "bootstrap_at": BOOTSTRAP,
    }
    assert manifest.complete is True
    assert manifest.generated_at == SWEPT_AT
    # The rows replay to this state: a restore from the month's base copy and the open
    # delta gives it back, the yank and the status of beta included.
    assert restored_differences(stream) == []


def test_sweep_fetches_records_for_versions_after_bootstrap(tmp_path):
    stream = bootstrapped(tmp_path, {"alpha": 10})
    alpha = page(
        "alpha",
        12,
        dist("alpha-1.0.tar.gz", OLD),
        dist("alpha-2.0.tar.gz", NEW),
        dist("alpha-2.1.tar.gz", NEW),
        versions=["1.0", "2.0", "2.1", "3.0"],
    )

    # 2.0 has a record. The JSON API has no page for 2.1 (a 404), and 3.0 has no file.
    report = stream.sweep(SWEPT_AT, pages=[alpha], releases=[("alpha", "2.0")])

    # Only a version with a file from after the bootstrap is asked for. The releases
    # from before are the history fill's, and are not read again here. A file from
    # after the bootstrap is exact: it was there to be seen as it came.
    assert written(stream.open(CURRENT, DELTA), 0) == [
        ("file", "alpha", 1, "alpha-1.0.tar.gz", 0),
        ("file", "alpha", 1, "alpha-2.0.tar.gz", 1),
        ("file", "alpha", 1, "alpha-2.1.tar.gz", 1),
        ("version", "alpha", 1, "1.0", 0),
        ("version", "alpha", 1, "2.0", 1),
        ("record", "alpha", 1, "2.0"),
        ("version", "alpha", 1, "2.1", 0),
        ("version", "alpha", 1, "3.0", 0),
        ("swept", "alpha", 1, "active"),
    ]
    assert requested(stream) == [
        project_url("alpha"),
        version_url("alpha", "2.0"),
        version_url("alpha", "2.1"),
    ]
    assert report == report_of(requests=3, versions_fetched=2, swept=1)
    # The record is observed at the time of the sweep.
    records = stream.open(CURRENT, DELTA).execute(
        "SELECT version_raw, observed_at FROM release_records"
    )
    assert records.fetchall() == [("2.0", SWEPT_AT)]
    assert versions_of(stream.open(CURRENT, STATE)) == [
        ("alpha", 1, "1.0", 0),
        ("alpha", 1, "2.0", 1),
        ("alpha", 1, "2.1", 0),
        ("alpha", 1, "3.0", 0),
    ]


def test_sweep_exit_code_3_while_unswept_remain(tmp_path, monkeypatch, capsys):
    stream = bootstrapped(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")
    monkeypatch.setattr("pypi_registry.run.GitHub", lambda repo, token: stream.gh)
    monkeypatch.setattr("pypi_registry.run.Http", lambda: stream.http)

    def sweep_here(gh, http, workdir, *, now, max_seconds) -> RunReport:
        """What `main` calls, on the stream's clock and on the day of its bootstrap."""
        assert now.utcoffset() == timedelta(0)
        return sweep(
            gh,
            http,
            workdir,
            now=datetime.fromisoformat(SWEPT_AT),
            max_seconds=max_seconds,
            clock=stream.clock,
        )

    monkeypatch.setattr("pypi_registry.run.sweep", sweep_here)
    stream.serve([page("alpha", 10), page("beta", 20)])

    # Each request takes a second, so a second and a half has time for two projects.
    # gamma is left, and the exit code says that another run is wanted.
    assert main(["sweep", "--max-seconds", "1.5"]) == 3

    printed = json.loads(capsys.readouterr().out)
    assert (printed["swept"], printed["remaining"]) == (2, 1)
    assert stream.manifest().complete is False
    assert list((tmp_path / "work").iterdir()) == [], "the work directory is removed"

    # The run that follows reads gamma, and there is nothing left.
    stream.serve([page("gamma", 30)])

    assert main(["sweep", "--max-seconds", "1.5"]) == 0

    printed = json.loads(capsys.readouterr().out)
    assert (printed["swept"], printed["remaining"]) == (1, 0)
    assert stream.manifest().complete is True


@pytest.mark.parametrize(
    ("swept", "remaining", "code"),
    [
        (2, 1, 3),
        (7, 0, 0),
        (0, 0, 0),
        (0, 5, 0),
    ],
    ids=["some left", "none left", "nothing to do", "nothing could be swept"],
)
def test_main_sweep_exit_code(tmp_path, monkeypatch, swept, remaining, code):
    # A run that sweeps nothing while pages remain has met only pages that it cannot
    # use: missing, quarantined or stale. A run that followed at once would meet the
    # same ones, and one that dispatches itself again each time would never stop.
    report = report_of(swept=swept, remaining=remaining)
    monkeypatch.setattr("pypi_registry.run.sweep", lambda *args, **options: report)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")

    assert main(["sweep"]) == code


def test_main_run_exits_0_whatever_is_left(tmp_path, monkeypatch):
    # Only a sweep says that it is to be run again. A daily run that ran out of time
    # leaves projects to the next one, and that is no reason to run anything.
    report = report_of(swept=2, remaining=1)
    monkeypatch.setattr("pypi_registry.run.run_once", lambda *args, **options: report)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")

    assert main(["run"]) == 0


def test_main_sweeps_with_one_github_client(tmp_path, monkeypatch, capsys):
    made: list[tuple[str, str]] = []
    calls: list[dict] = []

    class Client(GitHub):
        def __init__(self, repo: str, token: str, **options) -> None:
            super().__init__(repo, token, **options)
            made.append((repo, token))

    report = RunReport(0, 0, 0, 0, 5, 6, 2, 1, [])

    def sweep_here(gh, http, workdir, **options) -> RunReport:
        assert isinstance(gh, Client)
        assert isinstance(http, Http)
        # A work directory of its own, empty, under ./work.
        assert workdir.parent == tmp_path / "work"
        assert list(workdir.iterdir()) == []
        calls.append({"gh": gh, "workdir": workdir, **options})
        return report

    monkeypatch.setattr("pypi_registry.run.GitHub", Client)
    monkeypatch.setattr("pypi_registry.run.sweep", sweep_here)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)

    assert main(["sweep"]) == 3

    assert made == [("py-observatory/data", "t0ken")]
    (call,) = calls
    # A sweep does not bootstrap, and has no override to give.
    assert set(call) == {"gh", "workdir", "now", "max_seconds"}
    assert call["max_seconds"] == 16_200
    assert call["now"].utcoffset() == timedelta(0)
    assert not call["workdir"].exists(), "the work directory is removed afterwards"
    assert json.loads(capsys.readouterr().out) == dataclasses.asdict(report)

    monkeypatch.setenv("GITHUB_REPOSITORY", "someone/data")

    assert main(["sweep", "--max-seconds", "60"]) == 3

    assert made[-1] == ("someone/data", "t0ken")
    assert calls[-1]["max_seconds"] == 60
    assert calls[-1]["gh"] is not calls[0]["gh"], "one client for each process"

    monkeypatch.delenv("GITHUB_TOKEN")
    with pytest.raises(KeyError, match="GITHUB_TOKEN"):
        main(["sweep"])


def test_sweep_skips_pages_it_cannot_use(tmp_path):
    stream = bootstrapped(
        tmp_path, {"fine": 10, "gone": 20, "held": 30, "lost": 40, "stale": 50}
    )
    # gone has no page (a 404, as a project that PyPI does not have), and lost is gone
    # for good (a 410). held is quarantined, and its page lists no files. stale is an old
    # copy: its serial is below the 50 that the state holds.
    stream.pypi.add(project_url("lost"), status=410)
    held = page("held", 31, status="quarantined")
    stale = page("stale", 45, dist("stale-1.0.tar.gz", OLD), versions=["1.0"])
    pages = [
        page("fine", 10, dist("fine-1.0.tar.gz", OLD), versions=["1.0"]),
        held,
        stale,
    ]

    report = stream.sweep(SWEPT_AT, pages=pages)

    # Nothing is written for them: no row, no serial, and no event, so a quarantine is
    # not recorded here. The daily run records it, when it sees the project leave the
    # root. Each of them is asked for, and is left for the next sweep.
    assert written(stream.open(CURRENT, DELTA), 0) == [
        ("file", "fine", 1, "fine-1.0.tar.gz", 0),
        ("version", "fine", 1, "1.0", 0),
        ("swept", "fine", 1, "active"),
    ]
    assert requested(stream) == [
        project_url(name) for name in ("fine", "gone", "held", "lost", "stale")
    ]
    assert report == report_of(requests=5, swept=1, remaining=4)
    assert project_rows(stream.open(CURRENT, STATE)) == {
        "fine": (1, 10, "active", 1),
        "gone": (1, 20, "active", 0),
        "held": (1, 30, "active", 0),
        "lost": (1, 40, "active", 0),
        "stale": (1, 50, "active", 0),
    }
    manifest = stream.manifest()
    assert manifest.coverage["unswept"] == manifest.coverage["remaining"] == 4
    assert manifest.complete is False

    # The next sweep tries them again. The old copy has been replaced by then, and is
    # swept. The others are as they were.
    before = applied(stream)
    stream.pypi.requests.clear()
    fresh = page("stale", 50, dist("stale-1.0.tar.gz", OLD), versions=["1.0"])

    report = stream.sweep(SWEPT_AT, pages=[held, fresh])

    assert written(stream.open(CURRENT, DELTA), before) == [
        ("file", "stale", 1, "stale-1.0.tar.gz", 0),
        ("version", "stale", 1, "1.0", 0),
        ("swept", "stale", 1, "active"),
    ]
    assert requested(stream) == [
        project_url(name) for name in ("gone", "held", "lost", "stale")
    ]
    assert report == report_of(requests=4, swept=1, remaining=3)


def test_sweep_leaves_removed_and_quarantined_projects_alone(tmp_path):
    stream = bootstrapped(tmp_path, {**PROJECTS, "delta": 40})
    # A daily run reads a root without gamma and delta. gamma has no page, so it is
    # removed. delta is quarantined. Neither has been read in full.
    stream.run(
        "2026-11-02T08:00:00Z",
        110,
        {"alpha": 10, "beta": 20},
        pages=[page("delta", 41, status="quarantined")],
    )
    stream.pypi.requests.clear()

    report = stream.sweep(SWEPT_AT, pages=[page("alpha", 10), page("beta", 20)])

    # The sweep asks for neither of them, though they are not swept.
    assert requested(stream) == [project_url("alpha"), project_url("beta")]
    assert report == report_of(requests=2, swept=2)
    assert project_rows(stream.open(CURRENT, STATE)) == {
        "alpha": (1, 10, "active", 1),
        "beta": (1, 20, "active", 1),
        "gamma": (1, 30, "removed", 0),
        "delta": (1, 40, "quarantined", 0),
    }
    # And they are not what is left: with alpha and beta swept, nothing is.
    manifest = stream.manifest()
    assert manifest.coverage["projects"] == 2
    assert (manifest.coverage["unswept"], manifest.coverage["remaining"]) == (0, 0)
    assert manifest.complete is True


def test_sweep_stops_at_its_budget_and_the_next_run_goes_on(tmp_path):
    stream = bootstrapped(tmp_path)

    # Each request takes a second, and the budget is checked before each project: two
    # start within a second and a half, and the third does not.
    report = stream.sweep(
        SWEPT_AT, pages=[page("alpha", 10), page("beta", 20)], max_seconds=1.5
    )

    assert requested(stream) == [project_url("alpha"), project_url("beta")]
    assert report == report_of(requests=2, swept=2, remaining=1)
    manifest = stream.manifest()
    assert manifest.complete is False
    assert (manifest.coverage["remaining"], manifest.coverage["unswept"]) == (1, 1)
    assert project_rows(stream.open(CURRENT, STATE)) == {
        "alpha": (1, 10, "active", 1),
        "beta": (1, 20, "active", 1),
        "gamma": (1, 30, "active", 0),
    }

    # The next sweep reads the one that is left, and none of the two again.
    stream.pypi.requests.clear()

    report = stream.sweep(SWEPT_AT, pages=[page("gamma", 30)])

    assert requested(stream) == [project_url("gamma")]
    assert report == report_of(requests=1, swept=1)
    manifest = stream.manifest()
    assert manifest.complete is True
    assert (manifest.coverage["remaining"], manifest.coverage["unswept"]) == (0, 0)

    # With nothing left to read, a sweep reads nothing, and publishes as before.
    stream.pypi.requests.clear()

    assert stream.sweep(SWEPT_AT) == report_of()
    assert requested(stream) == []
    assert stream.manifest().complete is True


def test_sweep_keeps_the_cursor_and_data_through_of_the_last_run(tmp_path):
    stream = bootstrapped(tmp_path)
    # A daily run reads a root at serial 110. A project has appeared in it, and is read
    # in full as a new one, so the sweep has nothing to read for it.
    new = page("delta", 105, dist("delta-1.0.tar.gz", NEW), versions=["1.0"])
    stream.run(
        "2026-11-02T08:00:00Z",
        110,
        {**PROJECTS, "delta": 105},
        pages=[new],
        releases=[("delta", "1.0")],
    )
    before = applied(stream)
    stream.pypi.requests.clear()

    stream.sweep(
        SWEPT_AT, pages=[page(name, serial) for name, serial in PROJECTS.items()]
    )

    # The sweep reads no root, so it has no serial to write. It writes no serial row and
    # no cursor row, and the project keeps the serial that the root gave it.
    assert requested(stream) == [project_url(name) for name in PROJECTS]
    assert written(stream.open(CURRENT, DELTA), before) == [
        ("swept", "alpha", 1, "active"),
        ("swept", "beta", 1, "active"),
        ("swept", "gamma", 1, "active"),
    ]
    state = stream.open(CURRENT, STATE)
    assert meta_of(state)["cursor"] == "110"
    assert project_rows(state)["alpha"] == (1, 10, "active", 1)
    # The manifest goes on from the last run's: the same cursor and data_through.
    manifest = stream.manifest()
    assert manifest.cursor == "110"
    assert manifest.data_through == Watermark("110", "serial")


def test_sweep_and_daily_runs_share_a_month_that_restores(tmp_path):
    stream = Stream(tmp_path)
    projects = {"alpha": 10, "beta": 20, "gamma": 30, "delta": 40}
    stream.run("2026-11-29T06:00:00Z", 100, projects, bootstrap=True)
    uploaded = "2026-11-29T09:00:00Z"

    # 11-29: a sweep with time for two projects reads alpha and beta.
    first = stream.sweep(
        "2026-11-29T12:00:00Z",
        pages=[
            page("alpha", 10, dist("alpha-1.0.tar.gz", OLD), versions=["1.0"]),
            page("beta", 20, dist("beta-0.1.tar.gz", OLD), versions=["0.1"]),
        ],
        max_seconds=1.5,
    )

    assert (first.swept, first.remaining) == (2, 2)
    assert restored_differences(stream) == []

    # 11-30: a daily run. A file of alpha is yanked, which is a change to a project that
    # the sweep read. gamma, which it did not reach, has a new file, and is read in full.
    stream.run(
        "2026-11-30T06:00:00Z",
        110,
        {**projects, "alpha": 11, "gamma": 31},
        pages=[
            page(
                "alpha",
                11,
                dist("alpha-1.0.tar.gz", OLD, yanked="broken"),
                versions=["1.0"],
            ),
            page(
                "gamma",
                31,
                dist("gamma-1.0.tar.gz", OLD),
                dist("gamma-1.1.tar.gz", uploaded),
                versions=["1.0", "1.1"],
            ),
        ],
        releases=[("gamma", "1.1")],
    )

    assert restored_differences(stream) == []

    # 12-01: the sweep reads the last project. First November closes: its two deltas,
    # one with the sweep's rows and one with the daily run's, merge into the month's
    # part, which refuses a key that comes twice.
    last = stream.sweep(
        "2026-12-01T06:00:00Z",
        pages=[page("delta", 40, dist("delta-1.0.tar.gz", OLD), versions=["1.0"])],
    )

    assert (last.swept, last.remaining) == (1, 0)
    assert restored_differences(stream) == []
    assert sealed_name("2026-11-29") in stream.assets(NOVEMBER)
    assert sealed_name("2026-11-30") in stream.assets(NOVEMBER)
    assert table_counts(stream.expand(NOVEMBER, PART_NOVEMBER)) == {
        "cursors": 1,
        "events": 1,
        "files": 4,
        "release_records": 1,
        "serials": 2,
        "swept": 3,
        "versions": 4,
    }
    assert project_rows(stream.open(CURRENT, STATE)) == {
        "alpha": (1, 11, "active", 1),
        "beta": (1, 20, "active", 1),
        "gamma": (1, 31, "active", 1),
        "delta": (1, 40, "active", 1),
    }
    assert stream.manifest().complete is True


def test_sweep_refuses_a_stream_that_has_never_published(tmp_path):
    stream = Stream(tmp_path)

    # The sweep never builds a first state: only the daily run's bootstrap does.
    with pytest.raises(PriorStateError, match="no release pypi-registry-current"):
        stream.sweep(SWEPT_AT)

    assert stream.writes() == []
    assert stream.pypi.requests == []


def test_sweep_never_starts_a_state(tmp_path, monkeypatch):
    stream = Stream(tmp_path)
    # `fetch_prior` finds no state only for a bootstrap. Were it to find none for a run
    # that is not one, the sweep would stop, and not go on with no state at all.
    monkeypatch.setattr("pypi_registry.run.fetch_prior", lambda *args, **options: None)

    with pytest.raises(PriorStateError, match="no published state to sweep"):
        stream.sweep(SWEPT_AT)

    assert stream.writes() == []
    assert stream.pypi.requests == []


def test_sweep_publishes_nothing_when_a_page_is_malformed(tmp_path):
    stream = bootstrapped(tmp_path)
    held = stream.github.asset_bytes(CURRENT, MANIFEST)
    stream.github.calls.clear()
    stream.pypi.add(project_url("beta"), body=b"<html>maintenance</html>", times=1)

    # alpha is read before beta, and the page of beta ends the run.
    with pytest.raises(ValueError, match="not JSON"):
        stream.sweep(SWEPT_AT, pages=[page("alpha", 10)])

    assert stream.writes() == []
    assert stream.github.asset_bytes(CURRENT, MANIFEST) == held
    assert project_rows(stream.open(CURRENT, STATE))["alpha"] == (1, 10, "active", 0)

    # The next sweep starts again, from alpha, which was never swept.
    report = stream.sweep(
        SWEPT_AT, pages=[page(name, serial) for name, serial in PROJECTS.items()]
    )

    assert report == report_of(requests=3, swept=3)
    assert ROOT_URL not in requested(stream)
