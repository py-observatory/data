"""The records pass: the record of each project's current release, read from PyPI's JSON
API when the registry has none, a time budget at a time.

Each test starts from a stream that the bootstrap built on a fake GitHub and a sweep read
in full, with files from before the bootstrap, so that no version has a record. It then
runs the pass with the records that a fake PyPI serves, as a run of the stream does.
"""

import json
from datetime import datetime
from pathlib import Path

import pytest

from obskit.http import UpstreamError
from obskit.release import PriorStateError
from obskit.sqlite import table_counts
from opened_connections import OPENED
from pypi_registry.process import current_version
from pypi_registry.run import SWEEP_INTERVAL, RecordsReport, main, records
from pypi_registry.simple import version_url
from pypi_registry.store import STATE_SCHEMA, open_db
from run_helpers import (
    CURRENT,
    DELTA,
    NOVEMBER,
    PART_NOVEMBER,
    STATE,
    Stream,
    dist,
    meta_of,
    page,
    project_rows,
    release,
    restored_differences,
    versions_of,
    written,
)

# The day of the bootstrap and of the sweep, unless a test says otherwise, and on it:
# when the stream is bootstrapped, an upload after that, the time of the sweep that reads
# every page, and the time of the records runs. OLD is an upload before the bootstrap.
DAY = "2026-11-02"
BOOTSTRAP = f"{DAY}T06:00:00Z"
NEW = f"{DAY}T09:00:00Z"
SWEPT_AT = f"{DAY}T12:00:00Z"
RECORDED_AT = f"{DAY}T18:00:00Z"
OLD = "2026-01-01T00:00:00Z"


def swept(tmp_path: Path, pages: list[dict], day: str = DAY) -> Stream:
    """A stream that the bootstrap built on `day` from the projects of `pages`, and that
    a sweep later that day read in full from them. The requests so far are forgotten,
    so a test counts those of its records runs alone."""
    stream = Stream(tmp_path)
    projects = {body["name"]: body["meta"]["_last-serial"] for body in pages}
    stream.run(f"{day}T06:00:00Z", 100, projects, bootstrap=True)
    stream.sweep(f"{day}T12:00:00Z", pages=pages)
    stream.pypi.requests.clear()
    return stream


def old_release(name: str, serial: int, *versions: str) -> dict:
    """The page of a project with one sdist of each of `versions`, all uploaded before
    the bootstrap, so that the sweep asks for no record."""
    files = [dist(f"{name}-{version}.tar.gz", OLD) for version in versions]
    return page(name, serial, *files, versions=list(versions))


def requested(stream: Stream) -> list[str]:
    """The URLs that PyPI was asked for, in order."""
    return [url for url, _ in stream.pypi.requests]


def applied(stream: Stream) -> int:
    """The highest seq that the published state has applied."""
    return int(meta_of(stream.open(CURRENT, STATE))["applied_seq"])


def progress(stream: Stream) -> tuple[object, object]:
    """The records pass's progress as the current manifest's coverage gives it."""
    coverage = stream.manifest().coverage
    return coverage["records_after"], coverage["records_left"]


def test_current_version_skips_yanked_files_and_prefers_a_final_release(tmp_path):
    state = open_db(tmp_path / "state.db", STATE_SCHEMA, create=True)
    OPENED.append(state)
    files = [
        # alpha offers 1.0 and a pre-release above it, which a final release beats. 1.5
        # has only a yanked file, and the .exe carries no version that can be read.
        ("alpha", 1, "alpha-1.0.tar.gz", 0, None),
        ("alpha", 1, "alpha-1.0-py3-none-any.whl", 0, None),
        ("alpha", 1, "alpha-2.0rc1.tar.gz", 0, None),
        ("alpha", 1, "alpha-1.5.tar.gz", 1, "broken"),
        ("alpha", 1, "alpha-3.0.win32.exe", 0, None),
        # A file of another incarnation is not a file of this one.
        ("alpha", 2, "alpha-9.0.tar.gz", 0, None),
        # beta's 1.1 has a yanked sdist and a wheel that is not, so it is offered.
        ("beta", 1, "beta-1.0.tar.gz", 0, None),
        ("beta", 1, "beta-1.1.tar.gz", 1, "broken"),
        ("beta", 1, "beta-1.1-py3-none-any.whl", 0, None),
        # gamma offers only a pre-release, and delta offers nothing.
        ("gamma", 1, "gamma-2.0rc1.tar.gz", 0, None),
        ("gamma", 1, "gamma-1.0.tar.gz", 1, "broken"),
        ("delta", 1, "delta-1.0.tar.gz", 1, "broken"),
        # The version is the canonical one, as the state keeps versions.
        ("eta", 1, "eta-1.0.0.RC1.tar.gz", 0, None),
    ]
    state.executemany("INSERT INTO files_current VALUES (?, ?, ?, ?, ?)", files)

    assert current_version(state, "alpha", 1) == "1.0"
    assert current_version(state, "alpha", 2) == "9.0"
    assert current_version(state, "beta", 1) == "1.1"
    assert current_version(state, "gamma", 1) == "2.0rc1"
    assert current_version(state, "delta", 1) is None
    assert current_version(state, "eta", 1) == "1.0.0rc1"
    assert current_version(state, "nothing", 1) is None


def test_a_project_without_its_current_record_gets_one(tmp_path):
    alpha = page(
        "alpha",
        10,
        dist("alpha-1.0.tar.gz", OLD),
        dist("alpha-2.0.tar.gz", OLD),
        dist("alpha-2.0-py3-none-any.whl", OLD),
        versions=["1.0", "2.0"],
    )
    stream = swept(tmp_path, [alpha])
    before = applied(stream)

    report = stream.records(RECORDED_AT, releases=[("alpha", "2.0")])

    # Only the current release's record is asked for, and only that record is written.
    # The version is in the state already, and a second row for it would be a key that
    # the month's merge of deltas refuses.
    assert report == RecordsReport(
        processed=1, recorded=1, missing=0, refused=0, left=0
    )
    assert requested(stream) == [version_url("alpha", "2.0")]
    assert written(stream.open(CURRENT, DELTA), before) == [
        ("record", "alpha", 1, "2.0"),
    ]
    recorded = stream.open(CURRENT, DELTA).execute(
        "SELECT version_raw, observed_at FROM release_records"
    )
    assert recorded.fetchall() == [("2.0", RECORDED_AT)]
    # Applying the record marks the version. The older one keeps no record.
    assert versions_of(stream.open(CURRENT, STATE)) == [
        ("alpha", 1, "1.0", 0),
        ("alpha", 1, "2.0", 1),
    ]
    # The pass reached the end of the list, so it has nothing left and starts again
    # from the first name next time.
    assert progress(stream) == (None, 0)
    assert restored_differences(stream) == []


def test_a_current_version_that_has_a_record_is_not_fetched(tmp_path):
    pages = [
        # alpha's current release is from after the bootstrap, so the sweep read its
        # record. Its old release has none, which is why the pass comes to alpha.
        page(
            "alpha",
            10,
            dist("alpha-1.0.tar.gz", OLD),
            dist("alpha-2.0.tar.gz", NEW),
            versions=["1.0", "2.0"],
        ),
        # beta's current file is of a version that its page does not list, so the state
        # has not seen it, and a record of it could not be applied.
        page(
            "beta",
            20,
            dist("beta-1.0.tar.gz", OLD),
            dist("beta-3.0.tar.gz", OLD),
            versions=["1.0"],
        ),
        # gamma has a version and no file, so it has no current release.
        page("gamma", 30, versions=["1.0"]),
    ]
    stream = Stream(tmp_path)
    stream.run(BOOTSTRAP, 100, {"alpha": 10, "beta": 20, "gamma": 30}, bootstrap=True)
    stream.sweep(SWEPT_AT, pages=pages, releases=[("alpha", "2.0")])
    stream.pypi.requests.clear()
    before = applied(stream)

    report = stream.records(RECORDED_AT)

    assert report == RecordsReport(
        processed=3, recorded=0, missing=0, refused=0, left=0
    )
    assert requested(stream) == []
    assert written(stream.open(CURRENT, DELTA), before) == []
    assert versions_of(stream.open(CURRENT, STATE)) == [
        ("alpha", 1, "1.0", 0),
        ("alpha", 1, "2.0", 1),
        ("beta", 1, "1.0", 0),
        ("gamma", 1, "1.0", 0),
    ]
    assert progress(stream) == (None, 0)


@pytest.mark.parametrize("status", [404, 410])
def test_a_missing_record_writes_nothing(tmp_path, status):
    stream = swept(tmp_path, [old_release("alpha", 10, "1.0")])
    stream.pypi.add(version_url("alpha", "1.0"), status=status)
    before = applied(stream)

    report = stream.records(RECORDED_AT)

    # PyPI has no record of the release. Nothing is written, and the version still has
    # no record. The run publishes all the same, with its progress.
    assert report == RecordsReport(
        processed=1, recorded=0, missing=1, refused=0, left=0
    )
    assert requested(stream) == [version_url("alpha", "1.0")]
    assert written(stream.open(CURRENT, DELTA), before) == []
    assert versions_of(stream.open(CURRENT, STATE)) == [("alpha", 1, "1.0", 0)]
    assert stream.manifest().generated_at == RECORDED_AT
    assert progress(stream) == (None, 0)


REFUSED_REPLIES = [
    pytest.param(b"<html>", id="not JSON"),
    pytest.param({"info": "beta 1.0", "urls": []}, id="a record of the wrong shape"),
    pytest.param(release("other", "1.0"), id="another project's record"),
    pytest.param(release("beta", "2.0"), id="another release's record"),
]


@pytest.mark.parametrize("reply", REFUSED_REPLIES)
def test_a_refused_record_is_counted_and_the_pass_goes_on(tmp_path, reply):
    stream = swept(
        tmp_path,
        [
            old_release("alpha", 10, "1.0"),
            old_release("beta", 20, "1.0"),
            old_release("gamma", 30, "1.0"),
            old_release("zeta", 40, "1.0"),
        ],
    )
    stream.pypi.add(version_url("beta", "1.0"), body=reply, times=1)
    before = applied(stream)

    # Each request takes a second, so two and a half seconds has time for three
    # projects, and zeta is left.
    report = stream.records(
        RECORDED_AT, releases=[("alpha", "1.0"), ("gamma", "1.0")], max_seconds=2.5
    )

    # beta's reply makes no record. Nothing is written for it, its version still has no
    # record, and the pass goes on to gamma.
    assert report == RecordsReport(
        processed=3, recorded=2, missing=0, refused=1, left=1
    )
    assert requested(stream) == [
        version_url("alpha", "1.0"),
        version_url("beta", "1.0"),
        version_url("gamma", "1.0"),
    ]
    assert written(stream.open(CURRENT, DELTA), before) == [
        ("record", "alpha", 1, "1.0"),
        ("record", "gamma", 1, "1.0"),
    ]
    assert versions_of(stream.open(CURRENT, STATE)) == [
        ("alpha", 1, "1.0", 1),
        ("beta", 1, "1.0", 0),
        ("gamma", 1, "1.0", 1),
        ("zeta", 1, "1.0", 0),
    ]
    assert progress(stream) == ("gamma", 1)
    assert restored_differences(stream) == []

    # The next run goes on after gamma, and does not ask for beta's record again.
    stream.pypi.requests.clear()

    report = stream.records("2026-11-02T20:00:00Z", releases=[("zeta", "1.0")])

    assert requested(stream) == [version_url("zeta", "1.0")]
    assert report == RecordsReport(
        processed=1, recorded=1, missing=0, refused=0, left=0
    )
    assert progress(stream) == (None, 0)


def test_a_failed_request_still_stops_the_records_run(tmp_path):
    stream = swept(
        tmp_path, [old_release("alpha", 10, "1.0"), old_release("beta", 20, "1.0")]
    )
    # PyPI fails beta's request on every try. That may pass, unlike a reply that makes
    # no record, so the run stops and publishes nothing, and the next run asks again.
    stream.pypi.add(version_url("beta", "1.0"), status=503)
    published = stream.writes()

    with pytest.raises(UpstreamError):
        stream.records(RECORDED_AT, releases=[("alpha", "1.0")])

    assert stream.writes() == published
    assert progress(stream) == (None, None)


def test_the_pass_reads_the_swept_projects_that_are_not_removed_or_quarantined(
    tmp_path,
):
    stream = Stream(tmp_path)
    projects = {"alpha": 10, "beta": 20, "gamma": 30, "delta": 40, "eta": 50}
    stream.run(BOOTSTRAP, 100, projects, bootstrap=True)
    # The sweep reads every page but eta's, which is missing, so eta stays unswept.
    # beta is archived, which is a status of a project that is still there.
    stream.sweep(
        SWEPT_AT,
        pages=[
            old_release("alpha", 10, "1.0"),
            page(
                "beta",
                20,
                dist("beta-1.0.tar.gz", OLD),
                versions=["1.0"],
                status="archived",
            ),
            old_release("gamma", 30, "1.0"),
            old_release("delta", 40, "1.0"),
        ],
    )
    # A daily run reads a root without gamma and delta. gamma has no page, so it is
    # removed, and delta is quarantined.
    stream.run(
        "2026-11-02T15:00:00Z",
        110,
        {"alpha": 10, "beta": 20, "eta": 50},
        pages=[page("delta", 41, status="quarantined")],
    )
    assert project_rows(stream.open(CURRENT, STATE)) == {
        "alpha": (1, 10, "active", 1),
        "beta": (1, 20, "archived", 1),
        "gamma": (1, 30, "removed", 1),
        "delta": (1, 40, "quarantined", 1),
        "eta": (1, 50, "active", 0),
    }
    stream.pypi.requests.clear()

    report = stream.records(RECORDED_AT, releases=[("alpha", "1.0"), ("beta", "1.0")])

    assert requested(stream) == [
        version_url("alpha", "1.0"),
        version_url("beta", "1.0"),
    ]
    assert report == RecordsReport(
        processed=2, recorded=2, missing=0, refused=0, left=0
    )


def test_a_record_for_a_version_seen_this_month_merges_at_month_close(tmp_path):
    # 11-29: the bootstrap, and a sweep that writes alpha's version with no record.
    stream = swept(tmp_path, [old_release("alpha", 10, "1.0")], "2026-11-29")

    # 11-30: the records pass writes its record, in the next day's delta.
    report = stream.records("2026-11-30T06:00:00Z", releases=[("alpha", "1.0")])

    assert report.recorded == 1
    assert versions_of(stream.open(CURRENT, STATE)) == [("alpha", 1, "1.0", 1)]
    assert restored_differences(stream) == []

    # 12-01: the first run of December closes November. Its state, rebuilt from the
    # base copy and the two deltas, is the state, and the deltas merge into the
    # month's part, which refuses a key that comes twice: the version is in one delta
    # and its record in the other.
    stream.run("2026-12-01T06:00:00Z", 110, {"alpha": 10})

    assert PART_NOVEMBER in stream.assets(NOVEMBER)
    counts = table_counts(stream.expand(NOVEMBER, PART_NOVEMBER))
    assert (counts["versions"], counts["release_records"]) == (1, 1)
    assert versions_of(stream.open(CURRENT, STATE)) == [("alpha", 1, "1.0", 1)]
    assert restored_differences(stream) == []


def test_records_asks_for_another_run_while_work_is_left(tmp_path, monkeypatch, capsys):
    stream = swept(
        tmp_path,
        [
            old_release("alpha", 10, "1.0"),
            old_release("beta", 20, "1.0"),
            old_release("gamma", 30, "1.0"),
        ],
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")
    monkeypatch.setattr("pypi_registry.run.GitHub", lambda repo, token: stream.gh)

    def records_here(gh, workdir, *, max_seconds) -> RecordsReport:
        """What `main` calls, on the stream's clock and PyPI, on the day of its sweep."""
        return records(
            gh,
            workdir,
            max_seconds=max_seconds,
            clock=stream.clock,
            http=stream.http,
            now=datetime.fromisoformat(RECORDED_AT),
        )

    monkeypatch.setattr("pypi_registry.run.records", records_here)
    # PyPI has no record of alpha's release, and has beta's and gamma's.
    stream.serve([], [("beta", "1.0"), ("gamma", "1.0")])

    # Each request takes a second, so a second and a half has time for two projects.
    # gamma is left, and the exit code says that another run is wanted.
    assert main(["records", "--max-seconds", "1.5"]) == 3

    printed = json.loads(capsys.readouterr().out)
    assert printed == {
        "processed": 2,
        "recorded": 1,
        "missing": 1,
        "refused": 0,
        "left": 1,
    }
    assert progress(stream) == ("beta", 1)
    assert list((tmp_path / "work").iterdir()) == [], "the work directory is removed"

    # The next run goes on after beta. It does not ask again for alpha's record, which
    # PyPI did not have, and it reaches the end of the list.
    stream.pypi.requests.clear()

    assert main(["records", "--max-seconds", "1.5"]) == 0

    assert requested(stream) == [version_url("gamma", "1.0")]
    printed = json.loads(capsys.readouterr().out)
    assert printed == {
        "processed": 1,
        "recorded": 1,
        "missing": 0,
        "refused": 0,
        "left": 0,
    }
    assert progress(stream) == (None, 0)


@pytest.mark.parametrize(
    ("processed", "left", "code"),
    [(2, 1, 3), (7, 0, 0), (0, 0, 0), (0, 5, 0)],
    ids=["some left", "none left", "nothing to do", "no time for any"],
)
def test_main_records_exit_code(tmp_path, monkeypatch, processed, left, code):
    # A run that processed nothing while projects are left made no progress, and a
    # chain that dispatched itself again each time could go round for ever.
    report = RecordsReport(
        processed=processed, recorded=0, missing=0, refused=0, left=left
    )
    monkeypatch.setattr("pypi_registry.run.records", lambda *args, **options: report)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")

    assert main(["records"]) == code


def test_main_records_with_its_budget_and_a_work_directory(tmp_path, monkeypatch):
    calls: list[tuple[Path, dict]] = []

    def records_here(gh, workdir, **options) -> RecordsReport:
        # A work directory of its own, empty.
        assert list(workdir.iterdir()) == []
        calls.append((workdir.parent, options))
        return RecordsReport(processed=0, recorded=0, missing=0, refused=0, left=0)

    monkeypatch.setattr("pypi_registry.run.records", records_here)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "t0ken")

    main(["records"])
    main(["records", "--max-seconds", "60", "--workdir", str(tmp_path / "big")])

    # `main` gives no client for PyPI: the records run makes its own, paced as the
    # sweep's is (see the next test).
    assert calls == [
        (tmp_path / "work", {"max_seconds": 16_200}),
        (tmp_path / "big", {"max_seconds": 60}),
    ]


def test_records_paces_its_requests_as_the_sweep_does(tmp_path, monkeypatch):
    stream = swept(tmp_path, [old_release("alpha", 10, "1.0")])
    made: list[dict] = []

    def http_here(**options):
        made.append(options)
        return stream.http

    monkeypatch.setattr("pypi_registry.run.Http", http_here)

    # It reads close to a million records, as the sweep reads close to a million
    # pages, so it goes at the sweep's pace.
    report = records(
        stream.gh,
        stream.workdir(),
        max_seconds=60,
        clock=stream.clock,
        now=datetime.fromisoformat(RECORDED_AT),
    )

    assert made == [{"interval": SWEEP_INTERVAL}]
    assert report.missing == 1


def test_other_runs_carry_the_records_progress(tmp_path):
    stream = swept(
        tmp_path, [old_release("alpha", 10, "1.0"), old_release("beta", 20, "1.0")]
    )
    # Before any records run, the pass has not begun: nothing is known of what is left.
    assert progress(stream) == (None, None)

    # A records run with time for one project leaves the other.
    stream.records(RECORDED_AT, releases=[("alpha", "1.0")], max_seconds=0.5)

    assert progress(stream) == ("alpha", 1)

    # A daily run and a sweep keep the pass's progress as it was.
    stream.run("2026-11-02T20:00:00Z", 110, {"alpha": 10, "beta": 20})

    assert progress(stream) == ("alpha", 1)

    stream.sweep("2026-11-02T21:00:00Z")

    assert progress(stream) == ("alpha", 1)

    # The next records run goes on from there.
    stream.pypi.requests.clear()

    report = stream.records("2026-11-02T22:00:00Z", releases=[("beta", "1.0")])

    assert requested(stream) == [version_url("beta", "1.0")]
    assert report == RecordsReport(
        processed=1, recorded=1, missing=0, refused=0, left=0
    )
    assert progress(stream) == (None, 0)


def test_records_never_starts_a_state(tmp_path, monkeypatch):
    stream = Stream(tmp_path)
    # `fetch_prior` finds no state only for a bootstrap. Were it to find none for a
    # records run, the run would stop, and not go on with no state at all.
    monkeypatch.setattr("pypi_registry.run.fetch_prior", lambda *args, **options: None)

    with pytest.raises(PriorStateError, match="no published state to read records"):
        stream.records(RECORDED_AT)

    assert stream.writes() == []
    assert stream.pypi.requests == []
