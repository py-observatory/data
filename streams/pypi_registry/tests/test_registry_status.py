"""The status pass: each run of the stream re-reads the pages of one slice of the most
downloaded projects, and records a change of status, which moves no serial.

Each test starts from a stream that the bootstrap built on a fake GitHub and a sweep read
in full, and publishes the downloads stream's serving database beside it, with the sets
of the most downloaded projects that the pass reads. It then runs the stream with the
pages that a fake PyPI serves, as a run of the stream does.
"""

import dataclasses
import sqlite3
from collections.abc import Callable
from contextlib import closing
from itertools import islice
from pathlib import Path

import pytest

from obskit.manifest import Asset, Manifest, Watermark, producer
from obskit.release import publish_current
from obskit.zst import compress_file, sha256_file
from pypi_registry.run import SLICES, StatusReport, in_slot
from pypi_registry.simple import ROOT_URL, project_url
from run_helpers import (
    CURRENT,
    DELTA,
    REPO,
    STATE,
    Stream,
    meta_of,
    page,
    project_rows,
    restored_differences,
    written,
)

# The day of every run, and on it: when the stream is bootstrapped, the sweep that reads
# every page, a daily run before the pass, and the daily runs of the tests.
DAY = "2027-01-04"
BOOTSTRAP = f"{DAY}T06:00:00Z"
SWEPT_AT = f"{DAY}T08:00:00Z"
EARLIER = f"{DAY}T10:00:00Z"
RUN_AT = f"{DAY}T12:00:00Z"
LATER = f"{DAY}T18:00:00Z"

# The downloads stream's release and its serving database, which holds the sets.
DOWNLOADS = "pypi-downloads"
DOWNLOADS_CURRENT = "pypi-downloads-current"
SERVING = "pypi-downloads-serving.db.zst"
# The serving database's table of the sets, as the downloads stream makes it.
TOP_SET = """
CREATE TABLE top_set (year INTEGER NOT NULL, project_id INTEGER NOT NULL,
    name TEXT NOT NULL, entered INTEGER NOT NULL, rank INTEGER NOT NULL,
    PRIMARY KEY (year, project_id)) WITHOUT ROWID
"""


def slice_names(slot: int, count: int) -> list[str]:
    """The first `count` names of the form top-NNN that fall in the slice `slot`, which
    are in name order."""
    names = (f"top-{number:03d}" for number in range(1_000))
    return list(islice((name for name in names if in_slot(name, slot)), count))


def listed(pages: list[dict]) -> dict[str, int]:
    """The projects of `pages` with their serials, as a root lists them."""
    return {body["name"]: body["meta"]["_last-serial"] for body in pages}


def swept(tmp_path: Path, pages: list[dict]) -> Stream:
    """A stream that the bootstrap built from the projects of `pages`, and that a sweep
    read in full from them. The requests so far are forgotten, so a test counts those
    of its own runs alone."""
    stream = Stream(tmp_path)
    stream.run(BOOTSTRAP, 100, listed(pages), bootstrap=True)
    stream.sweep(SWEPT_AT, pages=pages)
    stream.pypi.requests.clear()
    return stream


def serving_db(path: Path, sets: dict[int, list[str]]) -> Path:
    """A serving database at `path` whose top_set holds each year's names of `sets`,
    ranked in order, as entered at the year's first close. It has none of the other
    tables, which the pass does not read."""
    rows = [
        (year, rank, name, (year - 1) * 100 + 12, rank)
        for year, names in sets.items()
        for rank, name in enumerate(names, start=1)
    ]
    with closing(sqlite3.connect(path)) as db:
        db.execute(TOP_SET)
        db.executemany("INSERT INTO top_set VALUES (?, ?, ?, ?, ?)", rows)
        db.commit()
    return path


def publish_downloads(stream: Stream, db: Path, name: str = SERVING) -> None:
    """Publish pypi-downloads-current on the stream's fake GitHub with the file `db`,
    compressed, as its asset `name`, which is all that its manifest lists."""
    packed = stream.folder("downloads") / name
    compress_file(db, packed)
    entry = Asset(
        name=name,
        tag=DOWNLOADS_CURRENT,
        role="public",
        format="sqlite+zstd",
        asset_bytes=packed.stat().st_size,
        asset_sha256=sha256_file(packed),
        db_bytes=db.stat().st_size,
        db_sha256=sha256_file(db),
        tables={},
    )
    manifest = Manifest(
        stream=DOWNLOADS,
        producer=producer(REPO),
        generated_at=BOOTSTRAP,
        complete=False,
        coverage={},
        data_through=Watermark(DAY, "day"),
        cursor=None,
        licences=[],
        assets=[entry],
    )
    publish_current(stream.gh, DOWNLOADS, [packed], manifest, bootstrap=True)


def publish_sets(stream: Stream, sets: dict[int, list[str]]) -> None:
    """Publish the downloads stream's serving database with the sets `sets`."""
    made = stream.folder("made") / SERVING.removesuffix(".zst")
    publish_downloads(stream, serving_db(made, sets))


def requested(stream: Stream) -> list[str]:
    """The URLs that PyPI was asked for, in order."""
    return [url for url, _ in stream.pypi.requests]


def applied(stream: Stream) -> int:
    """The highest seq that the published state has applied."""
    return int(meta_of(stream.open(CURRENT, STATE))["applied_seq"])


def next_slice(stream: Stream) -> object:
    """The slice that the next status pass reads, as the current manifest gives it."""
    return stream.manifest().coverage["status_slice"]


def leftovers(stream: Stream) -> list[Path]:
    """What the last run left in its work directory of the downloads stream's files."""
    workdir = stream.tmp_path / f"run-{stream.runs}"
    return [path for path in workdir.iterdir() if "downloads" in path.name]


def test_each_member_falls_in_one_of_16_slices():
    names = [f"top-{number:03d}" for number in range(1_000)]

    slots = {name: [s for s in range(SLICES) if in_slot(name, s)] for name in names}

    assert SLICES == 16
    assert all(len(found) == 1 for found in slots.values())
    assert {found[0] for found in slots.values()} == set(range(SLICES))
    # A name's slice is its sha256, so it stays from run to run and from one year's set
    # to the next. These are pinned, because another hash would move every name.
    pinned = {"boto3": 15, "requests": 14, "numpy": 10, "setuptools": 1}
    for name, slot in pinned.items():
        assert [s for s in range(SLICES) if in_slot(name, s)] == [slot]


@pytest.mark.parametrize(
    ("before", "after"),
    [("active", "archived"), ("archived", "active"), ("deprecated", "archived")],
)
def test_a_changed_status_writes_one_event(tmp_path, before, after):
    changing, steady, unknown, former = slice_names(0, 4)
    (elsewhere,) = slice_names(1, 1)
    pages = [
        page(changing, 10, status=before),
        page(steady, 20),
        page(former, 30),
        page(elsewhere, 40),
    ]
    stream = swept(tmp_path, pages)
    # The newest year's set is the one read. `unknown` is a name that the registry
    # does not hold, `former` is only in last year's set, and `elsewhere` is in
    # another slice.
    publish_sets(
        stream, {2026: [former, changing], 2027: [changing, steady, unknown, elsewhere]}
    )
    before_seq = applied(stream)

    # The root shows no change: archiving moves no serial.
    report = stream.run(
        RUN_AT,
        110,
        listed(pages),
        pages=[page(changing, 10, status=after), page(steady, 20)],
    )

    assert requested(stream) == [ROOT_URL, project_url(changing), project_url(steady)]
    assert written(stream.open(CURRENT, DELTA), before_seq) == [
        ("event", changing, 1, "status_changed", None, None, after),
        ("cursor", 110),
    ]
    assert project_rows(stream.open(CURRENT, STATE))[changing] == (1, 10, after, 1)
    assert report.status == StatusReport(
        slot=0, members=2, read=2, changed=1, skipped=None
    )
    assert report.changed == 0, "no project changed in the root"
    assert report.requests == 3
    # The pass read its slice, so the next run reads the next one.
    assert next_slice(stream) == 1
    assert leftovers(stream) == []
    assert restored_differences(stream) == []


def test_a_project_whose_serial_moved_is_left_to_the_daily_path(tmp_path):
    moved, gone, held, stale = slice_names(0, 4)
    pages = [page(moved, 10), page(gone, 20), page(held, 30), page(stale, 40)]
    stream = swept(tmp_path, pages)
    publish_sets(stream, {2027: [moved, gone, held, stale]})
    before_seq = applied(stream)

    # The root has not caught up with any of them. moved's page is newer than the
    # state, gone's is missing (404), held's is quarantined and stale's is an old copy.
    # Each is left to the daily run that finds it in the root, or probes it, which reads
    # the files and the serial with the status.
    report = stream.run(
        RUN_AT,
        110,
        listed(pages),
        pages=[
            page(moved, 11, status="archived"),
            page(held, 30, status="quarantined"),
            page(stale, 39, status="archived"),
        ],
    )

    assert requested(stream) == [ROOT_URL, *(project_url(p["name"]) for p in pages)]
    assert written(stream.open(CURRENT, DELTA), before_seq) == [("cursor", 110)]
    rows = project_rows(stream.open(CURRENT, STATE))
    assert {name: row[2] for name, row in rows.items()} == dict.fromkeys(
        listed(pages), "active"
    )
    assert report.status == StatusReport(
        slot=0, members=4, read=4, changed=0, skipped=None
    )
    assert next_slice(stream) == 1

    # The next root lists moved at its new serial, and the daily run records the change
    # of status with the rest of the page. The pass reads the next slice, which has
    # none of the set.
    stream.pypi.requests.clear()
    before_seq = applied(stream)

    report = stream.run(
        LATER,
        120,
        listed(pages) | {moved: 11},
        pages=[page(moved, 11, status="archived")],
    )

    assert requested(stream) == [ROOT_URL, project_url(moved)]
    assert written(stream.open(CURRENT, DELTA), before_seq) == [
        ("event", moved, 1, "status_changed", None, None, "archived"),
        ("serial", moved, 11),
        ("cursor", 120),
    ]
    assert report.status == StatusReport(
        slot=1, members=0, read=0, changed=0, skipped=None
    )


def test_a_member_that_the_run_read_is_not_read_again(tmp_path):
    changed, steady = slice_names(0, 2)
    stream = swept(tmp_path, [page(changed, 10), page(steady, 20)])
    publish_sets(stream, {2027: [changed, steady]})
    before_seq = applied(stream)

    # The root lists `changed` at a new serial, so the run reads its page and records
    # its status there. The state does not hold that yet, and a second read would
    # record it again.
    report = stream.run(
        RUN_AT,
        110,
        {changed: 11, steady: 20},
        pages=[page(changed, 11, status="archived"), page(steady, 20)],
    )

    assert requested(stream) == [ROOT_URL, project_url(changed), project_url(steady)]
    assert written(stream.open(CURRENT, DELTA), before_seq) == [
        ("event", changed, 1, "status_changed", None, None, "archived"),
        ("serial", changed, 11),
        ("cursor", 110),
    ]
    assert report.changed == 1
    assert report.status == StatusReport(
        slot=0, members=2, read=1, changed=0, skipped=None
    )
    assert next_slice(stream) == 1


def test_the_pass_reads_only_swept_projects_that_are_not_removed_or_quarantined(
    tmp_path,
):
    active, archived, gone, held, unswept = slice_names(0, 5)
    stream = Stream(tmp_path)
    projects = {active: 10, archived: 20, gone: 30, held: 40, unswept: 50}
    stream.run(BOOTSTRAP, 100, projects, bootstrap=True)
    # The sweep reads every page but unswept's, which is missing, so it stays unswept.
    stream.sweep(
        SWEPT_AT,
        pages=[
            page(active, 10),
            page(archived, 20, status="archived"),
            page(gone, 30),
            page(held, 40),
        ],
    )
    # A daily run reads a root without gone and held. gone has no page, so it is
    # removed, and held is quarantined.
    stream.run(
        EARLIER,
        110,
        {active: 10, archived: 20, unswept: 50},
        pages=[page(held, 41, status="quarantined")],
    )
    assert project_rows(stream.open(CURRENT, STATE)) == {
        active: (1, 10, "active", 1),
        archived: (1, 20, "archived", 1),
        gone: (1, 30, "removed", 1),
        held: (1, 40, "quarantined", 1),
        unswept: (1, 50, "active", 0),
    }
    publish_sets(stream, {2027: [active, archived, gone, held, unswept]})
    stream.pypi.requests.clear()
    before_seq = applied(stream)

    # An archived project is read, to see it come back. A removed or quarantined one
    # is the daily run's: it probes held again, as it does each quarantined name that
    # the root does not list. The sweep reads unswept's page in full, and records its
    # status then: a change of status needs a status that was read.
    report = stream.run(
        RUN_AT,
        120,
        {active: 10, archived: 20, unswept: 50},
        pages=[
            page(held, 41, status="quarantined"),
            page(active, 10),
            page(archived, 20),
        ],
    )

    assert requested(stream) == [
        ROOT_URL,
        project_url(held),
        project_url(active),
        project_url(archived),
    ]
    assert written(stream.open(CURRENT, DELTA), before_seq) == [
        ("event", archived, 1, "status_changed", None, None, "active"),
        ("cursor", 120),
    ]
    assert report.status == StatusReport(
        slot=0, members=3, read=2, changed=1, skipped=None
    )


def no_release(stream: Stream, member: str) -> None:
    """The downloads stream has not published."""


def no_serving_database(stream: Stream, member: str) -> None:
    """The downloads stream's manifest lists no serving database."""
    made = stream.folder("made") / "pypi-downloads-state.db"
    publish_downloads(stream, serving_db(made, {2027: [member]}), made.name + ".zst")


def other_bytes(stream: Stream, member: str) -> None:
    """The release holds other bytes than its manifest gives the serving database."""
    publish_sets(stream, {2027: [member]})
    other = serving_db(stream.folder("other") / "other.db", {2027: ["other"]})
    packed = stream.folder("other") / SERVING
    compress_file(other, packed)
    stream.replace_asset(DOWNLOADS_CURRENT, SERVING, packed)


def no_top_set(stream: Stream, member: str) -> None:
    """The serving database holds no table of the sets."""
    made = stream.folder("made") / SERVING.removesuffix(".zst")
    with closing(sqlite3.connect(made)) as db:
        db.execute("CREATE TABLE folded (day INTEGER PRIMARY KEY, seq INTEGER)")
        db.commit()
    publish_downloads(stream, made)


def not_a_database(stream: Stream, member: str) -> None:
    """The serving database is not SQLite at all."""
    made = stream.folder("made") / SERVING.removesuffix(".zst")
    made.write_text(f"{member}\n", encoding="utf-8")
    publish_downloads(stream, made)


@pytest.mark.parametrize(
    ("breaking", "reason"),
    [
        (no_release, f"has no release {DOWNLOADS_CURRENT}"),
        (no_serving_database, f"lists no {SERVING}"),
        (other_bytes, "has sha256"),
        (no_top_set, "no such table: top_set"),
        (not_a_database, "file is not a database"),
    ],
    ids=[
        "no release",
        "no serving database",
        "other bytes",
        "no top set",
        "not sqlite",
    ],
)
def test_an_unreadable_downloads_release_skips_the_pass(
    tmp_path, breaking: Callable[[Stream, str], None], reason
):
    (member,) = slice_names(5, 1)
    stream = swept(tmp_path, [page(member, 10)])
    # The pass has come round to the sixth slice.
    manifest = stream.manifest()
    coverage = {**manifest.coverage, "status_slice": 5}
    stream.replace_manifest(dataclasses.replace(manifest, coverage=coverage))
    breaking(stream, member)

    report = stream.run(RUN_AT, 110, {member: 10}, pages=[page(member, 10)])

    # The run goes on without the pass, and publishes. The slice is read next time.
    status = report.status
    assert (status.slot, status.members, status.read, status.changed) == (5, 0, 0, 0)
    assert reason in status.skipped
    assert requested(stream) == [ROOT_URL]
    assert stream.manifest().generated_at == RUN_AT
    assert next_slice(stream) == 5
    assert leftovers(stream) == []


def test_an_empty_top_set_reads_nothing(tmp_path):
    # Until the downloads stream's first month close, its set is empty.
    (member,) = slice_names(0, 1)
    stream = swept(tmp_path, [page(member, 10)])
    publish_sets(stream, {})

    report = stream.run(RUN_AT, 110, {member: 10}, pages=[page(member, 10)])

    assert requested(stream) == [ROOT_URL]
    assert report.status == StatusReport(
        slot=0, members=0, read=0, changed=0, skipped=None
    )
    # The set was read, and the slice holds none of it, so the pass is done with it.
    assert next_slice(stream) == 1


def test_the_pass_stops_at_the_deadline(tmp_path):
    first, second, third = slice_names(0, 3)
    pages = [page(first, 10), page(second, 20), page(third, 30)]
    stream = swept(tmp_path, pages)
    publish_sets(stream, {2027: [first, second, third]})
    before_seq = applied(stream)

    # Each request takes a second. The root takes one of the three, so the pass has
    # time for two pages before the deadline.
    report = stream.run(
        RUN_AT,
        110,
        listed(pages),
        pages=[page(first, 10, status="archived"), page(second, 20)],
        max_seconds=3,
    )

    assert requested(stream) == [ROOT_URL, project_url(first), project_url(second)]
    assert written(stream.open(CURRENT, DELTA), before_seq) == [
        ("event", first, 1, "status_changed", None, None, "archived"),
        ("cursor", 110),
    ]
    assert report.status == StatusReport(
        slot=0, members=3, read=2, changed=1, skipped=None
    )
    # The slice was not read to its end, so the next run reads it again, in full.
    assert next_slice(stream) == 0

    stream.pypi.requests.clear()
    before_seq = applied(stream)

    report = stream.run(
        LATER,
        120,
        listed(pages),
        pages=[
            page(first, 10, status="archived"),
            page(second, 20),
            page(third, 30, status="archived"),
        ],
    )

    assert requested(stream) == [ROOT_URL, *(project_url(p["name"]) for p in pages)]
    assert written(stream.open(CURRENT, DELTA), before_seq) == [
        ("event", third, 1, "status_changed", None, None, "archived"),
        ("cursor", 120),
    ]
    assert report.status == StatusReport(
        slot=0, members=3, read=3, changed=1, skipped=None
    )
    assert next_slice(stream) == 1


def test_other_runs_carry_the_status_slice(tmp_path):
    (member,) = slice_names(0, 1)
    stream = Stream(tmp_path)
    stream.run(BOOTSTRAP, 100, {member: 10}, bootstrap=True)

    # The bootstrap starts the pass at the first slice.
    assert next_slice(stream) == 0

    publish_sets(stream, {2027: [member]})
    stream.sweep(SWEPT_AT, pages=[page(member, 10)])

    assert next_slice(stream) == 0

    stream.run(EARLIER, 110, {member: 10}, pages=[page(member, 10)])

    assert next_slice(stream) == 1

    # A sweep and a records run keep the slice as it was. Only a daily run reads one.
    stream.sweep(RUN_AT)

    assert next_slice(stream) == 1

    stream.records(LATER)

    assert next_slice(stream) == 1


def test_the_status_pass_slice_wraps_round(tmp_path):
    (member,) = slice_names(SLICES - 1, 1)
    stream = swept(tmp_path, [page(member, 10)])
    publish_sets(stream, {2027: [member]})
    manifest = stream.manifest()
    coverage = {**manifest.coverage, "status_slice": SLICES - 1}
    stream.replace_manifest(dataclasses.replace(manifest, coverage=coverage))

    report = stream.run(RUN_AT, 110, {member: 10}, pages=[page(member, 10)])

    assert report.status == StatusReport(
        slot=SLICES - 1, members=1, read=1, changed=0, skipped=None
    )
    # After the last slice the pass starts again from the first.
    assert next_slice(stream) == 0
