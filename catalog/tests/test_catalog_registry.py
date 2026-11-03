import sqlite3
from collections.abc import Sequence
from contextlib import closing
from datetime import datetime
from pathlib import Path

import pytest

from catalog.config import load_config
from catalog.inputs import needed_assets
from catalog.registry import build_registry, latest_version
from catalog.schema import SERVING_DB, SERVING_SCHEMA
from catalog.stage import RegistryStage, stage_registry
from catalog_helpers import REPO, TOKEN, no_sleep, publish_registry
from obskit.github import GitHub
from obskit.sqlite import open_db
from obskit.testing import FakeGitHub
from pypi_registry.diff import EVENT_KINDS

REGISTRY = load_config().streams["pypi-registry"]

NAME = "sampleproject"
SDIST = "sampleproject-1.0.tar.gz"
WHEEL = "sampleproject-1.0-py3-none-any.whl"
UPLOADED = "2017-08-06T03:43:27.195869Z"
LATER = "2017-08-06T03:44:02.000000Z"

# The tables of the registry's state. A row of any other table is a row of a delta.
STATE_TABLES = frozenset({"meta", "projects", "files_current", "versions_seen"})


@pytest.fixture
def gh() -> GitHub:
    return GitHub(REPO, TOKEN, transport=FakeGitHub(REPO), sleep=no_sleep)


def unix(time: str) -> int:
    """The Unix seconds of the RFC 3339 `time`, as Python reads it and not SQLite."""
    return int(datetime.fromisoformat(time).timestamp())


def seen(version: str, *, name: str = NAME, **columns) -> dict:
    """A row of the state's versions_seen: `name` has had `version`."""
    return {"name": name, "version": version, **columns}


def file(
    filename: str,
    version: str | None,
    uploaded: str | None,
    *,
    name: str = NAME,
    **columns,
) -> dict:
    """A row of a delta's files: a file of `name`, as the delta first saw it."""
    return {
        "name": name,
        "filename": filename,
        "version": version,
        "upload_time": uploaded,
        **columns,
    }


def current(
    filename: str, *, name: str = NAME, yanked: str | None = None, **columns
) -> dict:
    """A row of the state's files_current: a file that `name` has now, yanked for the
    reason `yanked` when one is given."""
    return {
        "name": name,
        "filename": filename,
        "yanked": int(yanked is not None),
        "yanked_reason": yanked,
        **columns,
    }


def event(name: str, incarnation: int, kind: str, at: str, **columns) -> dict:
    """A row of a delta's events, observed at `at`."""
    return {
        "name": name,
        "incarnation": incarnation,
        "kind": kind,
        "observed_at": at,
        **columns,
    }


def release(
    name: str,
    version: str,
    uploaded: str,
    *,
    removed: bool = False,
    yanked: str | None = None,
) -> dict[str, list[dict]]:
    """The rows, by table, that make a release of `name` with one file, an sdist that
    was uploaded at `uploaded`. The state has seen the version, and the open delta has
    seen the file. The state has the file now unless it was `removed`, and has it
    yanked for the reason `yanked` when one is given."""
    filename = f"{name}-{version}.tar.gz"
    return {
        "versions_seen": [seen(version, name=name)],
        "files": [file(filename, version, uploaded, name=name)],
        "files_current": []
        if removed
        else [current(filename, name=name, yanked=yanked)],
    }


def staged(
    gh: GitHub, tmp_path: Path, *, state: dict, open_delta: dict, **deltas
) -> RegistryStage:
    """Publish a registry of these rows, as its run of 2026-11-02 leaves it, and stage
    what the catalog reads of it. `deltas` are `sealed` and `parts`, as
    `publish_registry` takes them."""
    manifest = publish_registry(
        gh, state=state, open_day="2026-11-02", open_delta=open_delta, **deltas
    )
    workdir = tmp_path / "work"
    workdir.mkdir()
    return stage_registry(gh, needed_assets(manifest, REGISTRY.tables), workdir)


def build(
    gh: GitHub,
    tmp_path: Path,
    *,
    state: dict | None = None,
    open_delta: dict | None = None,
    releases: Sequence[dict[str, list[dict]]] = (),
    **deltas,
) -> Path:
    """Stage a registry of these rows (see `staged`), build the serving database from
    it, and return the database's path. The rows of each of `releases`, which `release`
    makes, are added to those of `state` and of the open delta."""
    state = {table: list(rows) for table, rows in (state or {}).items()}
    open_delta = {table: list(rows) for table, rows in (open_delta or {}).items()}
    for rows in releases:
        for table, added in rows.items():
            held = state if table in STATE_TABLES else open_delta
            held.setdefault(table, []).extend(added)
    stage = staged(gh, tmp_path, state=state, open_delta=open_delta, **deltas)
    db = tmp_path / SERVING_DB
    with closing(open_db(db, SERVING_SCHEMA, create=True)) as con:
        build_registry(con, stage)
    return db


def select(db: Path, query: str) -> list[tuple]:
    with closing(sqlite3.connect(db)) as con:
        return con.execute(query).fetchall()


def release_rows(db: Path) -> list[tuple]:
    return select(
        db,
        "SELECT project_id, version, uploaded, files, yanked, yanked_reason, has_record "
        "FROM release ORDER BY project_id, version",
    )


def test_projects_in_name_order_with_status_and_first_seen(gh, tmp_path):
    db = build(
        gh,
        tmp_path,
        state={
            "projects": [
                {
                    "name": "zebra",
                    "first_seen": "2026-10-03T04:05:06Z",
                    "first_seen_exact": 1,
                },
                {
                    "name": "apple",
                    "status": "quarantined",
                    "first_seen": "2026-10-02T00:00:00Z",
                },
                {
                    "name": "mango",
                    "incarnation": 3,
                    "status": "removed",
                    "first_seen": "2026-10-04T00:00:00Z",
                },
            ]
        },
    )

    # The ids are in name order. A project that has no release counts none, and has no
    # upload and no latest version.
    assert select(
        db,
        "SELECT project_id, name, incarnation, status, first_seen, first_seen_exact, "
        "releases, first_uploaded, latest_version FROM project ORDER BY project_id",
    ) == [
        (1, "apple", 1, "quarantined", "2026-10-02T00:00:00Z", 0, 0, None, None),
        (2, "mango", 3, "removed", "2026-10-04T00:00:00Z", 0, 0, None, None),
        (3, "zebra", 1, "active", "2026-10-03T04:05:06Z", 1, 0, None, None),
    ]


def test_releases_take_upload_files_and_record(gh, tmp_path):
    # 1.0 has an sdist in the delta of 11-01 and a wheel, uploaded later, in the open
    # delta. 1.1 has one file, and its release record.
    next_sdist = "sampleproject-1.1.tar.gz"
    db = build(
        gh,
        tmp_path,
        state={
            "projects": [{"name": NAME}],
            "versions_seen": [seen("1.0"), seen("1.1", record=1)],
            "files_current": [current(SDIST), current(WHEEL), current(next_sdist)],
        },
        open_delta={
            "files": [file(WHEEL, "1.0", LATER), file(next_sdist, "1.1", LATER)]
        },
        sealed={"2026-11-01": {"files": [file(SDIST, "1.0", UPLOADED)]}},
    )

    assert release_rows(db) == [
        (1, "1.0", 1501991007, 2, 0, None, 0),
        (1, "1.1", 1501991042, 1, 0, None, 1),
    ]
    # Both releases have a file now, and the project's first upload is the earliest.
    assert select(db, "SELECT releases, first_uploaded FROM project") == [
        (2, 1501991007)
    ]


def test_a_wholly_yanked_release_is_yanked_with_its_reason(gh, tmp_path):
    # Both files of 1.0 are yanked. One of the two files of 2.0 is, and the other is
    # not, so 2.0 can still be installed.
    next_sdist = "sampleproject-2.0.tar.gz"
    next_wheel = "sampleproject-2.0-py3-none-any.whl"
    db = build(
        gh,
        tmp_path,
        state={
            "projects": [{"name": NAME}],
            "versions_seen": [seen("1.0"), seen("2.0")],
            "files_current": [
                current(SDIST, yanked="broken"),
                current(WHEEL, yanked="broken"),
                current(next_sdist),
                current(next_wheel, yanked="superseded"),
            ],
        },
        open_delta={
            "files": [
                file(SDIST, "1.0", UPLOADED),
                file(WHEEL, "1.0", UPLOADED),
                file(next_sdist, "2.0", LATER),
                file(next_wheel, "2.0", LATER),
            ]
        },
    )

    assert select(
        db, "SELECT version, files, yanked, yanked_reason FROM release ORDER BY version"
    ) == [("1.0", 2, 1, "broken"), ("2.0", 2, 0, None)]


def test_a_release_without_readable_files_has_no_upload_and_no_count(gh, tmp_path):
    # The project's page lists 1.0, and the only file that the state has is an installer
    # whose name gives no version. The stage sums no release from it.
    installer = "sampleproject-setup.exe"
    db = build(
        gh,
        tmp_path,
        state={
            "projects": [{"name": NAME}],
            "versions_seen": [seen("1.0")],
            "files_current": [current(installer)],
        },
        open_delta={"files": [file(installer, None, UPLOADED)]},
    )

    # It is not yanked, as no file of it is known to be. Nothing says that a file of it
    # is there, so it is not counted, and nothing says when it was uploaded.
    assert release_rows(db) == [(1, "1.0", None, None, 0, None, 0)]
    assert select(
        db, "SELECT releases, first_uploaded, latest_version FROM project"
    ) == [(0, None, None)]


def test_a_project_never_read_counts_no_releases(gh, tmp_path):
    db = build(
        gh,
        tmp_path,
        state={
            "projects": [
                {"name": "emptied"},
                {"name": "kept"},
                # Its page has not been read in full, so nothing is known of its
                # releases.
                {"name": "unread", "swept": 0},
            ]
        },
        releases=[
            release("emptied", "1.0", UPLOADED, removed=True),
            release("kept", "1.0", UPLOADED),
        ],
    )

    # A project that was read and has no file now has no release, which is 0. One
    # that was never read has a count that is not known.
    assert select(db, "SELECT name, swept, releases FROM project ORDER BY name") == [
        ("emptied", 1, 0),
        ("kept", 1, 1),
        ("unread", 0, None),
    ]


def test_latest_version_prefers_finals_and_skips_yanked_and_removed(gh, tmp_path):
    assert latest_version(["1.0", "2.0rc1"]) == "1.0"
    assert latest_version(["2.0rc1"]) == "2.0rc1"
    # PEP 440 puts 1.10 above 1.9 and a post-release above its release. A dev release is
    # a pre-release, and the highest of the pre-releases is the answer when no final
    # release is left.
    assert latest_version(["1.9", "1.10", "1.2"]) == "1.10"
    assert latest_version(["1.0", "1.0.post1"]) == "1.0.post1"
    assert latest_version(["1.0", "2.0.dev1"]) == "1.0"
    assert latest_version(["2.0.dev1", "2.0a1", "1.9rc1"]) == "2.0a1"
    assert latest_version([]) is None

    db = build(
        gh,
        tmp_path,
        state={
            "projects": [{"name": NAME}, {"name": "rc-only"}, {"name": "yanked-only"}]
        },
        releases=[
            release(NAME, "1.0", UPLOADED),
            release(NAME, "1.1", UPLOADED, yanked="broken"),
            release(NAME, "1.2", UPLOADED, removed=True),
            release("rc-only", "1.0.dev1", UPLOADED),
            release("rc-only", "2.0rc1", UPLOADED),
            release("yanked-only", "1.0", UPLOADED, yanked="broken"),
        ],
    )

    # 1.1 is yanked and 1.2 has no file now, so sampleproject is at 1.0. A yanked release
    # has files, and counts as a release. One that has none does not.
    assert select(
        db, "SELECT name, releases, latest_version FROM project ORDER BY name"
    ) == [("rc-only", 2, "2.0rc1"), (NAME, 2, "1.0"), ("yanked-only", 1, None)]
    # A release with no file now is not yanked, as it has no file that is.
    assert select(
        db,
        "SELECT version, files, yanked FROM release JOIN project USING (project_id) "
        "WHERE name = 'sampleproject' ORDER BY version",
    ) == [("1.0", 1, 0), ("1.1", 1, 1), ("1.2", 0, 0)]


def test_latest_version_skips_versions_pep_440_refuses():
    assert latest_version(["2004d", "1.0"]) == "1.0"
    assert latest_version(["2004d"]) is None
    # Python will not read a number of over 4,300 digits, and packaging refuses the
    # version with a plain ValueError, and not with its InvalidVersion.
    assert latest_version(["9" * 5000, "1.0"]) == "1.0"


def test_latest_version_returns_the_string_given_whatever_the_order():
    # 1.0 and 1.0.0 are one version to PEP 440, which has no higher of the two, so the
    # one that sorts last as text is returned, and not the one that comes first.
    assert latest_version(["1.0", "1.0.0"]) == "1.0.0"
    assert latest_version(["1.0.0", "1.0"]) == "1.0.0"
    # The string is as it was given, and not the form that PEP 440 gives it.
    assert latest_version(["v1.0", "0.9"]) == "v1.0"
    assert latest_version(iter(["1.0-1"])) == "1.0-1"


def test_only_the_current_incarnation_has_releases(gh, tmp_path):
    # sampleproject was removed and registered again, and is at its second incarnation.
    # Both released a 1.0, and the first also released a 0.5. The state lost the files
    # of the first when it was removed.
    first = "2017-08-06T03:43:27Z"
    second = "2026-10-20T08:00:00Z"
    next_sdist = "sampleproject-2.0.tar.gz"
    db = build(
        gh,
        tmp_path,
        state={
            "projects": [{"name": NAME, "incarnation": 2}],
            "versions_seen": [
                seen("0.5", incarnation=1),
                seen("1.0", incarnation=1, record=1),
                seen("1.0", incarnation=2),
                seen("2.0", incarnation=2),
            ],
            "files_current": [
                current(SDIST, incarnation=2),
                current(next_sdist, incarnation=2),
            ],
        },
        open_delta={
            "files": [
                file("sampleproject-0.5.zip", "0.5", first, incarnation=1),
                file("sampleproject-1.0.zip", "1.0", first, incarnation=1),
                file(SDIST, "1.0", second, incarnation=2),
                file(next_sdist, "2.0", second, incarnation=2),
            ]
        },
    )

    # Neither the first incarnation's 0.5, nor its upload of 1.0, nor its record of it.
    assert release_rows(db) == [
        (1, "1.0", unix(second), 1, 0, None, 0),
        (1, "2.0", unix(second), 1, 0, None, 0),
    ]
    assert select(db, "SELECT incarnation, releases, first_uploaded FROM project") == [
        (2, 2, unix(second))
    ]


def test_status_since_is_the_last_status_event(gh, tmp_path):
    # Each project's events are spread over the parts that they were observed in.
    db = build(
        gh,
        tmp_path,
        state={
            "projects": [
                {"name": "archived", "status": "archived"},
                {"name": "back-again", "incarnation": 2},
                {"name": "files-only"},
                {"name": "in-and-out", "status": "quarantined"},
                # Its second incarnation has no status event of its own.
                {"name": "next-life", "incarnation": 2},
                {"name": "quiet"},
            ]
        },
        parts={
            "2026-09": {
                "events": [
                    event("back-again", 1, "created", "2026-09-01T01:00:00Z"),
                    event("back-again", 1, "removed", "2026-09-20T01:00:00Z"),
                    event("in-and-out", 1, "created", "2026-09-02T01:00:00Z"),
                    event("in-and-out", 1, "quarantined", "2026-09-10T01:00:00Z"),
                    event("in-and-out", 1, "unquarantined", "2026-09-15T01:00:00Z"),
                    event("next-life", 1, "created", "2026-09-03T01:00:00Z"),
                    event("next-life", 1, "removed", "2026-09-25T01:00:00Z"),
                ]
            },
            "2026-10": {
                "events": [
                    event("back-again", 2, "reregistered", "2026-10-05T01:00:00Z"),
                    event("in-and-out", 1, "quarantined", "2026-10-02T01:00:00Z"),
                    # A file is a change of a project, and not of its status.
                    event("in-and-out", 1, "file_removed", "2026-10-03T01:00:00Z"),
                    event("files-only", 1, "file_removed", "2026-10-04T01:00:00Z"),
                ]
            },
        },
        sealed={
            "2026-11-01": {
                "events": [
                    event(
                        "archived",
                        1,
                        "status_changed",
                        "2026-11-01T01:00:00Z",
                        detail="archived",
                    )
                ]
            }
        },
        open_delta={
            "events": [
                event("back-again", 2, "yanked", "2026-11-02T01:00:00Z", detail="bad"),
            ]
        },
    )

    assert select(db, "SELECT name, status_since FROM project ORDER BY name") == [
        ("archived", "2026-11-01T01:00:00Z"),
        ("back-again", "2026-10-05T01:00:00Z"),
        ("files-only", None),
        ("in-and-out", "2026-10-02T01:00:00Z"),
        ("next-life", None),
        ("quiet", None),
    ]


def test_only_the_events_that_set_a_status_give_status_since(gh, tmp_path):
    sets_a_status = {
        "created",
        "removed",
        "quarantined",
        "unquarantined",
        "reregistered",
        "status_changed",
    }
    # A new kind of event is not counted until it is said here whether it sets a status.
    assert set(EVENT_KINDS) - sets_a_status == {"file_removed", "yanked", "unyanked"}
    at = "2026-10-05T01:00:00Z"
    names = {kind: kind.replace("_", "-") for kind in EVENT_KINDS}
    db = build(
        gh,
        tmp_path,
        state={"projects": [{"name": name} for name in names.values()]},
        open_delta={
            "events": [event(names[kind], 1, kind, at) for kind in EVENT_KINDS]
        },
    )

    assert dict(select(db, "SELECT name, status_since FROM project")) == {
        names[kind]: at if kind in sets_a_status else None for kind in EVENT_KINDS
    }


def test_events_keep_their_seq_and_every_incarnation(gh, tmp_path):
    # sampleproject was created in September, lost a file and was removed in October,
    # and was registered again in November, when a file of the new incarnation was
    # yanked.
    created = event(NAME, 1, "created", "2026-09-05T01:00:00Z", seq=3)
    file_removed = event(
        NAME,
        1,
        "file_removed",
        "2026-10-01T01:00:00Z",
        seq=41,
        version="1.0",
        filename=WHEEL,
    )
    removed = event(NAME, 1, "removed", "2026-10-02T01:00:00Z", seq=45)
    reregistered = event(NAME, 2, "reregistered", "2026-11-01T01:00:00Z", seq=52)
    yanked = event(
        NAME,
        2,
        "yanked",
        "2026-11-02T01:00:00Z",
        seq=57,
        version="1.0",
        filename=SDIST,
        detail="broken",
        source_at="2026-11-02T00:30:00Z",
    )
    db = build(
        gh,
        tmp_path,
        state={"projects": [{"name": NAME, "incarnation": 2}]},
        open_delta={"events": [yanked]},
        sealed={"2026-11-01": {"events": [reregistered]}},
        parts={
            "2026-09": {"events": [created]},
            "2026-10": {"events": [file_removed, removed]},
        },
    )

    assert select(
        db,
        "SELECT seq, name, incarnation, kind, version, filename, detail, source_at, "
        "observed_at FROM event ORDER BY seq",
    ) == [
        (3, NAME, 1, "created", None, None, None, None, "2026-09-05T01:00:00Z"),
        (
            41,
            NAME,
            1,
            "file_removed",
            "1.0",
            WHEEL,
            None,
            None,
            "2026-10-01T01:00:00Z",
        ),
        (45, NAME, 1, "removed", None, None, None, None, "2026-10-02T01:00:00Z"),
        (52, NAME, 2, "reregistered", None, None, None, None, "2026-11-01T01:00:00Z"),
        (
            57,
            NAME,
            2,
            "yanked",
            "1.0",
            SDIST,
            "broken",
            "2026-11-02T00:30:00Z",
            "2026-11-02T01:00:00Z",
        ),
    ]


def test_growth_counts_releases_and_new_projects_by_month(gh, tmp_path):
    # alpha's first release was removed and its third is yanked, and both were uploaded,
    # so both count. A project is new in the month of its earliest upload. charlie's
    # releases come either side of midnight at the end of October, the first less than
    # a microsecond before it, and delta's has no upload that is known.
    db = build(
        gh,
        tmp_path,
        state={
            "projects": [
                {"name": name} for name in ("alpha", "bravo", "charlie", "delta")
            ],
            "versions_seen": [seen("1.0", name="delta")],
        },
        releases=[
            release("alpha", "0.9", "2017-07-30T10:00:00Z", removed=True),
            release("alpha", "1.0", UPLOADED),
            release("alpha", "1.1", "2017-08-20T12:00:00Z", yanked="broken"),
            release("bravo", "1.0", "2017-08-25T00:00:00Z"),
            release("charlie", "1.0", "2026-10-31T23:59:59.999999Z"),
            release("charlie", "1.1", "2026-11-01T00:00:00Z"),
        ],
    )

    # A month without an upload has no row.
    assert select(
        db, "SELECT month, projects, releases FROM growth ORDER BY month"
    ) == [
        ("2017-07", 1, 1),
        ("2017-08", 1, 3),
        ("2026-10", 1, 1),
        ("2026-11", 0, 1),
    ]
    assert select(db, "SELECT name, first_uploaded FROM project ORDER BY name") == [
        ("alpha", unix("2017-07-30T10:00:00Z")),
        ("bravo", unix("2017-08-25T00:00:00Z")),
        ("charlie", unix("2026-10-31T23:59:59.999999Z")),
        ("delta", None),
    ]


def test_the_state_and_the_stage_are_attached_read_only_and_detached(gh, tmp_path):
    stage = staged(gh, tmp_path, state={"projects": [{"name": NAME}]}, open_delta={})
    statements: list[str] = []
    with closing(open_db(tmp_path / SERVING_DB, SERVING_SCHEMA, create=True)) as con:
        con.set_trace_callback(statements.append)

        build_registry(con, stage)

        # Neither the state nor the stage is left attached, and no transaction is open.
        assert [name for _, name, _ in con.execute("PRAGMA database_list")] == ["main"]
        assert not con.in_transaction
    # The state and the stage came from outside, so each is attached once, read-only.
    attached = [s for s in statements if s.startswith("ATTACH")]
    assert len(attached) == 2
    for db in (stage.state, stage.stage):
        uri = f"{db.resolve().as_uri()}?mode=ro"
        assert sum(uri in s for s in attached) == 1
