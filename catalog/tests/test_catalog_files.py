import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from catalog.config import load_config
from catalog.files import _LATEST_FILES, build_files, file_kind, wheel_tags
from catalog.inputs import needed_assets
from catalog.registry import build_registry
from catalog.schema import SERVING_DB, SERVING_SCHEMA
from catalog.stage import attach_read_only, stage_registry
from catalog_helpers import REPO, TOKEN, no_sleep, publish_registry
from obskit.github import GitHub
from obskit.sqlite import open_db
from obskit.testing import FakeGitHub

REGISTRY = load_config().streams["pypi-registry"]
NAME = "sampleproject"
UPLOADED = "2017-08-06T03:43:27.195869Z"


def test_a_wheel_gives_its_three_tags():
    assert wheel_tags("sampleproject-1.0-py3-none-any.whl") == {("py3", "none", "any")}


def test_a_compressed_tag_set_is_expanded():
    assert wheel_tags(
        "numpy-2.3.4-cp312-cp312-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl"
    ) == {
        ("cp312", "cp312", "manylinux_2_27_x86_64"),
        ("cp312", "cp312", "manylinux_2_28_x86_64"),
    }
    assert wheel_tags("six-1.17.0-py2.py3-none-any.whl") == {
        ("py2", "none", "any"),
        ("py3", "none", "any"),
    }


def test_a_build_tag_is_skipped():
    assert wheel_tags("pkg-1.0-1-cp39-abi3-win_amd64.whl") == {
        ("cp39", "abi3", "win_amd64")
    }


def test_a_free_threaded_abi_is_kept_as_written():
    assert wheel_tags("numpy-2.3.4-cp313-cp313t-macosx_14_0_arm64.whl") == {
        ("cp313", "cp313t", "macosx_14_0_arm64")
    }


def test_tags_are_lowercased():
    assert wheel_tags("Foo-1.0-PY3-NONE-ANY.WHL") == {("py3", "none", "any")}


def test_extra_dashes_still_give_the_last_three_fields():
    assert wheel_tags("my-odd-name-1.0-py3-none-any.whl") == {("py3", "none", "any")}


@pytest.mark.parametrize(
    "filename",
    ["broken.whl", "pkg-1.0-py3.whl", "pkg-1.0-py3-none.whl", "pkg-1.0.tar.gz", ""],
)
def test_a_name_that_is_not_a_readable_wheel_gives_none(filename):
    assert wheel_tags(filename) is None


@pytest.mark.parametrize(
    ("filename", "kind"),
    [
        ("pkg-1.0-py3-none-any.whl", "wheel"),
        ("broken.whl", "wheel"),
        ("pkg-1.0.tar.gz", "sdist"),
        ("pkg-1.0.zip", "sdist"),
        ("pkg-1.0.tar.bz2", "sdist"),
        ("pkg-1.0.tgz", "sdist"),
        ("pkg-1.0.tar.xz", "sdist"),
        ("pkg-1.0.tar", "sdist"),
        ("PKG-1.0.TAR.GZ", "sdist"),
        ("pkg-1.0-py2.7.egg", "other"),
        ("pkg-1.0.win32.exe", "other"),
        ("pkg-1.0.msi", "other"),
    ],
)
def test_file_kind(filename, kind):
    assert file_kind(filename) == kind


# ---- the summary of each project's latest release


@pytest.fixture
def gh() -> GitHub:
    return GitHub(REPO, TOKEN, transport=FakeGitHub(REPO), sleep=no_sleep)


def seen(version: str, *, name: str = NAME, **columns) -> dict:
    """A row of the state's versions_seen: `name` has had `version`."""
    return {"name": name, "version": version, **columns}


def file(filename: str, version: str, *, name: str = NAME, **columns) -> dict:
    """A row of a delta's files: a file of `name`, as the delta first saw it."""
    return {
        "name": name,
        "filename": filename,
        "version": version,
        "upload_time": UPLOADED,
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


def one_project(
    files: list[tuple[str, str, int | None, int]],
    *,
    yanked: frozenset[str] = frozenset(),
    name: str = NAME,
) -> tuple[dict, dict]:
    """The state and the open delta of one project `name` whose files are `files`, each
    `(filename, version, size, has_provenance)`: every version seen, every file current,
    and the files named in `yanked` yanked for the reason "broken"."""
    versions = sorted({version for _, version, _, _ in files})
    state = {
        "projects": [{"name": name}],
        "versions_seen": [seen(version, name=name) for version in versions],
        "files_current": [
            current(f, name=name, yanked="broken" if f in yanked else None)
            for f, _, _, _ in files
        ],
    }
    open_delta = {
        "files": [
            file(f, version, name=name, size=size, has_provenance=provenance)
            for f, version, size, provenance in files
        ]
    }
    return state, open_delta


def build(gh: GitHub, tmp_path: Path, state: dict, open_delta: dict) -> Path:
    """Publish a registry of these rows, as its run of 2026-11-02 leaves it, stage what
    the catalog reads of it, build the registry's tables and the summary of the latest
    releases, and return the database's path."""
    manifest = publish_registry(
        gh, state=state, open_day="2026-11-02", open_delta=open_delta
    )
    workdir = tmp_path / "work"
    workdir.mkdir()
    stage = stage_registry(gh, needed_assets(manifest, REGISTRY.tables), workdir)
    db = tmp_path / SERVING_DB
    with closing(open_db(db, SERVING_SCHEMA, create=True)) as con:
        build_registry(con, stage)
        build_files(con, stage)
    return db


def select(db: Path, query: str) -> list[tuple]:
    with closing(sqlite3.connect(db)) as con:
        return con.execute(query).fetchall()


def latest_files(db: Path) -> list[tuple]:
    return select(db, "SELECT * FROM latest_files ORDER BY project_id")


def latest_tags(db: Path) -> list[tuple]:
    return select(
        db,
        "SELECT t.python, t.abi, t.platform FROM latest_wheel_tag AS l "
        "JOIN wheel_tag AS t USING (wheel_tag_id) ORDER BY 1, 2, 3",
    )


def test_the_latest_release_is_summed_and_its_wheel_tags_listed(gh, tmp_path):
    # 1.1 is latest: an sdist with provenance, a pure wheel, and a cp312 wheel for two
    # manylinux tags. 1.0's wheel is not counted.
    files = [
        ("sampleproject-1.0-py3-none-any.whl", "1.0", 900, 0),
        ("sampleproject-1.1.tar.gz", "1.1", 5000, 1),
        ("sampleproject-1.1-py3-none-any.whl", "1.1", 1200, 0),
        (
            "sampleproject-1.1-cp312-cp312-"
            "manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl",
            "1.1",
            3000,
            1,
        ),
    ]
    db = build(gh, tmp_path, *one_project(files))

    assert latest_files(db) == [(1, "1.1", 3, 2, 1, 5000, 1200, 3000, 2)]
    assert latest_tags(db) == [
        ("cp312", "cp312", "manylinux_2_27_x86_64"),
        ("cp312", "cp312", "manylinux_2_28_x86_64"),
        ("py3", "none", "any"),
    ]


def test_yanked_files_of_the_latest_release_are_left_out(gh, tmp_path):
    # The cp312 wheel of 1.1 is yanked; the release is not wholly yanked, so it is latest.
    yanked = "sampleproject-1.1-cp312-cp312-manylinux_2_28_x86_64.whl"
    files = [
        ("sampleproject-1.1.tar.gz", "1.1", 5000, 0),
        ("sampleproject-1.1-py3-none-any.whl", "1.1", 1200, 0),
        (yanked, "1.1", 3000, 1),
    ]
    db = build(gh, tmp_path, *one_project(files, yanked=frozenset({yanked})))

    assert latest_files(db) == [(1, "1.1", 2, 1, 1, 5000, 1200, 1200, 0)]
    assert latest_tags(db) == [("py3", "none", "any")]


def test_unknown_sizes_are_left_out_of_the_sizes(gh, tmp_path):
    files = [
        ("sampleproject-1.1.tar.gz", "1.1", None, 0),
        ("sampleproject-1.1-py3-none-any.whl", "1.1", None, 0),
    ]
    db = build(gh, tmp_path, *one_project(files))

    assert latest_files(db) == [(1, "1.1", 2, 1, 1, None, None, None, 0)]


def test_only_the_current_incarnations_files_count(gh, tmp_path):
    # Incarnation 1 of sampleproject had 1.1 with a wheel; the name was registered again,
    # and incarnation 2's 1.1 has only an sdist.
    wheel = "sampleproject-1.1-py3-none-any.whl"
    sdist = "sampleproject-1.1.tar.gz"
    state = {
        "projects": [{"name": NAME, "incarnation": 2}],
        "versions_seen": [seen("1.1", incarnation=2)],
        "files_current": [current(wheel), current(sdist, incarnation=2)],
    }
    open_delta = {
        "files": [
            file(wheel, "1.1", size=1200, has_provenance=0),
            file(sdist, "1.1", incarnation=2, size=700, has_provenance=0),
        ]
    }
    db = build(gh, tmp_path, state, open_delta)

    assert latest_files(db) == [(1, "1.1", 1, 0, 1, 700, None, None, 0)]
    assert latest_tags(db) == []


def test_a_project_without_a_latest_version_has_no_summary(gh, tmp_path):
    # aproject's only release is wholly yanked; bproject is not swept, so it has no
    # versions.
    sdist = "aproject-1.0.tar.gz"
    state = {
        "projects": [{"name": "aproject"}, {"name": "bproject", "swept": 0}],
        "versions_seen": [seen("1.0", name="aproject")],
        "files_current": [current(sdist, name="aproject", yanked="broken")],
    }
    open_delta = {
        "files": [file(sdist, "1.0", name="aproject", size=100, has_provenance=0)]
    }
    db = build(gh, tmp_path, state, open_delta)

    assert latest_files(db) == []
    assert select(db, "SELECT * FROM latest_wheel_tag") == []


def test_an_unreadable_wheel_name_counts_but_gives_no_tags(gh, tmp_path):
    db = build(
        gh, tmp_path, *one_project([("sampleproject-1.1-py3.whl", "1.1", 800, 0)])
    )

    assert latest_files(db) == [(1, "1.1", 1, 1, 0, None, 800, 800, 0)]
    assert latest_tags(db) == []


def test_wheel_tags_are_shared_between_projects(gh, tmp_path):
    a_state, a_delta = one_project(
        [("aproject-1.0-py3-none-any.whl", "1.0", 100, 0)], name="aproject"
    )
    b_state, b_delta = one_project(
        [("bproject-2.0-py3-none-any.whl", "2.0", 200, 0)], name="bproject"
    )
    state = {table: a_state[table] + b_state[table] for table in a_state}
    open_delta = {"files": a_delta["files"] + b_delta["files"]}
    db = build(gh, tmp_path, state, open_delta)

    assert select(db, "SELECT * FROM wheel_tag") == [(1, "py3", "none", "any")]
    assert select(db, "SELECT * FROM latest_wheel_tag ORDER BY 1") == [(1, 1), (2, 1)]


def test_the_latest_files_are_found_by_name_incarnation_and_version(gh, tmp_path):
    # The stage holds tens of millions of files, so each project's latest files must be
    # found through the whole key of current_files, not by name alone.
    db = build(gh, tmp_path, *one_project([("sampleproject-1.1.tar.gz", "1.1", 10, 0)]))
    with closing(open_db(db, SERVING_SCHEMA)) as con:
        attach_read_only(con, tmp_path / "work" / "stage.db", "stage")
        plan = " ".join(
            row[3] for row in con.execute(f"EXPLAIN QUERY PLAN {_LATEST_FILES}")
        )

    assert "name=? AND incarnation=? AND version=?" in plan
