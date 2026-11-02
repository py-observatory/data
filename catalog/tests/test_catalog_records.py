import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from catalog.config import load_config
from catalog.inputs import needed_assets
from catalog.records import CHANGE_FIELDS, build_records
from catalog.registry import build_registry
from catalog.schema import SERVING_DB, SERVING_SCHEMA
from catalog.stage import stage_registry
from catalog_helpers import REPO, TOKEN, no_sleep, publish_registry
from obskit.github import GitHub
from obskit.sqlite import open_db
from obskit.testing import FakeGitHub

REGISTRY = load_config().streams["pypi-registry"]


@pytest.fixture
def gh() -> GitHub:
    return GitHub(REPO, TOKEN, transport=FakeGitHub(REPO), sleep=no_sleep)


def record(name: str, version: str, uploaded: str | None, **columns) -> dict:
    """A row of a delta's release_records. Left out, a column is NULL."""
    return {"name": name, "version": version, "uploaded_at": uploaded, **columns}


def project(name: str, *versions: str, status: str = "active", **columns) -> dict:
    """The rows, by table, of a project that has a file in each of `versions`, and
    whose state has seen each with a record when `record` gives it one."""
    rows: dict[str, list[dict]] = {
        "projects": [{"name": name, "status": status}],
        "versions_seen": [],
        "files": [],
        "files_current": [],
    }
    for version in versions:
        filename = f"{name}-{version}.tar.gz"
        rows["versions_seen"].append({"name": name, "version": version, "record": 1})
        rows["files"].append(
            {
                "name": name,
                "filename": filename,
                "version": version,
                "upload_time": "2020-01-01T00:00:00Z",
            }
        )
        rows["files_current"].append({"name": name, "filename": filename})
    return rows


def build(
    gh: GitHub, tmp_path: Path, projects: list[dict], records: list[dict]
) -> tuple[Path, int]:
    """Publish a registry of these projects and release records, stage it, build the
    registry and then the records, and return the serving database and the number of
    requirements that PEP 508 refused."""
    state: dict[str, list[dict]] = {}
    delta: dict[str, list[dict]] = {"release_records": records}
    for rows in projects:
        for table, added in rows.items():
            held = delta if table == "files" else state
            held.setdefault(table, []).extend(added)
    manifest = publish_registry(
        gh, state=state, open_day="2026-11-02", open_delta=delta
    )
    workdir = tmp_path / "work"
    workdir.mkdir()
    stage = stage_registry(gh, needed_assets(manifest, REGISTRY.tables), workdir)
    db = tmp_path / SERVING_DB
    with closing(open_db(db, SERVING_SCHEMA, create=True)) as con:
        build_registry(con, stage)
        refused = build_records(con, stage)
        assert not con.in_transaction
        assert [n for _, n, _ in con.execute("PRAGMA database_list")] == ["main"]
    return db, refused


def select(db: Path, query: str) -> list[tuple]:
    with closing(sqlite3.connect(db)) as con:
        return con.execute(query).fetchall()


def fields(db: Path, name: str) -> tuple:
    return select(
        db,
        "SELECT record_version, name_raw, summary, license, license_expression, "
        "requires_python, home_page, project_urls, keywords "
        f"FROM project WHERE name = '{name}'",
    )[0]


def changes(db: Path, name: str) -> list[tuple]:
    return select(
        db,
        "SELECT version, field, value, compared_with FROM change "
        f"JOIN project USING (project_id) WHERE name = '{name}' "
        "ORDER BY version, field",
    )


def test_project_fields_come_from_the_latest_versions_record(gh, tmp_path):
    urls = '{"Home":"https://example.org"}'
    db, _ = build(
        gh,
        tmp_path,
        [project("foo", "1.0", "1.1")],
        [
            record("foo", "1.0", "2020-01-01T00:00:00Z", summary="old"),
            record(
                "foo",
                "1.1",
                "2020-02-01T00:00:00Z",
                name_raw="Foo",
                summary="new",
                license="MIT",
                license_expression="MIT",
                requires_python=">=3.9",
                home_page="https://example.org",
                project_urls=urls,
                keywords="a, b",
            ),
        ],
    )

    assert fields(db, "foo") == (
        "1.1", "Foo", "new", "MIT", "MIT", ">=3.9", "https://example.org", urls,
        "a, b",
    )  # fmt: skip


def test_record_version_falls_back_to_the_newest_record(gh, tmp_path):
    # Latest is 1.0, which has no record. 2.0rc1 and 0.9 do, and 2.0rc1 is newer.
    db, _ = build(
        gh,
        tmp_path,
        [project("foo", "1.0", "2.0rc1", "0.9")],
        [
            record("foo", "0.9", "2019-01-01T00:00:00Z", summary="older"),
            record("foo", "2.0rc1", "2021-01-01T00:00:00Z", summary="the rc"),
        ],
    )

    assert select(db, "SELECT latest_version FROM project") == [("1.0",)]
    assert fields(db, "foo")[:3] == ("2.0rc1", None, "the rc")


def test_ties_in_upload_time_go_to_the_larger_version_and_nulls_are_oldest(
    gh, tmp_path
):
    db, _ = build(
        gh,
        tmp_path,
        [project("foo", "9.0")],
        [
            record("foo", "1.0", None, summary="no time"),
            record("foo", "1.1", "2020-01-01T00:00:00Z", summary="a"),
            record("foo", "1.2", "2020-01-01T00:00:00Z", summary="b"),
        ],
    )

    assert fields(db, "foo")[:3] == ("1.2", None, "b")
    # The record without a time comes first, so 1.1 is compared with it.
    assert [(v, c) for v, f, _, c in changes(db, "foo") if f == "summary"] == [
        ("1.0", None),
        ("1.1", "1.0"),
        ("1.2", "1.1"),
    ]


def test_only_the_current_incarnation_counts(gh, tmp_path):
    rows = project("foo", "1.0")
    rows["projects"][0]["incarnation"] = 2
    db, _ = build(
        gh,
        tmp_path,
        [rows],
        [
            record("foo", "1.0", "2020-01-01T00:00:00Z", summary="gone", incarnation=1),
            record("foo", "1.0", "2020-06-01T00:00:00Z", summary="now", incarnation=2),
        ],
    )

    assert fields(db, "foo")[2] == "now"
    assert changes(db, "foo") == [("1.0", "summary", "now", None)]


def test_a_project_without_a_record_has_no_fields_and_no_requirements(gh, tmp_path):
    db, refused = build(gh, tmp_path, [project("foo", "1.0")], [])

    assert fields(db, "foo") == (None,) * 9
    assert select(db, "SELECT count(*) FROM requirement") == [(0,)]
    assert select(db, "SELECT count(*) FROM project_classifier") == [(0,)]
    assert select(db, "SELECT count(*) FROM change") == [(0,)]
    assert refused == 0


def test_the_first_record_lists_every_field_against_nothing(gh, tmp_path):
    db, _ = build(
        gh,
        tmp_path,
        [project("foo", "1.0")],
        [
            record(
                "foo",
                "1.0",
                "2020-01-01T00:00:00Z",
                name_raw="Foo",
                summary="s",
                keywords="",
                dynamic='["version"]',
            )
        ],
    )

    # An empty string is a value. NULL fields give no row.
    assert changes(db, "foo") == [
        ("1.0", "dynamic", '["version"]', None),
        ("1.0", "keywords", "", None),
        ("1.0", "name_raw", "Foo", None),
        ("1.0", "summary", "s", None),
    ]


def test_changes_list_only_the_fields_that_changed(gh, tmp_path):
    db, _ = build(
        gh,
        tmp_path,
        [project("foo", "1.0", "1.1")],
        [
            record("foo", "1.0", "2020-01-01T00:00:00Z", summary="old", license="MIT"),
            record("foo", "1.1", "2020-02-01T00:00:00Z", summary="new", license="MIT"),
        ],
    )

    assert [r for r in changes(db, "foo") if r[0] == "1.1"] == [
        ("1.1", "summary", "new", "1.0")
    ]


def test_a_cleared_field_is_a_change_to_null(gh, tmp_path):
    db, _ = build(
        gh,
        tmp_path,
        [project("foo", "1.0", "1.1", "1.2")],
        [
            record("foo", "1.0", "2020-01-01T00:00:00Z", summary="old"),
            record("foo", "1.1", "2020-02-01T00:00:00Z"),
            record("foo", "1.2", "2020-03-01T00:00:00Z"),
        ],
    )

    # 1.2 has NULL after NULL, which is no change.
    assert [r for r in changes(db, "foo") if r[0] != "1.0"] == [
        ("1.1", "summary", None, "1.0")
    ]


def test_readme_facts_compare_as_one_field(gh, tmp_path):
    facts = {
        "readme_sha256": "ab",
        "readme_bytes": 10,
        "readme_bytes_exact": 1,
        "readme_words": 2,
        "readme_content_type": "text/markdown",
    }
    db, _ = build(
        gh,
        tmp_path,
        [project("foo", "1.0", "1.1", "1.2")],
        [
            record("foo", "1.0", "2020-01-01T00:00:00Z", **facts),
            record(
                "foo", "1.1", "2020-02-01T00:00:00Z", **{**facts, "readme_words": 3}
            ),
            record(
                "foo", "1.2", "2020-03-01T00:00:00Z", **{**facts, "readme_words": 3}
            ),
        ],
    )

    rows = changes(db, "foo")
    assert [r[:2] for r in rows] == [("1.0", "readme"), ("1.1", "readme")]
    assert json.loads(rows[0][2]) == {
        "bytes": 10,
        "bytes_exact": 1,
        "content_type": "text/markdown",
        "sha256": "ab",
        "words": 2,
    }
    # Sorted keys and no spaces.
    assert rows[1][2] == (
        '{"bytes":10,"bytes_exact":1,"content_type":"text/markdown",'
        '"sha256":"ab","words":3}'
    )
    assert rows[1][3] == "1.0"


def test_readme_is_null_without_a_hash(gh, tmp_path):
    db, _ = build(
        gh,
        tmp_path,
        [project("foo", "1.0")],
        [record("foo", "1.0", "2020-01-01T00:00:00Z", readme_words=5, summary="s")],
    )

    assert [r[1] for r in changes(db, "foo")] == ["summary"]


def test_the_change_fields_are_the_briefed_ones():
    assert len(CHANGE_FIELDS) == 20
    assert CHANGE_FIELDS[0] == "name_raw" and CHANGE_FIELDS[-1] == "readme"


def test_requirements_name_the_projects_they_depend_on(gh, tmp_path):
    text = "Requests[socks]>=2; python_version<'3.13'"
    db, refused = build(
        gh,
        tmp_path,
        [project("foo", "1.0"), project("bar", "1.0")],
        [
            record(
                "foo",
                "1.0",
                "2020-01-01T00:00:00Z",
                requires_dist=json.dumps([text, "Typing_Extensions"]),
            ),
            record(
                "bar", "1.0", "2020-01-01T00:00:00Z", requires_dist=json.dumps([text])
            ),
        ],
    )

    assert refused == 0
    with closing(sqlite3.connect(db)) as con:
        shared = con.execute(
            "SELECT count(*) FROM requirement_text WHERE text = ?", (text,)
        ).fetchone()
    assert shared == (1,)
    assert select(
        db,
        "SELECT p.name, t.text, r.depends_on FROM requirement r "
        "JOIN project p USING (project_id) JOIN requirement_text t USING (requirement_id) "
        "ORDER BY p.name, r.depends_on",
    ) == [
        ("bar", text, "requests"),
        ("foo", text, "requests"),
        ("foo", "Typing_Extensions", "typing-extensions"),
    ]
    with closing(sqlite3.connect(db)) as con:
        found = con.execute(
            "SELECT project_id FROM requirement INDEXED BY requirement_by_dependency "
            "WHERE depends_on = 'requests' ORDER BY project_id"
        ).fetchall()
    assert found == [(1,), (2,)]


def test_requirements_come_from_the_record_version_only(gh, tmp_path):
    db, _ = build(
        gh,
        tmp_path,
        [project("foo", "1.0", "1.1")],
        [
            record("foo", "1.0", "2020-01-01T00:00:00Z", requires_dist='["old>=1"]'),
            record("foo", "1.1", "2020-02-01T00:00:00Z", requires_dist='["new>=1"]'),
        ],
    )

    assert select(db, "SELECT depends_on FROM requirement") == [("new",)]


def test_unparseable_and_self_requirements_are_left_out(gh, tmp_path):
    db, refused = build(
        gh,
        tmp_path,
        [project("foo", "1.0"), project("bar", "1.0")],
        [
            record("foo", "1.0", "2020-01-01T00:00:00Z", requires_dist='["foo[all]"]'),
            record(
                "bar",
                "1.0",
                "2020-01-01T00:00:00Z",
                requires_dist='["not a requirement!!", "baz"]',
            ),
        ],
    )

    assert refused == 1
    assert select(db, "SELECT depends_on FROM requirement") == [("baz",)]
    assert select(db, "SELECT count(*) FROM requirement_text") == [(1,)]


def test_classifiers_are_interned_and_counted_without_removed_projects(gh, tmp_path):
    both = json.dumps(["A :: B", "C"])
    db, _ = build(
        gh,
        tmp_path,
        [
            project("foo", "1.0"),
            project("bar", "1.0", status="removed"),
            project("baz", "1.0", status="quarantined"),
            project("qux", "1.0", status="archived"),
        ],
        [
            record("foo", "1.0", "2020-01-01T00:00:00Z", classifiers=both),
            record("bar", "1.0", "2020-01-01T00:00:00Z", classifiers=both),
            record("baz", "1.0", "2020-01-01T00:00:00Z", classifiers=both),
            record("qux", "1.0", "2020-01-01T00:00:00Z", classifiers='["C", "C"]'),
        ],
    )

    assert select(db, "SELECT text, projects FROM classifier ORDER BY text") == [
        ("A :: B", 1),
        ("C", 2),
    ]
    # The removed and quarantined projects still link to their classifiers.
    assert select(db, "SELECT count(*) FROM project_classifier") == [(7,)]


def test_licences_count_live_projects_by_expression(gh, tmp_path):
    def with_licence(name, expression, status="active"):
        return project(name, "1.0", status=status), record(
            name, "1.0", "2020-01-01T00:00:00Z", license_expression=expression
        )

    made = [
        with_licence("a", "MIT"),
        with_licence("b", "MIT"),
        with_licence("c", "MIT", "removed"),
        with_licence("d", "GPL-3.0", "quarantined"),
        with_licence("e", None),
        with_licence("f", "Apache-2.0", "archived"),
    ]
    db, _ = build(gh, tmp_path, [p for p, _ in made], [r for _, r in made])

    assert select(db, "SELECT licence, projects FROM licence ORDER BY licence") == [
        ("Apache-2.0", 1),
        ("MIT", 2),
    ]
