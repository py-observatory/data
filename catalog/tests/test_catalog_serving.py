import json
import sqlite3
from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest

from catalog.config import load_config
from catalog.inputs import fingerprint, needed_assets
from catalog.schema import JOINT, SOURCED
from catalog.serving import (
    BUDGET_BYTES,
    CarryError,
    StreamInput,
    build_serving,
    over_budget,
)
from catalog.stage import stage_osv, stage_registry
from catalog_helpers import REPO, TOKEN, no_sleep, publish_osv, publish_registry
from obskit.github import GitHub
from obskit.manifest import Manifest, Watermark
from obskit.testing import FakeGitHub

REGISTRY = load_config().streams["pypi-registry"]
OSV = load_config().streams["osv-pypi"]
FIRST = "2026-11-02"
SECOND = "2026-11-03"


@pytest.fixture
def gh() -> GitHub:
    return GitHub(REPO, TOKEN, transport=FakeGitHub(REPO), sleep=no_sleep)


def project(name: str, *versions: str, status: str = "active") -> dict:
    """The rows, by table, of a project that has a file in each of `versions`, whose
    state has seen each of them with a record. The files are of the open delta."""
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
                "upload_time": "2026-10-05T00:00:00Z",
            }
        )
        rows["files_current"].append({"name": name, "filename": filename})
    return rows


def record(name: str, version: str, **columns) -> dict:
    """A row of a delta's release_records. Left out, a column is NULL."""
    return {
        "name": name,
        "version": version,
        "uploaded_at": "2026-10-05T00:00:00Z",
        **columns,
    }


def event(name: str, kind: str, at: str) -> dict:
    return {"name": name, "incarnation": 1, "kind": kind, "observed_at": at}


def registry_rows(
    projects: Sequence[dict], records: Sequence[dict], events: Sequence[dict] = ()
) -> tuple[dict, dict]:
    """The state and the open delta that hold `projects`, `records` and `events`."""
    state: dict[str, list[dict]] = {}
    delta: dict[str, list[dict]] = {
        "release_records": list(records),
        "events": list(events),
    }
    for rows in projects:
        for table, added in rows.items():
            held = delta if table == "files" else state
            held.setdefault(table, []).extend(added)
    return state, delta


def stream_input(stream: str, manifest: Manifest, assets, staged) -> StreamInput:
    return StreamInput(
        stream=stream,
        generated_at=manifest.generated_at,
        data_through=manifest.data_through,
        complete=manifest.complete,
        fingerprint=fingerprint(assets),
        licences=manifest.licences,
        staged=staged,
    )


def registry_input(
    gh: GitHub, tmp_path: Path, projects, records, events=(), *, day: str = FIRST
) -> StreamInput:
    """Publish the registry as its run on `day` leaves it, and stage it."""
    state, delta = registry_rows(projects, records, events)
    manifest = publish_registry(gh, state=state, open_day=day, open_delta=delta)
    work = tmp_path / f"registry-{day}"
    work.mkdir()
    assets = needed_assets(manifest, REGISTRY.tables)
    return stream_input(
        "pypi-registry", manifest, assets, stage_registry(gh, assets, work)
    )


def osv_input(
    gh: GitHub,
    tmp_path: Path,
    advisories: Sequence[Mapping[str, object]],
    *,
    day: str = FIRST,
) -> StreamInput:
    """Publish osv-pypi as its run on `day` leaves it, and stage it."""
    manifest = publish_osv(gh, advisories=advisories, open_day=day)
    work = tmp_path / f"osv-{day}"
    work.mkdir()
    assets = needed_assets(manifest, OSV.tables)
    return stream_input("osv-pypi", manifest, assets, stage_osv(gh, assets, work))


def carried(item: StreamInput) -> StreamInput:
    """The input as a build has it when the stream could not be read. Its values are
    not those of `item`, so that a test sees that the carried rows do not come from
    it."""
    return StreamInput(
        stream=item.stream,
        generated_at="2099-01-01T00:00:00Z",
        data_through=Watermark("ignored", "day"),
        complete=not item.complete,
        fingerprint="ignored",
        licences=[],
        staged=None,
    )


def affected_by(name: str, introduced: str, fixed: str, *, id: str) -> dict:
    """An advisory that affects the releases of `name` from `introduced` up to, but not
    including, `fixed`."""
    events = [{"introduced": introduced}, {"fixed": fixed}]
    entry = {"name": name, "ranges": [{"type": "ECOSYSTEM", "events": events}]}
    return {"id": id, "affected": json.dumps([entry])}


def select(db: Path, query: str) -> list[tuple]:
    with closing(sqlite3.connect(db)) as con:
        return con.execute(query).fetchall()


def assert_carried(out: Path, previous: Path, stream: str) -> None:
    """Every table of `stream` in the database `out` holds what it held in `previous`,
    row for row, and holds something, so that the comparison says something."""
    for table in SOURCED[stream]:
        query = f"SELECT * FROM {table} ORDER BY 1, 2"
        assert select(out, query) == select(previous, query), table
        assert select(out, query), table


def a_registry(gh, tmp_path, *, day: str = FIRST) -> StreamInput:
    """A registry with a project, foo, at 1.0 and 1.1, and a removed and a quarantined
    one, with records and events that fill every table that a registry fills."""
    return registry_input(
        gh,
        tmp_path,
        [
            project("foo", "1.0", "1.1"),
            project("gone", "0.1", status="removed"),
            project("held", "0.1", status="quarantined"),
        ],
        [
            record(
                "foo",
                "1.0",
                summary="Draws charts",
                keywords="plotting, graphs",
                license_expression="MIT",
                classifiers='["Topic :: Scientific"]',
                requires_dist='["bar>=1"]',
            ),
            record(
                "foo",
                "1.1",
                summary="Draws better charts",
                keywords="plotting, graphs",
                license_expression="MIT",
                classifiers='["Topic :: Scientific"]',
                requires_dist='["bar>=1"]',
            ),
            record("gone", "0.1"),
            record("held", "0.1"),
        ],
        [
            event("foo", "created", "2026-10-01T00:00:00Z"),
            event("gone", "removed", "2026-10-02T00:00:00Z"),
            event("held", "quarantined", "2026-10-03T00:00:00Z"),
        ],
        day=day,
    )


def test_a_build_fills_every_table_from_fresh_inputs(gh, tmp_path):
    registry = a_registry(gh, tmp_path)
    osv = osv_input(gh, tmp_path, [affected_by("foo", "1.0", "1.1", id="PYSEC-1")])
    out = tmp_path / "out.db"

    report = build_serving(out, [registry, osv], None)

    assert report.carried == []
    assert report.unparsed_requirements == 0
    assert select(out, "SELECT stream, carried FROM input ORDER BY stream") == [
        ("osv-pypi", 0),
        ("pypi-registry", 0),
    ]
    for item in (registry, osv):
        assert sorted(
            select(
                out,
                "SELECT upstream, url, licence, terms FROM attribution "
                f"WHERE stream = '{item.stream}'",
            ),
            key=str,
        ) == sorted(
            (
                (
                    licence["name"],
                    licence.get("url"),
                    licence.get("licence"),
                    licence.get("terms"),
                )
                for licence in item.licences
            ),
            key=str,
        )
    for table in (*SOURCED["pypi-registry"], *SOURCED["osv-pypi"], *JOINT):
        assert report.tables[table] > 0, table
    assert report.tables["input"] == 2


def test_a_stream_without_input_keeps_its_tables_from_the_previous_build(gh, tmp_path):
    registry = a_registry(gh, tmp_path)
    previous = tmp_path / "previous.db"
    build_serving(previous, [registry, osv_input(gh, tmp_path, [])], None)

    # The next day's osv has an advisory for a release that the carried registry has.
    osv = osv_input(
        gh, tmp_path, [affected_by("foo", "1.0", "1.1", id="PYSEC-2")], day=SECOND
    )
    out = tmp_path / "out.db"
    report = build_serving(out, [carried(registry), osv], previous)

    assert report.carried == ["pypi-registry"]
    assert_carried(out, previous, "pypi-registry")
    assert select(
        out, "SELECT stream, generated_at, data_through, complete, carried FROM input"
        " WHERE stream = 'pypi-registry'"
    ) == [("pypi-registry", registry.generated_at, registry.data_through.value, 1, 1)]  # fmt: skip
    assert select(
        out,
        "SELECT upstream FROM attribution WHERE stream = 'pypi-registry'",
    ) == [(licence["name"],) for licence in registry.licences]
    assert select(
        out,
        "SELECT p.name, a.version, d.id FROM affected AS a "
        "JOIN project AS p USING (project_id) JOIN advisory AS d USING (advisory_id)",
    ) == [("foo", "1.0", "PYSEC-2")]
    assert select(out, "SELECT carried FROM input WHERE stream = 'osv-pypi'") == [(0,)]


def test_carried_advisories_meet_new_releases(gh, tmp_path):
    previous = tmp_path / "previous.db"
    old = registry_input(gh, tmp_path, [project("foo", "1.0")], [record("foo", "1.0")])
    osv = osv_input(gh, tmp_path, [affected_by("foo", "1.0", "2.0", id="PYSEC-1")])
    build_serving(previous, [old, osv], None)

    new = registry_input(
        gh,
        tmp_path,
        [project("foo", "1.0", "1.5", "2.0")],
        [record("foo", "1.0"), record("foo", "1.5"), record("foo", "2.0")],
        day=SECOND,
    )
    out = tmp_path / "out.db"
    report = build_serving(out, [new, carried(osv)], previous)

    assert report.carried == ["osv-pypi"]
    assert_carried(out, previous, "osv-pypi")
    assert select(out, "SELECT version FROM affected ORDER BY version") == [
        ("1.0",),
        ("1.5",),
    ]
    assert select(out, "SELECT id FROM advisory") == [("PYSEC-1",)]
    assert select(out, "SELECT carried FROM input WHERE stream = 'osv-pypi'") == [(1,)]


def test_carrying_without_a_previous_build_fails(gh, tmp_path):
    registry = a_registry(gh, tmp_path)
    out = tmp_path / "out.db"

    with pytest.raises(CarryError):
        build_serving(out, [carried(registry)], None)
    assert not out.exists()


def test_carrying_a_stream_that_the_previous_build_lacks_fails(gh, tmp_path):
    previous = tmp_path / "previous.db"
    build_serving(previous, [osv_input(gh, tmp_path, [])], None)

    with pytest.raises(CarryError):
        build_serving(
            tmp_path / "out.db", [carried(a_registry(gh, tmp_path))], previous
        )


def test_search_finds_projects_by_name_summary_and_keywords(gh, tmp_path):
    out = tmp_path / "out.db"
    build_serving(out, [a_registry(gh, tmp_path)], None)

    def found(term: str) -> list[tuple]:
        return select(out, f"SELECT name FROM search WHERE search MATCH '{term}'")

    assert found("foo") == [("foo",)]
    assert found("charts") == [("foo",)]
    assert found("plotting") == [("foo",)]
    assert found("nothing") == []


def test_tombstone_lists_removed_and_quarantined_projects(gh, tmp_path):
    out = tmp_path / "out.db"
    build_serving(out, [a_registry(gh, tmp_path)], None)

    assert select(out, "SELECT name, status, status_since FROM tombstone ORDER BY name") == [
        ("gone", "removed", "2026-10-02T00:00:00Z"),
        ("held", "quarantined", "2026-10-03T00:00:00Z"),
    ]  # fmt: skip


def test_a_failed_build_leaves_no_database(gh, tmp_path):
    registry = a_registry(gh, tmp_path)
    broken = replace(registry, stream="no-such-stream")
    out = tmp_path / "out.db"

    with pytest.raises(KeyError):
        build_serving(out, [broken], None)
    assert not out.exists()


def test_over_budget(tmp_path):
    packed = tmp_path / "catalog.db.zst"
    with packed.open("wb") as file:
        file.truncate(BUDGET_BYTES)
    assert not over_budget(packed)

    with packed.open("r+b") as file:
        file.truncate(BUDGET_BYTES + 1)
    assert over_budget(packed)


def test_building_into_an_existing_file_leaves_it_alone(gh, tmp_path):
    out = tmp_path / "out.db"
    out.write_bytes(b"not mine to remove")

    with pytest.raises(FileExistsError):
        build_serving(out, [osv_input(gh, tmp_path, [])], None)
    assert out.read_bytes() == b"not mine to remove"


def test_a_previous_build_that_lacks_a_table_is_refused(gh, tmp_path):
    registry = a_registry(gh, tmp_path)
    previous = tmp_path / "previous.db"
    build_serving(previous, [registry], None)
    with closing(sqlite3.connect(previous)) as con:
        con.execute("DROP TABLE change")
        con.commit()
    out = tmp_path / "out.db"

    with pytest.raises(ValueError, match="change"):
        build_serving(out, [carried(registry)], previous)
    assert not out.exists()
