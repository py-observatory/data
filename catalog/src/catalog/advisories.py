"""OSV's advisories in the serving database, and the releases that each one affects.

An advisory is copied from osv's state as it is kept there. Its `affected` list is
merged into one row for each project it names, because OSV writes a project once for
each package entry, and a reader wants what the advisory says of a project as a whole.

Which releases an advisory affects is worked out afterwards, against the releases that
the registry knows, as `expand_affected` says.
"""

import json
import sqlite3
from collections.abc import Iterator
from itertools import groupby
from operator import itemgetter

from catalog.stage import OsvStage, attach_read_only
from packaging.version import Version
from pypi_registry.names import canonical_version

# Every advisory of osv's state, numbered in id order. The JSON columns are kept as the
# text that osv keeps.
_INSERT_ADVISORIES = """
INSERT INTO main.advisory (
    advisory_id, id, kind, status, published, modified, withdrawn, summary,
    aliases, related, severity, refs, database_specific
)
SELECT
    row_number() OVER (ORDER BY id),
    id, kind, status, published, modified, withdrawn, summary,
    aliases, related, severity, refs, database_specific
FROM osv.advisories
ORDER BY id
"""

_ADVISORIES_WITH_ENTRIES = """
SELECT a.advisory_id, o.affected
FROM osv.advisories AS o
JOIN main.advisory AS a ON a.id = o.id
WHERE o.affected IS NOT NULL
ORDER BY a.advisory_id
"""

_INSERT_PACKAGE = """
INSERT INTO main.advisory_package (advisory_id, name, ranges, versions)
VALUES (?, ?, ?, ?)
"""

# Each package of an active advisory that has not been withdrawn, whose name is a project
# of the registry, in the order of the project so that its releases are read once.
_PACKAGES_TO_EXPAND = """
SELECT p.project_id, a.advisory_id, k.ranges, k.versions
FROM main.advisory AS a
JOIN main.advisory_package AS k ON k.advisory_id = a.advisory_id
JOIN main.project AS p ON p.name = k.name
WHERE a.status = 'active' AND a.withdrawn IS NULL
ORDER BY p.project_id, a.advisory_id
"""

_PROJECT_VERSIONS = "SELECT version FROM main.release WHERE project_id = ?"

_INSERT_AFFECTED = (
    "INSERT INTO main.affected (project_id, version, advisory_id) VALUES (?, ?, ?)"
)


def build_advisories(con: sqlite3.Connection, stage: OsvStage) -> None:
    """Fill `advisory` and `advisory_package` of the serving database that `con` is open
    on, from the advisories of the osv state that `stage` holds. Both tables must be
    empty, and `con` must open URIs, as the one that `open_db` gives does.

    Everything is committed and osv's state is detached before this returns, so that the
    connection holds no transaction. An error leaves it attached, and what was written
    uncommitted, for the caller to close the connection on and discard the database.
    """
    attach_read_only(con, stage.state, "osv")
    con.execute(_INSERT_ADVISORIES)
    for advisory_id, affected in con.execute(_ADVISORIES_WITH_ENTRIES).fetchall():
        con.executemany(
            _INSERT_PACKAGE,
            (
                (advisory_id, name, _compact(ranges), _compact(versions))
                for name, ranges, versions in _merge_entries(json.loads(affected))
            ),
        )
    # A database cannot be detached while the transaction that read it is open.
    con.commit()
    con.execute("DETACH DATABASE osv")


def _compact(value: list) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _merge_entries(entries: list[dict]) -> Iterator[tuple[str, list, list]]:
    """The entries of one advisory as one (name, ranges, versions) for each name, in the
    order that the names first appear. The ranges are those of the name's entries one
    after the other, and the versions are theirs in order, each once."""
    merged: dict[str, tuple[list, dict[str, None]]] = {}
    for entry in entries:
        ranges, versions = merged.setdefault(entry["name"], ([], {}))
        ranges.extend(entry.get("ranges") or [])
        versions.update(dict.fromkeys(entry.get("versions") or []))
    for name, (ranges, versions) in merged.items():
        yield name, ranges, list(versions)


def affects(version: Version, ranges: list[dict]) -> bool:
    """Whether `version` is in any ECOSYSTEM range of `ranges`, by OSV's rules. A range
    whose events PEP 440 cannot read matches nothing."""
    return any(
        _in_range(version, r["events"]) for r in ranges if r.get("type") == "ECOSYSTEM"
    )


def _in_range(version: Version, events: list[dict]) -> bool:
    points = []
    try:
        for event in events:
            ((kind, value),) = event.items()
            if kind == "limit":
                if value != "*" and version >= Version(value):
                    return False
                continue
            zero = kind == "introduced" and value == "0"
            points.append((None if zero else Version(value), kind))
    except ValueError:
        return False
    # "0" first, then by version; at one version, introduced before fixed and last_affected
    points.sort(
        key=lambda p: (p[0] is not None, p[0] or Version("0"), p[1] != "introduced")
    )
    affected = False
    for point, kind in points:
        if kind == "introduced":
            if point is None or version >= point:
                affected = True
        elif kind == "fixed":
            if version >= point:
                affected = False
        elif kind == "last_affected":
            if version > point:
                affected = False
    return affected


def expand_affected(con: sqlite3.Connection) -> int:
    """Fill `affected` with each release that an active advisory affects, and return the
    number of rows written. `project`, `release` and `advisory_package` must be filled.

    An advisory counts when it is active and not withdrawn, and only for the packages
    whose name is a project. A release is affected when `canonical_version` of a version
    that the advisory lists is the release's version. Otherwise it is affected when its
    version is one that PEP 440 reads and `affects` holds. A version that PEP 440 refuses
    is therefore affected only when it is listed.
    """
    written = 0
    packages = con.execute(_PACKAGES_TO_EXPAND).fetchall()
    for project_id, group in groupby(packages, key=itemgetter(0)):
        releases = _parsed_releases(con, project_id)
        rows = []
        for _, advisory_id, ranges, versions in group:
            ranges = json.loads(ranges)
            listed = {canonical_version(v) for v in json.loads(versions)}
            rows.extend(
                (project_id, text, advisory_id)
                for text, parsed in releases
                if text in listed or (parsed is not None and affects(parsed, ranges))
            )
        con.executemany(_INSERT_AFFECTED, rows)
        written += len(rows)
    return written


def _parsed_releases(
    con: sqlite3.Connection, project_id: int
) -> list[tuple[str, Version | None]]:
    """The versions of the project's releases, each with its parse, or None when PEP 440
    refuses it. The parse is made once for all the advisories that name the project."""
    parsed = []
    for (text,) in con.execute(_PROJECT_VERSIONS, (project_id,)):
        try:
            parsed.append((text, Version(text)))
        except ValueError:
            parsed.append((text, None))
    return parsed
