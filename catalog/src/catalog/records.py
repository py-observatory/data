"""What each release's record says: a project's fields, what each release changed, and
its dependencies, classifiers and licence.

A release record is what PyPI's JSON page said of one release. The registry keeps one for
each release it has read, and the stage holds them (see catalog.stage). Only the records of
a project's current incarnation count, because those of an earlier one belong to a name
that was removed and registered again.

A project takes its fields, its requirements and its classifiers from the record of one
release, its `record_version`: the latest version when that release has a record, and
otherwise the newest record. What each release changed is the difference between its
record and the record before it, taken in upload order.
"""

import json
import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from itertools import groupby
from operator import itemgetter

from catalog.stage import RegistryStage, attach_read_only
from obskit.names import canonical_name
from packaging.requirements import InvalidRequirement, Requirement

# The fields of a record whose changes are kept. `readme` stands for the five facts of the
# README, which are compared as one.
CHANGE_FIELDS = (
    "name_raw",
    "summary",
    "license",
    "license_expression",
    "license_files",
    "requires_python",
    "requires_dist",
    "provides_extra",
    "classifiers",
    "keywords",
    "project_urls",
    "home_page",
    "download_url",
    "author",
    "author_email_names",
    "maintainer",
    "maintainer_email_names",
    "platform",
    "dynamic",
    "readme",
)

# The columns of a record that this module reads, besides the ones it keys a record by.
_README_COLUMNS = (
    "readme_sha256",
    "readme_bytes",
    "readme_bytes_exact",
    "readme_words",
    "readme_content_type",
)
_COLUMNS = (
    *(field for field in CHANGE_FIELDS if field != "readme"),
    *_README_COLUMNS,
)

# The fields that a project takes as they are from its record_version.
_PROJECT_FIELDS = (
    "name_raw",
    "summary",
    "license",
    "license_expression",
    "requires_python",
    "home_page",
    "project_urls",
    "keywords",
)

# Every record, in the order of the stage's key, so that the records of one name and
# incarnation come together and the table is read in one pass.
_RECORDS = f"""
SELECT name, incarnation, version, uploaded_at, {", ".join(_COLUMNS)}
FROM stage.release_records
ORDER BY name, incarnation, version
"""

_SET_FIELDS = f"""
UPDATE main.project
SET record_version = ?, {", ".join(f"{field} = ?" for field in _PROJECT_FIELDS)}
WHERE project_id = ?
"""

_INSERT_CHANGE = """
INSERT INTO main.change (project_id, version, field, value, compared_with)
VALUES (?, ?, ?, ?, ?)
"""

# A requirement's text is interned: one row for each distinct text, shared by every
# project that has it.
_INTERN_REQUIREMENT = "INSERT OR IGNORE INTO main.requirement_text (text) VALUES (?)"
_REQUIREMENT_ID = "SELECT requirement_id FROM main.requirement_text WHERE text = ?"
_INSERT_REQUIREMENT = """
INSERT OR IGNORE INTO main.requirement (project_id, requirement_id, depends_on)
VALUES (?, ?, ?)
"""

_INTERN_CLASSIFIER = (
    "INSERT OR IGNORE INTO main.classifier (text, projects) VALUES (?, 0)"
)
_CLASSIFIER_ID = "SELECT classifier_id FROM main.classifier WHERE text = ?"
_INSERT_PROJECT_CLASSIFIER = """
INSERT OR IGNORE INTO main.project_classifier (project_id, classifier_id)
VALUES (?, ?)
"""

# A removed or quarantined project keeps its links, but does not count.
_COUNT_CLASSIFIERS = """
UPDATE main.classifier
SET projects = (
    SELECT count(*)
    FROM main.project_classifier AS pc
    JOIN main.project AS p ON p.project_id = pc.project_id
    WHERE pc.classifier_id = classifier.classifier_id
        AND p.status NOT IN ('removed', 'quarantined')
)
"""

_INSERT_LICENCES = """
INSERT INTO main.licence (licence, projects)
SELECT license_expression, count(*)
FROM main.project
WHERE license_expression IS NOT NULL AND status NOT IN ('removed', 'quarantined')
GROUP BY license_expression
"""


def build_records(con: sqlite3.Connection, stage: RegistryStage) -> int:
    """Fill the record fields of `project`, and `change`, `requirement_text`,
    `requirement`, `classifier`, `project_classifier` and `licence`, of the serving
    database that `con` is open on, from the release records that `stage` holds.

    `project` must be filled already (see catalog.registry.build_registry), and the
    other tables must be empty. `con` must open URIs, as the one that `open_db` gives
    does, because the stage is attached by the URI that makes it read-only.

    Returns the number of requirements that PEP 508 refused. They are left out, and so
    is a requirement of a project on itself. Everything is committed and the stage is
    detached before this returns, so that the connection holds no transaction. An error
    leaves it attached, and what was written uncommitted, for the caller to close the
    connection on and discard the database.
    """
    projects = {
        name: (project_id, incarnation, latest)
        for project_id, name, incarnation, latest in con.execute(
            "SELECT project_id, name, incarnation, latest_version FROM main.project"
        )
    }
    attach_read_only(con, stage.stage, "stage")
    refused = 0
    for (name, incarnation), group in groupby(
        con.execute(_RECORDS), key=itemgetter(0, 1)
    ):
        known = projects.get(name)
        if known is None or known[1] != incarnation:
            continue
        project_id, _, latest = known
        records = sorted(
            (_record(row) for row in group), key=lambda record: record.order
        )
        refused += _write_project(con, project_id, name, latest, records)
    con.execute(_COUNT_CLASSIFIERS)
    con.execute(_INSERT_LICENCES)
    # A database cannot be detached while the transaction that read it is open.
    con.commit()
    con.execute("DETACH DATABASE stage")
    return refused


class _Record:
    """One release record: its version, when it was uploaded, and its columns by name."""

    __slots__ = ("version", "uploaded_at", "columns")

    def __init__(self, version: str, uploaded_at: str | None, columns: Mapping) -> None:
        self.version = version
        self.uploaded_at = uploaded_at
        self.columns = columns

    @property
    def order(self) -> tuple[bool, str, str]:
        """The place of the record in upload order: by upload time, a record without
        one before every record with one, and then by version as text, so that of two
        records uploaded together the larger version is the newer."""
        return (self.uploaded_at is not None, self.uploaded_at or "", self.version)

    def changeable(self) -> dict[str, str | None]:
        """The value of each of CHANGE_FIELDS, as text, with the README's facts built
        into one."""
        values = {field: self.columns[field] for field in CHANGE_FIELDS[:-1]}
        values["readme"] = _readme(self.columns)
        return values


def _record(row: Sequence) -> _Record:
    """The record that a row of `_RECORDS` holds."""
    _, _, version, uploaded_at, *values = row
    return _Record(version, uploaded_at, dict(zip(_COLUMNS, values, strict=True)))


def _readme(columns: Mapping) -> str | None:
    """The facts of a README as compact JSON with sorted keys, or None when the record
    has no hash of one."""
    if columns["readme_sha256"] is None:
        return None
    facts = {
        "bytes": columns["readme_bytes"],
        "bytes_exact": columns["readme_bytes_exact"],
        "content_type": columns["readme_content_type"],
        "sha256": columns["readme_sha256"],
        "words": columns["readme_words"],
    }
    return json.dumps(facts, sort_keys=True, separators=(",", ":"))


def _write_project(
    con: sqlite3.Connection,
    project_id: int,
    name: str,
    latest: str | None,
    records: list[_Record],
) -> int:
    """Write what the `records` of one project give, which are in upload order, and
    return the number of requirements that PEP 508 refused."""
    con.executemany(_INSERT_CHANGE, _changes(project_id, records))
    used = next((r for r in records if r.version == latest), records[-1])
    fields = (used.columns[field] for field in _PROJECT_FIELDS)
    con.execute(_SET_FIELDS, (used.version, *fields, project_id))
    refused = _write_requirements(con, project_id, name, used.columns["requires_dist"])
    _write_classifiers(con, project_id, used.columns["classifiers"])
    return refused


def _changes(project_id: int, records: list[_Record]) -> Iterator[tuple]:
    """The rows of `change` for the `records` of one project, in upload order. The
    first record gives each field that is not NULL, compared with nothing. Each later
    one gives each field that differs from the record before, one that became NULL
    included."""
    before: dict[str, str | None] = {}
    compared_with = None
    for record in records:
        values = record.changeable()
        for field in CHANGE_FIELDS:
            if compared_with is None:
                changed = values[field] is not None
            else:
                changed = values[field] != before[field]
            if changed:
                yield project_id, record.version, field, values[field], compared_with
        before, compared_with = values, record.version


def _write_requirements(
    con: sqlite3.Connection, project_id: int, name: str, requires_dist: str | None
) -> int:
    """Intern the requirements in the JSON list `requires_dist` and link them to the
    project. Return the number that PEP 508 refused."""
    refused = 0
    for text in _json_list(requires_dist):
        try:
            depends_on = canonical_name(Requirement(text).name)
        except InvalidRequirement:
            refused += 1
            continue
        if depends_on == name:
            continue
        con.execute(_INTERN_REQUIREMENT, (text,))
        (requirement_id,) = con.execute(_REQUIREMENT_ID, (text,)).fetchone()
        con.execute(_INSERT_REQUIREMENT, (project_id, requirement_id, depends_on))
    return refused


def _write_classifiers(
    con: sqlite3.Connection, project_id: int, classifiers: str | None
) -> None:
    """Intern the classifiers in the JSON list `classifiers` and link them to the
    project."""
    for text in _json_list(classifiers):
        con.execute(_INTERN_CLASSIFIER, (text,))
        (classifier_id,) = con.execute(_CLASSIFIER_ID, (text,)).fetchone()
        con.execute(_INSERT_PROJECT_CLASSIFIER, (project_id, classifier_id))


def _json_list(text: str | None) -> list[str]:
    """The strings of a JSON array that a record holds, or none for NULL."""
    return [] if text is None else json.loads(text)
