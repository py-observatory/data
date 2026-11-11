"""The registry's projects, releases and events in the serving database, and how many of
each are new in a month.

The registry is staged as two databases (see catalog.stage): its state, as it keeps it,
and the stage, which holds what was taken from its deltas. A project is a row of the
state's projects. Its releases are the versions that the state has seen of its current
incarnation, and each takes the upload time and the file counts that the stage summed for
it. The events are copied as they are, those of every incarnation, because they are the
history of a name and not only of the project that holds it now.

A release's record gives a project the rest of what it says of itself, such as its summary
and its licence. Those columns are left empty here, and are filled afterwards from the
record of the project's latest version, which is worked out here.
"""

import sqlite3
from itertools import groupby
from operator import itemgetter

from catalog.stage import RegistryStage, attach_read_only
from pypi_registry.names import latest_version

# The kinds of event that set a project's status: it is created, removed, quarantined or
# let out of quarantine, registered again, or given another status. The registry's other
# events are about a file.
STATUS_KINDS = (
    "created",
    "removed",
    "quarantined",
    "unquarantined",
    "reregistered",
    "status_changed",
)

# Every project of the state, numbered in name order. A project has no release yet, so a
# swept one counts none. One that is not swept, whose page has not been read in full,
# has a count that is not known, and 0 would read as a project with no release.
_INSERT_PROJECTS = """
INSERT INTO main.project (
    project_id, name, incarnation, status, first_seen, first_seen_exact, swept, releases
)
SELECT
    row_number() OVER (ORDER BY name),
    name, incarnation, status, first_seen, first_seen_exact, swept,
    CASE WHEN swept THEN 0 END
FROM state.projects
"""

# The versions that the state has seen of each project's current incarnation, each with
# what the stage summed of its files, in the order of the table's key. A version of an
# earlier incarnation belongs to a name that was removed and registered again, and is not
# a release of the name now.
#
# A version that the stage has no row for has no file whose version could be read, so its
# upload and its file count are not known. It is not yanked either: a release is yanked
# when it has files now and every one of them is, and the reason is kept only then,
# because the stage keeps one reason for a release, which says nothing of a release that
# can still be installed. The upload is the earliest, which the registry writes as
# 2017-08-06T03:43:27.195869Z, in whole seconds. SQLite reads a time with no zone as UTC.
_INSERT_RELEASES = """
INSERT INTO main.release (
    project_id, version, uploaded, files, yanked, yanked_reason, has_record
)
SELECT
    project_id, version, uploaded, files, yanked,
    CASE WHEN yanked THEN yanked_reason END,
    has_record
FROM (
    SELECT
        p.project_id AS project_id,
        v.version AS version,
        unixepoch(substr(r.uploaded, 1, 19)) AS uploaded,
        r.files_now AS files,
        coalesce(r.files_now > 0 AND r.files_yanked = r.files_now, 0) AS yanked,
        r.yanked_reason AS yanked_reason,
        v.record AS has_record
    FROM main.project AS p
    JOIN state.versions_seen AS v
        ON v.name = p.name AND v.incarnation = p.incarnation
    LEFT JOIN stage.release_files AS r
        ON r.name = v.name AND r.incarnation = v.incarnation AND r.version = v.version
)
ORDER BY project_id, version
"""

# The totals of each project's releases. `releases` counts those that have a file now, a
# yanked release among them, and stays unknown for a project that is not swept.
# `first_uploaded` is the earliest upload of any release, including one whose files have
# all gone.
_SET_RELEASE_TOTALS = """
UPDATE main.project
SET
    releases = CASE WHEN project.swept THEN totals.releases END,
    first_uploaded = totals.first_uploaded
FROM (
    SELECT
        project_id,
        count(*) FILTER (WHERE files > 0) AS releases,
        min(uploaded) AS first_uploaded
    FROM main.release
    GROUP BY project_id
) AS totals
WHERE project.project_id = totals.project_id
"""

# The releases that a project offers: those with a file now that are not wholly yanked.
# The table is read in the order of its key, which has the project first.
_OFFERED_RELEASES = """
SELECT project_id, version FROM main.release
WHERE files > 0 AND yanked = 0
ORDER BY project_id
"""

_SET_LATEST_VERSION = "UPDATE main.project SET latest_version = ? WHERE project_id = ?"

# When the last event that set a project's status was observed. An event of an earlier
# incarnation says nothing of this one, as a name that is registered again starts anew.
_SET_STATUS_SINCE = f"""
UPDATE main.project
SET status_since = latest.observed_at
FROM (
    SELECT name, incarnation, max(observed_at) AS observed_at
    FROM stage.events
    WHERE kind IN ({", ".join("?" * len(STATUS_KINDS))})
    GROUP BY name, incarnation
) AS latest
WHERE project.name = latest.name AND project.incarnation = latest.incarnation
"""

# Every event that the stage holds, with the seq that the registry gave it.
_INSERT_EVENTS = """
INSERT INTO main.event (
    seq, name, incarnation, kind, version, filename, detail, source_at, observed_at
)
SELECT
    seq, name, incarnation, kind, version, filename, detail, source_at, observed_at
FROM stage.events
"""

# For each month, the releases uploaded in it and the projects whose first upload was in
# it. A release counts by its upload whether or not it is still there. A month in which
# nothing was uploaded has no row.
_INSERT_GROWTH = """
INSERT INTO main.growth (month, projects, releases)
SELECT month, sum(projects), sum(releases)
FROM (
    SELECT
        strftime('%Y-%m', uploaded, 'unixepoch') AS month,
        0 AS projects,
        count(*) AS releases
    FROM main.release
    WHERE uploaded IS NOT NULL
    GROUP BY month
    UNION ALL
    SELECT
        strftime('%Y-%m', first_uploaded, 'unixepoch') AS month,
        count(*) AS projects,
        0 AS releases
    FROM main.project
    WHERE first_uploaded IS NOT NULL
    GROUP BY month
)
GROUP BY month
"""


def build_registry(con: sqlite3.Connection, stage: RegistryStage) -> None:
    """Fill `project`, `release`, `event` and `growth` of the serving database that
    `con` is open on, from the registry that `stage` holds.

    `project` gets every column but `record_version` and those after it, which come from
    a release's record. The four tables must be empty. `con` must be a connection that
    opens URIs, as the one that `open_db` gives does, because the state and the stage
    are attached by the URIs that make them read-only.

    Everything is committed and the two databases are detached before this returns, so
    that the connection holds no transaction and can attach others. An error leaves them
    attached, and what was written uncommitted, for the caller to close the connection
    on and discard the database.
    """
    attach_read_only(con, stage.state, "state")
    attach_read_only(con, stage.stage, "stage")
    con.execute(_INSERT_PROJECTS)
    con.execute(_INSERT_RELEASES)
    con.execute(_SET_RELEASE_TOTALS)
    _set_latest_versions(con)
    con.execute(_SET_STATUS_SINCE, STATUS_KINDS)
    con.execute(_INSERT_EVENTS)
    con.execute(_INSERT_GROWTH)
    # A database cannot be detached while the transaction that read it is open.
    con.commit()
    con.execute("DETACH DATABASE stage")
    con.execute("DETACH DATABASE state")


def _set_latest_versions(con: sqlite3.Connection) -> None:
    """Give each project the latest version of the releases that it offers. A project
    that offers none keeps no latest version, and neither does one whose releases are
    all refused by PEP 440.

    The releases are read in one pass, in the order of their project, so that the
    versions of one project are in hand at a time, and not the millions of the table.
    """
    offered = con.execute(_OFFERED_RELEASES)
    latest = (
        (latest_version(version for _, version in releases), project_id)
        for project_id, releases in groupby(offered, key=itemgetter(0))
    )
    con.executemany(_SET_LATEST_VERSION, latest)
