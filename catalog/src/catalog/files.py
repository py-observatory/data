"""What the files of a release say by their names.

A wheel's name ends in three tags, its Python, its ABI and its platform, each of which may
list several values joined by dots (PEP 425). A file's kind, wheel, sdist or other, is
read from how its name ends. A filename is data from outside: it is only ever split as a
string, never used as a path.

`build_files` sums the files of each project's latest version, as the stage holds them,
into the catalog's latest_files, and lists its wheels' tags in latest_wheel_tag.
"""

import re
import sqlite3
from collections.abc import Iterator
from itertools import groupby, product
from operator import itemgetter

from catalog.stage import RegistryStage, attach_read_only

# How the name of a source distribution ends, the legacy formats included. A name is
# compared with these lowercased.
SDIST_SUFFIXES = (".tar.gz", ".zip", ".tar.bz2", ".tgz", ".tar.xz", ".tar")

_WHEEL = ".whl"

# Every value of a Python, ABI or platform tag starts with a letter and holds only
# letters, digits and underscores, once lowercased: a dash or a dot in a platform is
# written as an underscore (PEP 425).
_TAG_VALUE = re.compile(r"[a-z][a-z0-9_]*")


def wheel_tags(filename: str) -> frozenset[tuple[str, str, str]] | None:
    """The (python, abi, platform) tags of the wheel named `filename`, lowercased, with
    every combination of a tag set that lists several values. None when the name does
    not end in `.whl`, has fewer than five fields split by `-` before it, or has a tag
    value that no tag could have, such as an empty one or one that starts with a digit.
    The last three fields are the tags whatever comes before them, so a build tag, or a
    dash that a project's name should not have kept, does not change them."""
    if not filename.lower().endswith(_WHEEL):
        return None
    fields = filename[: -len(_WHEEL)].split("-")
    if len(fields) < 5:
        return None
    python, abi, platform = (field.lower().split(".") for field in fields[-3:])
    if not all(_TAG_VALUE.fullmatch(value) for value in (*python, *abi, *platform)):
        return None
    return frozenset(product(python, abi, platform))


def file_kind(filename: str) -> str:
    """`"wheel"` when `filename` ends in `.whl`, `"sdist"` when it ends in one of
    SDIST_SUFFIXES, and `"other"` for anything else, such as an egg or an installer.
    Case does not matter."""
    lowered = filename.lower()
    if lowered.endswith(_WHEEL):
        return "wheel"
    if lowered.endswith(SDIST_SUFFIXES):
        return "sdist"
    return "other"


# The files that each project's latest version has now and has not yanked, in the order
# of the projects, so that one project's files are in hand at a time. The stage's
# columns have no type and the catalog's incarnation is an INTEGER, so a plain comparison
# would apply INTEGER affinity and keep SQLite from using current_files' key past the
# name; the unary + drops it, so each version's files are found by the whole key.
_LATEST_FILES = """
SELECT p.project_id, p.latest_version, c.filename, c.size, c.has_provenance
FROM main.project AS p
JOIN stage.current_files AS c
    ON c.name = p.name
    AND c.incarnation = +p.incarnation
    AND c.version = p.latest_version
ORDER BY p.project_id
"""

_INSERT_TAG = (
    "INSERT INTO main.wheel_tag (wheel_tag_id, python, abi, platform) "
    "VALUES (?, ?, ?, ?)"
)
_INSERT_LATEST = """
INSERT INTO main.latest_files (
    project_id, version, files, wheels, sdists, sdist_bytes,
    wheel_min_bytes, wheel_max_bytes, provenance
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_INSERT_LATEST_TAG = (
    "INSERT INTO main.latest_wheel_tag (project_id, wheel_tag_id) VALUES (?, ?)"
)


def build_files(con: sqlite3.Connection, stage: RegistryStage) -> None:
    """Fill `latest_files`, `wheel_tag` and `latest_wheel_tag` of the serving database
    that `con` is open on, from the files of each project's latest version that `stage`
    holds. `project` must hold the projects and their latest versions, as
    `build_registry` leaves it, and the three tables must be empty.

    The tags are numbered in their order, so the files are read twice: once to find
    every tag, and once to write each project's summary, so that only one project's
    files and the set of distinct tags are in hand at a time. `con` must open URIs, as
    the one that `open_db` gives does, because the stage is attached by the URI that
    makes it read-only. Everything is committed and the stage detached before this
    returns. An error leaves it attached, and what was written uncommitted, for the
    caller to close the connection on and discard the database.
    """
    attach_read_only(con, stage.stage, "stage")
    tags: set[tuple[str, str, str]] = set()
    for _, project_tags in _summaries(con):
        tags |= project_tags
    ids = {tag: place for place, tag in enumerate(sorted(tags), start=1)}
    con.executemany(_INSERT_TAG, ((place, *tag) for tag, place in ids.items()))
    for row, project_tags in _summaries(con):
        con.execute(_INSERT_LATEST, row)
        con.executemany(
            _INSERT_LATEST_TAG, ((row[0], ids[tag]) for tag in sorted(project_tags))
        )
    # A database cannot be detached while the transaction that read it is open.
    con.commit()
    con.execute("DETACH DATABASE stage")


def _summaries(
    con: sqlite3.Connection,
) -> Iterator[tuple[tuple, frozenset[tuple[str, str, str]]]]:
    """Each project's summary, as `_summary` makes it, read from the files of its latest
    version one project at a time."""
    for project_id, files in groupby(con.execute(_LATEST_FILES), key=itemgetter(0)):
        yield _summary(project_id, list(files))


def _summary(
    project_id: int, files: list[tuple]
) -> tuple[tuple, frozenset[tuple[str, str, str]]]:
    """The row of latest_files for the project `project_id`, whose latest version's
    `files` are rows of `_LATEST_FILES`, and the tags of its wheels."""
    version = files[0][1]
    wheel_sizes, sdist_sizes = [], []
    wheels = sdists = provenance = 0
    tags: set[tuple[str, str, str]] = set()
    for _, _, filename, size, has_provenance in files:
        provenance += bool(has_provenance)
        kind = file_kind(filename)
        if kind == "wheel":
            wheels += 1
            if size is not None:
                wheel_sizes.append(size)
            tags |= wheel_tags(filename) or frozenset()
        elif kind == "sdist":
            sdists += 1
            if size is not None:
                sdist_sizes.append(size)
    row = (
        project_id,
        version,
        len(files),
        wheels,
        sdists,
        max(sdist_sizes, default=None),
        min(wheel_sizes, default=None),
        max(wheel_sizes, default=None),
        provenance,
    )
    return row, frozenset(tags)
