"""PyPI's Simple API (PEP 691 JSON): the URLs to read, and parsers for the replies.

`/simple/` lists every project with its serial, and `/simple/<project>/` lists one
project's files and its status. A reply is data from outside. It is read as JSON and
never run, and a reply of any other shape raises ValueError: a guess at a missing or
odd value would be stored as a fact. Keys that this module does not name are ignored,
because the API gains keys over time. A reply that does not say it is API major
version 1 is refused, because another major version may lay a reply out another way.
"""

import json
import reprlib
from dataclasses import dataclass
from typing import TypeGuard
from urllib.parse import quote

from pypi_registry.emails import without_addresses
from pypi_registry.names import (
    canonical_name,
    canonical_version,
    version_from_filename,
)

ROOT_URL = "https://pypi.org/simple/"
# The reply to this type is PEP 691 JSON of API major version 1.
ACCEPT = "application/vnd.pypi.simple.v1+json"

# The major version of the API that is read: the text before the first dot of
# `meta.api-version`, so "1.4" is major version 1.
_API_MAJOR = "1"
# How much of a bad value an error message shows. A reply can hold anything, and its
# error goes to a log and to an issue.
_SHOWN = 60
# repr() cannot be trusted to show it. It recurses once for each level of nesting, and
# overflows the stack on a value that json read, which can be nested tens of thousands
# of levels deep. It also builds the whole text of a huge list or string before any of
# that is cut. This stops at three levels, and keeps a few items of a list and the two
# ends of a long string, so its work does not grow with the value.
_REPR = reprlib.Repr(maxlevel=3, maxstring=_SHOWN)


def project_url(name: str) -> str:
    """The Simple API page of a project, from its canonical name."""
    return f"{ROOT_URL}{name}/"


def version_url(name: str, version: str) -> str:
    """The JSON API page of one release, from the canonical name of its project. A
    version can hold `+` and `!`, which mean something in a URL, so it is quoted."""
    return f"https://pypi.org/pypi/{name}/{quote(version, safe='')}/json"


@dataclass(frozen=True)
class Root:
    """The index as `/simple/` lists it."""

    serial: int  # meta._last-serial: how far the index has got
    projects: dict[str, int]  # canonical name -> that project's _last-serial


@dataclass(frozen=True)
class FileInfo:
    """One file of a project page."""

    filename: str
    version: str | None  # canonical, from the filename; None when it will not parse
    size: int | None
    upload_time: str | None  # as the page writes it
    requires_python: str | None  # as the page writes it, never parsed
    yanked: bool
    yanked_reason: str | None  # with no email address; None if not yanked or no reason
    has_metadata: bool  # the file's core metadata is served beside it
    has_provenance: bool  # the file has a provenance (attestation) link


@dataclass(frozen=True)
class ProjectPage:
    """One project's `/simple/<project>/` page."""

    name: str  # canonical
    serial: int  # meta._last-serial
    status: str  # the PEP 792 status as written, such as active or quarantined
    status_reason: str | None
    files: list[FileInfo]
    versions: list[str]  # canonical


def _show(value: object) -> str:
    """`value` as an error message shows it: its start, in at most _SHOWN characters.
    Whatever the reply holds, this neither recurses without a bound nor does work that
    grows with the value."""
    text = _REPR.repr(value)
    return text if len(text) <= _SHOWN else f"{text[: _SHOWN - 3]}..."


def _refuse(what: str, path: str, expected: str, value: object) -> ValueError:
    """The error for a value of the wrong shape: where it is and what was expected."""
    return ValueError(f"{what}: {path} must be {expected}, not {_show(value)}")


def _is_count(value: object) -> TypeGuard[int]:
    """Whether `value` is a JSON integer that is not negative, for a serial or a size.
    A bool is not one, though Python counts it as an int, and nor is a float."""
    return type(value) is int and value >= 0


def _load_object(body: bytes, what: str) -> dict:
    """The JSON object in `body`, or a ValueError for a body that is not JSON, is not
    an object, or is nested too deeply for the JSON reader."""
    try:
        document = json.loads(body)
    except RecursionError as exc:
        raise ValueError(f"{what} is nested too deeply to read") from exc
    except ValueError as exc:  # a JSONDecodeError, or a UnicodeDecodeError
        raise ValueError(f"{what} is not JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"{what} must be a JSON object, not {_show(document)}")
    return document


def _read_serial(document: dict, what: str) -> int:
    """The `meta._last-serial` of a reply, once `meta.api-version` is known to be major
    version 1. The version comes first, so a reply laid out another way is refused for
    its version and not for whatever else differs."""
    meta = document.get("meta")
    if not isinstance(meta, dict):
        raise _refuse(what, "meta", "an object", meta)
    version = meta.get("api-version")
    if not isinstance(version, str) or version.split(".")[0] != _API_MAJOR:
        expected = f"a version of API major version {_API_MAJOR}, like '{_API_MAJOR}.4'"
        raise _refuse(what, "meta.api-version", expected, version)
    serial = meta.get("_last-serial")
    if not _is_count(serial):
        raise _refuse(what, "meta._last-serial", "a non-negative integer", serial)
    return serial


def _read_project(entry: object, index: int, what: str) -> tuple[str, int]:
    """The name and serial of one entry of the root's `projects`. This runs once for
    every project on PyPI, hundreds of thousands of times, so the path of an entry is
    built only when the entry is wrong."""
    if not isinstance(entry, dict):
        raise _refuse(what, f"projects[{index}]", "an object", entry)
    name = entry.get("name")
    if not isinstance(name, str) or not name:
        raise _refuse(what, f"projects[{index}].name", "a non-empty string", name)
    serial = entry.get("_last-serial")
    if not _is_count(serial):
        expected = "a non-negative integer"
        raise _refuse(what, f"projects[{index}]._last-serial", expected, serial)
    return name, serial


def parse_root(body: bytes) -> Root:
    """The index serial and every project's serial, from the body of `ROOT_URL`.

    Each name is made canonical. When two names meet in one canonical name, the higher
    serial is kept. Raises ValueError for a body that is not an API version 1 root.
    """
    what = "the Simple API root"
    document = _load_object(body, what)
    serial = _read_serial(document, what)
    entries = document.get("projects")
    if not isinstance(entries, list):
        raise _refuse(what, "projects", "a list", entries)
    projects: dict[str, int] = {}
    for index, entry in enumerate(entries):
        name, project_serial = _read_project(entry, index, what)
        key = canonical_name(name)
        if project_serial > projects.get(key, -1):
            projects[key] = project_serial
    return Root(serial, projects)


def _read_status(document: dict, what: str) -> tuple[str, str | None]:
    """A project's PEP 792 status and its reason. A page with no `project-status` is
    active. A status that this code has not met is kept as written."""
    if "project-status" not in document:
        return "active", None
    marker = document["project-status"]
    if not isinstance(marker, dict):
        raise _refuse(what, "project-status", "an object", marker)
    status = marker.get("status")
    if not isinstance(status, str) or not status:
        raise _refuse(what, "project-status.status", "a non-empty string", status)
    reason = marker.get("reason")
    if reason is not None and not isinstance(reason, str):
        raise _refuse(what, "project-status.reason", "a string or null", reason)
    return status, reason


def _read_yanked(value: object, path: str, what: str) -> tuple[bool, str | None]:
    """Whether a file is yanked, and why. PEP 691 gives a bool, or else a non-empty
    string, which means yanked and is the reason. The empty string is refused: PEP 691
    would read it as not yanked, and PEP 592's HTML form as yanked with no reason.

    A person wrote the reason, so it loses any email address, as a release's does. A
    reason that nothing is left of is None, and the file is still yanked."""
    if isinstance(value, bool):
        return value, None
    if isinstance(value, str) and value:
        return True, without_addresses(value)
    raise _refuse(what, path, "a boolean or a non-empty reason string", value)


def _read_optional_string(entry: dict, key: str, where: str, what: str) -> str | None:
    """The string at `key` of a file entry, or None when it is null or left out."""
    value = entry.get(key)
    if value is not None and not isinstance(value, str):
        raise _refuse(what, f"{where}.{key}", "a string or null", value)
    return value


def _read_file(entry: object, index: int, what: str) -> FileInfo:
    """One entry of a page's `files`.

    `has_metadata` is whether `core-metadata` is truthy (PyPI writes the hashes of the
    metadata file, or true or false), and `has_provenance` is whether `provenance` is
    present and not null. The hashes and the URL of the file are not kept.
    """
    where = f"files[{index}]"
    if not isinstance(entry, dict):
        raise _refuse(what, where, "an object", entry)
    filename = entry.get("filename")
    if not isinstance(filename, str) or not filename:
        raise _refuse(what, f"{where}.filename", "a non-empty string", filename)
    size = entry.get("size")
    if size is not None and not _is_count(size):
        raise _refuse(what, f"{where}.size", "a non-negative integer or null", size)
    upload_time = _read_optional_string(entry, "upload-time", where, what)
    requires_python = _read_optional_string(entry, "requires-python", where, what)
    # A file with no `yanked` key is not yanked.
    yanked, yanked_reason = _read_yanked(
        entry.get("yanked", False), f"{where}.yanked", what
    )
    return FileInfo(
        filename=filename,
        version=version_from_filename(filename),
        size=size,
        upload_time=upload_time,
        requires_python=requires_python,
        yanked=yanked,
        yanked_reason=yanked_reason,
        has_metadata=bool(entry.get("core-metadata")),
        has_provenance=entry.get("provenance") is not None,
    )


def _read_versions(document: dict, what: str) -> list[str]:
    """The page's `versions`, each made canonical (one that is not PEP 440 is kept as
    written)."""
    entries = document.get("versions")
    if not isinstance(entries, list):
        raise _refuse(what, "versions", "a list", entries)
    for index, entry in enumerate(entries):
        if not isinstance(entry, str) or not entry:
            raise _refuse(what, f"versions[{index}]", "a non-empty string", entry)
    return [canonical_version(entry) for entry in entries]


def parse_project_page(body: bytes) -> ProjectPage:
    """One project's name, serial, status, files and versions, from the body of its
    `project_url`.

    The name is the page's own `name`, made canonical, and the status is its
    `project-status`, or active when it has none. Raises ValueError for a body that is
    not an API version 1 project page.
    """
    what = "a project page"
    document = _load_object(body, what)
    serial = _read_serial(document, what)
    name = document.get("name")
    if not isinstance(name, str) or not name:
        raise _refuse(what, "name", "a non-empty string", name)
    status, status_reason = _read_status(document, what)
    entries = document.get("files")
    if not isinstance(entries, list):
        raise _refuse(what, "files", "a list", entries)
    files = [_read_file(entry, index, what) for index, entry in enumerate(entries)]
    versions = _read_versions(document, what)
    return ProjectPage(
        name=canonical_name(name),
        serial=serial,
        status=status,
        status_reason=status_reason,
        files=files,
        versions=versions,
    )
