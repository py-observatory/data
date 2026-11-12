"""Names and versions as PyPI identifies them.

A project is known by its PEP 503 name and a release by its PEP 440 version, and each
has many spellings of one thing. The stream keys its tables on the canonical form and
keeps the raw spelling where a table needs it.
"""

import re
from collections.abc import Iterable

from packaging.utils import parse_sdist_filename, parse_wheel_filename
from packaging.version import Version

# PEP 503: a run of hyphens, underscores and dots is one hyphen.
_SEPARATORS = re.compile(r"[-_.]+")


def canonical_name(name: str) -> str:
    """The PEP 503 form of a project name: each run of `-`, `_` and `.` is one `-`, and
    the letters are lowercase."""
    return _SEPARATORS.sub("-", name).lower()


def canonical_version(raw: str) -> str:
    """The PEP 440 form of a version, as `packaging` writes it: "1.0.0-RC1" is
    "1.0.0rc1".

    A version that PEP 440 does not allow comes back as it was written. Old releases
    have such versions, and they are data and not errors. `packaging` signals every
    version that it refuses with a ValueError. That includes InvalidVersion, and also
    a number of over 4,300 digits, which Python will not turn into an int.
    """
    try:
        return str(Version(raw))
    except ValueError:
        return raw


def version_from_filename(filename: str) -> str | None:
    """The canonical version in the name of a wheel or an sdist, or None when the
    filename is neither or does not parse (an egg, an .exe, a name with no version).

    A wheel is told by its `.whl` ending. Everything else goes to the sdist reader,
    which takes `.tar.gz` and `.zip` and refuses any other ending. Each reader raises
    a ValueError subclass for a name it cannot read, and a ValueError for a number of
    over 4,300 digits, and all of them mean no version here.
    """
    try:
        if filename.endswith(".whl"):
            version = parse_wheel_filename(filename)[1]
        else:
            version = parse_sdist_filename(filename)[1]
    except ValueError:
        return None
    return str(version)


def latest_version(versions: Iterable[str]) -> str | None:
    """The latest of `versions`, as PyPI names a project's latest version: the highest
    final release, or when there is none, the highest pre-release or dev release, or
    None when there is no version at all. A final release beats every pre-release, so a
    project at 1.0 that has a 2.0rc1 is at 1.0.

    Versions are ordered as PEP 440 orders them, so 1.10 is above 1.9. A version that
    PEP 440 refuses has no place in the order, and is left out. The string returned is
    the one given, and not the form that PEP 440 gives it. Two strings can be one
    version, as 1.0 and 1.0.0 are, and then the one that sorts last as text is returned,
    so that the answer does not depend on the order of `versions`.
    """
    finals: list[tuple[Version, str]] = []
    pre_releases: list[tuple[Version, str]] = []
    for text in versions:
        try:
            version = Version(text)
        except ValueError:
            # InvalidVersion is one. So is the refusal of a number of over 4,300 digits,
            # which Python will not turn into an int, and which is not an InvalidVersion.
            continue
        (pre_releases if version.is_prerelease else finals).append((version, text))
    chosen = finals or pre_releases
    return max(chosen)[1] if chosen else None
