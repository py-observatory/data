"""What the files of a release say by their names.

A wheel's name ends in three tags, its Python, its ABI and its platform, each of which may
list several values joined by dots (PEP 425). A file's kind, wheel, sdist or other, is
read from how its name ends. A filename is data from outside: it is only ever split as a
string, never used as a path.
"""

from itertools import product

# How the name of a source distribution ends, the legacy formats included. A name is
# compared with these lowercased.
SDIST_SUFFIXES = (".tar.gz", ".zip", ".tar.bz2", ".tgz", ".tar.xz", ".tar")

_WHEEL = ".whl"


def wheel_tags(filename: str) -> frozenset[tuple[str, str, str]] | None:
    """The (python, abi, platform) tags of the wheel named `filename`, lowercased, with
    every combination of a tag set that lists several values. None when the name does
    not end in `.whl`, or has fewer than five fields split by `-` before it. The last
    three fields are the tags whatever comes before them, so a build tag, or a dash that
    a project's name should not have kept, does not change them."""
    if not filename.lower().endswith(_WHEEL):
        return None
    fields = filename[: -len(_WHEEL)].split("-")
    if len(fields) < 5:
        return None
    python, abi, platform = (field.lower().split(".") for field in fields[-3:])
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
