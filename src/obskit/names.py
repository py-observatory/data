"""Project names as PyPI identifies them.

A project has many spellings of one name, and every stream keys a project on its PEP
503 form. The rule lives in one place, so that two streams always name one project the
same way.
"""

import re

# PEP 503: a run of hyphens, underscores and dots is one hyphen.
_SEPARATORS = re.compile(r"[-_.]+")


def canonical_name(name: str) -> str:
    """The PEP 503 form of a project name: each run of `-`, `_` and `.` is one `-`, and
    the letters are lowercase."""
    return _SEPARATORS.sub("-", name).lower()
