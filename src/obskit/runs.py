"""What a run sets up before its first step: its GitHub client and its work directory.

`github_from_env` makes the client from the variables that a workflow gives its job.
`run_directory` makes the directory that the run works in, and removes it when the run
ends.
"""

import os
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from obskit.github import GitHub

# The repository that a run publishes to when GITHUB_REPOSITORY does not say.
REPOSITORY = "py-observatory/data"


def github_from_env() -> GitHub:
    """The client of a run: for the repository that GITHUB_REPOSITORY names, or
    REPOSITORY when it is not set, with the token in GITHUB_TOKEN.

    A run makes one client and uses it for every call. The client paces its writes to
    GitHub's limits, and a second client would not count the writes of the first.
    Raises KeyError when GITHUB_TOKEN is not set.
    """
    return GitHub(
        os.environ.get("GITHUB_REPOSITORY", REPOSITORY), os.environ["GITHUB_TOKEN"]
    )


@contextmanager
def run_directory(base: Path) -> Generator[Path]:
    """Make a new directory for one run in `base`, and give the block its path. It is
    removed when the block ends, an error included.

    `base` is made if it is missing, though not its parents, and a relative `base` is
    taken from the working directory. The new directory's name starts with `run-`.

    SQLite writes its temporary files, VACUUM's copy of a whole database among them, to
    SQLITE_TMPDIR, and otherwise to /var/tmp or /tmp, which may be on another disk.
    This sets SQLITE_TMPDIR to `base` before the block starts, so before any SQLite
    work. That keeps the files on the work's disk, and so in the free disk that a month
    close logs. A folder that is set already is kept, and the setting stays for the rest
    of the process.
    """
    base = base.absolute()
    base.mkdir(exist_ok=True)
    os.environ.setdefault("SQLITE_TMPDIR", str(base.resolve()))
    with tempfile.TemporaryDirectory(prefix="run-", dir=base) as workdir:
        yield Path(workdir)
