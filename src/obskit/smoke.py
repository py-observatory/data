"""The kit's smoke test: a swap publish and a read back, on the stream `kit-smoke`.

The unit tests run the kit against a fake of GitHub. This runs it against the real
releases API, every week (see .github/workflows/smoke.yml), so a change in GitHub's
behaviour shows up on a stream that holds nothing of value, and not on the registry.

`run` reads the stream's prior state from `kit-smoke-current` (`fetch_prior`), which
holds a database of one table, `counter`, with one row. It publishes a new state whose
counter is one higher (`publish_current`), and then reads the release back as a
stream's next run would and checks that it holds what was published: the same manifest,
the same sha256 for the file and for the database in it, and the new counter. The first
run needs `--bootstrap`, and only a stream that has never published accepts it, as for
every stream.

Every error of the kit propagates, a 404 and a 502 alike, and ends the run. A read back
that differs from the publish is reported and returns 1.
"""

import argparse
import json
import os
import sqlite3
import sys
import tempfile
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from obskit.github import GitHub
from obskit.manifest import Manifest, Watermark, producer
from obskit.release import fetch_prior, prepare_asset, publish_current
from obskit.tags import current_tag
from obskit.zst import sha256_file

STREAM = "kit-smoke"

# The repository that `main` publishes to when GITHUB_REPOSITORY does not say.
_REPOSITORY = "py-observatory/data"
_STATE_DB = "kit-smoke-state.db"
# The history class of each table, as `prepare_asset` asks for them.
_CLASSES = {"counter": "snapshot"}


def _counter(db: Path) -> int:
    """The counter in the state database `db`. Raises ValueError unless the table has
    exactly one row."""
    with closing(
        sqlite3.connect(f"{db.resolve(strict=True).as_uri()}?mode=ro", uri=True)
    ) as con:
        rows = con.execute("SELECT n FROM counter").fetchall()
    if len(rows) != 1:
        raise ValueError(f"{db} must hold one counter, and holds {len(rows)} rows")
    return rows[0][0]


def _write_state(db: Path, counter: int) -> None:
    with closing(sqlite3.connect(db)) as con:
        con.execute("CREATE TABLE counter (n INTEGER NOT NULL)")
        con.execute("INSERT INTO counter (n) VALUES (?)", (counter,))
        con.commit()


def run(gh: GitHub, workdir: Path, *, bootstrap: bool) -> int:
    """One smoke test in the empty directory `workdir`. Returns 0 when the release
    read back is what was published, and 1 when it differs.

    The prior state is read with `fetch_prior`, with `bootstrap` as given, so a stream
    that has published refuses `bootstrap` and one that has not refuses to go on
    without it (PriorStateError). The kit's other errors propagate as they are raised.
    """
    prior = fetch_prior(gh, STREAM, workdir, bootstrap=bootstrap)
    counter = 1 if prior is None else _counter(workdir / _STATE_DB) + 1

    # The prior state sits in `workdir` under the names of the new one, so the new
    # state is built in a directory of its own.
    publishing = workdir / "publish"
    publishing.mkdir()
    db = publishing / _STATE_DB
    _write_state(db, counter)
    packed, asset = prepare_asset(
        db, publishing, tag=current_tag(STREAM), role="state", classes=_CLASSES
    )
    manifest = Manifest(
        stream=STREAM,
        producer=producer(gh.repo),
        generated_at=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        complete=True,
        coverage={},
        data_through=Watermark(str(counter), "serial"),
        cursor=str(counter),
        licences=[],
        assets=[asset],
    )
    publish_current(gh, STREAM, [packed], manifest, bootstrap=prior is None)

    readback = workdir / "readback"
    readback.mkdir()
    fetched = fetch_prior(gh, STREAM, readback, bootstrap=False)
    problems = []
    if fetched != manifest:
        problems.append("the manifest in the release is not the one that was published")
    if sha256_file(readback / packed.name) != asset.asset_sha256:
        problems.append(f"{packed.name} was published with a different sha256")
    if sha256_file(readback / _STATE_DB) != asset.db_sha256:
        problems.append(f"{_STATE_DB} was published with a different sha256")
    elif _counter(readback / _STATE_DB) != counter:
        problems.append(f"the counter that was published is not {counter}")
    if problems:
        print(
            f"{STREAM}: the release read back is not what was published to "
            f"{current_tag(STREAM)}: {'; '.join(problems)}",
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps({"stream": STREAM, "counter": counter, "sha256": asset.asset_sha256})
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run the smoke test from the command line: `python -m obskit.smoke`.

    It needs GITHUB_TOKEN, and publishes to GITHUB_REPOSITORY, or to
    py-observatory/data when that is not set. It makes one GitHub client for the whole
    run, and works in a new directory under ./work, which is removed afterwards.
    `--bootstrap` creates `kit-smoke-current`, and only a stream that has never
    published accepts it. Returns what `run` returns, and every error propagates.
    """
    parser = argparse.ArgumentParser(
        prog="python -m obskit.smoke",
        description="Publish to kit-smoke-current and read it back.",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="create the release: only for a stream that has never published",
    )
    args = parser.parse_args(argv)
    gh = GitHub(
        os.environ.get("GITHUB_REPOSITORY", _REPOSITORY), os.environ["GITHUB_TOKEN"]
    )
    work = Path.cwd() / "work"
    work.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="smoke-", dir=work) as workdir:
        return run(gh, Path(workdir), bootstrap=args.bootstrap)


if __name__ == "__main__":
    raise SystemExit(main())
