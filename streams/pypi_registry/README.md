# pypi-registry

The registry of PyPI: every project, its releases and files, which files are yanked and why, and the status of each project. It is published as SQLite files compressed with zstd, in the `pypi-registry-current` release and the dated releases beside it. The [repository README](../../README.md) says how to read them.

## Where the data comes from

Everything is read from PyPI, one request at a time, with a User-Agent that names this repository and holds no email address.

- The Simple API root, `https://pypi.org/simple/`, lists every project with its serial. A run compares it with the serials it stored, to find the projects that are new, changed or gone.
- The page of a changed project, `https://pypi.org/simple/<project>/`, lists its files (size, upload time, `requires-python`, whether a file is yanked and why, and whether it has core metadata and a provenance link) and the status of the project.
- The metadata of a release comes once, from `https://pypi.org/pypi/<project>/<version>/json`.

The Simple API is read as PEP 691 JSON (`application/vnd.pypi.simple.v1+json`). PyPI's terms of use are linked from `stream.toml`.

## What is kept

- Project names are in PEP 503 canonical form, and versions are in PEP 440 canonical form. The spelling that PyPI gave is kept beside them.
- There is no README text. A release keeps a fingerprint of its README, its size, its word count and its format.
- There are no email addresses. The names of people are kept.

## In this folder

- `stream.toml`: the stream's id, its upstream and terms of use, and each table with its history class.
- `src/pypi_registry/names.py`: canonical names and versions, and the version that a filename carries.
- `src/pypi_registry/simple.py`: the Simple API's URLs and the parsers for its root and project pages. A reply of a shape that the parsers do not know raises a ValueError.
- `src/pypi_registry/records.py`: the record kept for each release, with the facts of its README in place of its text. Email addresses are taken out of it, and the names of people stay. A reply of a shape that it does not know raises a ValueError.
- `src/pypi_registry/diff.py`: what changed since the last run. It sorts the names of the root into new, changed, vanished and returned, compares a project page with the stored files, and reads the reply to a probe of a name that left the root. It also defines the events that a run records.
- `src/pypi_registry/store.py`: the state database and the delta databases. A run writes each change as a numbered row of a delta, and `apply_delta` applies the rows to the state in order. The state at the end of any day can be rebuilt from the state at the start of the month and that month's deltas.
- `src/pypi_registry/layout.py`: the names of the databases in the releases, and what a run seals when a day or a month has ended.
- `src/pypi_registry/process.py`: the processing of projects. It probes the names that left the root, and reads the page of each project that is new, changed or listed again, writing what it finds to the open delta. A page that is missing or older than the root says is skipped and read again by the next run, a quarantined page keeps the stored files, and no file, version or release record is written twice.
- `src/pypi_registry/run.py`: the run. It reads the last published state, and refuses a root older than that state before it writes anything. When a day has ended, it seals the open delta into the month's release. When a month has ended, it first rebuilds the month's state from its base copy and its deltas and checks that this equals the state, and only then merges the month's deltas into its part. It then processes the projects (`process.py`), applies the delta to the state, checks the new manifest against the last one with the publish gates, and publishes.
- `tests/`: offline tests, and the fixtures they read. `run_helpers.py` holds what the tests of processing (`test_process.py`) and of the run (`test_run.py`) share, and `conftest.py` closes the connections that a test opened.

## What is published

- `pypi-registry-current` holds the state (`pypi-registry-state.db.zst`), the open delta of the day of the last run (`pypi-registry-delta-open.db.zst`) and `manifest.json`, which lists every asset of the stream.
- `pypi-registry-month-YYYY-MM` holds the state as the month began (`pypi-registry-state-base-YYYY-MM.db.zst`) and the delta of each day that had a run (`pypi-registry-delta-YYYY-MM-DD.db.zst`). Once the month is over, it also holds the month's deltas merged into one database (`pypi-registry-YYYY-MM.db.zst`) and the month's manifest (`manifest-YYYY-MM.json`). Its assets are never replaced.

The state at the end of any day is the month's base copy with that month's deltas applied in day order, up to that day (`run.replay`). Each run that closes a month checks that this gives back the state before it publishes.

## Running

A run publishes to `GITHUB_REPOSITORY` (`py-observatory/data` when it is not set), with the token in `GITHUB_TOKEN`:

```
uv run --package pypi-registry python -m pypi_registry run
```

`--bootstrap` builds the first state, and only a stream that has never published accepts it. `--override-reason TEXT` publishes although a publish gate failed, and the manifest logs the reason. It does not override a failed month close: when the month that ended, rebuilt from its base copy and its deltas, differs from the state, the run stops whatever the reason, because publishing would build on a month that its releases cannot restore. `--max-seconds` is how long after its start a run stops starting projects (18,000 by default), and the projects that it does not reach are read by the next run.

A bootstrap that adds the month's base copy and then fails before it publishes cannot simply be run again. A second bootstrap refuses a `pypi-registry-current` release that the first one left, even a half-made one. Without one, a second bootstrap in the same month stops with CreateOnceError on `pypi-registry-state-base-YYYY-MM.db.zst`, because the state that it builds is new bytes. The way back is a manual reset with the owner: delete the half-made `pypi-registry-current` release, if there is one, and that base copy, and record the deletions in a dated entry in `RETENTION.md`. Waiting for the next month keeps the base copy, as the next month's has another name, but a half-made `pypi-registry-current` must still go.

## Tests

The stream is a member of the repository's uv workspace, so sync every package once before running its tests:

```
uv sync --all-packages
uv run pytest streams/pypi_registry/tests
```
