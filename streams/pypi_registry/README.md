# pypi-registry

The registry of PyPI: every project, its releases and files, which files are yanked and why, and the status of each project. It is published as SQLite files compressed with zstd, in the `pypi-registry-current` release and the dated releases beside it. The [repository README](../../README.md) says how to read them.

## Where the data comes from

Everything is read from PyPI, one request at a time, with a User-Agent that names this repository and holds no email address.

- The Simple API root, `https://pypi.org/simple/`, lists every project with its serial. A run compares it with the serials it stored, to find the projects that are new, changed or gone.
- The page of a changed project, `https://pypi.org/simple/<project>/`, lists its files (size, upload time, `requires-python`, whether a file is yanked and why, and whether it has core metadata and a provenance link) and the status of the project.
- The metadata of a release comes once, from `https://pypi.org/pypi/<project>/<version>/json`: when a run sees a new release, and from the records pass for the release that a project is at.

The Simple API is read as PEP 691 JSON (`application/vnd.pypi.simple.v1+json`). PyPI's terms of use are linked from `stream.toml`.

## What is kept

- Project names are in PEP 503 canonical form, and versions are in PEP 440 canonical form. The spelling that PyPI gave is kept beside them.
- There is no README text. A release keeps a fingerprint of its README, its size, its word count and its format.
- There are no email addresses, in the metadata of releases or in the yank reasons of files. Hidden forms such as `jane%40x.org` and `jane [at] x [dot] org` (in any pair of brackets) count, and plain words such as `jane at x dot org` do not, because ordinary prose reads the same way. The names of people are kept. What counts as an address, and how one is taken out of a text, is defined in the kit's `obskit.emails`.

## In this folder

- `stream.toml`: the stream's id, its upstream and terms of use, and each table with its history class.
- `src/pypi_registry/names.py`: canonical names and versions, the version that a filename carries, and the latest of a project's versions as PyPI names it, which the catalog uses too.
- `src/pypi_registry/simple.py`: the Simple API's URLs and the parsers for its root and project pages. A reply of a shape that the parsers do not know raises a ValueError.
- `src/pypi_registry/records.py`: the record kept for each release, with the facts of its README in place of its text. Email addresses are taken out of it, as they are out of the yank reasons of files, and the names of people stay. A reply of a shape that it does not know raises a ValueError.
- `src/pypi_registry/diff.py`: what changed since the last run. It sorts the names of the root into new, changed, vanished and returned, compares a project page with the stored files, and reads the reply to a probe of a name that left the root. It also defines the events that a run records.
- `src/pypi_registry/store.py`: the state database and the delta databases. A run writes each change as a numbered row of a delta, and `apply_delta` applies the rows to the state in order. The state at the end of any day can be rebuilt from the state at the start of the month and that month's deltas.
- `src/pypi_registry/layout.py`: the names of the databases in the releases, and what a run seals when a day or a month has ended.
- `src/pypi_registry/process.py`: the processing of projects. It probes the names that left the root, and reads the page of each project that is new, changed or listed again, writing what it finds to the open delta. A page that is missing or older than the root says is skipped and read again by the next run, a quarantined page keeps the stored files, and no file, version or release record is written twice. It also holds the records pass's step for one project, which reads the record of the release that the project is at.
- `src/pypi_registry/run.py`: the run. It reads the last published state, and refuses a root older than that state before it writes anything. When a day has ended, it seals the open delta into the month's release. When a month has ended, it first rebuilds the month's state from its base copy and its deltas and checks that this equals the state, and only then merges the month's deltas into its part. It then processes the projects (`process.py`), applies the delta to the state, checks the new manifest against the last one with the publish gates, and publishes. The sweep (`sweep`) is the same run without a root: it reads the page of each project that no run has read in full yet. The records pass (`records`) is another such run, which reads the record of each project's current release that the state lacks.
- `tests/`: offline tests, and the fixtures they read. `run_helpers.py` holds what the tests of processing (`test_process.py`), of the run (`test_run.py`), of the sweep (`test_sweep.py`) and of the records pass (`test_registry_records_pass.py`) share: a clock and a transport, PyPI's replies, and the runs of a stream on a fake GitHub. The texts that the tests of records (`test_records.py`) and of the Simple API (`test_simple.py`) share, which are forms of email addresses, texts that only look like one, and texts made to be slow to scrub, come from the kit's `obskit.testing`. `conftest.py` closes the connections that a test opened, which are kept in `opened_connections.py`, a module that imports nothing of the stream.

## What is published

- `pypi-registry-current` holds the state (`pypi-registry-state.db.zst`), the open delta of the day of the last run (`pypi-registry-delta-open.db.zst`) and `manifest.json`, which lists every asset of the stream.
- `pypi-registry-month-YYYY-MM` holds the state as the month began (`pypi-registry-state-base-YYYY-MM.db.zst`) and the delta of each day that had a run (`pypi-registry-delta-YYYY-MM-DD.db.zst`). Once the month is over, it also holds the month's deltas merged into one database (`pypi-registry-YYYY-MM.db.zst`) and the month's manifest (`manifest-YYYY-MM.json`). Its assets are never replaced.

The state at the end of any day is the month's base copy with that month's deltas applied in day order, up to that day (`run.replay`). Each run that closes a month checks that this gives back the state before it publishes.

## Running

A run publishes to `GITHUB_REPOSITORY` (`py-observatory/data` when it is not set), with the token in `GITHUB_TOKEN`:

```
uv run --package pypi-registry python -m pypi_registry run
```

`--bootstrap` builds the first state, and only a stream that has never published accepts it. `--override-reason TEXT` publishes although a publish gate failed, and the manifest logs the reason. It does not override a failed month close: when the month that ended, rebuilt from its base copy and its deltas, differs from the state, the run stops whatever the reason, because publishing would build on a month that its releases cannot restore. `--max-seconds` is how long after its start a run stops starting projects (18,000 by default), and the projects that it does not reach are read by the next run. `--workdir PATH` is the folder in which a run makes its own directory and removes it afterwards (`work` by default). A month close holds the month's deltas there, so a workflow can point it at a larger disk. SQLite's temporary files, such as the copy that VACUUM makes of the month's part, go in that folder too, unless `SQLITE_TMPDIR` is set.

A file that the daily run finds is marked as seen when it appeared (`first_seen_exact`), with the time of that run as its `first_seen`. That has a limit while the daily run has a backlog (`coverage.remaining > 0`): a project that one run does not reach is read by a later run, and a file that it gained in between is still marked exact, with the later run's time.

A bootstrap that adds the month's base copy and then fails before it publishes cannot simply be run again. A second bootstrap refuses a `pypi-registry-current` release that the first one left, even a half-made one. Without one, a second bootstrap in the same month stops with CreateOnceError on `pypi-registry-state-base-YYYY-MM.db.zst`, because the state that it builds is new bytes. The way back is a manual reset with the owner: delete the half-made `pypi-registry-current` release, if there is one, and that base copy, and record the deletions in a dated entry in `RETENTION.md`. Waiting for the next month keeps the base copy, as the next month's has another name, but a half-made `pypi-registry-current` must still go.

A month close that keeps failing needs a person, and what to do depends on the error:

- A `GateError` that names the replay: do not override it. The month, rebuilt from its base copy and its sealed deltas, differs from the state, so look into the base copy and the sealed deltas.
- A `CreateOnceError` on the month's part (`pypi-registry-YYYY-MM.db.zst`): usually the zstd or SQLite build changed between a failed attempt and its retry, so the retry built the part as other bytes. First pin back the Python and uv versions that the failed attempt used, and run again. Only if that is impossible, and only with the owner's dated entry in `RETENTION.md`, delete the month's part and `manifest-YYYY-MM.json` from the month's release, so that the next run builds them again.
- Bump the pinned Python (`.python-version`) and uv (the `version` of each `setup-uv` step in the workflows) deliberately, and never on the first of a month.

### The sweep

The bootstrap stores every project of the root without its page, so each starts unswept, and the daily run reads a project's page only when the project changes. The sweep reads the rest:

```
uv run --package pypi-registry python -m pypi_registry sweep
```

It goes on from the last published state and reads no root. For each project whose page no run has read in full, it records the files with their yanks and the status of the project, as the page shows them now. They are rows of the delta and not events, because nothing says when they changed, and none of the files is marked as seen when it appeared: the sweep comes to a project when its turn is, and not when the project changes. It writes no serial and no cursor, so the manifest keeps those of the last daily run. A page that is missing, quarantined or older than the state is skipped, and the next sweep asks for it again; the daily run records a project that PyPI has removed or quarantined when the root stops listing it.

A sweep waits 1.25 seconds between requests, about 0.8 a second, so that the pages of close to a million projects take about two weeks of sweeps. `--max-seconds` is how long after its start a sweep stops starting projects (16,200 by default), so each sweep reads part of the projects. The manifest's `coverage.unswept` says how many are left, and `coverage.remaining` is what the last daily run left to the next one, which a sweep keeps as it was. The data is `complete` only when both are 0, for every run: the manifest of the bootstrap is not complete, because it has read no page. A sweep exits with 3 when it has read some projects and others are left, which is the signal to run it again, and with 0 otherwise. One that reads nothing exits with 0 although projects are left: it met only pages that it cannot use, and another sweep would meet the same ones. Any other failure ends the sweep with an error and publishes nothing, so the next sweep reads again what that one had read.

### The records pass

The record of a release holds what a project says of itself: its summary, its licence, its requirements and the rest. A run reads the record of each release that appears after the bootstrap, but the bootstrap and the sweep store the releases from before it without their records, so most projects lack the record of the release that they are at. The records pass reads it:

```
uv run --package pypi-registry python -m pypi_registry records
```

It goes on from the last published state and reads no root and no page. It takes the projects that are swept and neither removed nor quarantined, and that hold a version without a record, in name order. For each one it works out the release that the project is at, as the catalog does: the highest PEP 440 version among its files that are not yanked, where a final release beats every pre-release. When the state holds that version without a record, the pass asks `https://pypi.org/pypi/<project>/<version>/json` for it and writes the record alone. The version is in the state already, and applying the record marks it as recorded. A project whose current release has a record writes nothing and costs no request. One whose record PyPI does not have (404 or 410) costs its request and writes nothing. Only the current release is read: the records of a project's older releases from before 2026-11-01 are not fetched.

It waits 1.25 seconds between requests, as the sweep does, so its roughly 900,000 requests take about 75 runs of 15,000 seconds, about two weeks. `--max-seconds` is how long after its start a run stops starting projects (16,200 by default). The manifest's `coverage.records_after` is the last project that a records run came to, and the next one goes on after it. `coverage.records_left` is how many projects that may lack a record come after that. Most projects keep an older release without a record, so most of those that the pass comes to have nothing to ask for, and `records_left` counts more projects than there are requests left. A run that comes to the end of the list sets `records_after` to null, so that the next one starts again from the first name, and until then a record that PyPI did not have is not asked for again. Both are null until the first records run, and every other run keeps them as they were. A records run exits with 3 when it came to some projects and others are left, which is the signal to run it again, and with 0 otherwise. It changes neither the daily run's backlog nor whether the data is `complete`.

### On GitHub

`tick.yml` runs the daily run every six hours, and the [osv-pypi](../osv_pypi/README.md) stream's run beside it, in a job of its own. `backfill.yml` runs the sweep and the records pass, each as a chain of runs that each dispatch the next, and its `task` input says which. `smoke.yml` runs the kit's weekly smoke test. Start them in this order: push the repository, dispatch `tick.yml` with `bootstrap` set to `pypi-registry`, dispatch `smoke.yml` with `bootstrap`, and then dispatch `backfill.yml` to start the sweep. Once the sweep is done (`coverage.unswept` is 0), dispatch `backfill.yml` again with `-f task=records` to start the records pass. A scheduled tick or smoke run that comes before its bootstrap fails closed and opens its rolling issue. That is expected, and the first run that succeeds closes it.

The tick's `bootstrap` names the one stream that it is for, and so does an override. `override_reason` publishes although a gate failed, and only for the stream that `override_for` names, so give both: `gh workflow run tick.yml -f override_for=pypi-registry -f override_reason='the reason'`. A reason without `override_for` overrides nothing. A bootstrap or an override that names osv-pypi leaves the registry's run an ordinary one, and the other way round.

The tick and the backfill share one concurrency group, which holds one running job and one waiting job, and a job that queues replaces the one that waits. So while a backfill chain goes on the Actions page shows some cancelled ticks: a tick that queues behind a waiting tick replaces it, and so can a run that the backfill dispatches. This is harmless, because the next tick catches up.

There is never more than one backfill chain: before each try at dispatching a run, the backfill looks for another of its runs that has not completed, and leaves the chain to it if there is one. So run one task at a time, because a task dispatched while the other's chain goes on ends one of the two chains. To stop the chain, disable the workflow with `gh workflow disable backfill.yml`. Cancelling a run that is still waiting does not stop it, because the report job then dispatches a new one. The runs already under way finish, and the next dispatch fails while the workflow is disabled, which opens the backfill's issue. To resume, enable the workflow with `gh workflow enable backfill.yml` and dispatch it once with the task that was running, such as `gh workflow run backfill.yml -f task=sweep`. The first good run closes the issue.

## Tests

The stream is a member of the repository's uv workspace, so sync every package once before running its tests:

```
uv sync --all-packages
uv run pytest streams/pypi_registry/tests
```
