# osv-pypi

Every advisory in OSV.dev that affects a PyPI project: vulnerabilities, with ids such as `GHSA-*` and `PYSEC-*`, and malware reports, with `MAL-*`. Like the registry, the stream publishes SQLite files compressed with zstd, to the `osv-pypi-current` release and the dated releases beside it. They appear when its first run, a bootstrap, has published (see "On GitHub"). The [repository README](../../README.md) says how to read them.

## Where the data comes from

OSV.dev's public bucket, `https://osv-vulnerabilities.storage.googleapis.com/PyPI/`, which is read one request at a time with a User-Agent that names this repository and holds no email address.

- `modified_id.csv` lists every advisory with the time it last changed, newest first, as lines of `<time>,<id>`. The `x-goog-generation` header of the reply says which copy of the file was served, and that generation only grows.
- `all.zip` holds the record of every advisory.
- `<id>.json` is the record of one advisory.

OSV.dev's terms are linked from `stream.toml`, which also lists the databases that its advisories come from, with their licences.

## What is kept

Each record that a run writes is kept as one version of its advisory, with what the record says of PyPI.

- The id, and the kind: `malware` for an id that starts with `MAL-`, and `vulnerability` for every other. When the record was modified, published and withdrawn, as OSV writes the times.
- The summary, and in place of the text of `details` its facts: the sha256 of its UTF-8 bytes, how many bytes it has and how many words. A change to the text shows as another fingerprint, and the text itself is not republished.
- The aliases, the related ids and the severity scores.
- The PyPI entries of `affected`. Each has the project's name in PEP 503 canonical form and as OSV wrote it, its ranges with their events, and the versions that it lists. Versions are kept exactly as OSV gives them, so reading them as PEP 440 versions is left to whoever uses the data.
- The references, each its type and its URL.
- Five keys of `database_specific`: `severity`, `cwe_ids`, `github_reviewed`, `github_reviewed_at` and `nvd_published_at`.
- The schema version of the record.

What is left out: the entries of `affected` for other ecosystems, `credits`, which name people and how to reach them, the origins and indicators of the malware reports, and every other key. OSV's schema keeps gaining keys, and a key that the stream does not name is not read.

There are no email addresses. What counts as one, hidden forms included, and how one is taken out of a text, is defined in the kit's `obskit.emails`, as for the registry. Addresses are taken out of the summary. A reference whose URL holds an email address is left out, and so is a `mailto:` link. A URL is kept whole or not at all, because taking the address out would leave a link that points nowhere, in a part that is never replaced. Today about 1.3% of the references are left out this way, nearly all of them archive links of a Fedora mailing list, because the list's address is in the URL. The `repo` of a range is left out in the same way when it holds an address, as `git@github.com:foo/bar.git` does. Any other text, such as a version, a project name or an alias, is kept as it is, because changing it would change what it names. A record with an address in one of those is refused whole, as a record that does not parse.

## The tables

The stream has five tables. Two make up the state, and three record each change to it as rows of a delta. The history class of each table is in `stream.toml`.

- `meta` (state): `cursor`, `bootstrap_at` and `applied_seq`, each as a key and its value. The cursor is the generation of the last index that a run read, `bootstrap_at` is when the state was first built, and `applied_seq` is the highest `seq` of a delta row that the state has applied.
- `advisories` (state): one row for each advisory that the stream has seen. Its `status` is `active` while OSV's index lists it and `removed` once it does not. `first_seen` is when the stream first saw it, which for the advisories of the first run is the time of that run, and not when the advisory first appeared. The rest of the row is the advisory's last version: the kept fields listed under `versions` below, and the `seq` of the delta row that holds it.
- `versions` (delta): each version that a run read, so a delta can hold several versions of one advisory. Its columns are `seq`, `observed_at` (the time of the run), and the kept fields: `id`, `kind`, `modified`, `modified_key`, `published`, `withdrawn`, `summary`, `details_sha256`, `details_bytes`, `details_words`, `aliases`, `related`, `severity`, `affected`, `refs`, `database_specific` and `schema_version`. `modified_key` is `modified` with its fraction padded to nine digits, so that it sorts as the time does. The references are `refs`, because `references` is a keyword of SQL. The columns that hold lists or objects hold compact JSON text with its keys in order, and are null when they would be empty.
- `events` (delta): `removed`, when an advisory that the state held as active is no longer in the index, and `returned`, when a removed one is listed again.
- `cursors` (delta): the generation of each index that a run read.

## How a run goes

A run goes on from the last published state, which it reads from `osv-pypi-current`. That fails closed: a state that cannot be read ends the run, and only a bootstrap starts from nothing.

1. It reads the index, with the generation of the copy that was served. An index whose generation is below the stored cursor is a stale copy, and the run stops before it writes anything.
2. When a day has ended since the open delta began, it seals the open delta into the month's release. When a month has ended, it first rebuilds the month's state from its base copy and its deltas and checks that this equals the state, and only then merges the month's deltas into its part (see "What is published").
3. It compares the index with the state. An advisory that the state lacks, or that the index lists with a later time than the version that the state holds, has its record fetched and written as a version. An active advisory that the index no longer lists gets a `removed` event, and a removed one that it lists again gets `returned`. Records are fetched one at a time, in id order, until `--max-seconds` seconds have passed since the run began. The advisories that are not reached are left for the next run.
4. A record that is missing (HTTP 404 or 410) writes nothing, not even a removal, because only the index says that an advisory is gone. A stale copy, which is older than the time that the index gives or no later than the version held, writes nothing either. The next run asks for both again.
5. It writes the generation of the index as the cursor, and applies the delta to the state.
6. It publishes. The state and the open delta are compressed and swapped into `osv-pypi-current` with a manifest that the publish gates have checked against the last one.

The manifest says that the data goes through the newest time in the index, to the second (`data_through`: the fraction is cut and never rounded, so the time is never ahead of the data), and gives the index's generation as the cursor. Its `coverage` counts the active advisories (`advisories`), the malware reports (`malware`) and the withdrawn advisories (`withdrawn`) among them, the removed advisories (`removed`), and the advisories that the run left to the next one (`remaining`): a stale copy, a missing record, a record that did not parse and one that was not fetched in time. The data is `complete` when none is left.

The first run, the bootstrap, builds the state from the index and from `all.zip`. It writes a version of each record of the dump that the index lists and that parses to the first open delta, and applies it, so the part of the first month holds the first version that the stream saw of every advisory. Then it compares the index with that state, as a later run does, which fetches what the dump lacked: a record that the dump does not hold or that did not parse, and the newer version of one whose copy in the dump is older than the index says. It adds the month's base copy and publishes. The dump is read in memory, within size bounds, and is never extracted to disk, because a zip from the internet is data and nothing in it is run.

## What fails a run

Three things fail a run, and each opens the stream's rolling issue (see "On GitHub"). The first two stop the run before it has published anything to `osv-pypi-current`. The third fails it after the publish.

**A stale index.** The index's generation is below the cursor that the state stores, so it is a stale copy. The run stops with `StaleIndexError` before it writes anything, and the next run reads the index again.

**A gate.** The kit's gates compare the manifest about to be published with the last one, and hold the publish when the cursor or `data_through` goes back, when a closed asset is not kept as it was, or when a table has lost rows. The stream adds a limit of its own: more than 100 `removed` events in one run. A removal stays in the month parts for good, and an index that was cut short would remove every advisory that it lost, so a run that finds more removals than that waits for a person. A held publish goes out only with a reason that a person gives (see "On GitHub"), and the manifest logs the reason and the failures that it overrode. A month that its base copy and its deltas do not rebuild stops the run as well, and no reason overrides that, because publishing would build on a month that its releases cannot restore. A month close that keeps failing needs a person, and [the registry's README](../pypi_registry/README.md) says what to do for each error, which applies here with this stream's names.

If OSV ever dropped its newest advisory from the index with nothing newer, `data_through` would go back and the gate would hold the publish until an override. Override it for this stream with `override_for = osv-pypi` and a reason. The manifest then logs the reason, and later runs compare with the lower time.

**A record that does not parse.** OSV gathers advisories from several databases, and its schema keeps changing: the schema versions of its records run from 1.7.3 to 1.9.2 today. One record that the stream cannot read yet should not hold back every other, so a record that does not parse is skipped and named, and the run publishes the rest. The registry fails a whole run instead. `main` then exits with 2, after the publish, so that the workflow's step fails and the stream's failure issue opens. The state holds nothing of that record, or only an older version, so every later run fetches it again and tries once more, and the first run that parses every record exits with 0 and closes the issue. While one is left, the manifest says `complete: false` and counts it in `coverage.remaining`. A record that keeps failing means that the parser (`records.py`) needs a change. The run log names the ids, as lines of `skipped <id>: <reason>`, and the report that the run prints lists them as `unparsed`. The reason is shown with every email address taken out, and is not shown at all when an `@`, a `%40` or an `at` in brackets is left in it, because the log of a public repository is public.

Every other error ends the run as well: a request to OSV that still fails after its retries, an index or a dump of a shape that the stream does not know, or a record that breaks a rule of the state.

## In this folder

- `stream.toml`: the stream's id, its upstreams with their licences and terms, and each table with its history class.
- `src/osv_pypi/index.py`: the addresses in the bucket, and the reader of `modified_id.csv`. It gives each advisory's id with the time it last changed, and the generation of the copy that it read. A body of a shape that it does not know raises a ValueError.
- `src/osv_pypi/records.py`: the version kept for each copy of a record, with the facts of `details` in place of its text. Email addresses are taken out of it as "What is kept" says, and the entries of `affected` that are not for PyPI, `credits` and the keys that it does not name are dropped. A record of a shape that it does not know raises a ValueError, which names the advisory, the path and the value.
- `src/osv_pypi/dump.py`: `all.zip`, read in memory and within bounds. It bounds the download and checks the whole list of members before it yields a record, then reads each member through a limit and adds up the bytes as they come. Whatever `zipfile` raises over a damaged archive is a ValueError.
- `src/osv_pypi/store.py`: the state database and the delta databases. A run writes each change as a numbered row of a delta, and `apply_delta` applies the rows to the state in order, so that the state at the end of any day can be rebuilt from the state at the start of the month and that month's deltas. It refuses a version that goes back, a version of a removed advisory, and a cursor that goes back.
- `src/osv_pypi/process.py`: what a run takes from the index, and what the bootstrap takes from the dump. It writes the removals and returns, fetches the records that the index calls for until the time budget is spent, and skips and names a record that does not parse.
- `src/osv_pypi/run.py`: the run, as "How a run goes" says. It reads the last published state, refuses a stale index before it writes anything, rolls over the day or the month, processes the index, applies the delta, checks the new manifest against the last one with the publish gates and the limit on removals, and publishes. `main` exits with 2 when a record did not parse.
- `src/osv_pypi/__main__.py`: `python -m osv_pypi`, which runs `main`.
- `tests/`: offline tests. The names of the test modules carry `osv`, because pytest imports a test folder that has no `__init__.py` by file name, and the registry's tests already use names such as `test_stream_toml.py`. `osv_run_helpers.py` holds what the tests of processing and of the run share: a clock and a transport, OSV's records, index and dump as a test builds them, and the runs of the stream on a fake GitHub and a fake OSV.
- `tests/fixtures/`: three records made by hand in the shape of a `GHSA-*`, a `PYSEC-*` and a `MAL-*` advisory, which the tests read.

## What is published

- `osv-pypi-current` holds the state (`osv-pypi-state.db.zst`), the open delta of the day of the last run (`osv-pypi-delta-open.db.zst`) and `manifest.json`, which lists every asset of the stream.
- `osv-pypi-month-YYYY-MM` holds the state as the month began (`osv-pypi-state-base-YYYY-MM.db.zst`) and the delta of each day that had a run (`osv-pypi-delta-YYYY-MM-DD.db.zst`). Once the month is over, it also holds the month's deltas merged into one database (`osv-pypi-YYYY-MM.db.zst`) and the month's manifest (`manifest-YYYY-MM.json`). Its assets are never replaced.

The state at the end of any day is the month's base copy with that month's deltas applied in day order, up to that day. Each run that closes a month checks that this gives back the state before it publishes.

## Running

A run publishes to `GITHUB_REPOSITORY` (`py-observatory/data` when it is not set), with the token in `GITHUB_TOKEN`:

```
uv run --package osv-pypi python -m osv_pypi run
```

`--bootstrap` builds the first state, and only a stream that has never published accepts it. `--override-reason TEXT` publishes although a publish gate failed, and the manifest logs the reason. It does not override a failed month close. `--max-seconds` is how long after its start a run stops fetching records (1,200 by default), and the advisories that it does not reach are fetched by the next run. `--workdir PATH` is the folder in which a run makes its own directory and removes it afterwards (`work` by default). SQLite's temporary files go in that folder too, unless `SQLITE_TMPDIR` is set.

The run prints its report as JSON: how many versions it wrote (`new` and `changed`), how many advisories it removed and returned, how many are left for the next run (`remaining`), the requests that it made, the ids of the records that did not parse (`unparsed`) and the gate failures that it overrode. It exits with 0, or with 2 when a record did not parse. Either comes after the publish.

### On GitHub

`tick.yml` runs the stream every six hours, in its job `osv-pypi` beside the registry's job `pypi-registry`. The stream has a concurrency group of its own, so a run of it never waits for the registry's tick or its sweep, and is never replaced by one. Its failures keep one rolling issue, `[osv-pypi] scheduled run failed`, which gets a comment for each run that fails again and is closed by the first run that succeeds.

The first run is a dispatch of `tick.yml` with `bootstrap` set to `osv-pypi`:

```
gh workflow run tick.yml -f bootstrap=osv-pypi
```

The registry's job runs in the same dispatch as an ordinary run, because it bootstraps only when `bootstrap` is `pypi-registry`. A scheduled run that comes before the bootstrap fails closed and opens the stream's issue. That is expected, and the first run that succeeds closes it. A bootstrap that has more to fetch than fits in its time budget publishes with `complete` false, and the next run goes on from there.

A first publish that fails part way cannot be repaired by bootstrapping again. It adds the month's base copy, `osv-pypi-state-base-YYYY-MM.db.zst`, to `osv-pypi-month-YYYY-MM` before it makes `osv-pypi-current`, and an asset of a month release is never replaced. A second bootstrap is refused at once when the first one made `osv-pypi-current`, even a half-made one. When it did not, the second builds a state of other bytes and stops with `CreateOnceError` on that base copy.

You can tell this case by the two releases. `osv-pypi-current` is missing or empty (it holds no manifest, though it may hold some `swap-next-` copies), and `osv-pypi-month-YYYY-MM` holds a base copy. A person must then remove what the failed bootstrap left before bootstrapping again: the half-made `osv-pypi-current`, if there is one, and that base copy. `RETENTION.md` allows such a deletion only on the owner's written decision, so record it there in a dated entry. An `osv-pypi-current` that holds a manifest, even as `swap-next-manifest.json`, is not this case, and the next scheduled run repairs it.

While the cause of a held publish stays, every run fails the same way. An override is a dispatch of `tick.yml` that names its stream in `override_for` and gives the reason in `override_reason`:

```
gh workflow run tick.yml -f override_for=osv-pypi -f override_reason='the reason'
```

A reason without `override_for` overrides nothing. One that names the registry is not the osv-pypi job's: each job takes the reason only when `override_for` is its own stream. So an override meant for one stream never publishes the other past its gates.

## Tests

The stream is a member of the repository's uv workspace, so sync every package once before running its tests:

```
uv sync --all-packages
uv run pytest streams/osv_pypi/tests
```
