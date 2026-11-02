# catalog

The streams of this repository read together: one SQLite database that holds every PyPI project, its releases, what each release changed, its requirements and classifiers, the registry's events and the advisories that affect it, with a search index over names, summaries and keywords. It is built from the releases of [pypi-registry](../streams/pypi_registry/README.md) and [osv-pypi](../streams/osv_pypi/README.md), and from nothing else: it makes no request except to the releases of this repository, and it adds no text of its own. The [repository README](../README.md) says how to read the files. The catalog appears when its first run, a bootstrap, has published (see "The first run").

## The releases

- `catalog-current` holds the latest build: `catalog.db.zst` and `manifest.json`. It is the only release that GitHub shows as Latest, and a new build replaces its assets through `swap-next-` copies, as the streams do.
- `catalog-month-YYYY-MM` holds the last build of that month, copied byte for byte when the first run of the next month begins: `catalog-YYYY-MM.db.zst` and `manifest-YYYY-MM.json`. Its assets are never replaced.
- `vYYYY-MM-DD` is the pin of a day. It holds one file, `release.json`, which gives the commit that wrote it and, for each stream, the create-once assets (each with its URL and sha256) that rebuild what the catalog reads of the stream as it stood at the end of that day. A pin is written once both streams have sealed the day, or 24 hours after the day ends, and then it marks each stream that had not. Pins start on the day of the catalog's first build, and a pin that is written is never rewritten.

Months and pins are prereleases, so they never show as Latest.

## The tables

Ids (`project_id`, `advisory_id`, `requirement_id`, `classifier_id`) are made afresh by each build, in name order for projects and in id order for advisories, so link rows by name, and by version, and not by id across builds.

- `input`: one row for each stream that the build read, with the time of its manifest, its `data_through`, its fingerprint, and `carried`, which is 1 when the stream could not be read and its tables are the previous build's.
- `attribution`: the upstreams of each stream and their licences and terms, as the stream's manifest gives them.
- `project`: each project name (PEP 503) in its current incarnation, the removed and quarantined included, with its status, when it was first seen and first uploaded, how many releases have a file now, its latest version, and the fields of the release that `record_version` names.
- `release`: each release of a project's current incarnation, with the time of its earliest upload, how many files it has, whether every one is yanked, the reason, and whether its record is known.
- `change`: for each recorded release, the fields that it changed from the release recorded before it. The first record lists every field that is not empty, with `compared_with` empty, so it is never a change.
- `requirement_text` and `requirement`: the requirements of each project's `record_version`, with the text of each kept once, and the project that each one names.
- `classifier` and `project_classifier`: the trove classifiers of each project's `record_version`, kept once each, with a count of the projects that are neither removed nor quarantined.
- `licence`: those projects counted by licence expression.
- `growth`: new projects (by first upload) and new releases, by month of upload.
- `event`: every event of the registry, every incarnation included.
- `advisory`: every advisory of osv-pypi, with its kind (`malware` or `vulnerability`), its status and its times.
- `advisory_package`: what each advisory says of each project that it names: the ranges and the versions that OSV lists.
- `affected`: each known release that an active advisory affects, which is one that OSV lists, or that falls in an `ECOSYSTEM` range when its version is read as PEP 440. `GIT` ranges are kept in `advisory_package` but never evaluated, a version that PEP 440 refuses is affected only when OSV lists it, and a withdrawn or removed advisory is listed and affects nothing.
- `search`: a full-text index (FTS5) over name, summary and keywords.
- `tombstone`: a view of the projects that are removed or quarantined.

A project's latest version is the highest PEP 440 version among its releases that have a file now and are not wholly yanked, and a final release beats every pre-release and dev release. A project's fields and requirements come from the record of that release, or from the newest record when it has none.

## What is not there yet

- Download numbers. They come from a stream that is not published yet, and the catalog adds their tables when it is.
- The fields of a release's record (the summary, requirements, classifiers and the rest) exist only for releases uploaded since 2026-11-01. The registry's history fill will add the earlier ones, and the catalog picks them up as it lands. Until then `has_record` is 0 for them, a project whose releases all lack a record has empty fields, and `change` has no rows for them.

## When a stream cannot be read, and when it is stale

A stream that cannot be read, because its release is missing or its files do not check out, keeps its tables from the previous build. Its `input` row says `carried = 1`, the manifest names it, and the run exits with 2 after it has published. The next run that can read it builds again. The first build needs every stream, so a bootstrap stops instead.

A stream is stale when its `data_through` has not changed for longer than it should: 30 hours for the registry and 72 for osv-pypi. The catalog records when it first saw each value, and the manifest says which streams are stale. A carried or stale stream makes the manifest `complete: false`, and the run exits with 2, so that the catalog's issue opens (see "On GitHub"). The `data_through` of the catalog is the oldest `generated_at` among the inputs it used, and a build from older inputs than the last build's waits for a person's reason. So does a build with fewer rows in `project` or `event` than the last one, because the registry never loses a project or an event, and fewer means that a stream went back.

A run builds on a bootstrap, on the first run of a new month, when the assets that it reads have changed and 50 minutes have passed since the last build, and anyway once the last build is 24 hours old. Every other run builds nothing, and still checks freshness and writes the pins that are due. A build whose compressed database is over 200,000,000 bytes warns, says `over_budget` in the manifest's coverage and publishes.

## Attributions

The catalog copies the upstreams of each stream into its `attribution` table and the licences of its manifest, so they travel with the data. The registry reads PyPI, whose terms of use are linked from its `stream.toml`. osv-pypi reads OSV.dev, whose terms are linked from its `stream.toml`, and OSV gathers advisories from the GitHub Advisory Database and the PyPA Advisory Database (both CC-BY-4.0) and the OpenSSF malicious-packages repository (Apache-2.0). Reusing the advisories means keeping those credits.

## In this folder

- `catalog.toml`: what the catalog reads of each stream, how long before a stream is stale, and the pace of builds and pins.
- `src/catalog/schema.py`: the serving database's schema.
- `src/catalog/config.py`, `inputs.py`: the configuration, and the reading of each stream's manifest.
- `src/catalog/stage.py`, `registry.py`, `advisories.py`, `records.py`, `serving.py`: staging each stream's parts, and building the tables.
- `src/catalog/pins.py`: the daily pin and the month's copy.
- `src/catalog/run.py`: the run, and `python -m catalog` (`__main__.py`).
- `tests/`: offline tests, whose module names start with `test_catalog_`.

## Running

A run publishes to `GITHUB_REPOSITORY` (`py-observatory/data` when it is not set), with the token in `GITHUB_TOKEN`:

```
uv run --package catalog python -m catalog run
```

`--bootstrap` makes the first build, and only a catalog that has never published accepts it. `--override-reason TEXT` publishes although a publish gate failed, and the manifest logs the reason. `--workdir PATH` is the folder in which a run makes its own directory and removes it afterwards (`work` by default). SQLite's temporary files go in that folder too, unless `SQLITE_TMPDIR` is set. The run prints its report as JSON, and exits with 0, or with 2 when a stream was carried, is stale or could not be read. Either comes after the publish.

### On GitHub

`tick.yml` has a job `catalog`. It waits for osv-pypi, which is short, and not for the registry's job, which can queue for hours behind the sweep, so it reads the registry's last publish. It runs after an osv-pypi that failed too, and has a concurrency group of its own. Its failures keep one rolling issue, `[catalog] scheduled run failed`, which gets a comment for each run that fails again and is closed by the first run that succeeds. After the build, whatever its outcome, `scripts/keepalive.sh` makes an empty commit on main when main's newest commit is 45 days old, because GitHub disables the scheduled workflows of a public repository after 60 days with no commit and the data lives in releases. It runs before the issue is opened or closed, so a keepalive that fails opens the issue too.

### The first run

Run it straight after both streams have published, with `bootstrap` set to `catalog`:

```
gh workflow run tick.yml -f bootstrap=catalog
```

The two stream jobs run in the same dispatch as ordinary runs, because each bootstraps only when `bootstrap` names it. A scheduled run that comes before the bootstrap fails closed and opens the catalog's issue, which the first successful run closes. A first build needs every stream.

An override is a dispatch that names the catalog in `override_for` and gives the reason in `override_reason`:

```
gh workflow run tick.yml -f override_for=catalog -f override_reason='the reason'
```

## Tests

```
uv sync --all-packages
uv run pytest catalog/tests
```
