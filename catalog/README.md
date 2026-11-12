# catalog

The streams of this repository read together: one SQLite database that holds every PyPI project, its releases, what each release changed, its requirements and classifiers, the registry's events, the advisories that affect it and its downloads, with a search index over names, summaries and keywords. It is built from the releases of [pypi-registry](../streams/pypi_registry/README.md), [osv-pypi](../streams/osv_pypi/README.md) and [pypi-downloads](../streams/pypi_downloads/README.md), and from nothing else: it makes no request except to the releases of this repository, and it adds no text of its own. The [repository README](../README.md) says how to read the files. The catalog appears when its first run, a bootstrap, has published (see "The first run").

## The releases

- `catalog-current` holds the latest build: `catalog.db.zst` and `manifest.json`. It is the only release that GitHub shows as Latest, and a new build replaces its assets through `swap-next-` copies, as the streams do.
- `catalog-month-YYYY-MM` holds the last build of that month, copied byte for byte when the first run of the next month begins: `catalog-YYYY-MM.db.zst` and `manifest-YYYY-MM.json`. Its assets are never replaced.
- `vYYYY-MM-DD` is the pin of a day. It holds one file, `release.json`, which gives the commit that wrote it and, for each stream, the create-once assets (each with its URL and sha256) that rebuild what the catalog reads of the stream as it stood at the end of that day. A pin is written once every stream has sealed the day, or 24 hours after the day ends, and then it marks each stream that had not. Pins start on the day of the catalog's first build, and a pin that is written is never rewritten.

Months and pins are prereleases, so they never show as Latest.

## The tables

Ids (`project_id`, `advisory_id`, `requirement_id`, `classifier_id`, `requires_python_id`, `wheel_tag_id`) are made afresh by each build, in name order for projects, in id order for advisories, in text order for requires-python and in tag order for wheel tags, so link rows by name, version, text or tag, and not by id across builds. The download tables use the `project_id` of `project`, found by the name that the downloads stream gives.

- `input`: one row for each stream that the build read, with the time of its manifest, its `data_through`, its fingerprint, `complete`, which is 1 when the stream's manifest says that its data is complete, and `carried`, which is 1 when the stream could not be read and its tables are the previous build's. A carried stream keeps the row of the build that read it, `complete` included. The manifest's coverage says `complete` of each stream in the same way.
- `attribution`: the upstreams of each stream and their licences and terms, as the stream's manifest gives them.
- `project`: each project name (PEP 503) in its current incarnation, the removed and quarantined included, with its status, when it was first seen and first uploaded, `swept`, which is 1 once the registry has read the project's own page in full, how many releases have a file now, its latest version, and the fields of the release that `record_version` names. A project that is not swept is known by its name alone, so it has no releases listed and its `releases` is empty, because 0 would read as a project with no release.
- `release`: each release of a project's current incarnation, with the time of its earliest upload, how many files it has, whether every one is yanked, the reason, whether its record is known, and its requires-python, which is what the earliest uploaded of its files that gives one says.
- `requires_python_text`: each requires-python that a release has, once, as its files write it.
- `latest_files`: for each project's latest version, how many files it has now that are not yanked, how many are wheels and how many source distributions, the size of its largest sdist, the range of its wheels' sizes, and how many files carry a provenance (an attestation). Older releases have no such summary.
- `wheel_tag` and `latest_wheel_tag`: the tags (PEP 425) of the latest version's wheels, each tag once, lowercased, and which project's latest version has each. A wheel whose name cannot be split into its tags is counted in `latest_files` but gives none.
- `change`: for each recorded release, the fields that it changed from the release recorded before it. The first record lists every field that is not empty, with `compared_with` empty, so it is never a change.
- `requirement_text` and `requirement`: the requirements of each project's `record_version`, with the text of each kept once, the project that each one names, and `optional`, which is 1 when its marker names an extra (`pytest; extra == 'test'`), so that a count of the projects that depend on another can leave out test and documentation extras.
- `classifier` and `project_classifier`: the trove classifiers of each project's `record_version`, kept once each, with a count of the projects that are neither removed nor quarantined.
- `licence`: those projects counted by licence expression.
- `growth`: new projects (by first upload) and new releases, by month of upload.
- `event`: every event of the registry, every incarnation included.
- `advisory`: every advisory of osv-pypi, with its kind (`malware` or `vulnerability`), its status and its times.
- `advisory_package`: what each advisory says of each project that it names: the ranges and the versions that OSV lists.
- `affected`: each known release that an active advisory affects, which is one that OSV lists, or that falls in an `ECOSYSTEM` range when its version is read as PEP 440. `GIT` ranges are kept in `advisory_package` but never evaluated, a version that PEP 440 refuses is affected only when OSV lists it, and a withdrawn or removed advisory is listed and affects nothing.
- `download_month`: each project's downloads in each month that has counted days, for every project with downloads. `month` is year × 100 + month. `downloads` includes the downloads by mirror clients, which `mirror_downloads` counts, so downloads without mirrors are the difference, here and in every download table.
- `download_period`: for each month, how many of its days are counted (`days`) and how many it has (`days_in_month`), with the downloads of all of PyPI on those days. A project's trend is its `download_month` rows read against these: a month with fewer days counted than it has holds only those days, and is not a fall.
- `download_last30`: each project's downloads over the 30 days that end with the newest day counted, with `rank` by downloads without mirrors, highest first and ties by name. It ranks only the projects that the registry knows, so a name that it does not know takes no place, while `download_top.rank` is the stream's rank at the close, over every name that it counted.
- `download_day`: the daily downloads of the projects of each year's set (see `download_top`; `day` counts days from 1970-01-01), for the year of the newest day counted and the year before, as `download_daily_years` in `catalog.toml` sets.
- `download_top`: for each year, the projects whose daily downloads are kept, with the month at whose close each entered the set (`entered`, year × 100 + month) and its rank then. Each month's close ranks the projects by their downloads without mirrors in that month, and those of the month's top 15,000 that are not in the year's set yet enter it, so the set grows through the year. December's close falls in the next year, so it starts that year's set.
- `download_break`: the days on which the counted series breaks, with a note on why, so that a trend is not read across one as if nothing had changed.
- `download_coverage`: one row that says how far the counted history reaches (see "What is not there yet"), the days of the window of `download_last30` and how many of them are counted (`window_days`), and the names with downloads that the registry does not know (`unmatched_projects`) with their downloads in the window (`unmatched_downloads`). Those names are left out of the other download tables.
- `search`: a full-text index (FTS5) over name, summary and keywords.
- `tombstone`: a view of the projects that are removed or quarantined.

A project's latest version is the highest PEP 440 version among its releases that have a file now and are not wholly yanked, and a final release beats every pre-release and dev release. A project's fields and requirements come from the record of that release, or from the newest record when it has none.

## What is not there yet

- All of the download history. The downloads stream counts forward from its launch and back towards 2016-01-22, the first day of PyPI's table, and its manifest says `complete: false` until the backfill gets there with no day missing, so its `input` row says `complete = 0`. That leaves the catalog's own manifest complete. `download_coverage` says how far the history reaches: the launch day, `first_day`, from which every day to launch is counted, `last_day`, the newest day counted, `days`, how many are counted, and `backfill_left`, how many days before `first_day` are left to count.
- Whole months and a whole window. A month at either end of the history, or one with a day not counted yet, holds fewer days than it has, and so can the window of `download_last30`: `download_period.days` against `days_in_month`, and `window_days` against 30, say how many days each holds.
- Daily downloads for every project. Every project has monthly totals, and only the projects of each year's set have daily rows (see `download_top`): those that entered at the year's first close have every counted day of the year, and the others the days from the close at which they entered, so `download_day` is never the whole of PyPI. The first set comes from November 2026, at the close on 2026-12-01, and until then `download_top` and `download_day` are empty. Years before the stream's launch have no set, so they have monthly totals only.
- All of PyPI, while a stream's data is not complete. The registry's manifest says `complete: false` until its sweep has read the page of every project, and the stream's `input` row then says `complete = 0`. Until then, what is counted over the whole of PyPI (`growth`, `licence` and the counts of `classifier`) covers only what the streams have read, and each project that the registry has not read has `swept = 0`.
- Releases whose files carry no version that can be read (old eggs, `.exe` installers, legacy sdist names), which are about 0.15% of releases. Such a release has `files` and `uploaded` empty. It does not count in `releases` or `growth`, and it is never a project's latest version.
- Releases deleted before the registry began. `growth` and `first_uploaded` leave them out until the registry's history fill.
- The record of every release, which holds its summary, requirements, classifiers and the rest. The registry has the record of each release uploaded since 2026-11-01, and its records pass fetches, for each project, the record of the release that is current when the pass reaches it. Until the pass reaches it, a project whose releases all lack a record has empty fields. The change history of releases before 2026-11-01 is not fetched: a release that was current when the pass reached its project has `has_record = 1`, and keeps it after a newer release, and the others have `has_record = 0` and no rows in `change`. A current release stays at 0 when PyPI does not have its record, or when the pass refused the record because it could not read it.

## When a stream cannot be read, and when it is stale

A stream that cannot be read, because its release is missing or its files do not check out, keeps its tables from the previous build. Its `input` row says `carried = 1`, the manifest names it, and the run exits with 2 after it has published. The next run that can read it builds again. The first build needs every stream, so a bootstrap stops instead.

A stream is stale when its `data_through` has not changed for longer than it should: 30 hours for the registry, 72 for osv-pypi and 54 for pypi-downloads. The catalog records when it first saw each value, and the manifest says which streams are stale. A carried or stale stream makes the manifest `complete: false`, and the run exits with 2, so that the catalog's issue opens (see "On GitHub"). A run that builds nothing exits with 2 too while the last build's manifest says `complete: false`, so that the issue stays open until a build finds every stream well. The `data_through` of the catalog is the oldest `generated_at` among the inputs it used, and a build from older inputs than the last build's waits for a person's reason. So does a build with fewer rows in `project` or `event` than the last one, because the registry never loses a project or an event, and fewer means that a stream went back.

A run builds on a bootstrap, on the first run of a new month, when the assets that it reads have changed and 50 minutes have passed since the last build, and anyway once the last build is 24 hours old. Every other run builds nothing, and still checks freshness and writes the pins that are due. A build whose compressed database is over 200,000,000 bytes warns, says `over_budget` in the manifest's coverage and publishes.

## Attributions

The catalog copies the upstreams of each stream into its `attribution` table and the licences of its manifest, so they travel with the data. The registry reads PyPI, whose terms of use are linked from its `stream.toml`. osv-pypi reads OSV.dev, whose terms are linked from its `stream.toml`, and OSV gathers advisories from the GitHub Advisory Database and the PyPA Advisory Database (both CC-BY-4.0) and the OpenSSF malicious-packages repository (Apache-2.0). Reusing the advisories means keeping those credits. pypi-downloads counts PyPI's public BigQuery table of downloads, whose terms are linked from its `stream.toml`.

## In this folder

- `catalog.toml`: what the catalog reads of each stream, how long before a stream is stale, and the pace of builds and pins.
- `src/catalog/schema.py`: the serving database's schema.
- `src/catalog/config.py`, `inputs.py`: the configuration, and the reading of each stream's manifest.
- `src/catalog/stage.py`, `registry.py`, `advisories.py`, `records.py`, `downloads.py`, `serving.py`: staging each stream's parts, and building the tables.
- `src/catalog/pins.py`: the daily pin and the month's copy.
- `src/catalog/run.py`: the run, and `python -m catalog` (`__main__.py`).
- `tests/`: offline tests, whose module names start with `test_catalog_`.

## Running

A run publishes to `GITHUB_REPOSITORY` (`py-observatory/data` when it is not set), with the token in `GITHUB_TOKEN`:

```
uv run --package catalog python -m catalog run
```

`--bootstrap` makes the first build, and only a catalog that has never published accepts it. `--override-reason TEXT` publishes although a publish gate failed, and the manifest logs the reason. `--workdir PATH` is the folder in which a run makes its own directory and removes it afterwards (`work` by default). SQLite's temporary files go in that folder too, unless `SQLITE_TMPDIR` is set. The run prints its report as JSON, and exits with 0, or with 2 when a stream was carried, is stale or could not be read, or when it built nothing and the last build's manifest says `complete: false`. Either comes after the publish.

### On GitHub

`tick.yml` has a job `catalog`. It waits for osv-pypi, which is short, and not for the registry's job, which can queue for hours behind the sweep, nor for the downloads job, so it reads their last publishes. It runs after an osv-pypi that failed too, and has a concurrency group of its own. Its failures keep one rolling issue, `[catalog] scheduled run failed`, which gets a comment for each run that fails again and is closed by the first run that succeeds. After the build, whatever its outcome, `scripts/keepalive.sh` makes an empty commit on main when main's newest commit is 45 days old, because GitHub disables the scheduled workflows of a public repository after 60 days with no commit and the data lives in releases. It runs before the issue is opened or closed, so a keepalive that fails opens the issue too.

### The first run

Run it straight after every stream has published, with `bootstrap` set to `catalog`:

```
gh workflow run tick.yml -f bootstrap=catalog
```

The stream jobs run in the same dispatch as ordinary runs, because each bootstraps only when `bootstrap` names it. A scheduled run that comes before the bootstrap fails closed and opens the catalog's issue, which the first successful run closes. A first build needs every stream.

An override is a dispatch that names the catalog in `override_for` and gives the reason in `override_reason`:

```
gh workflow run tick.yml -f override_for=catalog -f override_reason='the reason'
```

## Tests

```
uv sync --all-packages
uv run pytest catalog/tests
```
