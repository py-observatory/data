# py-observatory data

This repository publishes public data about every PyPI project, as SQLite databases compressed with zstd (`.db.zst`). Each data stream keeps its latest files in a `<stream>-current` release, with dated releases named `<stream>-month-YYYY-MM` and `<stream>-year-YYYY` alongside it; all of them are on the [releases page](https://github.com/py-observatory/data/releases). Plain `zstd -d` decompresses every `.db.zst` file. The licence is in [`LICENSE`](LICENSE).

## Streams

- [pypi-registry](streams/pypi_registry/README.md): the registry of PyPI: every project, its releases and files, which files are yanked and why, and the status of each project.
- [osv-pypi](streams/osv_pypi/README.md): every advisory in OSV.dev that affects a PyPI project, vulnerabilities and malware reports.
- [pypi-downloads](streams/pypi_downloads/README.md): how many times each PyPI project was downloaded on each UTC day, counted from PyPI's public BigQuery table.
- [catalog](catalog/README.md): the three streams above read together into one database, with each project's releases, requirements, classifiers, events, advisories and downloads, and a search index.
