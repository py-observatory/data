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
- There is no README text. A release keeps a fingerprint of its README, its size and its word count.
- There are no email addresses. The names of people are kept.

## In this folder

- `stream.toml`: the stream's id, its upstream and terms of use, and each table with its history class.
- `src/pypi_registry/names.py`: canonical names and versions, and the version that a filename carries.
- `src/pypi_registry/simple.py`: the Simple API's URLs and the parsers for its root and project pages. A reply of a shape that the parsers do not know raises a ValueError.
- `tests/`: offline tests, and the fixtures they read.

## Tests

The stream is a member of the repository's uv workspace, so sync every package once before running its tests:

```
uv sync --all-packages
uv run pytest streams/pypi_registry/tests
```
