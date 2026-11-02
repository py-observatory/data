# osv-pypi

Every advisory in OSV.dev that affects a PyPI project: vulnerabilities, with ids such as `GHSA-*` and `PYSEC-*`, and malware reports, with `MAL-*`. Like the registry, it is published as SQLite files compressed with zstd, in the `osv-pypi-current` release and the dated releases beside it. The [repository README](../../README.md) says how to read them.

This README is a skeleton, and it grows with the stream.

## Where the data comes from

OSV.dev's public bucket, `https://osv-vulnerabilities.storage.googleapis.com/PyPI/`, which is read one request at a time with a User-Agent that names this repository and holds no email address.

- `modified_id.csv` lists every advisory with the time it last changed, newest first, as lines of `<time>,<id>`. The `x-goog-generation` header of the reply says which copy of the file was served, and that generation only grows.
- `all.zip` holds the record of every advisory.
- `<id>.json` is the record of one advisory.

OSV.dev's terms are linked from `stream.toml`, which also lists the databases that its advisories come from, with their licences.

## In this folder

- `stream.toml`: the stream's id, its upstreams with their licences and terms, and each table with its history class.
- `src/osv_pypi/index.py`: the addresses in the bucket, and the reader of `modified_id.csv`. It gives each advisory's id with the time it last changed, and the generation of the copy that it read. A body of a shape that it does not know raises a ValueError.
- `tests/`: offline tests. The names of the test modules carry `osv`, because pytest imports a test folder that has no `__init__.py` by file name, and the registry's tests already use names such as `test_stream_toml.py`.

## Tests

The stream is a member of the repository's uv workspace, so sync every package once before running its tests:

```
uv sync --all-packages
uv run pytest streams/osv_pypi/tests
```
