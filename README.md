# py-observatory data

This repository publishes public data about every PyPI project, as SQLite databases compressed with zstd (`.db.zst`). Each data stream keeps its latest files in a `<stream>-current` release, with dated releases named `<stream>-month-YYYY-MM` and `<stream>-year-YYYY` alongside it; all of them are on the [releases page](https://github.com/py-observatory/data/releases). Plain `zstd -d` decompresses every `.db.zst` file. The licence is in [`LICENSE`](LICENSE).
