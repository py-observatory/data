"""zstd file helpers. Every published database is a `.db.zst`."""

import hashlib
import shutil
from compression import zstd
from compression.zstd import CompressionParameter
from pathlib import Path

_CHUNK_SIZE = 1024 * 1024

# A 2**27 byte window is the largest that plain `zstd -d` reads without `--long`, so
# every file written here decompresses without flags. The checksum makes a damaged
# file fail loudly.
_COMPRESS_OPTIONS = {
    CompressionParameter.compression_level: 19,
    CompressionParameter.window_log: 27,
    CompressionParameter.enable_long_distance_matching: 1,
    CompressionParameter.checksum_flag: 1,
}


def compress_file(src: Path, dst: Path) -> None:
    """Compress `src` into `dst`, streaming 1 MiB at a time."""
    with (
        open(src, "rb") as fin,
        zstd.open(dst, "wb", options=_COMPRESS_OPTIONS) as fout,
    ):
        shutil.copyfileobj(fin, fout, _CHUNK_SIZE)


def decompress_file(src: Path, dst: Path) -> None:
    """Expand the zstd file `src` into `dst`, streaming 1 MiB at a time."""
    with zstd.open(src, "rb") as fin, open(dst, "wb") as fout:
        shutil.copyfileobj(fin, fout, _CHUNK_SIZE)


def sha256_file(path: Path) -> str:
    """The SHA-256 of the file's bytes, as lowercase hex."""
    with open(path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()
