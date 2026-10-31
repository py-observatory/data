import shutil
import subprocess
from pathlib import Path

import pytest

from obskit.zst import compress_file, decompress_file, sha256_file

MIB = 1024 * 1024


def one_mib_of_text() -> bytes:
    line = b"the quick brown fox jumps over the lazy dog\n"
    return (line * (MIB // len(line) + 1))[:MIB]


def compressed(tmp_path: Path, data: bytes) -> Path:
    src = tmp_path / "data.txt"
    src.write_bytes(data)
    dst = tmp_path / "data.txt.zst"
    compress_file(src, dst)
    return dst


def test_round_trip(tmp_path):
    data = one_mib_of_text()
    dst = compressed(tmp_path, data)
    restored = tmp_path / "restored.txt"

    decompress_file(dst, restored)

    assert dst.stat().st_size < len(data)
    assert restored.read_bytes() == data


@pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd CLI not installed")
def test_cli_reads_without_flags(tmp_path):
    data = one_mib_of_text()
    dst = compressed(tmp_path, data)

    result = subprocess.run(["zstd", "-d", "-c", dst], capture_output=True)

    assert result.returncode == 0, result.stderr
    assert result.stdout == data


def test_sha256_file(tmp_path):
    path = tmp_path / "abc"
    path.write_bytes(b"abc")

    assert (
        sha256_file(path)
        == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_frame_header_pins_window_and_checksum(tmp_path):
    header = compressed(tmp_path, one_mib_of_text()).read_bytes()[:6]

    # RFC 8878, section 3.1.1. A window above 2**27 is what makes plain `zstd -d`
    # refuse a file, and this check holds on machines that have no zstd CLI.
    assert header[:4] == bytes.fromhex("28b52ffd"), "magic number"
    assert header[4] == 0x04, "content checksum on, no dictionary, no declared size"
    assert header[5] == 0x88, "window exponent 17, so a 2**27 byte window"
