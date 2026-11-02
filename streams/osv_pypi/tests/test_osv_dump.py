import io
import json
import re
import struct
import tracemalloc
import warnings
import zipfile
import zlib
from pathlib import Path

import pytest

from obskit.emails import holds_an_at
from obskit.http import Http, UpstreamError
from obskit.testing import FakeHttp
from osv_pypi import dump
from osv_pypi.dump import (
    MAX_DUMP_BYTES,
    MAX_MEMBER_BYTES,
    MAX_MEMBERS,
    MAX_TOTAL_BYTES,
    get_dump,
    read_dump,
)
from osv_pypi.index import DUMP_URL

FIXTURES = Path(__file__).parent / "fixtures"
MIB = 1024 * 1024
ID = "GHSA-aaaa-bbbb-cccc"
NAME = ID + ".json"
RECORD = b'{"id": "GHSA-aaaa-bbbb-cccc"}'


# ---- archives, whole and damaged


def archive(
    members: list[tuple[str, bytes]], method: int = zipfile.ZIP_DEFLATED
) -> bytes:
    """A zip of `members`, each a name and its bytes, made in memory. OSV's real archive
    is deflated."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", method) as zf:
        for name, data in members:
            zf.writestr(name, data)
    return buffer.getvalue()


def numbered(count: int, data: bytes = RECORD) -> bytes:
    """An archive of `count` members, each `data`, named GHSA-aaaa-bbbb-ccc0.json and so
    on up to ccc9."""
    return archive([(f"GHSA-aaaa-bbbb-ccc{n}.json", data) for n in range(count)])


# Where each field that these tests damage sits, as an offset and a struct format: in a
# member's local header, and in its entry of the central directory.
_LOCAL_HEADER = {
    "flags": (6, "<H"),
    "method": (8, "<H"),
    "crc": (14, "<I"),
    "compress_size": (18, "<I"),
    "file_size": (22, "<I"),
}
_DIRECTORY_ENTRY = {
    "version": (6, "<H"),
    "flags": (8, "<H"),
    "method": (10, "<H"),
    "crc": (16, "<I"),
    "compress_size": (20, "<I"),
    "file_size": (24, "<I"),
}
_HEADER_SIZE = 30  # the fixed part of a member's local header
_ENTRY_SIZE = 46  # the fixed part of an entry of the central directory
_END_SIZE = 22  # the end record, which has no comment in an archive that zipfile wrote


def directory_entries(body: bytes) -> list[int]:
    """Where each member's entry starts in the central directory of `body`, which is an
    archive that zipfile wrote."""
    count, _, start = struct.unpack_from("<HII", body, len(body) - _END_SIZE + 10)
    starts = []
    for _ in range(count):
        starts.append(start)
        name, extra, comment = struct.unpack_from("<HHH", body, start + 28)
        start += _ENTRY_SIZE + name + extra + comment
    return starts


def patched(body: bytes, member: int = 0, **fields: int) -> bytes:
    """`body` with fields of one member changed to `fields`, in the member's local
    header and in its entry of the central directory. This is how a damaged archive
    differs from a good one, and how a hostile one lies."""
    damaged = bytearray(body)
    entry = directory_entries(body)[member]
    (local,) = struct.unpack_from("<I", body, entry + 42)
    for field, value in fields.items():
        assert field in _LOCAL_HEADER or field in _DIRECTORY_ENTRY, field
        if field in _DIRECTORY_ENTRY:
            offset, form = _DIRECTORY_ENTRY[field]
            struct.pack_into(form, damaged, entry + offset, value)
        if field in _LOCAL_HEADER:
            offset, form = _LOCAL_HEADER[field]
            struct.pack_into(form, damaged, local + offset, value)
    return bytes(damaged)


def repeated() -> bytes:
    """An archive that lists NAME twice. zipfile warns when it writes a name again, and
    a warning is no part of what these tests check."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return archive([(NAME, RECORD), (NAME, RECORD)])


def with_nul_in_the_name() -> bytes:
    """An archive whose member is named `GHSA-aaaa-bbbb-cccc.json`, a NUL and `.txt`.
    zipfile cuts a name at its first NUL, so ZipInfo.filename reads as NAME and only
    orig_filename shows the rest. zipfile cannot write such a name, so the archive is
    written with an X and patched."""
    return archive([(NAME + "X.txt", RECORD)]).replace(b"X.txt", b"\x00.txt")


def with_a_name_that_is_not_utf8() -> bytes:
    """An archive that sets the flag for UTF-8 names, and whose name starts with the
    byte 0xFF, which UTF-8 cannot hold."""
    body = archive([(NAME, b"{}")]).replace(b"GHSA", b"\xffHSA")
    return patched(body, flags=0x800)


def with_another_name_in_the_header(body: bytes, name: bytes) -> bytes:
    """`body`, an archive of one member, with `name` in the member's local header in
    place of the name that the central directory gives it. The directory comes after
    the member, so it moves by as many bytes as the name grows, and the end record says
    where it starts."""
    (written,) = struct.unpack_from("<H", body, 26)  # the length of the header's name
    header = bytearray(body[:_HEADER_SIZE])
    struct.pack_into("<H", header, 26, len(name))
    damaged = bytearray(header + name + body[_HEADER_SIZE + written :])
    start_at = len(damaged) - _END_SIZE + 16
    (start,) = struct.unpack_from("<I", damaged, start_at)
    struct.pack_into("<I", damaged, start_at, start + len(name) - written)
    return bytes(damaged)


def with_a_directory_past_the_end(body: bytes) -> bytes:
    """`body` with an end record that puts the central directory far past the end."""
    damaged = bytearray(body)
    struct.pack_into("<I", damaged, len(body) - _END_SIZE + 16, 0xFFFFFF00)
    return bytes(damaged)


def with_a_zip64_offset_too_large(body: bytes) -> bytes:
    """`body`, an archive of one member, with the member's header offset in a ZIP64
    extra field of its directory entry, and 2**63, more than any file can be seeked
    to."""
    (start,) = directory_entries(body)
    (name_size,) = struct.unpack_from("<H", body, start + 28)
    extra = struct.pack("<HHQ", 1, 8, 2**63)  # field 1 is ZIP64, with an 8-byte value
    at = start + _ENTRY_SIZE + name_size
    damaged = bytearray(body[:at] + extra + body[at:])
    struct.pack_into("<H", damaged, start + 30, len(extra))  # the extra field's size
    # An offset of 0xFFFFFFFF says that the real one is in the extra field.
    struct.pack_into("<I", damaged, start + 42, 0xFFFFFFFF)
    # The directory has grown by the extra field, and its size is in the end record.
    size_at = len(damaged) - _END_SIZE + 12
    (size,) = struct.unpack_from("<I", damaged, size_at)
    struct.pack_into("<I", damaged, size_at, size + len(extra))
    return bytes(damaged)


GOOD = archive([(NAME, RECORD)])
STORED = archive([(NAME, RECORD)], zipfile.ZIP_STORED)


# ---- the bounds


def test_the_bounds():
    assert MAX_DUMP_BYTES == 256 * MIB
    assert MAX_MEMBERS == 200_000
    assert MAX_MEMBER_BYTES == 4 * MIB
    assert MAX_TOTAL_BYTES == 1024 * MIB


# ---- reading the archive


def fixture(name: str) -> tuple[str, bytes]:
    """The member that a hand-made record of fixtures/ makes: its id and `.json`, and
    its bytes."""
    data = (FIXTURES / f"{name}.json").read_bytes()
    return json.loads(data)["id"] + ".json", data


def test_read_dump_yields_members():
    # They are not in the order of their ids, and come out in the order of the archive.
    members = [fixture("pysec"), fixture("ghsa"), fixture("mal")]

    records = list(read_dump(archive(members)))

    assert records == [
        ("PYSEC-2019-311", members[0][1]),
        ("GHSA-4m7q-9xv2-hc3p", members[1][1]),
        ("MAL-2026-40017", members[2][1]),
    ]


def test_read_dump_reads_stored_members():
    members = [fixture("ghsa"), fixture("mal")]

    records = list(read_dump(archive(members, zipfile.ZIP_STORED)))

    assert records == [
        ("GHSA-4m7q-9xv2-hc3p", members[0][1]),
        ("MAL-2026-40017", members[1][1]),
    ]


def test_read_dump_checks_every_member_before_it_yields_one():
    # The last name is bad. Nothing has been yielded by the time that it is refused.
    body = archive(
        [(NAME, RECORD), ("GHSA-aaaa-bbbb-ccce.json", RECORD), ("x.txt", b"")]
    )
    yielded = []

    with pytest.raises(ValueError, match="'x.txt'"):
        for member in read_dump(body):
            yielded.append(member)

    assert yielded == []


def test_read_dump_checks_sizes_as_it_reads(monkeypatch):
    # A member is read when it is asked for, so the first comes out before the second is
    # found to be over the limit.
    monkeypatch.setattr(dump, "MAX_MEMBER_BYTES", 16)
    records = read_dump(
        archive([(NAME, b"x" * 16), ("GHSA-aaaa-bbbb-ccce.json", b"x" * 17)])
    )

    assert next(records) == (ID, b"x" * 16)
    with pytest.raises(ValueError, match="over the limit"):
        next(records)


def test_read_dump_writes_nothing_to_disk(tmp_path, monkeypatch):
    # An extraction goes to the working directory when it is given no other place.
    monkeypatch.chdir(tmp_path)

    assert list(read_dump(GOOD)) == [(ID, RECORD)]

    assert list(tmp_path.iterdir()) == []


def test_read_dump_reads_a_member_of_exactly_the_limit():
    body = archive([(NAME, bytes(MAX_MEMBER_BYTES))])

    ((_, record),) = read_dump(body)

    assert len(record) == MAX_MEMBER_BYTES


def test_read_dump_reads_as_many_members_and_bytes_as_the_limits_allow(monkeypatch):
    monkeypatch.setattr(dump, "MAX_MEMBERS", 3)
    monkeypatch.setattr(dump, "MAX_TOTAL_BYTES", 3 * len(RECORD))

    assert len(list(read_dump(numbered(3)))) == 3


def test_read_dump_stops_a_bomb_at_the_limit():
    # A member of 128 MiB that deflates to about 130 KB. Reading it whole would hold
    # 128 MiB, so the peak shows that the read stopped at the limit and not later.
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        with zf.open(NAME, "w") as member:
            for _ in range(128):
                member.write(bytes(MIB))
    bomb = buffer.getvalue()
    assert len(bomb) < MIB

    tracemalloc.start()
    try:
        with pytest.raises(ValueError, match="over the limit"):
            list(read_dump(bomb))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < 32 * MIB


# ---- archives that are refused

# Names that are not `<id>.json` for an advisory id, each with what is wrong with it.
BAD_NAMES = [
    ("GHSA-aaaa-bbbb-cccc", "no .json"),
    ("GHSA-aaaa-bbbb-cccc.JSON", "the suffix in capitals"),
    ("GHSA-aaaa-bbbb-cccc.txt", "another suffix"),
    ("GHSA-aaaa-bbbb-cccc.json.json", "two suffixes"),
    ("GHSA-aaaa-bbbb-cccc.json ", "a blank at the end"),
    ("GHSA-aaaa-bbbb-cccc.json\n", "a line feed at the end"),
    (" GHSA-aaaa-bbbb-cccc.json", "a blank at the start"),
    ("modified_id.csv", "OSV's index"),
    (".json", "no id"),
    ("GHSA-.json", "an id that ends at its hyphen"),
    ("ghsa-aaaa-bbbb-cccc.json", "an id in lower case"),
    ("GHSA-aaaa-bbbb-é.json", "a letter outside ASCII"),
    ("A-" + "b" * 63 + ".json", "an id of 65 characters"),
    ("records/GHSA-aaaa-bbbb-cccc.json", "a path"),
    ("/GHSA-aaaa-bbbb-cccc.json", "an absolute path"),
    ("../GHSA-aaaa-bbbb-cccc.json", "a path that climbs"),
    ("records\\GHSA-aaaa-bbbb-cccc.json", "a path with a backslash"),
    ("records/", "a directory"),
    ("GHSA-aaaa-bbbb-cccc.json/", "a directory named like a record"),
    ("A-" + "b" * 60_000 + ".json", "an id of 60,000 characters"),
]

# Each case: the archive, the limits that the test lowers (the names in `dump` and
# their new values), and what the ValueError says.
HOSTILE = [
    # ---- not an archive
    pytest.param(
        b"this is no zip file",
        {},
        "cannot be read as a zip",
        id="bytes that are no zip",
    ),
    pytest.param(b"", {}, "cannot be read as a zip", id="no bytes"),
    pytest.param(
        GOOD[: len(GOOD) // 2], {}, "cannot be read as a zip", id="a truncated archive"
    ),
    pytest.param(
        patched(GOOD, version=64),
        {},
        "cannot be read as a zip.*version 6.4",
        id="a zip made for a later version",
    ),
    pytest.param(
        with_a_directory_past_the_end(GOOD),
        {},
        f"'{NAME}' cannot be read.*negative seek",
        id="an end record that puts the directory past the end",
    ),
    pytest.param(
        with_a_name_that_is_not_utf8(),
        {},
        "cannot be read as a zip.*utf-8",
        id="a name that is not UTF-8",
    ),
    # ---- the member list
    pytest.param(
        numbered(3),
        {"MAX_MEMBERS": 2},
        "has 3 members, over the limit of 2",
        id="more members than the limit",
    ),
    *[
        pytest.param(
            archive([(name, RECORD)]),
            {},
            "is not named <id>.json",
            id=f"the name: {why}",
        )
        for name, why in BAD_NAMES
    ],
    pytest.param(
        with_nul_in_the_name(),
        {},
        r"\\x00.*is not named <id>.json",
        id="the name: a NUL that zipfile cuts the name at",
    ),
    pytest.param(
        repeated(), {}, f"'{NAME}' is in the dump more than once", id="a repeated name"
    ),
    pytest.param(
        patched(GOOD, flags=0x1), {}, f"'{NAME}' is encrypted", id="an encrypted member"
    ),
    pytest.param(
        archive([(NAME, RECORD)], zipfile.ZIP_BZIP2),
        {},
        "method 12.*only stored and deflated",
        id="bzip2",
    ),
    pytest.param(
        archive([(NAME, RECORD)], zipfile.ZIP_LZMA),
        {},
        "method 14.*only stored and deflated",
        id="lzma",
    ),
    pytest.param(
        archive([(NAME, RECORD)], zipfile.ZIP_ZSTANDARD),
        {},
        "method 93.*only stored and deflated",
        id="zstd",
    ),
    pytest.param(
        patched(GOOD, method=99),
        {},
        "method 99.*only stored and deflated",
        id="a method that zipfile cannot read",
    ),
    # ---- sizes, read as the members are read
    pytest.param(
        archive([(NAME, b"x" * 17)]),
        {"MAX_MEMBER_BYTES": 16},
        f"'{NAME}' is over the limit of 16 bytes",
        id="a deflated member over the limit",
    ),
    pytest.param(
        archive([(NAME, b"x" * 17)], zipfile.ZIP_STORED),
        {"MAX_MEMBER_BYTES": 16},
        f"'{NAME}' is over the limit of 16 bytes",
        id="a stored member over the limit",
    ),
    pytest.param(
        patched(archive([(NAME, b"x" * 1000)]), file_size=10),
        {},
        "Bad CRC-32",
        id="a header that claims fewer bytes than the member holds",
    ),
    pytest.param(
        numbered(3, b"x" * 8),
        {"MAX_TOTAL_BYTES": 20},
        r"add up to over 20 bytes.*'GHSA-aaaa-bbbb-ccc2\.json'",
        id="a running total over the limit",
    ),
    # ---- members that do not read
    pytest.param(
        patched(STORED, crc=0x12345678), {}, "Bad CRC-32", id="a stored member's CRC"
    ),
    pytest.param(
        patched(archive([(NAME, bytes(range(256)) * 4)]), crc=0x12345678),
        {},
        "Bad CRC-32",
        id="a deflated member's CRC",
    ),
    pytest.param(
        patched(archive([(NAME, bytes(range(256)) * 4)]), compress_size=10),
        {},
        "Bad CRC-32",
        id="a deflated member cut short",
    ),
    pytest.param(
        patched(archive([(NAME, b"\xff" * 64)], zipfile.ZIP_STORED), method=8),
        {},
        f"'{NAME}' cannot be read.*decompressing",
        id="a deflate stream that is not one",
    ),
    pytest.param(
        patched(
            archive([(NAME, RECORD), ("GHSA-aaaa-bbbb-ccce.json", RECORD)]),
            compress_size=1000,
        ),
        {},
        "Overlapped entries",
        id="a member whose data runs into the next",
    ),
    pytest.param(
        patched(GOOD, flags=0x20),
        {},
        "flag bit 5",
        id="a member with compressed patched data",
    ),
    pytest.param(
        with_a_zip64_offset_too_large(GOOD),
        {},
        f"'{NAME}' cannot be read.*too large",
        id="a ZIP64 offset too large to seek to",
    ),
]


@pytest.mark.parametrize(("body", "limits", "reason"), HOSTILE)
def test_read_dump_refuses_hostile_archives(monkeypatch, body, limits, reason):
    for bound, value in limits.items():
        monkeypatch.setattr(dump, bound, value)

    with pytest.raises(ValueError, match=reason):
        list(read_dump(body))


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(zipfile.BadZipFile("Bad CRC-32 for file"), id="BadZipFile"),
        pytest.param(zlib.error("Error -3 while decompressing data"), id="zlib.error"),
        pytest.param(EOFError(), id="EOFError"),
        pytest.param(
            NotImplementedError("strong encryption"), id="NotImplementedError"
        ),
        pytest.param(RuntimeError("File is encrypted"), id="RuntimeError"),
        pytest.param(OverflowError("int too large"), id="OverflowError"),
        pytest.param(ValueError("negative seek value -1"), id="ValueError"),
    ],
)
def test_read_dump_names_the_member_that_zipfile_fails_on(monkeypatch, error):
    # Whatever zipfile raises as it reads a member ends as a ValueError that names the
    # member and shows what zipfile said. The archive is a good one, and the failure is
    # put in.
    def fail(self, size=-1):
        raise error

    monkeypatch.setattr(zipfile.ZipExtFile, "read", fail)

    with pytest.raises(ValueError, match=f"'{NAME}' cannot be read: ") as caught:
        list(read_dump(GOOD))

    assert (str(error) or "EOFError") in str(caught.value)
    # zipfile's error is kept neither as the cause nor as the context, so a traceback
    # never prints its text, which can quote a name from the archive in full.
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert caught.value.__suppress_context__


def test_a_refused_dump_shows_no_at_from_the_archive():
    # The central directory names a record, and the member's own header names an
    # address. zipfile's error quotes both names whole, and the error goes to the run's
    # log, which is public for a public repository.
    named = "GHSA-aaaa-0000-0001.json"
    body = with_another_name_in_the_header(archive([(named, RECORD)]), b"jane@x.org")

    with pytest.raises(ValueError, match=f"'{named}' cannot be read") as caught:
        list(read_dump(body))

    refusal = caught.value
    assert not holds_an_at(str(refusal))
    for behind in (refusal.__cause__, refusal.__context__):
        assert behind is None or not holds_an_at(str(behind))
    assert refusal.__suppress_context__


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(
            archive([("A-" + "b" * 60_000 + ".json", RECORD)]),
            id="a name of 60,000 characters",
        ),
        pytest.param(b"x" * 100_000, id="100,000 bytes that are no zip"),
    ],
)
def test_read_dump_errors_stay_short(body):
    # The archive is data from outside, and an error goes to a log and to an issue.
    with pytest.raises(ValueError) as caught:
        list(read_dump(body))
    assert len(str(caught.value)) < 300


def test_read_dump_cuts_what_zipfile_says_short(monkeypatch):
    # zipfile puts names from the archive into its messages, up to 64 KiB of them.
    def fail(self, size=-1):
        raise zipfile.BadZipFile("File name in directory differs from " + "x" * 100_000)

    monkeypatch.setattr(zipfile.ZipExtFile, "read", fail)

    with pytest.raises(ValueError, match="File name in directory") as caught:
        list(read_dump(GOOD))

    assert len(str(caught.value)) < 300


# ---- reading the dump from OSV


@pytest.fixture
def fake() -> FakeHttp:
    return FakeHttp()


@pytest.fixture
def http(fake: FakeHttp) -> Http:
    return Http(transport=fake, sleep=lambda seconds: None)


def test_get_dump_reads_the_dump(fake, http):
    fake.add(DUMP_URL, body=GOOD)

    assert get_dump(http) == GOOD
    assert [url for url, _ in fake.requests] == [DUMP_URL]


@pytest.mark.parametrize("status", [304, 404, 410])
def test_get_dump_must_be_a_200(fake, http, status):
    # The body is a good archive, so only the status is wrong.
    fake.add(DUMP_URL, status=status, body=GOOD)

    with pytest.raises(ValueError, match=f"HTTP {status}"):
        get_dump(http)


def test_get_dump_leaves_a_failed_request_to_http(fake, http):
    fake.add(DUMP_URL, status=403)

    with pytest.raises(UpstreamError) as caught:
        get_dump(http)

    assert caught.value.status == 403


def test_get_dump_refuses_a_body_over_the_limit(monkeypatch, fake, http):
    monkeypatch.setattr(dump, "MAX_DUMP_BYTES", 100)
    fake.add(DUMP_URL, body=b"x" * 100, times=1)
    fake.add(DUMP_URL, body=b"x" * 101, times=1)

    # A body of exactly the limit is returned. That it is no zip is read_dump's to say.
    assert get_dump(http) == b"x" * 100
    with pytest.raises(ValueError, match=re.escape("101 bytes, over the limit of 100")):
        get_dump(http)
