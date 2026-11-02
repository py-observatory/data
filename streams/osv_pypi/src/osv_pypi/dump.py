"""OSV's full dump of the PyPI advisories, read in memory and within bounds.

`all.zip` holds the record of every advisory that affects a PyPI project, one member
`<id>.json` each: 37.6 MB with 26,777 members today. The archive comes from the
internet, so it is data and nothing more. `zipfile` parses it over bytes in memory,
nothing in it is run, and nothing is extracted or written to disk, so a member's name is
never a path.

Every number in an archive is a claim, and a hostile archive makes claims that are
false or absurd: a header that says 10 bytes over a stream that never ends, or millions
of members. So no size in it is trusted. `get_dump` bounds the download. `read_dump`
checks the whole member list before it yields a record, then reads each member through
a limit and adds up the bytes as they come. Whatever `zipfile` or `zlib` raise over a
damaged archive is a ValueError, which names the member when one was being read, so
that `read_dump` fails in one way.
"""

import io
import zipfile
import zlib
from collections.abc import Iterator

from obskit.http import Http
from obskit.show import show
from osv_pypi.index import DUMP_URL, is_advisory_id

# The real dump is 37.6 MB, so a body over this, seven times as much, is not a copy of
# it. Http has read the whole body by the time `get_dump` sees it, so this decides what
# is kept and parsed, and not what was downloaded.
MAX_DUMP_BYTES = 256 * 1024 * 1024
# The real dump has 26,777 members. The count is taken once `zipfile` has read the whole
# member list, so this refuses the archive before a record is read, and MAX_DUMP_BYTES
# is what bounds the list itself: some five million entries, which take about 2 GB to
# hold.
MAX_MEMBERS = 200_000
# The largest real record is 76 KB today, so a member over this is not a record.
MAX_MEMBER_BYTES = 4 * 1024 * 1024
# The real records add up to 81 MB. A member's limit does not bound the total: a small
# archive can list thousands of members that each expand to it.
MAX_TOTAL_BYTES = 1024 * 1024 * 1024

_SUFFIX = ".json"
# Bit 0 of a member's flags in the zip format says that it is encrypted.
_ENCRYPTED = 0x1
# OSV's archive is deflated. `zipfile` reads bzip2, lzma and zstd as well, and each
# would be one more decoder that bytes from outside are handed to. An lzma stream even
# sets the size of its own decoder's dictionary.
_METHODS = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)

# What reading a damaged or hostile zip can raise. Each becomes a ValueError, which
# names the member when one was being read.
_ZIP_ERRORS = (
    # Not a zip, a truncated one, a CRC that does not match, members that overlap.
    zipfile.BadZipFile,
    zlib.error,  # a deflate stream that is not one
    EOFError,  # a member that ends before its header says
    # An encrypted member, which `_checked_members` has refused before, and its subclass
    # NotImplementedError: a zip that needs a later version, and a member with strong
    # encryption or patched data.
    RuntimeError,
    OverflowError,  # a ZIP64 offset that is too large to seek to
    ValueError,  # a negative offset, and a name that is not UTF-8
)


def get_dump(http: Http) -> bytes:
    """The bytes of `all.zip` as OSV serves it now.

    Raises ValueError for any reply but a 200 (Http returns a 404 and a 410 and does not
    raise, and a 304 cannot come, because no ETag goes out), and for a body of over
    MAX_DUMP_BYTES. A request that fails is Http's UpstreamError, and it is not caught
    here.
    """
    reply = http.get(DUMP_URL)
    if reply.status != 200:
        raise ValueError(f"GET {DUMP_URL} returned HTTP {reply.status}, not 200")
    if len(reply.body) > MAX_DUMP_BYTES:
        raise ValueError(
            f"the dump is {len(reply.body):,} bytes, "
            f"over the limit of {MAX_DUMP_BYTES:,}"
        )
    return reply.body


def _said(exc: Exception) -> str:
    """What `zipfile` or `zlib` said, as an error message shows it (see `show`). It is
    cut short, because `zipfile` puts names from the archive into some of its messages,
    and a name is up to 64 KiB, and it is not shown when it holds an at. An EOFError has
    no text, so its name stands for it.

    The error itself is dropped. The ValueError that takes its place is raised once the
    handler has ended, and from None, so the error is neither its cause nor its context,
    and a traceback never prints its text in full."""
    return show(str(exc) or type(exc).__name__)


def _open(body: bytes) -> zipfile.ZipFile:
    """The archive in `body`, with its member list read. Raises ValueError for a body
    that `zipfile` cannot read as a zip."""
    try:
        return zipfile.ZipFile(io.BytesIO(body))
    except _ZIP_ERRORS as exc:
        said = _said(exc)
    raise ValueError(f"the dump cannot be read as a zip file: {said}") from None


def _advisory_id(name: str) -> str:
    """The id in a member's name, which must be `<id>.json` for an id that
    `is_advisory_id` accepts. Any other name raises ValueError, a path and a directory
    among them: neither can have an id in front of `.json`."""
    advisory_id = name.removesuffix(_SUFFIX)
    # A name with no suffix comes back from removesuffix as it is.
    if advisory_id == name or not is_advisory_id(advisory_id):
        raise ValueError(
            f"the member {show(name)} is not named <id>.json for an advisory id"
        )
    return advisory_id


def _checked_members(archive: zipfile.ZipFile) -> list[tuple[str, zipfile.ZipInfo]]:
    """Each member of the archive with its id, in archive order, when the whole member
    list is sound. Nothing is read of any member's bytes yet. Raises ValueError for more
    than MAX_MEMBERS members, a name that `_advisory_id` refuses, a name that is listed
    twice, an encrypted member and a method that is not stored or deflated.

    The names are the ones that the archive writes (`orig_filename`). `filename` is the
    name as `zipfile` reads it: cut at the first NUL byte, and changed by the Unicode
    path field of a member. A name could hide behind either.
    """
    infos = archive.infolist()
    if len(infos) > MAX_MEMBERS:
        raise ValueError(
            f"the dump has {len(infos):,} members, over the limit of {MAX_MEMBERS:,}"
        )
    members: dict[str, zipfile.ZipInfo] = {}
    for info in infos:
        name = info.orig_filename
        advisory_id = _advisory_id(name)
        if advisory_id in members:
            raise ValueError(f"the member {show(name)} is in the dump more than once")
        if info.flag_bits & _ENCRYPTED:
            raise ValueError(f"the member {show(name)} is encrypted")
        if info.compress_type not in _METHODS:
            raise ValueError(
                f"the member {show(name)} is compressed with method "
                f"{info.compress_type}, and only stored and deflated members are read"
            )
        members[advisory_id] = info
    return list(members.items())


def _read_member(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> bytes:
    """The bytes of one member, when there are at most MAX_MEMBER_BYTES of them.

    `read` is asked for one byte more than the limit, and a decoder stops there, so a
    member that expands for ever costs the limit and no more, whatever size its header
    claims. Getting that extra byte means that the member is over the limit, and raises
    ValueError. So does a member that `zipfile` cannot read to its end, which is where
    it checks the CRC of a member that is read whole.
    """
    name = info.orig_filename
    try:
        with archive.open(info) as member:
            record = member.read(MAX_MEMBER_BYTES + 1)
    except _ZIP_ERRORS as exc:
        said = _said(exc)
    else:
        if len(record) > MAX_MEMBER_BYTES:
            raise ValueError(
                f"the member {show(name)} is over the limit of "
                f"{MAX_MEMBER_BYTES:,} bytes"
            )
        return record
    raise ValueError(f"the member {show(name)} cannot be read: {said}") from None


def read_dump(body: bytes) -> Iterator[tuple[str, bytes]]:
    """The records of the dump `body`, which is the bytes of `all.zip`: the id and the
    bytes of each member, in archive order. The archive is read through `zipfile` over
    `io.BytesIO`. Nothing is extracted, written or run.

    The whole member list is checked before the first record is yielded. Raises
    ValueError for an archive that is not a zip, for more than MAX_MEMBERS members, for
    a name that is not `<id>.json` for an id that `is_advisory_id` accepts (a path and a
    directory are not), for a name that is listed twice, for an encrypted member, and
    for a compression method other than stored and deflated.

    Each member is read when it is asked for, through a limit, and the bytes are added
    up. Raises ValueError for a member of over MAX_MEMBER_BYTES, whatever its header
    claims, and for a total of over MAX_TOTAL_BYTES. Whatever `zipfile` or `zlib` raise
    over a damaged member (a CRC that does not match, members that overlap, a stream
    that does not decompress) is a ValueError as well, which names the member and shows
    what was said. The error itself is not kept, as cause or as context, because its
    text can quote a name from the archive in full.

    A ValueError refuses the whole dump and ends the iteration: the records yielded
    before it came from an archive that is refused.
    """
    with _open(body) as archive:
        total = 0
        for advisory_id, info in _checked_members(archive):
            record = _read_member(archive, info)
            total += len(record)
            if total > MAX_TOTAL_BYTES:
                raise ValueError(
                    f"the members of the dump add up to over {MAX_TOTAL_BYTES:,} "
                    f"bytes, at {show(info.orig_filename)}"
                )
            yield advisory_id, record
