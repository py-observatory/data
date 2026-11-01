import json
import re
import time
import tracemalloc
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace

import pytest

from email_texts import ADDRESSES, HOSTILE_TEXTS, NOT_ADDRESSES, WHITE_SPACE_TEXTS
from pypi_registry.simple import (
    ACCEPT,
    ROOT_URL,
    FileInfo,
    ProjectPage,
    Root,
    parse_project_page,
    parse_root,
    project_url,
    version_url,
)

FIXTURES = Path(__file__).parent / "fixtures"

# Deeper than json can read, so reading it raises RecursionError.
TOO_DEEP = 5_000_000

# Nesting that json reads and repr() cannot show. On the macOS machine where this was
# found, json reads about 116,000 levels and repr() overflows the stack from about
# 69,000. Where json's limit is lower, these bodies fail in json and not in repr(), and
# the tests still hold. The tests that use `deeply_nested` do not depend on either limit.
READABLE_DEPTH = 100_000


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def body(document: object) -> bytes:
    """A document as the body of a reply."""
    return json.dumps(document).encode()


def edited(base: dict, changes: dict | None = None, drop: tuple[str, ...] = ()) -> dict:
    """A copy of `base` with `changes` set and the keys in `drop` taken out."""
    result = {**base, **(changes or {})}
    for key in drop:
        del result[key]
    return result


def root_document(*projects: tuple[str, int], **changes) -> dict:
    """A valid root listing `projects` as (name, serial) pairs."""
    return edited(
        {
            "meta": {"api-version": "1.4", "_last-serial": 100},
            "projects": [{"name": n, "_last-serial": s} for n, s in projects],
        },
        changes,
    )


def file_entry(changes: dict | None = None, drop: tuple[str, ...] = ()) -> dict:
    """A valid file entry of a project page."""
    return edited(
        {
            "filename": "demo-1.0.tar.gz",
            "size": 10,
            "upload-time": "2024-01-02T03:04:05.000006Z",
            "requires-python": None,
            "yanked": False,
            "core-metadata": False,
            "provenance": None,
        },
        changes,
        drop,
    )


def page_document(changes: dict | None = None, drop: tuple[str, ...] = ()) -> dict:
    """A valid project page with one file."""
    return edited(
        {
            "meta": {"api-version": "1.4", "_last-serial": 7},
            "name": "demo",
            "versions": ["1.0"],
            "files": [file_entry()],
        },
        changes,
        drop,
    )


def deeply_nested(depth: int = 300_000) -> list:
    """A list nested `depth` levels deep. It is built in a loop and never goes through
    json, so no limit of json applies to it, and it is deeper than repr() can show."""
    deep: list = []
    for _ in range(depth):
        deep = [deep]
    return deep


def nested_text(depth: int) -> bytes:
    """The JSON of a list nested `depth` levels deep."""
    return b"[" * depth + b"]" * depth


def parsed_file(changes: dict | None = None, drop: tuple[str, ...] = ()) -> FileInfo:
    """The one file of a valid page whose file entry is edited."""
    page = parse_project_page(
        body(page_document({"files": [file_entry(changes, drop)]}))
    )
    (info,) = page.files
    return info


# ---- the URLs


def test_urls():
    assert ROOT_URL == "https://pypi.org/simple/"
    assert ACCEPT == "application/vnd.pypi.simple.v1+json"
    assert project_url("sampleproject") == "https://pypi.org/simple/sampleproject/"
    assert version_url("sampleproject", "4.0.0") == (
        "https://pypi.org/pypi/sampleproject/4.0.0/json"
    )


def test_version_url_quotes_the_version():
    # `+` and `!` are legal in a version and mean something in a URL.
    assert version_url("foo", "1!2.0+local.1") == (
        "https://pypi.org/pypi/foo/1%212.0%2Blocal.1/json"
    )
    assert version_url("foo", "a/b c") == "https://pypi.org/pypi/foo/a%2Fb%20c/json"


# ---- the root


def test_parse_root():
    root = parse_root(fixture("root.json"))
    assert root.serial == 25862120
    # The names are canonical, whatever their spelling in the reply.
    assert root.projects == {
        "sampleproject": 25862117,
        "django": 25500310,
        "zope-interface": 25100042,
        "foo-bar": 25400007,
        "a": 12,
    }


def test_parse_root_rejects_api_v2():
    document = root_document(("foo", 1), meta={"api-version": "2.0", "_last-serial": 1})
    with pytest.raises(ValueError, match="api-version"):
        parse_root(body(document))


@pytest.mark.parametrize("version", ["1", "1.0", "1.1", "1.4", "1.12"])
def test_parse_root_accepts_any_minor_version_of_api_1(version):
    document = root_document(
        ("foo", 1), meta={"api-version": version, "_last-serial": 5}
    )
    assert parse_root(body(document)) == Root(5, {"foo": 1})


@pytest.mark.parametrize(
    "version",
    ["2.0", "0.9", "10.0", "11", "", "x", "1x", ".1", "01.0", " 1.0", 1, 1.0, None],
)
def test_parse_root_rejects_any_other_api_version(version):
    document = root_document(meta={"api-version": version, "_last-serial": 5})
    with pytest.raises(ValueError, match="meta.api-version"):
        parse_root(body(document))


def test_parse_root_rejects_a_root_with_no_api_version():
    with pytest.raises(ValueError, match="meta.api-version"):
        parse_root(body(root_document(meta={"_last-serial": 5})))


@pytest.mark.parametrize(
    ("projects", "expected"),
    [
        # The higher serial wins, whichever of the two comes first.
        ([("Foo_Bar", 10), ("foo-bar", 20)], {"foo-bar": 20}),
        ([("foo-bar", 20), ("Foo_Bar", 10)], {"foo-bar": 20}),
        ([("Foo.Bar", 5), ("foo_bar", 9), ("FOO-BAR", 7)], {"foo-bar": 9}),
        ([("foo-bar", 8), ("Foo_Bar", 8)], {"foo-bar": 8}),
        # Names that do not meet stay apart, and a serial of 0 is a serial.
        ([("foo", 0), ("bar", 3), ("Foo", 0)], {"foo": 0, "bar": 3}),
    ],
)
def test_parse_root_canonical_collision_keeps_the_higher_serial(projects, expected):
    root = parse_root(body(root_document(*projects)))
    assert root.projects == expected


def test_parse_root_of_an_index_with_no_projects():
    assert parse_root(body(root_document())) == Root(100, {})


def test_parse_root_ignores_keys_it_does_not_know():
    document = root_document(("foo", 1), extra={"a": 1})
    document["meta"]["tracks"] = []
    document["projects"][0]["note"] = "x"
    assert parse_root(body(document)) == Root(100, {"foo": 1})


GOOD_META = {"api-version": "1.4", "_last-serial": 7}


@pytest.mark.parametrize(
    ("document", "path"),
    [
        ([], "the Simple API root"),
        ("text", "the Simple API root"),
        ({"projects": []}, "meta"),
        ({"meta": [], "projects": []}, "meta"),
        ({"meta": {"api-version": "1.4"}, "projects": []}, "meta._last-serial"),
        (
            {"meta": {"api-version": "1.4", "_last-serial": "7"}, "projects": []},
            "meta._last-serial",
        ),
        (
            {"meta": {"api-version": "1.4", "_last-serial": 7.5}, "projects": []},
            "meta._last-serial",
        ),
        (
            {"meta": {"api-version": "1.4", "_last-serial": True}, "projects": []},
            "meta._last-serial",
        ),
        (
            {"meta": {"api-version": "1.4", "_last-serial": -1}, "projects": []},
            "meta._last-serial",
        ),
        # Python's json reads NaN and Infinity, which are floats and not serials.
        (
            {
                "meta": {"api-version": "1.4", "_last-serial": float("nan")},
                "projects": [],
            },
            "meta._last-serial",
        ),
        (
            {
                "meta": {"api-version": "1.4", "_last-serial": float("inf")},
                "projects": [],
            },
            "meta._last-serial",
        ),
        ({"meta": GOOD_META}, "projects"),
        ({"meta": GOOD_META, "projects": {}}, "projects"),
        ({"meta": GOOD_META, "projects": None}, "projects"),
        ({"meta": GOOD_META, "projects": ["foo"]}, "projects[0]"),
        ({"meta": GOOD_META, "projects": [{"_last-serial": 1}]}, "projects[0].name"),
        (
            {"meta": GOOD_META, "projects": [{"name": 7, "_last-serial": 1}]},
            "projects[0].name",
        ),
        (
            {"meta": GOOD_META, "projects": [{"name": "", "_last-serial": 1}]},
            "projects[0].name",
        ),
        (
            {"meta": GOOD_META, "projects": [{"name": "foo"}]},
            "projects[0]._last-serial",
        ),
        (
            {"meta": GOOD_META, "projects": [{"name": "foo", "_last-serial": "1"}]},
            "projects[0]._last-serial",
        ),
        (
            {"meta": GOOD_META, "projects": [{"name": "foo", "_last-serial": 1.5}]},
            "projects[0]._last-serial",
        ),
        (
            {"meta": GOOD_META, "projects": [{"name": "foo", "_last-serial": False}]},
            "projects[0]._last-serial",
        ),
        (
            {"meta": GOOD_META, "projects": [{"name": "foo", "_last-serial": None}]},
            "projects[0]._last-serial",
        ),
        (
            {"meta": GOOD_META, "projects": [{"name": "foo", "_last-serial": -3}]},
            "projects[0]._last-serial",
        ),
        # The index of the entry is in the message, so a bad one can be found among
        # hundreds of thousands.
        (
            {
                "meta": GOOD_META,
                "projects": [
                    {"name": "a", "_last-serial": 1},
                    {"name": "b", "_last-serial": 2},
                    {"name": "c"},
                ],
            },
            "projects[2]._last-serial",
        ),
    ],
)
def test_parse_root_rejects_a_shape_it_does_not_know(document, path):
    with pytest.raises(ValueError, match=re.escape(path)):
        parse_root(body(document))


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"not json",
        b"<html><body>502 Bad Gateway</body></html>",
        b'{"meta": ',
        b"\xff\xfe\x00",
        b"{}\x00",
    ],
)
def test_parse_root_rejects_a_body_that_is_not_json(raw):
    with pytest.raises(ValueError, match="the Simple API root is not JSON"):
        parse_root(raw)


def test_parse_root_rejects_json_nested_too_deeply_to_read():
    raw = b'{"meta": ' + b"[" * TOO_DEEP + b"]" * TOO_DEEP + b"}"
    with pytest.raises(ValueError, match="too deeply"):
        parse_root(raw)


# ---- a project page


def test_parse_project_page_fields():
    page = parse_project_page(fixture("page-sampleproject.json"))
    assert page.name == "sampleproject"
    assert page.serial == 25862117
    assert page.status == "active"
    assert page.status_reason is None
    # The versions come in the order of the reply.
    assert page.versions == ["1.2.0", "1.3.1", "2.0.0", "4.0.0"]
    old = ">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4"
    assert page.files == [
        FileInfo(
            filename="sampleproject-1.2.0-py2.py3-none-any.whl",
            version="1.2.0",
            size=3795,
            upload_time="2015-06-14T14:38:05.875222Z",
            requires_python=None,
            yanked=False,
            yanked_reason=None,
            has_metadata=True,
            has_provenance=False,
        ),
        FileInfo(
            filename="sampleproject-1.3.1-py2.py3-none-any.whl",
            version="1.3.1",
            size=3991,
            upload_time="2019-11-04T20:36:25.256613Z",
            requires_python=old,
            yanked=False,
            yanked_reason=None,
            has_metadata=True,
            has_provenance=False,
        ),
        FileInfo(
            filename="sampleproject-2.0.0-py3-none-any.whl",
            version="2.0.0",
            size=4209,
            upload_time="2020-06-25T19:09:43.103653Z",
            requires_python=">=3.5, <4",
            yanked=True,
            yanked_reason="broken build",
            has_metadata=True,
            has_provenance=False,
        ),
        FileInfo(
            filename="sampleproject-4.0.0-py3-none-any.whl",
            version="4.0.0",
            size=4661,
            upload_time="2024-11-06T22:37:09.220617Z",
            requires_python=">=3.9",
            yanked=False,
            yanked_reason=None,
            has_metadata=True,
            has_provenance=True,
        ),
        # An sdist has no separate metadata file.
        FileInfo(
            filename="sampleproject-4.0.0.tar.gz",
            version="4.0.0",
            size=5760,
            upload_time="2024-11-06T22:37:10.868088Z",
            requires_python=">=3.9",
            yanked=False,
            yanked_reason=None,
            has_metadata=False,
            has_provenance=True,
        ),
    ]


def test_yanked_reason_string():
    page = parse_project_page(fixture("page-sampleproject.json"))
    yanked = [(f.filename, f.yanked_reason) for f in page.files if f.yanked]
    assert yanked == [("sampleproject-2.0.0-py3-none-any.whl", "broken build")]
    # A file that is not yanked has no reason.
    assert all(f.yanked_reason is None for f in page.files if not f.yanked)


@pytest.mark.parametrize(
    ("changes", "drop", "yanked", "reason"),
    [
        ({"yanked": False}, (), False, None),
        ({"yanked": True}, (), True, None),
        ({"yanked": "broken build"}, (), True, "broken build"),
        ({"yanked": "  "}, (), True, "  "),  # a reason is kept as it is written
        ({"yanked": "Ünïcode ✓ reason"}, (), True, "Ünïcode ✓ reason"),
        # PEP 691 lets a file leave the key out, and then it is not yanked.
        ({}, ("yanked",), False, None),
        # A reason loses its email addresses. One that is only an address leaves the
        # file yanked with no reason.
        ({"yanked": "jane@x.org"}, (), True, None),
        ({"yanked": "broken, mail jane [at] x [dot] org"}, (), True, "broken, mail"),
    ],
)
def test_yanked_forms(changes, drop, yanked, reason):
    info = parsed_file(changes, drop)
    assert (info.yanked, info.yanked_reason) == (yanked, reason)


@pytest.mark.parametrize("address", ["jane@x.org", "<jane@x.org>", *ADDRESSES])
def test_a_yank_reason_that_is_only_an_address_leaves_the_file_yanked_with_no_reason(
    address,
):
    info = parsed_file({"yanked": address})
    assert (info.yanked, info.yanked_reason) == (True, None)


@pytest.mark.parametrize("address", ["jane@x.org", *ADDRESSES])
def test_a_yank_reason_loses_an_address_of_any_form(address):
    info = parsed_file({"yanked": f"broken, mail {address}"})
    assert (info.yanked, info.yanked_reason) == (True, "broken, mail")
    info = parsed_file({"yanked": f"broken, ask Jane <{address}> first"})
    assert (info.yanked, info.yanked_reason) == (True, "broken, ask Jane first")


@pytest.mark.parametrize(("text", "kept"), WHITE_SPACE_TEXTS)
def test_a_yank_reason_loses_an_address_with_white_space_of_any_kind(text, kept):
    info = parsed_file({"yanked": text})
    assert (info.yanked, info.yanked_reason) == (True, kept)


@pytest.mark.parametrize("text", NOT_ADDRESSES)
def test_a_yank_reason_that_only_looks_like_an_address_is_kept(text):
    assert parsed_file({"yanked": text}).yanked_reason == text


@pytest.mark.parametrize("text", HOSTILE_TEXTS)
def test_reading_a_yank_reason_takes_time_in_proportion_to_its_length(text):
    raw = body(page_document({"files": [file_entry({"yanked": text})]}))
    started = time.perf_counter()
    parse_project_page(raw)
    assert time.perf_counter() - started < 1.0


@pytest.mark.parametrize("value", [None, 1, 0, 1.0, [], {}, ["broken"], ""])
def test_yanked_that_is_not_a_bool_or_a_reason_is_refused(value):
    # An empty string is refused too. PEP 691 asks for a non-empty reason and reads a
    # falsy `yanked` as not yanked, while PEP 592's HTML form writes an empty value for
    # yanked with no reason, so "" has two readings that disagree.
    document = page_document({"files": [file_entry({"yanked": value})]})
    with pytest.raises(ValueError, match=re.escape("files[0].yanked")):
        parse_project_page(body(document))


def test_quarantined_page():
    page = parse_project_page(fixture("page-quarantined.json"))
    assert page.status == "quarantined"
    assert page.files == []
    assert page.versions == []
    assert page.status_reason == "Held while the project is checked for malware"
    assert page.name == "example-quarantined"
    assert page.serial == 25861990


def test_a_page_without_project_status_is_active():
    document = page_document()
    assert "project-status" not in document
    page = parse_project_page(body(document))
    assert (page.status, page.status_reason) == ("active", None)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ({"status": "active"}, ("active", None)),
        ({"status": "archived"}, ("archived", None)),
        ({"status": "archived", "reason": "Moved"}, ("archived", "Moved")),
        ({"status": "deprecated", "reason": None}, ("deprecated", None)),
        # A status that is new to this code is recorded, not refused.
        ({"status": "frozen"}, ("frozen", None)),
        ({"status": "quarantined", "reason": "Ünï"}, ("quarantined", "Ünï")),
    ],
)
def test_project_status_forms(status, expected):
    page = parse_project_page(body(page_document({"project-status": status})))
    assert (page.status, page.status_reason) == expected


@pytest.mark.parametrize(
    ("status", "path"),
    [
        (None, "project-status"),
        ("active", "project-status"),
        ([], "project-status"),
        ({}, "project-status.status"),
        ({"reason": "x"}, "project-status.status"),
        ({"status": None}, "project-status.status"),
        ({"status": 1}, "project-status.status"),
        ({"status": ""}, "project-status.status"),
        ({"status": "active", "reason": 5}, "project-status.reason"),
        ({"status": "active", "reason": ["x"]}, "project-status.reason"),
    ],
)
def test_a_project_status_of_another_shape_is_refused(status, path):
    document = page_document({"project-status": status})
    with pytest.raises(ValueError, match=re.escape(path)):
        parse_project_page(body(document))


def test_a_page_name_is_canonical():
    page = parse_project_page(body(page_document({"name": "Sample_Project.X"})))
    assert page.name == "sample-project-x"


def test_page_versions_are_canonical_and_keep_an_invalid_one_as_written():
    versions = ["1.0.0-RC1", "2.0", "1!3.0+Local", "not a version"]
    page = parse_project_page(body(page_document({"versions": versions})))
    assert page.versions == ["1.0.0rc1", "2.0", "1!3.0+local", "not a version"]


def test_a_file_version_is_read_from_the_filename_and_is_canonical():
    wheel = parsed_file({"filename": "demo-1.0.0RC1-py3-none-any.whl"})
    assert wheel.version == "1.0.0rc1"
    sdist = parsed_file({"filename": "Demo_Pkg-2.5.tar.gz"})
    assert sdist.version == "2.5"


def test_a_file_that_will_not_parse_has_no_version_and_is_still_kept():
    info = parsed_file({"filename": "demo-1.0.win32.exe", "size": 99})
    assert (info.filename, info.version, info.size) == ("demo-1.0.win32.exe", None, 99)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ({"sha256": "ab" * 32}, True),  # PEP 658: the hashes of the metadata file
        (True, True),
        (False, False),
    ],
)
def test_has_metadata_is_the_truthiness_of_core_metadata(value, expected):
    assert parsed_file({"core-metadata": value}).has_metadata is expected


def test_has_metadata_is_false_when_core_metadata_is_left_out():
    assert parsed_file(drop=("core-metadata",)).has_metadata is False


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("https://pypi.org/integrity/demo/1.0/demo-1.0.tar.gz/provenance", True),
        (None, False),
    ],
)
def test_has_provenance_is_whether_provenance_is_not_null(value, expected):
    assert parsed_file({"provenance": value}).has_provenance is expected


def test_has_provenance_is_false_when_provenance_is_left_out():
    assert parsed_file(drop=("provenance",)).has_provenance is False


def test_the_optional_file_fields_may_be_null_or_left_out():
    for info in (
        parsed_file({"size": None, "upload-time": None, "requires-python": None}),
        parsed_file(drop=("size", "upload-time", "requires-python")),
    ):
        assert info.size is None
        assert info.upload_time is None
        assert info.requires_python is None


def test_a_file_size_of_zero_is_a_size():
    assert parsed_file({"size": 0}).size == 0


def test_parse_project_page_ignores_keys_it_does_not_know():
    document = page_document({"alternate-locations": ["https://example.org/simple/"]})
    document["files"][0]["hashes"] = {"sha256": "ab" * 32}
    document["files"][0]["gpg-sig"] = False
    document["meta"]["tracks"] = []
    page = parse_project_page(body(document))
    assert page == parse_project_page(body(page_document()))


@pytest.mark.parametrize("version", ["1", "1.0", "1.1", "1.4", "1.12"])
def test_parse_project_page_accepts_any_minor_version_of_api_1(version):
    meta = {"api-version": version, "_last-serial": 7}
    assert parse_project_page(body(page_document({"meta": meta}))).serial == 7


@pytest.mark.parametrize(
    "version", ["2.0", "0.9", "10.0", "", "1x", ".1", "01.0", 1, 1.0, None]
)
def test_parse_project_page_rejects_any_other_api_version(version):
    meta = {"api-version": version, "_last-serial": 7}
    with pytest.raises(ValueError, match="meta.api-version"):
        parse_project_page(body(page_document({"meta": meta})))


def test_parse_project_page_rejects_api_v2_before_looking_at_the_rest():
    # A newer API may lay the page out another way, so its version is read first.
    document = {"meta": {"api-version": "2.0"}, "something": "else"}
    with pytest.raises(ValueError, match="meta.api-version"):
        parse_project_page(body(document))


@pytest.mark.parametrize(
    ("changes", "drop", "path"),
    [
        ({}, ("meta",), "meta"),
        ({"meta": []}, (), "meta"),
        ({"meta": {"api-version": "1.4"}}, (), "meta._last-serial"),
        (
            {"meta": {"api-version": "1.4", "_last-serial": "7"}},
            (),
            "meta._last-serial",
        ),
        (
            {"meta": {"api-version": "1.4", "_last-serial": True}},
            (),
            "meta._last-serial",
        ),
        ({"meta": {"api-version": "1.4", "_last-serial": -1}}, (), "meta._last-serial"),
        ({}, ("name",), "name"),
        ({"name": None}, (), "name"),
        ({"name": ""}, (), "name"),
        ({"name": 7}, (), "name"),
        ({}, ("files",), "files"),
        ({"files": None}, (), "files"),
        ({"files": {}}, (), "files"),
        ({"files": ["demo-1.0.tar.gz"]}, (), "files[0]"),
        ({"files": [file_entry(), 5]}, (), "files[1]"),
        ({"files": [file_entry(drop=("filename",))]}, (), "files[0].filename"),
        ({"files": [file_entry({"filename": ""})]}, (), "files[0].filename"),
        ({"files": [file_entry({"filename": 3})]}, (), "files[0].filename"),
        ({"files": [file_entry({"size": "10"})]}, (), "files[0].size"),
        ({"files": [file_entry({"size": 1.5})]}, (), "files[0].size"),
        ({"files": [file_entry({"size": True})]}, (), "files[0].size"),
        ({"files": [file_entry({"size": -1})]}, (), "files[0].size"),
        (
            {"files": [file_entry({"upload-time": 20240102})]},
            (),
            "files[0].upload-time",
        ),
        (
            {"files": [file_entry({"requires-python": 3})]},
            (),
            "files[0].requires-python",
        ),
        ({"files": [file_entry(), file_entry({"size": "x"})]}, (), "files[1].size"),
        ({}, ("versions",), "versions"),
        ({"versions": None}, (), "versions"),
        ({"versions": "1.0"}, (), "versions"),
        ({"versions": ["1.0", 2]}, (), "versions[1]"),
        ({"versions": [None]}, (), "versions[0]"),
        ({"versions": [""]}, (), "versions[0]"),
    ],
)
def test_parse_project_page_rejects_a_shape_it_does_not_know(changes, drop, path):
    with pytest.raises(ValueError, match=re.escape(path)):
        parse_project_page(body(page_document(changes, drop)))


@pytest.mark.parametrize("document", [[], "text", 7, None])
def test_parse_project_page_rejects_a_document_that_is_not_an_object(document):
    with pytest.raises(ValueError, match="a project page"):
        parse_project_page(body(document))


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"not json",
        b"<html><body>502 Bad Gateway</body></html>",
        b'{"meta": ',
        b"\xff\xfe\x00",
        b"{}\x00",
    ],
)
def test_parse_project_page_rejects_a_body_that_is_not_json(raw):
    with pytest.raises(ValueError, match="a project page is not JSON"):
        parse_project_page(raw)


def test_parse_project_page_rejects_json_nested_too_deeply_to_read():
    raw = b'{"meta": ' + b"[" * TOO_DEEP + b"]" * TOO_DEEP + b"}"
    with pytest.raises(ValueError, match="too deeply"):
        parse_project_page(raw)


# ---- the message of an error


def test_a_message_names_the_document_the_path_and_the_value():
    document = page_document({"files": [file_entry({"size": "12"})]})
    with pytest.raises(ValueError) as caught:
        parse_project_page(body(document))
    assert str(caught.value) == (
        "a project page: files[0].size must be a non-negative integer or null, not '12'"
    )
    with pytest.raises(ValueError) as caught:
        parse_root(body([1, 2]))
    assert str(caught.value) == "the Simple API root must be a JSON object, not [1, 2]"
    # A value shows three levels deep, and `...` stands for what lies below them.
    with pytest.raises(ValueError) as caught:
        parse_root(body([[[[[1]]]]]))
    assert str(caught.value) == (
        "the Simple API root must be a JSON object, not [[[[...]]]]"
    )


@pytest.mark.parametrize(
    ("bad", "seen"),
    [
        (["x" * 5000], "xxx"),
        # Wide as well as long, so cutting each part short is not enough.
        ([["x" * 5000] * 6 for _ in range(6)], "xxx"),
        ({"k" * 500: "v" * 500}, "kkk"),
        ("z" * 5000, "zzz"),
    ],
)
def test_a_message_shows_a_long_bad_value_only_in_part(bad, seen):
    # Neither a size nor an api-version can be a list, an object or a long string, so
    # each is refused where it stands. A reply can hold anything, and its error goes to
    # a log and to an issue.
    with pytest.raises(ValueError, match=seen) as page_error:
        parse_project_page(body(page_document({"files": [file_entry({"size": bad})]})))
    with pytest.raises(ValueError, match=seen) as root_error:
        parse_root(body(root_document(meta={"api-version": bad, "_last-serial": 1})))
    assert len(str(page_error.value)) < 300
    assert len(str(root_error.value)) < 300


# A bad value, as a function that puts the value `deep` into a document, and the path
# that the error for it names.
DEEP_VALUES = [
    pytest.param(
        parse_root,
        lambda deep: root_document(meta={"api-version": deep, "_last-serial": 1}),
        "meta.api-version",
        id="root api-version",
    ),
    pytest.param(
        parse_root,
        lambda deep: {
            "meta": GOOD_META,
            "projects": [{"name": deep, "_last-serial": 1}],
        },
        "projects[0].name",
        id="root name",
    ),
    pytest.param(
        parse_root, lambda deep: deep, "must be a JSON object", id="root document"
    ),
    pytest.param(
        parse_project_page,
        lambda deep: page_document({"versions": [deep]}),
        "versions[0]",
        id="page version",
    ),
    pytest.param(
        parse_project_page,
        lambda deep: page_document({"files": [file_entry({"yanked": deep})]}),
        "files[0].yanked",
        id="page yanked",
    ),
    pytest.param(
        parse_project_page,
        lambda deep: page_document(
            {"project-status": {"status": "active", "reason": deep}}
        ),
        "project-status.reason",
        id="page status reason",
    ),
    pytest.param(
        parse_project_page,
        lambda deep: deep,
        "must be a JSON object",
        id="page document",
    ),
]


@pytest.mark.parametrize(("parse", "build", "path"), DEEP_VALUES)
def test_a_bad_value_too_deep_for_repr_is_still_a_value_error(
    monkeypatch, parse, build, path
):
    # repr() recurses once for each level of nesting, and overflows the stack on a value
    # that is nested deeply enough. The reply here never goes through json, so how deep
    # it is does not depend on json's limit.
    document = build(deeply_nested())
    monkeypatch.setattr(
        "pypi_registry.simple.json", SimpleNamespace(loads=lambda body: document)
    )
    with pytest.raises(ValueError, match=re.escape(path)) as caught:
        parse(b"")
    assert len(str(caught.value)) < 300


@pytest.mark.parametrize(
    "make_huge",
    [lambda: "y" * 20_000_000, lambda: [0] * 3_000_000],
    ids=["a string of 20 MB", "a list of 3 million"],
)
def test_showing_a_huge_bad_value_does_not_build_its_whole_text(monkeypatch, make_huge):
    # The message holds 60 characters of the value, and the work to show them does not
    # grow with the value. repr() would build the whole text of a string or a list first,
    # which is as much memory again as the value. The value is made before the memory is
    # counted and the reply does not go through json, so what is counted is the parser.
    document = page_document({"files": [file_entry({"size": make_huge()})]})
    monkeypatch.setattr(
        "pypi_registry.simple.json", SimpleNamespace(loads=lambda body: document)
    )
    tracemalloc.start()
    try:
        with pytest.raises(ValueError, match=re.escape("files[0].size")):
            parse_project_page(b"")
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < 1_000_000


READABLE_BODIES = [
    pytest.param(
        parse_root,
        b'{"meta": {"api-version": '
        + nested_text(READABLE_DEPTH)
        + b', "_last-serial": 1}, "projects": []}',
        id="root api-version",
    ),
    pytest.param(
        parse_project_page,
        b'{"meta": {"api-version": "1.4", "_last-serial": 1}, "name": "demo",'
        b' "files": [], "versions": [' + nested_text(READABLE_DEPTH) + b"]}",
        id="page version",
    ),
    pytest.param(parse_root, nested_text(READABLE_DEPTH), id="root document"),
    pytest.param(parse_project_page, nested_text(READABLE_DEPTH), id="page document"),
]


@pytest.mark.parametrize(("parse", "raw"), READABLE_BODIES)
def test_a_body_nested_as_deep_as_json_reads_is_still_a_value_error(parse, raw):
    # The case as it comes in: a body that json reads, holding a bad value that repr()
    # cannot show. Where json's limit is lower, json is what refuses it.
    with pytest.raises(ValueError):
        parse(raw)


# ---- the types


@pytest.mark.parametrize(
    ("value", "field"),
    [
        (Root(1, {}), "serial"),
        (
            FileInfo("a-1.tar.gz", "1", 1, None, None, False, None, False, False),
            "yanked",
        ),
        (ProjectPage("a", 1, "active", None, [], []), "status"),
    ],
)
def test_the_parsed_types_are_frozen(value, field):
    with pytest.raises(FrozenInstanceError):
        setattr(value, field, None)
