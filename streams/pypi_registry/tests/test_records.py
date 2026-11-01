import copy
import hashlib
import json
import random
import re
import sqlite3
import time
from contextlib import closing
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, cast

import pytest

from pypi_registry.records import (
    README_PREFIX_BYTES,
    RECORD_COLUMNS,
    ReadmeFacts,
    readme_facts,
    release_record,
    strip_emails,
)

FIXTURES = Path(__file__).parent / "fixtures"
OBSERVED_AT = "2026-11-01T06:00:00Z"

# What counts as an email address, as a plain pattern that shares nothing with the code
# under test.
ADDRESS = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# The text fields of a release that can hold something a person wrote, and so an address.
SCRUBBED_COLUMNS = [
    "summary",
    "license",
    "keywords",
    "home_page",
    "download_url",
    "platform",
    "yanked_reason",
]


def sample() -> dict:
    """The sampleproject 4.0.0 reply, read afresh so that a test may edit it."""
    path = FIXTURES / "version-sampleproject-4.0.0.json"
    return json.loads(path.read_text(encoding="utf-8"))


def record_of(
    changes: dict | None = None,
    *,
    files: list | None = None,
    name: str = "sampleproject",
    incarnation: int = 1,
) -> dict:
    """The record of the sample release, with `changes` made to its `info` and `files`
    (the reply's `urls`) taken from the reply unless given."""
    document = sample()
    info = {**document["info"], **(changes or {})}
    urls = document["urls"] if files is None else files
    return release_record(name, incarnation, info, urls, observed_at=OBSERVED_AT)


def texts(record: dict) -> list[str]:
    """Every value of a record that is a string."""
    return [value for value in record.values() if isinstance(value, str)]


def whole_characters_within(text: str, limit: int) -> bytes:
    """The longest start of `text`, in whole characters, that fits in `limit` UTF-8
    bytes. It is built one character at a time, so it shares no code with the function
    that the tests check against it."""
    kept = bytearray()
    for character in text:
        encoded = character.encode()
        if len(kept) + len(encoded) > limit:
            break
        kept += encoded
    return bytes(kept)


# ---- README facts


def test_readme_prefix_is_40000_bytes():
    assert README_PREFIX_BYTES == 40_000


def test_readme_facts_none_and_empty():
    none = readme_facts(None, "text/markdown")
    assert (none.sha256, none.bytes, none.words) == (None, None, None)
    assert none.content_type == "text/markdown"
    empty = readme_facts("", "text/plain")
    assert (empty.sha256, empty.bytes, empty.words) == (None, 0, 0)
    assert empty.content_type == "text/plain"
    # The whole description is in hand either way, so neither length is a lower bound.
    assert none.bytes_exact is True
    assert empty.bytes_exact is True


def test_readme_facts_short():
    text = "Hello, wörld!\n\nTwo paragraphs here.\n"
    facts = readme_facts(text, "text/markdown")
    assert facts == ReadmeFacts(
        sha256=hashlib.sha256(text.encode()).hexdigest(),
        bytes=37,  # 36 characters, and the o with a diaeresis takes two bytes
        bytes_exact=True,
        words=5,
        content_type="text/markdown",
    )


def test_readme_facts_of_whitespace_only_text():
    facts = readme_facts("  \n\t", None)
    assert facts.sha256 == hashlib.sha256(b"  \n\t").hexdigest()
    assert (facts.bytes, facts.words) == (4, 0)


@pytest.mark.parametrize(
    "field", ["sha256", "bytes", "bytes_exact", "words", "content_type"]
)
def test_readme_facts_are_frozen(field):
    with pytest.raises(FrozenInstanceError):
        setattr(readme_facts("x", None), field, None)


def test_readme_fingerprint_uses_first_40000_bytes():
    facts = readme_facts("a" * 50000, None)
    assert facts.sha256 == hashlib.sha256(b"a" * 40000).hexdigest()
    assert facts.bytes == 50000
    assert facts.bytes_exact is True
    assert facts.words == 1


def test_readme_fingerprint_does_not_see_past_the_prefix():
    # BigQuery keeps the first 40,000 bytes of a README and PyPI serves the whole of it,
    # and the two must give one fingerprint.
    head = "a" * README_PREFIX_BYTES
    tail = " and a tail that the fingerprint cannot see"
    cut = readme_facts(head, None)
    whole = readme_facts(head + tail, None)
    assert whole.sha256 == cut.sha256
    assert cut.bytes == README_PREFIX_BYTES
    assert whole.bytes == README_PREFIX_BYTES + len(tail)


def test_readme_boundary_multibyte_trimmed():
    # The cut at 40,000 bytes falls between the two bytes of the e with an acute accent.
    text = "a" * 39999 + "é" + "b" * 10
    facts = readme_facts(text, None)
    assert facts.sha256 == hashlib.sha256(b"a" * 39999).hexdigest()
    assert facts.bytes == 39999 + 2 + 10


@pytest.mark.parametrize("character", ["é", "€", "\U0001f600"])
@pytest.mark.parametrize(
    "before", range(README_PREFIX_BYTES - 4, README_PREFIX_BYTES + 1)
)
def test_readme_boundary_every_split_of_a_multibyte_character(character, before):
    # Two, three and four bytes long, starting 4 bytes before the cut up to exactly at
    # it: the character is cut inside, or ends exactly at the cut, or is left out.
    text = "a" * before + character + "z" * 5
    expected = whole_characters_within(text, README_PREFIX_BYTES)
    facts = readme_facts(text, None)
    assert facts.sha256 == hashlib.sha256(expected).hexdigest()
    assert facts.bytes == len(text.encode())


def test_readme_with_a_character_that_utf8_cannot_hold_is_refused():
    # A lone surrogate is not text. It is not guessed at, and it is not encoded some
    # other way that no other reader of the fingerprint would agree with.
    with pytest.raises(ValueError):
        readme_facts("a\ud800b", None)


# ---- people's names


def test_strip_emails_several_entries():
    assert strip_emails("Jane Doe <jane@x.org>, Bob <bob@y.com>") == "Jane Doe, Bob"


def test_strip_emails_bare_address_only():
    assert strip_emails("jane@x.org") is None
    assert strip_emails("<jane@x.org>") is None
    assert strip_emails("jane@x.org, bob@y.com") is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("Jane Doe", "Jane Doe"),
        ("Jane Doe <jane@x.org>", "Jane Doe"),
        ('"A. Random Developer" <author@example.com>', "A. Random Developer"),
        ("'Jane Doe' <jane@x.org>", "Jane Doe"),
        ("<jane@x.org> Jane Doe", "Jane Doe"),
        ("Jane Doe jane@x.org", "Jane Doe"),  # an address with no brackets
        ("Jane (jane@x.org)", "Jane"),
        ("Jane < jane@x.org >", "Jane"),  # spaces inside the brackets
        ("Jane <jane@x.org, Bob>", "Jane"),  # a bracket is one group, whatever is in it
        ("Jane<jane@x.org>Doe", "Jane Doe"),  # and it parts what was either side
        ("Jane,jane@x.org", "Jane"),  # no space after the comma
        ("jane@x.org,bob@y.com", None),
        ("Jane <jane@x.org", "Jane"),  # a bracket that never closes
        ("jane@x.org>", None),
        ("Conan O'Brien <conan@x.org>", "Conan O'Brien"),  # only the ends lose quotes
        (" ,  , Jane , , ", "Jane"),  # empty parts are dropped
        ("Jane   Doe", "Jane Doe"),  # a run of spaces inside a name is one space
        # Display names are kept in any script (Latin with diaereses, and Chinese), and an
        # address with such a name goes like any other.
        (
            "Jörg Müller <jörg@example.org>, 李雷 <lilei@example.cn>",
            "Jörg Müller, 李雷",
        ),
    ],
)
def test_strip_emails_forms(value, expected):
    assert strip_emails(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "@",
        "@@",
        "x @ y",
        "<@>",
        "a@b",
        "name@",
        "@name",
        ",@,",
        "a@b@c",
        "<<jane@x.org>>",
        "Jane <jane@x.org> <jane2@y.com>",
        "Jane <jane@x.org, Bob>",
    ],
)
def test_strip_emails_never_leaves_an_at_sign(value):
    result = strip_emails(value)
    assert result is None or "@" not in result


# ---- the record's columns


def test_record_columns_are_in_the_documented_order():
    assert RECORD_COLUMNS == (
        "name",
        "name_raw",
        "incarnation",
        "version",
        "version_raw",
        "uploaded_at",
        "summary",
        "license",
        "license_expression",
        "license_files",
        "requires_python",
        "requires_dist",
        "provides_extra",
        "classifiers",
        "keywords",
        "project_urls",
        "home_page",
        "download_url",
        "author",
        "author_email_names",
        "maintainer",
        "maintainer_email_names",
        "platform",
        "dynamic",
        "readme_sha256",
        "readme_bytes",
        "readme_bytes_exact",
        "readme_words",
        "readme_content_type",
        "yanked",
        "yanked_reason",
        "source",
        "observed_at",
    )


def test_release_record_columns_and_no_email():
    info = sample()["info"]
    # The fixture is only a test if it carries an address in each field of a person.
    for key in ("author", "author_email", "maintainer", "maintainer_email"):
        assert "@" in info[key], key
    record = record_of()
    assert list(record) == list(RECORD_COLUMNS)
    assert record["name_raw"] == "sampleproject"
    assert [value for value in texts(record) if "@" in value] == []


def test_release_record_for_the_sample_release():
    document = sample()
    info = document["info"]
    description = info["description"]
    record = release_record(
        "sampleproject", 2, info, document["urls"], observed_at=OBSERVED_AT
    )
    assert record == {
        "name": "sampleproject",
        "name_raw": "sampleproject",
        "incarnation": 2,
        "version": "4.0.0",
        "version_raw": "4.0.0",
        "uploaded_at": "2024-11-06T22:37:09.220617Z",
        "summary": "A sample Python project",
        "license": info["license"],
        "license_expression": None,
        "license_files": None,
        "requires_python": ">=3.9",
        "requires_dist": (
            r'["peppercorn","check-manifest; extra == \"dev\"",'
            r'"coverage; extra == \"test\""]'
        ),
        "provides_extra": '["dev","test"]',
        "classifiers": (
            '["Development Status :: 3 - Alpha","Intended Audience :: Developers",'
            '"License :: OSI Approved :: MIT License",'
            '"Programming Language :: Python :: 3",'
            '"Programming Language :: Python :: 3 :: Only",'
            '"Programming Language :: Python :: 3.10",'
            '"Programming Language :: Python :: 3.11",'
            '"Programming Language :: Python :: 3.12",'
            '"Programming Language :: Python :: 3.13",'
            '"Programming Language :: Python :: 3.9",'
            '"Topic :: Software Development :: Build Tools"]'
        ),
        "keywords": "sample, setuptools, development",
        "project_urls": (
            '{"Bug Reports":"https://github.com/pypa/sampleproject/issues",'
            '"Funding":"https://donate.pypi.org",'
            '"Homepage":"https://github.com/pypa/sampleproject",'
            '"Say Thanks!":"http://saythanks.io/to/example",'
            '"Source":"https://github.com/pypa/sampleproject/"}'
        ),
        "home_page": None,
        "download_url": None,
        "author": "A. Random Developer",
        "author_email_names": "A. Random Developer",
        "maintainer": "A. Great Maintainer",
        "maintainer_email_names": "A. Great Maintainer",
        "platform": None,
        "dynamic": None,
        "readme_sha256": hashlib.sha256(description.encode()).hexdigest(),
        "readme_bytes": 560,  # 556 characters: the two curly quotes take three bytes
        "readme_bytes_exact": True,
        "readme_words": 74,
        "readme_content_type": "text/markdown",
        "yanked": False,
        "yanked_reason": None,
        "source": "json-api",
        "observed_at": OBSERVED_AT,
    }


def test_release_record_names_and_versions():
    record = record_of(
        {"name": "Sample_Project", "version": "1.0.0-RC1"},
        name="sample-project",
        incarnation=3,
    )
    # The name passed in is the key. The display spelling is kept beside it.
    assert (record["name"], record["name_raw"]) == ("sample-project", "Sample_Project")
    assert (record["version"], record["version_raw"]) == ("1.0.0rc1", "1.0.0-RC1")
    assert record["incarnation"] == 3


def test_release_record_keeps_a_version_that_pep_440_refuses():
    record = record_of({"version": "0.1dev-r1234"})
    assert (record["version"], record["version_raw"]) == (
        "0.1dev-r1234",
        "0.1dev-r1234",
    )


# ---- when it was uploaded


def test_release_record_uploaded_at_is_earliest_file():
    early = "2024-11-06T22:37:09.220617Z"
    files = [
        {"upload_time_iso_8601": "2024-11-06T22:37:10.868088Z"},
        {"upload_time_iso_8601": early},
        {"upload_time_iso_8601": "2024-11-07T00:00:00.000001Z"},
    ]
    assert record_of(files=files)["uploaded_at"] == early
    assert record_of(files=files[::-1])["uploaded_at"] == early


def test_release_record_uploaded_at_orders_by_time_and_not_by_text():
    # Python writes no fraction for a time on the whole second, and "." sorts before "Z",
    # so as text the later time would come first.
    files = [
        {"upload_time_iso_8601": "2024-11-06T22:37:09.220617Z"},
        {"upload_time_iso_8601": "2024-11-06T22:37:09Z"},
    ]
    assert record_of(files=files)["uploaded_at"] == "2024-11-06T22:37:09Z"
    # The same for a time given in another zone.
    files = [
        {"upload_time_iso_8601": "2024-11-06T22:30:00Z"},
        {"upload_time_iso_8601": "2024-11-06T23:00:00+01:00"},
    ]
    assert record_of(files=files)["uploaded_at"] == "2024-11-06T23:00:00+01:00"


def test_release_record_uploaded_at_takes_a_time_with_no_zone_as_utc():
    # Such a time and one with a zone can still be compared.
    files = [
        {"upload_time_iso_8601": "2024-11-06T22:30:00Z"},
        {"upload_time_iso_8601": "2024-11-06T22:00:00"},
    ]
    assert record_of(files=files)["uploaded_at"] == "2024-11-06T22:00:00"


def test_release_record_uploaded_at_when_files_have_no_time():
    assert record_of(files=[])["uploaded_at"] is None
    assert record_of(files=[{}, {"upload_time_iso_8601": None}])["uploaded_at"] is None
    files = [{}, {"upload_time_iso_8601": "2024-01-01T00:00:00Z"}]
    assert record_of(files=files)["uploaded_at"] == "2024-01-01T00:00:00Z"


# ---- lists and dicts


def test_release_record_lists_and_dicts_are_compact_json():
    changes = {
        "classifiers": ["Topic :: A", "Topic :: B"],
        "requires_dist": ['pkg>=1; python_version < "3.12"', "café>=2"],
        "provides_extra": ["dev"],
        "license_files": ["LICENSE", "licenses/LICENSE-é.txt"],
        "dynamic": ["version", "readme"],
        "project_urls": {"b": "https://b.example", "a": "https://a.example"},
    }
    record = record_of(changes)
    assert record["classifiers"] == '["Topic :: A","Topic :: B"]'
    assert record["requires_dist"] == (
        '["pkg>=1; python_version < \\"3.12\\"","café>=2"]'
    )
    assert record["provides_extra"] == '["dev"]'
    # A character outside ASCII is written as it is, and not as an escape.
    assert record["license_files"] == '["LICENSE","licenses/LICENSE-é.txt"]'
    assert record["dynamic"] == '["version","readme"]'
    # A dict is written with its keys in order.
    assert record["project_urls"] == '{"a":"https://a.example","b":"https://b.example"}'
    for column in ("classifiers", "requires_dist", "license_files", "project_urls"):
        assert json.loads(record[column]) == changes[column]


@pytest.mark.parametrize(
    "column",
    ["classifiers", "requires_dist", "provides_extra", "license_files", "dynamic"],
)
@pytest.mark.parametrize("empty", [[], None])
def test_release_record_empty_lists_become_none(column, empty):
    assert record_of({column: empty})[column] is None


@pytest.mark.parametrize("empty", [{}, None])
def test_release_record_empty_project_urls_become_none(empty):
    assert record_of({"project_urls": empty})["project_urls"] is None


def test_release_record_without_the_optional_keys():
    document = sample()
    info = {"name": "demo", "version": "1.0"}
    record = release_record("demo", 1, info, document["urls"], observed_at=OBSERVED_AT)
    assert list(record) == list(RECORD_COLUMNS)
    absent = [c for c in RECORD_COLUMNS if record[c] is None]
    assert {
        "summary",
        "license",
        "classifiers",
        "readme_sha256",
        "readme_bytes",
    } <= set(absent)
    assert (record["yanked"], record["yanked_reason"]) == (False, None)


# ---- README facts in a record


def test_release_record_has_readme_facts_and_no_readme_text():
    info = sample()["info"]
    description = info["description"]
    record = record_of()
    assert record["readme_sha256"] == hashlib.sha256(description.encode()).hexdigest()
    assert record["readme_bytes"] == len(description.encode())
    assert record["readme_bytes_exact"] is True
    assert record["readme_words"] == len(description.split())
    assert record["readme_content_type"] == "text/markdown"
    # No value holds a sentence of the text, and the text has no column.
    assert "description" not in record
    assert not [v for v in texts(record) if "A sample project that exists" in v]


def test_release_record_readme_of_a_long_text_is_fingerprinted_on_its_start():
    text = "a" * 39999 + "é" + "b" * 10
    record = record_of({"description": text, "description_content_type": "text/plain"})
    assert record["readme_sha256"] == hashlib.sha256(b"a" * 39999).hexdigest()
    assert record["readme_bytes"] == 40011
    assert record["readme_content_type"] == "text/plain"


@pytest.mark.parametrize(
    ("description", "expected"),
    [(None, (None, None, None)), ("", (None, 0, 0))],
)
def test_release_record_without_a_readme(description, expected):
    record = record_of({"description": description, "description_content_type": None})
    facts = (record["readme_sha256"], record["readme_bytes"], record["readme_words"])
    assert facts == expected
    assert record["readme_content_type"] is None


# ---- no email addresses


def test_release_record_person_fields():
    record = record_of(
        {
            "author": "Jane Doe <jane@x.org>",
            "author_email": "Jane Doe <jane@x.org>, Bob <bob@y.com>",
            "maintainer": "bob@y.com",
            "maintainer_email": None,
        }
    )
    assert record["author"] == "Jane Doe"
    assert record["author_email_names"] == "Jane Doe, Bob"
    assert record["maintainer"] is None
    assert record["maintainer_email_names"] is None


def test_release_record_scrubs_emails_from_free_text():
    record = record_of(
        {
            "summary": "contact jane@x.org",
            "license": "Copyright Jane <jane@x.org>",
            "project_urls": {
                "Contact": "mailto:jane@x.org",
                "Homepage": "https://example.org/jane",
            },
            "requires_dist": ["foo @ https://example.com/foo.whl"],
        }
    )
    assert record["summary"] == "contact"
    assert record["license"] == "Copyright Jane"
    assert record["project_urls"] == '{"Homepage":"https://example.org/jane"}'
    # A requirement that names a URL is not a contact address.
    assert record["requires_dist"] == '["foo @ https://example.com/foo.whl"]'
    for column in ("summary", "license", "keywords", "home_page", "download_url"):
        assert "@" not in (record[column] or ""), column
    assert "@" not in record["project_urls"]


@pytest.mark.parametrize("column", SCRUBBED_COLUMNS)
def test_release_record_scrubs_every_free_text_column(column):
    record = record_of({column: "see jane@x.org for more"})
    assert record[column] == "see for more"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("contact jane@x.org", "contact"),
        ("jane@x.org is the contact", "is the contact"),
        ("write to jane@x.org now", "write to now"),
        ("Copyright Jane <jane@x.org>", "Copyright Jane"),
        ("Copyright Jane < jane@x.org >", "Copyright Jane"),
        ("Jane <jane@x.org> and Bob <bob@y.com>", "Jane and Bob"),
        ("a.b-c_d+tag%x@sub.example.co.uk", None),
        ("jane@x.org bob@y.com", None),
        ("<jane@x.org>", None),
        ("jane@x.org", None),
        # Only the whitespace beside an address goes, so a text keeps its layout.
        (
            "Copyright 2020 Jane <jane@x.org>\nAll rights reserved.",
            "Copyright 2020 Jane\nAll rights reserved.",
        ),
        ("one  two  jane@x.org  three", "one  two  three"),
        # An address in any script goes whole, and leaves no part of itself behind: a
        # Latin name with a diaeresis, a domain with one, and a Cyrillic top-level domain.
        ("contact jörg@example.org", "contact"),
        ("contact jane@exämple.org", "contact"),
        ("contact jane@example.рф", "contact"),
        # No address: the text comes back as it was.
        ("Decorator @cache for functions", "Decorator @cache for functions"),
        ("user@localhost", "user@localhost"),  # no domain with a dot
        ("x@y.z", "x@y.z"),  # and a top-level domain is two letters or more
        ("  Line one.\n\nLine two.  \n", "  Line one.\n\nLine two.  \n"),
        ("", None),
        (None, None),
    ],
)
def test_release_record_scrub_forms(text, expected):
    assert record_of({"summary": text})["summary"] == expected


@pytest.mark.parametrize("column", SCRUBBED_COLUMNS)
@pytest.mark.parametrize("text", ["me@<a@b.cc>x.org", "hello me@<a@b.cc>x.org"])
def test_release_record_drops_a_text_that_taking_out_an_address_turns_into_one(
    column, text
):
    # Taking out `<a@b.cc>` joins `me@` to `x.org`. Only text made to do that is like it,
    # so the whole of it goes and no address is left.
    assert record_of({column: text})[column] is None


@pytest.mark.parametrize("column", ["home_page", "download_url"])
@pytest.mark.parametrize("link", ["mailto:jane@x.org", "MAILTO:Jane@X.org", "mailto:"])
def test_release_record_drops_a_mail_link_for_a_page_or_a_download(column, link):
    assert record_of({column: link})[column] is None


def test_release_record_project_urls_drop_contact_entries():
    record = record_of(
        {
            "project_urls": {
                "Homepage": "https://example.org/jane",
                "Contact": "mailto:jane@x.org",
                "Mail": "MAILTO:jane@x.org",  # a scheme is not case sensitive
                "Write": "mailto:",  # no address, and still a mail link
                "Form": "https://example.org/contact?to=jane@x.org",
                "jane@x.org": "https://example.org/jane",  # the label holds it
                "Twitter": "https://twitter.com/@jane",  # an @ that is no address
                "Docs": "https://docs.example.org/",
            }
        }
    )
    assert record["project_urls"] == (
        '{"Docs":"https://docs.example.org/",'
        '"Homepage":"https://example.org/jane",'
        '"Twitter":"https://twitter.com/@jane"}'
    )


def test_release_record_project_urls_with_only_contact_entries_become_none():
    assert (
        record_of({"project_urls": {"Contact": "mailto:jane@x.org"}})["project_urls"]
        is None
    )


def test_no_field_of_a_record_keeps_an_address_whatever_the_text():
    # Texts put together at random from pieces of addresses, brackets and spaces. They
    # hold addresses of every shape, and some that taking another address out makes.
    pieces = [
        "me@",
        "<a@b.cc>",
        "x.org",
        "jane@x.org",
        "<jane@x.org>",
        "bob@y.com",
        "<",
        ">",
        "@",
        " ",
        "  ",
        "\t",
        "\n",
        ".",
        ",",
        "-",
        "a",
        "bc",
        "mailto:",
        "é",
        "рф",  # Cyrillic
    ]
    chooser = random.Random(20261101)
    people = ("author", "author_email", "maintainer", "maintainer_email")
    for _ in range(3000):
        text = "".join(chooser.choice(pieces) for _ in range(chooser.randint(1, 12)))
        changes: dict[str, object] = {column: text for column in SCRUBBED_COLUMNS}
        changes.update(dict.fromkeys(people, text))
        changes["project_urls"] = {text: text, "Docs": "https://docs.example.org/"}
        record = record_of(changes)
        for column in SCRUBBED_COLUMNS:
            assert not ADDRESS.search(record[column] or ""), (column, text)
        for column in (
            "author",
            "author_email_names",
            "maintainer",
            "maintainer_email_names",
        ):
            assert "@" not in (record[column] or ""), (column, text)
        for label, url in json.loads(record["project_urls"]).items():
            assert not ADDRESS.search(label) and not ADDRESS.search(url), text
            assert not url.lower().startswith("mailto:"), text


def test_release_record_leaves_requires_dist_alone():
    requirements = [
        "foo @ https://example.com/foo.whl",
        "bar @ git+ssh://git@github.com/org/bar.git",
        "baz>=1",
    ]
    record = record_of({"requires_dist": requirements})
    assert json.loads(record["requires_dist"]) == requirements


# ---- yanked


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"yanked": False, "yanked_reason": None}, (False, None)),
        ({"yanked": True, "yanked_reason": "broken build"}, (True, "broken build")),
        ({"yanked": True, "yanked_reason": None}, (True, None)),
        ({"yanked": True, "yanked_reason": ""}, (True, None)),
        # A string in place of the bool is the reason, as the Simple API writes it.
        ({"yanked": "broken build", "yanked_reason": None}, (True, "broken build")),
        # When both are given, the reason field is the reason.
        (
            {"yanked": "short", "yanked_reason": "a longer reason"},
            (True, "a longer reason"),
        ),
        ({"yanked": None, "yanked_reason": None}, (False, None)),
        # The reason is text that a person wrote.
        ({"yanked": True, "yanked_reason": "ask sec@x.org"}, (True, "ask")),
    ],
)
def test_release_record_yanked(changes, expected):
    record = record_of(changes)
    assert (record["yanked"], record["yanked_reason"]) == expected
    assert type(record["yanked"]) is bool


# ---- a reply of the wrong shape


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"summary": 5}, "info.summary must be a string or null, not 5"),
        ({"license": ["x"]}, "info.license must be a string or null"),
        ({"keywords": ["a", "b"]}, "info.keywords must be a string or null"),
        ({"home_page": 1.5}, "info.home_page must be a string or null"),
        ({"author": 3}, "info.author must be a string or null"),
        ({"author_email": []}, "info.author_email must be a string or null"),
        ({"maintainer": {}}, "info.maintainer must be a string or null"),
        ({"description": 7}, "info.description must be a string or null"),
        ({"description_content_type": 7}, "info.description_content_type must be"),
        ({"requires_python": 3}, "info.requires_python must be a string or null"),
        ({"platform": True}, "info.platform must be a string or null"),
        ({"classifiers": "x"}, "info.classifiers must be a list of strings or null"),
        ({"classifiers": {}}, "info.classifiers must be a list of strings or null"),
        ({"classifiers": ["a", 1]}, "info.classifiers must be a list of strings"),
        ({"requires_dist": {"a": 1}}, "info.requires_dist must be a list of strings"),
        ({"project_urls": ["x"]}, "info.project_urls must be an object of strings"),
        ({"project_urls": []}, "info.project_urls must be an object of strings"),
        ({"project_urls": {"a": 1}}, "info.project_urls must be an object of strings"),
        ({"name": None}, "info.name must be a non-empty string, not None"),
        ({"name": ""}, "info.name must be a non-empty string"),
        ({"version": None}, "info.version must be a non-empty string"),
        ({"version": 4}, "info.version must be a non-empty string, not 4"),
        ({"yanked": ""}, "info.yanked must be a boolean or a non-empty reason string"),
        ({"yanked": 1}, "info.yanked must be a boolean or a non-empty reason string"),
        ({"yanked_reason": 5}, "info.yanked_reason must be a string or null"),
    ],
)
def test_release_record_refuses_a_value_of_the_wrong_shape(changes, message):
    with pytest.raises(ValueError, match=re.escape(f"a release's JSON: {message}")):
        record_of(changes)


def test_release_record_refuses_info_and_files_of_the_wrong_shape():
    document = sample()
    with pytest.raises(ValueError, match=r"info must be an object, not None"):
        release_record("demo", 1, cast(Any, None), [], observed_at=OBSERVED_AT)
    with pytest.raises(ValueError, match=r"urls must be a list, not 'x'"):
        release_record(
            "demo", 1, document["info"], cast(Any, "x"), observed_at=OBSERVED_AT
        )


@pytest.mark.parametrize(
    ("files", "message"),
    [
        (["x"], "urls[0] must be an object, not 'x'"),
        ([{}, 5], "urls[1] must be an object, not 5"),
        (
            [{"upload_time_iso_8601": 20241106}],
            "urls[0].upload_time_iso_8601 must be a string or null, not 20241106",
        ),
        (
            [{"upload_time_iso_8601": "last week"}],
            "urls[0].upload_time_iso_8601 must be an ISO 8601 time, not 'last week'",
        ),
        (
            [{}, {"upload_time_iso_8601": ""}],
            "urls[1].upload_time_iso_8601 must be an ISO 8601 time, not ''",
        ),
    ],
)
def test_release_record_refuses_files_of_the_wrong_shape(files, message):
    with pytest.raises(ValueError, match=re.escape(f"a release's JSON: {message}")):
        record_of(files=files)


def test_release_record_error_for_a_bad_time_does_not_carry_the_whole_text():
    # The text of a time that will not parse could be any length, and a traceback would
    # print all of it if the error were chained to the one that datetime raises.
    with pytest.raises(ValueError) as error:
        record_of(files=[{"upload_time_iso_8601": "x" * 100_000}])
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__
    assert len(str(error.value)) < 200


def test_release_record_shows_a_bad_value_without_recursing_into_it():
    # A bad value can be nested deeper than repr() can show. The error must still be
    # a ValueError that says where the value is.
    deep: list = []
    for _ in range(300_000):
        deep = [deep]
    with pytest.raises(ValueError, match=r"info\.summary must be a string or null"):
        record_of({"summary": deep})


def test_release_record_shows_only_the_start_of_a_long_bad_value():
    with pytest.raises(ValueError) as error:
        record_of({"summary": ["x" * 1000] * 1000})
    assert len(str(error.value)) < 200


# ---- the record as data


def test_release_record_does_not_change_its_inputs():
    document = sample()
    before = copy.deepcopy(document)
    release_record(
        "sampleproject", 1, document["info"], document["urls"], observed_at=OBSERVED_AT
    )
    assert document == before


def test_release_record_values_are_plain_data_that_sqlite_binds_by_column():
    # The release_records table is made from RECORD_COLUMNS, and a row is a record.
    record = record_of({"yanked": True, "yanked_reason": "broken build"})
    assert all(v is None or type(v) in (str, int, bool) for v in record.values())
    columns = ", ".join(RECORD_COLUMNS)
    marks = ", ".join(f":{column}" for column in RECORD_COLUMNS)
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute(f"CREATE TABLE release_records ({columns})")
        connection.execute(
            f"INSERT INTO release_records ({columns}) VALUES ({marks})", record
        )
        (row,) = connection.execute(f"SELECT {columns} FROM release_records")
    assert dict(zip(RECORD_COLUMNS, row, strict=True)) == record


# ---- a hostile text takes no longer than its length

# Each of these makes a naive regular expression do work in proportion to the square of
# the length: 20,000 characters took one a second or more, so 50,000 take it several
# seconds. The code under test takes a few milliseconds, and the limit is a second.
LONG = 50_000


@pytest.mark.parametrize(
    "text",
    [
        "a" * LONG,
        " " * LONG,
        "<" + "@" * LONG,
        "< " * (LONG // 2),
        "a@" * (LONG // 2),
        "a@" + "b." * (LONG // 2),
        "a." * (LONG // 2) + "@",
        ",".join(["a"] * (LONG // 2)),
    ],
    ids=[
        "one long word",
        "spaces",
        "bracket then at signs",
        "brackets and spaces",
        "repeated at signs",
        "domain of single letters",
        "dotted word then at sign",
        "commas",
    ],
)
def test_scrubbing_takes_time_in_proportion_to_the_text(text):
    started = time.perf_counter()
    record_of(
        {
            "summary": text,
            "license": text,
            "home_page": text,
            "author": text,
            "author_email": text,
            "project_urls": {"label": text},
        }
    )
    assert time.perf_counter() - started < 1.0
