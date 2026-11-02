import hashlib
import json
import re
import sqlite3
import time
from contextlib import closing
from pathlib import Path

import pytest

from obskit.emails import holds_address
from obskit.testing import ADDRESS_FORMS, HOSTILE_TEXTS
from osv_pypi.records import VERSION_COLUMNS, parse_record

FIXTURES = Path(__file__).parent / "fixtures"
MIB = 1024 * 1024
GHSA = "GHSA-4m7q-9xv2-hc3p"
MAL = "MAL-2026-40017"
PYSEC = "PYSEC-2019-311"

# In `changed`, the value that takes a key out instead of setting it.
DROP = object()


def raw(name: str) -> bytes:
    """The bytes of a record in fixtures/. Each is made by hand in the shape of OSV's
    records, and none of them is a real one."""
    return (FIXTURES / f"{name}.json").read_bytes()


def document(name: str) -> dict:
    """A record from fixtures/, read afresh so that a test may edit it."""
    return json.loads(raw(name))


def body(record: object) -> bytes:
    """A record as the bytes that OSV serves."""
    return json.dumps(record).encode()


def row_of(record: dict) -> dict:
    """The row of an edited record, read as the advisory that its own id names."""
    return parse_record(body(record), advisory_id=record["id"])


def changed(name: str, path: tuple[str | int, ...], value: object) -> bytes:
    """The body of the record `name` with the value at `path` set to `value`, or taken
    out when `value` is DROP."""
    record = document(name)
    *parents, last = path
    holder = record
    for key in parents:
        holder = holder[key]
    if value is DROP:
        del holder[last]
    else:
        holder[last] = value
    return body(record)


def padded(name: str, size: int) -> bytes:
    """The record `name` followed by spaces, which JSON allows, to make `size` bytes."""
    data = raw(name)
    return data + b" " * (size - len(data))


def texts(row: dict) -> list[str]:
    """Every value of a row that is a string."""
    return [value for value in row.values() if isinstance(value, str)]


def strict_json(text: str) -> object:
    """`text` read as JSON that any reader takes: NaN and Infinity are refused."""

    def refuse(constant: str) -> None:
        raise ValueError(f"{constant} is not JSON")

    return json.loads(text, parse_constant=refuse)


# ---- the columns


def test_version_columns_are_in_the_documented_order():
    assert VERSION_COLUMNS == (
        "id",
        "kind",
        "modified",
        "modified_key",
        "published",
        "withdrawn",
        "summary",
        "details_sha256",
        "details_bytes",
        "details_words",
        "aliases",
        "related",
        "severity",
        "affected",
        "refs",
        "database_specific",
        "schema_version",
    )


# ---- the three kinds of record


def test_ghsa_record_keeps_only_pypi_entries():
    record = document("ghsa")
    # The fixture is only a test if it has an entry for another ecosystem.
    ecosystems = [entry["package"]["ecosystem"] for entry in record["affected"]]
    assert ecosystems == ["PyPI", "npm"]

    row = parse_record(raw("ghsa"), advisory_id=GHSA)

    assert list(row) == list(VERSION_COLUMNS)
    assert row == {
        "id": GHSA,
        "kind": "vulnerability",
        "modified": "2025-03-04T17:20:11.482903Z",
        "modified_key": "2025-03-04T17:20:11.482903000Z",
        "published": "2024-11-18T21:30:52Z",
        "withdrawn": None,
        "summary": "example-archive writes files outside the target directory",
        "details_sha256": hashlib.sha256(record["details"].encode()).hexdigest(),
        "details_bytes": 150,
        "details_words": 26,
        "aliases": '["CVE-2024-52811"]',
        "related": None,
        "severity": (
            '[{"score":"CVSS:3.1/AV:N/AC:L/PR:N/UI:R/S:U/C:N/I:H/A:N",'
            '"type":"CVSS_V3"},'
            '{"score":"CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:P/VC:N/VI:H/VA:N/SC:N/SI:N/SA:N",'
            '"type":"CVSS_V4"}]'
        ),
        # The events of the range keep their order: two intervals, one after the other.
        "affected": (
            '[{"name":"example-archive","name_raw":"example-archive",'
            '"ranges":[{"events":[{"introduced":"0"},{"fixed":"1.10.1"},'
            '{"introduced":"2.0.0"},{"fixed":"2.4.1"}],'
            '"repo":null,"type":"ECOSYSTEM"}],'
            '"versions":["1.10.0","1.9.2","2.0.0","2.4.0"]}]'
        ),
        "refs": (
            '[{"type":"WEB","url":"https://github.com/example/example-archive/'
            'security/advisories/GHSA-4m7q-9xv2-hc3p"},'
            '{"type":"ADVISORY","url":"https://nvd.nist.gov/vuln/detail/CVE-2024-52811"},'
            '{"type":"PACKAGE","url":"https://github.com/example/example-archive"}]'
        ),
        "database_specific": (
            '{"cwe_ids":["CWE-22"],"github_reviewed":true,'
            '"github_reviewed_at":"2024-11-18T21:30:52Z",'
            '"nvd_published_at":"2024-11-18T19:15:06Z","severity":"HIGH"}'
        ),
        "schema_version": "1.7.5",
    }
    # Nothing of the npm entry is left.
    assert not [text for text in texts(row) if "npm" in text or "3.0.7" in text]


def test_mal_record():
    record = document("mal")
    # The fixture is only a test if it has what a row drops.
    assert set(record["database_specific"]) == {"iocs", "malicious-packages-origins"}
    assert "@" in record["credits"][0]["contact"][0]

    row = parse_record(raw("mal"), advisory_id=MAL)

    assert row == {
        "id": MAL,
        "kind": "malware",
        "modified": "2026-08-12T11:02:45.913726404Z",
        "modified_key": "2026-08-12T11:02:45.913726404Z",
        "published": "2026-08-12T09:14:27Z",
        "withdrawn": None,
        "summary": "Malicious code in example-typosquat (PyPI)",
        "details_sha256": hashlib.sha256(record["details"].encode()).hexdigest(),
        "details_bytes": 159,
        "details_words": 26,
        "aliases": None,
        "related": None,
        "severity": None,
        # The entry lists its versions and has no ranges.
        "affected": (
            '[{"name":"example-typosquat","name_raw":"example-typosquat",'
            '"ranges":[],"versions":["0.0.1","0.0.2","1.0.0"]}]'
        ),
        "refs": (
            '[{"type":"EVIDENCE",'
            '"url":"https://reports.example.org/example-typosquat/1.0.0"}]'
        ),
        "database_specific": None,
        "schema_version": "1.9.2",
    }
    # The origins, the IOCs, the entry's indicators and the credits are gone.
    gone = [
        "malicious-packages-origins",
        "pypi/2026-08-example",
        "0b1c2d3e4f5a6b7c",
        "payload.example.net",
        "stage2.exe",
        "6f0d3c1e9a8b7c5d",
        "CWE-506",
        "Example Scanner",
        "research@scanner.example.com",
    ]
    assert not [(text, mark) for text in texts(row) for mark in gone if mark in text]


def test_pysec_record_with_git_range_and_withdrawn():
    record = document("pysec")

    row = parse_record(raw("pysec"), advisory_id=PYSEC)

    assert row == {
        "id": PYSEC,
        "kind": "vulnerability",
        "modified": "2026-06-10T17:01:19.825100252Z",
        "modified_key": "2026-06-10T17:01:19.825100252Z",
        "published": "2019-05-14T18:29:00Z",
        "withdrawn": "2024-11-22T04:37:04Z",
        "summary": None,
        "details_sha256": hashlib.sha256(record["details"].encode()).hexdigest(),
        "details_bytes": 113,
        "details_words": 21,
        "aliases": '["CVE-2019-11972","GHSA-8r3w-x9vq-2mf6"]',
        "related": None,
        "severity": None,
        # The GIT range keeps its repo and its commits, and the ECOSYSTEM range has no
        # repo.
        "affected": (
            '[{"name":"example-imaging","name_raw":"example-imaging",'
            '"ranges":[{"events":[{"introduced":"0"},'
            '{"fixed":"5f1c9e0a7d3b2c4e6f8a1b3c5d7e9f0a2b4c6d8e"}],'
            '"repo":"https://github.com/example/example-imaging","type":"GIT"},'
            '{"events":[{"introduced":"0"},{"fixed":"6.2.1"}],'
            '"repo":null,"type":"ECOSYSTEM"}],'
            '"versions":["6.0.0","6.1.0","6.2.0"]}]'
        ),
        "refs": (
            '[{"type":"ADVISORY","url":"https://nvd.nist.gov/vuln/detail/CVE-2019-11972"},'
            '{"type":"FIX","url":"https://github.com/example/example-imaging/commit/'
            '5f1c9e0a7d3b2c4e6f8a1b3c5d7e9f0a2b4c6d8e"}]'
        ),
        "database_specific": None,
        "schema_version": "1.7.3",
    }


def test_a_record_with_only_what_it_needs():
    record = {
        "id": GHSA,
        "modified": "2025-03-04T17:20:11Z",
        "affected": [{"package": {"ecosystem": "PyPI", "name": "example-archive"}}],
    }
    assert row_of(record) == {
        "id": GHSA,
        "kind": "vulnerability",
        "modified": "2025-03-04T17:20:11Z",
        "modified_key": "2025-03-04T17:20:11.000000000Z",
        "published": None,
        "withdrawn": None,
        "summary": None,
        "details_sha256": None,
        "details_bytes": None,
        "details_words": None,
        "aliases": None,
        "related": None,
        "severity": None,
        "affected": (
            '[{"name":"example-archive","name_raw":"example-archive",'
            '"ranges":[],"versions":[]}]'
        ),
        "refs": None,
        "database_specific": None,
        "schema_version": None,
    }


def test_keys_that_a_row_does_not_name_are_dropped_unread():
    # OSV's schema gains keys, and a key that a row does not keep is never read, so a
    # new one cannot stop a record.
    record = document("pysec")
    record["credits"] = [{"name": "Jane Doe", "contact": ["mailto:jane@x.org"]}]
    record["a_later_key"] = [[{"of": ["any", "shape"]}]]
    entry = record["affected"][0]
    entry["ecosystem_specific"] = {"affects": {"functions": ["imaging.decode"]}}
    entry["severity"] = [{"type": "CVSS_V3", "score": "anything"}]
    entry["ranges"][0]["database_specific"] = {"versions": [{"anything": 1}]}
    assert row_of(record) == parse_record(raw("pysec"), advisory_id=PYSEC)


def test_an_entry_for_another_ecosystem_is_not_read():
    record = document("ghsa")
    record["affected"][1] = {"package": {"ecosystem": "npm"}, "ranges": "x"}
    record["affected"].append({"package": None, "versions": [1, 2]})
    record["affected"].append({"ranges": [{"type": "GIT", "events": "x"}]})
    assert row_of(record) == parse_record(raw("ghsa"), advisory_id=GHSA)


# ---- no email address


@pytest.mark.parametrize("address", ["jane@x.org", "jane [at] x [dot] org"])
def test_addresses_never_survive(address):
    record = document("pysec")
    references = record["references"]
    record["summary"] = f"Reported by {address} in 2019"
    record["references"] = [
        *references,
        {"type": "REPORT", "url": f"mailto:{address}"},
        {"type": "WEB", "url": f"https://lists.example.org/thread?from={address}"},
    ]
    record["affected"][0]["ranges"][0]["repo"] = "git@github.com:foo/bar.git"

    row = row_of(record)

    assert row["summary"] == "Reported by in 2019"
    assert json.loads(row["refs"]) == references
    assert json.loads(row["affected"])[0]["ranges"][0]["repo"] is None
    assert not [text for text in texts(row) if "jane" in text or holds_address(text)]

    # A version is kept as it is, so an address in one refuses the whole record.
    hidden = document("pysec")
    hidden["affected"][0]["versions"].append(f"6.2.0+{address}")
    with pytest.raises(ValueError, match="affected holds an email address"):
        row_of(hidden)


@pytest.mark.parametrize("address", ADDRESS_FORMS)
def test_an_address_of_any_form_is_taken_out_of_what_is_filtered(address):
    record = document("pysec")
    references = record["references"]
    record["summary"] = f"Reported by {address} in 2019"
    record["references"] = [
        {"type": "WEB", "url": f"https://example.org/contact?to={address}"},
        *references,
    ]
    record["affected"][0]["ranges"][0]["repo"] = f"https://example.org/{address}.git"

    row = row_of(record)

    assert row["summary"] == "Reported by in 2019"
    assert json.loads(row["refs"]) == references
    assert json.loads(row["affected"])[0]["ranges"][0]["repo"] is None
    assert not [text for text in texts(row) if holds_address(text)]


# The places where a record's text is kept as it is, each as a path in the GHSA record
# and the column that the text lands in. Taking an address out of such a text would
# change what it names, so a record with an address there is refused whole.
KEPT_AS_IT_IS = [
    pytest.param(("affected", 0, "versions", 0), "affected", id="a version"),
    pytest.param(
        ("affected", 0, "ranges", 0, "events", 1, "fixed"), "affected", id="an event"
    ),
    pytest.param(("affected", 0, "package", "name"), "affected", id="a package name"),
    pytest.param(("aliases", 0), "aliases", id="an alias"),
    pytest.param(("severity", 0, "score"), "severity", id="a score"),
    pytest.param(
        ("database_specific", "severity"), "database_specific", id="a database value"
    ),
    pytest.param(
        ("database_specific", "cwe_ids", 0), "database_specific", id="a CWE id"
    ),
    pytest.param(("schema_version",), "schema_version", id="the schema version"),
]


@pytest.mark.parametrize(("path", "column"), KEPT_AS_IT_IS)
@pytest.mark.parametrize("address", ADDRESS_FORMS)
def test_an_address_in_a_text_kept_as_it_is_refuses_the_record(path, column, address):
    with pytest.raises(ValueError, match=f"{column} holds an email address") as caught:
        parse_record(changed("ghsa", path, f"1.0 {address}"), advisory_id=GHSA)
    # The message names the column, and never shows the address.
    assert not holds_address(str(caught.value))


def test_an_address_that_a_line_break_runs_through_is_found_inside_json():
    version = "jane [at] example\n[dot] com"
    # Written as JSON, the line break is `\n`, and the address no longer reads as one,
    # so the guard reads each string before the column is written.
    assert holds_address(version)
    assert not holds_address(json.dumps([version], ensure_ascii=False))
    with pytest.raises(ValueError, match="affected holds an email address"):
        parse_record(
            changed("ghsa", ("affected", 0, "versions", 0), version), advisory_id=GHSA
        )


def test_an_address_that_json_writes_into_a_column_is_found():
    version = "\x1f@x.org"
    # The string holds no address, but JSON writes the control character as `\u001f`,
    # and the column would then read as one. What is published is the column.
    assert not holds_address(version)
    assert holds_address(json.dumps([version], ensure_ascii=False))
    with pytest.raises(ValueError, match="affected holds an email address"):
        parse_record(
            changed("ghsa", ("affected", 0, "versions", 0), version), advisory_id=GHSA
        )


@pytest.mark.parametrize(
    "reference",
    [
        {"type": "REPORT", "url": "mailto:security@example.org"},
        {"type": "REPORT", "url": "MAILTO:security@example.org"},  # any case
        {"type": "REPORT", "url": "mailto:"},  # no address, and still a mail link
        {"type": "WEB", "url": "https://lists.example.org/t?from=jane@x.org"},
        {"type": "jane@x.org", "url": "https://example.org/"},  # in the type
    ],
)
def test_references_drop_a_mail_link_and_an_entry_with_an_address(reference):
    record = document("ghsa")
    references = record["references"]
    record["references"] = [references[0], reference, *references[1:]]
    assert json.loads(row_of(record)["refs"]) == references


@pytest.mark.parametrize(
    "references",
    [[{"type": "REPORT", "url": "mailto:security@example.org"}], [], None, DROP],
)
def test_references_with_nothing_left_give_none(references):
    data = changed("ghsa", ("references",), references)
    assert parse_record(data, advisory_id=GHSA)["refs"] is None


@pytest.mark.parametrize(
    ("repo", "kept"),
    [
        ("git@github.com:foo/bar.git", None),
        ("git+ssh://git@github.com/foo/bar.git", None),
        (
            "https://github.com/example/example-imaging",
            "https://github.com/example/example-imaging",
        ),
        (None, None),
    ],
)
def test_a_repo_is_kept_whole_or_not_at_all(repo, kept):
    # Taking the address out of `git@github.com:foo/bar.git` would leave a repo that
    # points nowhere, in a part that is never replaced.
    data = changed("pysec", ("affected", 0, "ranges", 0, "repo"), repo)
    (entry,) = json.loads(parse_record(data, advisory_id=PYSEC)["affected"])
    assert entry["ranges"][0]["repo"] == kept


@pytest.mark.parametrize(
    "text", [pytest.param(text, id=name) for text, name in HOSTILE_TEXTS]
)
def test_a_hostile_text_takes_time_in_proportion_to_its_length(text):
    # Each place that the filters read and the guard reads again. Such a text makes a
    # naive pattern take seconds, and the kit's takes milliseconds.
    record = document("pysec")
    record["summary"] = text
    record["references"][0]["url"] = text
    record["affected"][0]["ranges"][0]["repo"] = text
    started = time.perf_counter()
    row_of(record)
    assert time.perf_counter() - started < 1.0


# ---- details


@pytest.mark.parametrize(
    ("details", "facts"),
    [
        pytest.param(
            # 59 characters: the e with an acute accent takes two bytes, the dash three.
            "Crafted input crashes the décoder.\n\nFixed in 2.0 – upgrade.",
            (
                "fe3f391abeceb5061222ee7629e286d68ea852cc1d5e50438189ecf7df8a5821",
                62,
                10,
            ),
            id="a text",
        ),
        pytest.param(
            "  \n\t", (hashlib.sha256(b"  \n\t").hexdigest(), 4, 0), id="white space"
        ),
        pytest.param("", (None, 0, 0), id="empty"),
        pytest.param(None, (None, None, None), id="null"),
        pytest.param(DROP, (None, None, None), id="absent"),
    ],
)
def test_details_facts(details, facts):
    row = parse_record(changed("ghsa", ("details",), details), advisory_id=GHSA)
    assert (row["details_sha256"], row["details_bytes"], row["details_words"]) == facts
    # The text has no column, and no value holds any of it.
    assert "details" not in row
    assert not [text for text in texts(row) if "décoder" in text]


def test_details_that_utf8_cannot_hold_are_refused():
    # A lone surrogate is not text. It has no UTF-8 bytes to count or to fingerprint.
    with pytest.raises(ValueError, match=re.escape("details must be text that UTF-8")):
        parse_record(changed("ghsa", ("details",), "a\ud800b"), advisory_id=GHSA)


# ---- aliases, related and severity


def test_aliases_sorted_and_deduplicated():
    record = document("ghsa")
    record["aliases"] = ["PYSEC-2024-77", "CVE-2024-52811", "PYSEC-2024-77"]
    record["related"] = ["GHSA-2wxp-9c8r-hq4v", "CVE-2024-52810", "CVE-2024-52810"]
    row = row_of(record)
    assert row["aliases"] == '["CVE-2024-52811","PYSEC-2024-77"]'
    assert row["related"] == '["CVE-2024-52810","GHSA-2wxp-9c8r-hq4v"]'


@pytest.mark.parametrize("key", ["aliases", "related"])
@pytest.mark.parametrize("empty", [[], None, DROP])
def test_no_aliases_give_none(key, empty):
    record = document("ghsa")
    record[key] = empty
    if empty is DROP:
        del record[key]
    assert row_of(record)[key] is None


def test_an_alias_of_128_characters_is_kept():
    alias = "A-" + "1" * 126
    row = parse_record(changed("ghsa", ("aliases",), [alias]), advisory_id=GHSA)
    assert json.loads(row["aliases"]) == [alias]


@pytest.mark.parametrize("empty", [[], None, DROP])
def test_no_severity_gives_none(empty):
    data = changed("ghsa", ("severity",), empty)
    assert parse_record(data, advisory_id=GHSA)["severity"] is None


def test_severity_keeps_its_type_and_score_only():
    record = document("ghsa")
    record["severity"][0]["source"] = "a key that a row does not name"
    expected = parse_record(raw("ghsa"), advisory_id=GHSA)["severity"]
    assert row_of(record)["severity"] == expected


# ---- affected


@pytest.mark.parametrize(
    ("name_raw", "name"),
    [
        ("Foo_Bar", "foo-bar"),
        ("zope.interface", "zope-interface"),
        ("Django", "django"),
        ("a-_.-b", "a-b"),
        ("example-archive", "example-archive"),
    ],
)
def test_package_names_canonical_with_raw_kept(name_raw, name):
    data = changed("ghsa", ("affected", 0, "package", "name"), name_raw)
    (entry,) = json.loads(parse_record(data, advisory_id=GHSA)["affected"])
    assert (entry["name"], entry["name_raw"]) == (name, name_raw)


@pytest.mark.parametrize("key", ["introduced", "fixed", "last_affected", "limit"])
def test_each_kind_of_event_is_kept(key):
    data = changed("ghsa", ("affected", 0, "ranges", 0, "events", 1), {key: "1.10.1"})
    (entry,) = json.loads(parse_record(data, advisory_id=GHSA)["affected"])
    assert entry["ranges"][0]["events"][1] == {key: "1.10.1"}


@pytest.mark.parametrize("range_type", ["ECOSYSTEM", "SEMVER", "GIT"])
def test_each_type_of_range_is_kept(range_type):
    data = changed("ghsa", ("affected", 0, "ranges", 0, "type"), range_type)
    (entry,) = json.loads(parse_record(data, advisory_id=GHSA)["affected"])
    assert entry["ranges"][0]["type"] == range_type


def test_every_pypi_entry_is_kept_in_order():
    record = document("ghsa")
    second = json.loads(json.dumps(record["affected"][0]))
    second["package"]["name"] = "Example_Archive_Two"
    record["affected"].append(second)
    names = [entry["name"] for entry in json.loads(row_of(record)["affected"])]
    assert names == ["example-archive", "example-archive-two"]


# ---- database_specific


def test_database_specific_keeps_five_keys_with_plain_values():
    record = document("ghsa")
    record["database_specific"] = {
        "severity": "LOW",
        "cwe_ids": [],
        "github_reviewed": False,
        # null is a JSON scalar, and GitHub writes it when NVD has no date.
        "github_reviewed_at": None,
        "nvd_published_at": None,
        "source": "https://github.com/github/advisory-database",
        "iocs": {"domains": ["payload.example.net"]},
    }
    assert row_of(record)["database_specific"] == (
        '{"cwe_ids":[],"github_reviewed":false,"github_reviewed_at":null,'
        '"nvd_published_at":null,"severity":"LOW"}'
    )


@pytest.mark.parametrize(
    "value",
    [
        b'{"level": "HIGH"}',
        b'["HIGH", 3]',
        b'[["HIGH"]]',
        # JSON has no such numbers. json reads them as floats that are not finite, and
        # would write them back as NaN and Infinity, which no JSON reader takes.
        b"NaN",
        b"Infinity",
        b"-Infinity",
        b"1e400",
    ],
)
def test_database_specific_drops_a_value_that_is_not_plain(value):
    data = raw("ghsa").replace(b'"severity": "HIGH"', b'"severity": ' + value)
    assert data != raw("ghsa")
    row = parse_record(data, advisory_id=GHSA)
    kept = strict_json(row["database_specific"])
    assert isinstance(kept, dict)
    assert set(kept) == {
        "cwe_ids",
        "github_reviewed",
        "github_reviewed_at",
        "nvd_published_at",
    }


@pytest.mark.parametrize(
    "value", [{"source": "https://example.org"}, {"severity": {"a": 1}}, {}, None, DROP]
)
def test_database_specific_with_nothing_left_gives_none(value):
    data = changed("ghsa", ("database_specific",), value)
    assert parse_record(data, advisory_id=GHSA)["database_specific"] is None


# ---- a record that is refused


REFUSALS = [
    pytest.param(
        lambda: raw("ghsa"),
        "GHSA-2wxp-9c8r-hq4v",
        re.escape(f"id must be 'GHSA-2wxp-9c8r-hq4v', not '{GHSA}'"),
        id="an id mismatch",
    ),
    pytest.param(
        lambda: changed("ghsa", ("modified",), DROP),
        GHSA,
        re.escape("modified must be an RFC 3339 UTC time, not None"),
        id="no modified",
    ),
    pytest.param(
        lambda: changed("ghsa", ("affected",), document("ghsa")["affected"][0]),
        GHSA,
        re.escape("affected must be a list, not {"),
        id="an affected that is not a list",
    ),
    pytest.param(
        lambda: changed("ghsa", ("affected", 0), DROP),
        GHSA,
        re.escape("affected must be a list that holds an entry for PyPI, not [{"),
        id="no PyPI entry",
    ),
    pytest.param(
        lambda: changed("ghsa", ("affected", 0, "ranges", 0, "type"), "OTHER"),
        GHSA,
        re.escape(
            "affected[0].ranges[0].type must be ECOSYSTEM, SEMVER or GIT, not 'OTHER'"
        ),
        id="a range type OTHER",
    ),
    pytest.param(
        lambda: changed(
            "ghsa",
            ("affected", 0, "ranges", 0, "events", 1),
            {"fixed": "1.10.1", "introduced": "2.0.0"},
        ),
        GHSA,
        re.escape(
            "affected[0].ranges[0].events[1] must be an object with one key out of "
            "introduced, fixed, last_affected and limit, "
            "not {'fixed': '1.10.1', 'introduced': '2.0.0'}"
        ),
        id="an event with two keys",
    ),
    pytest.param(
        lambda: padded("ghsa", 4 * MIB + 1),
        GHSA,
        re.escape("the body is 4,194,305 bytes, over the limit of 4,194,304"),
        id="an oversized body",
    ),
    pytest.param(
        # json reads 10,000 levels, so the body is refused as a list, shown three levels
        # deep. Where json reads less, json refuses it, and that is a ValueError too.
        lambda: b"[" * 10_000 + b"]" * 10_000,
        GHSA,
        r"the body (must be a JSON object, not \[\[\[\[\.\.\.\]\]\]\]$|is nested too)",
        id="nesting 10,000 deep",
    ),
]


@pytest.mark.parametrize(("make_body", "advisory_id", "reason"), REFUSALS)
def test_parse_record_refuses(make_body, advisory_id, reason):
    with pytest.raises(ValueError, match=reason) as caught:
        parse_record(make_body(), advisory_id=advisory_id)
    # The message names the advisory, and stays short whatever the record holds.
    assert str(caught.value).startswith(f"the record of '{advisory_id}': ")
    assert len(str(caught.value)) < 300


def test_parse_record_reads_a_body_of_exactly_4_mib():
    expected = parse_record(raw("ghsa"), advisory_id=GHSA)
    assert parse_record(padded("ghsa", 4 * MIB), advisory_id=GHSA) == expected


def test_the_size_is_checked_before_anything_else():
    # These bytes are not UTF-8 either, and the size is what refuses them.
    with pytest.raises(ValueError, match="over the limit"):
        parse_record(b"\xff" * (4 * MIB + 1), advisory_id=GHSA)


def test_a_body_nested_too_deeply_for_json_is_a_value_error():
    # 2 MB, and so under the limit, and far deeper than json reads.
    with pytest.raises(ValueError, match="the body is nested too deeply to read"):
        parse_record(b"[" * 1_000_000 + b"]" * 1_000_000, advisory_id=GHSA)


@pytest.mark.parametrize(
    ("data", "reason"),
    [
        (b"", "the body is not JSON"),
        (b"<html><body>502 Bad Gateway</body></html>", "the body is not JSON"),
        (b'{"id": "GHSA-4m7q-9xv2-hc3p",', "the body is not JSON"),
        (b"\xef\xbb\xbf{}", "the body is not JSON"),  # a byte order mark
        (b'{"id": "GHSA-\xff"}', "the body is not UTF-8"),
        (b'"GHSA-4m7q-9xv2-hc3p"', "the body must be a JSON object, not 'GHSA-"),
        (b"[]", re.escape("the body must be a JSON object, not []")),
    ],
)
def test_a_body_that_is_not_a_json_object_is_refused(data, reason):
    with pytest.raises(ValueError, match=reason):
        parse_record(data, advisory_id=GHSA)


# Each is a path in the GHSA record, the value put there (DROP takes it out), and the
# message, which names the path and shows the value.
WRONG_SHAPES = [
    (("id",), 5, f"id must be '{GHSA}', not 5"),
    (("modified",), 20250304, "modified must be an RFC 3339 UTC time, not 20250304"),
    (("modified",), "2025-03-04", "modified: '2025-03-04' is not an RFC 3339 UTC time"),
    (
        ("modified",),
        "2025-02-30T17:20:11Z",
        "modified: '2025-02-30T17:20:11Z' is not a time that exists",
    ),
    (("published",), 5, "published must be an RFC 3339 UTC time or null, not 5"),
    (("published",), "yesterday", "published: 'yesterday' is not an RFC 3339 UTC"),
    (
        ("withdrawn",),
        "2024-11-22T04:37:04+00:00",
        "withdrawn: '2024-11-22T04:37:04+00:00' is not an RFC 3339 UTC time",
    ),
    (("summary",), ["x"], "summary must be a string or null, not ['x']"),
    (
        ("details",),
        {"text": "x"},
        "details must be a string or null, not {'text': 'x'}",
    ),
    (("schema_version",), 1.7, "schema_version must be a string or null, not 1.7"),
    (("aliases",), "CVE-2024-52811", "aliases must be a list or null, not 'CVE-2024"),
    (("related",), {"id": 1}, "related must be a list or null, not {'id': 1}"),
    (
        ("aliases",),
        ["CVE-2024-52811", 7],
        "aliases[1] must be a non-empty string of at most 128 characters, not 7",
    ),
    (
        ("aliases",),
        [""],
        "aliases[0] must be a non-empty string of at most 128 characters, not ''",
    ),
    (
        ("related",),
        ["A-" + "1" * 127],
        "related[0] must be a non-empty string of at most 128 characters, not 'A-111",
    ),
    (("severity",), {"type": "CVSS_V3"}, "severity must be a list or null, not {'type"),
    (("severity", 0), "CVSS:3.1", "severity[0] must be an object, not 'CVSS:3.1'"),
    (("severity", 0, "type"), DROP, "severity[0].type must be a string, not None"),
    (("severity", 1, "score"), 9.8, "severity[1].score must be a string, not 9.8"),
    (("affected",), DROP, "affected must be a list, not None"),
    (("affected", 0), "example-archive", "affected[0] must be an object, not 'example"),
    (
        ("affected", 0, "package"),
        "example-archive",
        "affected[0].package must be an object or null, not 'example-archive'",
    ),
    (
        ("affected", 0, "package", "name"),
        "",
        "affected[0].package.name must be a non-empty string, not ''",
    ),
    (
        ("affected", 0, "package", "name"),
        DROP,
        "affected[0].package.name must be a non-empty string, not None",
    ),
    (
        ("affected", 0, "ranges"),
        {"type": "ECOSYSTEM"},
        "affected[0].ranges must be a list or null, not {'type': 'ECOSYSTEM'}",
    ),
    (
        ("affected", 0, "ranges", 0),
        "ECOSYSTEM",
        "affected[0].ranges[0] must be an object, not 'ECOSYSTEM'",
    ),
    (
        ("affected", 0, "ranges", 0, "type"),
        DROP,
        "affected[0].ranges[0].type must be ECOSYSTEM, SEMVER or GIT, not None",
    ),
    (
        ("affected", 0, "ranges", 0, "type"),
        "ecosystem",
        "affected[0].ranges[0].type must be ECOSYSTEM, SEMVER or GIT, not 'ecosystem'",
    ),
    (
        ("affected", 0, "ranges", 0, "repo"),
        5,
        "affected[0].ranges[0].repo must be a string or null, not 5",
    ),
    (
        ("affected", 0, "ranges", 0, "events"),
        DROP,
        "affected[0].ranges[0].events must be a list, not None",
    ),
    (
        ("affected", 0, "ranges", 0, "events", 0),
        {},
        "affected[0].ranges[0].events[0] must be an object with one key out of "
        "introduced, fixed, last_affected and limit, not {}",
    ),
    (
        ("affected", 0, "ranges", 0, "events", 0),
        {"introduce": "0"},
        "affected[0].ranges[0].events[0] must be an object with one key out of "
        "introduced, fixed, last_affected and limit, not {'introduce': '0'}",
    ),
    (
        ("affected", 0, "ranges", 0, "events", 0),
        ["introduced", "0"],
        "affected[0].ranges[0].events[0] must be an object with one key out of "
        "introduced, fixed, last_affected and limit, not ['introduced', '0']",
    ),
    (
        ("affected", 0, "ranges", 0, "events", 0, "introduced"),
        0,
        "affected[0].ranges[0].events[0].introduced must be a string, not 0",
    ),
    (
        ("affected", 0, "versions"),
        "1.10.0",
        "affected[0].versions must be a list or null, not '1.10.0'",
    ),
    (
        ("affected", 0, "versions", 1),
        1.9,
        "affected[0].versions[1] must be a string, not 1.9",
    ),
    (("references",), {"type": "WEB"}, "references must be a list or null, not {'typ"),
    (("references", 0), "https://x.org", "references[0] must be an object, not 'http"),
    (("references", 0, "url"), DROP, "references[0].url must be a string, not None"),
    (("references", 1, "type"), 3, "references[1].type must be a string, not 3"),
    (
        ("database_specific",),
        ["CWE-22"],
        "database_specific must be an object or null, not ['CWE-22']",
    ),
]


@pytest.mark.parametrize(("path", "value", "message"), WRONG_SHAPES)
def test_parse_record_names_the_path_and_the_value(path, value, message):
    with pytest.raises(ValueError) as caught:
        parse_record(changed("ghsa", path, value), advisory_id=GHSA)
    assert str(caught.value).startswith(f"the record of '{GHSA}': {message}")


@pytest.mark.parametrize(
    ("path", "column"),
    [
        (("summary",), "summary"),
        (("schema_version",), "schema_version"),
        (("affected", 0, "versions", 0), "affected"),
        (("aliases", 0), "aliases"),
        (("references", 0, "url"), "refs"),
    ],
)
def test_text_that_utf8_cannot_hold_is_refused(path, column):
    # JSON can write a lone surrogate as `\ud800` in bytes that are good UTF-8. SQLite
    # cannot store it, so a row with one would stop the run that stores it.
    data = changed("ghsa", path, "6.0\ud800")
    with pytest.raises(ValueError) as caught:
        parse_record(data, advisory_id=GHSA)
    assert str(caught.value) == (
        f"the record of '{GHSA}': {column} holds '6.0\\ud800', which UTF-8 cannot hold"
    )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("summary",), ["x" * 5000] * 50),
        (("modified",), "2025-" + "0" * 100_000),
        (("affected", 0, "ranges", 0, "type"), "OTHER" * 20_000),
        (("related",), ["A-" + "1" * 100_000]),
        (("id",), "GHSA-" + "x" * 100_000),
    ],
)
def test_an_error_stays_short_whatever_the_value(path, value):
    # A record is data from outside, and an error goes to a log and to an issue.
    with pytest.raises(ValueError) as caught:
        parse_record(changed("ghsa", path, value), advisory_id=GHSA)
    assert len(str(caught.value)) < 300


# ---- the row as data


def test_a_row_is_plain_data_that_sqlite_binds_by_column():
    # The stream's tables are made from VERSION_COLUMNS, and a row is bound by name.
    row = parse_record(raw("ghsa"), advisory_id=GHSA)
    assert all(value is None or type(value) in (str, int) for value in row.values())
    columns = ", ".join(VERSION_COLUMNS)
    marks = ", ".join(f":{column}" for column in VERSION_COLUMNS)
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute(f"CREATE TABLE versions ({columns})")
        connection.execute(f"INSERT INTO versions ({columns}) VALUES ({marks})", row)
        (stored,) = connection.execute(f"SELECT {columns} FROM versions")
    assert dict(zip(VERSION_COLUMNS, stored, strict=True)) == row


def test_a_json_column_writes_a_character_outside_ascii_as_it_is():
    record = document("ghsa")
    record["affected"][0]["package"]["name"] = "Zürich_Tools"
    record["references"][0]["url"] = "https://例え.jp/zürich"
    row = row_of(record)
    assert row["affected"].startswith(
        '[{"name":"zürich-tools","name_raw":"Zürich_Tools"'
    )
    assert row["refs"].startswith('[{"type":"WEB","url":"https://例え.jp/zürich"}')
