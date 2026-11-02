import time

import pytest

from obskit.emails import (
    holds_address,
    holds_an_at,
    is_mail_link,
    strip_emails,
    without_addresses,
)
from obskit.testing import (
    ADDRESS_FORMS,
    HOSTILE_TEXTS,
    NOT_ADDRESSES,
    NPM_URL,
    WHITE_SPACE_TEXTS,
)

# ---- strip_emails


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
        # An address goes whole when a line break runs through it.
        ("Jane Doe <jane [at] example\n[dot] com>", "Jane Doe"),
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


@pytest.mark.parametrize("address", ADDRESS_FORMS)
def test_strip_emails_takes_out_an_address_of_any_form(address):
    assert strip_emails(address) is None
    assert strip_emails(f"<{address}>") is None
    assert strip_emails(f"Jane Doe <{address}>") == "Jane Doe"
    assert strip_emails(f"Jane Doe {address}") == "Jane Doe"
    assert strip_emails(f"Jane ({address})") == "Jane"
    assert strip_emails(f"Jane<{address}>Doe") == "Jane Doe"
    assert strip_emails(f"Jane Doe <{address}>, Bob {address}") == "Jane Doe, Bob"


@pytest.mark.parametrize("text", NOT_ADDRESSES)
def test_strip_emails_keeps_a_name_that_only_looks_like_an_address(text):
    assert strip_emails(text) == text


@pytest.mark.parametrize(("text", "kept"), WHITE_SPACE_TEXTS)
def test_strip_emails_takes_out_an_address_with_white_space_of_any_kind(text, kept):
    assert strip_emails(text) == kept


@pytest.mark.parametrize(
    "text",
    [
        "jane [at) x.org",
        "jane (at] x.org",
        "jane {at> x.org",
        "jane <at} x.org",
        "jane%40x [dot) org",
        # The word is not alone in its brackets.
        "jane [at home] x.org",
        "jane [that] x.org",
    ],
)
def test_strip_emails_keeps_a_word_that_is_not_alone_in_a_matching_pair_of_brackets(
    text,
):
    assert strip_emails(text) == text


# ---- holds_address


@pytest.mark.parametrize("address", ["jane@x.org", *ADDRESS_FORMS])
def test_holds_address_finds_an_address_of_any_form(address):
    assert holds_address(address)
    assert holds_address(f"see {address} for more")
    assert holds_address(f"Copyright Jane <{address}>")


@pytest.mark.parametrize(("text", "kept"), WHITE_SPACE_TEXTS)
def test_holds_address_finds_an_address_with_white_space_of_any_kind(text, kept):
    assert holds_address(text)
    assert not holds_address(kept)


@pytest.mark.parametrize("text", NOT_ADDRESSES)
def test_holds_address_finds_none_in_a_text_that_only_looks_like_one(text):
    assert not holds_address(text)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("a.b-c_d+tag%x@sub.example.co.uk", True),
        # An address in any script: a name with a diaeresis, a domain with one, and a
        # Cyrillic top-level domain.
        ("jörg@example.org", True),
        ("jane@exämple.org", True),
        ("jane@example.рф", True),
        ("Decorator @cache for functions", False),
        ("user@localhost", False),  # no domain with a dot
        ("x@y.z", False),  # and a top-level domain is two letters or more
        ("https://twitter.com/@jane", False),
        ("", False),
    ],
)
def test_holds_address_forms(text, expected):
    assert holds_address(text) is expected


# ---- without_addresses


@pytest.mark.parametrize("address", ["jane@x.org", *ADDRESS_FORMS])
def test_without_addresses_takes_out_an_address_of_any_form(address):
    assert without_addresses(f"see {address} for more") == "see for more"
    assert without_addresses(f"Copyright Jane <{address}>") == "Copyright Jane"
    assert without_addresses(address) is None


@pytest.mark.parametrize(("text", "kept"), WHITE_SPACE_TEXTS)
def test_without_addresses_takes_out_an_address_with_white_space_of_any_kind(
    text, kept
):
    assert without_addresses(text) == kept


@pytest.mark.parametrize("text", NOT_ADDRESSES)
def test_without_addresses_keeps_a_text_that_only_looks_like_an_address(text):
    assert without_addresses(text) == text


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("contact jane@x.org", "contact"),
        ("jane@x.org is the contact", "is the contact"),
        ("write to jane@x.org now", "write to now"),
        ("Copyright Jane < jane@x.org >", "Copyright Jane"),
        ("Jane <jane@x.org> and Bob <bob@y.com>", "Jane and Bob"),
        ("jane@x.org bob@y.com", None),
        # Only the white space beside an address goes, so a text keeps its layout.
        (
            "Copyright 2020 Jane <jane@x.org>\nAll rights reserved.",
            "Copyright 2020 Jane\nAll rights reserved.",
        ),
        ("one  two  jane@x.org  three", "one  two  three"),
        # A line break before an address stays, so taking it out never joins two lines.
        ("one\njane@x.org two", "one\n two"),
        # A text with no address comes back as it was, white space and all.
        ("  Line one.\n\nLine two.  \n", "  Line one.\n\nLine two.  \n"),
        ("", None),
        (None, None),
    ],
)
def test_without_addresses_forms(text, expected):
    assert without_addresses(text) == expected


@pytest.mark.parametrize("text", ["me@<a@b.cc>x.org", "hello me@<a@b.cc>x.org"])
def test_without_addresses_drops_a_text_that_taking_out_an_address_turns_into_one(
    text,
):
    # Taking out `<a@b.cc>` joins `me@` to `x.org`. Only a text made to do that is like
    # it, so the whole of it goes and no address is left.
    assert without_addresses(text) is None


# ---- is_mail_link


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("mailto:jane@x.org", True),
        ("MAILTO:Jane@X.org", True),  # a scheme is not case sensitive
        ("mailto:", True),  # no address, and still a mail link
        ("  mailto:jane@x.org", True),  # blanks before the scheme
        # An address in another link is for holds_address to find.
        ("https://example.org/contact?to=jane@x.org", False),
        ("https://example.org/mailto:jane", False),
        (NPM_URL, False),
        ("", False),
    ],
)
def test_is_mail_link(url, expected):
    assert is_mail_link(url) is expected


def test_a_url_with_a_percent_40_and_no_name_before_it_is_kept_whole():
    # A stream keeps a link whole or not at all, and this one, a scoped npm package's,
    # is neither a mail link nor a link that holds an address.
    assert not is_mail_link(NPM_URL)
    assert not holds_address(NPM_URL)
    assert without_addresses(NPM_URL) == NPM_URL


# ---- holds_an_at


@pytest.mark.parametrize(
    ("opening", "closing"), [("[", "]"), ("(", ")"), ("{", "}"), ("<", ">")]
)
@pytest.mark.parametrize("word", ["at", "AT", "At", "aT"])
def test_holds_an_at_in_each_pair_of_brackets_in_any_case(opening, closing, word):
    assert holds_an_at(f"jane.doe {opening}{word}{closing} exa...")


@pytest.mark.parametrize(
    "text",
    [
        "'jane.doe@exa...",
        "'jane.doe%40exa...",
        "@",
        "%40",
        # Stricter than holds_address, which finds no address here: a value cut short
        # can leave a `%40` with no name before it as well.
        NPM_URL,
    ],
)
def test_holds_an_at_sign_or_a_percent_40(text):
    assert holds_an_at(text)


@pytest.mark.parametrize(
    ("blank", "escape"),
    [
        (" ", " "),  # a space is the one blank that Python writes as it is
        ("\n", r"\n"),
        ("\r", r"\r"),
        ("\t", r"\t"),
        ("\xa0", r"\xa0"),  # a no-break space
        ("\u2003", r"\u2003"),  # an em space
    ],
)
def test_holds_an_at_with_blanks_in_its_brackets_as_they_are_or_as_python_shows_them(
    blank, escape
):
    text = f"jane.doe [{blank}at{blank}] exa"
    assert holds_an_at(text)
    # An error message shows a value as Python writes it, with each blank but a space
    # written as an escape, which no longer reads as a blank.
    shown = repr(text)
    assert f"[{escape}at{escape}]" in shown
    assert holds_an_at(shown)


@pytest.mark.parametrize(
    "text",
    [
        "look at numpy",
        "jane at x dot org",
        "cat",
        "[ab]",
        "[cat]",
        "[ate]",
        "[that]",
        "[at home]",
        "[a t]",
        # Brackets that do not make a pair.
        "[at)",
        "(at]",
        "{at>",
        "<at}",
        # A backslash that writes no blank.
        r"[\qat]",
        "%4",
        "",
    ],
)
def test_holds_no_at_in_a_text_without_one(text):
    assert not holds_an_at(text)


# ---- a hostile text


@pytest.mark.parametrize(
    "text", [pytest.param(text, id=name) for text, name in HOSTILE_TEXTS]
)
def test_a_hostile_text_takes_each_function_well_under_a_second(text):
    # Each text makes a naive pattern take seconds or never end, and these take
    # milliseconds.
    started = time.perf_counter()
    holds_address(text)
    without_addresses(text)
    strip_emails(text)
    is_mail_link(text)
    holds_an_at(text)
    assert time.perf_counter() - started < 1.0
