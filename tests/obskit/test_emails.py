import pytest

from obskit.emails import strip_emails
from obskit.testing import ADDRESS_FORMS, NOT_ADDRESSES, WHITE_SPACE_TEXTS


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
