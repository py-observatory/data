"""The texts that the tests of records and of the Simple API share: the forms of an
email address that count as one, texts that look like an address and are not one, and
texts made to be slow to scrub."""

import pytest

# Each of these is one whole address. The at is `@`, `%40` or `at` in a pair of
# brackets, and a dot of the domain is `.` or `dot` in a pair of brackets.
ADDRESSES = [
    "jane%40x.org",
    "JANE%40X.ORG",
    "jane [at] x [dot] org",
    "jane[at]x[dot]org",
    "jane [ at ] x [ dot ] org",
    "jane(at)x.org",
    "jane (AT) mail (DOT) x (DOT) org",
    "jane {at} x {dot} co {dot} uk",
    "jane <at> x <dot> org",
    "jane@x [dot] org",
    # White space of any kind may stand around the brackets and inside them.
    "jane [at] example\n[dot] com",
    "jane [at] example\r\n[dot] com",
    "jane\xa0[at]\xa0x\xa0[dot]\xa0org",
    "jane [\nat\n] x [\xa0dot\xa0] org",
]

# Texts with an address that a line break or no-break spaces run through, each with what
# is left of it once the address is taken out.
WHITE_SPACE_TEXTS = [
    (
        "Copyright (c) 2020 Jane Doe <jane [at] example\n[dot] com>",
        "Copyright (c) 2020 Jane Doe",
    ),
    (
        "Copyright (c) 2020 Jane Doe <jane [at] example\r\n[dot] com>",
        "Copyright (c) 2020 Jane Doe",
    ),
    ("broken, mail jane\xa0[at]\xa0x\xa0[dot]\xa0org", "broken, mail"),
]

# A `%40` with no name before it, as in the URL of a scoped npm package.
NPM_URL = "https://www.npmjs.com/package/%40scope%2Fname"

# None of these holds an address, so each is kept as it is.
NOT_ADDRESSES = [
    # Plain words cannot be told from prose.
    "jane at x dot org",
    "look at numpy dot product",
    "jane AT x DOT org",
    "Meet us [at] PyCon",  # no domain
    "x [dot] org",  # no at
    NPM_URL,
]

# Each of these can make a naive regular expression do work in proportion to the square
# of the length, or worse: 20,000 characters took one a second or more, so 50,000 take
# it several seconds. The code under test takes a few milliseconds, and the limit is a
# second.
LONG = 50_000

HOSTILE_TEXTS = [
    # Two short texts come first. A pattern that can read one of them in more than one
    # way takes seconds over it, as its work doubles or more with each repeat, so each
    # fails within seconds when its safeguard is gone. Under such a pattern some of the
    # long texts after them would never end, so these run first.
    pytest.param("a@x" + " [dot]  " * 12, id="a few bracketed dots two blanks apart"),
    pytest.param("a@" + "b." * 24, id="a few single letters after an at sign"),
    pytest.param("a" * LONG, id="one long word"),
    pytest.param(" " * LONG, id="spaces"),
    pytest.param("<" + "@" * LONG, id="bracket then at signs"),
    pytest.param("< " * (LONG // 2), id="brackets and spaces"),
    pytest.param("a@" * (LONG // 2), id="repeated at signs"),
    pytest.param("a@" + "b." * (LONG // 2), id="domain of single letters"),
    pytest.param("a." * (LONG // 2) + "@", id="dotted word then at sign"),
    pytest.param(",".join(["a"] * (LONG // 2)), id="commas"),
    pytest.param("[at]" * (LONG // 4), id="bracketed ats"),
    pytest.param("a [at] " * (LONG // 7), id="names and bracketed ats"),
    pytest.param("a%40" * (LONG // 4), id="names and encoded ats"),
    pytest.param("a [at] " + "b [dot] " * (LONG // 8), id="domain of bracketed dots"),
    pytest.param("a(at)" + "b." * (LONG // 2), id="bracketed at then single letters"),
    pytest.param("[" * LONG, id="opening brackets"),
    pytest.param("[ " * (LONG // 2), id="opening brackets and spaces"),
    pytest.param("a [at]\n" * (LONG // 7), id="names and bracketed ats, a line each"),
]
