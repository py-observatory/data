"""Email addresses: what counts as one, and how one is taken out of a text.

No published data holds an email address. A release's record and a file's yank reason
are both made from what people wrote, and both go by this one reading of an address, so
that a text that counts as one in either counts in the other.

An address may be written plainly or hidden, as in `jane%40x.org` or
`jane [at] x [dot] org`. The hidden forms are written for nothing else, so they count.
Plain words do not: `jane at x dot org` cannot be told apart from
`look at numpy dot product`.
"""

import re


def _in_brackets(word: str) -> str:
    """A pattern for `word` in any case, alone inside one matching pair of brackets:
    `[]`, `()`, `{}` or `<>`. White space of any kind, a line break or a no-break space
    among them, may stand inside the brackets and on either side of them.

    The white space is possessive: a match never gives any of it back, and nothing after
    it needs any. Where two of these meet, as in `x [dot]  [dot]`, the white space
    between them could otherwise be shared out between the two in several ways, and a
    text with many of them would be tried in every way: 12 of them take seconds.
    """
    inside = rf"\s*+(?i:{word})\s*+"
    pairs = [(r"\[", r"\]"), (r"\(", r"\)"), (r"\{", r"\}"), ("<", ">")]
    either = "|".join(f"{opening}{inside}{closing}" for opening, closing in pairs)
    return rf"\s*+(?:{either})\s*+"


_AT_WORD = _in_brackets("at")
_DOT_WORD = _in_brackets("dot")

# `<name@host>`: brackets with an address in them and nothing else that is a bracket.
# The text before the first @ cannot hold an @, so each bracket is read in one pass. A
# text of many `<` and `@` would otherwise be read again from each of them.
BRACKETED_ADDRESS = re.compile(r"<[^<>@]*@[^<>]*>")

# An email address: a name of letters, digits and `._%+-`, an at, and a domain that
# ends in a dot and two letters or more. `\w` takes the letters of every script, so an
# address in another script goes whole and leaves no part of itself behind.
#
# The at is `@`, `%40` (`@` as a URL writes it) or `at` in brackets. A dot of the domain
# is `.` or `dot` in brackets, and any form of the at goes with any form of the dot.
#
# The lookbehind makes a match start where a name starts. Without it, every letter of a
# long word is tried as a start, the word is read to its end from each, and a text of
# 50,000 letters and no address takes several seconds.
#
# The domain repeats a letter, a digit, `_`, `.` or `-`, or else a dot in brackets, and
# no character can begin both, so each part of a domain is read in one way only. If the
# repeat took `.` as a form of the dot as well, each `.` could be read in two ways, and
# a domain of many dots that is not an address would take twice as long with each dot.
ADDRESS = re.compile(
    rf"(?<![\w.%+-])[\w.%+-]+(?:@|%40|{_AT_WORD})"
    rf"(?:[\w.-]|{_DOT_WORD})+(?:\.|{_DOT_WORD})[^\W\d_]{{2,}}"
)

# An address with the brackets around it and the spaces before it. Those are spaces and
# tabs only, so that taking out an address at the start of a line never joins that line
# to the one before. The first lookbehind is for the same reason as the one above, for a
# long run of spaces.
REMOVAL = re.compile(
    rf"(?<![ \t])[ \t]*(?:<\s*{ADDRESS.pattern}\s*>|{ADDRESS.pattern})"
)


def holds_address(text: str) -> bool:
    """Whether `text` holds an email address of any form."""
    return ADDRESS.search(text) is not None


def strip_emails(value: str | None) -> str | None:
    """The names in a field of people, with every email address taken out.

    A field such as `author_email` holds `Name <address>` entries or bare addresses,
    separated by commas. Each address, of any form, first becomes a lone `@`, so that
    the rest of the work treats it as it treats an address written with one: brackets
    with an `@` in them go, and so does any word that holds an `@`. The text of each
    entry that is left is trimmed of spaces and quotes, an entry with nothing left is
    dropped, and the rest are joined with ", ". With nothing left the result is None.

    Taking brackets or words out can make another address: `jane [at] me@x x [dot] org`
    leaves `jane [at] x [dot] org`. Only a text written to do that looks like this, so
    the result is then None. It never holds an `@` or an address.
    """
    if value is None:
        return None
    names = []
    for entry in BRACKETED_ADDRESS.sub(" ", ADDRESS.sub("@", value)).split(","):
        words = [word for word in entry.split() if "@" not in word]
        name = " ".join(words).strip(" \"'")
        if name:
            names.append(name)
    result = ", ".join(names) or None
    if result is not None and holds_address(result):
        return None
    return result


def without_addresses(text: str | None) -> str | None:
    """`text` with every email address taken out, with the brackets around it and the
    spaces before it, and then trimmed. A text with no address comes back as it was.
    None comes back for a text that is empty or that nothing is left of.

    Taking an address out can join the text on either side into another address:
    `me@<a@b.cc>x.org` leaves `me@x.org`. Only a text written to do that looks like
    this, so it is dropped whole, and what comes back never holds an address. Taking
    out what is left would make the work grow with how deeply a text nests.
    """
    if text is None:
        return None
    cleaned, removed = REMOVAL.subn("", text)
    if removed:
        text = cleaned.strip()
        if holds_address(text):
            return None
    return text or None


def is_mail_link(url: str) -> bool:
    """Whether `url` is a `mailto:` link. A scheme is not case sensitive."""
    return url.lstrip().lower().startswith("mailto:")
