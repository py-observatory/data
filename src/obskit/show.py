"""A bad value as an error message shows it.

A stream reads data from outside, and an error that names a bad value goes to a log and
to an issue. So the value is shown in part: whatever it is, the message stays short,
and showing it takes little work. A value that may hold an email address is not shown
at all. Every stream shows a bad value this one way.
"""

import reprlib

from obskit.emails import holds_an_at

# How much of a bad value an error message shows. A reply can hold anything, and its
# error goes to a log and to an issue.
_SHOWN = 60
# repr() cannot be trusted to show it. It recurses once for each level of nesting, and
# overflows the stack on a value that json read, which can be nested tens of thousands
# of levels deep. It also builds the whole text of a huge list or string before any of
# that is cut. This stops at three levels, and keeps a few items of a list and the two
# ends of a long string, so its work does not grow with the value.
_REPR = reprlib.Repr(maxlevel=3, maxstring=_SHOWN)

# What an error message shows in place of a value that may hold an email address.
WITHHELD = "<not shown, as it may hold an email address>"


def show(value: object) -> str:
    """`value` as an error message shows it: its start, in at most _SHOWN characters.
    Whatever the reply holds, this neither recurses without a bound nor does work that
    grows with the value.

    When that text holds an at of any form (`obskit.emails.holds_an_at`), it is
    WITHHELD instead. Error messages go to the run's log, which is public for a public
    repository. A value cut short can lose the end of its domain and keep its at, and
    is then no longer an address that a filter finds.
    """
    text = _REPR.repr(value)
    shown = text if len(text) <= _SHOWN else f"{text[: _SHOWN - 3]}..."
    return WITHHELD if holds_an_at(shown) else shown
