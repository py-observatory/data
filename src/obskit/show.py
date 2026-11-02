"""A bad value as an error message shows it.

A stream reads data from outside, and an error that names a bad value goes to a log and
to an issue. So the value is shown in part: whatever it is, the message stays short,
and showing it takes little work. Every stream shows a bad value this one way.
"""

import reprlib

# How much of a bad value an error message shows. A reply can hold anything, and its
# error goes to a log and to an issue.
_SHOWN = 60
# repr() cannot be trusted to show it. It recurses once for each level of nesting, and
# overflows the stack on a value that json read, which can be nested tens of thousands
# of levels deep. It also builds the whole text of a huge list or string before any of
# that is cut. This stops at three levels, and keeps a few items of a list and the two
# ends of a long string, so its work does not grow with the value.
_REPR = reprlib.Repr(maxlevel=3, maxstring=_SHOWN)


def show(value: object) -> str:
    """`value` as an error message shows it: its start, in at most _SHOWN characters.
    Whatever the reply holds, this neither recurses without a bound nor does work that
    grows with the value."""
    text = _REPR.repr(value)
    return text if len(text) <= _SHOWN else f"{text[: _SHOWN - 3]}..."
