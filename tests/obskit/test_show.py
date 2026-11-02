import json

import pytest

from obskit.show import WITHHELD, show


def built_nested(depth: int) -> list:
    """A list nested `depth` levels deep, built in a loop. It never goes through json,
    so it can be deeper than repr() can go."""
    deep: list = []
    for _ in range(depth):
        deep = [deep]
    return deep


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(
            lambda: json.loads("[" * 10_000 + "]" * 10_000),
            id="nested 10,000 levels deep, as json reads it",
        ),
        pytest.param(lambda: built_nested(300_000), id="nested deeper than repr goes"),
        pytest.param(lambda: [0] * 3_000_000, id="a list of 3 million"),
        pytest.param(lambda: "y" * 20_000_000, id="a string of 20 million"),
        # Wide as well as long, so cutting each part short is not enough.
        pytest.param(lambda: {"k" * 500: "v" * 500}, id="an object of long strings"),
    ],
)
def test_show_gives_at_most_60_characters_of_any_value(make):
    # A value from outside can be anything, and its error goes to a log and to an issue.
    assert len(show(make())) <= 60


def test_show_stops_three_levels_down():
    assert show(json.loads("[" * 10_000 + "]" * 10_000)) == "[[[[...]]]]"


def test_show_cuts_a_long_text_and_says_so():
    text = show({"k" * 500: "v" * 500})
    assert len(text) == 60
    assert text.startswith("{'kkk")
    assert text.endswith("...")


@pytest.mark.parametrize(
    "value",
    ["GHSA-mjcr-rqjg-rhg3", "", 42, -1.5, True, None, ["a", 1], {"type": "OTHER"}],
)
def test_show_a_short_value_as_repr_would(value):
    assert show(value) == repr(value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("jane@example.org", id="an address"),
        pytest.param({"id": "jane%40example.org"}, id="an encoded at"),
        pytest.param(["jane [at] example [dot] org"], id="an at in brackets"),
        # Cut to 60 characters, the address loses the end of its domain, and with it
        # what makes it an address, but it keeps its at.
        pytest.param(
            {"a": "a" * 25, "z": "jane.doe@example.org"}, id="an address cut short"
        ),
        # Python writes the line breaks as escapes, which are no blanks to a filter.
        pytest.param("jane [\nat\n] x [\ndot\n] org", id="blanks written as escapes"),
        pytest.param("@", id="an at alone"),
    ],
)
def test_show_holds_back_a_value_that_holds_an_at(value):
    # An error message goes to the run's log, which is public for a public repository.
    assert show(value) == "<not shown, as it may hold an email address>"
    assert show(value) == WITHHELD


def test_show_shows_a_word_at_that_is_not_alone_in_brackets():
    assert show("look at numpy [at home]") == "'look at numpy [at home]'"
