import json

import pytest

from obskit.show import show


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
