import pytest

from obskit.names import canonical_name


def test_canonical_name():
    assert canonical_name("Foo_Bar.baz") == "foo-bar-baz"
    assert canonical_name("A--b") == "a-b"


@pytest.mark.parametrize(
    ("name", "canonical"),
    [
        ("Django", "django"),
        ("zope.interface", "zope-interface"),
        ("Foo_Bar.baz", "foo-bar-baz"),
        ("A--b", "a-b"),
        ("a-_.b", "a-b"),  # a mix of separators is still one hyphen
        ("ABC123", "abc123"),
        ("x", "x"),
        ("a.b_c-d", "a-b-c-d"),
        ("_private", "-private"),  # a leading separator is not dropped
        ("trailing.", "trailing-"),
        ("", ""),
    ],
)
def test_canonical_name_follows_pep_503(name, canonical):
    # PEP 503 is `re.sub(r"[-_.]+", "-", name).lower()`, and so is packaging's rule. The
    # forms are written out here, because the kit's tests use the standard library only.
    assert canonical_name(name) == canonical
    # A canonical name is its own canonical form.
    assert canonical_name(canonical) == canonical
