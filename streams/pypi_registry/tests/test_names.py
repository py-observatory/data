import pytest
from packaging.utils import canonicalize_name

from pypi_registry.names import (
    canonical_name,
    canonical_version,
    version_from_filename,
)


def test_canonical_name():
    assert canonical_name("Foo_Bar.baz") == "foo-bar-baz"
    assert canonical_name("A--b") == "a-b"


@pytest.mark.parametrize(
    "name",
    [
        "Django",
        "zope.interface",
        "Foo_Bar.baz",
        "A--b",
        "a-_.b",  # a mix of separators is still one hyphen
        "ABC123",
        "x",
        "a.b_c-d",
        "_private",  # a leading separator is not dropped
        "trailing.",
        "",
    ],
)
def test_canonical_name_follows_pep_503(name):
    # PEP 503 is `re.sub(r"[-_.]+", "-", name).lower()`, and so is packaging's rule.
    assert canonical_name(name) == canonicalize_name(name)
    # A canonical name is its own canonical form.
    assert canonical_name(canonical_name(name)) == canonical_name(name)


def test_canonical_version():
    assert canonical_version("1.0.0-RC1") == "1.0.0rc1"
    assert canonical_version("not a version") == "not a version"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1.0", "1.0"),
        ("1.0.0", "1.0.0"),  # 1.0 and 1.0.0 are equal but spelled apart, and stay so
        ("V1.0", "1.0"),
        ("1.0.0-RC1", "1.0.0rc1"),
        ("2.0.0-alpha.1", "2.0.0a1"),
        ("1.0-1", "1.0.post1"),
        ("1.0.DEV3", "1.0.dev3"),
        ("1.0a", "1.0a0"),
        ("1!2.0", "1!2.0"),  # an epoch is kept
        ("1.0+Ubuntu.1", "1.0+ubuntu.1"),
        ("01.02", "1.2"),
        (" 1.0 ", "1.0"),
    ],
)
def test_canonical_version_normalizes(raw, expected):
    assert canonical_version(raw) == expected


@pytest.mark.parametrize(
    "raw", ["not a version", "", "1.x", "1.0.0.", "latest", "1.0 beta", "0.1dev-r1234"]
)
def test_canonical_version_keeps_an_invalid_version_as_it_is(raw):
    # Old releases carry versions that PEP 440 does not allow. They are data, not errors.
    assert canonical_version(raw) == raw


@pytest.mark.parametrize(
    "raw", ["1." + "9" * 5000, "9" * 5000 + "!1.0", "1.0rc" + "9" * 5000]
)
def test_canonical_version_keeps_a_number_that_python_will_not_read(raw):
    # Python refuses to make an int of 4,300 digits or more, and `packaging` lets that
    # error through as a plain ValueError and not as InvalidVersion.
    assert canonical_version(raw) == raw


def test_version_from_filename():
    assert version_from_filename("Foo_Bar-1.0-py3-none-any.whl") == "1.0"
    assert version_from_filename("foo-bar-2.0.tar.gz") == "2.0"
    assert version_from_filename("weird.exe") is None


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("foo-1.0.zip", "1.0"),
        ("foo-1.0-1-py3-none-any.whl", "1.0"),  # a build tag is not the version
        ("foo_bar-1.0.0rc1-cp314-cp314-manylinux_2_28_x86_64.whl", "1.0.0rc1"),
        ("foo-1.0+local-py3-none-any.whl", "1.0+local"),
        ("foo-1!2.0.tar.gz", "1!2.0"),
        # The version comes back canonical, as canonical_version gives it.
        ("foo-1.0.0RC1.tar.gz", "1.0.0rc1"),
        ("foo-1.0.0RC1-py3-none-any.whl", "1.0.0rc1"),
        # A name with hyphens: the version is what follows the last hyphen.
        ("foo-bar-baz-3.1.4.tar.gz", "3.1.4"),
    ],
)
def test_version_from_filename_forms(filename, expected):
    assert version_from_filename(filename) == expected


@pytest.mark.parametrize(
    "filename",
    [
        "",
        ".whl",
        "foo.tar.gz",  # no version at all
        "foo-bar.tar.gz",  # what follows the last hyphen is not a version
        "foo-1.0-beta.tar.gz",
        "foo-1.0.0-RC1.tar.gz",  # a hyphen in the version cannot be told from the name
        "foo-1.0-py3-none.whl",  # too few parts
        "foo-notaversion-py3-none-any.whl",
        "foo__bar-1.0-py3-none-any.whl",  # PEP 427 escapes a name without `__`
        "foo bar-1.0-py3-none-any.whl",
        # Formats that PyPI served once and the parsers do not read.
        "foo-1.0.tar.bz2",
        "foo-1.0.tgz",
        "foo-1.0.egg",
        "foo-1.0.win32.exe",
        "foo-1.0-py3-none-any.WHL",
        "foo-1.0-py3-none-any.whl.metadata",
    ],
)
def test_version_from_filename_is_none_when_it_will_not_parse(filename):
    assert version_from_filename(filename) is None


@pytest.mark.parametrize(
    "filename",
    [
        "foo-1." + "9" * 5000 + ".tar.gz",
        "foo-1." + "9" * 5000 + "-py3-none-any.whl",
    ],
)
def test_version_from_filename_is_none_for_a_number_python_will_not_read(filename):
    assert version_from_filename(filename) is None
