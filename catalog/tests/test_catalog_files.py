import pytest
from catalog.files import file_kind, wheel_tags


def test_a_wheel_gives_its_three_tags():
    assert wheel_tags("sampleproject-1.0-py3-none-any.whl") == {("py3", "none", "any")}


def test_a_compressed_tag_set_is_expanded():
    assert wheel_tags(
        "numpy-2.3.4-cp312-cp312-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl"
    ) == {
        ("cp312", "cp312", "manylinux_2_27_x86_64"),
        ("cp312", "cp312", "manylinux_2_28_x86_64"),
    }
    assert wheel_tags("six-1.17.0-py2.py3-none-any.whl") == {
        ("py2", "none", "any"),
        ("py3", "none", "any"),
    }


def test_a_build_tag_is_skipped():
    assert wheel_tags("pkg-1.0-1-cp39-abi3-win_amd64.whl") == {
        ("cp39", "abi3", "win_amd64")
    }


def test_a_free_threaded_abi_is_kept_as_written():
    assert wheel_tags("numpy-2.3.4-cp313-cp313t-macosx_14_0_arm64.whl") == {
        ("cp313", "cp313t", "macosx_14_0_arm64")
    }


def test_tags_are_lowercased():
    assert wheel_tags("Foo-1.0-PY3-NONE-ANY.WHL") == {("py3", "none", "any")}


def test_extra_dashes_still_give_the_last_three_fields():
    assert wheel_tags("my-odd-name-1.0-py3-none-any.whl") == {("py3", "none", "any")}


@pytest.mark.parametrize(
    "filename",
    ["broken.whl", "pkg-1.0-py3.whl", "pkg-1.0-py3-none.whl", "pkg-1.0.tar.gz", ""],
)
def test_a_name_that_is_not_a_readable_wheel_gives_none(filename):
    assert wheel_tags(filename) is None


@pytest.mark.parametrize(
    ("filename", "kind"),
    [
        ("pkg-1.0-py3-none-any.whl", "wheel"),
        ("broken.whl", "wheel"),
        ("pkg-1.0.tar.gz", "sdist"),
        ("pkg-1.0.zip", "sdist"),
        ("pkg-1.0.tar.bz2", "sdist"),
        ("pkg-1.0.tgz", "sdist"),
        ("pkg-1.0.tar.xz", "sdist"),
        ("pkg-1.0.tar", "sdist"),
        ("PKG-1.0.TAR.GZ", "sdist"),
        ("pkg-1.0-py2.7.egg", "other"),
        ("pkg-1.0.win32.exe", "other"),
        ("pkg-1.0.msi", "other"),
    ],
)
def test_file_kind(filename, kind):
    assert file_kind(filename) == kind
