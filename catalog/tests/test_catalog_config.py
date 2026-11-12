import tomllib
from pathlib import Path

import pytest

from catalog.config import StreamAdmission, load_config

ROOT = Path(__file__).resolve().parents[2]

BUILD = """\
[build]
min_gap_minutes = 50
max_gap_hours = 24
pin_wait_hours = 24
download_daily_years = 2
"""
STREAMS = """\
[streams.pypi-registry]
tables = ["projects", "files"]
stale_after_hours = 30

[streams.osv-pypi]
tables = ["advisories"]
stale_after_hours = 72
"""
# A file that loads. Each refusal below breaks it in one place.
VALID = f"{BUILD}\n{STREAMS}"

# Each limit as VALID writes it, with the key that an error about it must name.
LIMITS = [
    ("min_gap_minutes = 50", "build.min_gap_minutes"),
    ("max_gap_hours = 24", "build.max_gap_hours"),
    ("pin_wait_hours = 24", "build.pin_wait_hours"),
    ("download_daily_years = 2", "build.download_daily_years"),
    ("stale_after_hours = 72", "streams.osv-pypi.stale_after_hours"),
]
# What no limit may be: zero or less, a float, a string, and a bool, which TOML's `true`
# loads as and Python counts as an int.
BAD_LIMITS = ["0", "-1", "1.5", '"50"', "true"]


def changed(old: str, new: str) -> str:
    """VALID with `old` written as `new`. `old` is in it once, so that a case changes
    the place it names and no other."""
    assert VALID.count(old) == 1, old
    return VALID.replace(old, new)


REFUSED = [
    # An unknown key, at each level of the file.
    pytest.param(
        changed("[build]", "colour = 1\n\n[build]"),
        "colour",
        id="unknown key at the top",
    ),
    pytest.param(
        changed("pin_wait_hours = 24", "pin_wait_hours = 24\ncolour = 1"),
        "build.colour",
        id="unknown key in build",
    ),
    pytest.param(
        changed("stale_after_hours = 72", "stale_after_hours = 72\ncolour = 1"),
        "streams.osv-pypi.colour",
        id="unknown key in a stream",
    ),
    # Each key that the file must have.
    pytest.param(STREAMS, "build", id="no build"),
    pytest.param(BUILD, "streams", id="no streams"),
    pytest.param(
        changed("min_gap_minutes = 50\n", ""),
        "build.min_gap_minutes",
        id="no min_gap_minutes",
    ),
    pytest.param(
        changed("max_gap_hours = 24\n", ""),
        "build.max_gap_hours",
        id="no max_gap_hours",
    ),
    pytest.param(
        changed("pin_wait_hours = 24\n", ""),
        "build.pin_wait_hours",
        id="no pin_wait_hours",
    ),
    pytest.param(
        changed("download_daily_years = 2\n", ""),
        "build.download_daily_years",
        id="no download_daily_years",
    ),
    pytest.param(
        changed('tables = ["advisories"]\n', ""),
        "streams.osv-pypi.tables",
        id="no tables",
    ),
    pytest.param(
        changed("stale_after_hours = 72\n", ""),
        "streams.osv-pypi.stale_after_hours",
        id="no stale_after_hours",
    ),
    # A stream with no tables, and a table listed twice.
    pytest.param(
        changed('["advisories"]', "[]"),
        "streams.osv-pypi.tables",
        id="a stream with no tables",
    ),
    pytest.param(
        changed('["advisories"]', '["advisories", "advisories"]'),
        "streams.osv-pypi.tables",
        id="a table listed twice",
    ),
    # Tables that are not a list of names, which would otherwise admit the letters of
    # a string, or a table that has no name. No letter of "files" is in it twice, so
    # the letters would not be refused as a table that is listed twice.
    pytest.param(
        changed('["advisories"]', '"files"'),
        "streams.osv-pypi.tables",
        id="tables is a string",
    ),
    pytest.param(
        changed('["advisories"]', "[7]"),
        "streams.osv-pypi.tables",
        id="a table that is not a string",
    ),
    # A limit of 0, or one that is not an int.
    *[
        pytest.param(
            changed(line, f"{line.partition(' = ')[0]} = {bad}"),
            key,
            id=f"{key}={bad}",
        )
        for line, key in LIMITS
        for bad in BAD_LIMITS
    ],
    # The shortest wait between builds must be shorter than the longest.
    pytest.param(
        changed("min_gap_minutes = 50", "min_gap_minutes = 1440"),
        "build.min_gap_minutes",
        id="min gap equals max gap",
    ),
    pytest.param(
        changed("min_gap_minutes = 50", "min_gap_minutes = 1500"),
        "build.min_gap_minutes",
        id="min gap over max gap",
    ),
    # A stream id that the kit's tags refuse, and the catalog itself.
    pytest.param(
        changed("[streams.osv-pypi]", "[streams.Osv_Pypi]"),
        "Osv_Pypi",
        id="stream id with capitals and an underscore",
    ),
    pytest.param(
        changed("[streams.osv-pypi]", "[streams.osv--pypi]"),
        "osv--pypi",
        id="stream id with a double hyphen",
    ),
    pytest.param(
        changed("[streams.osv-pypi]", "[streams.catalog]"),
        "streams.catalog",
        id="the catalog",
    ),
    # A value where the file should have a table.
    pytest.param("build = 5\n\n" + STREAMS, "build", id="build is not a table"),
    pytest.param("streams = 5\n\n" + BUILD, "streams", id="streams is not a table"),
    pytest.param(
        f"{BUILD}\n[streams]\nosv-pypi = 5\n",
        "streams.osv-pypi",
        id="a stream is not a table",
    ),
]


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "catalog.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_load_config_reads_the_admitted_streams():
    config = load_config()

    # The downloads stream comes after the registry, whose projects its tables link to.
    assert list(config.streams) == ["pypi-registry", "osv-pypi", "pypi-downloads"]
    assert config.streams["pypi-registry"] == StreamAdmission(
        stream="pypi-registry",
        tables=frozenset(
            {
                "projects",
                "versions_seen",
                "files_current",
                "files",
                "release_records",
                "events",
            }
        ),
        stale_after_hours=30,
    )
    assert config.streams["osv-pypi"] == StreamAdmission(
        stream="osv-pypi", tables=frozenset({"advisories"}), stale_after_hours=72
    )
    assert config.streams["pypi-downloads"] == StreamAdmission(
        stream="pypi-downloads",
        tables=frozenset(
            {
                "projects",
                "days",
                "breaks",
                "monthly",
                "months",
                "top_set",
                "daily",
                "recent",
            }
        ),
        stale_after_hours=54,
    )
    assert config.min_gap_minutes == 50
    assert config.max_gap_hours == 24
    assert config.pin_wait_hours == 24
    assert config.download_daily_years == 2


@pytest.mark.parametrize(("text", "key"), REFUSED)
def test_load_config_refuses(tmp_path, text, key):
    with pytest.raises(ValueError) as refusal:
        load_config(write(tmp_path, text))

    # A TOMLDecodeError is a ValueError too. Each case is refused for what it says, and
    # not because it is not TOML.
    assert not isinstance(refusal.value, tomllib.TOMLDecodeError)
    assert key in str(refusal.value)


def test_load_config_accepts_a_min_gap_a_minute_short_of_the_max_gap(tmp_path):
    text = changed("min_gap_minutes = 50", "min_gap_minutes = 1439")

    assert load_config(write(tmp_path, text)).min_gap_minutes == 1439


def test_admitted_tables_exist_in_their_streams():
    for admission in load_config().streams.values():
        folder = admission.stream.replace("-", "_")
        stream_toml = ROOT / "streams" / folder / "stream.toml"
        tables = tomllib.loads(stream_toml.read_text(encoding="utf-8"))["tables"]

        missing = sorted(admission.tables - set(tables))
        assert not missing, f"{admission.stream} has no table {', '.join(missing)}"
