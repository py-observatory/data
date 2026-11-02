import tomllib
from pathlib import Path

from osv_pypi import STREAM

STREAM_TOML = Path(__file__).parents[1] / "stream.toml"

# The history classes that the kit's manifest accepts. A table's class says how the
# publish gates treat it.
HISTORY_CLASSES = {"append", "events", "snapshot", "derived"}


def load() -> dict:
    return tomllib.loads(STREAM_TOML.read_text(encoding="utf-8"))


def test_stream_toml_names_the_stream():
    config = load()
    assert config["id"] == "osv-pypi"
    assert config["id"] == STREAM
    assert config["description"]


def test_stream_toml_names_its_upstreams_and_their_licences():
    assert load()["upstreams"] == [
        {
            "name": "OSV.dev data dumps",
            "url": "https://osv-vulnerabilities.storage.googleapis.com/PyPI/",
            "licence": "NOASSERTION",
            "terms": "https://google.github.io/osv.dev/data/",
        },
        {
            "name": "GitHub Advisory Database (GHSA-*)",
            "url": "https://github.com/github/advisory-database",
            "licence": "CC-BY-4.0",
        },
        {
            "name": "PyPA Advisory Database (PYSEC-*)",
            "url": "https://github.com/pypa/advisory-database",
            "licence": "CC-BY-4.0",
        },
        {
            "name": "OpenSSF Malicious Packages (MAL-*)",
            "url": "https://github.com/ossf/malicious-packages",
            "licence": "Apache-2.0",
        },
    ]


def test_stream_toml_gives_each_table_its_history_class():
    assert load()["tables"] == {
        "meta": "snapshot",
        "advisories": "snapshot",
        "versions": "append",
        "events": "events",
        "cursors": "append",
    }


def test_stream_toml_uses_only_known_history_classes():
    assert set(load()["tables"].values()) <= HISTORY_CLASSES
