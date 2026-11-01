import tomllib
from pathlib import Path

from pypi_registry import STREAM

STREAM_TOML = Path(__file__).parents[1] / "stream.toml"


def load() -> dict:
    return tomllib.loads(STREAM_TOML.read_text(encoding="utf-8"))


def test_stream_toml_names_the_stream():
    config = load()
    assert config["id"] == "pypi-registry"
    assert config["id"] == STREAM
    assert config["description"]


def test_stream_toml_names_its_upstream_and_terms():
    assert load()["upstreams"] == [
        {
            "name": "PyPI Simple and JSON APIs",
            "url": "https://pypi.org/",
            "licence": "NOASSERTION",
            "terms": "https://policies.python.org/pypi.org/Terms-of-Use/",
        }
    ]


def test_stream_toml_gives_each_table_its_history_class():
    assert load()["tables"] == {
        "meta": "snapshot",
        "files_current": "snapshot",
        "projects": "append",
        "versions_seen": "append",
        "files": "append",
        "versions": "append",
        "release_records": "append",
        "serials": "append",
        "swept": "append",
        "cursors": "append",
        "events": "events",
    }
