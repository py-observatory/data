from pypi_downloads import STREAM
from pypi_downloads.config import load
from pypi_downloads.serving import TABLES


def test_config_reads_stream_toml():
    c = load()
    assert c.federation.audience.endswith(
        "/workloadIdentityPools/github/providers/data"
    )
    assert c.federation.service_account.startswith("downloads-github@")
    assert (c.cpu, c.memory_gib, c.task_timeout_seconds, c.workers) == (4, 2, 1800, 4)
    assert c.tables == {
        "meta": "snapshot",
        "projects": "append",
        "days": "append",
        "breaks": "append",
        "spend": "snapshot",
        "clearances": "snapshot",
        "counts": "append",
        "folded": "derived",
        "months": "derived",
        "monthly": "derived",
        "top_set": "derived",
        "daily": "derived",
        "recent": "derived",
    }
    assert c.deployer == "counter-deployer@py-observatory.iam.gserviceaccount.com"
    assert c.bucket == "py-observatory-counts"
    assert c.project_number == "893320255656"
    assert c.upstreams[0]["licence"] == "NOASSERTION"


def test_every_table_of_the_serving_database_is_derived():
    # The gates hold no derived table to its rows, and the serving database is made
    # again from the counts whenever it cannot be updated.
    tables = load().tables
    assert {table: tables.get(table) for table in TABLES} == dict.fromkeys(
        TABLES, "derived"
    )


def test_stream_id_is_the_package_constant():
    assert STREAM == "pypi-downloads"
