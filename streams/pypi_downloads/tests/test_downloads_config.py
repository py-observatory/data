from pypi_downloads import STREAM
from pypi_downloads.config import load


def test_config_reads_stream_toml():
    c = load()
    assert c.federation.audience.endswith(
        "/workloadIdentityPools/github/providers/data"
    )
    assert c.federation.service_account.startswith("downloads-github@")
    assert (c.cpu, c.memory_gib, c.task_timeout_seconds, c.workers) == (4, 2, 1800, 4)
    assert set(c.tables) == {"meta", "projects", "days", "breaks", "spend", "counts"}
    assert c.deployer == "counter-deployer@py-observatory.iam.gserviceaccount.com"
    assert c.bucket == "py-observatory-counts"
    assert c.project_number == "893320255656"
    assert c.upstreams[0]["licence"] == "NOASSERTION"


def test_stream_id_is_the_package_constant():
    assert STREAM == "pypi-downloads"
