"""The stream's settings, read from its stream.toml: where its Google Cloud project,
bucket and counter job are, who signs in, and what the counter runs with."""

import tomllib
from dataclasses import dataclass
from pathlib import Path

from obskit.gcp import Federation

STREAM_TOML = Path(__file__).resolve().parents[2] / "stream.toml"


@dataclass(frozen=True)
class Config:
    """The settings of stream.toml. `federation` is how the tick signs in. `deployer`
    is the service account of the deploy workflow, which signs in through the same
    pool and provider."""

    project: str
    project_number: str
    region: str
    bucket: str
    job: str
    subscription: str
    federation: Federation
    deployer: str
    image_repo: str
    counter_service_account: str
    cpu: int
    memory_gib: int
    task_timeout_seconds: int
    workers: int
    tables: dict[str, str]
    upstreams: list[dict]


def load(path: Path = STREAM_TOML) -> Config:
    """Read the stream's settings from `path`. Raises KeyError for a setting that the
    file lacks."""
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    gcp = raw["gcp"]
    counter = raw["counter"]
    return Config(
        project=gcp["project"],
        project_number=gcp["project_number"],
        region=gcp["region"],
        bucket=gcp["bucket"],
        job=gcp["job"],
        subscription=gcp["subscription"],
        federation=Federation(
            gcp["project_number"], gcp["pool"], gcp["provider"], gcp["service_account"]
        ),
        deployer=gcp["deployer"],
        image_repo=gcp["image_repo"],
        counter_service_account=gcp["counter_service_account"],
        cpu=counter["cpu"],
        memory_gib=counter["memory_gib"],
        task_timeout_seconds=counter["task_timeout_seconds"],
        workers=counter["workers"],
        tables=dict(raw["tables"]),
        upstreams=list(raw["upstreams"]),
    )
