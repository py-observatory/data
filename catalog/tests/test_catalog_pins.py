import hashlib
import json
import tempfile
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from catalog.config import load_config
from catalog.inputs import StreamRead, read_stream
from catalog.pins import (
    PIN_ASSET,
    copy_month,
    due_days,
    pin_document,
    pin_ready,
    write_pin,
)
from catalog.run import run_once
from catalog_helpers import REPO, TOKEN, no_sleep, publish_osv, publish_registry
from obskit.github import GitHub
from obskit.manifest import Asset, Manifest, dumps, loads
from obskit.release import CreateOnceError, add_create_once
from obskit.testing import FakeGitHub

CONFIG = load_config()
CURRENT = "catalog-current"
# The day that most of these pins are for.
DAY = date(2026, 11, 2)
SECOND = timedelta(seconds=1)


@pytest.fixture
def fake() -> FakeGitHub:
    return FakeGitHub(REPO)


@pytest.fixture
def gh(fake: FakeGitHub) -> GitHub:
    return GitHub(REPO, TOKEN, transport=fake, sleep=no_sleep)


def registry(gh: GitHub, open_day: str, *sealed: str) -> Manifest:
    """Publish the registry as its run on `open_day` leaves it, having sealed each day
    of `sealed`. A pin names assets and never reads them, so the state and the open
    delta are empty, and each sealed delta holds only a cursor of its day, which gives
    each its own bytes."""
    rows = {
        day: {"cursors": [{"serial": int(day.replace("-", "")), "fetched_at": day}]}
        for day in sealed
    }
    return publish_registry(gh, state={}, open_day=open_day, open_delta={}, sealed=rows)


def osv(gh: GitHub, open_day: str, *sealed: str) -> Manifest:
    """Publish osv as its run on `open_day` leaves it, having sealed each day of
    `sealed`, as `registry` does."""
    rows = {
        day: {"cursors": [{"generation": 1, "observed_at": f"{day}T00:00:00Z"}]}
        for day in sealed
    }
    return publish_osv(gh, advisories=[], open_day=open_day, sealed=rows)


def read_all(gh: GitHub, tmp_path: Path) -> list[StreamRead]:
    """Each stream that catalog.toml admits, read as a run reads it, in that order."""
    return [
        read_stream(gh, admission, Path(tempfile.mkdtemp(dir=tmp_path)))
        for admission in CONFIG.streams.values()
    ]


def unreadable(stream: str) -> StreamRead:
    """A read of `stream` that failed."""
    return StreamRead(stream, None, [], None, f"{stream} could not be read")


def sha256_of(fake: FakeGitHub, tag: str, name: str) -> str:
    """The sha256 of the bytes that the release `tag` holds as `name`."""
    return hashlib.sha256(fake.asset_bytes(tag, name)).hexdigest()


def text_of(document: dict) -> bytes:
    """`document` as a pin's release.json holds it."""
    return (json.dumps(document, sort_keys=True, indent=2) + "\n").encode()


def writes(fake: FakeGitHub) -> list[tuple[str, str]]:
    """The requests that changed a release since the fake's calls were last cleared."""
    return [(method, url) for method, url, _ in fake.calls if method != "GET"]


def test_due_days_start_at_the_first_build_and_end_yesterday():
    first = date(2026, 11, 2)

    # The day of the first build is not over, so nothing is due on it.
    assert due_days(first, None, first) == []
    assert due_days(first, None, date(2026, 11, 5)) == [
        date(2026, 11, 2),
        date(2026, 11, 3),
        date(2026, 11, 4),
    ]
    # After the last pin, the days start on the day after it.
    assert due_days(first, date(2026, 11, 3), date(2026, 11, 5)) == [date(2026, 11, 4)]
    assert due_days(first, date(2026, 11, 4), date(2026, 11, 5)) == []
    assert due_days(first, date(2026, 11, 29), date(2026, 12, 2)) == [
        date(2026, 11, 30),
        date(2026, 12, 1),
    ]


def test_a_pin_waits_until_every_stream_has_sealed_the_day(gh, tmp_path):
    # The registry has sealed the 2nd, and osv is still open on it.
    registry(gh, "2026-11-03", "2026-11-02")
    osv(gh, "2026-11-02")
    morning = datetime(2026, 11, 3, 6, 0, tzinfo=UTC)

    assert pin_ready(DAY, read_all(gh, tmp_path), morning, 24) is False

    # Once osv has sealed it too, the pin is ready, long before the wait is over.
    osv(gh, "2026-11-03", "2026-11-02")
    reads = read_all(gh, tmp_path)
    assert pin_ready(DAY, reads, morning, 24) is True
    # Neither has sealed the 3rd.
    assert pin_ready(date(2026, 11, 3), reads, morning, 24) is False
    # A stream that cannot be read has sealed nothing that the catalog can see.
    registry_read, _ = reads
    assert pin_ready(DAY, [registry_read, unreadable("osv-pypi")], morning, 24) is False


def test_a_pin_is_written_anyway_once_the_wait_is_over(gh, fake, tmp_path):
    registry(gh, "2026-11-03", "2026-11-02")
    osv(gh, "2026-11-02")
    reads = read_all(gh, tmp_path)
    # The 2nd ends at midnight, and the wait of 24 hours after that.
    over = datetime(2026, 11, 4, tzinfo=UTC)

    assert pin_ready(DAY, reads, over - SECOND, 24) is False
    assert pin_ready(DAY, reads, over, 24) is True
    assert pin_ready(DAY, reads, datetime(2026, 11, 3, 12, tzinfo=UTC), 12) is True

    document = pin_document(DAY, reads, CONFIG, REPO, "")
    assert write_pin(gh, DAY, document, tmp_path) is True

    pin = json.loads(fake.asset_bytes("v2026-11-02", PIN_ASSET))
    assert pin == document
    # osv has not sealed the day, and the pin says so. What it names of osv is what
    # rebuilds the day as far as osv has it: the base copy of the month.
    assert pin["streams"]["osv-pypi"] == {
        "assets": [
            {
                "name": "osv-pypi-state-base-2026-11.db.zst",
                "period": "2026-11",
                "sha256": sha256_of(
                    fake, "osv-pypi-month-2026-11", "osv-pypi-state-base-2026-11.db.zst"
                ),
                "tag": "osv-pypi-month-2026-11",
                "url": "https://github.com/py-observatory/data/releases/download/"
                "osv-pypi-month-2026-11/osv-pypi-state-base-2026-11.db.zst",
            }
        ],
        "complete": False,
        "error": None,
        "sealed_through": "2026-11-01",
    }
    assert pin["streams"]["pypi-registry"]["complete"] is True


def test_the_pin_document_names_create_once_assets_with_urls_and_the_commit(
    gh, fake, tmp_path
):
    # The registry has sealed the 2nd and the 3rd, and osv the 2nd. The pin of the 2nd
    # names the base copy of the month and the delta of the 2nd of each: not the delta
    # of the 3rd, which is after the day, nor the state or the open delta, which the
    # next run of the stream replaces.
    registry(gh, "2026-11-04", "2026-11-02", "2026-11-03")
    osv(gh, "2026-11-03", "2026-11-02")
    reads = read_all(gh, tmp_path)
    registry_month = "pypi-registry-month-2026-11"
    osv_month = "osv-pypi-month-2026-11"

    document = pin_document(DAY, reads, CONFIG, REPO, "8f3c2a1")

    expected = {
        "commit": "8f3c2a1",
        "day": "2026-11-02",
        "repo": "py-observatory/data",
        "streams": {
            "osv-pypi": {
                "assets": [
                    {
                        "name": "osv-pypi-state-base-2026-11.db.zst",
                        "period": "2026-11",
                        "sha256": sha256_of(
                            fake, osv_month, "osv-pypi-state-base-2026-11.db.zst"
                        ),
                        "tag": "osv-pypi-month-2026-11",
                        "url": "https://github.com/py-observatory/data/releases/"
                        "download/osv-pypi-month-2026-11/"
                        "osv-pypi-state-base-2026-11.db.zst",
                    },
                    {
                        "name": "osv-pypi-delta-2026-11-02.db.zst",
                        "period": "2026-11-02",
                        "sha256": sha256_of(
                            fake, osv_month, "osv-pypi-delta-2026-11-02.db.zst"
                        ),
                        "tag": "osv-pypi-month-2026-11",
                        "url": "https://github.com/py-observatory/data/releases/"
                        "download/osv-pypi-month-2026-11/"
                        "osv-pypi-delta-2026-11-02.db.zst",
                    },
                ],
                "complete": True,
                "error": None,
                "sealed_through": "2026-11-02",
            },
            "pypi-registry": {
                "assets": [
                    {
                        "name": "pypi-registry-state-base-2026-11.db.zst",
                        "period": "2026-11",
                        "sha256": sha256_of(
                            fake,
                            registry_month,
                            "pypi-registry-state-base-2026-11.db.zst",
                        ),
                        "tag": "pypi-registry-month-2026-11",
                        "url": "https://github.com/py-observatory/data/releases/"
                        "download/pypi-registry-month-2026-11/"
                        "pypi-registry-state-base-2026-11.db.zst",
                    },
                    {
                        "name": "pypi-registry-delta-2026-11-02.db.zst",
                        "period": "2026-11-02",
                        "sha256": sha256_of(
                            fake,
                            registry_month,
                            "pypi-registry-delta-2026-11-02.db.zst",
                        ),
                        "tag": "pypi-registry-month-2026-11",
                        "url": "https://github.com/py-observatory/data/releases/"
                        "download/pypi-registry-month-2026-11/"
                        "pypi-registry-delta-2026-11-02.db.zst",
                    },
                ],
                "complete": True,
                "error": None,
                "sealed_through": "2026-11-03",
            },
        },
    }
    assert document == expected

    assert write_pin(gh, DAY, document, tmp_path) is True

    release = fake.release("v2026-11-02")
    assert (release["prerelease"], release["make_latest"]) == (True, "false")
    assert [asset["name"] for asset in release["assets"]] == ["release.json"]
    assert fake.asset_bytes("v2026-11-02", "release.json") == text_of(expected)


@pytest.mark.parametrize(
    ("read", "error", "sealed"),
    [
        pytest.param(
            unreadable("pypi-registry"),
            "pypi-registry could not be read",
            None,
            id="a stream that cannot be read",
        ),
        pytest.param(
            None,
            "lists no closed base copy of 2026-10",
            "2026-11-01",
            id="a day before the stream's first month",
        ),
    ],
)
def test_a_stream_that_cannot_be_pinned_is_named_with_its_error(
    gh, tmp_path, read, error, sealed
):
    registry(gh, "2026-11-02")
    osv(gh, "2026-11-02")
    reads = read_all(gh, tmp_path)
    if read is not None:
        reads[0] = read

    # The registry made its first base copy in November, so nothing rebuilds October.
    document = pin_document(date(2026, 10, 31), reads, CONFIG, REPO, "")

    pinned = document["streams"]["pypi-registry"]
    assert (pinned["assets"], pinned["complete"], pinned["sealed_through"]) == (
        [],
        False,
        sealed,
    )
    assert error in pinned["error"]


def test_a_pin_that_exists_is_left_alone(gh, fake, tmp_path):
    registry(gh, "2026-11-03", "2026-11-02")
    osv(gh, "2026-11-03", "2026-11-02")
    reads = read_all(gh, tmp_path)
    # An earlier run wrote the pin of the 2nd, at another commit.
    earlier = tmp_path / "earlier"
    earlier.mkdir()
    (earlier / PIN_ASSET).write_bytes(
        text_of(pin_document(DAY, reads, CONFIG, REPO, "earlier"))
    )
    add_create_once(gh, "catalog", "v2026-11-02", [earlier / PIN_ASSET])
    # And a run stopped after creating the release of the 3rd, before adding the pin.
    gh.create_release("v2026-11-03", prerelease=True, latest=False)
    fake.calls.clear()

    later = pin_document(DAY, reads, CONFIG, REPO, "later")
    assert write_pin(gh, DAY, later, tmp_path) is False

    assert writes(fake) == []
    assert (
        fake.asset_bytes("v2026-11-02", PIN_ASSET) == (earlier / PIN_ASSET).read_bytes()
    )

    third = date(2026, 11, 3)
    document = pin_document(third, reads, CONFIG, REPO, "later")
    assert write_pin(gh, third, document, tmp_path) is True

    assert fake.asset_bytes("v2026-11-03", PIN_ASSET) == text_of(document)


def test_a_pin_that_github_lists_without_a_digest_is_not_taken_as_written(
    gh, fake, tmp_path, monkeypatch
):
    registry(gh, "2026-11-03", "2026-11-02")
    osv(gh, "2026-11-03", "2026-11-02")
    document = pin_document(DAY, read_all(gh, tmp_path), CONFIG, REPO, "8f3c2a1")
    stuck = tmp_path / "stuck"
    stuck.mkdir()
    (stuck / PIN_ASSET).write_bytes(text_of(document))
    add_create_once(gh, "catalog", "v2026-11-02", [stuck / PIN_ASSET])
    # An upload that stuck or failed: GitHub lists release.json with no digest.
    listed = gh.assets
    monkeypatch.setattr(
        gh,
        "assets",
        lambda release: [{**asset, "digest": None} for asset in listed(release)],
    )

    # The pin is not skipped as written. Adding it again fails loudly, for a person
    # to look at, instead of leaving the day without a pin for good.
    with pytest.raises(CreateOnceError, match=PIN_ASSET):
        write_pin(gh, DAY, document, tmp_path)


def test_the_month_copy_copies_the_last_build_byte_for_byte(gh, fake, tmp_path):
    registry(gh, "2026-11-30")
    osv(gh, "2026-11-30")
    run_once(
        gh,
        Path(tempfile.mkdtemp(dir=tmp_path)),
        datetime(2026, 11, 30, 12, tzinfo=UTC),
        config=CONFIG,
        bootstrap=True,
        override_reason=None,
    )
    text = fake.asset_bytes(CURRENT, "manifest.json")
    built = fake.asset_bytes(CURRENT, "catalog.db.zst")
    prior = loads(text.decode())
    (serving,) = prior.assets
    workdir = tmp_path / "month"
    workdir.mkdir()
    fake.calls.clear()

    # In the month of the last build, there is nothing to copy.
    last_moment = datetime(2026, 11, 30, 23, 59, 59, tzinfo=UTC)
    assert copy_month(gh, prior, workdir, last_moment) == []
    assert fake.calls == []

    entries = copy_month(gh, prior, workdir, last_moment + SECOND)

    month = "catalog-month-2026-11"
    release = fake.release(month)
    assert (release["prerelease"], release["make_latest"]) == (True, "false")
    assert [asset["name"] for asset in release["assets"]] == [
        "catalog-2026-11.db.zst",
        "manifest-2026-11.json",
    ]
    assert fake.asset_bytes(month, "catalog-2026-11.db.zst") == built
    assert sha256_of(fake, month, "catalog-2026-11.db.zst") == serving.asset_sha256
    copied = fake.asset_bytes(month, "manifest-2026-11.json")
    assert copied.decode() == dumps(prior)
    assert copied == text
    digest = hashlib.sha256(copied).hexdigest()
    assert entries == [
        replace(
            serving,
            name="catalog-2026-11.db.zst",
            tag=month,
            period="2026-11",
            closed=True,
        ),
        Asset(
            name="manifest-2026-11.json",
            tag=month,
            role="public",
            format="json",
            asset_bytes=len(copied),
            asset_sha256=digest,
            db_bytes=len(copied),
            db_sha256=digest,
            tables={},
            period="2026-11",
            closed=True,
        ),
    ]
