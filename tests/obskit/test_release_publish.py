import dataclasses
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from obskit import release
from obskit.github import Fatal, GitHub, NotFound
from obskit.manifest import Asset, Manifest, ManifestError, TableInfo, Watermark, dumps
from obskit.release import (
    AssetLimitError,
    CreateOnceError,
    PriorStateError,
    UploadError,
    add_create_once,
    fetch_prior,
    prepare_asset,
    publish_current,
)
from obskit.tags import TagError
from obskit.testing import FakeGitHub
from obskit.zst import decompress_file, sha256_file

REPO = "py-observatory/data"
TOKEN = "t0ken"
STREAM = "example"
TAG = "example-current"
MONTH = "example-month-2026-11"
API = f"https://api.github.com/repos/{REPO}"
UPLOADS = f"https://uploads.github.com/repos/{REPO}"
MANIFEST = "manifest.json"
STATE = "example-state.db.zst"
DELTA = "example-delta-open.db.zst"


def no_sleep(seconds: float) -> None:
    pass


@pytest.fixture
def fake() -> FakeGitHub:
    return FakeGitHub(REPO)


@pytest.fixture
def gh(fake: FakeGitHub) -> GitHub:
    return GitHub(REPO, TOKEN, transport=fake, sleep=no_sleep)


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    path = tmp_path / "work"
    path.mkdir()
    return path


def make_db(path: Path, label: str, **tables: int) -> Path:
    """A database holding each table of `tables` with that many rows, each row
    naming `label`; one table `rows` of 3 rows when no tables are given."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as con:
        for table, count in (tables or {"rows": 3}).items():
            con.execute(f"CREATE TABLE {table} (k INTEGER PRIMARY KEY, v TEXT)")
            values = [(f"{label}: {table} {i}",) for i in range(count)]
            con.executemany(f"INSERT INTO {table} (v) VALUES (?)", values)
        con.commit()
    return path


def manifest_of(assets: list[Asset], stream: str = STREAM) -> Manifest:
    return Manifest(
        stream=stream,
        producer={
            "repo": REPO,
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "run_id": "1234567890",
            "obskit": "0.1.0",
            "python": "3.14.0",
        },
        generated_at="2026-11-03T01:02:03Z",
        complete=True,
        coverage={},
        data_through=Watermark("123456", "serial"),
        cursor="123456",
        licences=[],
        assets=assets,
    )


@dataclass
class Publish:
    """The state files of one publish by asset name, in upload order, the database
    that each holds, and the publish's manifest."""

    files: dict[str, Path]
    dbs: dict[str, Path]
    manifest: Manifest


def build(tmp_path: Path, label: str, *names: str) -> Publish:
    """A publish of the state assets `names` (STATE when none are given), each
    holding rows unique to `label`, prepared as a stream prepares them. Its manifest
    also lists a base copy in the month release, which never touches -current."""
    directory = tmp_path / label
    directory.mkdir()
    files: dict[str, Path] = {}
    dbs: dict[str, Path] = {}
    entries = []
    for name in names or (STATE,):
        dbs[name] = make_db(directory / "db" / name.removesuffix(".zst"), label)
        files[name], entry = prepare_asset(
            dbs[name], directory, tag=TAG, role="state", classes={"rows": "snapshot"}
        )
        entries.append(entry)
    base = dataclasses.replace(
        entries[0],
        name="example-state-base-2026-11.db.zst",
        tag=MONTH,
        period="2026-11",
        closed=True,
    )
    return Publish(files, dbs, manifest_of([*entries, base]))


def publish(gh: GitHub, p: Publish, *, bootstrap: bool = False) -> None:
    publish_current(gh, STREAM, list(p.files.values()), p.manifest, bootstrap=bootstrap)


def seed(gh: GitHub, fake: FakeGitHub, directory: Path, held: dict[str, bytes]):
    """Create the release TAG holding each of `held`'s bytes under its asset name,
    then forget the calls that did it."""
    directory.mkdir()
    created = gh.create_release(TAG)
    for name, data in held.items():
        path = directory / name
        path.write_bytes(data)
        gh.upload(created, name, path)
    fake.calls.clear()


def listing(fake: FakeGitHub, tag: str = TAG) -> list[dict]:
    """Every asset of the release `tag`, which must exist."""
    found = fake.release(tag)
    assert found is not None, f"there is no release {tag}"
    return found["assets"]


def names(fake: FakeGitHub, tag: str = TAG) -> list[str]:
    return sorted(asset["name"] for asset in listing(fake, tag))


def held(fake: FakeGitHub, name: str, tag: str = TAG) -> bytes:
    return fake.asset_bytes(tag, name)


def asset_url(fake: FakeGitHub, name: str, tag: str = TAG) -> str:
    """The API URL of the asset `name`, which a rename or a delete of it uses."""
    [found] = [asset for asset in listing(fake, tag) if asset["name"] == name]
    return f"{API}/releases/assets/{found['id']}"


def writes(fake: FakeGitHub) -> list[tuple[str, str]]:
    """The POST, PATCH and DELETE requests, in order."""
    return [(method, url) for method, url, _ in fake.calls if method != "GET"]


def uploads(fake: FakeGitHub) -> list[str]:
    """The names given to the uploads, in order."""
    return [
        parse_qs(urlsplit(url).query)["name"][0]
        for method, url, _ in fake.calls
        if method == "POST" and url.startswith(UPLOADS)
    ]


def swap_copies(fake: FakeGitHub) -> list[str]:
    return [
        name for name in names(fake) if name.startswith(("swap-next-", "swap-prev-"))
    ]


def expanded(workdir: Path, name: str) -> bytes:
    return (workdir / name.removesuffix(".zst")).read_bytes()


# --- publish_current: creating -current, and the swap ---


def test_publish_creates_current_on_bootstrap(gh, fake, tmp_path, workdir):
    v1 = build(tmp_path, "v1", STATE, DELTA)

    publish(gh, v1, bootstrap=True)

    created = fake.release(TAG)
    assert created["prerelease"] is True
    assert created["make_latest"] == "false"
    assert names(fake) == sorted([STATE, DELTA, MANIFEST])
    for name, path in v1.files.items():
        assert held(fake, name) == path.read_bytes()
    written = v1.files[STATE].parent / MANIFEST
    assert written.read_text(encoding="utf-8") == dumps(v1.manifest)
    assert held(fake, MANIFEST) == written.read_bytes()
    assert fetch_prior(gh, STREAM, workdir, bootstrap=False) == v1.manifest


def test_publish_without_bootstrap_on_missing_release_refuses(gh, fake, tmp_path):
    v1 = build(tmp_path, "v1")

    with pytest.raises(PriorStateError, match="--bootstrap") as caught:
        publish(gh, v1)

    assert isinstance(caught.value.__cause__, NotFound), "the 404 is the evidence"
    assert writes(fake) == []
    assert fake.release(TAG) is None


def test_bootstrap_refuses_a_release_that_holds_a_published_asset(gh, fake, tmp_path):
    v1 = build(tmp_path, "v1")
    seed(gh, fake, tmp_path / "seed", {f"swap-next-{STATE}": b"x", "notes.txt": b"y"})

    with pytest.raises(PriorStateError, match="notes.txt"):
        publish(gh, v1, bootstrap=True)

    assert writes(fake) == []


def test_bootstrap_clears_an_interrupted_first_publish(gh, fake, tmp_path, workdir):
    # A first publish stopped while uploading, so the release holds swap-next copies
    # and nothing else.
    v1 = build(tmp_path, "v1")
    seed(gh, fake, tmp_path / "seed", {f"swap-next-{STATE}": b"partial"})
    leftover = asset_url(fake, f"swap-next-{STATE}")

    publish(gh, v1, bootstrap=True)

    assert writes(fake)[0] == ("DELETE", leftover)
    assert names(fake) == sorted([STATE, MANIFEST])
    assert fetch_prior(gh, STREAM, workdir, bootstrap=False) == v1.manifest


def test_publish_swaps_and_keeps_prev(gh, fake, tmp_path):
    old = build(tmp_path, "old", STATE, DELTA)
    new = build(tmp_path, "new", STATE, DELTA)
    publish(gh, old, bootstrap=True)

    publish(gh, new)

    final = [STATE, DELTA, MANIFEST]
    assert names(fake) == sorted(final + [f"swap-prev-{name}" for name in final])
    for name in (STATE, DELTA):
        assert held(fake, name) == new.files[name].read_bytes()
        assert held(fake, f"swap-prev-{name}") == old.files[name].read_bytes()
    assert held(fake, MANIFEST) == dumps(new.manifest).encode()
    assert held(fake, f"swap-prev-{MANIFEST}") == dumps(old.manifest).encode()
    assert not [name for name in names(fake) if name.startswith("swap-next-")]


def test_publish_deletes_stale_swaps_first(gh, fake, tmp_path):
    v1, v2, v3 = (build(tmp_path, label) for label in ("v1", "v2", "v3"))
    publish(gh, v1, bootstrap=True)
    publish(gh, v2)
    # A run that stopped while uploading left a swap-next copy beside the swap-prev
    # copies of the publish before it.
    gh.upload(gh.release_by_tag(TAG), f"swap-next-{STATE}", v3.files[STATE])
    stale = {asset_url(fake, name) for name in swap_copies(fake)}
    assert len(stale) == 3
    fake.calls.clear()

    publish(gh, v3)

    done = writes(fake)
    methods = [method for method, _ in done]
    assert methods == ["DELETE"] * 3 + ["POST"] * 2 + ["PATCH"] * 4
    assert {url for _, url in done[:3]} == stale
    assert names(fake) == sorted(
        [STATE, MANIFEST, f"swap-prev-{STATE}", f"swap-prev-{MANIFEST}"]
    )
    assert held(fake, STATE) == v3.files[STATE].read_bytes()


def test_publish_manifest_is_last(gh, fake, tmp_path):
    old = build(tmp_path, "old", STATE, DELTA)
    new = build(tmp_path, "new", STATE, DELTA)
    publish(gh, old, bootstrap=True)
    fake.calls.clear()

    publish(gh, new)

    assert uploads(fake) == [
        f"swap-next-{STATE}",
        f"swap-next-{DELTA}",
        f"swap-next-{MANIFEST}",
    ]
    renames = [url for method, url in writes(fake) if method == "PATCH"]
    # Each name in turn, data first: the old asset moves aside, the new one moves in.
    expected = []
    for name in (STATE, DELTA, MANIFEST):
        expected += [asset_url(fake, f"swap-prev-{name}"), asset_url(fake, name)]
    assert renames == expected
    method, url, _ = fake.calls[-1]
    assert (method, url) == ("PATCH", asset_url(fake, MANIFEST)), "the final call"


# --- A publish that stops part way ---


def test_interrupted_publish_recovers(gh, fake, tmp_path, workdir):
    v1, v2, v3 = (build(tmp_path, label) for label in ("v1", "v2", "v3"))
    publish(gh, v1, bootstrap=True)
    # The run stops between moving the old state aside and moving the new one in.
    fake.fail("PATCH", f"name=swap-next-{STATE}", 422)
    with pytest.raises(Fatal):
        publish(gh, v2)
    assert names(fake) == sorted(
        [f"swap-prev-{STATE}", f"swap-next-{STATE}", MANIFEST, f"swap-next-{MANIFEST}"]
    )

    prior = fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert prior == v1.manifest
    assert expanded(workdir, STATE) == v1.dbs[STATE].read_bytes()
    downloaded = asset_url(fake, f"swap-prev-{STATE}")
    assert ("GET", downloaded) in [(m, u) for m, u, _ in fake.calls]

    publish(gh, v3)

    assert not [name for name in names(fake) if name.startswith("swap-next-")]
    assert held(fake, STATE) == v3.files[STATE].read_bytes()
    assert held(fake, f"swap-prev-{STATE}") == v1.files[STATE].read_bytes()
    assert fetch_prior(gh, STREAM, workdir, bootstrap=False) == v3.manifest


def test_crash_before_manifest_rename_uses_new_manifest(gh, fake, tmp_path, workdir):
    old = build(tmp_path, "old", STATE, DELTA)
    new = build(tmp_path, "new", STATE, DELTA)
    publish(gh, old, bootstrap=True)
    # The run stops after moving manifest.json aside, before its successor moves in.
    fake.fail("PATCH", f"name=swap-next-{MANIFEST}", 422)
    with pytest.raises(Fatal):
        publish(gh, new)
    assert MANIFEST not in names(fake)

    prior = fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert prior == new.manifest
    for name in (STATE, DELTA):
        assert expanded(workdir, name) == new.dbs[name].read_bytes()


def test_crash_between_manifest_renames_is_finished_first(gh, fake, tmp_path, workdir):
    v1, v2, v3 = (build(tmp_path, label) for label in ("v1", "v2", "v3"))
    publish(gh, v1, bootstrap=True)
    fake.fail("PATCH", f"name=swap-next-{MANIFEST}", 422)
    with pytest.raises(Fatal):
        publish(gh, v2)
    live = asset_url(fake, f"swap-next-{MANIFEST}")
    fake.calls.clear()

    publish(gh, v3)

    done = writes(fake)
    assert done[0] == ("PATCH", live), "the interrupted swap is finished first"
    assert ("DELETE", live) not in done
    assert asset_url(fake, f"swap-prev-{MANIFEST}") == live
    assert held(fake, f"swap-prev-{MANIFEST}") == dumps(v2.manifest).encode()
    assert held(fake, MANIFEST) == dumps(v3.manifest).encode()
    assert not [name for name in names(fake) if name.startswith("swap-next-")]
    assert fetch_prior(gh, STREAM, workdir, bootstrap=False) == v3.manifest


def test_second_crash_after_data_rename_keeps_live_state(gh, fake, tmp_path, workdir):
    v1, v2, v3, v4 = (
        build(tmp_path, label, STATE, DELTA) for label in ("v1", "v2", "v3", "v4")
    )
    publish(gh, v1, bootstrap=True)
    # Publish 2 stops between its two data files: the state is swapped, so its name
    # holds bytes that the live manifest (v1) does not name. The delta is not.
    fake.fail("PATCH", f"name={DELTA}", 422)
    with pytest.raises(Fatal):
        publish(gh, v2)
    assert held(fake, STATE) == v2.files[STATE].read_bytes()
    assert held(fake, f"swap-prev-{STATE}") == v1.files[STATE].read_bytes()
    # Publish 3 swaps both data files and stops before it moves manifest.json aside.
    fake.fail("PATCH", f"name={MANIFEST}", 422)
    with pytest.raises(Fatal):
        publish(gh, v3)

    prior = fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert prior == v1.manifest
    for name in (STATE, DELTA):
        assert expanded(workdir, name) == v1.dbs[name].read_bytes()
    publish(gh, v4)
    assert fetch_prior(gh, STREAM, workdir, bootstrap=False) == v4.manifest
    assert not [name for name in names(fake) if name.startswith("swap-next-")]


@pytest.mark.parametrize("copy", ["swap-prev-", "swap-next-"])
def test_repair_never_deletes_the_live_copy(gh, fake, tmp_path, copy):
    # A layout that only a hand edit leaves: the name holds other bytes, the live
    # copy is in one swap name and junk in the other. The other bytes move aside to
    # the junk's swap name, and only the junk is deleted to make room.
    live, other, junk, v2 = (
        build(tmp_path, label) for label in ("live", "other", "junk", "v2")
    )
    spare = {"swap-prev-": "swap-next-", "swap-next-": "swap-prev-"}[copy]
    seed(
        gh,
        fake,
        tmp_path / "seed",
        {
            MANIFEST: dumps(live.manifest).encode(),
            STATE: other.files[STATE].read_bytes(),
            copy + STATE: live.files[STATE].read_bytes(),
            spare + STATE: junk.files[STATE].read_bytes(),
        },
    )
    source = asset_url(fake, copy + STATE)
    junk_url = asset_url(fake, spare + STATE)

    publish(gh, v2)

    assert writes(fake)[0] == ("DELETE", junk_url)
    assert ("DELETE", source) not in writes(fake)
    assert asset_url(fake, f"swap-prev-{STATE}") == source
    assert held(fake, f"swap-prev-{STATE}") == live.files[STATE].read_bytes()
    assert held(fake, STATE) == v2.files[STATE].read_bytes()


def test_publish_refuses_when_no_copy_holds_the_live_bytes(gh, fake, tmp_path):
    # The live manifest still has to be renamed, and the state put back, but the
    # delta has no live copy: every copy is looked for before anything is renamed.
    live, other, v2 = (
        build(tmp_path, label, STATE, DELTA) for label in ("live", "other", "v2")
    )
    seed(
        gh,
        fake,
        tmp_path / "seed",
        {
            f"swap-next-{MANIFEST}": dumps(live.manifest).encode(),
            f"swap-prev-{STATE}": live.files[STATE].read_bytes(),
            DELTA: other.files[DELTA].read_bytes(),
            f"swap-next-{DELTA}": other.files[DELTA].read_bytes(),
        },
    )

    with pytest.raises(PriorStateError, match=DELTA):
        publish(gh, v2)

    assert writes(fake) == []


@pytest.mark.parametrize(
    "held_names",
    [[], [STATE, f"swap-prev-{MANIFEST}"]],
    ids=["empty", "no live manifest"],
)
def test_publish_refuses_a_release_without_a_live_manifest(
    gh, fake, tmp_path, held_names
):
    v1, v2 = build(tmp_path, "v1"), build(tmp_path, "v2")
    data = {
        STATE: v1.files[STATE].read_bytes(),
        f"swap-prev-{MANIFEST}": dumps(v1.manifest).encode(),
    }
    seed(gh, fake, tmp_path / "seed", {name: data[name] for name in held_names})

    with pytest.raises(PriorStateError, match=MANIFEST):
        publish(gh, v2)

    assert writes(fake) == []


def test_publish_refuses_a_foreign_live_manifest(gh, fake, tmp_path):
    v1, v2 = build(tmp_path, "v1"), build(tmp_path, "v2")
    foreign = dataclasses.replace(v1.manifest, stream="other")
    seed(
        gh,
        fake,
        tmp_path / "seed",
        {MANIFEST: dumps(foreign).encode(), STATE: v1.files[STATE].read_bytes()},
    )

    with pytest.raises(PriorStateError, match="other"):
        publish(gh, v2)

    assert writes(fake) == []


# --- What publish_current checks before it writes ---


def bad_sha256(m: Manifest) -> Manifest:
    """`m` with an asset whose sha256 the strict reader refuses."""
    first, *rest = m.assets
    spoilt = dataclasses.replace(first, asset_sha256="XYZ")
    return dataclasses.replace(m, assets=[spoilt, *rest])


def int_coverage_key(m: Manifest) -> Manifest:
    """`m` with a coverage key that JSON turns from 1 into "1"."""
    return dataclasses.replace(m, coverage={1: "one"})


@pytest.mark.parametrize(
    "spoil", [bad_sha256, int_coverage_key], ids=["refused", "changed"]
)
def test_publish_refuses_invalid_manifest(gh, fake, tmp_path, spoil):
    v1 = build(tmp_path, "v1")

    with pytest.raises(ManifestError):
        publish_current(
            gh, STREAM, list(v1.files.values()), spoil(v1.manifest), bootstrap=True
        )

    assert writes(fake) == []


def test_publish_refuses_files_that_the_manifest_does_not_list(gh, fake, tmp_path):
    v1, v2 = build(tmp_path, "v1"), build(tmp_path, "v2")

    with pytest.raises(ValueError, match=STATE):
        publish_current(
            gh, STREAM, list(v2.files.values()), v1.manifest, bootstrap=True
        )

    assert writes(fake) == []


@pytest.mark.parametrize("reader", ["strict", "lenient"])
def test_publish_refuses_duplicate_current_entry(
    gh, fake, tmp_path, monkeypatch, reader
):
    # As when a stream builds its next manifest from the prior assets plus the new
    # entries: the state is listed twice, and only the last entry matches the file.
    old, new = build(tmp_path, "old"), build(tmp_path, "new")
    stale = old.manifest.assets[0]
    repeated = dataclasses.replace(new.manifest, assets=[stale, *new.manifest.assets])
    if reader == "lenient":
        # The files check refuses it on its own, should a reader let it through.
        monkeypatch.setattr(release, "loads", lambda text: repeated)

    with pytest.raises(ValueError, match=STATE):
        publish_current(gh, STREAM, list(new.files.values()), repeated, bootstrap=True)

    assert fake.calls == []


def test_publish_checks_the_stream_id(gh, fake, tmp_path):
    v1 = build(tmp_path, "v1")

    with pytest.raises(TagError):
        publish_current(
            gh, "Not A Stream", list(v1.files.values()), v1.manifest, bootstrap=True
        )

    assert fake.calls == []


def test_publish_refuses_before_uploading_at_the_asset_limit(
    gh, fake, tmp_path, monkeypatch
):
    v1, v2 = build(tmp_path, "v1"), build(tmp_path, "v2")
    publish(gh, v1, bootstrap=True)
    publish(gh, v2)
    fake.calls.clear()
    # After the swap-prev copies go, the state and the manifest stay, and the next
    # publish adds two more: four in all.
    monkeypatch.setattr(release, "MAX_ASSETS", 4)

    with pytest.raises(AssetLimitError):
        publish(gh, build(tmp_path, "v3"))

    assert uploads(fake) == []


# --- add_create_once ---


def small_files(directory: Path, **contents: bytes) -> list[Path]:
    directory.mkdir(parents=True)
    paths = []
    for stem, data in contents.items():
        paths.append(directory / f"{stem}.db.zst")
        paths[-1].write_bytes(data)
    return paths


def test_create_once_idempotent_same_content(gh, fake, tmp_path):
    files = small_files(tmp_path / "files", a=b"first", b=b"second")

    first = add_create_once(gh, STREAM, MONTH, files)
    fake.calls.clear()
    again = add_create_once(gh, STREAM, MONTH, files)

    assert writes(fake) == [], "nothing is created or uploaded the second time"
    assert [asset["name"] for asset in first] == ["a.db.zst", "b.db.zst"]
    assert again == first
    created = fake.release(MONTH)
    assert created["prerelease"] is True
    assert created["make_latest"] == "false"
    assert names(fake, MONTH) == ["a.db.zst", "b.db.zst"]
    assert held(fake, "a.db.zst", MONTH) == b"first"


def test_create_once_refuses_different_content(gh, fake, tmp_path):
    [original] = small_files(tmp_path / "one", a=b"first")
    add_create_once(gh, STREAM, MONTH, [original])
    fake.calls.clear()
    new, changed = small_files(tmp_path / "two", b=b"new", a=b"changed")

    with pytest.raises(CreateOnceError, match="a.db.zst"):
        add_create_once(gh, STREAM, MONTH, [new, changed])

    assert writes(fake) == [], "every file is checked before any upload"
    assert held(fake, "a.db.zst", MONTH) == b"first"


class Undigested:
    """A transport that passes each request to `fake`, but lists every asset with a
    null digest, as GitHub does for an asset that it has no digest for."""

    def __init__(self, fake: FakeGitHub) -> None:
        self.fake = fake

    def request(self, method, url, headers, body):
        status, reply_headers, data = self.fake.request(method, url, headers, body)
        if "/assets?" in url and status == 200:
            listed = [{**asset, "digest": None} for asset in json.loads(data)]
            data = json.dumps(listed).encode()
        return status, reply_headers, data


def test_create_once_refuses_an_asset_without_a_digest(gh, fake, tmp_path):
    files = small_files(tmp_path / "files", a=b"first")
    add_create_once(gh, STREAM, MONTH, files)
    fake.calls.clear()
    undigested = GitHub(REPO, TOKEN, transport=Undigested(fake), sleep=no_sleep)

    with pytest.raises(CreateOnceError, match="a.db.zst"):
        add_create_once(undigested, STREAM, MONTH, files)

    assert writes(fake) == []


def test_create_once_refuses_current_tag(gh, fake, tmp_path):
    files = small_files(tmp_path / "files", a=b"first")

    with pytest.raises(ValueError, match=TAG) as caught:
        add_create_once(gh, STREAM, TAG, files)

    assert type(caught.value) is ValueError, "the tag is a tag of the stream"
    assert fake.calls == []


def test_create_once_refuses_foreign_tag(gh, fake, tmp_path):
    files = small_files(tmp_path / "files", a=b"first")

    with pytest.raises(TagError):
        add_create_once(gh, "pypi-registry", "other-month-2026-11", files)

    assert fake.calls == []


def test_create_once_refuses_an_empty_list(gh, fake):
    # With nothing to add, it would still create the release.
    with pytest.raises(ValueError, match="nothing to add"):
        add_create_once(gh, STREAM, MONTH, [])

    assert fake.calls == []


def fill(fake: FakeGitHub, created: dict, count: int) -> None:
    """Give the release `created` `count` small assets, sent straight to the fake so
    that no write pacing is involved."""
    for i in range(count):
        url = f"{UPLOADS}/releases/{created['id']}/assets?name=filler-{i:03d}.txt"
        status, _, _ = fake.request("POST", url, {}, b"filler")
        assert status == 201


def test_asset_count_limit(gh, fake, tmp_path):
    fill(fake, gh.create_release(MONTH), 899)
    fake.calls.clear()
    files = small_files(tmp_path / "files", a=b"first")

    with pytest.raises(AssetLimitError, match="900"):
        add_create_once(gh, STREAM, MONTH, files)

    assert writes(fake) == []


def test_asset_count_warning(gh, fake, tmp_path, monkeypatch):
    fill(fake, gh.create_release(MONTH), 2)
    monkeypatch.setattr(release, "WARN_ASSETS", 3)
    files = small_files(tmp_path / "files", a=b"first")

    with pytest.warns(UserWarning, match=MONTH):
        add_create_once(gh, STREAM, MONTH, files)

    assert "a.db.zst" in names(fake, MONTH)


# --- prepare_asset ---


def test_prepare_asset_counts_rows_and_hashes(tmp_path, workdir):
    db = make_db(tmp_path / "src" / "example-state.db", "v1", projects=3, events=2)
    classes = {"projects": "snapshot", "events": "events", "absent": "append"}

    packed, asset = prepare_asset(
        db,
        workdir,
        tag=MONTH,
        role="state",
        classes=classes,
        period="2026-11",
        closed=True,
    )

    assert packed == workdir / "example-state.db.zst"
    assert asset == Asset(
        name="example-state.db.zst",
        tag=MONTH,
        role="state",
        format="sqlite+zstd",
        asset_bytes=packed.stat().st_size,
        asset_sha256=sha256_file(packed),
        db_bytes=db.stat().st_size,
        db_sha256=sha256_file(db),
        tables={"events": TableInfo(2, "events"), "projects": TableInfo(3, "snapshot")},
        period="2026-11",
        closed=True,
    )
    decompress_file(packed, tmp_path / "round-trip.db")
    assert (tmp_path / "round-trip.db").read_bytes() == db.read_bytes()
    _, plain = prepare_asset(db, workdir, tag=TAG, role="public", classes=classes)
    assert (plain.tag, plain.role, plain.period, plain.closed) == (
        TAG,
        "public",
        None,
        False,
    )


def test_prepare_asset_requires_class_for_every_table(tmp_path, workdir):
    db = make_db(tmp_path / "src" / "example-state.db", "v1", projects=3, events=2)

    with pytest.raises(ValueError, match="events"):
        prepare_asset(
            db, workdir, tag=TAG, role="state", classes={"projects": "snapshot"}
        )

    assert list(workdir.iterdir()) == [], "nothing is compressed"


def test_asset_size_limit(tmp_path, workdir, monkeypatch):
    db = make_db(tmp_path / "src" / "example-state.db", "v1")
    monkeypatch.setattr(release, "MAX_ASSET_BYTES", 10)

    with pytest.raises(AssetLimitError, match="example-state.db.zst"):
        prepare_asset(db, workdir, tag=TAG, role="state", classes={"rows": "snapshot"})


def test_asset_size_warning(tmp_path, workdir, monkeypatch):
    db = make_db(tmp_path / "src" / "example-state.db", "v1")
    monkeypatch.setattr(release, "WARN_ASSET_BYTES", 10)

    with pytest.warns(UserWarning, match="example-state.db.zst"):
        packed, _ = prepare_asset(
            db, workdir, tag=TAG, role="state", classes={"rows": "snapshot"}
        )

    assert packed.exists()


# --- Every upload is checked ---


class CorruptingUploads:
    """A transport that passes each request to `fake`, but flips the first byte of
    the body of every upload named `name`, so GitHub stores other bytes."""

    def __init__(self, fake: FakeGitHub, name: str) -> None:
        self.fake = fake
        self.name = name

    def request(self, method, url, headers, body):
        if method == "POST" and url.startswith(UPLOADS):
            if parse_qs(urlsplit(url).query)["name"] == [self.name]:
                body = bytes([body[0] ^ 0xFF]) + body[1:]
        return self.fake.request(method, url, headers, body)


def test_publish_checks_each_upload(fake, tmp_path):
    corrupting = CorruptingUploads(fake, f"swap-next-{STATE}")
    corrupted = GitHub(REPO, TOKEN, transport=corrupting, sleep=no_sleep)

    with pytest.raises(UploadError, match=STATE):
        publish(corrupted, build(tmp_path, "v1"), bootstrap=True)

    assert uploads(fake) == [f"swap-next-{STATE}"], "nothing more is uploaded"
    assert names(fake) == [f"swap-next-{STATE}"], "nothing is renamed"


def test_create_once_checks_each_upload(fake, tmp_path):
    files = small_files(tmp_path / "files", a=b"first", b=b"second")
    corrupted = GitHub(
        REPO, TOKEN, transport=CorruptingUploads(fake, "a.db.zst"), sleep=no_sleep
    )

    with pytest.raises(UploadError, match="a.db.zst"):
        add_create_once(corrupted, STREAM, MONTH, files)

    assert uploads(fake) == ["a.db.zst"], "nothing more is uploaded"
