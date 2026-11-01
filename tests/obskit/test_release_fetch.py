import dataclasses
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from obskit.github import GitHub, NotFound, Transient
from obskit.manifest import Asset, Manifest, ManifestError, TableInfo, Watermark, dumps
from obskit.release import PriorStateError, fetch_prior
from obskit.testing import FakeGitHub
from obskit.zst import compress_file, sha256_file

REPO = "py-observatory/data"
TOKEN = "t0ken"
STREAM = "example"
TAG = "example-current"
TAG_URL = f"https://api.github.com/repos/{REPO}/releases/tags/{TAG}"
DOWNLOADS = f"https://github.com/{REPO}/releases/download/{TAG}/"
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


def pack(
    directory: Path, name: str, content: bytes, **fields: object
) -> tuple[Path, Asset]:
    """Compress `content` into `directory/name`. Returns the file and its manifest
    entry: a state asset of the current release, unless `fields` say otherwise."""
    directory.mkdir(parents=True, exist_ok=True)
    db = directory / name.removesuffix(".zst")
    db.write_bytes(content)
    packed = directory / name
    compress_file(db, packed)
    entry = Asset(
        name=name,
        tag=TAG,
        role="state",
        format="sqlite+zstd",
        asset_bytes=packed.stat().st_size,
        asset_sha256=sha256_file(packed),
        db_bytes=len(content),
        db_sha256=sha256_file(db),
        tables={"rows": TableInfo(1, "snapshot")},
    )
    return packed, dataclasses.replace(entry, **fields)


def write_manifest(path: Path, assets: list[Asset]) -> Manifest:
    manifest = Manifest(
        stream=STREAM,
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
    path.write_text(dumps(manifest), encoding="utf-8")
    return manifest


@dataclass
class Publish:
    """The files that one publish of the stream uploads, by their final asset names
    (manifest.json included), its manifest, and the bytes of each expanded state."""

    files: dict[str, Path]
    manifest: Manifest
    expanded: dict[str, bytes]


def build(tmp_path: Path, label: str, *names: str) -> Publish:
    """A publish whose state assets are `names`, each holding bytes unique to
    `label`."""
    directory = tmp_path / label
    files: dict[str, Path] = {}
    expanded: dict[str, bytes] = {}
    entries = []
    for name in names:
        expanded[name] = f"{label}: {name}\n".encode() * 1000
        files[name], entry = pack(directory, name, expanded[name])
        entries.append(entry)
    files[MANIFEST] = directory / MANIFEST
    return Publish(files, write_manifest(files[MANIFEST], entries), expanded)


def seed(gh: GitHub, fake: FakeGitHub, files: dict[str, Path]) -> None:
    """Create the release TAG holding each file under its asset name, then forget
    the calls that did it."""
    release = gh.create_release(TAG)
    for name, path in files.items():
        gh.upload(release, name, path)
    fake.calls.clear()


def downloads(fake: FakeGitHub) -> list[str]:
    """The names of the assets downloaded from TAG, in order."""
    return [
        url.removeprefix(DOWNLOADS)
        for method, url, _ in fake.calls
        if method == "GET" and url.startswith(DOWNLOADS)
    ]


def requests(fake: FakeGitHub) -> list[tuple[str, str]]:
    return [(method, url) for method, url, _ in fake.calls]


def contents(workdir: Path) -> list[str]:
    return sorted(path.name for path in workdir.iterdir())


def expanded(workdir: Path, name: str) -> bytes:
    return (workdir / name.removesuffix(".zst")).read_bytes()


# --- "No release yet" needs a 404 and --bootstrap ---


def test_fetch_prior_404_with_bootstrap_returns_none(gh, fake, workdir):
    assert fetch_prior(gh, STREAM, workdir, bootstrap=True) is None

    assert requests(fake) == [("GET", TAG_URL)]
    assert contents(workdir) == []


def test_fetch_prior_404_without_bootstrap_refuses(gh, fake, workdir):
    with pytest.raises(PriorStateError, match="--bootstrap") as caught:
        fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert isinstance(caught.value.__cause__, NotFound), "the 404 is the evidence"
    assert requests(fake) == [("GET", TAG_URL)]
    assert contents(workdir) == []


@pytest.mark.parametrize("bootstrap", [True, False])
def test_fetch_prior_502_fails_closed(gh, fake, workdir, bootstrap):
    # No release exists, so a 404 waits behind the six 502s. They must never be read
    # as "no release yet".
    fake.fail("GET", "/releases/tags/", 502, times=6)

    with pytest.raises(Transient) as caught:
        fetch_prior(gh, STREAM, workdir, bootstrap=bootstrap)

    assert caught.value.status == 502
    assert requests(fake) == [("GET", TAG_URL)] * 6
    assert contents(workdir) == []


class Unreachable:
    """A transport on which every request times out."""

    def __init__(self) -> None:
        self.requests = 0

    def request(self, method, url, headers, body):
        self.requests += 1
        raise TimeoutError("timed out")


@pytest.mark.parametrize("bootstrap", [True, False])
def test_fetch_prior_timeout_fails_closed(workdir, bootstrap):
    transport = Unreachable()
    gh = GitHub(REPO, TOKEN, transport=transport, sleep=no_sleep)

    with pytest.raises(Transient):
        fetch_prior(gh, STREAM, workdir, bootstrap=bootstrap)

    assert transport.requests == 6
    assert contents(workdir) == []


@pytest.mark.parametrize("held", ["nothing", "a publish"])
def test_bootstrap_refused_when_current_exists(gh, fake, tmp_path, workdir, held):
    seed(gh, fake, build(tmp_path, "v1", STATE).files if held == "a publish" else {})

    with pytest.raises(PriorStateError, match="--bootstrap"):
        fetch_prior(gh, STREAM, workdir, bootstrap=True)

    assert requests(fake) == [("GET", TAG_URL)], "it looks no further"
    assert contents(workdir) == []


# --- The manifest ---


@pytest.mark.parametrize("held", ["nothing", "state and an old manifest"])
def test_missing_manifest_refuses(gh, fake, tmp_path, workdir, held):
    old = build(tmp_path, "old", STATE)
    files = {
        STATE: old.files[STATE],
        f"swap-prev-{MANIFEST}": old.files[MANIFEST],  # stale, so never used
    }
    seed(gh, fake, files if held != "nothing" else {})

    with pytest.raises(PriorStateError, match=MANIFEST):
        fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert downloads(fake) == []
    assert contents(workdir) == []


def test_uses_swap_next_manifest_when_manifest_is_missing(gh, fake, tmp_path, workdir):
    # A publish was killed between renaming manifest.json to swap-prev- and
    # swap-next-manifest.json to manifest.json. Every data asset is already in place.
    old = build(tmp_path, "old", STATE)
    new = build(tmp_path, "new", STATE)
    seed(
        gh,
        fake,
        {
            f"swap-prev-{STATE}": old.files[STATE],
            STATE: new.files[STATE],
            f"swap-prev-{MANIFEST}": old.files[MANIFEST],
            f"swap-next-{MANIFEST}": new.files[MANIFEST],
        },
    )

    prior = fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert prior == new.manifest
    assert expanded(workdir, STATE) == new.expanded[STATE]
    assert downloads(fake) == [f"swap-next-{MANIFEST}", STATE]


def test_an_invalid_manifest_propagates(gh, fake, tmp_path, workdir):
    bad = tmp_path / MANIFEST
    bad.write_text('{"schema_version": 1}', encoding="utf-8")
    seed(gh, fake, {MANIFEST: bad})

    with pytest.raises(ManifestError) as caught:
        fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert any(MANIFEST in note and TAG in note for note in caught.value.__notes__)


# --- The state assets ---


def test_fetch_prior_downloads_and_expands_state(gh, fake, tmp_path, workdir):
    src = tmp_path / "src"
    state_file, state = pack(src, STATE, b"state\n" * 1000)
    delta_file, delta = pack(src, DELTA, b"delta\n" * 1000, period="2026-11-03")
    # Two entries that fetch_prior leaves alone: a public asset of the current
    # release, and a state asset (a base copy) that lives in a month release.
    public_file, public = pack(src, "example-summary.db.zst", b"x\n", role="public")
    _, base = pack(
        src,
        "example-state-base-2026-11.db.zst",
        b"base\n" * 1000,
        tag="example-month-2026-11",
        period="2026-11",
        closed=True,
    )
    manifest = write_manifest(src / MANIFEST, [state, delta, public, base])
    seed(
        gh,
        fake,
        {
            STATE: state_file,
            DELTA: delta_file,
            public.name: public_file,
            MANIFEST: src / MANIFEST,
        },
    )

    prior = fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert prior == manifest
    assert expanded(workdir, STATE) == b"state\n" * 1000
    assert expanded(workdir, DELTA) == b"delta\n" * 1000
    assert (workdir / STATE).read_bytes() == state_file.read_bytes()
    assert (workdir / DELTA).read_bytes() == delta_file.read_bytes()
    assert downloads(fake) == [MANIFEST, STATE, DELTA]
    assert contents(workdir) == sorted(
        [MANIFEST, STATE, "example-state.db", DELTA, "example-delta-open.db"]
    )
    assert {method for method, _ in requests(fake)} == {"GET"}, "it only reads"


def test_falls_back_to_swap_prev_with_matching_digest(gh, fake, tmp_path, workdir):
    # A publish was killed between renaming the state to swap-prev- and renaming
    # swap-next- into its place, so no asset has the plain name.
    old = build(tmp_path, "old", STATE)
    new = build(tmp_path, "new", STATE)
    seed(
        gh,
        fake,
        {
            MANIFEST: old.files[MANIFEST],
            f"swap-prev-{STATE}": old.files[STATE],
            f"swap-next-{STATE}": new.files[STATE],
            f"swap-next-{MANIFEST}": new.files[MANIFEST],
        },
    )

    prior = fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert prior == old.manifest
    assert (workdir / STATE).read_bytes() == old.files[STATE].read_bytes()
    assert expanded(workdir, STATE) == old.expanded[STATE]
    assert downloads(fake) == [MANIFEST, f"swap-prev-{STATE}"]


def test_prefers_digest_match_over_name(gh, fake, tmp_path, workdir):
    # A publish was killed between two data renames: the state's plain name already
    # holds the new bytes, while manifest.json still names the old ones, which are in
    # swap-prev-. The delta was not renamed yet.
    old = build(tmp_path, "old", STATE, DELTA)
    new = build(tmp_path, "new", STATE, DELTA)
    seed(
        gh,
        fake,
        {
            MANIFEST: old.files[MANIFEST],
            f"swap-prev-{STATE}": old.files[STATE],
            STATE: new.files[STATE],
            DELTA: old.files[DELTA],
            f"swap-next-{DELTA}": new.files[DELTA],
            f"swap-next-{MANIFEST}": new.files[MANIFEST],
        },
    )

    prior = fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert prior == old.manifest
    assert expanded(workdir, STATE) == old.expanded[STATE]
    assert expanded(workdir, DELTA) == old.expanded[DELTA]
    assert downloads(fake) == [MANIFEST, f"swap-prev-{STATE}", DELTA]


CANDIDATES = [STATE, f"swap-prev-{STATE}", f"swap-next-{STATE}"]


@pytest.mark.parametrize(
    ("matching", "chosen"),
    [
        (CANDIDATES, STATE),
        (CANDIDATES[1:], f"swap-prev-{STATE}"),
        (CANDIDATES[2:], f"swap-next-{STATE}"),
    ],
    ids=["name", "swap-prev", "swap-next"],
)
def test_candidates_are_tried_in_order(gh, fake, tmp_path, workdir, matching, chosen):
    wanted = build(tmp_path, "wanted", STATE)
    other = build(tmp_path, "other", STATE)
    held = {
        name: (wanted if name in matching else other).files[STATE]
        for name in CANDIDATES
    }
    seed(gh, fake, {MANIFEST: wanted.files[MANIFEST], **held})

    fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert downloads(fake) == [MANIFEST, chosen]
    assert expanded(workdir, STATE) == wanted.expanded[STATE]


def test_state_asset_sha_mismatch_refuses(gh, fake, tmp_path, workdir):
    old = build(tmp_path, "old", STATE)
    new = build(tmp_path, "new", STATE)
    other = build(tmp_path, "other", STATE)
    # Each candidate name holds bytes other than those that the manifest names.
    seed(
        gh,
        fake,
        {
            MANIFEST: old.files[MANIFEST],
            STATE: new.files[STATE],
            f"swap-prev-{STATE}": other.files[STATE],
            f"swap-next-{STATE}": new.files[STATE],
        },
    )

    with pytest.raises(PriorStateError, match=STATE):
        fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert downloads(fake) == [MANIFEST], "no state is downloaded"
    assert contents(workdir) == [MANIFEST]


def test_missing_state_asset_refuses(gh, fake, tmp_path, workdir):
    old = build(tmp_path, "old", STATE, DELTA)
    seed(gh, fake, {MANIFEST: old.files[MANIFEST], STATE: old.files[STATE]})

    with pytest.raises(PriorStateError, match=DELTA):
        fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert downloads(fake) == [MANIFEST], "every asset is found before any download"


class Corrupting:
    """A transport that passes each request to `fake`, but flips the first byte of
    every download of the asset `name`."""

    def __init__(self, fake: FakeGitHub, name: str) -> None:
        self.fake = fake
        self.url = DOWNLOADS + name

    def request(self, method, url, headers, body):
        status, reply_headers, data = self.fake.request(method, url, headers, body)
        if url == self.url:
            data = bytes([data[0] ^ 0xFF]) + data[1:]
        return status, reply_headers, data


def test_downloaded_bytes_are_checked(gh, fake, tmp_path, workdir):
    seed(gh, fake, build(tmp_path, "v1", STATE).files)
    # The listed digest matches the manifest, and the bytes that arrive do not.
    corrupted = GitHub(REPO, TOKEN, transport=Corrupting(fake, STATE), sleep=no_sleep)

    with pytest.raises(PriorStateError, match=STATE):
        fetch_prior(corrupted, STREAM, workdir, bootstrap=False)

    assert not (workdir / "example-state.db").exists(), "nothing is expanded"


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


def test_an_asset_without_a_digest_is_not_trusted(gh, fake, tmp_path, workdir):
    seed(gh, fake, build(tmp_path, "v1", STATE).files)
    undigested = GitHub(REPO, TOKEN, transport=Undigested(fake), sleep=no_sleep)

    with pytest.raises(PriorStateError, match=STATE):
        fetch_prior(undigested, STREAM, workdir, bootstrap=False)

    assert downloads(fake) == [MANIFEST], "no state is downloaded"


def test_expanded_db_is_checked(gh, fake, tmp_path, workdir):
    published = build(tmp_path, "v1", STATE)
    [entry] = published.manifest.assets
    wrong = dataclasses.replace(entry, db_sha256="0" * 64)
    write_manifest(published.files[MANIFEST], [wrong])
    seed(gh, fake, published.files)

    with pytest.raises(PriorStateError, match="db_sha256"):
        fetch_prior(gh, STREAM, workdir, bootstrap=False)


def test_files_already_in_workdir_are_never_reused(gh, fake, tmp_path, workdir):
    published = build(tmp_path, "v1", STATE)
    seed(gh, fake, published.files)
    # Left by an earlier run: the right compressed bytes, and a database that a
    # failed expansion cut short.
    (workdir / STATE).write_bytes(published.files[STATE].read_bytes())
    (workdir / "example-state.db").write_bytes(published.expanded[STATE][:100])

    fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert downloads(fake) == [MANIFEST, STATE], "downloaded again"
    assert expanded(workdir, STATE) == published.expanded[STATE]


def test_an_asset_without_zst_is_checked_and_not_expanded(gh, fake, tmp_path, workdir):
    src = tmp_path / "src"
    src.mkdir()
    data = b'{"cursor": "123456"}\n'
    cursor = src / "example-cursor.json"
    cursor.write_bytes(data)
    entry = Asset(
        name=cursor.name,
        tag=TAG,
        role="state",
        format="json",
        asset_bytes=len(data),
        asset_sha256=sha256_file(cursor),
        db_bytes=len(data),
        db_sha256=sha256_file(cursor),
        tables={},
    )
    write_manifest(src / MANIFEST, [entry])
    seed(gh, fake, {cursor.name: cursor, MANIFEST: src / MANIFEST})

    fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert (workdir / cursor.name).read_bytes() == data
    assert contents(workdir) == sorted([MANIFEST, cursor.name])


# --- Errors after the release is found are never "no release" ---


@pytest.mark.parametrize(
    ("needle", "status", "error"),
    [
        ("/assets?", 404, NotFound),
        (f"/{TAG}/{MANIFEST}", 404, NotFound),
        (f"/{TAG}/{STATE}", 404, NotFound),
        ("/assets?", 502, Transient),
        (f"/{TAG}/{STATE}", 502, Transient),
    ],
    ids=[
        "listing 404",
        "manifest 404",
        "state 404",
        "listing 502",
        "state 502",
    ],
)
def test_errors_after_the_release_is_found_propagate(
    gh, fake, tmp_path, workdir, needle, status, error
):
    seed(gh, fake, build(tmp_path, "v1", STATE).files)
    fake.fail("GET", needle, status, times=6)

    with pytest.raises(error) as caught:
        fetch_prior(gh, STREAM, workdir, bootstrap=False)

    assert caught.value.status == status
