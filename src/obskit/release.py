"""A stream's releases.

`fetch_prior` reads a stream's prior state from `<stream>-current`, and
`publish_current` swaps the next state into it. `add_create_once` adds files to a
release whose assets are never replaced, and `fetch_assets` reads assets back from any
release. `prepare_asset` compresses a database and describes it for the manifest.

Reading prior state fails closed. An error once read as "no release yet" let a run
publish over good data, so only a confirmed HTTP 404 of the `-current` release, with
`--bootstrap`, means that a stream has no prior state. Any other doubt raises.

Publishing never leaves a gap. Wherever a publish stops, `fetch_prior` still finds the
last good state, and the next publish puts that state back under its own names before
it deletes anything.
"""

import tempfile
import warnings
from pathlib import Path

from obskit.github import GitHub, NotFound
from obskit.manifest import Asset, Manifest, ManifestError, TableInfo, dumps, loads
from obskit.sqlite import table_counts
from obskit.tags import check_tag, current_tag
from obskit.zst import compress_file, decompress_file, sha256_file

# GitHub refuses an asset of 2 GiB and a release of more than 1,000 assets. The kit
# refuses short of both, and warns well before.
MAX_ASSET_BYTES = 2_040_109_465
WARN_ASSET_BYTES = 1_500_000_000
WARN_ASSETS = 500
MAX_ASSETS = 900

_MANIFEST = "manifest.json"
# A publish uploads every asset as `swap-next-<name>`, then, data first and the
# manifest last, renames `<name>` to `swap-prev-<name>` and `swap-next-<name>` to
# `<name>`. Before it uploads, it puts the bytes that the live manifest names back
# under their own names, and only then deletes the swap copies. So wherever
# publishes stop, one after another, the bytes that the live manifest names are under
# one of the three names.
_SWAP_NEXT = "swap-next-"
_SWAP_PREV = "swap-prev-"


class PriorStateError(RuntimeError):
    """A stream's prior state is missing or does not match its manifest, or
    `--bootstrap` was asked for on a stream that has published."""


class AssetLimitError(RuntimeError):
    """An asset would be too large, or a release would hold too many assets."""


class CreateOnceError(RuntimeError):
    """A create-once release already holds an asset of that name with other bytes."""


class UploadError(RuntimeError):
    """GitHub lists an uploaded asset with a digest other than the file's sha256,
    or with none."""


def prepare_asset(
    db: Path,
    workdir: Path,
    *,
    tag: str,
    role: str,
    classes: dict[str, str],
    period: str | None = None,
    closed: bool = False,
) -> tuple[Path, Asset]:
    """Compress the SQLite database `db` to `workdir/<db name>.zst`. Returns that file
    and its manifest entry.

    `classes` gives each table's history class, and a class for a table that `db`
    lacks is ignored. Raises ValueError for a table without a class, before anything
    is compressed, and AssetLimitError for a compressed file over MAX_ASSET_BYTES.
    Warns from WARN_ASSET_BYTES.
    """
    counts = table_counts(db)
    missing = [table for table in counts if table not in classes]
    if missing:
        raise ValueError(
            f"{db} has tables without a history class: {', '.join(missing)}"
        )
    packed = workdir / f"{db.name}.zst"
    compress_file(db, packed)
    size = packed.stat().st_size
    if size > MAX_ASSET_BYTES:
        raise AssetLimitError(
            f"{packed.name} is {size:,} bytes, over the limit of {MAX_ASSET_BYTES:,}"
        )
    if size >= WARN_ASSET_BYTES:
        warnings.warn(
            f"{packed.name} is {size:,} bytes, and assets over {MAX_ASSET_BYTES:,} "
            "bytes are refused",
            stacklevel=2,
        )
    return packed, Asset(
        name=packed.name,
        tag=tag,
        role=role,
        format="sqlite+zstd",
        asset_bytes=size,
        asset_sha256=sha256_file(packed),
        db_bytes=db.stat().st_size,
        db_sha256=sha256_file(db),
        tables={table: TableInfo(n, classes[table]) for table, n in counts.items()},
        period=period,
        closed=closed,
    )


def fetch_prior(
    gh: GitHub, stream: str, workdir: Path, *, bootstrap: bool
) -> Manifest | None:
    """The manifest of the stream's last publish, with its state downloaded.

    Each state asset that the manifest puts in `<stream>-current` is downloaded to
    `workdir/<name>` and expanded to `workdir/<name without .zst>`, and both files are
    checked against the manifest. Both stay: a run copies the downloaded bytes into a
    create-once release, where compressing again could give other bytes. The release
    itself is never changed.

    Returns None only when `bootstrap` is set and GitHub answers HTTP 404 for
    `<stream>-current`. Raises PriorStateError when that release is missing without
    `bootstrap` or exists with it, when the manifest is missing, not what GitHub lists
    or another stream's (see `_read_manifest`), when the release holds no copy of a
    state asset with the manifest's sha256, and when a downloaded or expanded file
    does not match the manifest. Every other error propagates, a 404 from any later
    request included.
    """
    tag = current_tag(stream)
    try:
        release = gh.release_by_tag(tag)
    except NotFound as exc:
        if bootstrap:
            return None
        raise PriorStateError(
            f"{gh.repo} has no release {tag}. Run with --bootstrap only if the stream "
            f"{stream} has never published."
        ) from exc
    if bootstrap:
        raise PriorStateError(
            f"--bootstrap refused: {gh.repo} already has the release {tag}, so the "
            f"stream {stream} has published before."
        )
    listed = {asset["name"]: asset for asset in gh.assets(release)}
    _, manifest = _read_manifest(gh, stream, listed, workdir)
    state = [a for a in manifest.assets if a.role == "state" and a.tag == tag]
    # Find every asset before downloading any, so that a missing one fails fast.
    sources = [_find_source(asset, listed) for asset in state]
    for asset, source in zip(state, sources, strict=True):
        _fetch_asset(gh, asset, source, workdir, keep_download=True)
    return manifest


def fetch_assets(gh: GitHub, assets: list[Asset], workdir: Path) -> list[Path]:
    """Download the manifest entries `assets`, each from the release of its tag, and
    return the checked file that each expands to, in order.

    Each asset is downloaded to `workdir/<name>` and expanded to `workdir/<name
    without .zst>`, and both files are checked against the entry, as `fetch_prior`
    checks the state. Then the download is deleted, before the next asset is
    fetched: a month close fetches a month of deltas, and would otherwise hold each
    one twice. A file that is not a `.zst` is checked as it is, and its own path is
    returned. No release is changed.

    Every asset is found, by its tag and its name, before any is downloaded. Raises
    PriorStateError when a tag has no release (GitHub answers 404), when a release
    holds no asset of the name, and when a downloaded or expanded file does not match
    its entry. Every other error propagates. Raises ValueError, before any request,
    when two entries share a name, because both would be downloaded to one path.
    """
    repeated = _repeated([asset.name for asset in assets])
    if repeated:
        raise ValueError(
            f"more than one asset is named {', '.join(repeated)}, and each is "
            "downloaded to its name"
        )
    listings: dict[str, dict[str, dict]] = {}
    sources = []
    for asset in assets:
        if asset.tag not in listings:
            try:
                release = gh.release_by_tag(asset.tag)
            except NotFound as exc:
                raise PriorStateError(
                    f"{gh.repo} has no release {asset.tag}, which the manifest says "
                    f"holds {asset.name}"
                ) from exc
            listings[asset.tag] = {held["name"]: held for held in gh.assets(release)}
        source = listings[asset.tag].get(asset.name)
        if source is None:
            raise PriorStateError(
                f"{asset.tag} holds no {asset.name}, which the manifest lists in it"
            )
        sources.append(source)
    return [
        _fetch_asset(gh, asset, source, workdir, keep_download=False)
        for asset, source in zip(assets, sources, strict=True)
    ]


def publish_current(
    gh: GitHub, stream: str, files: list[Path], manifest: Manifest, *, bootstrap: bool
) -> None:
    """Swap `files` and `manifest` into `<stream>-current`. The release then holds
    each file under its own name and `manifest` as manifest.json, and the assets that
    they replace under `swap-prev-` names.

    Before any request, it checks the stream id (TagError), that `manifest` reads back
    as itself through the strict reader that the next run uses (ManifestError), and
    that the manifest's entries for this release are exactly `files`, each listed
    once, by name and sha256 (ValueError). A manifest that fails either check would
    stop every later run. It writes the manifest to `files[0].parent /
    "manifest.json"`.

    The release must exist without `bootstrap`. With it, the release is created when
    GitHub answers 404, and refused when it holds any asset that is not a swap copy
    (PriorStateError). Without it, the release is first put back to its live
    manifest (see `_repair`). Then every swap copy is deleted. It refuses the publish
    before uploading if the release would hold MAX_ASSETS assets (AssetLimitError),
    and warns from WARN_ASSETS.

    Each file is uploaded as `swap-next-<name>`, and the manifest last, and each
    upload must have its file's sha256 (UploadError). Then, data first and the
    manifest last, it renames `<name>` to `swap-prev-<name>` and `swap-next-<name>`
    to `<name>`.
    """
    tag = current_tag(stream)
    check_tag(stream, tag)
    text = _manifest_text(manifest)
    digests = _file_digests(files, manifest, tag)
    manifest_file = files[0].parent / _MANIFEST
    manifest_file.write_bytes(text.encode())
    digests[_MANIFEST] = sha256_file(manifest_file)

    try:
        release = gh.release_by_tag(tag)
    except NotFound as exc:
        if not bootstrap:
            raise PriorStateError(
                f"{gh.repo} has no release {tag}. Run with --bootstrap only if the "
                f"stream {stream} has never published."
            ) from exc
        release = gh.create_release(tag, prerelease=True, latest=False)
        listed: dict[str, dict] = {}
    else:
        listed = {asset["name"]: asset for asset in gh.assets(release)}
        if bootstrap:
            published = sorted(name for name in listed if not _is_swap(name))
            if published:
                raise PriorStateError(
                    f"--bootstrap refused: {tag} already holds {', '.join(published)},"
                    f" so the stream {stream} has published before."
                )
        else:
            _repair(gh, stream, listed)
        for name in [name for name in listed if _is_swap(name)]:
            gh.delete(listed.pop(name))
    # Every file and the manifest join the assets that are left.
    _check_count(tag, len(listed) + len(digests))

    uploaded: dict[str, dict] = {}
    for path in [*files, manifest_file]:
        swap_name = _SWAP_NEXT + path.name
        uploaded[path.name] = _upload(gh, release, swap_name, path, digests[path.name])
    for name, asset in uploaded.items():
        if name in listed:
            gh.rename(listed[name], _SWAP_PREV + name)
        gh.rename(asset, name)


def add_create_once(gh: GitHub, stream: str, tag: str, files: list[Path]) -> list[dict]:
    """Add `files` to the create-once release `tag` of `stream`, creating it (a
    prerelease, not Latest) when GitHub answers 404. Returns each file's release
    asset, in order.

    An asset is never replaced. A file whose name the release already holds with the
    same sha256 is skipped. With other bytes, or with no digest to compare, it raises
    CreateOnceError. Every file is checked before anything is uploaded, and each
    upload must have its file's sha256 (UploadError).

    Before any request, it raises TagError unless `tag` is a tag of `stream`, and
    ValueError for the stream's `-current` tag, which is swapped and never
    create-once, and for no files, which would still create the release. Before
    uploading, it raises AssetLimitError if the release would hold MAX_ASSETS assets,
    and warns from WARN_ASSETS.
    """
    check_tag(stream, tag)
    if tag == current_tag(stream):
        raise ValueError(
            f"{tag} is swapped on each publish, so it is never create-once"
        )
    if not files:
        raise ValueError(f"there is nothing to add to {tag}")
    _check_names(files)
    digests = {path.name: sha256_file(path) for path in files}
    try:
        release = gh.release_by_tag(tag)
    except NotFound:
        release = gh.create_release(tag, prerelease=True, latest=False)
    listed = {asset["name"]: asset for asset in gh.assets(release)}
    for path in files:
        held = listed.get(path.name)
        if held is not None and held.get("digest") != f"sha256:{digests[path.name]}":
            raise CreateOnceError(
                f"{tag} already holds {path.name} with the digest {held.get('digest')},"
                f" but {path} has the sha256 {digests[path.name]}, and a create-once "
                "asset is never replaced"
            )
    new = [path for path in files if path.name not in listed]
    _check_count(tag, len(listed) + len(new))
    for path in new:
        listed[path.name] = _upload(gh, release, path.name, path, digests[path.name])
    return [listed[path.name] for path in files]


def _is_swap(name: str) -> bool:
    return name.startswith((_SWAP_NEXT, _SWAP_PREV))


def _repeated(names: list[str]) -> list[str]:
    """The names that occur more than once in `names`, sorted."""
    return sorted({name for name in names if names.count(name) > 1})


def _check_names(files: list[Path]) -> None:
    """Raise ValueError when two of `files` share a name."""
    repeated = _repeated([path.name for path in files])
    if repeated:
        raise ValueError(f"more than one file is named {', '.join(repeated)}")


def _manifest_text(manifest: Manifest) -> str:
    """The JSON text of `manifest`. Raises ManifestError unless the strict reader
    reads it back as `manifest`."""
    text = dumps(manifest)
    try:
        again = loads(text)
    except ManifestError as exc:
        exc.add_note("It is the manifest to publish, and nothing was published.")
        raise
    if again != manifest:
        raise ManifestError(
            "the manifest to publish reads back from its JSON as a different "
            "manifest, so nothing was published"
        )
    return text


def _file_digests(files: list[Path], manifest: Manifest, tag: str) -> dict[str, str]:
    """The sha256 of each of `files`, by name.

    Raises ValueError when there is no file, when two share a name, when a name is
    manifest.json or starts with a swap prefix, and when the entries of `manifest`
    for the release `tag` are not exactly the files with their sha256, each listed
    once.
    """
    if not files:
        raise ValueError("there is nothing to publish")
    _check_names(files)
    reserved = [
        path.name for path in files if path.name == _MANIFEST or _is_swap(path.name)
    ]
    if reserved:
        raise ValueError(
            f"{', '.join(reserved)}: a file to publish may not be named {_MANIFEST} or "
            f"start with {_SWAP_NEXT} or {_SWAP_PREV}"
        )
    digests = {path.name: sha256_file(path) for path in files}
    listed = [asset for asset in manifest.assets if asset.tag == tag]
    repeated = _repeated([asset.name for asset in listed])
    if repeated:
        raise ValueError(
            f"the manifest lists {', '.join(repeated)} of {tag} more than once"
        )
    entries = {asset.name: asset.asset_sha256 for asset in listed}
    differ = sorted(
        name
        for name in entries.keys() | digests.keys()
        if entries.get(name) != digests.get(name)
    )
    if differ:
        raise ValueError(
            f"the manifest's entries for {tag} must be the files with their sha256, "
            f"and these differ: {', '.join(differ)}"
        )
    return digests


def _check_count(tag: str, count: int) -> None:
    """Raise AssetLimitError if the release `tag` would hold `count` assets, at
    MAX_ASSETS or more, and warn from WARN_ASSETS."""
    if count >= MAX_ASSETS:
        raise AssetLimitError(
            f"{tag} would hold {count} assets, and a release of {MAX_ASSETS} or more "
            "is refused"
        )
    if count >= WARN_ASSETS:
        warnings.warn(
            f"{tag} will hold {count} assets, and a release of {MAX_ASSETS} or more "
            "is refused",
            stacklevel=3,
        )


def _upload(gh: GitHub, release: dict, name: str, path: Path, sha256: str) -> dict:
    """Upload `path` as the asset `name` of `release`, and raise UploadError unless
    GitHub lists the upload with the digest `sha256`."""
    uploaded = gh.upload(release, name, path)
    if uploaded.get("digest") != f"sha256:{sha256}":
        raise UploadError(
            f"GitHub lists the upload {name} with the digest {uploaded.get('digest')},"
            f" but {path} has the sha256 {sha256}"
        )
    return uploaded


def _repair(gh: GitHub, stream: str, listed: dict[str, dict]) -> None:
    """Put `<stream>-current`, whose assets are `listed` by name, back to its live
    manifest, and keep `listed` up to date.

    A publish that stopped part way can leave the live manifest as
    swap-next-manifest.json, and bytes that it names under a swap name. Both go back
    under their own names here, so deleting the swap copies afterwards loses nothing
    that the live manifest names. The live manifest is renamed to manifest.json. For
    each of its assets in this release, bytes under the name that are not the live
    ones move aside to a swap name, and the live copy moves to the name.

    Raises PriorStateError as `_read_manifest` does, and when no copy of an asset has
    the live sha256. Every copy is found before anything is renamed.
    """
    tag = current_tag(stream)
    with tempfile.TemporaryDirectory() as tmp:
        found, live = _read_manifest(gh, stream, listed, Path(tmp))
    assets = [asset for asset in live.assets if asset.tag == tag]
    for asset in assets:
        _find_source(asset, listed)
    if found != _MANIFEST:
        listed[_MANIFEST] = gh.rename(listed.pop(found), _MANIFEST)
    for asset in assets:
        source = _find_source(asset, listed)["name"]
        if source == asset.name:
            continue
        if asset.name in listed:
            # Move the bytes under the name aside, to the swap name that does not
            # hold the live copy. Whatever is there already is deleted first, which
            # loses nothing that the live manifest names. A stopped publish leaves
            # the live copy in swap-prev-; only a hand edit leaves it in swap-next-.
            other = _SWAP_PREV if source == _SWAP_NEXT + asset.name else _SWAP_NEXT
            aside = other + asset.name
            if aside in listed:
                gh.delete(listed.pop(aside))
            listed[aside] = gh.rename(listed.pop(asset.name), aside)
        listed[asset.name] = gh.rename(listed.pop(source), asset.name)


def _read_manifest(
    gh: GitHub, stream: str, listed: dict[str, dict], directory: Path
) -> tuple[str, Manifest]:
    """The live manifest of `<stream>-current`, whose assets are `listed` by name, and
    the name of the asset that holds it. It is downloaded to `directory/<that name>`.

    The live manifest is `manifest.json`. When that is missing, it is
    `swap-next-manifest.json`, for one of two reasons. A publish stopped between
    moving manifest.json aside and moving its successor in, and then every data asset
    that the successor names is in place. Or the stream's first publish stopped
    before its manifest moved in, and then some of its data may still be under
    `swap-next-` names, where `_find_source` looks too.

    Raises PriorStateError when neither exists, when GitHub lists no digest for it
    (refused before downloading), when the downloaded bytes do not match that digest,
    and when the manifest is another stream's. A manifest that is not valid raises
    ManifestError, or another ValueError for bytes that are not UTF-8, with a note
    naming where it was read from.
    """
    tag = current_tag(stream)
    candidates = (_MANIFEST, _SWAP_NEXT + _MANIFEST)
    name = next((name for name in candidates if name in listed), None)
    if name is None:
        raise PriorStateError(
            f"{tag} holds neither {' nor '.join(candidates)}, so its state is unknown"
        )
    digest = listed[name].get("digest")
    if digest is None:
        raise PriorStateError(
            f"GitHub lists {name} in {tag} without a digest, so it cannot be checked"
        )
    path = directory / name
    gh.download(listed[name], path)
    got = sha256_file(path)
    if digest != f"sha256:{got}":
        raise PriorStateError(
            f"{name} in {tag} arrived with the sha256 {got}, but GitHub lists it with "
            f"the digest {digest}"
        )
    try:
        manifest = loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:  # a ManifestError, or bytes that are not UTF-8
        exc.add_note(f"It was read from {name} in the release {tag}.")
        raise
    if manifest.stream != stream:
        raise PriorStateError(
            f"{name} in {tag} is a manifest of the stream {manifest.stream!r}, not "
            f"{stream!r}"
        )
    return name, manifest


def _find_source(asset: Asset, listed: dict[str, dict]) -> dict:
    """The release asset that holds the bytes of the manifest entry `asset`: the first
    of `<name>`, `swap-prev-<name>` and `swap-next-<name>` whose digest is the entry's
    sha256.

    The digest decides, and not the name. A publish stopped between two renames leaves
    `<name>` holding the new bytes while `manifest.json` still names the old ones,
    which are then in `swap-prev-<name>`.
    """
    digest = f"sha256:{asset.asset_sha256}"
    names = [asset.name, _SWAP_PREV + asset.name, _SWAP_NEXT + asset.name]
    for name in names:
        held = listed.get(name)
        if held is not None and held.get("digest") == digest:
            return held
    found = [
        f"{name} has {listed[name].get('digest')}" for name in names if name in listed
    ]
    detail = "; ".join(found) if found else f"none of {', '.join(names)} exists"
    raise PriorStateError(
        f"{asset.tag} holds no copy of {asset.name} with sha256 {asset.asset_sha256} "
        f"({detail})"
    )


def _fetch_asset(
    gh: GitHub, asset: Asset, source: dict, workdir: Path, *, keep_download: bool
) -> Path:
    """Download the release asset `source`, which holds the manifest entry `asset`,
    and return the path of the checked file that it expands to.

    `source` is downloaded by its id, so the bytes are those of the asset that was
    listed, and not those of whichever asset holds its name by then.

    The download goes to `workdir/<name>` and must have the entry's `asset_sha256`. A
    `.zst` is expanded to `workdir/<name without .zst>`, which must have the entry's
    `db_sha256`, and the download is then deleted unless `keep_download` is set; any
    other file is checked against `db_sha256` as it is. Both files are written afresh
    every time: a file already in `workdir` may be a partial one left by a failed
    run, so it is never reused.
    """
    packed = workdir / asset.name
    gh.download(source, packed)
    got = sha256_file(packed)
    if got != asset.asset_sha256:
        raise PriorStateError(
            f"{source['name']} in {asset.tag} has sha256 {got}, but the manifest "
            f"gives {asset.name} the sha256 {asset.asset_sha256}"
        )
    expanded = workdir / asset.name.removesuffix(".zst")
    if expanded != packed:
        decompress_file(packed, expanded)
    got = sha256_file(expanded)
    if got != asset.db_sha256:
        raise PriorStateError(
            f"{asset.name} in {asset.tag} expands to sha256 {got}, but the manifest "
            f"gives it the db_sha256 {asset.db_sha256}"
        )
    if expanded != packed and not keep_download:
        packed.unlink()
    return expanded
