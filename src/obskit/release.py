"""A stream's releases. `fetch_prior` reads its prior state from `<stream>-current`.

Reading prior state fails closed. An error once read as "no release yet" let a run
publish over good data, so only a confirmed HTTP 404 of the `-current` release, with
`--bootstrap`, means that a stream has no prior state. Any other doubt raises.
"""

from pathlib import Path

from obskit.github import GitHub, NotFound
from obskit.manifest import Asset, Manifest, loads
from obskit.tags import current_tag
from obskit.zst import decompress_file, sha256_file

_MANIFEST = "manifest.json"
# A publish uploads every asset as `swap-next-<name>`, then, data first and the
# manifest last, renames `<name>` to `swap-prev-<name>` and `swap-next-<name>` to
# `<name>`. Wherever one publish stops, the bytes that its surviving manifest names
# are under one of the three names.
_SWAP_NEXT = "swap-next-"
_SWAP_PREV = "swap-prev-"


class PriorStateError(RuntimeError):
    """A stream's prior state is missing or does not match its manifest, or
    `--bootstrap` was asked for on a stream that has published."""


def fetch_prior(
    gh: GitHub, stream: str, workdir: Path, *, bootstrap: bool
) -> Manifest | None:
    """The manifest of the stream's last publish, with its state downloaded.

    Each state asset that the manifest puts in `<stream>-current` is downloaded to
    `workdir/<name>` and expanded to `workdir/<name without .zst>`, and both files are
    checked against the manifest. The release itself is never changed.

    Returns None only when `bootstrap` is set and GitHub answers HTTP 404 for
    `<stream>-current`. Raises PriorStateError when that release is missing without
    `bootstrap` or exists with it, when it holds no manifest, when it holds no copy
    of a state asset with the manifest's sha256, and when a downloaded or expanded
    file does not match the manifest. Every other error propagates, a 404 from any
    later request included.
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
    manifest = _read_manifest(gh, listed, tag, workdir)
    state = [a for a in manifest.assets if a.role == "state" and a.tag == tag]
    # Find every asset before downloading any, so that a missing one fails fast.
    sources = [_find_source(asset, listed) for asset in state]
    for asset, source in zip(state, sources, strict=True):
        _fetch_asset(gh, asset, source, workdir)
    return manifest


def _read_manifest(
    gh: GitHub, listed: dict[str, dict], tag: str, workdir: Path
) -> Manifest:
    """The manifest of the release `tag`, whose assets are `listed` by name.

    It is `manifest.json`. When that is missing, a publish stopped between moving it
    aside and putting its successor in place, and the successor is
    `swap-next-manifest.json`. The manifest is renamed last, so every data asset that
    the successor names is in place by then.
    """
    candidates = (_MANIFEST, _SWAP_NEXT + _MANIFEST)
    name = next((name for name in candidates if name in listed), None)
    if name is None:
        raise PriorStateError(
            f"{tag} holds neither {' nor '.join(candidates)}, so its state is unknown"
        )
    path = workdir / name
    gh.download(listed[name], path)
    try:
        return loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:  # a ManifestError, or bytes that are not UTF-8
        exc.add_note(f"It was read from {name} in the release {tag}.")
        raise


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


def _fetch_asset(gh: GitHub, asset: Asset, source: dict, workdir: Path) -> Path:
    """Download the release asset `source`, which holds the manifest entry `asset`,
    and return the path of the checked file that it expands to.

    The download goes to `workdir/<name>` and must have the entry's `asset_sha256`. A
    `.zst` is expanded to `workdir/<name without .zst>`, which must have the entry's
    `db_sha256`; any other file is checked against `db_sha256` as it is. Both files
    are written afresh every time: a file already in `workdir` may be a partial one
    left by a failed run, so it is never reused.
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
    return expanded
