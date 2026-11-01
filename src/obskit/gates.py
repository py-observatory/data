"""Gates: a stream's guard against publishing less history than the run before it.

`check` compares the manifest of the last publish with the one about to go out, and
returns every way that the new one falls short. A failure blocks the publish unless a
person gives a reason to override it. This module only finds the failures: the
stream's run applies the block and the override.
"""

import re
from dataclasses import dataclass

from obskit.manifest import Manifest

# The digits are `[0-9]` and not `\d`, which also matches the digits of other scripts.
_SERIAL = re.compile(r"[0-9]+")


@dataclass(frozen=True)
class GateFailure:
    """One way in which the new manifest holds less than the prior one."""

    rule: str  # cursor, data_through, closed-kept or rows
    detail: str  # what changed, in words for a person


def check(
    prior: Manifest | None,
    new: Manifest,
    *,
    allowed_shrink: dict[str, int] | None = None,
) -> list[GateFailure]:
    """Every rule that `new` breaks against `prior`, in the order cursor, data_through,
    closed-kept and rows. An empty list means that it breaks none of them.

    `prior` is the manifest of the stream's last publish, or None for its first
    publish, which has nothing to fall short of. The rules:

    - cursor: when both manifests have one, it does not go back. Cursors compare as
      whole numbers.
    - data_through: when both watermarks have the same granularity, the value does not
      go back. Serials compare as whole numbers, and days, months and instants as
      text, which puts them in time order. Watermarks of different granularities are
      not compared.
    - closed-kept: every closed asset of `prior` is in `new` with the same tag, name
      and asset_sha256.
    - rows: an asset of `prior` that is not closed, and has an asset of the same name
      and period in `new`, has not lost rows. An append or events table loses none. A
      snapshot table loses at most `allowed_shrink[table]`, and none when the table is
      not in `allowed_shrink`. A derived table is not checked. A table that is gone
      from the asset has lost all its rows. Each table is judged by its history class
      in `prior`, so calling it another class does not excuse a loss. An asset with
      no match, like the open delta of a new day, is not compared.

    A cursor or a serial that is not digits only cannot be compared, and that is a
    failure too.
    """
    if prior is None:
        return []
    return [
        *_cursor(prior, new),
        *_data_through(prior, new),
        *_closed_kept(prior, new),
        *_rows(prior, new, allowed_shrink or {}),
    ]


def _behind(
    rule: str, what: str, was: str, now: str, *, serial: bool
) -> list[GateFailure]:
    """No failure, or one when `now` is behind `was`: as whole numbers when `serial`,
    and as text when not. Serials that are not digits only cannot be compared, and
    give a failure as well."""
    if serial:
        if not (_SERIAL.fullmatch(was) and _SERIAL.fullmatch(now)):
            return [
                GateFailure(
                    rule,
                    f"{what} is not digits only, so it cannot be compared: "
                    f"it went from {was!r} to {now!r}",
                )
            ]
        behind = int(now) < int(was)
    else:
        behind = now < was
    if behind:
        return [GateFailure(rule, f"{what} went back from {was} to {now}")]
    return []


def _cursor(prior: Manifest, new: Manifest) -> list[GateFailure]:
    if prior.cursor is None or new.cursor is None:
        return []
    return _behind("cursor", "the cursor", prior.cursor, new.cursor, serial=True)


def _data_through(prior: Manifest, new: Manifest) -> list[GateFailure]:
    was, now = prior.data_through, new.data_through
    if was.granularity != now.granularity:
        return []
    return _behind(
        "data_through",
        f"data_through ({was.granularity})",
        was.value,
        now.value,
        serial=was.granularity == "serial",
    )


def _closed_kept(prior: Manifest, new: Manifest) -> list[GateFailure]:
    held = {(asset.tag, asset.name): asset for asset in new.assets}
    failures: list[GateFailure] = []
    for old in prior.assets:
        if not old.closed:
            continue
        now = held.get((old.tag, old.name))
        if now is None:
            change = "is missing from the new manifest"
        elif now.asset_sha256 != old.asset_sha256:
            change = f"changed from sha256 {old.asset_sha256} to {now.asset_sha256}"
        else:
            continue
        failures.append(
            GateFailure(
                "closed-kept", f"the closed asset {old.name} of {old.tag} {change}"
            )
        )
    return failures


def _rows(
    prior: Manifest, new: Manifest, allowed_shrink: dict[str, int]
) -> list[GateFailure]:
    matches = {(asset.name, asset.period): asset for asset in new.assets}
    failures: list[GateFailure] = []
    for old in prior.assets:
        now = None if old.closed else matches.get((old.name, old.period))
        if now is None:
            continue
        where = old.name if old.period is None else f"{old.name} ({old.period})"
        for table, was in old.tables.items():
            if was.history_class == "derived":
                continue
            kept = now.tables.get(table)
            rows = 0 if kept is None else kept.rows
            # Only a snapshot table may lose rows. A class that is none of the known
            # ones is held to the strictest rule.
            allowed = (
                allowed_shrink.get(table, 0) if was.history_class == "snapshot" else 0
            )
            if was.rows - rows <= allowed:
                continue
            detail = (
                f"the {was.history_class} table {table} in {where} "
                f"went from {was.rows:,} rows to {rows:,}"
            )
            if kept is None:
                detail += " (the table is gone)"
            if allowed:
                detail += f", and at most {allowed:,} may go"
            failures.append(GateFailure("rows", detail))
    return failures
