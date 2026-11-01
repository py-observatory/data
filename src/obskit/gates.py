"""Gates: a stream's guard against publishing less history than the run before it.

`check` compares the manifest of the last publish with the one about to go out, and
returns every way that the new one falls short. A failure blocks the publish unless a
person gives a reason to override it. This module finds the failures, and defines the
`GateError` that a run raises to stop: the stream's run applies the block and the
override.
"""

import re
from dataclasses import dataclass

from obskit.manifest import Asset, Manifest, TableInfo

# The digits are `[0-9]` and not `\d`, which also matches the digits of other scripts.
_SERIAL = re.compile(r"[0-9]+")


@dataclass(frozen=True)
class GateFailure:
    """One way in which the new manifest holds less than the prior one."""

    rule: str  # cursor, data_through, closed-kept or rows
    detail: str  # what changed, in words for a person


class GateError(RuntimeError):
    """What the run would publish fails a check, so nothing was published to the
    current release. `failures` lists each failure."""

    def __init__(self, failures: list[GateFailure]) -> None:
        self.failures = list(failures)
        listed = "; ".join(f"{failure.rule}: {failure.detail}" for failure in failures)
        super().__init__(f"nothing was published, because the run fails: {listed}")


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

    - cursor: when `prior` has one, `new` has one, and it does not go back. Cursors
      compare as whole numbers.
    - data_through: when both watermarks have the same granularity, the value does not
      go back. Serials compare as whole numbers, and days, months and instants as
      text, which puts them in time order. Watermarks of different granularities are
      not compared.
    - closed-kept: every closed asset of `prior` is in `new` with the same tag, name
      and asset_sha256, and is still closed.
    - rows: every asset of `prior` that is not closed comes back in `new` under the
      same tag and name. One with no period, like the state database, comes back with
      no period and has not lost rows: an append or events table loses none, a
      snapshot table loses at most `allowed_shrink[table]` (none when the table is not
      in `allowed_shrink`), and a derived table is not checked. A table that is gone
      has lost all its rows, and so has every table of an asset that is not there.
      Each table is judged by its history class in `prior`, so calling it another
      class does not excuse a loss. One with a period, like the open delta, must come
      back with the same period or a later one (periods compare as text, like
      watermarks), and fails when it does not, whatever its tables hold. With the same
      period it is checked as above. With a later period, the open delta of a new day,
      it is not compared.

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
    if prior.cursor is None:
        return []
    if new.cursor is None:
        return [
            GateFailure(
                "cursor",
                f"the cursor is missing from the new manifest: it was {prior.cursor}",
            )
        ]
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


def _by_tag_and_name(manifest: Manifest) -> dict[tuple[str, str], Asset]:
    """The assets of `manifest` by tag and name, which tell them apart."""
    return {(asset.tag, asset.name): asset for asset in manifest.assets}


def _closed_kept(prior: Manifest, new: Manifest) -> list[GateFailure]:
    held = _by_tag_and_name(new)
    failures: list[GateFailure] = []
    for old in prior.assets:
        if not old.closed:
            continue
        now = held.get((old.tag, old.name))
        if now is None:
            changes = ["is missing from the new manifest"]
        else:
            changes = []
            if now.asset_sha256 != old.asset_sha256:
                changes.append(
                    f"changed from sha256 {old.asset_sha256} to {now.asset_sha256}"
                )
            if not now.closed:
                changes.append("is no longer closed")
        if changes:
            failures.append(
                GateFailure(
                    "closed-kept",
                    f"the closed asset {old.name} of {old.tag} {' and '.join(changes)}",
                )
            )
    return failures


def _rows(
    prior: Manifest, new: Manifest, allowed_shrink: dict[str, int]
) -> list[GateFailure]:
    held = _by_tag_and_name(new)
    failures: list[GateFailure] = []
    for old in prior.assets:
        if old.closed:
            continue
        now = held.get((old.tag, old.name))
        if old.period is None:
            # It has to come back with no period, or it has lost every table.
            failures += _lost_rows(old, now, allowed_shrink)
        elif now is None:
            failures.append(
                GateFailure(
                    "rows",
                    f"the open asset {old.name} of {old.tag} (period {old.period}) "
                    "is missing from the new manifest",
                )
            )
        elif now.period is None or now.period < old.period:
            period = "no period" if now.period is None else f"the period {now.period}"
            failures.append(
                GateFailure(
                    "rows",
                    f"the open asset {old.name} of {old.tag} went back from the "
                    f"period {old.period} to {period}",
                )
            )
        elif now.period == old.period:
            failures += _lost_rows(old, now, allowed_shrink)
        # Otherwise the period is later: a new day for the asset, which is not compared.
    return failures


def _lost_rows(
    old: Asset, now: Asset | None, allowed_shrink: dict[str, int]
) -> list[GateFailure]:
    """One failure for each table of `old` that has lost rows in `now`, the asset that
    it has to come back as. With no such asset, or one that has another period, every
    table of `old` is gone."""
    tables: dict[str, TableInfo] = {}
    if now is None:
        gone = "the asset is missing from the new manifest"
    elif now.period != old.period:
        gone = (
            f"the asset is missing: the new manifest gives it the period {now.period}"
        )
    else:
        tables, gone = now.tables, "the table is gone"
    where = old.name if old.period is None else f"{old.name} ({old.period})"
    failures: list[GateFailure] = []
    for table, was in old.tables.items():
        if was.history_class == "derived":
            continue
        kept = tables.get(table)
        rows = 0 if kept is None else kept.rows
        # Only a snapshot table may lose rows. A class that is none of the known ones
        # is held to the strictest rule.
        allowed = allowed_shrink.get(table, 0) if was.history_class == "snapshot" else 0
        if was.rows - rows <= allowed:
            continue
        detail = (
            f"the {was.history_class} table {table} in {where} "
            f"went from {was.rows:,} rows to {rows:,}"
        )
        if kept is None:
            detail += f" ({gone})"
        if allowed:
            detail += f", and at most {allowed:,} may go"
        failures.append(GateFailure("rows", detail))
    return failures
