"""The smallest event-sourced stream, `kit-toy`, as the Store that the tests of
obskit.cycle run the cycle on, and what those tests share: writers of its databases, the
names of its releases and assets, and its releases on a fake GitHub, seeded with what
its runs would have published.

Its state is a table of items, each a key and a value, and the meta that holds the
highest `seq` applied. Its delta is a table of puts, each of which sets an item. The
puts are numbered by `seq` across all of its deltas, as every stream's deltas are.
"""

import sqlite3
from contextlib import closing
from datetime import date
from pathlib import Path

from obskit import cycle
from obskit.github import GitHub
from obskit.manifest import Asset, Manifest, Watermark, dumps, loads
from obskit.release import add_create_once, fetch_prior, prepare_asset, publish_current
from obskit.sqlite import open_db
from obskit.tags import current_tag, month_tag
from obskit.testing import FakeGitHub
from obskit.zst import decompress_file

STATE_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE items (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""
DELTA_SCHEMA = """
CREATE TABLE puts (seq INTEGER PRIMARY KEY, key TEXT NOT NULL, value TEXT NOT NULL);
"""


def apply(state: sqlite3.Connection, delta: sqlite3.Connection) -> None:
    """Upsert the puts of `delta` above the state's applied_seq into its items, in seq
    order, store the highest seq applied as its applied_seq, and commit, which is what
    a Store's `apply` must do."""
    (applied,) = state.execute(
        "SELECT value FROM meta WHERE key = 'applied_seq'"
    ).fetchone()
    puts = delta.execute(
        "SELECT seq, key, value FROM puts WHERE seq > ? ORDER BY seq", (int(applied),)
    ).fetchall()
    state.executemany(
        "INSERT INTO items (key, value) VALUES (?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        [(key, value) for _, key, value in puts],
    )
    top = puts[-1][0] if puts else int(applied)
    state.execute("UPDATE meta SET value = ? WHERE key = 'applied_seq'", (str(top),))
    state.commit()


TOY = cycle.Store(
    stream="kit-toy",
    state_schema=STATE_SCHEMA,
    delta_schema=DELTA_SCHEMA,
    state_tables=("meta", "items"),
    classes={"meta": "snapshot", "items": "snapshot", "puts": "append"},
    licences=[
        {
            "name": "A toy upstream",
            "url": "https://example.org/",
            "licence": "CC0-1.0",
            "terms": "https://example.org/terms",
        }
    ],
    apply=apply,
)

# The releases of the toy stream, and the assets that they hold, by name.
REPO = "py-observatory/data"
CURRENT = "kit-toy-current"
OCTOBER = "kit-toy-month-2026-10"
NOVEMBER = "kit-toy-month-2026-11"
DECEMBER = "kit-toy-month-2026-12"
MANIFEST = "manifest.json"
STATE = "kit-toy-state.db.zst"
DELTA = "kit-toy-delta-open.db.zst"
BASE_OCTOBER = "kit-toy-state-base-2026-10.db.zst"
BASE_NOVEMBER = "kit-toy-state-base-2026-11.db.zst"
BASE_DECEMBER = "kit-toy-state-base-2026-12.db.zst"
PART_OCTOBER = "kit-toy-2026-10.db.zst"
PART_NOVEMBER = "kit-toy-2026-11.db.zst"
MANIFEST_OCTOBER = "manifest-2026-10.json"
MANIFEST_NOVEMBER = "manifest-2026-11.json"

# Who published the seeded manifests.
_PRODUCER = {
    "repo": REPO,
    "commit": "0123456789abcdef0123456789abcdef01234567",
    "run_id": "1234567890",
    "obskit": "0.1.0",
    "python": "3.14.10",
}


def sealed_name(day: str) -> str:
    """The asset of the sealed delta of `day`, given as YYYY-MM-DD."""
    return f"kit-toy-delta-{day}.db.zst"


def write_state(path: Path, items: dict[str, str], *, applied_seq: int) -> Path:
    """Make a state database of the toy stream at `path`, which holds `items` and has
    applied every put up to `applied_seq`."""
    with closing(open_db(path, STATE_SCHEMA, create=True)) as state:
        state.execute(
            "INSERT INTO meta (key, value) VALUES ('applied_seq', ?)",
            (str(applied_seq),),
        )
        state.executemany("INSERT INTO items (key, value) VALUES (?, ?)", items.items())
        state.commit()
    return path


def write_delta(path: Path, puts: list[tuple[int, str, str]]) -> Path:
    """Make a delta database of the toy stream at `path`, which holds `puts`, each as
    its seq, key and value."""
    with closing(open_db(path, DELTA_SCHEMA, create=True)) as delta:
        delta.executemany("INSERT INTO puts (seq, key, value) VALUES (?, ?, ?)", puts)
        delta.commit()
    return path


def no_sleep(seconds: float) -> None:
    """GitHub's retries and pacing wait for nothing in a test."""


class ToyStream:
    """The releases of the toy stream, on a fake GitHub.

    `seed` publishes what the runs of a month would have left, and `roll_over` starts
    a run as a stream does. Each run has a work directory of its own, as each run has a
    new runner, so all that a run leaves to the next goes through the releases. The
    GitHub client is one for the whole test, as a stream's `main` makes one for a
    process.
    """

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.github = FakeGitHub(REPO)
        self.gh = GitHub(REPO, "t0ken", transport=self.github, sleep=no_sleep)
        self.runs = 0
        self.made = 0  # files made by the helpers below, each in a folder of its own

    def seed(
        self, base: dict[str, str], days: dict[date, list[tuple[str, str]]]
    ) -> Manifest:
        """Publish what the runs of one month would have left, and return the manifest.

        `base` is the items of the state as the month began, and `days` the puts of
        each day of the month that had a run, which are numbered from 1 in day order.
        The month's release holds the copy of the state as the month began and the
        sealed delta of each day but the last. The last day's delta is the open delta,
        which the current release holds with the state, which is the base with every
        put applied, and the manifest, which lists them all.
        """
        numbered: list[tuple[date, list[tuple[int, str, str]]]] = []
        seq = 0
        for day, changes in sorted(days.items()):
            puts = []
            for key, value in changes:
                seq += 1
                puts.append((seq, key, value))
            numbered.append((day, puts))
        items = dict(base)
        for _, puts in numbered:
            items.update((key, value) for _, key, value in puts)
        *sealed_days, (last, open_puts) = numbered
        first = numbered[0][0]
        folder = self.folder("seed")
        current = current_tag(TOY.stream)
        month = month_tag(TOY.stream, first)

        base_db = folder / TOY.base_state_name(first.year, first.month)
        closed = [
            prepare_asset(
                write_state(base_db, base, applied_seq=0),
                folder,
                tag=month,
                role="state",
                classes=TOY.classes,
                period=f"{first:%Y-%m}",
                closed=True,
            )
        ]
        for day, puts in sealed_days:
            closed.append(
                prepare_asset(
                    write_delta(folder / TOY.delta_name(day), puts),
                    folder,
                    tag=month,
                    role="public",
                    classes=TOY.classes,
                    period=day.isoformat(),
                    closed=True,
                )
            )
        add_create_once(self.gh, TOY.stream, month, [path for path, _ in closed])

        state = prepare_asset(
            write_state(folder / TOY.state_db, items, applied_seq=seq),
            folder,
            tag=current,
            role="state",
            classes=TOY.classes,
        )
        delta = prepare_asset(
            write_delta(folder / TOY.open_delta_db, open_puts),
            folder,
            tag=current,
            role="state",
            classes=TOY.classes,
            period=last.isoformat(),
        )
        manifest = Manifest(
            stream=TOY.stream,
            producer=_PRODUCER,
            generated_at=f"{last:%Y-%m-%d}T06:00:00Z",
            complete=True,
            coverage={"items": len(items)},
            data_through=Watermark(last.isoformat(), "day"),
            cursor=None,
            licences=TOY.licences,
            assets=[state[1], delta[1], *(entry for _, entry in closed)],
        )
        publish_current(
            self.gh, TOY.stream, [state[0], delta[0]], manifest, bootstrap=True
        )
        return manifest

    def roll_over(self, today: date) -> tuple[Path, list[Asset]]:
        """Start a run on `today` as a stream does: fetch the prior state into a new
        work directory, and roll over. Returns the directory, and the entries of the
        assets that the rollover added to the month releases."""
        workdir = self.workdir()
        prior = fetch_prior(self.gh, TOY.stream, workdir, bootstrap=False)
        carried = cycle.carried(TOY, prior)
        return workdir, cycle.roll_over(self.gh, TOY, workdir, prior, carried, today)

    def workdir(self) -> Path:
        """A new, empty work directory, as each run has a new runner."""
        self.runs += 1
        path = self.tmp_path / f"run-{self.runs}"
        path.mkdir(parents=True)
        return path

    def folder(self, label: str) -> Path:
        self.made += 1
        path = self.tmp_path / label / str(self.made)
        path.mkdir(parents=True)
        return path

    def manifest(self, tag: str = CURRENT, name: str = MANIFEST) -> Manifest:
        return loads(self.github.asset_bytes(tag, name).decode())

    def entries(self) -> dict[str, Asset]:
        """The assets that the current manifest lists, by name."""
        return {asset.name: asset for asset in self.manifest().assets}

    def assets(self, tag: str) -> list[str]:
        """The names of the assets of the release `tag`, sorted."""
        release = self.github.release(tag)
        return sorted(asset["name"] for asset in release["assets"]) if release else []

    def expand(self, tag: str, name: str) -> Path:
        """The database of the asset `name` of `tag`, expanded into a file."""
        packed = self.folder("published") / name
        packed.write_bytes(self.github.asset_bytes(tag, name))
        db = packed.with_suffix("")
        decompress_file(packed, db)
        return db

    def writes(self) -> list[tuple[str, str]]:
        """The requests to GitHub that change something, in order."""
        return [
            (method, url) for method, url, _ in self.github.calls if method != "GET"
        ]

    def delete_asset(self, tag: str, name: str) -> None:
        """Delete the asset `name` of `tag`, as a hand edit would."""
        release = self.gh.release_by_tag(tag)
        held = {asset["name"]: asset for asset in self.gh.assets(release)}
        self.gh.delete(held[name])

    def replace_asset(self, tag: str, name: str, path: Path) -> None:
        """Put the file at `path` in place of the asset `name` of `tag`, as a hand edit
        would."""
        self.delete_asset(tag, name)
        self.gh.upload(self.gh.release_by_tag(tag), name, path)

    def replace_manifest(self, manifest: Manifest) -> None:
        """Put `manifest` in place of the current release's manifest.json."""
        path = self.folder("edited") / MANIFEST
        path.write_text(dumps(manifest), encoding="utf-8")
        self.replace_asset(CURRENT, MANIFEST, path)
