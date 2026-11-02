import json
import math
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from dataclasses import fields
from pathlib import Path

import pytest

from obskit.emails import holds_address
from obskit.http import Http, UpstreamError
from obskit.sqlite import open_db
from obskit.testing import FakeHttp
from osv_pypi.index import Index, instant_key, record_url
from osv_pypi.process import ProcessReport, process_dump, process_index
from osv_pypi.records import VERSION_COLUMNS, parse_record
from osv_pypi.store import DELTA_SCHEMA, STATE_SCHEMA, STATE_TABLES, Delta, apply

FIXTURES = Path(__file__).parent / "fixtures"
GENERATION = 1793526521815797
# When the state was first built, and the time of the runs of these tests, a day later.
BOOTSTRAP_AT = "2026-11-01T06:00:00Z"
OBSERVED_AT = "2026-11-02T06:00:00Z"
# When the versions that a state holds were modified, the time that the index gives an
# advisory that changed since then, and the time of a stale copy of its record, from
# before that change.
STORED = "2026-10-01T08:00:00Z"
INDEXED = "2026-11-01T10:00:00Z"
EARLIER = "2026-11-01T09:00:00Z"
# The advisory of the tests that need only one.
DEMO = "GHSA-4m7q-9xv2-hc3p"


def record(advisory_id: str, modified: str, **edits: object) -> bytes:
    """A record of `advisory_id` as OSV serves it, modified at `modified`: the GHSA
    record in fixtures/ with that id and that time, and the keys of `edits` set."""
    document = json.loads((FIXTURES / "ghsa.json").read_bytes())
    document.update(edits, id=advisory_id, modified=modified)
    return json.dumps(document).encode()


def listing(times: dict[str, str]) -> Index:
    """An index that lists each advisory of `times` at its time."""
    keys = {advisory_id: instant_key(time) for advisory_id, time in times.items()}
    return Index(keys, GENERATION)


def counts(**given: object) -> ProcessReport:
    """A report with the counts given, 0 for the others, and no unparsed record unless
    one is given."""
    zero = {field.name: 0 for field in fields(ProcessReport)}
    return ProcessReport(**(zero | {"unparsed": []} | given))


# The connections that the helpers below open. SQLite warns about a connection that is
# collected while it is open, so each test closes what it opened when it ends.
OPENED: list[sqlite3.Connection] = []


@pytest.fixture(autouse=True)
def close_connections() -> Iterator[None]:
    yield
    while OPENED:
        OPENED.pop().close()


def make_state(
    tmp_path: Path,
    *,
    active: dict[str, str] | None = None,
    removed: dict[str, str] | None = None,
) -> sqlite3.Connection:
    """A state that holds the `active` and the `removed` advisories, each with a version
    modified at its time. It is built as the runs before built it, by applying a delta,
    so it is a state that they could have left."""
    active, removed = active or {}, removed or {}
    state = open_db(tmp_path / "state.db", STATE_SCHEMA, create=True)
    OPENED.append(state)
    state.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?)",
        [("applied_seq", "0"), ("bootstrap_at", BOOTSTRAP_AT)],
    )
    state.commit()
    with closing(open_db(tmp_path / "before.db", DELTA_SCHEMA, create=True)) as conn:
        delta = Delta(conn, BOOTSTRAP_AT)
        for advisory_id, modified in (active | removed).items():
            body = record(advisory_id, modified)
            delta.version(parse_record(body, advisory_id=advisory_id))
        for advisory_id in removed:
            delta.removed(advisory_id)
        conn.commit()
        apply(state, conn)
    return state


def state_rows(state: sqlite3.Connection) -> dict[str, list[tuple]]:
    """Every row of each table of the state, sorted."""
    return {
        table: sorted(state.execute(f"SELECT * FROM {table}")) for table in STATE_TABLES
    }


def advisories(state: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    """Each advisory of the state, with its status and when its version was
    modified."""
    rows = state.execute("SELECT id, status, modified FROM advisories")
    return {advisory_id: (status, modified) for advisory_id, status, modified in rows}


def written(conn: sqlite3.Connection, after_seq: int) -> list[tuple]:
    """Every row of the delta above `after_seq`, in seq order: `("version", id,
    modified)` for a version, `(kind, id)` for a removal or a return, and `("cursor",
    generation)` for a cursor."""
    rows = conn.execute(
        "SELECT seq, 'version', id, modified FROM versions WHERE seq > :after "
        "UNION ALL SELECT seq, kind, id, NULL FROM events WHERE seq > :after "
        "UNION ALL SELECT seq, 'cursor', generation, NULL FROM cursors "
        "WHERE seq > :after ORDER BY seq",
        {"after": after_seq},
    )
    return [
        (kind, what) if modified is None else (kind, what, modified)
        for _, kind, what, modified in rows
    ]


class Clock:
    """A clock that moves only when it is moved, so no test waits for real."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class Ticking:
    """The replies of `fake`, each request taking one second on `clock`. A run spends
    its time budget on requests, so a test can say how many fit in it."""

    def __init__(self, fake: FakeHttp, clock: Clock) -> None:
        self.fake = fake
        self.clock = clock

    def request(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        self.clock.now += 1.0
        return self.fake.request(method, url, headers, body)


class Day:
    """The runs of one day. Each compares an index with the state into the day's open
    delta, and then applies the rows that it wrote, as a run does."""

    def __init__(
        self, tmp_path: Path, state: sqlite3.Connection, fake: FakeHttp
    ) -> None:
        self.state = state
        self.fake = fake
        self.clock = Clock()
        self.http = Http(
            transport=Ticking(fake, self.clock),
            sleep=self.clock.sleep,
            clock=self.clock,
        )
        self.conn = open_db(tmp_path / "delta.db", DELTA_SCHEMA, create=True)
        OPENED.append(self.conn)

    def applied(self) -> int:
        """The highest seq that the state has applied."""
        (value,) = self.state.execute(
            "SELECT value FROM meta WHERE key = 'applied_seq'"
        ).fetchone()
        return int(value)

    def delta(self) -> Delta:
        """The writer of the day's delta for the next run, numbered on from what the
        state has applied, as a run opens it."""
        return Delta(self.conn, OBSERVED_AT, first_seq=self.applied() + 1)

    def run(
        self, index: Index, *, budget: float = math.inf
    ) -> tuple[ProcessReport, list[tuple]]:
        """Compare `index` with the state within `budget` seconds, and apply the delta.
        Returns the report and the rows that this run wrote.

        The delta is applied with `store.apply`, which refuses the whole delta for a
        version that goes back, and for a version of a removed advisory with no return
        before it. So every run of these tests also shows that the state takes each
        row that was written."""
        self.fake.requests.clear()
        applied = self.applied()
        before = state_rows(self.state)
        report = process_index(
            self.state,
            self.delta(),
            self.http,
            index,
            deadline=self.clock() + budget,
            clock=self.clock,
        )
        # The state is read and never written. The run applies the delta after.
        assert state_rows(self.state) == before
        assert not self.state.in_transaction
        self.conn.commit()
        rows = written(self.conn, applied)
        apply(self.state, self.conn)
        return report, rows

    def requested(self) -> list[str]:
        """The URLs that the last run requested, in order."""
        return [url for url, _ in self.fake.requests]


# ---- what the index says of each advisory


def test_new_changed_removed_and_returned(tmp_path):
    # Each id says what its advisory is to the index, and they sort in this order.
    back = "GHSA-back-0000-0001"  # removed, and listed again with a later time
    back_as_it_was = "GHSA-back-0000-0002"  # removed, and listed again as it was
    changed = "GHSA-chng-0000-0001"  # active, and listed with a later time
    gone = "GHSA-gone-0000-0001"  # active, and not listed
    gone_before = "GHSA-gone-0000-0002"  # removed, and not listed
    same = "GHSA-same-0000-0001"  # active, and listed as it was
    new = "MAL-2026-1"  # not held, and listed
    state = make_state(
        tmp_path,
        active={changed: STORED, gone: STORED, same: STORED},
        removed={back: STORED, back_as_it_was: STORED, gone_before: STORED},
    )
    fake = FakeHttp()
    for advisory_id in (back, changed, new):
        fake.add(record_url(advisory_id), body=record(advisory_id, INDEXED))
    day = Day(tmp_path, state, fake)
    # OSV's index lists the newest first, and not by id.
    index = listing(
        {
            new: INDEXED,
            changed: INDEXED,
            back: INDEXED,
            same: STORED,
            back_as_it_was: STORED,
        }
    )

    report, rows = day.run(index)

    # The removals and the returns come first, and need no request. Then the records
    # of the advisories whose entry moved are fetched, by id. A return comes before the
    # version of its advisory, so that the advisory is active when the version
    # applies, and the version counts as a change.
    assert rows == [
        ("returned", back),
        ("returned", back_as_it_was),
        ("removed", gone),
        ("version", back, INDEXED),
        ("version", changed, INDEXED),
        ("version", new, INDEXED),
    ]
    assert day.requested() == [record_url(back), record_url(changed), record_url(new)]
    assert report == counts(new=1, changed=2, removed=1, returned=2)
    assert report.remaining == 0
    assert advisories(state) == {
        back: ("active", INDEXED),
        back_as_it_was: ("active", STORED),
        changed: ("active", INDEXED),
        gone: ("removed", STORED),
        gone_before: ("removed", STORED),
        same: ("active", STORED),
        new: ("active", INDEXED),
    }
    # A version row is the record as parse_record makes it, and a new advisory was
    # first seen by this run.
    columns = ", ".join(VERSION_COLUMNS)
    held = state.execute(
        f"SELECT first_seen, {columns} FROM advisories WHERE id = ?", (new,)
    ).fetchone()
    parsed = parse_record(record(new, INDEXED), advisory_id=new)
    assert held == (OBSERVED_AT, *parsed.values())

    # The state now says what the index says, so the next run has nothing to do.
    report, rows = day.run(index)

    assert rows == []
    assert day.requested() == []
    assert report == counts()


# ---- records that are left for the next run


# What the state holds of DEMO for each group of advisories whose record is fetched,
# and what each run counts: the one that gets a stale copy, and the next one, which
# gets the record at the time that the index gives.
STALE = {
    "new": ({}, counts(stale=1), counts(new=1)),
    "changed": ({"active": {DEMO: STORED}}, counts(stale=1), counts(changed=1)),
    "returned": (
        {"removed": {DEMO: STORED}},
        counts(returned=1, stale=1),
        counts(changed=1),
    ),
}


@pytest.mark.parametrize(
    ("group", "listed_at", "copy_at"),
    [
        pytest.param("new", INDEXED, EARLIER, id="new"),
        pytest.param("changed", INDEXED, EARLIER, id="changed"),
        pytest.param("returned", INDEXED, EARLIER, id="returned"),
        # As text, a whole second sorts after its own fractions, so this copy would
        # pass for one from after the change.
        pytest.param(
            "new",
            "2026-11-01T10:00:00.5Z",
            "2026-11-01T10:00:00Z",
            id="new, older by half a second",
        ),
    ],
)
def test_stale_record_left_pending(tmp_path, group, listed_at, copy_at):
    held, stale_report, next_report = STALE[group]
    state = make_state(tmp_path, **held)
    fake = FakeHttp()
    # The index says that the advisory changed at `listed_at`, but the first copy of
    # its record is older than that.
    fake.add(record_url(DEMO), body=record(DEMO, copy_at), times=1)
    fake.add(record_url(DEMO), body=record(DEMO, listed_at))
    day = Day(tmp_path, state, fake)
    index = listing({DEMO: listed_at})

    report, rows = day.run(index)

    # A stale copy writes no version. A return needs no record, so it is written.
    assert rows == ([("returned", DEMO)] if group == "returned" else [])
    assert day.requested() == [record_url(DEMO)]
    assert report == stale_report
    assert report.remaining == 1

    # The advisory is still to do, so the next run fetches it again.
    report, rows = day.run(index)

    assert rows == [("version", DEMO, listed_at)]
    assert report == next_report
    assert report.remaining == 0


@pytest.mark.parametrize("status", [404, 410])
def test_missing_record_counted(tmp_path, status):
    back, changed, new = "GHSA-back-0000-0001", "GHSA-chng-0000-0001", "MAL-2026-1"
    state = make_state(tmp_path, active={changed: STORED}, removed={back: STORED})
    fake = FakeHttp()
    for advisory_id in (back, changed, new):
        fake.add(record_url(advisory_id), status=status, times=1)
        fake.add(record_url(advisory_id), body=record(advisory_id, INDEXED))
    day = Day(tmp_path, state, fake)
    index = listing({back: INDEXED, changed: INDEXED, new: INDEXED})

    report, rows = day.run(index)

    # The index lists each of them, so a missing record may be a fault that passes.
    # Nothing is written of it, and the next run fetches it again. A return needs no
    # record, so it is written.
    assert rows == [("returned", back)]
    assert len(day.requested()) == 3
    assert report == counts(returned=1, missing=3)
    assert report.remaining == 3

    report, rows = day.run(index)

    assert rows == [
        ("version", back, INDEXED),
        ("version", changed, INDEXED),
        ("version", new, INDEXED),
    ]
    assert report == counts(new=1, changed=2)


def test_unparsed_record_is_skipped_and_named(tmp_path):
    first, second, third = (f"GHSA-{n}{n}{n}{n}-aaaa-aaaa" for n in "234")
    state = make_state(tmp_path)
    fake = FakeHttp()
    fake.add(record_url(first), body=record(first, INDEXED))
    fake.add(record_url(second), body=record(second, INDEXED, withdrawn="yesterday"))
    fake.add(record_url(third), body=record(third, INDEXED))
    day = Day(tmp_path, state, fake)
    index = listing(dict.fromkeys((first, second, third), INDEXED))

    report, rows = day.run(index)

    # The malformed record is skipped, and the others go on. The report names it by
    # its id.
    assert rows == [("version", first, INDEXED), ("version", third, INDEXED)]
    assert report == counts(new=2, unparsed=[second])
    assert report.remaining == 1

    # It is tried again at every run.
    report, rows = day.run(index)

    assert day.requested() == [record_url(second)]
    assert rows == []
    assert report == counts(unparsed=[second])


# Addresses, each with another form of the at, that a refusal cuts short when it shows
# one after 25 letters in an object, since it shows at most 60 characters of a value.
# Each loses the end of its domain and keeps its at. With each is what is shown of it.
CUT_AT_THE_END = {
    "@": ("jane.doe@example.org", "'jane.doe@example..."),
    "%40": ("jane.doe%40example.org", "'jane.doe%40examp..."),
    "[at]": ("jane.doe [at] example [dot] org", "'jane.doe [at] ex..."),
    "(AT)": ("jane.doe (AT) example (DOT) org", "'jane.doe (AT) ex..."),
    "{ at }": ("jane.doe { at } example { dot } org", "'jane.doe { at } ..."),
    "<at>": ("jane.doe <at> example <dot> org", "'jane.doe <at> ex..."),
}


@pytest.mark.parametrize(
    ("alias", "shown", "kept"),
    [
        # The address is taken out, and the rest of the reason is shown.
        pytest.param(
            {"id": "jane@example.org"}, "jane@example.org", True, id="an address"
        ),
        # Taking out the address in brackets joins what is left into another one.
        pytest.param(
            {"id": "me@<a@b.cc>x.org"},
            "me@<a@b.cc>x.org",
            False,
            id="an address in another",
        ),
        *(
            pytest.param(
                {"a": "a" * 25, "z": address},
                shown,
                False,
                id=f"cut at the end, {form}",
            )
            for form, (address, shown) in CUT_AT_THE_END.items()
        ),
        # A long text is shown by its two ends, and this one is cut after the at.
        pytest.param(
            "x" * 14 + " jane.doe@example.org" + "1" * 165,
            " jane.doe@exa...111",
            False,
            id="cut in the middle",
        ),
        # Python writes a line break as \n where it shows a value, and the filter finds
        # no address where the blanks of its at are written so.
        pytest.param(
            {"id": "jane [\nat\n] x [\xa0dot\xa0] org"},
            r"'jane [\nat\n] x [\xa0dot\xa0] org'",
            False,
            id="blanks written as escapes",
        ),
    ],
)
def test_a_refusal_is_logged_without_an_address(tmp_path, capsys, alias, shown, kept):
    body = record(DEMO, INDEXED, aliases=[alias])
    # The reason why the record is refused shows the bad value, address and all, or as
    # much of it as a refusal shows.
    with pytest.raises(ValueError, match=r"aliases\[0\]") as refusal:
        parse_record(body, advisory_id=DEMO)
    assert shown in str(refusal.value)
    state = make_state(tmp_path)
    fake = FakeHttp()
    fake.add(record_url(DEMO), body=body)
    day = Day(tmp_path, state, fake)

    report, _ = day.run(listing({DEMO: INDEXED}))

    # The log names the record, and says why it was refused only when that can be said
    # with no address and no at left in it, since the log of a public repository is
    # public. Nothing goes to stdout, which a run keeps for its report.
    assert report.unparsed == [DEMO]
    logged = capsys.readouterr()
    assert logged.out == ""
    assert not holds_address(logged.err)
    if kept:
        reason = str(refusal.value).replace(shown, "")
    else:
        reason = "the reason is not shown, as it may hold an email address"
    assert logged.err == f"skipped {DEMO}: {reason}\n"


@pytest.mark.parametrize("status", [304, 403, 500])
def test_a_reply_other_than_200_404_or_410_fails_the_run(tmp_path, status):
    # Http raises for a request that fails, and returns a 304, which answers an ETag
    # that was never sent. Neither says that the record is missing, so either fails
    # the run, which publishes nothing.
    state = make_state(tmp_path)
    fake = FakeHttp()
    fake.add(record_url(DEMO), status=status)
    day = Day(tmp_path, state, fake)

    with pytest.raises(UpstreamError) as caught:
        day.run(listing({DEMO: INDEXED}))

    assert caught.value.status == status
    assert caught.value.url == record_url(DEMO)


# ---- the time budget


def test_deadline_defers_the_rest(tmp_path):
    new = [f"GHSA-aaaa-aaaa-000{n}" for n in range(1, 5)]
    back, gone = "PYSEC-2026-1", "PYSEC-2026-2"  # they sort after the new ones
    state = make_state(tmp_path, active={gone: STORED}, removed={back: STORED})
    fake = FakeHttp()
    for advisory_id in (*new, back):
        fake.add(record_url(advisory_id), body=record(advisory_id, INDEXED))
    day = Day(tmp_path, state, fake)
    # Listed newest first, and not by id, as OSV lists them.
    index = listing(dict.fromkeys((back, *reversed(new)), INDEXED))

    # With no time left, nothing is fetched. A removal and a return need no request,
    # so they are written whatever the time.
    report, rows = day.run(index, budget=0)

    assert rows == [("returned", back), ("removed", gone)]
    assert day.requested() == []
    assert report == counts(removed=1, returned=1, deferred=5)
    assert report.remaining == 5

    # Each request takes a second, and the deadline is checked before each fetch, so
    # the third fetch starts at 2 s and is the last one that starts.
    report, rows = day.run(index, budget=2.5)

    assert rows == [("version", advisory_id, INDEXED) for advisory_id in new[:3]]
    assert report == counts(new=3, deferred=2)
    assert report.remaining == 2

    # The returned advisory is active now, and its record is fetched as a changed
    # one's is.
    report, rows = day.run(index)

    assert rows == [("version", new[3], INDEXED), ("version", back, INDEXED)]
    assert report == counts(new=1, changed=1)


# ---- the dump of the bootstrap


def test_process_dump_writes_listed_records_only(tmp_path):
    listed = "GHSA-dump-0000-0001"
    unlisted = "GHSA-dump-0000-0002"  # in the dump, and not listed
    broken = "GHSA-dump-0000-0003"  # its copy in the dump does not parse
    lagging = "GHSA-dump-0000-0004"  # its copy in the dump is older than the index's
    undumped = "GHSA-dump-0000-0005"  # listed, and not in the dump
    index = listing(dict.fromkeys((listed, broken, lagging, undumped), INDEXED))
    dump = [
        (listed, record(listed, INDEXED)),
        (unlisted, record(unlisted, INDEXED)),
        (broken, record(broken, INDEXED)[:100]),
        (lagging, record(lagging, EARLIER)),
    ]
    state = make_state(tmp_path)
    fake = FakeHttp()
    for advisory_id in (broken, lagging, undumped):
        fake.add(record_url(advisory_id), body=record(advisory_id, INDEXED))
    day = Day(tmp_path, state, fake)

    # The bootstrap writes the records of the dump into its delta, and applies it. A
    # copy that lags is a version all the same, the first that the stream saw.
    assert process_dump(day.delta(), iter(dump), index) == 2
    day.conn.commit()
    assert written(day.conn, 0) == [
        ("version", listed, INDEXED),
        ("version", lagging, EARLIER),
    ]
    apply(state, day.conn)

    # Then it compares the index with the state, which fetches what the dump lacked:
    # the record that did not parse, the newer version of the one that lags, and the
    # record that the dump does not have.
    report, rows = day.run(index)

    assert rows == [
        ("version", broken, INDEXED),
        ("version", lagging, INDEXED),
        ("version", undumped, INDEXED),
    ]
    assert report == counts(new=2, changed=1)


def test_process_dump_lets_a_refused_dump_fail(tmp_path):
    # read_dump raises ValueError for a dump that it refuses as a whole, as
    # parse_record does for one record. Only the parse is caught, so a refused dump
    # fails the run, and is not taken for one record that did not parse.
    def refused_dump() -> Iterator[tuple[str, bytes]]:
        yield DEMO, record(DEMO, INDEXED)
        raise ValueError("the member 'GHSA-dump-0000-0002.json' cannot be read")

    conn = open_db(tmp_path / "delta.db", DELTA_SCHEMA, create=True)
    OPENED.append(conn)
    index = listing({DEMO: INDEXED, "GHSA-dump-0000-0002": INDEXED})

    with pytest.raises(ValueError, match="cannot be read"):
        process_dump(Delta(conn, BOOTSTRAP_AT), refused_dump(), index)
