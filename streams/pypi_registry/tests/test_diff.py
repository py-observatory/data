import random
from dataclasses import FrozenInstanceError

import pytest

from pypi_registry.diff import (
    EVENT_KINDS,
    Event,
    FileDiff,
    RootDiff,
    classify_probe,
    diff_files,
    diff_root,
)
from pypi_registry.simple import FileInfo, ProjectPage, Root


def file_info(
    filename: str, *, yanked: bool = False, reason: str | None = None
) -> FileInfo:
    """A file of a page. A diff reads only its name and whether it is yanked and why."""
    return FileInfo(
        filename=filename,
        version="1.0",
        size=10,
        upload_time="2024-01-02T03:04:05.000006Z",
        requires_python=None,
        yanked=yanked,
        yanked_reason=reason,
        has_metadata=False,
        has_provenance=False,
    )


def page_of(*files: FileInfo, status: str = "active") -> ProjectPage:
    """A project page with `files` and the given status."""
    return ProjectPage(
        name="demo",
        serial=7,
        status=status,
        status_reason=None,
        files=list(files),
        versions=["1.0"],
    )


EMPTY = RootDiff(new=[], changed=[], vanished=[], returned=[])

# The stored files of a project, as `diff_files` takes them: the name of each file, and
# whether it is yanked and why.
type Stored = dict[str, tuple[bool, str | None]]


# ---- the root


def test_diff_root_new_changed_vanished():
    active = {"kept": 5, "bumped": 5, "gone": 3, "also-gone": 1}
    root = Root(100, {"kept": 5, "bumped": 6, "fresh": 1, "another-new": 9})
    # None of the dicts is in order, and each list comes back sorted.
    assert diff_root(active, {}, root) == RootDiff(
        new=["another-new", "fresh"],
        changed=["bumped"],
        vanished=["also-gone", "gone"],
        returned=[],
    )


def test_returned_names():
    # A quarantined name and a removed name that are listed again are both returned,
    # and neither is new: each has a stored incarnation to continue or to follow, and
    # `inactive` says which. A new name would start at incarnation 1.
    inactive = {"q": "quarantined", "r": "removed"}
    diff = diff_root({}, inactive, Root(100, {"r": 4, "q": 9}))
    assert diff.returned == ["q", "r"]
    assert diff.new == []
    assert diff.changed == []
    assert diff.vanished == []


def test_returned_names_are_told_from_the_rest_of_the_root():
    active = {"kept": 1, "bumped": 1, "gone": 1}
    inactive = {"q": "quarantined", "r": "removed", "still-away": "removed"}
    root = Root(100, {"kept": 1, "bumped": 2, "fresh": 1, "r": 3, "q": 3})
    assert diff_root(active, inactive, root) == RootDiff(
        new=["fresh"], changed=["bumped"], vanished=["gone"], returned=["q", "r"]
    )


def test_an_inactive_name_that_is_not_listed_is_not_reported():
    # Its status is known already, and it is not probed again.
    inactive = {"q": "quarantined", "r": "removed"}
    assert diff_root({}, inactive, Root(100, {})) == EMPTY


def test_a_root_that_matches_the_store_reports_nothing():
    assert diff_root({"a": 1, "b": 2}, {}, Root(100, {"b": 2, "a": 1})) == EMPTY


@pytest.mark.parametrize("listed", [4, 6])
def test_a_serial_that_differs_either_way_is_changed(listed):
    assert diff_root({"a": 5}, {}, Root(100, {"a": listed})).changed == ["a"]


def test_a_serial_of_zero_is_a_serial():
    # 0 is falsy, and a name with that serial is still stored.
    diff = diff_root({"a": 0, "b": 0}, {}, Root(100, {"a": 0, "b": 1}))
    assert diff == RootDiff(new=[], changed=["b"], vanished=[], returned=[])


def test_diff_root_with_an_empty_side():
    assert diff_root({}, {}, Root(100, {"b": 2, "a": 1})) == RootDiff(
        new=["a", "b"], changed=[], vanished=[], returned=[]
    )
    assert diff_root({"b": 2, "a": 1}, {}, Root(100, {})) == RootDiff(
        new=[], changed=[], vanished=["a", "b"], returned=[]
    )
    assert diff_root({}, {}, Root(100, {})) == EMPTY


def test_diff_root_leaves_its_inputs_as_they_were():
    active = {"kept": 5, "gone": 3}
    inactive = {"q": "quarantined"}
    projects = {"kept": 5, "q": 8, "fresh": 1}
    diff_root(active, inactive, Root(100, projects))
    assert active == {"kept": 5, "gone": 3}
    assert inactive == {"q": "quarantined"}
    assert projects == {"kept": 5, "q": 8, "fresh": 1}


def test_diff_root_agrees_with_a_name_by_name_reading_of_the_rules():
    rng = random.Random(2026)
    names = [f"p{n}" for n in range(40)]
    for _ in range(300):
        active = {n: rng.randrange(3) for n in names if rng.random() < 0.35}
        inactive = {
            n: rng.choice(["quarantined", "removed"])
            for n in names
            if n not in active and rng.random() < 0.3
        }
        listed = {n: rng.randrange(3) for n in names if rng.random() < 0.5}
        expected = RootDiff(new=[], changed=[], vanished=[], returned=[])
        for n in sorted(names):
            if n in active and n not in listed:
                expected.vanished.append(n)
            elif n in active and listed[n] != active[n]:
                expected.changed.append(n)
            elif n in inactive and n in listed:
                expected.returned.append(n)
            elif n in listed and n not in active and n not in inactive:
                expected.new.append(n)
        assert diff_root(active, inactive, Root(100, listed)) == expected


# ---- the files of a page


def test_diff_files_added_removed_yank_changes():
    before: Stored = {
        "demo-1.0.tar.gz": (False, None),  # as it was
        "demo-1.1.tar.gz": (False, None),  # yanked since
        "demo-1.2.tar.gz": (True, "broken build"),  # unyanked since
        "demo-1.3.tar.gz": (True, "old reason"),  # still yanked, for another reason
        "demo-1.4.tar.gz": (False, None),  # not on the page any more
    }
    new_sdist = file_info("demo-2.0.tar.gz")
    new_yanked = file_info("demo-2.0-py3-none-any.whl", yanked=True, reason="bad wheel")
    page = page_of(
        file_info("demo-1.0.tar.gz"),
        file_info("demo-1.1.tar.gz", yanked=True, reason="security fix"),
        file_info("demo-1.2.tar.gz"),
        file_info("demo-1.3.tar.gz", yanked=True, reason="new reason"),
        new_sdist,
        new_yanked,
    )
    diff = diff_files(before, page)
    # A file that is first seen yanked is added with its yank, and it is not also a yank
    # change, because its row already shows the yank.
    assert diff.added == [new_yanked, new_sdist]
    assert diff.removed == ["demo-1.4.tar.gz"]
    assert diff.yank_changes == [
        ("demo-1.1.tar.gz", True, "security fix"),
        ("demo-1.2.tar.gz", False, None),
        ("demo-1.3.tar.gz", True, "new reason"),
    ]


@pytest.mark.parametrize(
    ("was", "now"),
    [
        ((False, None), (True, None)),
        ((False, None), (True, "broken build")),
        ((True, None), (False, None)),
        ((True, "broken build"), (False, None)),
        # Still yanked, but for another reason or for none: the stored reason follows
        # the page, so the change is reported as a yank with the reason now there.
        ((True, None), (True, "broken build")),
        ((True, "broken build"), (True, None)),
        ((True, "broken build"), (True, "other")),
    ],
)
def test_each_change_of_yank_or_reason_is_a_yank_change(was, now):
    page = page_of(file_info("demo-1.0.tar.gz", yanked=now[0], reason=now[1]))
    diff = diff_files({"demo-1.0.tar.gz": was}, page)
    assert diff == FileDiff(
        added=[], removed=[], yank_changes=[("demo-1.0.tar.gz", *now)]
    )


@pytest.mark.parametrize("state", [(False, None), (True, None), (True, "broken build")])
def test_a_file_in_the_state_that_is_stored_is_not_a_change(state):
    yanked, reason = state
    page = page_of(file_info("demo-1.0.tar.gz", yanked=yanked, reason=reason))
    assert diff_files({"demo-1.0.tar.gz": state}, page) == FileDiff([], [], [])


def test_every_file_of_a_page_is_added_when_none_is_stored():
    # A project that has just been created, or registered again, has no stored file.
    page = page_of(file_info("demo-1.1.tar.gz"), file_info("demo-1.0.tar.gz"))
    diff = diff_files({}, page)
    assert [f.filename for f in diff.added] == ["demo-1.0.tar.gz", "demo-1.1.tar.gz"]
    assert diff.removed == []
    assert diff.yank_changes == []


def test_every_stored_file_is_removed_when_an_active_page_lists_none():
    # A project can delete all of its releases and stay registered.
    before: Stored = {"demo-1.1.tar.gz": (False, None), "demo-1.0.tar.gz": (True, "x")}
    assert diff_files(before, page_of()) == FileDiff(
        added=[], removed=["demo-1.0.tar.gz", "demo-1.1.tar.gz"], yank_changes=[]
    )


def test_diff_files_of_nothing_against_nothing():
    assert diff_files({}, page_of()) == FileDiff([], [], [])


def test_diff_files_lists_are_sorted_by_filename():
    # Each list holds enough names that a set's own order would not come out sorted by
    # chance, and the inputs are shuffled so that their order does not help either.
    def names(group: str) -> list[str]:
        return [f"{group}-{n}.tar.gz" for n in range(10)]

    rng = random.Random(7)
    unchanged, removed, yanked, added = (
        names(group) for group in ("keep", "gone", "yank", "new")
    )
    stored = [*unchanged, *removed, *yanked]
    on_page = [file_info(name) for name in [*unchanged, *added]] + [
        file_info(name, yanked=True) for name in yanked
    ]
    rng.shuffle(stored)
    rng.shuffle(on_page)
    before: Stored = {name: (False, None) for name in stored}
    diff = diff_files(before, page_of(*on_page))
    assert [f.filename for f in diff.added] == sorted(added)
    assert diff.removed == sorted(removed)
    assert [change[0] for change in diff.yank_changes] == sorted(yanked)


def test_diff_files_leaves_its_inputs_as_they_were():
    before: Stored = {
        "demo-1.0.tar.gz": (False, None),
        "demo-1.1.tar.gz": (False, None),
    }
    page = page_of(
        file_info("demo-1.1.tar.gz", yanked=True), file_info("demo-1.2.tar.gz")
    )
    files = list(page.files)
    diff_files(before, page)
    assert before == {
        "demo-1.0.tar.gz": (False, None),
        "demo-1.1.tar.gz": (False, None),
    }
    assert page.files == files


# ---- the probe of a name that left the root


def test_classify_probe():
    assert classify_probe(404, None) == "removed"
    assert classify_probe(410, None) == "removed"
    assert classify_probe(200, page_of(status="quarantined")) == "quarantined"
    assert classify_probe(200, page_of(file_info("demo-1.0.tar.gz"))) == "present"


@pytest.mark.parametrize("status", ["active", "archived", "deprecated", "frozen"])
def test_a_200_whose_page_is_not_quarantined_is_present(status):
    # A status that this code has not met is kept as written, and it is not quarantine.
    assert classify_probe(200, page_of(status=status)) == "present"


@pytest.mark.parametrize("status", [404, 410])
@pytest.mark.parametrize("status_of_page", ["quarantined", "active"])
def test_the_status_alone_decides_a_missing_page(status, status_of_page):
    # A 404 or 410 has no page to read, and one that comes with it changes nothing.
    assert classify_probe(status, page_of(status=status_of_page)) == "removed"


@pytest.mark.parametrize(
    "status",
    [0, -1, 100, 201, 204, 301, 302, 304, 400, 401, 403, 429, 451, 500, 502, 503, 504],
)
@pytest.mark.parametrize("page", [None, page_of()], ids=["no page", "a page"])
def test_classify_probe_refuses_any_other_status(status, page):
    with pytest.raises(ValueError, match=rf"HTTP status {status}\b"):
        classify_probe(status, page)


def test_classify_probe_refuses_a_200_with_no_page():
    # Without its page a 200 cannot be told from a quarantine, and a guess would be
    # recorded as a fact.
    with pytest.raises(ValueError, match="no page"):
        classify_probe(200, None)


def test_a_probe_error_says_what_was_wrong_and_what_was_expected():
    with pytest.raises(ValueError) as caught:
        classify_probe(503, None)
    assert str(caught.value) == (
        "a probe of a vanished name got HTTP status 503, which is not a page (200) "
        "or a removal (404, 410)"
    )
    with pytest.raises(ValueError) as caught:
        classify_probe(200, None)
    assert str(caught.value) == (
        "a probe of a vanished name got HTTP status 200 and no page to read"
    )


# ---- events


def test_event_kinds():
    assert EVENT_KINDS == (
        "created",
        "removed",
        "quarantined",
        "unquarantined",
        "reregistered",
        "status_changed",
        "file_removed",
        "yanked",
        "unyanked",
    )


def test_an_event_needs_only_its_project_incarnation_and_kind():
    event = Event("demo", 2, "reregistered")
    assert (event.name, event.incarnation, event.kind) == ("demo", 2, "reregistered")
    assert (event.version, event.filename, event.detail, event.source_at) == (
        None,
        None,
        None,
        None,
    )


def test_an_event_takes_its_fields_in_order():
    event = Event(
        "demo", 1, "yanked", "1.0", "demo-1.0.tar.gz", "broken build", "2026-11-01"
    )
    assert event == Event(
        name="demo",
        incarnation=1,
        kind="yanked",
        version="1.0",
        filename="demo-1.0.tar.gz",
        detail="broken build",
        source_at="2026-11-01",
    )


@pytest.mark.parametrize("kind", EVENT_KINDS)
def test_every_listed_kind_makes_an_event(kind):
    assert Event("demo", 1, kind).kind == kind


@pytest.mark.parametrize(
    "kind", ["", "yank", "Created", "removed ", "deleted", "status-changed", None, 3]
)
def test_an_event_of_another_kind_is_refused(kind):
    with pytest.raises(ValueError, match="kind must be one of") as caught:
        Event("demo", 1, kind)
    assert f"not {kind!r}" in str(caught.value)
    assert "unquarantined" in str(caught.value)


# ---- the types


@pytest.mark.parametrize(
    ("value", "field"),
    [
        (Event("demo", 1, "created"), "kind"),
        (EMPTY, "new"),
        (FileDiff([], [], []), "added"),
    ],
)
def test_the_diff_types_are_frozen(value, field):
    with pytest.raises(FrozenInstanceError):
        setattr(value, field, None)
