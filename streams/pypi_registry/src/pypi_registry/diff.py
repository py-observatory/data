"""What changed since the last run, and what a probe of a vanished name found.

A run starts from the Simple API root, which lists every project with its serial.
`diff_root` compares it with the stored projects, to find the projects to fetch (new,
changed and returned) and the projects to probe (vanished). `diff_files` compares a
fetched page with the stored files of its project, and `classify_probe` reads the reply
to the probe of a vanished name.

Events record only what the facts cannot show: that a project was created, removed,
quarantined, released from quarantine or registered again, that its status changed,
that a file went away, and that a file was yanked or unyanked. `Event` is one such
record, and EVENT_KINDS lists its kinds. This module finds what happened and writes
nothing. The run turns what it finds into events and rows.

A name that left the root and is listed again is `returned`, and not new. The status
that was stored for it says how it left. A quarantined project that returns is the same
project, so it keeps its incarnation. A removed name that returns was registered again,
maybe by someone else, so it starts a new incarnation and shares no history with the
one before. The run makes that choice. This module only keeps a returned name out of
`new`, which would start it again from incarnation 1.
"""

from dataclasses import dataclass

from pypi_registry.simple import FileInfo, ProjectPage, Root

EVENT_KINDS = (
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


@dataclass(frozen=True)
class Event:
    """One event in the life of a project.

    `incarnation` is the incarnation of the project that the event is about. `version`
    and `filename` name the release or the file of an event that is about one. `detail`
    is the new status of a `status_changed` event and the reason of a `yanked` event.
    `source_at` is the time that upstream gave, when it gave one.

    Raises ValueError for a kind that is not in EVENT_KINDS.
    """

    name: str  # canonical
    incarnation: int
    kind: str
    version: str | None = None  # canonical
    filename: str | None = None
    detail: str | None = None
    source_at: str | None = None  # as upstream writes it

    def __post_init__(self) -> None:
        if self.kind not in EVENT_KINDS:
            kinds = ", ".join(EVENT_KINDS)
            raise ValueError(
                f"an event's kind must be one of {kinds}, not {self.kind!r}"
            )


@dataclass(frozen=True)
class RootDiff:
    """The names of a root that a run has work for, each list sorted."""

    new: list[str]  # listed, and not stored
    changed: list[str]  # listed and stored as active, with a different serial
    vanished: list[str]  # stored as active, and not listed
    returned: list[str]  # listed, and stored as removed or quarantined


def diff_root(active: dict[str, int], inactive: dict[str, str], root: Root) -> RootDiff:
    """Sort the names of the root and of the store into the four lists of RootDiff.

    `active` maps each stored project whose status is neither `removed` nor
    `quarantined` to its stored serial. `inactive` maps each stored project with one of
    those two statuses to its status. A name is in at most one of them, because a
    project's name is its key.

    A listed name is `returned` when `inactive` has it, `new` when neither map has it,
    and `changed` when `active` has it with another serial. A serial that is lower than
    the stored one counts too: a serial that differs is a page worth fetching again. A
    name of `active` that is not listed is `vanished`. A name of `inactive` that is not
    listed is not reported, because its status is known already.
    """
    listed = root.projects
    new = listed.keys() - active.keys() - inactive.keys()
    returned = listed.keys() & inactive.keys()
    vanished = active.keys() - listed.keys()
    changed = [
        name for name in listed.keys() & active.keys() if listed[name] != active[name]
    ]
    return RootDiff(
        new=sorted(new),
        changed=sorted(changed),
        vanished=sorted(vanished),
        returned=sorted(returned),
    )


@dataclass(frozen=True)
class FileDiff:
    """How a project page differs from the files stored for the project. Each list is
    sorted by filename."""

    added: list[FileInfo]  # on the page, and not stored
    removed: list[str]  # filenames that are stored, and not on the page
    # (filename, yanked, reason) as the page has it, for a stored file whose yank or
    # reason is not the stored one. A file that is not yanked has the reason None.
    yank_changes: list[tuple[str, bool, str | None]]


def diff_files(
    before: dict[str, tuple[bool, str | None]], page: ProjectPage
) -> FileDiff:
    """The files that a project page adds, removes and yanks or unyanks.

    `before` maps the name of each file stored for the project to whether the file is
    yanked and why. A file of the page that `before` lacks is added, and a file of
    `before` that the page lacks is removed. A file on both whose yank or reason differs
    is a yank change, with the yank and the reason that the page has now, which is
    `(name, False, None)` for a file that is no longer yanked. A file that is still
    yanked for another reason, or for none, is reported as yanked with the new reason,
    so that the stored reason follows the page. The files of a page are told apart by
    filename, as PyPI never reuses one.

    A quarantined project's page lists no files, and that says nothing about the stored
    ones: diffing it would remove every one. A quarantine is an event, and the stored
    files are kept, so the run must not diff such a page.
    """
    now = {info.filename: info for info in page.files}
    added = [now[name] for name in sorted(now.keys() - before.keys())]
    removed = sorted(before.keys() - now.keys())
    yank_changes = [
        (name, now[name].yanked, now[name].yanked_reason)
        for name in sorted(now.keys() & before.keys())
        if before[name] != (now[name].yanked, now[name].yanked_reason)
    ]
    return FileDiff(added=added, removed=removed, yank_changes=yank_changes)


def classify_probe(status: int, page: ProjectPage | None) -> str:
    """What the probe of a name that left the root found: "removed", "quarantined" or
    "present".

    `status` is the HTTP status of the project's `/simple/<project>/` page, and `page`
    is the page parsed from it, which only a 200 has. A 404 or 410 means the project was
    removed, whatever `page` is. A 200 with a quarantined page means it was quarantined,
    and any other 200 means that it is there.

    Raises ValueError for any other status, and for a 200 with no page: without its page
    a 200 cannot be told from a quarantine, and a guess would be recorded as a fact.
    """
    if status in (404, 410):
        return "removed"
    if status == 200:
        if page is None:
            raise ValueError(
                f"a probe of a vanished name got HTTP status {status} and no page "
                "to read"
            )
        return "quarantined" if page.status == "quarantined" else "present"
    raise ValueError(
        f"a probe of a vanished name got HTTP status {status}, which is not a page "
        "(200) or a removal (404, 410)"
    )
