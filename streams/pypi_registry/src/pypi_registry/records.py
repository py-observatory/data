"""Release records: what is kept of one release's metadata.

PyPI serves a release's metadata at `/pypi/<project>/<version>/json`. A release does not
change, so it is read once and kept as one record. Records go into monthly parts that are
never replaced, so two rules hold for every record:

- It has no README text. It has facts of the README instead: a fingerprint, the length
  in bytes, the number of words and the format.
- It has no email address. The names of people are kept.

A reply is data from outside. It is read as JSON and never run, and a value of the wrong
shape raises ValueError: a guess at a value would be stored as a fact. Keys that this
module does not name are ignored, because the API gains keys over time.
"""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime

from pypi_registry.names import canonical_version
from pypi_registry.simple import _refuse

# A README is fingerprinted on its first 40,000 bytes. BigQuery's copy of a README ends
# at that length, so the start of the text gives one fingerprint for the whole text that
# the JSON API serves and for BigQuery's cut one.
README_PREFIX_BYTES = 40_000

# The columns of a release record, in order. The `release_records` table is made from
# this tuple, so it is the table's schema.
RECORD_COLUMNS: tuple[str, ...] = (
    "name",
    "name_raw",
    "incarnation",
    "version",
    "version_raw",
    "uploaded_at",
    "summary",
    "license",
    "license_expression",
    "license_files",
    "requires_python",
    "requires_dist",
    "provides_extra",
    "classifiers",
    "keywords",
    "project_urls",
    "home_page",
    "download_url",
    "author",
    "author_email_names",
    "maintainer",
    "maintainer_email_names",
    "platform",
    "dynamic",
    "readme_sha256",
    "readme_bytes",
    "readme_bytes_exact",
    "readme_words",
    "readme_content_type",
    "yanked",
    "yanked_reason",
    "source",
    "observed_at",
)

# What a message calls the document that a record is made from.
_WHAT = "a release's JSON"

# `<name@host>`: brackets with an address in them and nothing else that is a bracket.
# The text before the first @ cannot hold an @, so each bracket is read in one pass. A
# text of many `<` and `@` would otherwise be read again from each of them.
_BRACKETED_ADDRESS = re.compile(r"<[^<>@]*@[^<>]*>")

# An email address: a name of letters, digits and `._%+-`, an `@`, and a domain that
# ends in a dot and two letters or more. `\w` takes the letters of every script, so an
# address in another script goes whole and leaves no part of itself behind.
#
# The lookbehind makes a match start where a name starts. Without it, every letter of a
# long word is tried as a start, the word is read to its end from each, and a text of
# 50,000 letters and no address takes several seconds.
_ADDRESS = r"(?<![\w.%+-])[\w.%+-]+@[\w.-]+\.[^\W\d_]{2,}"
_HAS_ADDRESS = re.compile(_ADDRESS)
# An address with the brackets around it and the spaces before it. The first lookbehind
# is for the same reason as the one above, for a long run of spaces.
_REMOVAL = re.compile(rf"(?<![ \t])[ \t]*(?:<\s*{_ADDRESS}\s*>|{_ADDRESS})")


@dataclass(frozen=True)
class ReadmeFacts:
    """What a record keeps of a README, in place of its text."""

    sha256: str | None  # of the first README_PREFIX_BYTES bytes; None with no text
    bytes: int | None  # the length in UTF-8 bytes; None when there is no description
    bytes_exact: bool  # False only when `bytes` is a lower bound
    words: int | None  # the words of the text, split on white space
    content_type: str | None  # `description_content_type`, as PyPI gives it


def readme_facts(description: str | None, content_type: str | None) -> ReadmeFacts:
    """The facts of a README, from its text and its format.

    The fingerprint is the sha256 of the first README_PREFIX_BYTES bytes of the UTF-8
    text, cut back to a whole character when the cut falls inside one. A null
    description has no fingerprint, length or word count. An empty one has no
    fingerprint and a length and word count of 0.

    `bytes_exact` is always True here, because the whole text is in hand. A length that
    is only a lower bound comes from a source that holds a cut copy. A text that UTF-8
    cannot hold, such as a lone surrogate, raises ValueError.
    """
    if description is None:
        return ReadmeFacts(None, None, True, None, content_type)
    text = description.encode()
    if not text:
        return ReadmeFacts(None, 0, True, 0, content_type)
    prefix = text[:README_PREFIX_BYTES].decode("utf-8", "ignore").encode()
    return ReadmeFacts(
        sha256=hashlib.sha256(prefix).hexdigest(),
        bytes=len(text),
        bytes_exact=True,
        words=len(description.split()),
        content_type=content_type,
    )


def strip_emails(value: str | None) -> str | None:
    """The names in a field of people, with every email address taken out.

    A field such as `author_email` holds `Name <address>` entries or bare addresses,
    separated by commas. An address in brackets goes, and so does any word that holds an
    `@`. The text of each entry that is left is trimmed of spaces and quotes, an entry
    with nothing left is dropped, and the rest are joined with ", ". With nothing left
    the result is None. It never holds an `@`.
    """
    if value is None:
        return None
    names = []
    for entry in _BRACKETED_ADDRESS.sub(" ", value).split(","):
        words = [word for word in entry.split() if "@" not in word]
        name = " ".join(words).strip(" \"'")
        if name:
            names.append(name)
    return ", ".join(names) or None


def _without_addresses(text: str | None) -> str | None:
    """`text` with every email address taken out, with the brackets around it and the
    spaces before it, and then trimmed. A text with no address comes back as it was.
    None comes back for a text that is empty or that nothing is left of.

    Taking an address out can join the text on either side into another address:
    `me@<a@b.cc>x.org` leaves `me@x.org`. Only a text written to do that looks like
    this, so it is dropped whole, and what comes back never holds an address. Taking
    out what is left would make the work grow with how deeply a text nests.
    """
    if text is None:
        return None
    cleaned, removed = _REMOVAL.subn("", text)
    if removed:
        text = cleaned.strip()
        if _HAS_ADDRESS.search(text):
            return None
    return text or None


def _is_mail_link(url: str) -> bool:
    """Whether `url` is a `mailto:` link. A scheme is not case sensitive."""
    return url.lstrip().lower().startswith("mailto:")


def _compact(value: object, *, sort_keys: bool = False) -> str:
    """`value` as JSON with no space and no escape of a character outside ASCII."""
    return json.dumps(
        value, separators=(",", ":"), ensure_ascii=False, sort_keys=sort_keys
    )


def _optional_text(info: dict, key: str) -> str | None:
    """The string at `key` of `info`, or None when it is null or left out."""
    value = info.get(key)
    if value is not None and not isinstance(value, str):
        raise _refuse(_WHAT, f"info.{key}", "a string or null", value)
    return value


def _required_text(info: dict, key: str) -> str:
    """The non-empty string at `key` of `info`."""
    value = info.get(key)
    if not isinstance(value, str) or not value:
        raise _refuse(_WHAT, f"info.{key}", "a non-empty string", value)
    return value


def _free_text(info: dict, key: str) -> str | None:
    """The string at `key` of `info`, with any email address taken out of it."""
    return _without_addresses(_optional_text(info, key))


def _link(info: dict, key: str) -> str | None:
    """The URL at `key` of `info`, or None when it is a mail link or holds an email
    address. A URL is kept whole or not at all, as a project URL is: taking the address
    out of `git+ssh://git@github.com/foo/bar.git` leaves `git+ssh:///foo/bar.git`,
    which points nowhere, and a record is never replaced."""
    url = _optional_text(info, key)
    if url is None or _is_mail_link(url) or _HAS_ADDRESS.search(url):
        return None
    return url or None


def _text_list(info: dict, key: str) -> str | None:
    """The list of strings at `key` of `info` as compact JSON, or None when it is null,
    left out or empty."""
    value = info.get(key)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise _refuse(_WHAT, f"info.{key}", "a list of strings or null", value)
    return _compact(value) if value else None


def _project_urls(info: dict) -> str | None:
    """The project URLs as compact JSON with the keys in order, or None when none are
    left. An entry is left out when its URL is a mail link, or when its URL or its label
    holds an email address, because it points at a person's mailbox."""
    value = info.get("project_urls")
    if value is None:
        return None
    if not isinstance(value, dict) or not all(
        isinstance(label, str) and isinstance(url, str) for label, url in value.items()
    ):
        raise _refuse(_WHAT, "info.project_urls", "an object of strings or null", value)
    kept = {
        label: url
        for label, url in value.items()
        if not (
            _is_mail_link(url) or _HAS_ADDRESS.search(label) or _HAS_ADDRESS.search(url)
        )
    }
    return _compact(kept, sort_keys=True) if kept else None


def _read_yanked(info: dict) -> tuple[bool, str | None]:
    """Whether a release is yanked, and why. PyPI gives a bool and a reason beside it.
    A non-empty string in place of the bool means yanked, with that string as the reason,
    as the Simple API writes it. A release with no `yanked` is not yanked."""
    value = info.get("yanked")
    reason = _optional_text(info, "yanked_reason")
    if value is None or isinstance(value, bool):
        return bool(value), reason
    if isinstance(value, str) and value:
        return True, value if reason is None else reason
    raise _refuse(_WHAT, "info.yanked", "a boolean or a non-empty reason string", value)


def _instant(text: str, path: str) -> datetime:
    """The moment that an ISO 8601 `text` gives, in UTC when it names no zone. The text
    is not chained to the error, which would carry the whole of it."""
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        raise _refuse(_WHAT, path, "an ISO 8601 time", text) from None
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _earliest_upload(files: list) -> str | None:
    """The earliest `upload_time_iso_8601` of the files, as PyPI wrote it, or None when
    no file has one. Times are compared as moments and not as text: Python writes no
    fraction for a time on the whole second, and "." sorts before "Z"."""
    times = []
    for index, entry in enumerate(files):
        if not isinstance(entry, dict):
            raise _refuse(_WHAT, f"urls[{index}]", "an object", entry)
        text = entry.get("upload_time_iso_8601")
        if text is None:
            continue
        path = f"urls[{index}].upload_time_iso_8601"
        if not isinstance(text, str):
            raise _refuse(_WHAT, path, "a string or null", text)
        times.append((_instant(text, path), text))
    return min(times)[1] if times else None


def release_record(
    name: str,
    incarnation: int,
    info: dict,
    files: list[dict],
    *,
    observed_at: str,
) -> dict[str, object]:
    """The record of one release, from the `info` and the `urls` (as `files`) of its JSON
    page. Its keys are exactly RECORD_COLUMNS, in order.

    `name` is the canonical name of the project, and it and `incarnation` key the record
    with the version. The display spelling of the name, `info.name`, is kept as
    `name_raw`. The version is made canonical, and `info.version` is kept as
    `version_raw`.

    The README has no column: `readme_facts` takes its place. The four fields of people
    go through `strip_emails`. The summary, licence, keywords, platform and yank reason
    lose any email address they hold. A home page or download URL is None when it is a
    `mailto:` link or holds an address, and a project URL is left out when it is a mail
    link or when its link or label holds an address. Requirements are kept as they are:
    `name @ url` and a VCS URL are not contact addresses. A text that nothing is left of
    is None.

    Lists are compact JSON, and so are the project URLs, with their keys in order. An
    empty list or object is None. `uploaded_at` is the earliest upload time of a file,
    or None when no file has one.

    Raises ValueError for a reply of the wrong shape, naming the value.
    """
    if not isinstance(info, dict):
        raise _refuse(_WHAT, "info", "an object", info)
    if not isinstance(files, list):
        raise _refuse(_WHAT, "urls", "a list", files)
    name_raw = _required_text(info, "name")
    version_raw = _required_text(info, "version")
    facts = readme_facts(
        _optional_text(info, "description"),
        _optional_text(info, "description_content_type"),
    )
    yanked, yanked_reason = _read_yanked(info)
    return {
        "name": name,
        "name_raw": name_raw,
        "incarnation": incarnation,
        "version": canonical_version(version_raw),
        "version_raw": version_raw,
        "uploaded_at": _earliest_upload(files),
        "summary": _free_text(info, "summary"),
        "license": _free_text(info, "license"),
        "license_expression": _optional_text(info, "license_expression"),
        "license_files": _text_list(info, "license_files"),
        "requires_python": _optional_text(info, "requires_python"),
        "requires_dist": _text_list(info, "requires_dist"),
        "provides_extra": _text_list(info, "provides_extra"),
        "classifiers": _text_list(info, "classifiers"),
        "keywords": _free_text(info, "keywords"),
        "project_urls": _project_urls(info),
        "home_page": _link(info, "home_page"),
        "download_url": _link(info, "download_url"),
        "author": strip_emails(_optional_text(info, "author")),
        "author_email_names": strip_emails(_optional_text(info, "author_email")),
        "maintainer": strip_emails(_optional_text(info, "maintainer")),
        "maintainer_email_names": strip_emails(
            _optional_text(info, "maintainer_email")
        ),
        "platform": _free_text(info, "platform"),
        "dynamic": _text_list(info, "dynamic"),
        "readme_sha256": facts.sha256,
        "readme_bytes": facts.bytes,
        "readme_bytes_exact": facts.bytes_exact,
        "readme_words": facts.words,
        "readme_content_type": facts.content_type,
        "yanked": yanked,
        "yanked_reason": _without_addresses(yanked_reason),
        "source": "json-api",
        "observed_at": observed_at,
    }
