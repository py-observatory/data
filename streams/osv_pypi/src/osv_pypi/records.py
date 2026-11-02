"""Advisory records: what is kept of one OSV advisory.

OSV.dev serves each advisory that affects a PyPI project as one JSON record, and serves
a new one when the advisory changes. Each copy of a record that the stream reads
becomes one version row. Rows go into monthly parts that are public and never
replaced, so three rules hold for every row:

- It keeps what the advisory says of PyPI. The entries of `affected` for other
  ecosystems go.
- It has no text of `details`, only its facts: a fingerprint, the length in bytes and
  the number of words, as the registry keeps of a README.
- It has no email address, written plainly or hidden as in `jane [at] x [dot] org`.
  `credits`, which name people and how to reach them, are not kept at all.

A record is data from outside. It is read as JSON and never run, and a value of the
wrong shape raises ValueError: a guess at a value would be stored as a fact. A key that
this module does not name is dropped unread, because OSV's schema gains keys.
"""

import hashlib
import json
import math
from collections.abc import Iterator

from obskit.emails import holds_address, is_mail_link, without_addresses
from obskit.names import canonical_name
from obskit.show import show
from osv_pypi.index import instant_key

# The columns of a version row, in order. The stream's tables are made from this tuple,
# so it is their schema. The references are `refs`, because `references` is a keyword
# of SQL.
VERSION_COLUMNS: tuple[str, ...] = (
    "id",
    "kind",
    "modified",
    "modified_key",
    "published",
    "withdrawn",
    "summary",
    "details_sha256",
    "details_bytes",
    "details_words",
    "aliases",
    "related",
    "severity",
    "affected",
    "refs",
    "database_specific",
    "schema_version",
)

# The columns that hold JSON, written by `_compact`. None in one of them means nothing.
_JSON_COLUMNS = frozenset(
    {"aliases", "related", "severity", "affected", "refs", "database_specific"}
)

# The largest real record is under 100 KB, so a body over this is not a record.
_MAX_RECORD_BYTES = 4 * 1024 * 1024

_MALWARE_PREFIX = "MAL-"  # the ids of OpenSSF's reports of malicious packages
_ECOSYSTEM = "PyPI"
_RANGE_TYPES = ("ECOSYSTEM", "SEMVER", "GIT")
_EVENT_KEYS = ("introduced", "fixed", "last_affected", "limit")
# The keys of `database_specific` that a row keeps. GitHub's advisories write them.
_DATABASE_KEYS = (
    "severity",
    "cwe_ids",
    "github_reviewed",
    "github_reviewed_at",
    "nvd_published_at",
)
# The longest alias or related id. The longest real one is under 50 characters.
_MAX_ID_LENGTH = 128


def _refuse(path: str, expected: str, value: object) -> ValueError:
    """The error for a value of the wrong shape: where it is and what was expected."""
    return ValueError(f"{path} must be {expected}, not {show(value)}")


def _compact(value: object) -> str:
    """`value` as a JSON column holds it: no spaces, the keys of each object in order,
    and characters outside ASCII written as they are."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def _load(body: bytes) -> dict:
    """The JSON object in `body`.

    The size is checked before anything else, so that an oversized body is never
    decoded. The bytes are strict UTF-8. A body that is not JSON, is not an object, or
    is nested more deeply than the JSON reader can go raises ValueError.
    """
    if len(body) > _MAX_RECORD_BYTES:
        raise ValueError(
            f"the body is {len(body):,} bytes, over the limit of {_MAX_RECORD_BYTES:,}"
        )
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"the body is not UTF-8: {exc}") from exc
    try:
        document = json.loads(text)
    except RecursionError as exc:
        raise ValueError("the body is nested too deeply to read") from exc
    except ValueError as exc:  # a JSONDecodeError
        raise ValueError(f"the body is not JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"the body must be a JSON object, not {show(document)}")
    return document


def _text(value: object, path: str) -> str:
    """`value`, which is at `path`, when it is a string."""
    if not isinstance(value, str):
        raise _refuse(path, "a string", value)
    return value


def _optional_text(value: object, path: str) -> str | None:
    """`value`, which is at `path`, when it is a string, or None when it is null or
    left out."""
    if value is not None and not isinstance(value, str):
        raise _refuse(path, "a string or null", value)
    return value


def _time(document: dict, key: str, *, required: bool = False) -> str | None:
    """The time at `key`, as the record writes it. It must be one that `instant_key`
    accepts: RFC 3339 in UTC. A time that is not required may be null or left out."""
    value = document.get(key)
    if value is None and not required:
        return None
    if not isinstance(value, str):
        expected = "an RFC 3339 UTC time" + ("" if required else " or null")
        raise _refuse(key, expected, value)
    try:
        instant_key(value)
    except ValueError as exc:
        raise ValueError(f"{key}: {exc}") from exc
    return value


def _details_facts(document: dict) -> tuple[str | None, int | None, int | None]:
    """The facts of `details` in place of its text: the sha256 of its UTF-8 bytes, how
    many bytes those are, and how many words it has, split on white space.

    With no text the three are None, and an empty text has no fingerprint, 0 bytes and
    0 words. A text that UTF-8 cannot hold, such as a lone surrogate, raises ValueError:
    it has no bytes to count.
    """
    text = _optional_text(document.get("details"), "details")
    if text is None:
        return None, None, None
    try:
        data = text.encode()
    except UnicodeEncodeError as exc:
        raise _refuse("details", "text that UTF-8 can hold", text) from exc
    if not data:
        return None, 0, 0
    return hashlib.sha256(data).hexdigest(), len(data), len(text.split())


def _ids(document: dict, key: str) -> list[str] | None:
    """The ids at `key`, which is `aliases` or `related`, sorted and each once, or None
    when there are none. Each is a non-empty string of at most _MAX_ID_LENGTH
    characters."""
    value = document.get(key)
    if value is None:
        return None
    if not isinstance(value, list):
        raise _refuse(key, "a list or null", value)
    for index, item in enumerate(value):
        if not isinstance(item, str) or not 0 < len(item) <= _MAX_ID_LENGTH:
            expected = f"a non-empty string of at most {_MAX_ID_LENGTH} characters"
            raise _refuse(f"{key}[{index}]", expected, item)
    return sorted(set(value)) or None


def _severity(document: dict) -> list[dict[str, str]] | None:
    """The scores of `severity`, each its type and its score, in order, or None when
    there are none."""
    value = document.get("severity")
    if value is None:
        return None
    if not isinstance(value, list):
        raise _refuse("severity", "a list or null", value)
    scores = []
    for index, entry in enumerate(value):
        where = f"severity[{index}]"
        if not isinstance(entry, dict):
            raise _refuse(where, "an object", entry)
        scores.append(
            {
                "type": _text(entry.get("type"), f"{where}.type"),
                "score": _text(entry.get("score"), f"{where}.score"),
            }
        )
    return scores or None


def _event(value: object, where: str) -> dict[str, str]:
    """One event of a range: an object with one key, which is introduced, fixed,
    last_affected or limit, and a version or a commit as a string."""
    expected = (
        "an object with one key out of introduced, fixed, last_affected and limit"
    )
    if not isinstance(value, dict) or len(value) != 1:
        raise _refuse(where, expected, value)
    (key,) = value
    if key not in _EVENT_KEYS:
        raise _refuse(where, expected, value)
    return {key: _text(value[key], f"{where}.{key}")}


def _range(value: object, where: str) -> dict[str, object]:
    """One range of an entry: its type, its repo and its events, in order.

    A repo is a git URL, and one that holds an email address, as
    `git@github.com:foo/bar.git` does, is None. It is kept whole or not at all: taking
    the address out would leave a URL that points nowhere, in a part that is never
    replaced.
    """
    if not isinstance(value, dict):
        raise _refuse(where, "an object", value)
    range_type = value.get("type")
    if range_type not in _RANGE_TYPES:
        raise _refuse(f"{where}.type", "ECOSYSTEM, SEMVER or GIT", range_type)
    repo = _optional_text(value.get("repo"), f"{where}.repo")
    events = value.get("events")
    if not isinstance(events, list):
        raise _refuse(f"{where}.events", "a list", events)
    return {
        "type": range_type,
        "repo": None if repo is None or holds_address(repo) else repo,
        "events": [
            _event(event, f"{where}.events[{index}]")
            for index, event in enumerate(events)
        ],
    }


def _listed(entry: dict, key: str, where: str) -> list:
    """The list at `key` of the entry at `where`, which is empty when the key is null
    or left out."""
    value = entry.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise _refuse(f"{where}.{key}", "a list or null", value)
    return value


def _pypi_entry(entry: dict, package: dict, where: str) -> dict[str, object]:
    """One PyPI entry of `affected`, as a row keeps it: the package's canonical name
    and its name as written, its ranges, and the versions that it lists, as strings in
    their order. The entry's `database_specific`, `ecosystem_specific` and `severity`
    are not kept, and nor is the package's `purl`, which spells the name again."""
    name = package.get("name")
    if not isinstance(name, str) or not name:
        raise _refuse(f"{where}.package.name", "a non-empty string", name)
    ranges = [
        _range(value, f"{where}.ranges[{index}]")
        for index, value in enumerate(_listed(entry, "ranges", where))
    ]
    versions = [
        _text(version, f"{where}.versions[{index}]")
        for index, version in enumerate(_listed(entry, "versions", where))
    ]
    return {
        "name": canonical_name(name),
        "name_raw": name,
        "ranges": ranges,
        "versions": versions,
    }


def _affected(document: dict) -> list[dict[str, object]]:
    """The entries of `affected` whose package is a PyPI one, in order.

    An entry for another ecosystem is not read past its package's ecosystem, and nor is
    an entry with no package. A record with no PyPI entry raises ValueError: the stream
    keeps advisories that affect PyPI, and a record of OSV's PyPI files that names none
    is broken.
    """
    entries = document.get("affected")
    if not isinstance(entries, list):
        raise _refuse("affected", "a list", entries)
    kept = []
    for index, entry in enumerate(entries):
        where = f"affected[{index}]"
        if not isinstance(entry, dict):
            raise _refuse(where, "an object", entry)
        package = entry.get("package")
        if package is None:
            continue
        if not isinstance(package, dict):
            raise _refuse(f"{where}.package", "an object or null", package)
        if package.get("ecosystem") == _ECOSYSTEM:
            kept.append(_pypi_entry(entry, package, where))
    if not kept:
        raise _refuse("affected", "a list that holds an entry for PyPI", entries)
    return kept


def _references(document: dict) -> list[dict[str, str]] | None:
    """The references, each its type and its URL, in order, or None when none are left.

    A reference is left out when its URL is a mail link, or when it holds an email
    address, because it points at a person's mailbox. A URL is kept whole or not at all,
    as a repo is.
    """
    value = document.get("references")
    if value is None:
        return None
    if not isinstance(value, list):
        raise _refuse("references", "a list or null", value)
    kept = []
    for index, entry in enumerate(value):
        where = f"references[{index}]"
        if not isinstance(entry, dict):
            raise _refuse(where, "an object", entry)
        reference_type = _text(entry.get("type"), f"{where}.type")
        url = _text(entry.get("url"), f"{where}.url")
        if is_mail_link(url) or holds_address(reference_type) or holds_address(url):
            continue
        kept.append({"type": reference_type, "url": url})
    return kept or None


def _is_plain(value: object) -> bool:
    """Whether `value` is a JSON scalar (a string, a number, a boolean or null) or a
    list of strings.

    A float that is not finite is no JSON number. json reads `NaN`, `Infinity` and a
    number too large for a float, such as `1e400`, and would write them back as `NaN`
    and `Infinity`, which no JSON reader takes.
    """
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, list):
        return all(isinstance(item, str) for item in value)
    return value is None or isinstance(value, (str, int, bool))


def _database_specific(document: dict) -> dict[str, object] | None:
    """The keys of `database_specific` that a row keeps (GitHub's severity, its CWE
    ids, when it was reviewed and when NVD published it), each when its value is
    plain, or None when none is left. The rest goes unread, the malware reports'
    origins and IOCs among it."""
    value = document.get("database_specific")
    if value is None:
        return None
    if not isinstance(value, dict):
        raise _refuse("database_specific", "an object or null", value)
    kept = {
        key: value[key]
        for key in _DATABASE_KEYS
        if key in value and _is_plain(value[key])
    }
    return kept or None


def _strings(value: object) -> Iterator[str]:
    """Every string in `value`, a value that this module built: the value when it is a
    string, and the keys and the values at every depth of its objects and lists. Such a
    value is a few levels deep at most, so this recursion is bounded."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)


def _refuse_what_is_never_published(
    values: dict[str, object], row: dict[str, object]
) -> None:
    """The last guard: raise ValueError if a column of the row holds an email address,
    or text that UTF-8 cannot hold. `values` are the row's values before its JSON
    columns were written.

    The rules above take addresses out of the summary, the references and the repos.
    Every other text, such as a version, a package name or an alias, is kept as it is,
    because changing it would change what it names, so a record with an address there
    is refused whole. Each string of a JSON column is read before the column is
    written, and the written column after, because each can hold an address that the
    other does not. JSON writes a line break as `\\n`, which hides an address that a
    line break runs through, as in `jane [at] x\\n[dot] org`. It writes a control
    character as an escape such as `\\u001f`, which can make one, as `\\u001f@x.org`
    reads.

    JSON can also carry a lone surrogate, as `\\ud800`, in bytes that are good UTF-8.
    SQLite refuses to store one, so a row that held it would stop the run that stores
    it, where a refused record is skipped and named.

    A message names the column, and never shows a text that holds an address.
    """
    for column in VERSION_COLUMNS:
        texts = list(_strings(values[column]))
        if column in _JSON_COLUMNS and row[column] is not None:
            texts.append(row[column])
        for text in texts:
            if holds_address(text):
                raise ValueError(
                    f"{column} holds an email address, which is never published"
                )
            try:
                text.encode()
            except UnicodeEncodeError as exc:
                raise ValueError(
                    f"{column} holds {show(text)}, which UTF-8 cannot hold"
                ) from exc


def _row(document: dict, advisory_id: str) -> dict[str, object]:
    """The version row of a record's JSON object, with its JSON columns written."""
    if document.get("id") != advisory_id:
        raise _refuse("id", show(advisory_id), document.get("id"))
    modified = _time(document, "modified", required=True)
    published = _time(document, "published")
    withdrawn = _time(document, "withdrawn")
    summary = without_addresses(_optional_text(document.get("summary"), "summary"))
    details_sha256, details_bytes, details_words = _details_facts(document)
    malware = advisory_id.startswith(_MALWARE_PREFIX)
    values: dict[str, object] = {
        "id": advisory_id,
        "kind": "malware" if malware else "vulnerability",
        "modified": modified,
        "modified_key": instant_key(modified),
        "published": published,
        "withdrawn": withdrawn,
        "summary": summary,
        "details_sha256": details_sha256,
        "details_bytes": details_bytes,
        "details_words": details_words,
        "aliases": _ids(document, "aliases"),
        "related": _ids(document, "related"),
        "severity": _severity(document),
        "affected": _affected(document),
        "refs": _references(document),
        "database_specific": _database_specific(document),
        "schema_version": _optional_text(
            document.get("schema_version"), "schema_version"
        ),
    }
    row = dict(values)
    for column in _JSON_COLUMNS:
        if values[column] is not None:
            row[column] = _compact(values[column])
    _refuse_what_is_never_published(values, row)
    return row


def parse_record(body: bytes, *, advisory_id: str) -> dict[str, object]:
    """The version row of one OSV record, from its bytes. `advisory_id` is the id that
    the record was read as, and the record's own `id` must be the same. The row's keys
    are exactly VERSION_COLUMNS, in order, and each value is None, an int or a str.

    - `kind` is `malware` for an id that starts `MAL-`, and `vulnerability` otherwise.
    - `modified` is required, and kept as written beside its `instant_key`, the
      `modified_key`. `published` and `withdrawn` may be null or left out. All three
      must be RFC 3339 UTC times.
    - `summary` loses any email address. `details` is kept as its facts.
    - `aliases` and `related` are sorted, each once. `severity` keeps each score's type
      and score.
    - `affected` keeps its PyPI entries only, and a record with none is refused. Each
      keeps its package's canonical name and its name as written, its ranges (of type
      ECOSYSTEM, SEMVER or GIT, each with its repo and its events in order) and its
      versions.
    - `refs` are the references, without a mail link or one that holds an address.
    - `database_specific` keeps five of GitHub's keys, each when its value is a JSON
      scalar or a list of strings.
    - `credits`, and every key that is not named here, are not kept.

    A JSON column is compact JSON with its keys in order, and None when it would be
    empty. No column holds an email address.

    The body is at most 4 MiB of strict UTF-8 and holds a JSON object. Raises
    ValueError for a record that it refuses, naming the advisory, the path and the
    value. The caller skips such a record and names it, and the others go on.
    """
    try:
        return _row(_load(body), advisory_id)
    except ValueError as exc:
        raise ValueError(f"the record of {show(advisory_id)}: {exc}") from exc
