"""The serving database that the catalog publishes: its schema, and which tables
belong to whom.

Each table has one owner. A stream's own tables (`SOURCED`) are filled from that
stream's data, and carried over from the previous build when the stream cannot be read.
The joint tables (`JOINT`) are made from the tables of more than one stream, so every
build rebuilds them, as it rebuilds the search index `search`. The tables of
`PER_STREAM` are keyed by stream.
"""

# The catalog's own stream id. It names the catalog's releases, such as `catalog-current`.
CATALOG = "catalog"
SERVING_DB = "catalog.db"

SERVING_SCHEMA = """
-- What the build read: one row for each admitted stream. A carried stream keeps the row
-- that the previous build wrote, `complete` included, with `carried` set. A stream's data
-- is complete when its manifest says so: the registry's is not until its sweep has read
-- the page of every project. While a stream's data is not complete, what is counted over
-- the whole of PyPI (`growth`, `licence` and `classifier.projects`) counts only what the
-- streams have read.
CREATE TABLE input (
    stream TEXT PRIMARY KEY,
    generated_at TEXT NOT NULL,        -- the stream's manifest
    data_through TEXT NOT NULL,
    granularity TEXT NOT NULL,
    fingerprint TEXT NOT NULL,         -- of the assets read (inputs.fingerprint)
    complete INTEGER NOT NULL,         -- 1: the stream's manifest says that its data is complete
    carried INTEGER NOT NULL           -- 1: the stream could not be read, and its tables are the previous build's
) WITHOUT ROWID;

-- The upstreams of each stream and their licences, from its manifest.
CREATE TABLE attribution (
    stream TEXT NOT NULL,
    upstream TEXT NOT NULL,
    url TEXT,
    licence TEXT,
    terms TEXT,
    PRIMARY KEY (stream, upstream)
) WITHOUT ROWID;

-- Each project name in its current incarnation, the removed and quarantined included.
-- A project whose own page the registry has not read in full (`swept` 0) is known by its
-- name alone, so it has no release rows, and its count of releases is not known.
CREATE TABLE project (
    project_id INTEGER PRIMARY KEY,    -- in name order, for this build only
    name TEXT NOT NULL UNIQUE,         -- PEP 503
    incarnation INTEGER NOT NULL,
    status TEXT NOT NULL,              -- active, archived, quarantined, removed, or as PyPI writes it
    status_since TEXT,                 -- when the last event that sets a status was observed
    first_seen TEXT NOT NULL,
    first_seen_exact INTEGER NOT NULL,
    swept INTEGER NOT NULL,            -- 1 once the registry has read the project's own page in full
    first_uploaded INTEGER,            -- the earliest upload of any release, Unix seconds
    releases INTEGER,                  -- releases that have a file now (see release); NULL while swept is 0
    latest_version TEXT,
    record_version TEXT,               -- the release whose record gives the fields below
    name_raw TEXT,
    summary TEXT,
    license TEXT,
    license_expression TEXT,
    requires_python TEXT,
    home_page TEXT,
    project_urls TEXT,                 -- JSON object, as the registry keeps it
    keywords TEXT
);

-- Each release of a project's current incarnation. A release whose files carry no
-- version that can be read (old eggs, .exe installers, legacy sdist names) has `uploaded`
-- and `files` NULL: it does not count in `project.releases` or `growth`, and it is never
-- the latest version. That is about 0.15% of releases. `growth` and
-- `project.first_uploaded` leave out the releases deleted before the registry began,
-- until its history fill.
CREATE TABLE release (
    project_id INTEGER NOT NULL,
    version TEXT NOT NULL,             -- canonical PEP 440, or as written when PEP 440 refuses it
    uploaded INTEGER,                  -- earliest file upload, Unix seconds; NULL when no file's version could be read
    files INTEGER,                     -- files it has now; NULL when unknown
    yanked INTEGER NOT NULL,           -- 1 when every file it has now is yanked
    yanked_reason TEXT,
    has_record INTEGER NOT NULL,
    PRIMARY KEY (project_id, version)
) WITHOUT ROWID;
CREATE INDEX release_by_upload ON release (uploaded);

-- The fields each recorded release changed.
CREATE TABLE change (
    project_id INTEGER NOT NULL,
    version TEXT NOT NULL,
    field TEXT NOT NULL,               -- one of records.CHANGE_FIELDS
    value TEXT,                        -- from this release on; NULL when it was cleared
    compared_with TEXT,                -- the version recorded before; NULL for the first record
    PRIMARY KEY (project_id, version, field)
) WITHOUT ROWID;

-- The requirements of each project's record_version, with the text interned.
CREATE TABLE requirement_text (
    requirement_id INTEGER PRIMARY KEY,
    text TEXT NOT NULL UNIQUE
);
CREATE TABLE requirement (
    project_id INTEGER NOT NULL,
    requirement_id INTEGER NOT NULL,
    depends_on TEXT NOT NULL,          -- PEP 503 name of the project required
    PRIMARY KEY (project_id, requirement_id)
) WITHOUT ROWID;
CREATE INDEX requirement_by_dependency ON requirement (depends_on, project_id);

-- Trove classifiers of each project's record_version, interned and counted.
CREATE TABLE classifier (
    classifier_id INTEGER PRIMARY KEY,
    text TEXT NOT NULL UNIQUE,
    projects INTEGER NOT NULL          -- projects that are neither removed nor quarantined
);
CREATE TABLE project_classifier (
    project_id INTEGER NOT NULL,
    classifier_id INTEGER NOT NULL,
    PRIMARY KEY (project_id, classifier_id)
) WITHOUT ROWID;
CREATE INDEX project_classifier_by_classifier ON project_classifier (classifier_id, project_id);

-- Projects that are neither removed nor quarantined, by licence expression.
CREATE TABLE licence (
    licence TEXT PRIMARY KEY,
    projects INTEGER NOT NULL
) WITHOUT ROWID;

-- New projects (by first upload) and new releases, by month of upload.
CREATE TABLE growth (
    month TEXT PRIMARY KEY,            -- YYYY-MM
    projects INTEGER NOT NULL,
    releases INTEGER NOT NULL
) WITHOUT ROWID;

-- Every registry event, every incarnation included.
CREATE TABLE event (
    seq INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    incarnation INTEGER NOT NULL,
    kind TEXT NOT NULL,
    version TEXT,
    filename TEXT,
    detail TEXT,
    source_at TEXT,
    observed_at TEXT NOT NULL
);
CREATE INDEX event_by_name ON event (name, seq);
CREATE INDEX event_by_kind ON event (kind, observed_at);

CREATE TABLE advisory (
    advisory_id INTEGER PRIMARY KEY,   -- in id order, for this build only
    id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,                -- malware or vulnerability
    status TEXT NOT NULL,              -- active or removed
    published TEXT,
    modified TEXT NOT NULL,
    withdrawn TEXT,
    summary TEXT,
    aliases TEXT,                      -- JSON, as osv-pypi keeps them
    related TEXT,
    severity TEXT,
    refs TEXT,
    database_specific TEXT
);
CREATE INDEX advisory_by_published ON advisory (published);

-- What each advisory says of each project it names: OSV's entries for one name, merged.
CREATE TABLE advisory_package (
    advisory_id INTEGER NOT NULL,
    name TEXT NOT NULL,                -- PEP 503
    ranges TEXT NOT NULL,              -- JSON list of the entries' ranges, in OSV's order
    versions TEXT NOT NULL,            -- JSON list of the versions OSV lists, each once
    PRIMARY KEY (advisory_id, name)
) WITHOUT ROWID;
CREATE INDEX advisory_package_by_name ON advisory_package (name);

-- Each known release that an active advisory affects.
CREATE TABLE affected (
    project_id INTEGER NOT NULL,
    version TEXT NOT NULL,
    advisory_id INTEGER NOT NULL,
    PRIMARY KEY (project_id, version, advisory_id)
) WITHOUT ROWID;
CREATE INDEX affected_by_advisory ON affected (advisory_id);

-- Search over name, summary and keywords.
CREATE VIRTUAL TABLE search USING fts5(
    name, summary, keywords,
    content='project', content_rowid='project_id',
    tokenize='unicode61 remove_diacritics 2'
);

CREATE VIEW tombstone AS
    SELECT name, incarnation, status, status_since FROM project
    WHERE status IN ('removed', 'quarantined');
"""

# The tables that each stream's data fills.
SOURCED: dict[str, tuple[str, ...]] = {
    "pypi-registry": (
        "project",
        "release",
        "change",
        "requirement_text",
        "requirement",
        "classifier",
        "project_classifier",
        "licence",
        "growth",
        "event",
    ),
    "osv-pypi": ("advisory", "advisory_package"),
}
# Made from the tables of more than one stream.
JOINT = ("affected",)
# What each build read of each stream, and the upstreams that it names.
PER_STREAM = ("input", "attribution")
