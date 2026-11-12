"""The registry's status pass reads the set of the most downloaded projects from the
downloads stream's serving database, by the stream's name, the asset's name and a query
that it writes out itself, since the registry takes no dependency on that stream. A
name that drifted on either side would make the pass skip in every run, saying only
that the set cannot be read, so these checks tie the registry's names to the downloads
stream's own.
"""

from contextlib import closing

from obskit.sqlite import open_db
from pypi_downloads import run as downloads_run
from pypi_downloads import serving
from pypi_registry import run as registry_run


def test_the_registry_names_the_downloads_stream_and_its_serving_database():
    assert registry_run._DOWNLOADS == downloads_run.STREAM
    assert registry_run._SERVING_ASSET == downloads_run.SERVING_ASSET


def test_the_registry_reads_the_newest_set_of_a_serving_database(tmp_path):
    # A serving database made as the downloads stream makes one. 2027's set holds a
    # project of 2026's, one that entered at 2027's first close and one that entered at
    # a later close.
    path = tmp_path / serving.SERVING_DB
    rows = [
        (2026, 7, "numpy", 202611, 1),
        (2027, 7, "numpy", 202612, 2),
        (2027, 3, "boto3", 202612, 1),
        (2027, 9, "zope-interface", 202701, 3),
    ]
    with closing(open_db(path, serving.SCHEMA, create=True)) as db:
        db.executemany(
            "INSERT INTO top_set (year, project_id, name, entered, rank) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        db.commit()

    assert registry_run._read_top_names(path) == ["boto3", "numpy", "zope-interface"]
