import pyarrow as pa

from counter.tally import MIRRORS, Tally, merge

DETAILS = pa.struct([("installer", pa.struct([("name", pa.string())]))])


def batch(projects, installers):
    details = [None if name is None else {"installer": {"name": name}} for name in installers]
    return pa.record_batch(
        [pa.array(projects, pa.string()), pa.array(details, DETAILS)],
        names=["project", "details"],
    )


def test_tally_counts_all_and_mirror_downloads():
    t = Tally()
    t.add(batch(["numpy", "numpy", "boto3", None, "numpy"],
                ["pip", "bandersnatch", "uv", "pip", None]))
    assert t.rows == 5 and t.null_project_rows == 1
    assert t.table().to_pylist() == [
        {"project": "boto3", "downloads": 1, "mirror_downloads": 0},
        {"project": "numpy", "downloads": 3, "mirror_downloads": 1},
    ]


def test_table_has_the_promised_schema():
    table = Tally().table()
    assert table.schema.types == [pa.string(), pa.int64(), pa.int64()]
    assert table.schema.names == ["project", "downloads", "mirror_downloads"]
    assert table.num_rows == 0


def test_tally_merges_across_many_batches():
    t = Tally()
    for i in range(600):
        t.add(batch(["a", "b", "a", None], ["devpi", "pip", "pip", "pip"]))
    assert t.rows == 2400 and t.null_project_rows == 600
    assert t.table().to_pylist() == [
        {"project": "a", "downloads": 1200, "mirror_downloads": 600},
        {"project": "b", "downloads": 600, "mirror_downloads": 0},
    ]


def test_mirror_names_match_exactly():
    assert MIRRORS == ("bandersnatch", "z3c.pypimirror", "Artifactory", "devpi")
    t = Tally()
    t.add(batch(["p"] * 6, ["Bandersnatch", "devpi-client", "devpi", "Artifactory",
                            "z3c.pypimirror", "artifactory"]))
    assert t.table().to_pylist() == [{"project": "p", "downloads": 6, "mirror_downloads": 3}]


def test_a_null_installer_is_not_a_mirror():
    t = Tally()
    t.add(batch(["p", "p"], [None, "devpi"]))
    t.add(batch(["p"], [None]))
    assert t.table().to_pylist() == [{"project": "p", "downloads": 3, "mirror_downloads": 1}]


def test_a_batch_of_only_null_projects_adds_no_rows_to_the_table():
    t = Tally()
    t.add(batch([None, None], ["pip", "devpi"]))
    assert t.rows == 2 and t.null_project_rows == 2
    assert t.table().num_rows == 0


def test_children_are_found_by_name_not_position():
    details = pa.struct([("other", pa.string()),
                         ("installer", pa.struct([("version", pa.string()),
                                                  ("name", pa.string())]))])
    b = pa.record_batch(
        [pa.array(["p"], pa.string()),
         pa.array([{"other": "x", "installer": {"version": "1", "name": "devpi"}}], details)],
        names=["project", "details"],
    )
    t = Tally()
    t.add(b)
    assert t.table().to_pylist() == [{"project": "p", "downloads": 1, "mirror_downloads": 1}]


def test_merge_sums_by_project_and_sorts():
    schema = pa.schema([("project", pa.string()), ("downloads", pa.int64()),
                        ("mirror_downloads", pa.int64())])
    one = pa.table({"project": ["b", "a"], "downloads": [2, 1], "mirror_downloads": [1, 0]},
                   schema=schema)
    two = pa.table({"project": ["c", "b"], "downloads": [5, 3], "mirror_downloads": [0, 3]},
                   schema=schema)
    assert merge([one, two]).to_pylist() == [
        {"project": "a", "downloads": 1, "mirror_downloads": 0},
        {"project": "b", "downloads": 5, "mirror_downloads": 4},
        {"project": "c", "downloads": 5, "mirror_downloads": 0},
    ]
    assert merge([]).num_rows == 0
