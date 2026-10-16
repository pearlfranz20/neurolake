import json

import duckdb

from neurolake.dbt_plugin import Plugin, duplicate_keys, last_wins


def test_last_value_wins_at_every_level():
    doc = '{"a": 1, "b": [{"x": 1, "x": 3}], "a": {"z": 2, "z": 5}}'
    assert json.loads(last_wins(doc)) == {"a": {"z": 5}, "b": [{"x": 3}]}
    assert duplicate_keys(doc) == ["$.b[*].x", "$.a", "$.a.z"]


def test_no_duplicates_and_invalid_json():
    assert duplicate_keys('{"a": [1, 2], "b": {}}') == []
    assert duplicate_keys("[]") == []
    assert duplicate_keys("{bad") is None and last_wins("{bad") is None
    assert last_wins('{"k": "é"}') == '{"k":"é"}'


def test_registered_functions_differ_from_duckdb_first_value():
    con = duckdb.connect()
    Plugin(name="neurolake", plugin_config={}).configure_connection(con)
    doc = '{"IntendedFor": "first", "IntendedFor": "last"}'
    got = con.execute(
        "select ?::json ->> 'IntendedFor', bids_json_last_wins(?)::json ->> 'IntendedFor', "
        "bids_json_duplicate_keys(?), bids_json_last_wins(null)",
        [doc, doc, doc],
    ).fetchone()
    assert got == ("first", "last", ["$.IntendedFor"], None)
