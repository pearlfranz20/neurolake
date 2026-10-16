"""audit/datasets.py on the fixture lake: exported counts match fct_dq_issue exactly,
first-vs-latest data is attached only for the audited snapshot, and the rendered pages
escape dataset text, cap long example lists by object, and refuse unsafe ids."""

import csv
import importlib.util
import json
import sys
from pathlib import Path

import duckdb
import pytest

_spec = importlib.util.spec_from_file_location(
    "audit_datasets", Path(__file__).parents[1] / "audit" / "datasets.py"
)
datasets = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = datasets
_spec.loader.exec_module(datasets)


def _lake_db(built_lake) -> str:
    return str(built_lake / "neurolake.duckdb")


def _truth(built_lake, sql):
    con = duckdb.connect(_lake_db(built_lake), read_only=True)
    return con.execute(sql).fetchall()


@pytest.fixture(scope="module")
def snapshots(built_lake):
    return dict(_truth(built_lake, "select dataset_id, snapshot from marts.dim_dataset order by 1"))


@pytest.fixture(scope="module")
def exported(built_lake, snapshots, tmp_path_factory):
    """Export with a cap of 1 object, a history entry for the first dataset (plus one for
    the second at a different snapshot, which must be ignored) and a pairs file that marks
    the third as having one snapshot."""
    tmp = tmp_path_factory.mktemp("datasets")
    a, b, c = list(snapshots)[:3]
    history = {
        "datasets": [
            {
                "dataset_id": a,
                "first_snapshot": "0.9.0",
                "latest_snapshot": snapshots[a],
                "first_errors": 7,
                "first_warnings": 0,
                "latest_errors": 0,
                "latest_warnings": 0,
            },
            {
                "dataset_id": b,
                "first_snapshot": "0.9.0",
                "latest_snapshot": "not-the-audited-one",
                "first_errors": 1,
                "first_warnings": 1,
                "latest_errors": 1,
                "latest_warnings": 1,
            },
        ]
    }
    (tmp / "history.json").write_text(json.dumps(history))
    with open(tmp / "pairs.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["dataset_id", "first", "latest"])
        w.writerow([c, snapshots[c], snapshots[c]])
    out = tmp / "datasets.json"
    datasets.export(_lake_db(built_lake), str(out), str(tmp / "history.json"), str(tmp / "pairs.csv"), 1)
    return json.loads(out.read_text())


def test_counts_match_fct_dq_issue(built_lake, exported):
    truth = {
        (ds_id, rule, sev): (n, objs, dets)
        for ds_id, rule, sev, n, objs, dets in _truth(
            built_lake,
            """select dataset_id, rule_id, severity, count(*), count(distinct object_id),
                      count(distinct detail)
               from marts.fct_dq_issue group by all""",
        )
    }
    got = {
        (d["id"], r["rule_id"], r["severity"]): (r["count"], r["objects"], r["details"])
        for d in exported["datasets"]
        for r in d["findings"]
    }
    assert got == truth
    per_ds = {
        ds_id: (err, warn, info)
        for ds_id, err, warn, info in _truth(
            built_lake,
            """select d.dataset_id,
                      count(i.issue_id) filter (where i.severity = 'error'),
                      count(i.issue_id) filter (where i.severity = 'warning'),
                      count(i.issue_id) filter (where i.severity = 'info')
               from marts.dim_dataset d left join marts.fct_dq_issue i using (dataset_id) group by 1""",
        )
    }
    assert {d["id"]: (d["errors"], d["warnings"], d["info"]) for d in exported["datasets"]} == per_ds
    for d in exported["datasets"]:
        assert d["status"] == datasets._status(d["errors"], d["warnings"])
        for r in d["findings"]:
            # cap of one object: every finding of that object, and nothing else
            assert len({o for o, _ in r["examples"]}) == 1
            assert 1 <= len(r["examples"]) <= r["count"]


def test_first_and_only_snapshot(exported, snapshots):
    a, b, c = list(snapshots)[:3]
    rows = {d["id"]: d for d in exported["datasets"]}
    assert rows[a]["first"] == {"snapshot": "0.9.0", "errors": 7, "warnings": 0, "status": "errors"}
    assert "only_snapshot" not in rows[a]
    assert "first" not in rows[b] and "only_snapshot" not in rows[b]  # history for another snapshot
    assert rows[c]["only_snapshot"] is True and "first" not in rows[c]


def test_change():
    d = {"errors": 0, "warnings": 3, "status": "warnings only"}
    assert datasets.change(d) == ("not compared", "", None)
    assert datasets.change({**d, "only_snapshot": True}) == ("only snapshot", "", None)
    first = {"errors": 1, "warnings": 3, "status": "errors"}
    assert datasets.change({**d, "first": first}) == ("errors to warnings only", "better", -1)
    first = {"errors": 0, "warnings": 1, "status": "warnings only"}
    assert datasets.change({**d, "first": first}) == ("1 to 3 findings", "worse", 2)
    first = {"errors": 0, "warnings": 3, "status": "warnings only"}
    assert datasets.change({**d, "first": first}) == ("no change", "", 0)


def test_join_details():
    j = datasets.join_details
    assert j(["a b c"]) == ["a b c"]
    assert j(["same detail", "same detail"]) == ["same detail"]
    # shared ending
    assert j(
        [
            "EEGReference (REQUIRED) not set by any applicable sidecar",
            "TaskName (REQUIRED) not set by any applicable sidecar",
        ]
    ) == ["EEGReference, TaskName (REQUIRED) not set by any applicable sidecar"]
    # shared beginning
    assert j(["IntendedFor target not found: a.nii", "IntendedFor target not found: b.nii"]) == [
        "IntendedFor target not found: a.nii, b.nii"
    ]
    # nothing in common: one line each, no semicolons
    assert j(["file is not valid JSON", "row 3 is short"]) == ["file is not valid JSON", "row 3 is short"]


def _render(data, tmp_path):
    src = tmp_path / "datasets.json"
    src.write_text(json.dumps(data))
    datasets.render_pages(str(src), str(tmp_path / "docs"))
    return tmp_path / "docs" / "datasets"


def test_render(exported, tmp_path):
    data = json.loads(json.dumps(exported))
    data["datasets"][0]["name"] = "<script>alert(1)</script>"
    # the fixtures never hit a rule on more than one object, so add a capped one
    data["datasets"][1]["findings"].append(
        {
            "rule_id": "missing_required_sidecar_field",
            "severity": "error",
            "count": 30,
            "objects": 10,
            "details": 2,
            "examples": [
                ["sub-01/eeg/a.edf", "A (REQUIRED) not set"],
                ["sub-01/eeg/a.edf", "B (REQUIRED) not set"],
            ],
        }
    )
    out = _render(data, tmp_path)
    assert "and 9 more, with 28 findings" in (out / f"{data['datasets'][1]['id']}.html").read_text()

    pages = {p.stem for p in out.glob("ds*.html")}
    assert pages == {d["id"] for d in data["datasets"]}
    index = (out / "index.html").read_text()
    assert index.count("<tr data-text=") == len(data["datasets"])

    first = data["datasets"][0]["id"]
    for page in (index, (out / f"{first}.html").read_text()):
        assert "<script>alert(1)</script>" not in page
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page
        assert page.startswith("<!doctype html>")

    # one object shown per rule, so every rule with more objects says how many are left
    capped = [(d, r) for d in data["datasets"] for r in d["findings"] if r["objects"] > 1]
    for d, r in capped:
        assert f"and {r['objects'] - 1:,} more" in (out / f"{d['id']}.html").read_text()


@pytest.mark.parametrize(
    "field,value", [("id", "javascript:alert(1)//"), ("id", "../evil"), ("snapshot", "1.0.0/../x")]
)
def test_bad_id_writes_nothing(exported, tmp_path, field, value):
    out = _render(exported, tmp_path)
    before = {p.name: p.read_bytes() for p in out.iterdir()}
    data = json.loads(json.dumps(exported))
    data["datasets"][-1][field] = value
    with pytest.raises(ValueError):
        _render(data, tmp_path)
    assert {p.name: p.read_bytes() for p in out.iterdir()} == before
