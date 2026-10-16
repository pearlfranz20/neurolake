"""audit/datasets.py on the fixture lake: exported counts match fct_dq_issue exactly,
and the rendered pages escape dataset text and cap long example lists."""

import importlib.util
import json
from pathlib import Path

import duckdb
import pytest

_spec = importlib.util.spec_from_file_location(
    "datasets", Path(__file__).parents[1] / "audit" / "datasets.py"
)
datasets = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(datasets)


@pytest.fixture(scope="module")
def exported(built_lake, tmp_path_factory):
    out = tmp_path_factory.mktemp("datasets") / "datasets.json"
    missing = str(out.parent / "absent")  # no history or pairs for the fixtures
    datasets.export(str(built_lake / "neurolake.duckdb"), str(out), missing, missing, 1)
    return out


def _truth(built_lake, sql):
    con = duckdb.connect(str(built_lake / "neurolake.duckdb"), read_only=True)
    return con.execute(sql).fetchall()


def test_counts_match_fct_dq_issue(built_lake, exported):
    data = json.loads(exported.read_text())
    truth = {
        (ds, rule, sev): n
        for ds, rule, sev, n in _truth(
            built_lake, "select dataset_id, rule_id, severity, count(*) from marts.fct_dq_issue group by all"
        )
    }
    got = {
        (d["id"], r["rule_id"], r["severity"]): r["count"] for d in data["datasets"] for r in d["findings"]
    }
    assert got == truth
    per_ds = {
        ds: (e, w, i)
        for ds, e, w, i in _truth(
            built_lake,
            """select d.dataset_id,
                      count(i.issue_id) filter (where i.severity = 'error'),
                      count(i.issue_id) filter (where i.severity = 'warning'),
                      count(i.issue_id) filter (where i.severity = 'info')
               from marts.dim_dataset d left join marts.fct_dq_issue i using (dataset_id) group by 1""",
        )
    }
    assert {d["id"]: (d["errors"], d["warnings"], d["info"]) for d in data["datasets"]} == per_ds
    for d in data["datasets"]:
        assert d["status"] == datasets._status(d["errors"], d["warnings"])
        assert all(len(r["examples"]) == min(r["count"], 1) for r in d["findings"])


def test_render(exported, tmp_path):
    data = json.loads(exported.read_text())
    data["datasets"][0]["name"] = "<script>alert(1)</script>"
    src = tmp_path / "datasets.json"
    src.write_text(json.dumps(data))
    datasets.render_pages(str(src), str(tmp_path / "docs"))
    out = tmp_path / "docs" / "datasets"

    pages = {p.stem for p in out.glob("ds*.html")}
    assert pages == {d["id"] for d in data["datasets"]}
    index = (out / "index.html").read_text()
    assert index.count("<tr data-text=") == len(data["datasets"])

    first = data["datasets"][0]["id"]
    for page in (index, (out / f"{first}.html").read_text()):
        assert "<script>alert(1)</script>" not in page
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page
        assert page.startswith("<!doctype html>")

    # the cap is 1 example per rule, so every rule with more findings says how many are left
    capped = [(d, r) for d in data["datasets"] for r in d["findings"] if r["count"] > 1]
    assert capped
    for d, r in capped:
        assert f"and {r['count'] - 1:,} more" in (out / f"{d['id']}.html").read_text()
