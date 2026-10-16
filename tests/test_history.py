"""The snapshot comparison in audit/history.py, on two builds of the fixtures: the
"latest" build fixes one defect in ds900003 and introduces another."""

import importlib.util
from pathlib import Path

import pytest

from neurolake import bronze, fixtures
from neurolake.sources import LocalBidsSource
from neurolake.transform import run_dbt

_spec = importlib.util.spec_from_file_location("history", Path(__file__).parents[1] / "audit" / "history.py")
history = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(history)


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    root = tmp_path_factory.mktemp("history")
    for side, snapshot in (("first", "1.0.0"), ("latest", "2.0.0")):
        for ds_root in fixtures.generate(root / f"bids-{side}"):
            if side == "latest" and ds_root.name == "ds900003":
                # fixed: the short participants row gets its missing column
                (ds_root / "participants.tsv").write_text(
                    "participant_id\tage\tsex\nsub-01\t30\tF\nsub-02\t33\tM\n"
                )
                # introduced: a new subject with data but no participants row
                (ds_root / "sub-03" / "anat").mkdir(parents=True)
                (ds_root / "sub-03" / "anat" / "sub-03_T1w.nii.gz").write_bytes(b"\0" * 64)
            bronze.ingest(LocalBidsSource(ds_root, snapshot=snapshot), root / side)
        run_dbt(root / side, ["build", "--quiet"])
    return history.analyze(str(root), str(root / "history.json"))


def test_cohort_excludes_derivatives(result):
    ids = {d["dataset_id"] for d in result["datasets"]}
    assert "ds900004" not in ids
    assert "ds900003" in ids


def test_fix_and_regression_are_attributed(result):
    rules = {r["rule_id"]: r for r in result["rules"]}
    assert rules["malformed_tsv_row"]["cleared"] == 1
    assert rules["subject_missing_from_participants"]["appeared"] == 1
    fates = {f["fate"]: f for f in result["issue_fates"]}
    assert fates["resolved"]["datasets"] == 1
    assert fates["introduced"]["datasets"] == 1


def test_untouched_datasets_are_unchanged(result):
    for d in result["datasets"]:
        if d["dataset_id"] != "ds900003":
            assert (d["first_errors"], d["first_warnings"]) == (d["latest_errors"], d["latest_warnings"])


def test_sign_test():
    assert history._sign_test(0, 0) == 1.0
    assert history._sign_test(5, 5) == 1.0
    assert history._sign_test(10, 0) == pytest.approx(2 / 1024)
