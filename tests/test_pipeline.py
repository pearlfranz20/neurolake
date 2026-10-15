"""End-to-end: every defect injected by the fixtures must surface in fct_dq_issue,
and clean data must not raise errors."""

import duckdb
import pytest

from neurolake.fixtures import EXPECTED_ISSUES, MUST_NOT_FLAG
from neurolake.report import build_report


@pytest.fixture(scope="module")
def con(built_lake):
    c = duckdb.connect(str(built_lake / "neurolake.duckdb"), read_only=True)
    yield c
    c.close()


def test_every_injected_defect_is_detected(con):
    issues = con.execute("select dataset_id, rule_id, object_id from marts.fct_dq_issue").fetchall()
    missing = [
        exp
        for exp in EXPECTED_ISSUES
        if not any(i[0] == exp[0] and i[1] == exp[1] and exp[2] in i[2] for i in issues)
    ]
    assert not missing, f"undetected defects: {missing}"


def test_known_false_positives_stay_quiet(con):
    issues = con.execute(
        "select dataset_id, rule_id, object_id || ' ' || detail from marts.fct_dq_issue"
    ).fetchall()
    hits = [
        (ds, rule, needle)
        for ds, rule, needle in MUST_NOT_FLAG
        if any(i[0] == ds and i[1] == rule and needle in i[2] for i in issues)
    ]
    assert not hits, f"false positives: {hits}"


def test_no_errors_outside_injected_defects(con):
    expected_rules = {(d, r) for d, r, _ in EXPECTED_ISSUES}
    errors = con.execute(
        "select distinct dataset_id, rule_id from marts.fct_dq_issue where severity = 'error'"
    ).fetchall()
    assert set(errors) <= expected_rules


def test_inheritance_resolves_overrides(con):
    rows = dict(
        con.execute(
            """select path, repetition_time_s from marts.fct_scan
           where dataset_id = 'ds900001' and subject_label = '03' and task_label = 'nback'"""
        ).fetchall()
    )
    assert rows == {
        "sub-03/func/sub-03_task-nback_run-1_bold.nii.gz": 2.0,
        "sub-03/func/sub-03_task-nback_run-2_bold.nii.gz": 1.5,
    }


def test_subject_dimension_keeps_mismatches(con):
    rows = con.execute(
        """select subject_label, in_participants_tsv, has_data from marts.dim_subject
           where dataset_id = 'ds900001' and subject_label in ('05', '07') order by 1"""
    ).fetchall()
    assert rows == [("05", False, True), ("07", True, False)]


def test_sex_and_handedness_normalised(con):
    assert con.execute(
        "select list(distinct sex order by sex) from marts.dim_subject where sex is not null"
    ).fetchone()[0] == ["F", "M"]


def test_acq_time_joined_from_scans_tsv(con):
    n_missing = con.execute(
        """select count(*) from marts.fct_scan
           where dataset_id = 'ds900001' and suffix in ('T1w', 'bold')
             and coalesce(task_label, '') <> 'sparse' and acq_time is null"""
    ).fetchone()[0]
    assert n_missing == 0


def test_report_renders(built_lake):
    page = build_report(built_lake / "neurolake.duckdb")
    for ds in ("ds900001", "ds900002", "ds900003", "ds900004"):
        assert ds in page
    assert "inconsistent_repetition_time" in page
    assert "<script" not in page


def test_inheritance_respects_every_entity(con):
    """MP2RAGE inversions differ only by inv-; each must get its own sidecar."""
    rows = dict(
        con.execute(
            """select path, effective_sidecar ->> 'InversionTime' from intermediate.int_effective_sidecars
           where path like '%MP2RAGE%'"""
        ).fetchall()
    )
    assert rows == {
        "sub-06/anat/sub-06_inv-1_MP2RAGE.nii.gz": "0.8",
        "sub-06/anat/sub-06_inv-2_MP2RAGE.nii.gz": "2.7",
    }


def test_folder_recordings_count_as_one_data_file(con):
    row = con.execute(
        "select size_bytes, sampling_frequency_hz from marts.fct_scan where extension = '.ds'"
    ).fetchall()
    assert row == [(32000, 1200.0)]


def test_unreadable_participants_does_not_look_missing(con):
    rules = {
        r
        for (r,) in con.execute(
            "select rule_id from marts.fct_dq_issue where dataset_id = 'ds900003'"
        ).fetchall()
    }
    assert "missing_participants_tsv" not in rules
