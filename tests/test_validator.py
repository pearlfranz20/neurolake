"""audit/validator.py: mapping, comparison and classification on small fake validator
output and lake facts. No network, no node, no lake."""

import csv
import gzip
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]


def _load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "audit" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


validator = _load("validator")
render = _load("render")

RULE_IDS = [r["rule_id"] for r in csv.DictReader((ROOT / "dbt" / "seeds" / "dq_rules.csv").open())]


def issue(key, code, files, severity="error"):
    return {
        "code": code,
        "key": key,
        "severity": severity,
        "reason": "",
        "additional_file_count": 0,
        "n_files": len(files),
        "files": files,
    }


def rec(ds, issues, output="json"):
    return {
        "dataset_id": ds,
        "snapshot": "1.0.0",
        "validator_version": "1.15.0",
        "flags": list(validator.FLAGS),
        "exit_code": 1 if issues else 0,
        "runtime_s": 1.0,
        "output": output,
        "issues": issues,
    }


def test_map_rules_exist_in_dq_rules_seed():
    rows = validator.load_map()
    assert rows
    assert {r["neurolake_rule_id"] for r in rows} <= set(RULE_IDS)
    assert all(r["justification"].strip() for r in rows)
    assert all(r["check"] and r["validator_key"] for r in rows)


def test_derivative_rules_match_the_model():
    sql = (ROOT / "dbt" / "models" / "marts" / "fct_dq_issue.sql").read_text()
    scoped = sql.split("u.rule_id in (", 1)[1].split(")", 1)[0]
    assert {s.strip().strip("'") for s in scoped.split(",") if s.strip()} == set(validator.DERIVATIVE_RULES)


def test_schema_errors_split_by_what_they_say():
    i = issue("JSON_SCHEMA_VALIDATION_ERROR", 55, [])
    assert validator.refined_key(i, "dataset_description.json", " should have required property 'Name'") == (
        "JSON_SCHEMA_VALIDATION_ERROR/dataset_description_required"
    )
    assert validator.refined_key(
        i, "sub-01/eeg/x_eeg.json", " should have required property 'SoftwareFilters'"
    ) == ("JSON_SCHEMA_VALIDATION_ERROR/sidecar_required")
    assert validator.refined_key(i, "dataset_description.json", ".Funding should be array") == (
        "JSON_SCHEMA_VALIDATION_ERROR/other"
    )
    assert validator.refined_key(issue("TSV_EQUAL_ROWS", 22, []), "a.tsv", "row 3") == "TSV_EQUAL_ROWS"


def test_assessment():
    assert validator.assessment(rec("ds1", [])) is None
    assert validator.assessment(None) == "not run"
    assert validator.assessment(rec("ds1", [], output="unparseable")) == "crashed without JSON output"
    blocked = rec("ds1", [issue("QUICK_VALIDATION_FAILED", 61, [["ds1", None]])])
    assert validator.assessment(blocked) == "QUICK_VALIDATION_FAILED"


def test_warnings_count_ignored_do_not():
    r = rec(
        "ds1",
        [
            issue("NO_T1W", 53, [["", None]], severity="ignore"),
            issue("SCANS_FILENAME_NOT_MATCH_DATASET", 129, [["sub-01/sub-01_scans.tsv", "x"]], "warning"),
        ],
    )
    assert set(validator.validator_hits(r)) == {"SCANS_FILENAME_NOT_MATCH_DATASET"}


def test_slim_keeps_mapped_file_lists_only():
    files = [[f"sub-{i:02d}/x.tsv", "e"] for i in range(20)]
    kept = validator.slim_issue(issue("TSV_EQUAL_ROWS", 22, list(files)), {"TSV_EQUAL_ROWS"})
    assert len(kept["files"]) == 20 and kept["files"][0][1] == "e"
    cut = validator.slim_issue(issue("INCONSISTENT_SUBJECTS", 38, list(files)), {"TSV_EQUAL_ROWS"})
    assert cut["n_files"] == 20 and len(cut["files"]) == 5 and cut["files"][0][1] is None


def test_record_round_trip_is_deterministic(tmp_path):
    r = rec("ds1", [issue("TSV_EQUAL_ROWS", 22, [["a.tsv", "row 2"]])])
    a, b = tmp_path / "a.json.gz", tmp_path / "b.json.gz"
    validator.write_record(a, r)
    validator.write_record(b, dict(reversed(list(r.items()))))
    assert a.read_bytes() == b.read_bytes()
    assert validator.read_record(a) == r
    with gzip.open(a, "rt") as fh:
        assert json.load(fh)["dataset_id"] == "ds1"


def test_duplicate_key_values():
    doc = '{"IntendedFor": ["a"], "X": 1, "IntendedFor": ["b"]}'
    assert validator.duplicate_key_values(doc, "IntendedFor") == [["a"], ["b"]]
    assert validator.duplicate_key_values('{"IntendedFor": "a"}', "IntendedFor") == ["a"]


def test_probe_needs_its_positive_control():
    ok = {"expected": {"PARTICIPANT_ID_MISMATCH": False, "PARTICIPANT_ID_PATTERN": True}}
    assert validator.never_emitted(ok) == {"PARTICIPANT_ID_MISMATCH"}
    blind = {"expected": {"PARTICIPANT_ID_MISMATCH": False, "PARTICIPANT_ID_PATTERN": False}}
    assert validator.never_emitted(blind) == set()
    assert validator.never_emitted(None) == set()


# One dataset per situation the classification has to handle.
DATASETS = [f"ds{i}" for i in range(1, 11)]
FIELDMAP_DOC = json.dumps({"IntendedFor": ["ses-01/func/gone_bold.nii.gz"]})[:-1] + (
    ', "IntendedFor": ["ses-02/func/here_bold.nii.gz"]}'
)


@pytest.fixture()
def comparison():
    tags = {ds: "1.0.0" for ds in DATASETS}
    records = {
        # both flag a malformed participants row
        "ds1": rec("ds1", [issue("TSV_EQUAL_ROWS", 22, [["participants.tsv", "row 3"]])]),
        # neurolake-only participants mismatch: the validator never reports it (probe)
        "ds2": rec("ds2", []),
        # validator-only malformed row in a file neurolake does not parse
        "ds3": rec("ds3", [issue("TSV_EQUAL_ROWS", 22, [["task-a_events.tsv", "row 36"]])]),
        # validator-only: .bidsignore hides dataset_description.json from the validator
        "ds4": rec("ds4", [issue("DATASET_DESCRIPTION_JSON_MISSING", 57, [])]),
        # validator could not assess; neurolake has a finding
        "ds5": rec("ds5", [issue("QUICK_VALIDATION_FAILED", 61, [["ds5", None]])]),
        # neurolake-only required fields on a recording with no sidecar
        "ds6": rec("ds6", []),
        # neurolake-only required field 1.15 does not require (version drift)
        "ds7": rec("ds7", []),
        # neurolake-only IntendedFor from a duplicate key
        "ds8": rec("ds8", []),
        # clean on both sides
        "ds9": rec("ds9", []),
        # validator-only required field neurolake's seed does not list (version drift)
        "ds10": rec(
            "ds10",
            [
                issue(
                    "JSON_SCHEMA_VALIDATION_ERROR",
                    55,
                    [
                        [
                            "sub-01/meg/sub-01_task-a_meg.json",
                            " should have required property 'DigitizedLandmarks'",
                        ]
                    ],
                )
            ],
        ),
    }
    issues = [
        ("ds1", "malformed_tsv_row", "participants.tsv", "row 3 has a different number of columns"),
        ("ds2", "participant_without_data", "sub-03", "listed in participants.tsv but no data files found"),
        ("ds5", "participant_without_data", "sub-09", "listed in participants.tsv but no data files found"),
        (
            "ds6",
            "missing_required_sidecar_field",
            "sub-01/eeg/sub-01_task-a_eeg.set",
            "SamplingFrequency (REQUIRED) not set by any applicable sidecar",
        ),
        (
            "ds7",
            "missing_required_sidecar_field",
            "sub-01/eeg/sub-01_task-a_eeg.set",
            "NewField (REQUIRED) not set by any applicable sidecar",
        ),
        (
            "ds8",
            "fieldmap_intended_for_missing_target",
            "sub-01/fmap/sub-01_fieldmap.json",
            "IntendedFor target not found: ses-01/func/gone_bold.nii.gz",
        ),
        (
            "ds9",
            "missing_participants_tsv",
            "participants.tsv",
            "dataset has subjects but no participants.tsv",
        ),
    ]
    facts = {
        "dataset_type": {ds: "raw" for ds in DATASETS},
        "core_ignored": {"ds4": {"dataset_description.json"}},
        "read_paths": {
            "ds1": {"participants.tsv"},
            "ds3": {"participants.tsv"},
            "ds10": {"sub-01/meg/sub-01_task-a_meg.json"},
        },
        "sidecarless": {"ds6": {"sub-01/eeg/sub-01_task-a_eeg.set"}},
        "required_seed": {"eeg": {"SamplingFrequency"}, "meg": {"SamplingFrequency"}},
        "intended_for_docs": {("ds8", "sub-01/fmap/sub-01_fieldmap.json"): FIELDMAP_DOC},
        "files": {"ds8": {"sub-01/ses-02/func/here_bold.nii.gz"}},
    }
    probes = {"expected": {"PARTICIPANT_ID_MISMATCH": False, "PARTICIPANT_ID_PATTERN": True}}
    handchecks = [
        {"dataset_id": "ds3", "check": "tsv_row_length", "direction": "only_validator", "note": "x"}
    ]
    return validator.build_comparison(
        tags, records, issues, facts, validator.load_map(), probes, RULE_IDS, handchecks
    )


def _dis(result, ds, check):
    return next(d for d in result["disagreements"] if d["dataset_id"] == ds and d["check"] == check)


def test_agreement_only_over_assessed(comparison):
    assert comparison["validator"]["assessed"] == 9
    tsv = next(c for c in comparison["checks"] if c["check"] == "tsv_row_length")
    assert (tsv["both"], tsv["only_validator"], tsv["only_neurolake"], tsv["neither"]) == (1, 1, 0, 7)
    part = next(c for c in comparison["checks"] if c["check"] == "participants_vs_data")
    assert part["only_neurolake"] == 1 and part["not_assessed_neurolake_flagged"] == 1
    assert part["both"] + part["only_neurolake"] + part["only_validator"] + part["neither"] == 9


@pytest.mark.parametrize(
    ("ds", "check", "direction", "category"),
    [
        ("ds2", "participants_vs_data", "only_neurolake", "real_validator_miss"),
        ("ds3", "tsv_row_length", "only_validator", "real_neurolake_miss"),
        ("ds4", "description_missing", "only_validator", "bidsignore_semantics"),
        ("ds5", "participants_vs_data", "validator_not_assessed", "validator_not_run"),
        ("ds6", "required_sidecar_field", "only_neurolake", "real_validator_miss"),
        ("ds7", "required_sidecar_field", "only_neurolake", "version_drift"),
        ("ds8", "intended_for", "only_neurolake", "other"),
        ("ds10", "required_sidecar_field", "only_validator", "version_drift"),
    ],
)
def test_every_disagreement_is_classified(comparison, ds, check, direction, category):
    d = _dis(comparison, ds, check)
    assert (d["direction"], d["category"]) == (direction, category)
    assert "unexplained" not in d["reason"]


def test_no_disagreement_left_unexplained(comparison):
    assert len(comparison["disagreements"]) == 8
    assert all(d["category"] in validator.CATEGORIES for d in comparison["disagreements"])


def test_unmatched_columns(comparison):
    no_eq = {r["rule_id"] for r in comparison["neurolake_rules_without_validator_equivalent"]}
    assert no_eq == {"missing_participants_tsv"}
    assert comparison["validator_errors_without_neurolake_rule"] == []


def test_handcheck_must_match_a_disagreement(comparison):
    tags = {"ds1": "1.0.0"}
    with pytest.raises(SystemExit, match="hand check"):
        validator.build_comparison(
            tags,
            {"ds1": rec("ds1", [])},
            [],
            {
                "dataset_type": {},
                "core_ignored": {},
                "read_paths": {},
                "sidecarless": {},
                "required_seed": {},
                "intended_for_docs": {},
                "files": {},
            },
            validator.load_map(),
            None,
            RULE_IDS,
            [{"dataset_id": "ds1", "check": "tsv_row_length", "direction": "only_validator", "note": "x"}],
        )


def test_unknown_rule_in_map_is_rejected():
    rows = [{"check": "c", "validator_key": "K", "neurolake_rule_id": "no_such_rule", "justification": "j"}]
    with pytest.raises(SystemExit, match="unknown rules"):
        validator.build_comparison({}, {}, [], {}, rows, None, RULE_IDS, [])


def test_section_states_method_and_guards_claims(comparison):
    html = "\n".join(render.validator_section(comparison))
    assert "1.15.0" in html and "2.x" in html
    assert "--gitTreeMode" in html and "--ignoreNiftiHeaders" in html
    assert "could assess 9 of the 10" in html
    assert "catches what the validator misses" not in html
    # the real-miss sentence carries its numbers
    assert "2 disagreements in 2 datasets" in html
