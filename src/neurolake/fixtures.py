"""Generate small synthetic BIDS datasets with known, deliberate defects.

The fixtures mirror the shape of real OpenNeuro datasets (inheritance-principle
sidecars, scans/participants TSVs, fieldmaps, EEG) but use tiny placeholder files,
so the whole pipeline and its data-quality rules can be tested offline. Every
injected defect is listed in EXPECTED_ISSUES so tests can assert they are caught.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# (dataset_id, rule_id, subject_or_object) for every defect injected below.
EXPECTED_ISSUES = {
    ("ds900001", "subject_missing_from_participants", "sub-05"),
    ("ds900001", "participant_without_data", "sub-07"),
    ("ds900001", "inconsistent_repetition_time", "task-nback"),
    ("ds900001", "fieldmap_intended_for_missing_target", "sub-02"),
    ("ds900001", "duplicate_json_key", "sub-03/fmap/sub-03_phasediff.json"),
    ("ds900002", "missing_required_sidecar_field", "sub-04"),
    ("ds900002", "inconsistent_sampling_frequency", "task-oddball"),
    ("ds900003", "invalid_json", "sub-01/anat/sub-01_T1w.json"),
    ("ds900003", "malformed_tsv_row", "participants.tsv"),
    ("ds900003", "dataset_description_missing_field", "BIDSVersion"),
    ("ds900003", "ambiguous_sidecar_inheritance", "sub-01/anat/sub-01_T1w.nii.gz"),
    ("ds900003", "metadata_read_error", "sub-02/sub-02_scans.tsv"),
    ("ds900002", "metadata_not_fetched", "sub-01/eeg/sub-01_task-rest_eeg.json"),
    ("ds900004", "derivative_dataset", "ds900004"),
    ("ds900005", "subject_folders_not_at_root", "study/"),
    ("ds900003", "bidsignore_matches_core_files", ".bidsignore"),
    ("ds900006", "bidsignore_hides_all_data", ".bidsignore"),
    ("ds900005", "participant_id_without_prefix", "participants.tsv"),
    ("ds900006", "duplicate_participant_id", "participants.tsv"),
}

# Real-world patterns that must NOT be reported (each was a false positive once).
MUST_NOT_FLAG = {
    # a repeated key resolves to its last value, whose IntendedFor target exists
    ("ds900001", "fieldmap_intended_for_missing_target", "sub-03"),
    # BIDS accepts VolumeTiming instead of RepetitionTime
    ("ds900001", "missing_required_sidecar_field", "task-sparse"),
    # sidecar exists but was not fetched, so the fields are unknown, not missing
    ("ds900002", "missing_required_sidecar_field", "task-rest"),
    # MEGIN crosstalk calibration files have no task or sidecar
    ("ds900002", "missing_required_sidecar_field", "acq-crosstalk"),
    # files listed in .bidsignore are outside the standard and never checked
    ("ds900003", "invalid_json", "extra/notes.json"),
    # a scans.tsv row pointing at a bidsignored file points at a file that exists
    ("ds900003", "scans_tsv_missing_file", "acq-scout"),
    # subjects whose data is all bidsignored still have data
    ("ds900006", "participant_without_data", ""),
    # a scans.tsv row may point at a folder-format recording (CTF .ds)
    ("ds900002", "scans_tsv_missing_file", "meg.ds"),
    # raw-data rules do not apply to derivative datasets
    ("ds900004", "fieldmap_intended_for_missing_target", ""),
}


def _write(path: Path, content: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")


def _json(path: Path, obj: dict) -> None:
    _write(path, json.dumps(obj, indent=2))


def _tsv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    lines = ["\t".join(header)] + ["\t".join(r) for r in rows]
    _write(path, "\n".join(lines) + "\n")


def _blob(path: Path, n: int) -> None:
    _write(path, b"\0" * n)


def _fmri(root: Path) -> None:
    _json(
        root / "dataset_description.json",
        {
            "Name": "Synthetic resting state and n-back fMRI",
            "BIDSVersion": "1.9.0",
            "License": "CC0",
            "Authors": ["A. Researcher", "B. Researcher"],
            "DatasetType": "raw",
        },
    )
    # sub-05 has data but is missing here; sub-07 is listed but has no data.
    # Sex is coded inconsistently on purpose; the model normalises it.
    _tsv(
        root / "participants.tsv",
        ["participant_id", "age", "sex", "handedness", "group"],
        [
            ["sub-01", "24", "M", "right", "control"],
            ["sub-02", "31", "F", "right", "patient"],
            ["sub-03", "n/a", "male", "left", "control"],
            ["sub-04", "27", "female", "R", "patient"],
            ["sub-06", "45", "f", "right", "control"],
            ["sub-07", "38", "M", "right", "patient"],
        ],
    )
    # Inheritance principle: top-level sidecars apply to every matching file.
    _json(root / "task-rest_bold.json", {"TaskName": "rest", "RepetitionTime": 2.0})
    _json(root / "task-nback_bold.json", {"TaskName": "nback", "RepetitionTime": 2.0})
    _json(
        root / "T1w.json",
        {"Manufacturer": "Siemens", "MagneticFieldStrength": 3, "EchoTime": 0.00298},
    )

    for i in range(1, 7):
        sub = f"sub-{i:02d}"
        _blob(root / sub / "anat" / f"{sub}_T1w.nii.gz", 2048 + i)
        _blob(root / sub / "func" / f"{sub}_task-rest_bold.nii.gz", 8192 + i)
        scans = [[f"anat/{sub}_T1w.nii.gz", f"2024-03-0{i}T09:00:00"]]
        scans.append([f"func/{sub}_task-rest_bold.nii.gz", f"2024-03-0{i}T09:10:00"])
        for run in (1, 2):
            name = f"{sub}_task-nback_run-{run}_bold.nii.gz"
            _blob(root / sub / "func" / name, 6144 + i)
            scans.append([f"func/{name}", f"2024-03-0{i}T09:2{run}:00"])
        _tsv(root / sub / f"{sub}_scans.tsv", ["filename", "acq_time"], scans)

        # Diffusion for even subjects only, with a subject-level sidecar.
        if i % 2 == 0:
            _blob(root / sub / "dwi" / f"{sub}_dwi.nii.gz", 12288)
            _write(root / sub / "dwi" / f"{sub}_dwi.bval", "0 1000 1000\n")
            _write(root / sub / "dwi" / f"{sub}_dwi.bvec", "0 1 0\n0 0 1\n0 0 0\n")
            _json(
                root / sub / "dwi" / f"{sub}_dwi.json",
                {"PhaseEncodingDirection": "j-", "EchoTime": 0.089},
            )

        # Fieldmaps: sub-02 points at a run that does not exist.
        if i in (1, 2):
            _blob(root / sub / "fmap" / f"{sub}_phasediff.nii.gz", 1024)
            run = "" if i == 1 else "_run-9"
            target = f"func/{sub}_task-rest{run}_bold.nii.gz"
            _json(
                root / sub / "fmap" / f"{sub}_phasediff.json",
                {"EchoTime1": 0.00492, "EchoTime2": 0.00738, "IntendedFor": [f"bids::{sub}/{target}"]},
            )

    # sub-03's fieldmap repeats IntendedFor. The first value points at a run that does
    # not exist, the last at one that does; readers that keep the last value (most of
    # them, and neurolake) must see a valid target.
    _blob(root / "sub-03" / "fmap" / "sub-03_phasediff.nii.gz", 1024)
    _write(
        root / "sub-03" / "fmap" / "sub-03_phasediff.json",
        '{"EchoTime1": 0.00492, "EchoTime2": 0.00738, '
        '"IntendedFor": ["bids::sub-03/func/sub-03_task-rest_run-7_bold.nii.gz"], '
        '"IntendedFor": ["bids::sub-03/func/sub-03_task-rest_bold.nii.gz"]}',
    )

    # sub-03 run-2 overrides the inherited TR (a classic acquisition mismatch).
    _json(
        root / "sub-03" / "func" / "sub-03_task-nback_run-2_bold.json",
        {"RepetitionTime": 1.5},
    )

    # Sparse sampling: VolumeTiming replaces RepetitionTime, which BIDS allows.
    _json(root / "task-sparse_bold.json", {"TaskName": "sparse", "VolumeTiming": [0.0, 4.0, 8.0]})
    _blob(root / "sub-01" / "func" / "sub-01_task-sparse_bold.nii.gz", 4096)

    # MP2RAGE: two inversions whose sidecars differ only by the inv- entity. Each
    # volume must inherit its own InversionTime, never the other one's.
    for inv, ti in ((1, 0.8), (2, 2.7)):
        stem = f"sub-06_inv-{inv}_MP2RAGE"
        _blob(root / "sub-06" / "anat" / f"{stem}.nii.gz", 4096)
        _json(root / "sub-06" / "anat" / f"{stem}.json", {"InversionTime": ti, "MagneticFieldStrength": 7})


def _eeg(root: Path) -> None:
    _json(
        root / "dataset_description.json",
        {"Name": "Synthetic auditory oddball EEG", "BIDSVersion": "1.8.0", "License": "CC-BY-4.0"},
    )
    _tsv(
        root / "participants.tsv",
        ["participant_id", "age", "sex"],
        [["sub-01", "22", "F"], ["sub-02", "29", "M"], ["sub-03", "35", "F"], ["sub-04", "41", "M"]],
    )
    for i in range(1, 5):
        sub = f"sub-{i:02d}"
        _blob(root / sub / "eeg" / f"{sub}_task-oddball_eeg.edf", 4096)
        _tsv(
            root / sub / "eeg" / f"{sub}_task-oddball_events.tsv",
            ["onset", "duration", "trial_type"],
            [["1.0", "0.1", "standard"], ["2.5", "0.1", "deviant"]],
        )
        # No top-level eeg sidecar, so sub-04 ends up with no SamplingFrequency.
        if i == 4:
            continue
        _json(
            root / sub / "eeg" / f"{sub}_task-oddball_eeg.json",
            {
                "TaskName": "oddball",
                "SamplingFrequency": 256 if i == 3 else 512,
                "PowerLineFrequency": 60,
                "EEGReference": "Cz",
                "SoftwareFilters": "n/a",
            },
        )

    # A resting EEG run whose sidecar lives in git-annex and was never fetched.
    _blob(root / "sub-01" / "eeg" / "sub-01_task-rest_eeg.edf", 4096)
    link = root / "sub-01" / "eeg" / "sub-01_task-rest_eeg.json"
    os.symlink("../../.git/annex/objects/Xx/Yy/MD5E-s312--0f1e2d.json/MD5E-s312--0f1e2d.json", link)

    # MEGIN crosstalk calibration: not a recording.
    _blob(root / "sub-02" / "meg" / "sub-02_acq-crosstalk_meg.fif", 1000)

    _tsv(
        root / "sub-01" / "sub-01_scans.tsv",
        ["filename", "acq_time"],
        [["meg/sub-01_task-oddball_meg.ds", "n/a"]],
    )

    # CTF MEG stores a recording as a folder; it must count as one data file.
    ds = root / "sub-01" / "meg" / "sub-01_task-oddball_meg.ds"
    _blob(ds / "sub-01_task-oddball_meg.meg4", 30000)
    _blob(ds / "sub-01_task-oddball_meg.res4", 2000)
    _json(
        root / "sub-01" / "meg" / "sub-01_task-oddball_meg.json",
        {
            "TaskName": "oddball",
            "SamplingFrequency": 1200,
            "PowerLineFrequency": 60,
            "DewarPosition": "upright",
            "SoftwareFilters": "n/a",
        },
    )


def _broken(root: Path) -> None:
    # Missing BIDSVersion, malformed participants row, unparseable sidecar, two
    # sidecars at the same level for sub-01, and a scans file that is not UTF-8.
    _json(root / "dataset_description.json", {"Name": "Synthetic dataset with structural defects"})
    _write(
        root / "participants.tsv",
        "participant_id\tage\tsex\nsub-01\t30\tF\nsub-02\t33\n",
    )
    for i in (1, 2):
        sub = f"sub-{i:02d}"
        _blob(root / sub / "anat" / f"{sub}_T1w.nii.gz", 2048)
    _write(root / "sub-01" / "anat" / "sub-01_T1w.json", '{"Manufacturer": "GE", ')
    _json(root / "sub-02" / "anat" / "sub-02_T1w.json", {"Manufacturer": "GE"})
    _write(root / "extra" / "notes.json", "{not json")
    _blob(root / "sub-01" / "anat" / "sub-01_acq-scout_T1w.nii.gz", 512)
    _write(root / ".bidsignore", "# non-BIDS material\nextra/\n*acq-scout*\n**/*dat*\n")
    _tsv(
        root / "sub-01" / "sub-01_scans.tsv",
        ["filename", "acq_time"],
        [["anat/sub-01_acq-scout_T1w.nii.gz", "n/a"]],
    )
    _json(root / "T1w.json", {"MagneticFieldStrength": 3})
    _json(root / "sub-01_T1w.json", {"MagneticFieldStrength": 1.5})
    _write(root / "sub-02" / "sub-02_scans.tsv", b"filename\tacq_time\nanat/sub-02_T1w.nii.gz\t\xff\xfe\n")


def _derivative(root: Path) -> None:
    # Pipeline outputs published as a dataset: a raw-style check would wrongly flag
    # the fieldmap, whose target is a raw file not included here.
    _json(
        root / "dataset_description.json",
        {"Name": "Synthetic preprocessed outputs", "BIDSVersion": "1.9.0", "DatasetType": "derivative"},
    )
    _blob(root / "sub-01" / "fmap" / "sub-01_desc-preproc_fieldmap.nii.gz", 1024)
    _json(
        root / "sub-01" / "fmap" / "sub-01_desc-preproc_fieldmap.json",
        {"IntendedFor": "func/sub-01_task-rest_bold.nii.gz", "Units": "Hz"},
    )


def _nested(root: Path) -> None:
    # The whole dataset was uploaded one folder too deep.
    _json(root / "dataset_description.json", {"Name": "Synthetic nested upload", "BIDSVersion": "1.9.0"})
    _tsv(root / "participants.tsv", ["participant_id"], [["01"]])
    _blob(root / "study" / "sub-01" / "anat" / "sub-01_T1w.nii.gz", 2048)


def _ignored(root: Path) -> None:
    # A catch-all `*` in .bidsignore hides every data file from the checks.
    _json(
        root / "dataset_description.json", {"Name": "Synthetic over-ignored dataset", "BIDSVersion": "1.8.0"}
    )
    _tsv(root / "participants.tsv", ["participant_id"], [["sub-01"], ["sub-02"], ["sub-02"]])
    for sub in ("sub-01", "sub-02"):
        _blob(root / sub / "anat" / f"{sub}_T1w.nii.gz", 2048)
    _write(root / ".bidsignore", "*\n")


def generate(out_dir: str | Path) -> list[Path]:
    """Write the fixture datasets under out_dir and return their roots."""
    out = Path(out_dir)
    roots = []
    builders = (
        ("ds900001", _fmri),
        ("ds900002", _eeg),
        ("ds900003", _broken),
        ("ds900004", _derivative),
        ("ds900005", _nested),
        ("ds900006", _ignored),
    )
    for dataset_id, builder in builders:
        root = out / dataset_id
        builder(root)
        roots.append(root)
    return roots
