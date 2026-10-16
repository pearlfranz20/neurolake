"""Cross-check neurolake's rules against the official bids-validator.

    python audit/validator.py run     [pairs.csv] [lake_dir] [validator_bin]
    python audit/validator.py probe   [validator_bin]
    python audit/validator.py compare [lake_dir] [out.json]

`run` shallow-clones the latest snapshot of every audited dataset (the `latest`
column of history_pairs.csv, asserted equal to `dim_dataset.snapshot` in the latest
lake), runs the legacy bids-validator on it and saves a compact, gzipped copy of its
output under audit/validator/<dataset>.json.gz.

Only bids-validator 1.15 (npm) is runnable here: the current schema-based 2.x is
distributed through JSR/Deno. The clone holds metadata in git and imaging data as
dangling git-annex symlinks, so the validator runs with --gitTreeMode (read the file
listing and sizes from git), --ignoreNiftiHeaders (no imaging bytes to read) and
--verbose (otherwise each issue's file list is cut at 10).

`compare` matches the two tools at dataset level for every validator code that tests
the same thing as a neurolake rule (audit/validator_map.csv), counts datasets the
validator could not assess separately, and classifies every disagreement from data:
version drift, .bidsignore semantics, validator not run, real misses, or other.
"""

from __future__ import annotations

import csv
import gzip
import json
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb

from neurolake.sources.github_mirror import MIRROR_URL, _git

AUDIT = Path(__file__).parent
OUT_DIR = AUDIT / "validator"
MAP_CSV = AUDIT / "validator_map.csv"
CLONES = Path("data/validator/clones")
VALIDATOR_BIN = "data/validator/node_modules/.bin/bids-validator"
FLAGS = ("--json", "--gitTreeMode", "--ignoreNiftiHeaders", "--verbose")
EVIDENCE_CHARS = 300

# Validator codes that mean "could not look at the dataset", not "found a problem".
NOT_ASSESSED_CODES = {
    61: "QUICK_VALIDATION_FAILED",
    114: "INCOMPLETE_DATASET",
}
# Validator checks that read NIfTI headers. They were switched off, so their absence
# says nothing about the data.
NIFTI_HEADER_CODES = {
    "NIFTI_HEADER_UNREADABLE",
    "NIFTI_UNIT",
    "NIFTI_PIXDIM",
    "NIFTI_DIMENSION",
    "NIFTI_TOO_SMALL",
    "NIFTI_PIXDIM4",
    "NIFTI_TR_MISMATCH",
    "REPETITION_TIME_MISMATCH",
    "VOLUME_COUNT_MISMATCH",
    "SLICE_TIMING_GREATER_THAN_TR",
    "EFFECTIVEECHOSPACING_TOO_LARGE",
    "TOTALREADOUTTIME_MISMATCH",
    "EFFECTIVEECHOSPACING_LARGER_THAN_TOTALREADOUTTIME",
    "BOLD_NOT_4D",
    "DWI_MISSING_BVEC",
    "SLICETIMING_VALUES_GREATOR_THAN_REPETITION_TIME",
}


# --------------------------------------------------------------------------- run


def clone_snapshot(ds: str, tag: str, dest: Path) -> Path:
    """Shallow clone of one snapshot tag. An existing clone at that tag is reused."""
    if (dest / ".git").exists():
        have = _git("-C", str(dest), "tag", "--points-at", "HEAD").split()
        if tag in have:
            return dest
        raise RuntimeError(f"{dest} exists at {have}, not {tag}; remove it first")
    dest.parent.mkdir(parents=True, exist_ok=True)
    _git(
        "-c",
        "advice.detachedHead=false",
        "clone",
        "--quiet",
        "--depth",
        "1",
        "--branch",
        tag,
        "--single-branch",
        MIRROR_URL.format(dataset=ds),
        str(dest),
    )
    return dest


def latest_tags(pairs_csv: str | Path, lake: str | Path) -> dict[str, str]:
    """Latest tag per dataset from the pairs file, asserted equal to the lake's snapshot."""
    with open(pairs_csv, newline="") as fh:
        tags = {r["dataset_id"]: r["latest"].strip() for r in csv.DictReader(fh)}
    con = duckdb.connect(str(Path(lake) / "neurolake.duckdb"), read_only=True)
    lake_tags = dict(con.execute("select dataset_id, snapshot from marts.dim_dataset").fetchall())
    con.close()
    if tags != lake_tags:
        diff = sorted(set(tags.items()) ^ set(lake_tags.items()))
        raise SystemExit(f"pairs file and lake disagree on snapshots: {diff[:10]}")
    return tags


def mapped_base_keys(map_csv: str | Path = MAP_CSV) -> set[str]:
    with open(map_csv, newline="") as fh:
        return {r["validator_key"].split("/")[0] for r in csv.DictReader(fh)}


def compact(raw: dict, keep_files: set[str]) -> dict:
    """Keep what the comparison needs. Issues whose key is in `keep_files` (the mapped
    ones) keep every file with its path and a truncated evidence string. Every other
    issue keeps its file count and the first few paths: those only enter the comparison
    as "the validator reported this code in this dataset"."""
    issues = []
    for group in ("errors", "warnings", "ignored"):
        for i in raw.get("issues", {}).get(group, []):
            files = []
            for f in i.get("files") or []:
                fobj = f.get("file") or {}
                path = (fobj.get("relativePath") or fobj.get("path") or "").lstrip("/")
                ev = f.get("evidence")
                files.append([path, ev[:EVIDENCE_CHARS] if isinstance(ev, str) else None])
            issues.append(
                slim_issue(
                    {
                        "code": i.get("code"),
                        "key": i.get("key"),
                        "severity": i.get("severity"),
                        "reason": i.get("reason"),
                        "additional_file_count": i.get("additionalFileCount", 0),
                        "n_files": len(files),
                        "files": files,
                    },
                    keep_files,
                )
            )
    s = raw.get("summary", {})
    return {
        "issues": issues,
        "summary": {k: s.get(k) for k in ("subjects", "sessions", "tasks", "modalities", "totalFiles")},
    }


def slim_issue(issue: dict, keep_files: set[str], n: int = 5) -> dict:
    issue.setdefault("n_files", len(issue["files"]))
    if issue["key"] not in keep_files:
        issue["files"] = [[p, None] for p, _ in issue["files"][:n]]
    return issue


def validate_one(ds: str, tag: str, validator_bin: str, keep_files: set[str], clones: Path = CLONES) -> dict:
    dest = clone_snapshot(ds, tag, clones / ds)
    t0 = time.monotonic()
    proc = subprocess.run([validator_bin, str(dest), *FLAGS], capture_output=True, text=True, timeout=900)
    runtime = round(time.monotonic() - t0, 2)
    rec = {
        "dataset_id": ds,
        "snapshot": tag,
        "validator_version": validator_version(validator_bin),
        "flags": list(FLAGS),
        "exit_code": proc.returncode,
        "runtime_s": runtime,
    }
    try:
        rec.update(compact(json.loads(proc.stdout), keep_files))
        rec["output"] = "json"
    except json.JSONDecodeError:
        rec["output"] = "unparseable"
        # 1.15 prints an unhandled rejection (a crash) to stdout instead of JSON
        rec["stdout_tail"] = re.sub(r"\x1b\[[0-9;]*m", "", proc.stdout).replace(str(Path.cwd()), ".")[-1500:]
        rec["stderr_tail"] = proc.stderr[-500:]
        rec["issues"] = []
    return rec


def write_record(path: Path, rec: dict) -> None:
    # mtime=0 and sorted keys, so a rerun with the same result leaves the file unchanged
    data = json.dumps(rec, sort_keys=True, separators=(",", ":")).encode()
    with open(path, "wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as fh:
        fh.write(data)


def read_record(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


_VERSION: dict[str, str] = {}


def validator_version(validator_bin: str) -> str:
    if validator_bin not in _VERSION:
        out = subprocess.run([validator_bin, "--version"], capture_output=True, text=True).stdout
        _VERSION[validator_bin] = out.strip().splitlines()[-1]
    return _VERSION[validator_bin]


def run(
    pairs_csv: str = "audit/history_pairs.csv",
    lake: str = "data/history/latest",
    validator_bin: str = VALIDATOR_BIN,
    workers: int = 6,
) -> None:
    tags = latest_tags(pairs_csv, lake)
    print(f"bids-validator {validator_version(validator_bin)} on {len(tags)} snapshots")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    failed = []
    with ThreadPoolExecutor(max_workers=int(workers)) as pool:
        keep = mapped_base_keys()
        futures = {pool.submit(validate_one, ds, tag, validator_bin, keep): ds for ds, tag in tags.items()}
        for fut in as_completed(futures):
            ds = futures[fut]
            try:
                rec = fut.result()
            except Exception as exc:
                failed.append(ds)
                print(f"{ds} FAILED: {exc}", file=sys.stderr)
                continue
            write_record(OUT_DIR / f"{ds}.json.gz", rec)
    if failed:
        raise SystemExit(f"{len(failed)} datasets failed to clone or run; rerun to retry: {failed}")
    print(f"wrote {len(tags)} files to {OUT_DIR}")


# ----------------------------------------------------------------------- compare


VALIDATOR_SEVERITIES = ("error", "warning")
NL_SEVERITIES = ("error", "warning")
# neurolake applies only these rules to derivative datasets (fct_dq_issue.sql, `scoped`)
DERIVATIVE_RULES = (
    "invalid_json",
    "malformed_tsv_row",
    "metadata_read_error",
    "metadata_not_fetched",
    "dataset_description_missing",
    "dataset_description_missing_field",
)
# REQUIRED sidecar fields in bids-validator 1.15.0: the `required` lists of
# validators/json/schemas/{eeg,ieeg,meg}.json (bundled into dist/commonjs/index.js), plus
# the two BOLD fields it checks in code (REPETITION_TIME_MUST_DEFINE, TASK_NAME_MUST_DEFINE).
VALIDATOR_REQUIRED = {
    "eeg": {"TaskName", "SamplingFrequency", "SoftwareFilters", "EEGReference", "PowerLineFrequency"},
    "ieeg": {"TaskName", "SamplingFrequency", "PowerLineFrequency", "SoftwareFilters", "iEEGReference"},
    "meg": {
        "TaskName",
        "SamplingFrequency",
        "PowerLineFrequency",
        "DewarPosition",
        "SoftwareFilters",
        "DigitizedLandmarks",
        "DigitizedHeadPoints",
    },
    "bold": {"TaskName", "RepetitionTime"},
}
CATEGORIES = (
    "version_drift",
    "bidsignore_semantics",
    "validator_not_run",
    "real_neurolake_miss",
    "real_validator_miss",
    "neurolake_artefact",
    "other",
)
# Issues about a file that is absent carry no path; this is the file they are about.
IMPLIED_FILE = {"DATASET_DESCRIPTION_JSON_MISSING": "dataset_description.json"}
_REQUIRED_PROP = re.compile(r"required property '([^']+)'")


def load_map(path: str | Path = MAP_CSV) -> list[dict]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def checks_from_map(rows: list[dict]) -> dict[str, dict]:
    """check -> {"validator_keys": set, "rules": set}"""
    out: dict[str, dict] = {}
    for r in rows:
        c = out.setdefault(r["check"], {"validator_keys": set(), "rules": set()})
        c["validator_keys"].add(r["validator_key"])
        c["rules"].add(r["neurolake_rule_id"])
    return out


def refined_key(issue: dict, path: str, evidence: str | None) -> str:
    """The validator key a file-level occurrence counts under. JSON schema errors are
    split by what they say, because only "missing required property" means the same
    as a neurolake rule."""
    key = issue["key"]
    if key != "JSON_SCHEMA_VALIDATION_ERROR":
        return key
    if evidence and _REQUIRED_PROP.search(evidence):
        if path == "dataset_description.json":
            return f"{key}/dataset_description_required"
        return f"{key}/sidecar_required"
    return f"{key}/other"


def validator_hits(rec: dict) -> dict[str, list[list]]:
    """refined key -> [[path, evidence], ...] for error and warning issues."""
    hits: dict[str, list[list]] = {}
    for i in rec.get("issues", []):
        if i["severity"] not in VALIDATOR_SEVERITIES:
            continue
        files = i["files"] or [["", None]]
        for path, ev in files:
            path = path or IMPLIED_FILE.get(i["key"], "")
            hits.setdefault(refined_key(i, path, ev), []).append([path, ev])
    return hits


def assessment(rec: dict | None) -> str | None:
    """None when the validator assessed the dataset, else why it could not."""
    if rec is None:
        return "not run"
    if rec.get("output") != "json":
        return "crashed without JSON output"
    codes = {i["code"] for i in rec.get("issues", [])}
    blocked = sorted(NOT_ASSESSED_CODES[c] for c in codes & set(NOT_ASSESSED_CODES))
    return ", ".join(blocked) if blocked else None


# ------------------------------------------------------------------------ probes

PROBES_JSON = AUDIT / "validator_probes.json"


PROBE_MODES = {
    "normal": ("--json", "--ignoreNiftiHeaders", "--verbose"),
    "git-tree": ("--json", "--gitTreeMode", "--ignoreNiftiHeaders", "--verbose"),
}


def probe(validator_bin: str = VALIDATOR_BIN, out: str | Path = PROBES_JSON) -> dict:
    """Run the validator on a tiny synthetic dataset with known defects, to tell "the
    validator found nothing" apart from "the validator never reports this". It runs in
    normal mode and in git-tree mode (the dataset is committed to a throwaway repository).

    participants.tsv lists sub-01 and sub-03 and writes `04` without its prefix, while the
    data folders are sub-01 and sub-02: so sub-02 is missing from the table and sub-03 has
    no data (PARTICIPANT_ID_MISMATCH), and `04` lacks the prefix (PARTICIPANT_ID_PATTERN,
    the positive control showing participants.tsv was read). A key counts as reported if
    any mode reports it.

    The source explains the result: in 1.15.0 fullTest declares `const participants = null`
    (dist/commonjs/cli.js:137602, index.js:133003) and passes that constant to the
    participants check, so PARTICIPANT_ID_MISMATCH is unreachable."""
    import tempfile

    planted = ("PARTICIPANT_ID_MISMATCH", "PARTICIPANT_ID_PATTERN")
    modes = {}
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "probe"
        for sub in ("01", "02"):
            (root / f"sub-{sub}" / "anat").mkdir(parents=True)
            (root / f"sub-{sub}" / "anat" / f"sub-{sub}_T1w.nii.gz").write_bytes(gzip.compress(b"\0" * 352))
        (root / "dataset_description.json").write_text(
            json.dumps({"Name": "probe", "BIDSVersion": "1.8.0", "Authors": ["A", "B"]})
        )
        (root / "participants.tsv").write_text("participant_id\tage\nsub-01\t30\nsub-03\t31\n04\t32\n")
        (root / "README").write_text("Synthetic dataset for probing which issues the validator reports.\n")
        _git("-C", str(root), "init", "-q")
        _git("-C", str(root), "add", "-A")
        _git(
            "-C",
            str(root),
            "-c",
            "user.name=probe",
            "-c",
            "user.email=probe@example.org",
            "commit",
            "-qm",
            "p",
        )
        for mode, flags in PROBE_MODES.items():
            proc = subprocess.run(
                [validator_bin, str(root), *flags], capture_output=True, text=True, timeout=300
            )
            raw = json.loads(proc.stdout)
            keys = sorted({i["key"] for group in raw["issues"].values() for i in group})
            modes[mode] = {
                "flags": list(flags),
                "keys_reported": keys,
                "expected": {k: k in keys for k in planted},
            }
    result = {
        "validator_version": validator_version(validator_bin),
        "setup": "participants.tsv lists sub-01, sub-03 and 04 (no prefix); data folders are sub-01, sub-02",
        "source": "fullTest declares `const participants = null` (dist/commonjs/cli.js:137602, "
        "dist/commonjs/index.js:133003) and passes it to the participants check",
        "modes": modes,
        "expected": {k: any(m["expected"][k] for m in modes.values()) for k in planted},
    }
    Path(out).write_text(json.dumps(result, indent=2) + "\n")
    print(f"wrote {out}: {result['expected']}")
    return result


def never_emitted(probes: dict | None) -> set[str]:
    """Validator keys the probe planted a defect for but that were not reported, provided
    the positive control fired (otherwise the probe proves nothing)."""
    if not probes or not probes["expected"].get("PARTICIPANT_ID_PATTERN"):
        return set()
    return {k for k, seen in probes["expected"].items() if not seen}


# --------------------------------------------------------------------- lake facts


def lake_facts(con) -> dict:
    """Per-dataset facts from the latest lake that the classification relies on."""

    def q(sql):
        return con.execute(sql).fetchall()

    facts: dict = {
        "dataset_type": dict(q("select dataset_id, dataset_type from marts.dim_dataset")),
        "core_ignored": {
            d: set(f)
            for d, f in q(
                "select dataset_id, bidsignored_core_files from staging.stg_ingest_runs where is_current"
            )
        },
        "read_paths": {},
        "sidecarless": {},
        "required_seed": {},
        "intended_for_docs": {},
    }
    # metadata files neurolake parses (JSON documents and TSV tables)
    for d, p in q(
        "select distinct dataset_id, path from staging.stg_json_docs "
        "union select distinct dataset_id, path from staging.stg_tsv_rows"
    ):
        facts["read_paths"].setdefault(d, set()).add(p)
    for d, p in q("select dataset_id, path from marts.fct_scan where n_sidecars = 0"):
        facts["sidecarless"].setdefault(d, set()).add(p)
    for suffix, field in q(
        "select suffix, field from reference.required_sidecar_fields where requirement = 'required'"
    ):
        facts["required_seed"].setdefault(suffix, set()).update(field.split("|"))
    # raw text of fieldmap sidecars with a missing IntendedFor target
    for d, p, doc in q("""
        select j.dataset_id, j.path, j.doc from staging.stg_json_docs j
        where (j.dataset_id, j.path) in (
            select dataset_id, fmap_sidecar_path from intermediate.int_fieldmap_targets
            where target_exists = false)
    """):
        facts["intended_for_docs"][(d, p)] = doc
    facts["files"] = {}
    for d, p in q("""
        select dataset_id, path from staging.stg_files
        where dataset_id in (select dataset_id from intermediate.int_fieldmap_targets where not target_exists)
    """):
        facts["files"].setdefault(d, set()).add(p)
    return facts


def duplicate_key_values(doc: str, key: str) -> list:
    """Every value a JSON document's top-level object gives `key`, in file order. Plain
    json.loads keeps only the last one."""
    pairs = json.loads(doc, object_pairs_hook=lambda kv: kv)
    return [v for k, v in pairs if k == key] if isinstance(pairs, list) else []


# ----------------------------------------------------------------- classification


def classify(d: dict, facts: dict, gaps: set[str]) -> tuple[str, str]:
    """(category, reason) for one dataset-level disagreement, from data only.

    `d` has dataset_id, check, direction ("only_neurolake" / "only_validator" /
    "validator_not_assessed"), rules, validator_keys, nl (list of (rule, object, detail))
    and v (refined key -> [[path, evidence]])."""
    ds = d["dataset_id"]
    if d["direction"] == "validator_not_assessed":
        return "validator_not_run", f"validator could not assess the dataset ({d['assessment']})"

    if d["direction"] == "only_validator":
        vfiles = sorted({p for hits in d["v"].values() for p, _ in hits if p})
        core = facts["core_ignored"].get(ds, set())
        if core & set(vfiles):
            return (
                "bidsignore_semantics",
                f".bidsignore matches {', '.join(sorted(core & set(vfiles)))}: the validator ignores it, "
                "neurolake never ignores core files",
            )
        if facts["dataset_type"].get(ds) == "derivative" and not set(d["rules"]) & set(DERIVATIVE_RULES):
            return "other", "derivative dataset: neurolake applies only file-integrity rules to derivatives"
        unread = [p for p in vfiles if p not in facts["read_paths"].get(ds, set())]
        if vfiles and len(unread) == len(vfiles):
            return (
                "real_neurolake_miss",
                f"neurolake does not parse {', '.join(unread[:3])}"
                + (f" and {len(unread) - 3} more" if len(unread) > 3 else ""),
            )
        if d["check"] == "required_sidecar_field":
            fields = {
                m.group(1)
                for hits in d["v"].values()
                for _, ev in hits
                if ev and (m := _REQUIRED_PROP.search(ev))
            }
            seeded = set().union(*facts["required_seed"].values()) if facts["required_seed"] else set()
            if fields and not fields & seeded:
                return (
                    "version_drift",
                    f"{', '.join(sorted(fields))} required by validator 1.15 but not by the current schema "
                    "rules neurolake follows",
                )
        return "other", "unexplained: validator reports it, neurolake does not"

    # only_neurolake
    if set(d["validator_keys"]) & gaps:
        keys = ", ".join(sorted(set(d["validator_keys"]) & gaps))
        return (
            "real_validator_miss",
            f"validator 1.15.0 never reports {keys}: a synthetic dataset with that defect passed "
            "(audit/validator_probes.json)",
        )
    if d["check"] == "required_sidecar_field":
        by_suffix: dict[str, set] = {}
        paths = set()
        for _, obj, detail in d["nl"]:
            paths.add(obj)
            suffix = obj.rsplit("_", 1)[-1].split(".", 1)[0]
            by_suffix.setdefault(suffix, set()).update(detail.split(" (", 1)[0].split(" or "))
        if all(not (f & VALIDATOR_REQUIRED.get(sfx, set())) for sfx, f in by_suffix.items()):
            return "version_drift", "fields required by the current schema, not by validator 1.15"
        if paths <= facts["sidecarless"].get(ds, set()):
            return (
                "real_validator_miss",
                "the recordings have no sidecar at all: validator 1.15.0 checks EEG, iEEG and MEG sidecar "
                "files against its schema but does not require that such a recording has one",
            )
    if d["check"] == "intended_for":
        sidecars = sorted({obj for _, obj, _ in d["nl"]})
        dup = []
        for sc in sidecars:
            doc = facts["intended_for_docs"].get((ds, sc))
            vals = duplicate_key_values(doc, "IntendedFor") if doc else []
            if len(vals) > 1:
                last = vals[-1] if isinstance(vals[-1], list) else [vals[-1]]
                subject = sc.split("/", 1)[0]
                files = facts["files"].get(ds, set())
                last_ok = all(t.startswith("bids:") or f"{subject}/{t}" in files for t in last)
                dup.append(last_ok)
        if dup and len(dup) == len(sidecars) and all(dup):
            return (
                "neurolake_artefact",
                f"{len(sidecars)} fieldmap sidecar{'s' if len(sidecars) != 1 else ''} set IntendedFor twice: "
                "neurolake keeps the first value (targets missing), the validator and most JSON parsers the "
                "last (targets exist). JSON leaves duplicate keys undefined, so the real finding is the "
                "duplicate key",
            )
    return "other", "unexplained: neurolake reports it, the validator does not"


# ------------------------------------------------------------------------ compare

HANDCHECK_CSV = AUDIT / "validator_handchecks.csv"


def compare(
    lake: str = "data/history/latest",
    out: str = "audit/validator_comparison.json",
    pairs_csv: str = "audit/history_pairs.csv",
) -> dict:
    tags = latest_tags(pairs_csv, lake)
    records = {p.name.removesuffix(".json.gz"): read_record(p) for p in sorted(OUT_DIR.glob("*.json.gz"))}
    for ds, rec in records.items():
        if tags.get(ds) != rec["snapshot"]:
            raise SystemExit(f"{ds}: validated {rec['snapshot']}, lake has {tags.get(ds)}")
    probes = json.loads(PROBES_JSON.read_text()) if PROBES_JSON.exists() else None
    con = duckdb.connect(str(Path(lake) / "neurolake.duckdb"), read_only=True)
    issues = con.execute(
        "select dataset_id, rule_id, object_id, detail from marts.fct_dq_issue "
        "where severity in ('error', 'warning')"
    ).fetchall()
    facts = lake_facts(con)
    rule_ids = [r[0] for r in con.execute("select rule_id from reference.dq_rules order by 1").fetchall()]
    snap_dates = con.execute(
        "select min(snapshot_created_at)::date, max(snapshot_created_at)::date from marts.dim_dataset"
    ).fetchone()
    con.close()
    result = build_comparison(tags, records, issues, facts, load_map(), probes, rule_ids, load_handchecks())
    result["lake"] = {
        "datasets": len(tags),
        "snapshots_from": str(snap_dates[0]),
        "snapshots_to": str(snap_dates[1]),
    }
    Path(out).write_text(json.dumps(result, indent=2) + "\n")
    print(f"wrote {out}")
    return result


def load_handchecks(path: str | Path = HANDCHECK_CSV) -> list[dict]:
    if not Path(path).exists():
        return []
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def build_comparison(
    tags: dict[str, str],
    records: dict[str, dict],
    issues: list[tuple],
    facts: dict,
    map_rows: list[dict],
    probes: dict | None,
    rule_ids: list[str],
    handchecks: list[dict],
) -> dict:
    """The whole comparison, from plain inputs so it can be tested without a lake."""
    unknown = sorted({r["neurolake_rule_id"] for r in map_rows} - set(rule_ids))
    if unknown:
        raise SystemExit(f"validator_map.csv names unknown rules: {unknown}")
    checks = checks_from_map(map_rows)
    nl: dict[str, dict[str, list]] = {}
    for ds, rule, obj, detail in issues:
        nl.setdefault(ds, {}).setdefault(rule, []).append((rule, obj, detail))
    gaps = never_emitted(probes)
    assess = {ds: assessment(records.get(ds)) for ds in tags}
    hits = {ds: validator_hits(records[ds]) if ds in records and not assess[ds] else {} for ds in tags}
    assessed = sorted(ds for ds in tags if not assess[ds])

    per_check, disagreements = [], []
    agreed: set[tuple[str, str, str]] = set()
    for name, spec in checks.items():
        rules, vkeys = sorted(spec["rules"]), sorted(spec["validator_keys"])
        counts = {"both": 0, "only_neurolake": 0, "only_validator": 0, "neither": 0}
        not_assessed_flagged = 0
        for ds in sorted(tags):
            n_found = [f for r in rules for f in nl.get(ds, {}).get(r, [])]
            v_found = {k: hits[ds][k] for k in vkeys if k in hits[ds]}
            if assess[ds]:
                if n_found:
                    not_assessed_flagged += 1
                    direction = "validator_not_assessed"
                else:
                    continue
            elif n_found and v_found:
                counts["both"] += 1
                agreed.add((ds, name, "both"))
                continue
            elif not n_found and not v_found:
                counts["neither"] += 1
                continue
            else:
                direction = "only_neurolake" if n_found else "only_validator"
                counts[direction] += 1
            d = {
                "dataset_id": ds,
                "check": name,
                "direction": direction,
                "rules": rules,
                "validator_keys": vkeys,
                "assessment": assess[ds],
                "nl": n_found,
                "v": v_found,
            }
            d["category"], d["reason"] = classify(d, facts, gaps)
            disagreements.append(
                {
                    "dataset_id": ds,
                    "check": name,
                    "direction": direction,
                    "category": d["category"],
                    "reason": d["reason"],
                    "neurolake_findings": len(n_found),
                    "neurolake_objects": len({o for _, o, _ in n_found}),
                    "neurolake_rules": sorted({r for r, _, _ in n_found}),
                    "neurolake_examples": [f"{r}: {o}" for r, o, _ in n_found[:3]],
                    "validator_files": sum(len(v) for v in v_found.values()),
                    "validator_examples": [f"{k}: {p}" for k, v in v_found.items() for p, _ in v[:3]][:3],
                }
            )
        flagged = counts["both"] + counts["only_neurolake"] + counts["only_validator"]
        per_check.append(
            {
                "check": name,
                "validator_keys": vkeys,
                "rules": rules,
                **counts,
                "assessed": len(assessed),
                "agree": counts["both"] + counts["neither"],
                "flagged_by_either": flagged,
                "not_assessed_neurolake_flagged": not_assessed_flagged,
            }
        )

    mapped_rules = {r["neurolake_rule_id"] for r in map_rows}
    rule_ds: dict[str, set] = {}
    rule_n: dict[str, int] = {}
    for ds, rule, _, _ in issues:
        rule_ds.setdefault(rule, set()).add(ds)
        rule_n[rule] = rule_n.get(rule, 0) + 1
    neurolake_only = [
        {"rule_id": r, "datasets": len(rule_ds[r]), "findings": rule_n[r]}
        for r in sorted(rule_ds, key=lambda r: (-len(rule_ds[r]), r))
        if r not in mapped_rules
    ]

    mapped_keys = {r["validator_key"] for r in map_rows}
    v_err: dict[str, dict] = {}
    for ds in assessed:
        for i in records[ds]["issues"]:
            if i["severity"] != "error":
                continue
            if i["key"] in NIFTI_HEADER_CODES:
                continue
            per_key: dict[str, int] = {}
            for p, ev in i["files"] or [["", None]]:
                k = refined_key(i, p, ev)
                per_key[k] = per_key.get(k, 0) + 1
            if len(per_key) == 1:  # unsplit issues may store only the first few files
                per_key = {k: i.get("n_files", len(i["files"])) for k in per_key}
            for k, n in per_key.items():
                if k in mapped_keys:
                    continue
                e = v_err.setdefault(k, {"key": k, "datasets": set(), "files": 0})
                e["datasets"].add(ds)
                e["files"] += n
    validator_only = sorted(
        ({"key": e["key"], "datasets": len(e["datasets"]), "files": e["files"]} for e in v_err.values()),
        key=lambda e: (-e["datasets"], e["key"]),
    )

    cat_counts: dict[str, dict[str, int]] = {}
    for d in disagreements:
        c = cat_counts.setdefault(d["direction"], {k: 0 for k in CATEGORIES})
        c[d["category"]] += 1

    # a hand check is about a disagreement, or about a pair both tools flagged ("both")
    keyset = {(d["dataset_id"], d["check"], d["direction"]) for d in disagreements} | agreed
    for h in handchecks:
        if (h["dataset_id"], h["check"], h["direction"]) not in keyset:
            raise SystemExit(f"hand check does not match a disagreement or agreement: {h}")

    rec0 = next((records[ds] for ds in assessed), {})
    runtimes = sorted(r["runtime_s"] for r in records.values())
    return {
        "validator": {
            "version": rec0.get("validator_version"),
            "flags": rec0.get("flags"),
            "datasets_run": len(records),
            "assessed": len(assessed),
            "runtime_median_s": runtimes[len(runtimes) // 2] if runtimes else None,
            "not_assessed": [{"dataset_id": ds, "reason": assess[ds]} for ds in sorted(tags) if assess[ds]],
            "nifti_header_checks": "off (--ignoreNiftiHeaders); their codes are excluded",
        },
        "checks": per_check,
        "disagreements": disagreements,
        "category_counts": cat_counts,
        "neurolake_rules_without_validator_equivalent": neurolake_only,
        "validator_errors_without_neurolake_rule": validator_only,
        "probes": probes,
        "hand_checked": handchecks,
    }


if __name__ == "__main__":
    cmd, *args = sys.argv[1:] or ["compare"]
    {"run": run, "probe": probe, "compare": compare}[cmd](*args)
