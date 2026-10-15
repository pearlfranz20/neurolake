"""Summarise a neurolake audit lake into findings.json.

    python audit/analyze.py <lake_dir> [out.json]

Every number in the write-up comes from this script, so the findings can be
regenerated from scratch with `make audit`.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

SQL = {
    "sample": """
        select count(*) as datasets,
               sum(n_subjects) as subjects,
               sum(n_scans) as data_files,
               round(sum(total_bytes) / 1e12, 2) as terabytes,
               min(snapshot_created_at)::date as first_snapshot,
               max(snapshot_created_at)::date as last_snapshot,
               count(*) filter (where dataset_type = 'derivative') as derivative_datasets
        from marts.dim_dataset
    """,
    "datatypes": """
        select datatype, count(distinct dataset_id) as datasets, count(*) as files
        from marts.fct_scan group by 1 order by 2 desc
    """,
    # Raw datasets only: derivative datasets are outside most rules.
    "headline": """
        with raw as (select dataset_id from marts.dim_dataset where dataset_type <> 'derivative'),
        per as (
            select r.dataset_id,
                   count(*) filter (where i.severity = 'error') as errors,
                   count(*) filter (where i.severity = 'warning') as warnings
            from raw r left join marts.fct_dq_issue i using (dataset_id)
            group by 1
        )
        select count(*) as raw_datasets,
               count(*) filter (where errors > 0) as with_errors,
               count(*) filter (where warnings > 0) as with_warnings,
               count(*) filter (where errors = 0 and warnings = 0) as clean
        from per
    """,
    # One row per rule and severity: a rule can be a warning for some datasets and
    # informational for others (ambiguous sidecars before BIDS 1.7).
    "rules": """
        with raw as (select count(*) n from marts.dim_dataset where dataset_type <> 'derivative')
        select i.rule_id, i.severity, r.description,
               count(distinct i.dataset_id) as datasets,
               round(100.0 * count(distinct i.dataset_id) / any_value(raw.n), 1) as pct_datasets,
               count(*) as issues
        from marts.fct_dq_issue i
        join reference.dq_rules r using (rule_id)
        cross join raw
        where i.rule_id not in ('derivative_dataset')
        group by i.rule_id, i.severity, r.description
        order by case i.severity when 'error' then 0 when 'warning' then 1 else 2 end, datasets desc
    """,
    # Severity by rule: a rule can be error for some fields and warning for others.
    "rule_severity": """
        select rule_id, severity, count(distinct dataset_id) as datasets
        from marts.fct_dq_issue group by all order by 1, 2
    """,
    "missing_fields": """
        select regexp_extract(detail, '^(.*) not set', 1) as field, severity,
               count(distinct dataset_id) as datasets, count(*) as files
        from marts.fct_dq_issue
        where rule_id in ('missing_required_sidecar_field', 'missing_recommended_sidecar_field')
        group by all order by datasets desc
    """,
    "by_year": """
        with per as (
            select d.dataset_id, year(d.snapshot_created_at) as yr,
                   count(i.issue_id) filter (where i.severity in ('error', 'warning')) as n
            from marts.dim_dataset d
            left join marts.fct_dq_issue i using (dataset_id)
            where d.dataset_type <> 'derivative'
            group by all
        )
        select case when yr <= 2019 then '2019 and earlier' else yr::varchar end as period,
               count(*) as datasets,
               count(*) filter (where n = 0) as clean,
               round(100.0 * count(*) filter (where n = 0) / count(*), 1) as pct_clean
        from per group by 1 order by 1
    """,
    "bids_versions": """
        select coalesce(regexp_extract(bids_version, '^([0-9]+[.][0-9]+)', 1), 'missing or unparseable')
                   as version,
               count(*) as datasets
        from marts.dim_dataset group by 1 order by 2 desc
    """,
    "acquisition_heterogeneity": """
        select rule_id, dataset_id, object_id, detail
        from marts.fct_dq_issue
        where rule_id in ('inconsistent_repetition_time', 'inconsistent_sampling_frequency')
        order by rule_id, dataset_id
    """,
}

# Subjects present on only one side of the participants.tsv / data join. When the two
# sides match after normalising (case, leading zeros, a doubled "sub" prefix,
# separators), the mismatch is an ID formatting problem rather than missing data.
ID_MISMATCH = """
    with s as (
        select dataset_id, subject_label, in_participants_tsv, has_data,
               regexp_replace(
                   regexp_replace(lower(regexp_replace(subject_label, '^(sub[-_]?)+', '')), '[-_ ]', '', 'g'),
                   '^0+([0-9])', '\\1') as norm
        from marts.dim_subject
        where in_participants_tsv <> has_data
          -- only datasets that have a participants.tsv, and only raw datasets
          and dataset_id in (select distinct dataset_id from staging.stg_participants)
          and dataset_id in (select dataset_id from marts.dim_dataset where dataset_type <> 'derivative')
    ),
    per as (
        select dataset_id,
               count(*) filter (where in_participants_tsv) as only_in_tsv,
               count(*) filter (where has_data) as only_in_data,
               count(*) filter (where in_participants_tsv and norm in (
                   select norm from s s2 where s2.dataset_id = s.dataset_id and s2.has_data)) as format_pairs
        from s group by dataset_id
    )
    select dataset_id, only_in_tsv, only_in_data, format_pairs,
           format_pairs > 0 as has_format_mismatch
    from per order by dataset_id
"""


EXTRA = {
    "error_datasets": """
        select dataset_id, list(distinct rule_id order by rule_id) as rules, count(*) as issues
        from marts.fct_dq_issue where severity = 'error' group by 1 order by issues desc
    """,
    "recent_vs_older": """
        with per as (
            select d.dataset_id, year(d.snapshot_created_at) >= 2024 as recent,
                   count(i.issue_id) filter (where i.severity in ('error', 'warning')) as n,
                   bool_or(i.rule_id = 'missing_participants_tsv') as no_participants
            from marts.dim_dataset d left join marts.fct_dq_issue i using (dataset_id)
            where d.dataset_type <> 'derivative' group by all
        )
        select recent, count(*) as datasets, count(*) filter (where n = 0) as clean,
               count(*) filter (where no_participants) as no_participants
        from per group by 1 order by 1
    """,
    "bidsignore_core": """
        select dataset_id, detail from marts.fct_dq_issue
        where rule_id = 'bidsignore_matches_core_files' order by dataset_id
    """,
    "recordings_without_sidecar": """
        select dataset_id, datatype, count(*) as recordings
        from marts.fct_scan
        where datatype in ('eeg', 'meg', 'ieeg', 'func') and n_sidecars = 0
          and not has_unfetched_sidecar  -- unknown, not missing
          and dataset_id in (select dataset_id from marts.dim_dataset where dataset_type <> 'derivative')
        group by all order by recordings desc
    """,
    # participants.tsv values exactly as written, next to the data folder names
    "id_format_examples": """
        with tsv as (
            select dataset_id, list(participant_id order by source_row_number)[:3] as in_participants_tsv
            from staging.stg_participants p
            where dataset_id in ('ds001838', 'ds004226')
              and not exists (select 1 from marts.dim_subject s where s.dataset_id = p.dataset_id
                              and s.subject_label = p.subject_label and s.has_data)
            group by 1
        ),
        folders as (
            select dataset_id, list('sub-' || subject_label order by subject_label)[:3] as in_data
            from marts.dim_subject
            where dataset_id in ('ds001838', 'ds004226') and has_data and not in_participants_tsv
            group by 1
        )
        select * from tsv join folders using (dataset_id) order by 1
    """,
    "subjects_quartiles": """
        select quantile_cont(n_subjects, [0.25, 0.5, 0.75]) as q, max(n_subjects) as max
        from marts.dim_dataset where dataset_type <> 'derivative'
    """,
}


def _wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    p = k / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / (1 + z * z / n)
    return centre - half, centre + half


def fisher_exact(a: int, b: int, c: int, d: int) -> float:
    """Two-sided Fisher's exact test for the 2x2 table [[a, b], [c, d]]."""
    from math import comb

    n, row1, col1 = a + b + c + d, a + b, a + c

    def p(x: int) -> float:
        return comb(row1, x) * comb(n - row1, col1 - x) / comb(n, col1)

    observed = p(a)
    lo, hi = max(0, col1 - (n - row1)), min(row1, col1)
    return sum(p(x) for x in range(lo, hi + 1) if p(x) <= observed * (1 + 1e-9))


def rows(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def main(lake: str, out: str = "audit/findings.json") -> None:
    con = duckdb.connect(str(Path(lake) / "neurolake.duckdb"), read_only=True)
    result = {name: rows(con, sql) for name, sql in SQL.items()}
    result["id_mismatch"] = rows(con, ID_MISMATCH)
    result.update({name: rows(con, sql) for name, sql in EXTRA.items()})
    result["rule_count"] = con.execute(
        "select count(*) from reference.dq_rules where default_severity in ('error', 'warning')"
    ).fetchone()[0]
    # Estimate how many mirrors exist from the probe log: share of probed accessions
    # with a snapshot, scaled to the whole accession range.
    log = Path("audit/probe_log.csv")
    if log.exists():
        import csv

        probes = list(csv.DictReader(log.open()))
        found = sum(p["status"] == "found" for p in probes)
        result["mirror_estimate"] = {
            "probed": len(probes),
            "found": found,
            "max_id": 7500,
            "estimated_mirrors": round(found / len(probes) * 7500, -2),
            # Wilson 95% interval on the hit rate, scaled to the accession range
            "ci95": [round(x * 7500, -2) for x in _wilson(found, len(probes))],
        }
    older, recent = sorted(result["recent_vs_older"], key=lambda r: r["recent"])
    result["recent_vs_older_test"] = {
        "older_clean": older["clean"],
        "older_n": older["datasets"],
        "recent_clean": recent["clean"],
        "recent_n": recent["datasets"],
        "fisher_p": round(
            fisher_exact(
                recent["clean"],
                recent["datasets"] - recent["clean"],
                older["clean"],
                older["datasets"] - older["clean"],
            ),
            3,
        ),
        "no_participants_p": round(
            fisher_exact(
                recent["no_participants"],
                recent["datasets"] - recent["no_participants"],
                older["no_participants"],
                older["datasets"] - older["no_participants"],
            ),
            3,
        ),
    }
    Path(out).write_text(json.dumps(result, indent=2, default=str))
    print(f"wrote {out}")


if __name__ == "__main__":
    main(*sys.argv[1:])
