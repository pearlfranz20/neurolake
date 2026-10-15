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
    "rules": """
        with raw as (select count(*) n from marts.dim_dataset where dataset_type <> 'derivative')
        select i.rule_id, r.description,
               count(distinct i.dataset_id) as datasets,
               round(100.0 * count(distinct i.dataset_id) / any_value(raw.n), 1) as pct_datasets,
               count(*) as issues
        from marts.fct_dq_issue i
        join reference.dq_rules r using (rule_id)
        cross join raw
        where i.rule_id not in ('derivative_dataset')
        group by i.rule_id, r.description
        order by datasets desc, issues desc
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


def rows(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def main(lake: str, out: str = "audit/findings.json") -> None:
    con = duckdb.connect(str(Path(lake) / "neurolake.duckdb"), read_only=True)
    result = {name: rows(con, sql) for name, sql in SQL.items()}
    result["id_mismatch"] = rows(con, ID_MISMATCH)
    Path(out).write_text(json.dumps(result, indent=2, default=str))
    print(f"wrote {out}")


if __name__ == "__main__":
    main(*sys.argv[1:])
