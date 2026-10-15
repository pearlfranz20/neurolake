-- One row per dataset: size, coverage and a simple readiness score, the share of
-- data files with no error- or warning-level issue attached to them.
with issues as (
    select * from {{ ref('fct_dq_issue') }}
),

issue_counts as (
    select
        dataset_id,
        count(*) filter (where severity = 'error') as n_errors,
        count(*) filter (where severity = 'warning') as n_warnings,
        count(*) filter (where severity = 'info') as n_info
    from issues
    group by dataset_id
),

-- issues that point at a specific data file or one of its sidecars
flagged_files as (
    select distinct s.data_file_id
    from {{ ref('fct_scan') }} as s
    inner join issues as i
        on i.dataset_id = s.dataset_id
        and i.severity in ('error', 'warning')
        and (
            (i.object_type = 'data_file' and i.object_id = s.path)
            or (i.object_type = 'file' and list_contains(s.sidecar_paths, i.object_id))
            or (i.object_type = 'subject' and i.object_id = 'sub-' || s.subject_label)
        )
),

file_scores as (
    select
        s.dataset_id,
        count(*) as n_scans,
        count(*) - count(ff.data_file_id) as n_clean_scans
    from {{ ref('fct_scan') }} as s
    left join flagged_files as ff using (data_file_id)
    group by s.dataset_id
)

select
    d.dataset_id,
    d.snapshot,
    d.dataset_name,
    d.bids_version,
    d.n_subjects,
    d.n_scans,
    d.datatypes,
    d.tasks,
    round(d.total_bytes / 1e9, 3) as total_gb,
    coalesce(ic.n_errors, 0) as n_errors,
    coalesce(ic.n_warnings, 0) as n_warnings,
    coalesce(ic.n_info, 0) as n_info,
    coalesce(fs.n_clean_scans, 0) as n_clean_scans,
    case when d.n_scans > 0 then round(100.0 * fs.n_clean_scans / d.n_scans, 1) end as clean_scan_pct,
    d.ingested_at
from {{ ref('dim_dataset') }} as d
left join issue_counts as ic using (dataset_id)
left join file_scores as fs using (dataset_id)
