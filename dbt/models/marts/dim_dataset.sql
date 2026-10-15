with runs as (
    select * from {{ ref('stg_ingest_runs') }} where is_current
),

scans as (
    select
        dataset_id,
        count(*) as n_scans,
        list(distinct datatype order by datatype) as datatypes,
        list(distinct task_label order by task_label) filter (where task_label is not null) as tasks,
        sum(size_bytes) as total_bytes
    from {{ ref('fct_scan') }}
    group by dataset_id
),

subjects as (
    select
        dataset_id,
        count(*) filter (where has_data) as n_subjects,
        count(distinct subject_label) as n_subjects_any
    from {{ ref('dim_subject') }}
    group by dataset_id
)

select
    r.dataset_id,
    r.snapshot,
    r.source,
    r.snapshot_created_at,
    r.finished_at as ingested_at,
    d.dataset_name,
    d.bids_version,
    d.license,
    coalesce(d.dataset_type, 'raw') as dataset_type,
    coalesce(d.n_authors, 0) as n_authors,
    coalesce(sub.n_subjects, 0) as n_subjects,
    coalesce(s.n_scans, 0) as n_scans,
    coalesce(s.datatypes, []) as datatypes,
    coalesce(s.tasks, []) as tasks,
    coalesce(s.total_bytes, 0) as total_bytes,
    r.n_files
from runs as r
left join {{ ref('stg_dataset_description') }} as d using (dataset_id)
left join scans as s using (dataset_id)
left join subjects as sub using (dataset_id)
