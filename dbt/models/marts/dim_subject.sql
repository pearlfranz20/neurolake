-- Every subject seen in participants.tsv or in the data, so mismatches stay visible.
with participants as (
    select *
    from {{ ref('stg_participants') }}
    qualify row_number() over (partition by dataset_id, subject_label order by source_row_number) = 1
),

with_data as (
    select
        dataset_id,
        subject_label,
        count(*) as n_scans,
        count(distinct session_label) as n_sessions,
        list(distinct datatype order by datatype) as datatypes,
        sum(size_bytes) as total_bytes
    from {{ ref('fct_scan') }}
    group by all
)

select
    coalesce(p.dataset_id, d.dataset_id) || ':sub-' || coalesce(p.subject_label, d.subject_label) as subject_id,
    coalesce(p.dataset_id, d.dataset_id) as dataset_id,
    coalesce(p.subject_label, d.subject_label) as subject_label,
    p.age_years,
    p.sex,
    p.handedness,
    p.participant_group,
    p.subject_label is not null as in_participants_tsv,
    d.subject_label is not null as has_data,
    coalesce(d.n_scans, 0) as n_scans,
    coalesce(d.n_sessions, 0) as n_sessions,
    coalesce(d.datatypes, []) as datatypes,
    coalesce(d.total_bytes, 0) as total_bytes,
    p.attributes
from participants as p
full outer join with_data as d
    on p.dataset_id = d.dataset_id
    and p.subject_label = d.subject_label
