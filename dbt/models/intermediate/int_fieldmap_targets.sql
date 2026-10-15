-- Resolve each fieldmap's IntendedFor entries to dataset-relative paths and check
-- that the target exists. Handles BIDS URIs (bids::sub-01/func/...) and the older
-- subject-relative form (func/...), and both string and list values. URIs into other
-- datasets (bids:<name>:...) cannot be checked here and get target_exists = null.
with fmap_sidecars as (
    select dataset_id, path, subject_label, doc -> 'IntendedFor' as intended_for
    from {{ ref('stg_json_docs') }}
    where datatype = 'fmap'
      and is_valid_json
      and (doc -> 'IntendedFor') is not null
),

entries as (
    select
        dataset_id,
        path as fmap_sidecar_path,
        subject_label,
        unnest(
            case json_type(intended_for)
                when 'ARRAY' then from_json(intended_for, '["VARCHAR"]')
                else [intended_for ->> '$']
            end
        ) as target_raw
    from fmap_sidecars
    where json_type(intended_for) <> 'NULL'
),

resolved as (
    select
        *,
        case
            when target_raw like 'bids::%' then substr(target_raw, 7)
            when target_raw like 'bids:%' then null
            else 'sub-' || subject_label || '/' || target_raw
        end as target_path
    from entries
    where target_raw is not null and target_raw <> ''
)

select
    r.dataset_id,
    r.fmap_sidecar_path,
    r.subject_label,
    r.target_raw,
    r.target_path,
    case when r.target_path is not null then f.path is not null end as target_exists
from resolved as r
left join {{ ref('stg_files') }} as f
    on f.dataset_id = r.dataset_id
    and f.path = r.target_path
