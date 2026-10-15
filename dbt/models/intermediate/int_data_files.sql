-- Primary data files: imaging volumes and electrophysiology recordings inside a
-- subject's datatype folder. Companion files (.json, .tsv, .bval, .vmrk...) are excluded.
-- Some formats are folders (CTF MEG `.ds`, MEF3 iEEG `.mefd`); each such folder is
-- collapsed into one recording whose size is the sum of its contents.
with folder_files as (
    select
        dataset_id,
        snapshot,
        regexp_extract(path, '^(sub-[a-zA-Z0-9]+/(?:ses-[a-zA-Z0-9]+/)?[a-z]+/[^/]+\.(?:ds|mefd))/', 1) as path,
        size_bytes
    from {{ ref('stg_files') }}
),

folder_recordings as (
    select dataset_id, snapshot, path, sum(size_bytes)::bigint as size_bytes
    from folder_files
    where path <> ''
    group by all
),

candidates as (
    select * from {{ ref('stg_files') }}
    union all by name
    select dataset_id, snapshot, path, size_bytes, {{ bids_path_columns('path') }}
    from folder_recordings
)

select
    c.*,
    c.dataset_id || ':' || c.path as data_file_id
from candidates as c
inner join {{ ref('data_file_extensions') }} as e
    on c.extension = e.extension
where c.subject_label is not null
  and c.datatype is not null
  -- MEGIN/Elekta fine-calibration and crosstalk files are scanner calibration, not
  -- recordings; BIDS gives them no sidecar.
  and not (c.datatype = 'meg' and coalesce(c.acq_label, '') in ('crosstalk', 'calibration'))
