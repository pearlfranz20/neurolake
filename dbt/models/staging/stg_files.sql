with current_runs as (
    select dataset_id, snapshot, run_id from {{ ref('stg_ingest_runs') }} where is_current
)

select
    f.dataset_id,
    f.snapshot,
    f.path,
    f.size_bytes,
    coalesce(f.is_bidsignored, false) as is_bidsignored,
    {{ bids_path_columns('f.path') }}
from {{ source('bronze', 'raw_files') }} as f
inner join current_runs using (dataset_id, snapshot, run_id)
