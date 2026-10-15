with current_runs as (
    select dataset_id, snapshot, run_id from {{ ref('stg_ingest_runs') }} where is_current
)

select e.dataset_id, e.snapshot, e.path, e.error, coalesce(e.error_kind, 'missing') as error_kind
from {{ source('bronze', 'raw_read_errors') }} as e
inner join current_runs using (dataset_id, snapshot, run_id)
