with current_runs as (
    select dataset_id, snapshot, run_id from {{ ref('stg_ingest_runs') }} where is_current
)

select
    d.dataset_id,
    d.snapshot,
    d.path,
    json_valid(d.content) as is_valid_json,
    case when json_valid(d.content) then d.content::json end as doc,
    {{ bids_path_columns('d.path') }}
from {{ source('bronze', 'raw_json_docs') }} as d
inner join current_runs using (dataset_id, snapshot, run_id)
