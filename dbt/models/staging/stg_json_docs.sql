-- A table, not a view: `doc` comes from a Python function registered by the
-- neurolake dbt plugin, which only exists on dbt's connections.
{{ config(materialized='table') }}

with current_runs as (
    select dataset_id, snapshot, run_id from {{ ref('stg_ingest_runs') }} where is_current
)

select
    d.dataset_id,
    d.snapshot,
    d.path,
    json_valid(d.content) as is_valid_json,
    -- A key may repeat. DuckDB would read the first value, while Python, JavaScript
    -- and the BIDS validator read the last. Files that repeat a key are resolved the
    -- way most readers do, every other file is read by DuckDB as is, and the
    -- repetition is reported as duplicate_json_key.
    case
        when not json_valid(d.content) then null
        when len(k.duplicate_keys) > 0 then bids_json_last_wins(d.content)::json
        else d.content::json
    end as doc,
    k.duplicate_keys,
    {{ bids_path_columns('d.path') }}
from {{ source('bronze', 'raw_json_docs') }} as d
inner join current_runs using (dataset_id, snapshot, run_id)
cross join lateral (
    select coalesce(
        case when json_valid(d.content) then bids_json_duplicate_keys(d.content) end, []
    ) as duplicate_keys
) as k
