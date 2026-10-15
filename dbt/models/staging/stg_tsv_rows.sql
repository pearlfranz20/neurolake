with current_runs as (
    select dataset_id, snapshot, run_id from {{ ref('stg_ingest_runs') }} where is_current
)

select
    t.dataset_id,
    t.snapshot,
    t.path,
    t.row_number,
    t.row_json::json as row_doc,
    t.is_malformed,
    case
        when t.path = 'participants.tsv' then 'participants'
        when t.path like '%\_sessions.tsv' escape '\' then 'sessions'
        when t.path like '%\_scans.tsv' escape '\' then 'scans'
    end as table_kind,
    coalesce(regexp_extract(t.path, '^(.*/)', 1), '') as directory
from {{ source('bronze', 'raw_tsv_rows') }} as t
inner join current_runs using (dataset_id, snapshot, run_id)
