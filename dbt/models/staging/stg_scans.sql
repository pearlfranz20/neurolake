-- *_scans.tsv rows. `filename` is relative to the folder holding the scans file.
select
    t.dataset_id,
    t.directory || (t.row_doc ->> 'filename') as path,
    try_cast({{ bids_na("t.row_doc ->> 'acq_time'") }} as timestamp) as acq_time,
    t.path as scans_file
from {{ ref('stg_tsv_rows') }} as t
where t.table_kind = 'scans'
  and (t.row_doc ->> 'filename') is not null
