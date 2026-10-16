-- One row per ingest run. The latest run per dataset defines its current snapshot;
-- every downstream model reads only current snapshots. Ties break on run_id, exactly
-- as neurolake.bronze.current_run does, so ingest skips agree with staging.
select
    dataset_id,
    snapshot,
    run_id,
    source,
    snapshot_created_at,
    started_at,
    finished_at,
    n_files,
    n_metadata_files,
    n_read_errors,
    coalesce(bidsignored_core_files, []) as bidsignored_core_files,
    row_number() over (partition by dataset_id order by finished_at desc nulls last, run_id desc) = 1 as is_current
from {{ source('bronze', 'raw_ingest_runs') }}
