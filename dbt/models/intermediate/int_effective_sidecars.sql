-- BIDS inheritance principle: a data file's metadata is the merge of every JSON
-- sidecar with the same suffix that sits in the file's folder or an ancestor and
-- whose entities are a subset of the file's. Closer, more specific sidecars win;
-- path order breaks remaining ties so the result is deterministic.
with data_files as (
    select * from {{ ref('int_data_files') }}
),

sidecars as (
    select
        s.*,
        {{ bids_entity_count('s') }} as n_entities
    from {{ ref('stg_json_docs') }} as s
    where s.extension = '.json'
      and s.path <> 'dataset_description.json'
),

candidates as (
    select
        f.data_file_id,
        s.path as sidecar_path,
        s.is_valid_json,
        s.doc,
        s.directory as sidecar_directory,
        s.depth as sidecar_depth,
        s.n_entities
    from data_files as f
    inner join sidecars as s
        on s.dataset_id = f.dataset_id
        and s.suffix = f.suffix
        and starts_with(f.directory, s.directory)
        and {{ bids_entities_compatible('s', 'f') }}
),

-- Sidecars that exist but were not fetched (git-annex). If one applies, the file's
-- metadata is incomplete and required-field checks must not run on it.
unfetched as (
    select f.*
    from {{ ref('stg_files') }} as f
    inner join {{ ref('stg_read_errors') }} as e using (dataset_id, path)
    where e.error_kind = 'not_fetched' and f.extension = '.json'
),

unfetched_hits as (
    select distinct d.data_file_id
    from data_files as d
    inner join unfetched as s
        on s.dataset_id = d.dataset_id
        and s.suffix = d.suffix
        and starts_with(d.directory, s.directory)
        and {{ bids_entities_compatible('s', 'd') }}
),

merged as (
    select
        data_file_id,
        -- start from {} so files with only invalid sidecars still reduce cleanly
        list_reduce(
            list_prepend(
                '{}'::json,
                list(doc order by sidecar_depth, n_entities, sidecar_path) filter (where is_valid_json)
            ),
            lambda acc, nxt: json_merge_patch(acc, nxt)
        ) as effective_sidecar,
        list(sidecar_path order by sidecar_depth, n_entities, sidecar_path) as sidecar_paths,
        count(*) as n_sidecars,
        -- BIDS allows at most one applicable sidecar per folder level
        count(*) > count(distinct sidecar_directory) as has_ambiguous_level,
        bool_or(not is_valid_json) as has_invalid_sidecar
    from candidates
    group by data_file_id
),

final as (
    select
        f.data_file_id,
        f.dataset_id,
        f.path,
        coalesce(m.effective_sidecar, '{}'::json) as effective_sidecar,
        coalesce(m.sidecar_paths, []) as sidecar_paths,
        coalesce(m.n_sidecars, 0) as n_sidecars,
        coalesce(m.has_invalid_sidecar, false) as has_invalid_sidecar,
        coalesce(m.has_ambiguous_level, false) as has_ambiguous_level,
        u.data_file_id is not null as has_unfetched_sidecar
    from data_files as f
    left join merged as m using (data_file_id)
    left join unfetched_hits as u using (data_file_id)
)

-- Parse each document once: every ->> re-parses the whole JSON, and some datasets
-- carry sidecars of hundreds of KB (long timing arrays), so separate extractions
-- multiply memory by the number of fields.
select
    *,
    json_keys(effective_sidecar) as sidecar_keys,
    json_extract_string(
        effective_sidecar,
        ['$.TaskName', '$.RepetitionTime', '$.EchoTime', '$.SamplingFrequency',
         '$.Manufacturer', '$.MagneticFieldStrength']
    ) as core_fields
from final
