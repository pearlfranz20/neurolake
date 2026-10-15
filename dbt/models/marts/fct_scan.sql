-- One row per primary data file, with its resolved (inherited) acquisition metadata.
with scans_tsv as (
    select dataset_id, path, acq_time
    from {{ ref('stg_scans') }}
    qualify row_number() over (partition by dataset_id, path order by scans_file) = 1
)

select
    f.data_file_id,
    f.dataset_id,
    f.snapshot,
    f.path,
    f.dataset_id || ':sub-' || f.subject_label as subject_id,
    f.subject_label,
    f.session_label,
    f.datatype,
    f.suffix,
    f.extension,
    f.entities,
    f.task_label,
    f.acq_label,
    f.run_label,
    f.echo_label,
    f.size_bytes,
    s.core_fields[1] as task_name,
    try_cast(s.core_fields[2] as double) as repetition_time_s,
    try_cast(s.core_fields[3] as double) as echo_time_s,
    try_cast(s.core_fields[4] as double) as sampling_frequency_hz,
    s.core_fields[5] as manufacturer,
    try_cast(s.core_fields[6] as double) as field_strength_t,
    t.acq_time,
    s.n_sidecars,
    s.sidecar_paths,
    s.has_invalid_sidecar,
    s.has_ambiguous_level,
    s.has_unfetched_sidecar,
    -- the merged sidecar itself stays in int_effective_sidecars: some datasets carry
    -- sidecars of hundreds of KB, and only key presence is needed downstream
    s.sidecar_keys
from {{ ref('int_data_files') }} as f
inner join {{ ref('int_effective_sidecars') }} as s using (data_file_id)
left join scans_tsv as t
    on t.dataset_id = f.dataset_id
    and t.path = f.path
