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
    f.task_label,
    f.acq_label,
    f.run_label,
    f.echo_label,
    f.size_bytes,
    s.effective_sidecar ->> 'TaskName' as task_name,
    try_cast(s.effective_sidecar ->> 'RepetitionTime' as double) as repetition_time_s,
    try_cast(s.effective_sidecar ->> 'EchoTime' as double) as echo_time_s,
    try_cast(s.effective_sidecar ->> 'SamplingFrequency' as double) as sampling_frequency_hz,
    s.effective_sidecar ->> 'Manufacturer' as manufacturer,
    try_cast(s.effective_sidecar ->> 'MagneticFieldStrength' as double) as field_strength_t,
    t.acq_time,
    s.n_sidecars,
    s.sidecar_paths,
    s.has_invalid_sidecar,
    s.has_ambiguous_level,
    s.effective_sidecar
from {{ ref('int_data_files') }} as f
inner join {{ ref('int_effective_sidecars') }} as s using (data_file_id)
left join scans_tsv as t
    on t.dataset_id = f.dataset_id
    and t.path = f.path
