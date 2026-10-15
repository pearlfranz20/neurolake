-- Acquisition parameters per dataset, datatype and task: the table you check
-- before pooling runs in an analysis.
select
    dataset_id,
    datatype,
    suffix,
    coalesce(task_label, '(none)') as task_label,
    count(*) as n_files,
    count(distinct subject_label) as n_subjects,
    list(distinct repetition_time_s order by repetition_time_s) filter (where repetition_time_s is not null) as repetition_times_s,
    list(distinct sampling_frequency_hz order by sampling_frequency_hz) filter (where sampling_frequency_hz is not null) as sampling_frequencies_hz,
    list(distinct manufacturer order by manufacturer) filter (where manufacturer is not null) as manufacturers,
    count(*) filter (where n_sidecars = 0) as n_files_without_sidecar,
    sum(size_bytes) as total_bytes
from {{ ref('fct_scan') }}
group by all
