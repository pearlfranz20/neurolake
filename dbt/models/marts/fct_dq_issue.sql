-- Data-quality findings, one row per issue. Each CTE is one rule; rules are
-- documented in the dq_rules seed so the report can explain them.

with
required as (
    select * from {{ ref('required_sidecar_fields') }}
),

subject_missing_from_participants as (
    select dataset_id, 'subject_missing_from_participants' as rule_id, 'warning' as severity,
        'subject' as object_type, 'sub-' || subject_label as object_id,
        n_scans || ' data files but no participants.tsv row' as detail
    from {{ ref('dim_subject') }}
    where has_data and not in_participants_tsv
      and dataset_id in (select distinct dataset_id from {{ ref('stg_participants') }})
),

participant_without_data as (
    select dataset_id, 'participant_without_data', 'warning',
        'subject', 'sub-' || subject_label,
        'listed in participants.tsv but no data files found'
    from {{ ref('dim_subject') }}
    where in_participants_tsv and not has_data
),

missing_participants_tsv as (
    select dataset_id, 'missing_participants_tsv', 'warning',
        'file', 'participants.tsv', 'dataset has subjects but no participants.tsv'
    from {{ ref('dim_dataset') }}
    where n_subjects > 0
      and dataset_id not in (select distinct dataset_id from {{ ref('stg_participants') }})
      -- an unreadable participants.tsv is reported as a read error instead
      and dataset_id not in (
          select dataset_id from {{ ref('stg_read_errors') }} where path = 'participants.tsv'
      )
),

-- grouped by acquisition too: acq-mb4 and acq-sb legitimately differ
inconsistent_repetition_time as (
    select dataset_id, 'inconsistent_repetition_time', 'warning',
        'task', 'task-' || task_label || coalesce('_acq-' || acq_label, ''),
        'RepetitionTime values: ' || array_to_string(list(distinct repetition_time_s order by repetition_time_s), ', ')
            || ' across ' || count(*) || ' runs'
    from {{ ref('fct_scan') }}
    where suffix = 'bold' and repetition_time_s is not null
    group by dataset_id, task_label, acq_label
    having count(distinct repetition_time_s) > 1
),

inconsistent_sampling_frequency as (
    select dataset_id, 'inconsistent_sampling_frequency', 'warning',
        'task', 'task-' || task_label || coalesce('_acq-' || acq_label, ''),
        'SamplingFrequency values: ' || array_to_string(list(distinct sampling_frequency_hz order by sampling_frequency_hz), ', ')
            || ' across ' || count(*) || ' recordings'
    from {{ ref('fct_scan') }}
    where datatype in ('eeg', 'meg', 'ieeg') and sampling_frequency_hz is not null
    group by dataset_id, datatype, task_label, acq_label
    having count(distinct sampling_frequency_hz) > 1
),

missing_required_sidecar_field as (
    select s.dataset_id, 'missing_required_sidecar_field', r.severity,
        'data_file', s.path, r.field || ' not set by any applicable sidecar'
    from {{ ref('fct_scan') }} as s
    inner join required as r
        on r.datatype = s.datatype and r.suffix = s.suffix
    where (s.effective_sidecar -> r.field) is null
),

fieldmap_intended_for_missing_target as (
    select dataset_id, 'fieldmap_intended_for_missing_target', 'error',
        'file', fmap_sidecar_path, 'IntendedFor target not found: ' || target_raw
    from {{ ref('int_fieldmap_targets') }}
    where target_exists = false
),

ambiguous_sidecar_inheritance as (
    select s.dataset_id, 'ambiguous_sidecar_inheritance', 'warning',
        'data_file', s.path,
        'more than one sidecar applies at the same folder level: '
            || array_to_string(s.sidecar_paths, ', ')
    from {{ ref('fct_scan') }} as s
    where s.has_ambiguous_level
),

invalid_json as (
    select dataset_id, 'invalid_json', 'error', 'file', path, 'file is not valid JSON'
    from {{ ref('stg_json_docs') }}
    where not is_valid_json
),

metadata_read_error as (
    select dataset_id, 'metadata_read_error', 'error', 'file', path, error
    from {{ ref('stg_read_errors') }}
),

malformed_tsv_row as (
    select dataset_id, 'malformed_tsv_row', 'error', 'file', path,
        'row ' || row_number || ' has a different number of columns than the header'
    from {{ ref('stg_tsv_rows') }}
    where is_malformed
),

dataset_description_missing as (
    select dataset_id, 'dataset_description_missing', 'error', 'file', 'dataset_description.json',
        'required file is missing'
    from {{ ref('dim_dataset') }}
    where dataset_id not in (select dataset_id from {{ ref('stg_dataset_description') }})
),

dataset_description_missing_field as (
    select d.dataset_id, 'dataset_description_missing_field', 'error', 'field', f.field,
        'required field missing from dataset_description.json'
    from {{ ref('stg_dataset_description') }} as d
    cross join (values ('Name'), ('BIDSVersion')) as f(field)
    where d.is_valid_json and (d.doc -> f.field) is null
),

scans_tsv_missing_file as (
    select sc.dataset_id, 'scans_tsv_missing_file', 'warning', 'file', sc.scans_file,
        'lists ' || sc.path || ' which does not exist'
    from {{ ref('stg_scans') }} as sc
    left join {{ ref('stg_files') }} as f
        on f.dataset_id = sc.dataset_id and f.path = sc.path
    where f.path is null
),

data_file_size_unknown as (
    select dataset_id, 'data_file_size_unknown', 'info', 'data_file', path,
        'size not available (annexed content not fetched?)'
    from {{ ref('fct_scan') }}
    where size_bytes is null
),

unioned as (
    select * from subject_missing_from_participants
    union all select * from participant_without_data
    union all select * from missing_participants_tsv
    union all select * from inconsistent_repetition_time
    union all select * from inconsistent_sampling_frequency
    union all select * from missing_required_sidecar_field
    union all select * from fieldmap_intended_for_missing_target
    union all select * from ambiguous_sidecar_inheritance
    union all select * from invalid_json
    union all select * from metadata_read_error
    union all select * from malformed_tsv_row
    union all select * from dataset_description_missing
    union all select * from dataset_description_missing_field
    union all select * from scans_tsv_missing_file
    union all select * from data_file_size_unknown
)

select
    md5(concat_ws('|', dataset_id, rule_id, object_type, object_id, detail)) as issue_id,
    dataset_id,
    rule_id,
    severity,
    object_type,
    object_id,
    detail
from unioned
