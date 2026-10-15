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
    -- 0.5% tolerance: rounding and measured-clock jitter are not real differences
    having max(repetition_time_s) > 1.005 * min(repetition_time_s)
),

inconsistent_sampling_frequency as (
    select dataset_id, 'inconsistent_sampling_frequency', 'warning',
        'task', 'task-' || task_label || coalesce('_acq-' || acq_label, ''),
        'SamplingFrequency values: ' || array_to_string(list(distinct sampling_frequency_hz order by sampling_frequency_hz), ', ')
            || ' across ' || count(*) || ' recordings'
    from {{ ref('fct_scan') }}
    where datatype in ('eeg', 'meg', 'ieeg') and sampling_frequency_hz is not null
    group by dataset_id, datatype, task_label, acq_label
    having max(sampling_frequency_hz) > 1.005 * min(sampling_frequency_hz)
),

-- BOLD TRs are seconds; values this large are almost always milliseconds
implausible_repetition_time as (
    select dataset_id, 'implausible_repetition_time', 'error',
        'data_file', path,
        'RepetitionTime ' || repetition_time_s || ' s; BIDS expects seconds (milliseconds by mistake?)'
    from {{ ref('fct_scan') }}
    where suffix = 'bold' and (repetition_time_s >= 20 or repetition_time_s <= 0)
),

-- Field levels follow the BIDS schema (seeds/required_sidecar_fields.csv cites the
-- schema rule for each). `field` may list alternatives (RepetitionTime|VolumeTiming).
-- Files with an unfetched (annexed) sidecar are skipped: their metadata is unknown.
missing_sidecar_field as (
    select s.dataset_id, 'missing_' || r.requirement || '_sidecar_field',
        case r.requirement when 'required' then 'error' else 'warning' end,
        'data_file', s.path,
        replace(r.field, '|', ' or ') || ' (' || upper(r.requirement) || ') not set by any applicable sidecar'
    from {{ ref('fct_scan') }} as s
    inner join required as r
        on r.datatype = s.datatype and r.suffix = s.suffix
    where not s.has_unfetched_sidecar
      -- some fields only apply when an entity is present (MEG TaskName needs task-)
      and (r.requires_entity is null
           or len(list_filter(s.entities, e -> starts_with(e, r.requires_entity || '-'))) > 0)
      and len(list_filter(string_split(r.field, '|'), f -> list_contains(s.sidecar_keys, f))) = 0
),

fieldmap_intended_for_missing_target as (
    select dataset_id, 'fieldmap_intended_for_missing_target', 'error',
        'file', fmap_sidecar_path, 'IntendedFor target not found: ' || target_raw
    from {{ ref('int_fieldmap_targets') }}
    where target_exists = false
),

-- The one-sidecar-per-level rule arrived in BIDS 1.7.0, so datasets declaring an
-- older version get an info note rather than a warning.
ambiguous_sidecar_inheritance as (
    select s.dataset_id, 'ambiguous_sidecar_inheritance',
        case when {{ bids_version_before('d.bids_version', 1, 7) }} then 'info' else 'warning' end,
        'data_file', s.path,
        'more than one sidecar applies at the same folder level: '
            || array_to_string(s.sidecar_paths, ', ')
            || case when {{ bids_version_before('d.bids_version', 1, 7) }}
                    then ' (allowed before BIDS 1.7.0; dataset declares ' || d.bids_version || ')' else '' end
    from {{ ref('fct_scan') }} as s
    inner join {{ ref('dim_dataset') }} as d using (dataset_id)
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
    where error_kind <> 'not_fetched'
),

-- annexed metadata that was not fetched is a limit of the ingest, not a dataset defect
metadata_not_fetched as (
    select dataset_id, 'metadata_not_fetched', 'info', 'file', path, error
    from {{ ref('stg_read_errors') }}
    where error_kind = 'not_fetched'
),

malformed_tsv_row as (
    select dataset_id, 'malformed_tsv_row', 'error', 'file', path,
        'row ' || row_number || ' has a different number of columns than the header'
    from {{ ref('stg_tsv_rows') }}
    where is_malformed
),

-- Subject folders must sit at the dataset root. When a whole dataset is uploaded one
-- level too deep, nothing is recognised and every other rule goes quiet.
subject_folders_not_at_root as (
    select dataset_id, 'subject_folders_not_at_root', 'error', 'folder',
        regexp_extract(path, '^([^/]+/)', 1),
        count(*) || ' files under ' || regexp_extract(path, '^([^/]+/)', 1) || 'sub-*/ instead of the root'
    from {{ ref('stg_files') }}
    where regexp_matches(path, '^[^/]+/sub-[a-zA-Z0-9]+/')
      and not regexp_matches(path, '^sub-')
      and not is_bidsignored
    group by dataset_id, regexp_extract(path, '^([^/]+/)', 1)
),

-- missing means absent from the listing; a file that exists but cannot be read is
-- reported by the read-error rules instead
dataset_description_missing as (
    select dataset_id, 'dataset_description_missing', 'error', 'file', 'dataset_description.json',
        'required file is missing'
    from {{ ref('dim_dataset') }}
    where dataset_id not in (
        select dataset_id from {{ ref('stg_files') }} where path = 'dataset_description.json'
    )
),

bidsignore_matches_core_files as (
    select r.dataset_id, 'bidsignore_matches_core_files', 'warning', 'file', '.bidsignore',
        'patterns match core files, which were kept: ' || array_to_string(r.bidsignored_core_files, ', ')
    from {{ ref('stg_ingest_runs') }} as r
    where r.is_current and len(r.bidsignored_core_files) > 0
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
    where not exists (
        -- a target may be a file or a folder-format recording (CTF .ds, MEF3 .mefd)
        select 1 from {{ ref('stg_files') }} as f
        where f.dataset_id = sc.dataset_id
          and (f.path = sc.path or starts_with(f.path, sc.path || '/'))
    )
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
    union all select * from implausible_repetition_time
    union all select * from inconsistent_sampling_frequency
    union all select * from missing_sidecar_field
    union all select * from fieldmap_intended_for_missing_target
    union all select * from ambiguous_sidecar_inheritance
    union all select * from invalid_json
    union all select * from metadata_read_error
    union all select * from metadata_not_fetched
    union all select * from malformed_tsv_row
    union all select * from subject_folders_not_at_root
    union all select * from dataset_description_missing
    union all select * from bidsignore_matches_core_files
    union all select * from dataset_description_missing_field
    union all select * from scans_tsv_missing_file
    union all select * from data_file_size_unknown
),

-- Derivative datasets (pipeline outputs) follow different rules. Only checks on file
-- integrity apply to them; raw-data rules would only produce noise.
derivative_dataset as (
    select dataset_id, 'derivative_dataset', 'info', 'dataset', dataset_id,
        'DatasetType is derivative; raw-data rules were not applied'
    from {{ ref('dim_dataset') }}
    where dataset_type = 'derivative'
),

scoped as (
    select u.*
    from unioned as u
    inner join {{ ref('dim_dataset') }} as d using (dataset_id)
    where d.dataset_type <> 'derivative'
       or u.rule_id in (
           'invalid_json', 'malformed_tsv_row', 'metadata_read_error', 'metadata_not_fetched',
           'dataset_description_missing', 'dataset_description_missing_field'
       )
    union all
    select * from derivative_dataset
)

select
    md5(concat_ws('|', dataset_id, rule_id, object_type, object_id, detail)) as issue_id,
    dataset_id,
    rule_id,
    severity,
    object_type,
    object_id,
    detail
from scoped
