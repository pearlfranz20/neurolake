-- participants.tsv, typed and normalised. Column names vary across datasets, so
-- only the common BIDS-recommended columns are pulled out; the full row is kept.
with rows as (
    select * from {{ ref('stg_tsv_rows') }} where table_kind = 'participants'
),

extracted as (
    select
        dataset_id,
        row_number,
        is_malformed,
        row_doc,
        {{ bids_na("row_doc ->> 'participant_id'") }} as participant_id_raw,
        {{ bids_na("row_doc ->> 'age'") }} as age_raw,
        lower({{ bids_na("row_doc ->> 'sex'") }}) as sex_raw,
        lower({{ bids_na("row_doc ->> 'handedness'") }}) as handedness_raw,
        {{ bids_na("row_doc ->> 'group'") }} as participant_group
    from rows
)

select
    dataset_id,
    regexp_replace(participant_id_raw, '^sub-', '') as subject_label,
    participant_id_raw as participant_id,
    try_cast(age_raw as double) as age_years,
    age_raw,
    case
        when sex_raw in ('m', 'male') then 'M'
        when sex_raw in ('f', 'female') then 'F'
        when sex_raw in ('o', 'other') then 'O'
    end as sex,
    case
        when handedness_raw in ('r', 'right') then 'R'
        when handedness_raw in ('l', 'left') then 'L'
        when handedness_raw in ('a', 'ambidextrous') then 'A'
    end as handedness,
    participant_group,
    is_malformed,
    row_number as source_row_number,
    row_doc as attributes
from extracted
where participant_id_raw is not null
