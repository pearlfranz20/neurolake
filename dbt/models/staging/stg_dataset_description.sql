select
    dataset_id,
    snapshot,
    is_valid_json,
    doc ->> 'Name' as dataset_name,
    doc ->> 'BIDSVersion' as bids_version_raw,
    -- tolerate 'v1.2.2' and 'Brain Imaging Data Structure v1.6.0'
    nullif(regexp_extract(doc ->> 'BIDSVersion', '([0-9]+\.[0-9]+(?:\.[0-9]+)?)', 1), '') as bids_version,
    doc ->> 'License' as license,
    coalesce(doc ->> 'DatasetType', 'raw') as dataset_type,
    coalesce(json_array_length(doc -> 'Authors'), 0) as n_authors,
    doc
from {{ ref('stg_json_docs') }}
where path = 'dataset_description.json'
