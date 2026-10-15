select
    dataset_id,
    snapshot,
    is_valid_json,
    doc ->> 'Name' as dataset_name,
    doc ->> 'BIDSVersion' as bids_version,
    doc ->> 'License' as license,
    coalesce(doc ->> 'DatasetType', 'raw') as dataset_type,
    coalesce(json_array_length(doc -> 'Authors'), 0) as n_authors,
    doc
from {{ ref('stg_json_docs') }}
where path = 'dataset_description.json'
