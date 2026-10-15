-- Marts must never mix snapshots of the same dataset.
select dataset_id
from {{ ref('fct_scan') }}
group by dataset_id
having count(distinct snapshot) > 1
