-- A subject row must come from at least one side of the participants/data join.
select * from {{ ref('dim_subject') }}
where not in_participants_tsv and not has_data
