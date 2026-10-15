{#
  Parse a BIDS path (relative to the dataset root) into its parts.
  BIDS filenames are `key-value` entity pairs joined by `_`, then a suffix, then an
  extension: sub-01_ses-pre_task-rest_run-1_bold.nii.gz
#}

{# Entities exposed as named columns, besides sub and ses. Matching and inheritance
   use the full `entities` list instead, so no entity is ever ignored. #}
{% macro bids_entity_keys() %}
    {{ return(['task', 'acq', 'ce', 'rec', 'dir', 'run', 'echo', 'part']) }}
{% endmacro %}

{% macro bids_filename(path) -%}
    regexp_extract({{ path }}, '([^/]+)$', 1)
{%- endmacro %}

{% macro bids_entity(path, key) -%}
    nullif(regexp_extract({{ bids_filename(path) }}, '(?:^|_){{ key }}-([a-zA-Z0-9]+)', 1), '')
{%- endmacro %}

{% macro bids_path_columns(path) -%}
    {{ bids_filename(path) }} as filename,
    coalesce(regexp_extract({{ path }}, '^(.*/)', 1), '') as directory,
    length({{ path }}) - length(replace({{ path }}, '/', '')) as depth,
    -- the datatype folder is the one directly under sub-*/ or sub-*/ses-*/
    nullif(regexp_extract({{ path }}, '^sub-[a-zA-Z0-9]+/(?:ses-[a-zA-Z0-9]+/)?([a-z]+)/[^/]+$', 1), '') as datatype,
    -- subject and session come from the folder when present, else the filename
    coalesce(
        nullif(regexp_extract({{ path }}, '^sub-([a-zA-Z0-9]+)/', 1), ''),
        {{ bids_entity(path, 'sub') }}
    ) as subject_label,
    coalesce(
        nullif(regexp_extract({{ path }}, '^sub-[a-zA-Z0-9]+/ses-([a-zA-Z0-9]+)/', 1), ''),
        {{ bids_entity(path, 'ses') }}
    ) as session_label,
    {%- for key in bids_entity_keys() %}
    {{ bids_entity(path, key) }} as {{ key }}_label,
    {%- endfor %}
    -- every key-value pair in the filename, e.g. ['sub-01', 'task-rest', 'run-2']
    regexp_extract_all(regexp_extract({{ bids_filename(path) }}, '^([^.]+)', 1), '(?:^|_)([a-zA-Z0-9]+-[a-zA-Z0-9]+)', 1) as entities,
    nullif(regexp_extract(regexp_extract({{ bids_filename(path) }}, '^([^.]+)', 1), '(?:^|_)([a-zA-Z0-9]+)$', 1), '') as suffix,
    coalesce(nullif(regexp_extract({{ bids_filename(path) }}, '(\.[^/]*)$', 1), ''), '') as extension
{%- endmacro %}

{#
  True when a sidecar applies to a data file under the BIDS inheritance principle:
  every key-value entity in the sidecar's name appears in the file's name, and any
  subject or session the sidecar's folder pins down matches the file's.
#}
{% macro bids_entities_compatible(sidecar, datafile) -%}
    list_has_all({{ datafile }}.entities, {{ sidecar }}.entities)
    and ({{ sidecar }}.subject_label is null or {{ sidecar }}.subject_label = {{ datafile }}.subject_label)
    and ({{ sidecar }}.session_label is null or {{ sidecar }}.session_label = {{ datafile }}.session_label)
{%- endmacro %}

{# Number of entities a sidecar pins down; more entities means more specific. #}
{% macro bids_entity_count(alias) -%}
    len({{ alias }}.entities)
{%- endmacro %}

{# BIDS writes missing values as the literal string n/a. #}
{% macro bids_na(expr) -%}
    nullif(nullif(trim({{ expr }}), 'n/a'), '')
{%- endmacro %}
