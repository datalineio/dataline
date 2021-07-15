{{ config(schema="_airbyte_test_normalization", tags=["nested-intermediate"]) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_conflict_stream_name_hashid,
    cast(conflict_stream_name as {{ type_json() }}) as conflict_stream_name,
    _airbyte_emitted_at
from {{ ref('conflict_stream_name_conflict_stream_name_ab1') }}
-- conflict_stream_name at conflict_stream_name/conflict_stream_name

