{{ config(schema="_airbyte_test_normalization", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_conflict_stream_name_hashid,
    {{ json_extract('conflict_stream_name', ['conflict_stream_name']) }} as conflict_stream_name,
    _airbyte_emitted_at
from {{ ref('conflict_stream_name') }}
where conflict_stream_name is not null
-- conflict_stream_name at conflict_stream_name/conflict_stream_name

