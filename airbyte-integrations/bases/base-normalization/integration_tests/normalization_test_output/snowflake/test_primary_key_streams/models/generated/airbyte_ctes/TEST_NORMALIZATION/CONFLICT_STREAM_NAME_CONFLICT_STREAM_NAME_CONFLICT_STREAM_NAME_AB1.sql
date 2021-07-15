{{ config(schema="_AIRBYTE_TEST_NORMALIZATION", tags=["nested-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _AIRBYTE_CONFLICT_STREAM_NAME_HASHID,
    {{ json_extract_scalar('CONFLICT_STREAM_NAME', ['groups']) }} as GROUPS,
    _airbyte_emitted_at
from {{ ref('CONFLICT_STREAM_NAME_CONFLICT_STREAM_NAME') }}
where CONFLICT_STREAM_NAME is not null
-- CONFLICT_STREAM_NAME at conflict_stream_name/conflict_stream_name/conflict_stream_name

