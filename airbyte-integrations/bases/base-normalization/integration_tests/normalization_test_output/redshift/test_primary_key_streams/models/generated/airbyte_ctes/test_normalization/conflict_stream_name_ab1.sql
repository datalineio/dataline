{{ config(schema="_airbyte_test_normalization", tags=["top-level-intermediate"]) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    {{ json_extract_scalar('_airbyte_data', ['id']) }} as id,
    {{ json_extract('_airbyte_data', ['conflict_stream_name']) }} as conflict_stream_name,
    _airbyte_emitted_at
from {{ source('test_normalization', '_airbyte_raw_conflict_stream_name') }}
-- conflict_stream_name

