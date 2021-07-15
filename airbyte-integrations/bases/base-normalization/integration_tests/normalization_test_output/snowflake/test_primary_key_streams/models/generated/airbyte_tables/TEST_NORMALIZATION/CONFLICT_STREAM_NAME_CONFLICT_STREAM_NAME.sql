{{ config(schema="TEST_NORMALIZATION", tags=["nested"]) }}
-- Final base SQL model
select
    _AIRBYTE_CONFLICT_STREAM_NAME_HASHID,
    CONFLICT_STREAM_NAME,
    _airbyte_emitted_at,
    _AIRBYTE_CONFLICT_STREAM_NAME_HASHID
from {{ ref('CONFLICT_STREAM_NAME_CONFLICT_STREAM_NAME_AB3') }}
-- CONFLICT_STREAM_NAME at conflict_stream_name/conflict_stream_name from {{ ref('CONFLICT_STREAM_NAME') }}

