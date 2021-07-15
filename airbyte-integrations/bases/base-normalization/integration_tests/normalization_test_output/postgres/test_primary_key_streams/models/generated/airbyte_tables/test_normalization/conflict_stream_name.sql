{{ config(schema="test_normalization", tags=["top-level"]) }}
-- Final base SQL model
select
    {{ adapter.quote('id') }},
    conflict_stream_name,
    _airbyte_emitted_at,
    _airbyte_conflict_stream_name_hashid
from {{ ref('conflict_stream_name_ab3') }}
-- conflict_stream_name from {{ source('test_normalization', '_airbyte_raw_conflict_stream_name') }}

