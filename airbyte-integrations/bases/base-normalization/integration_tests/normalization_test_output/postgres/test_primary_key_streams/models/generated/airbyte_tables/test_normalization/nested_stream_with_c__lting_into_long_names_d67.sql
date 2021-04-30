{{ config(alias="nested_stream_with_c__lting_into_long_names", schema="test_normalization", tags=["top-level"]) }}
-- Final base SQL model
select
    {{ adapter.quote('id') }},
    {{ adapter.quote('date') }},
    {{ adapter.quote('partition') }},
    _airbyte_emitted_at,
    _airbyte_nested_stre__nto_long_names_hashid
from {{ ref('nested_stream_with_c__lting_into_long_names_scd_d67') }}
-- nested_stream_with_c__lting_into_long_names from {{ source('test_normalization', '_airbyte_raw_nested_stream_with_complex_columns_resulting_into_long_names') }}
where _airbyte_active_row = True

