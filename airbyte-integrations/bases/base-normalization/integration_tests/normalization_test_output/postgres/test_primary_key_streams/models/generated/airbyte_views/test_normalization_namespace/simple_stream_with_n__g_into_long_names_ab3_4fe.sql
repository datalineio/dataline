{{ config(alias="simple_stream_with_n__g_into_long_names_ab3", schema="_airbyte_test_normalization_namespace", tags=["top-level-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        adapter.quote('date'),
    ]) }} as _airbyte_simple_stre__nto_long_names_hashid
from {{ ref('simple_stream_with_n__g_into_long_names_ab2_4fe') }}
-- simple_stream_with_n__lting_into_long_names

