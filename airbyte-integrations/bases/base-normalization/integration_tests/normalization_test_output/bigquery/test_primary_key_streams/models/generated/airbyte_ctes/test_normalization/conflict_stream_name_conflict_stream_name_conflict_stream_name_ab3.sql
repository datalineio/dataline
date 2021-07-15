{{ config(schema="_airbyte_test_normalization", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_airbyte_conflict_stream_name_2_hashid',
        adapter.quote('groups'),
    ]) }} as _airbyte_conflict_stream_name_3_hashid
from {{ ref('conflict_stream_name_conflict_stream_name_conflict_stream_name_ab2') }}
-- conflict_stream_name at conflict_stream_name/conflict_stream_name/conflict_stream_name

