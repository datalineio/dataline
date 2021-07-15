{{ config(schema="_AIRBYTE_TEST_NORMALIZATION", tags=["nested-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        '_AIRBYTE_CONFLICT_STREAM_NAME_HASHID',
        'CONFLICT_STREAM_NAME',
    ]) }} as _AIRBYTE_CONFLICT_STREAM_NAME_HASHID
from {{ ref('CONFLICT_STREAM_NAME_CONFLICT_STREAM_NAME_AB2') }}
-- CONFLICT_STREAM_NAME at conflict_stream_name/conflict_stream_name

