{{ config(schema="_AIRBYTE_TEST_NORMALIZATION", tags=["top-level-intermediate"]) }}
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ dbt_utils.surrogate_key([
        'ID',
        'CONFLICT_STREAM_NAME',
    ]) }} as _AIRBYTE_CONFLICT_STREAM_NAME_HASHID
from {{ ref('CONFLICT_STREAM_NAME_AB2') }}
-- CONFLICT_STREAM_NAME

