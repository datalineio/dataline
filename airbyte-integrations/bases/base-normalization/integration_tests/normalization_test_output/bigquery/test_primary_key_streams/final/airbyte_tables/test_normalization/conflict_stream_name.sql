

  create or replace table `dataline-integration-testing`.test_normalization.`conflict_stream_name`
  
  
  OPTIONS()
  as (
    
-- Final base SQL model
select
    id,
    conflict_stream_name,
    _airbyte_emitted_at,
    _airbyte_conflict_stream_name_hashid
from `dataline-integration-testing`._airbyte_test_normalization.`conflict_stream_name_ab3`
-- conflict_stream_name from `dataline-integration-testing`.test_normalization._airbyte_raw_conflict_stream_name
  );
    