

  create  table "postgres".test_normalization."conflict_stream_name___conflict_stream_name__dbt_tmp"
  as (
    
-- Final base SQL model
select
    _airbyte_conflict_stream_name_2_hashid,
    groups,
    _airbyte_emitted_at,
    _airbyte_conflict_stream_name_3_hashid
from "postgres"._airbyte_test_normalization."conflict_stream_name___conflict_stream_name_ab3"
-- conflict_stream_name at conflict_stream_name/conflict_stream_name/conflict_stream_name from "postgres".test_normalization."conflict_stream_name_conflict_stream_name"
  );