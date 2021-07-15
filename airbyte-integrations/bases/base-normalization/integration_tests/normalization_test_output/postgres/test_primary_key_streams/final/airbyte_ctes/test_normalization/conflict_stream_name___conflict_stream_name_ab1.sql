
  create view "postgres"._airbyte_test_normalization."conflict_stream_name___conflict_stream_name_ab1__dbt_tmp" as (
    
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_conflict_stream_name_2_hashid,
    jsonb_extract_path_text(conflict_stream_name, 'groups') as groups,
    _airbyte_emitted_at
from "postgres".test_normalization."conflict_stream_name_conflict_stream_name"
where conflict_stream_name is not null
-- conflict_stream_name at conflict_stream_name/conflict_stream_name/conflict_stream_name
  );
