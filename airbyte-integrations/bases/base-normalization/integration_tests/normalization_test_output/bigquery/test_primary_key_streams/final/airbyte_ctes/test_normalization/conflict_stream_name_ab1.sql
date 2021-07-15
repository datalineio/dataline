

  create or replace view `dataline-integration-testing`._airbyte_test_normalization.`conflict_stream_name_ab1`
  OPTIONS()
  as 
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    json_extract_scalar(_airbyte_data, "$['id']") as id,
    json_extract(_airbyte_data, "$['conflict_stream_name']") as conflict_stream_name,
    _airbyte_emitted_at
from `dataline-integration-testing`.test_normalization._airbyte_raw_conflict_stream_name
-- conflict_stream_name;

