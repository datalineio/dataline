
  create or replace  view "AIRBYTE_DATABASE"._AIRBYTE_TEST_NORMALIZATION."CONFLICT_STREAM_NAME_AB1"  as (
    
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    to_varchar(get_path(parse_json(_airbyte_data), '"id"')) as ID,
    get_path(parse_json(_airbyte_data), '"conflict_stream_name"') as CONFLICT_STREAM_NAME,
    _airbyte_emitted_at
from "AIRBYTE_DATABASE".TEST_NORMALIZATION._AIRBYTE_RAW_CONFLICT_STREAM_NAME
-- CONFLICT_STREAM_NAME
  );
