
  create or replace  view "AIRBYTE_DATABASE"._AIRBYTE_TEST_NORMALIZATION."CONFLICT_STREAM_NAME_CONFLICT_STREAM_NAME_AB1"  as (
    
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _AIRBYTE_CONFLICT_STREAM_NAME_HASHID,
    get_path(parse_json(CONFLICT_STREAM_NAME), '"conflict_stream_name"') as CONFLICT_STREAM_NAME,
    _airbyte_emitted_at
from "AIRBYTE_DATABASE".TEST_NORMALIZATION."CONFLICT_STREAM_NAME"
where CONFLICT_STREAM_NAME is not null
-- CONFLICT_STREAM_NAME at conflict_stream_name/conflict_stream_name
  );
