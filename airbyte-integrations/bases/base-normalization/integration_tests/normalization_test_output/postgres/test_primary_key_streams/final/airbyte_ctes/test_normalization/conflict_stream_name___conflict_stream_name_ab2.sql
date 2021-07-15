
  create view "postgres"._airbyte_test_normalization."conflict_stream_name___conflict_stream_name_ab2__dbt_tmp" as (
    
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_conflict_stream_name_2_hashid,
    cast(groups as 
    varchar
) as groups,
    _airbyte_emitted_at
from "postgres"._airbyte_test_normalization."conflict_stream_name___conflict_stream_name_ab1"
-- conflict_stream_name at conflict_stream_name/conflict_stream_name/conflict_stream_name
  );
