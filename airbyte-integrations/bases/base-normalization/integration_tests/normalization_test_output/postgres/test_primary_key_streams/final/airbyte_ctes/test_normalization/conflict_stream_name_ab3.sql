
  create view "postgres"._airbyte_test_normalization."conflict_stream_name_ab3__dbt_tmp" as (
    
-- SQL model to build a hash column based on the values of this record
select
    *,
    md5(cast(
    
    coalesce(cast("id" as 
    varchar
), '') || '-' || coalesce(cast(conflict_stream_name as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_conflict_stream_name_hashid
from "postgres"._airbyte_test_normalization."conflict_stream_name_ab2"
-- conflict_stream_name
  );
