

  create  table "postgres".test_normalization."nested_stream_with_co_e78_double_array_data_1b9__dbt_tmp"
  as (
    
-- Final base SQL model
select
    _airbyte_partition_hashid,
    "id",
    _airbyte_emitted_at,
    _airbyte_double_array_data_hashid
from "postgres"._airbyte_test_normalization."nested_stream_wit_e78_double_array_data_ab3"
-- double_array_data at nested_stream_with_complex_columns_resulting_into_long_names/partition/double_array_data from "postgres".test_normalization."nested_stream_with_complex_co_64a_partition"
  );