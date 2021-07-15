#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#


import os
from typing import Dict, List, Optional, Tuple

from airbyte_protocol.models.airbyte_protocol import DestinationSyncMode, SyncMode
from jinja2 import Template
from normalization.destination_type import DestinationType
from normalization.transform_catalog.destination_name_transformer import DestinationNameTransformer
from normalization.transform_catalog.table_name_registry import TableNameRegistry
from normalization.transform_catalog.utils import (
    is_airbyte_column,
    is_array,
    is_boolean,
    is_combining_node,
    is_integer,
    is_number,
    is_object,
    is_simple_property,
    is_string,
    jinja_call,
)

# using too many columns breaks ephemeral materialization (somewhere between 480 and 490 columns)
# let's use a lower value to be safely away from the limit...
MAXIMUM_COLUMNS_TO_USE_EPHEMERAL = 450


class StreamProcessor(object):
    """
    Takes as input an Airbyte Stream as described in the (configured) Airbyte Catalog's Json Schema.
    Associated input raw data is expected to be stored in a staging area table.

    This processor generates SQL models to transform such a stream into a final table in the destination schema.
    This is done by generating a DBT pipeline of transformations (multiple SQL models queries) that may be materialized
    in the intermediate schema "raw_schema" (changing the dbt_project.yml settings).
    The final output data should be written in "schema".

    The pipeline includes transformations such as:
    - Parsing a JSON blob column and extracting each field property in its own SQL column
    - Casting each SQL column to the proper JSON data type
    - Generating an artificial (primary key) ID column based on the hashing of the row

    If any nested columns are discovered in the stream, a JSON blob SQL column is created in the top level parent stream
    and a new StreamProcessor instance will be spawned for each children substreams. These Sub-Stream Processors are then
    able to generate models to parse and extract recursively from its parent StreamProcessor model into separate SQL tables
    the content of that JSON blob SQL column.
    """

    def __init__(
        self,
        stream_name: str,
        destination_type: DestinationType,
        raw_schema: str,
        schema: str,
        source_sync_mode: SyncMode,
        destination_sync_mode: DestinationSyncMode,
        cursor_field: List[str],
        primary_key: List[List[str]],
        json_column_name: str,
        properties: Dict,
        tables_registry: TableNameRegistry,
        from_table: str,
    ):
        """
        See StreamProcessor.create()
        """
        self.stream_name: str = stream_name
        self.destination_type: DestinationType = destination_type
        self.raw_schema: str = raw_schema
        self.schema: str = schema
        self.source_sync_mode: SyncMode = source_sync_mode
        self.destination_sync_mode: DestinationSyncMode = destination_sync_mode
        self.cursor_field: List[str] = cursor_field
        self.primary_key: List[List[str]] = primary_key
        self.json_column_name: str = json_column_name
        self.properties: Dict = properties
        self.tables_registry: TableNameRegistry = tables_registry
        self.from_table: str = from_table

        self.name_transformer: DestinationNameTransformer = DestinationNameTransformer(destination_type)
        self.json_path: List[str] = [stream_name]
        self.final_table_name: str = ""
        self.sql_outputs: Dict[str, str] = {}
        self.parent: Optional["StreamProcessor"] = None
        self.is_nested_array: bool = False

    @staticmethod
    def create_from_parent(
        parent, child_name: str, json_column_name: str, properties: Dict, is_nested_array: bool, from_table: str
    ) -> "StreamProcessor":
        """
        @param parent is the Stream Processor that originally created this instance to handle a nested column from that parent table.

        @param json_column_name is the name of the column in the parent data table containing the json column to transform
        @param properties is the json schema description of this nested stream
        @param is_nested_array is a boolean flag specifying if the child is a nested array that needs to be extracted

        @param tables_registry is the global context recording all tables created so far
        @param from_table is the parent table to extract the nested stream from

        The child stream processor will create a separate table to contain the unnested data.
        """
        result = StreamProcessor.create(
            stream_name=child_name,
            destination_type=parent.destination_type,
            raw_schema=parent.raw_schema,
            schema=parent.schema,
            # Nested Streams don't inherit parents sync modes?
            source_sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.append,
            cursor_field=[],
            primary_key=[],
            json_column_name=json_column_name,
            properties=properties,
            tables_registry=parent.tables_registry,
            from_table=from_table,
        )
        result.parent = parent
        result.is_nested_array = is_nested_array
        result.json_path = parent.json_path + [child_name]
        return result

    @staticmethod
    def create(
        stream_name: str,
        destination_type: DestinationType,
        raw_schema: str,
        schema: str,
        source_sync_mode: SyncMode,
        destination_sync_mode: DestinationSyncMode,
        cursor_field: List[str],
        primary_key: List[List[str]],
        json_column_name: str,
        properties: Dict,
        tables_registry: TableNameRegistry,
        from_table: str,
    ) -> "StreamProcessor":
        """
        @param stream_name of the stream being processed

        @param destination_type is the destination type of warehouse
        @param raw_schema is the name of the staging intermediate schema where to create internal tables/views
        @param schema is the name of the schema where to store the final tables where to store the transformed data

        @param source_sync_mode is describing how source are producing data
        @param destination_sync_mode is describing how destination should handle the new data batch
        @param cursor_field is the field to use to determine order of records
        @param primary_key is a list of fields to use as a (composite) primary key

        @param json_column_name is the name of the column in the raw data table containing the json column to transform
        @param properties is the json schema description of this stream

        @param tables_registry is the global context recording all tables created so far
        @param from_table is the table this stream is being extracted from originally
        """
        return StreamProcessor(
            stream_name,
            destination_type,
            raw_schema,
            schema,
            source_sync_mode,
            destination_sync_mode,
            cursor_field,
            primary_key,
            json_column_name,
            properties,
            tables_registry,
            from_table,
        )

    def collect_table_names(self):
        column_names = self.extract_column_names()
        self.tables_registry.register_table(self.get_schema(True), self.get_schema(False), self.stream_name, self.json_path)
        for child in self.find_children_streams(self.from_table, column_names):
            child.collect_table_names()

    def process(self) -> List["StreamProcessor"]:
        """
        See description of StreamProcessor class.
        @return List of StreamProcessor to handle recursively nested columns from this stream
        """
        # Check properties
        if not self.properties:
            print(f"  Ignoring stream '{self.stream_name}' from {self.current_json_path()} because properties list is empty")
            return []

        column_names = self.extract_column_names()
        column_count = len(column_names)

        if column_count == 0:
            print(f"  Ignoring stream '{self.stream_name}' from {self.current_json_path()} because no columns were identified")
            return []

        from_table = self.from_table
        # Transformation Pipeline for this stream
        from_table = self.add_to_outputs(self.generate_json_parsing_model(from_table, column_names), is_intermediate=True, suffix="ab1")
        from_table = self.add_to_outputs(
            self.generate_column_typing_model(from_table, column_names), is_intermediate=True, column_count=column_count, suffix="ab2"
        )
        from_table = self.add_to_outputs(
            self.generate_id_hashing_model(from_table, column_names), is_intermediate=True, column_count=column_count, suffix="ab3"
        )
        if self.destination_sync_mode.value == DestinationSyncMode.append_dedup.value:
            from_table = self.add_to_outputs(self.generate_dedup_record_model(from_table, column_names), is_intermediate=True, suffix="ab4")
            where_clause = "\nwhere _airbyte_row_num = 1"
            from_table = self.add_to_outputs(
                self.generate_scd_type_2_model(from_table, column_names) + where_clause,
                is_intermediate=False,
                column_count=column_count,
                suffix="scd",
            )
            where_clause = "\nwhere _airbyte_active_row = True"
            from_table = self.add_to_outputs(
                self.generate_final_model(from_table, column_names) + where_clause, is_intermediate=False, column_count=column_count
            )
            # TODO generate yaml file to dbt test final table where primary keys should be unique
        else:
            from_table = self.add_to_outputs(
                self.generate_final_model(from_table, column_names), is_intermediate=False, column_count=column_count
            )
        return self.find_children_streams(from_table, column_names)

    def extract_column_names(self) -> Dict[str, Tuple[str, str]]:
        """
        Generate a mapping of JSON properties to normalized SQL Column names, handling collisions and avoid duplicate names

        The mapped value to a field property is a tuple where:
         - the first value is the normalized "raw" column name
         - the second value is the normalized quoted column name to be used in jinja context
        """
        fields = []
        for field in self.properties.keys():
            if not is_airbyte_column(field):
                fields.append(field)
        result = {}
        field_names = set()
        for field in fields:
            field_name = self.name_transformer.normalize_column_name(field, in_jinja=False)
            jinja_name = self.name_transformer.normalize_column_name(field, in_jinja=True)
            if field_name in field_names:
                # TODO handle column name duplicates or collisions deterministically in this stream
                for i in range(1, 1000):
                    field_name = self.name_transformer.normalize_column_name(f"{field}_{i}", in_jinja=False)
                    jinja_name = self.name_transformer.normalize_column_name(f"{field}_{i}", in_jinja=True)
                    if field_name not in field_names:
                        break
            field_names.add(field_name)
            result[field] = (field_name, jinja_name)
        return result

    def find_children_streams(self, from_table: str, column_names: Dict[str, Tuple[str, str]]) -> List["StreamProcessor"]:
        """
        For each complex type properties, generate a new child StreamProcessor that produce separate child pipelines.
        The current stream/table is used as the parent from which to extract data from.
        """
        properties = self.properties
        children: List[StreamProcessor] = []
        for field in properties.keys():
            children_properties = None
            if is_airbyte_column(field):
                pass
            elif is_combining_node(properties[field]):
                # TODO: merge properties of all combinations
                pass
            elif "type" not in properties[field] or is_object(properties[field]["type"]):
                # properties without 'type' field are treated like properties with 'type' = 'object'
                children_properties = find_properties_object([], field, properties[field])
                is_nested_array = False
                json_column_name = column_names[field][1]
            elif is_array(properties[field]["type"]) and "items" in properties[field]:
                quoted_field = column_names[field][1]
                children_properties = find_properties_object([], field, properties[field]["items"])
                is_nested_array = True
                json_column_name = f"unnested_column_value({quoted_field})"
            if children_properties:
                for child_key in children_properties:
                    stream_processor = StreamProcessor.create_from_parent(
                        parent=self,
                        child_name=field,
                        json_column_name=json_column_name,
                        properties=children_properties[child_key],
                        is_nested_array=is_nested_array,
                        from_table=from_table,
                    )
                    children.append(stream_processor)
        return children

    def generate_json_parsing_model(self, from_table: str, column_names: Dict[str, Tuple[str, str]]) -> str:
        template = Template(
            """
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
{{ unnesting_before_query }}
select
  {%- if parent_hash_id %}
    {{ parent_hash_id }},
  {%- endif %}
  {%- for field in fields %}
    {{ field }},
  {%- endfor %}
    _airbyte_emitted_at
from {{ from_table }}
{{ unnesting_after_query }}
{{ sql_table_comment }}
"""
        )
        sql = template.render(
            unnesting_before_query=self.unnesting_before_query(),
            parent_hash_id=self.parent_hash_id(),
            fields=self.extract_json_columns(column_names),
            from_table=jinja_call(from_table),
            unnesting_after_query=self.unnesting_after_query(),
            sql_table_comment=self.sql_table_comment(),
        )
        return sql

    def extract_json_columns(self, column_names: Dict[str, Tuple[str, str]]) -> List[str]:
        return [
            StreamProcessor.extract_json_column(field, self.json_column_name, self.properties[field], column_names[field][0])
            for field in column_names
        ]

    @staticmethod
    def extract_json_column(property_name: str, json_column_name: str, definition: Dict, column_name: str) -> str:
        json_path = [property_name]
        json_extract = jinja_call(f"json_extract({json_column_name}, {json_path})")
        if "type" in definition:
            if is_array(definition["type"]):
                json_extract = jinja_call(f"json_extract_array({json_column_name}, {json_path})")
            elif is_object(definition["type"]):
                json_extract = jinja_call(f"json_extract({json_column_name}, {json_path})")
            elif is_simple_property(definition["type"]):
                json_extract = jinja_call(f"json_extract_scalar({json_column_name}, {json_path})")
        return f"{json_extract} as {column_name}"

    def generate_column_typing_model(self, from_table: str, column_names: Dict[str, Tuple[str, str]]) -> str:
        template = Template(
            """
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
  {%- if parent_hash_id %}
    {{ parent_hash_id }},
  {%- endif %}
  {%- for field in fields %}
    {{ field }},
  {%- endfor %}
    _airbyte_emitted_at
from {{ from_table }}
{{ sql_table_comment }}
    """
        )
        sql = template.render(
            parent_hash_id=self.parent_hash_id(),
            fields=self.cast_property_types(column_names),
            from_table=jinja_call(from_table),
            sql_table_comment=self.sql_table_comment(),
        )
        return sql

    def cast_property_types(self, column_names: Dict[str, Tuple[str, str]]) -> List[str]:
        return [self.cast_property_type(field, column_names[field][0], column_names[field][1]) for field in column_names]

    def cast_property_type(self, property_name: str, column_name: str, jinja_column: str) -> str:
        definition = self.properties[property_name]
        if "type" not in definition:
            print(f"WARN: Unknown type for column {property_name} at {self.current_json_path()}")
            return column_name
        elif is_array(definition["type"]):
            return column_name
        elif is_object(definition["type"]):
            sql_type = jinja_call("type_json()")
        # Treat simple types from narrower to wider scope type: boolean < integer < number < string
        elif is_boolean(definition["type"]):
            cast_operation = jinja_call(f"cast_to_boolean({jinja_column})")
            return f"{cast_operation} as {column_name}"
        elif is_integer(definition["type"]):
            sql_type = jinja_call("dbt_utils.type_bigint()")
        elif is_number(definition["type"]):
            sql_type = jinja_call("dbt_utils.type_float()")
        elif is_string(definition["type"]):
            sql_type = jinja_call("dbt_utils.type_string()")
        else:
            print(f"WARN: Unknown type {definition['type']} for column {property_name} at {self.current_json_path()}")
            return column_name
        return f"cast({column_name} as {sql_type}) as {column_name}"

    def generate_id_hashing_model(self, from_table: str, column_names: Dict[str, Tuple[str, str]]) -> str:
        template = Template(
            """
-- SQL model to build a hash column based on the values of this record
select
    *,
    {{ '{{' }} dbt_utils.surrogate_key([
      {%- if parent_hash_id %}
        '{{ parent_hash_id }}',
      {%- endif %}
      {%- for field in fields %}
        {{ field }},
      {%- endfor %}
    ]) {{ '}}' }} as {{ hash_id }}
from {{ from_table }}
{{ sql_table_comment }}
    """
        )
        sql = template.render(
            parent_hash_id=self.parent_hash_id(),
            fields=self.safe_cast_to_strings(column_names),
            hash_id=self.hash_id(),
            from_table=jinja_call(from_table),
            sql_table_comment=self.sql_table_comment(),
        )
        return sql

    def safe_cast_to_strings(self, column_names: Dict[str, Tuple[str, str]]) -> List[str]:
        return [StreamProcessor.safe_cast_to_string(self.properties[field], column_names[field][1]) for field in column_names]

    @staticmethod
    def safe_cast_to_string(definition: Dict, column_name: str) -> str:
        """
        Note that the result from this static method should always be used within a jinja context (for example, from jinja macro surrogate_key call)
        """
        if "type" not in definition:
            return column_name
        elif is_boolean(definition["type"]):
            return f"boolean_to_string({column_name})"
        elif is_array(definition["type"]):
            return f"array_to_string({column_name})"
        else:
            return column_name

    def generate_dedup_record_model(self, from_table: str, column_names: Dict[str, Tuple[str, str]]) -> str:
        template = Template(
            """
-- SQL model to prepare for deduplicating records based on the hash record column
select
  *,
  row_number() over (
    partition by {{ hash_id }}
    order by _airbyte_emitted_at asc
  ) as _airbyte_row_num
from {{ from_table }}
{{ sql_table_comment }}
        """
        )
        sql = template.render(
            hash_id=self.hash_id(),
            from_table=jinja_call(from_table),
            sql_table_comment=self.sql_table_comment(include_from_table=True),
        )
        return sql

    def generate_scd_type_2_model(self, from_table: str, column_names: Dict[str, Tuple[str, str]]) -> str:
        template = Template(
            """
-- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
select
  {%- if parent_hash_id %}
    {{ parent_hash_id }},
  {%- endif %}
  {%- for field in fields %}
    {{ field }},
  {%- endfor %}
    {{ cursor_field }} as _airbyte_start_at,
    lag({{ cursor_field }}) over (
        partition by {{ primary_key }}
        order by {{ cursor_field }} desc, _airbyte_emitted_at desc
    ) as _airbyte_end_at,
    lag({{ cursor_field }}) over (
        partition by {{ primary_key }}
        order by {{ cursor_field }} desc, _airbyte_emitted_at desc{{ cdc_updated_at_order }}
    ) is null {{ cdc_active_row }}as _airbyte_active_row,
    _airbyte_emitted_at,
    {{ hash_id }}
from {{ from_table }}
{{ sql_table_comment }}
        """
        )

        cdc_active_row_pattern = ""
        cdc_updated_order_pattern = ""
        if "_ab_cdc_deleted_at" in column_names.keys():
            cdc_active_row_pattern = "and _ab_cdc_deleted_at is null "
            cdc_updated_order_pattern = ", _ab_cdc_updated_at desc"

        sql = template.render(
            parent_hash_id=self.parent_hash_id(),
            fields=self.list_fields(column_names),
            cursor_field=self.get_cursor_field(column_names),
            primary_key=self.get_primary_key(column_names),
            hash_id=self.hash_id(),
            from_table=jinja_call(from_table),
            sql_table_comment=self.sql_table_comment(include_from_table=True),
            cdc_active_row=cdc_active_row_pattern,
            cdc_updated_at_order=cdc_updated_order_pattern,
        )
        return sql

    def get_cursor_field(self, column_names: Dict[str, Tuple[str, str]]) -> str:
        if not self.cursor_field:
            return "_airbyte_emitted_at"
        elif len(self.cursor_field) == 1:
            if not is_airbyte_column(self.cursor_field[0]):
                return column_names[self.cursor_field[0]][0]
            else:
                # using an airbyte generated column
                return self.cursor_field[0]
        else:
            raise ValueError(f"Unsupported nested cursor field {'.'.join(self.cursor_field)} for stream {self.stream_name}")

    def get_primary_key(self, column_names: Dict[str, Tuple[str, str]]) -> str:
        if self.primary_key and len(self.primary_key) > 0:
            return ", ".join([self.get_primary_key_from_path(column_names, path) for path in self.primary_key])
        else:
            raise ValueError(f"No primary key specified for stream {self.stream_name}")

    def get_primary_key_from_path(self, column_names: Dict[str, Tuple[str, str]], path: List[str]) -> str:
        if path and len(path) == 1:
            field = path[0]
            if not is_airbyte_column(field):
                if "type" in self.properties[field]:
                    property_type = self.properties[field]["type"]
                else:
                    property_type = "object"
                if is_number(property_type) or is_object(property_type):
                    # some destinations don't handle float columns (or complex types) as primary keys, turn them to string
                    return f"cast({column_names[field][0]} as {jinja_call('dbt_utils.type_string()')})"
                else:
                    return column_names[field][0]
            else:
                # using an airbyte generated column
                return f"cast({field} as {jinja_call('dbt_utils.type_string()')})"
        else:
            if path:
                raise ValueError(f"Unsupported nested path {'.'.join(path)} for stream {self.stream_name}")
            else:
                raise ValueError(f"No path specified for stream {self.stream_name}")

    def generate_final_model(self, from_table: str, column_names: Dict[str, Tuple[str, str]]) -> str:
        template = Template(
            """
-- Final base SQL model
select
  {%- if parent_hash_id %}
    {{ parent_hash_id }},
  {%- endif %}
  {%- for field in fields %}
    {{ field }},
  {%- endfor %}
    _airbyte_emitted_at,
    {{ hash_id }}
from {{ from_table }}
{{ sql_table_comment }}
    """
        )
        sql = template.render(
            parent_hash_id=self.parent_hash_id(),
            fields=self.list_fields(column_names),
            hash_id=self.hash_id(),
            from_table=jinja_call(from_table),
            sql_table_comment=self.sql_table_comment(include_from_table=True),
        )
        return sql

    @staticmethod
    def list_fields(column_names: Dict[str, Tuple[str, str]]) -> List[str]:
        return [column_names[field][0] for field in column_names]

    def add_to_outputs(self, sql: str, is_intermediate: bool, column_count: int = 0, suffix: str = "") -> str:
        schema = self.get_schema(is_intermediate)
        # MySQL table names need to be manually truncated, because it does not do it automatically
        truncate_name = self.destination_type == DestinationType.MYSQL
        table_name = self.tables_registry.get_table_name(schema, self.json_path, self.stream_name, suffix, truncate_name)
        file_name = self.tables_registry.get_file_name(schema, self.json_path, self.stream_name, suffix, truncate_name)
        file = f"{file_name}.sql"
        if is_intermediate:
            if column_count <= MAXIMUM_COLUMNS_TO_USE_EPHEMERAL:
                output = os.path.join("airbyte_ctes", self.schema, file)
            else:
                # dbt throws "maximum recursion depth exceeded" exception at runtime
                # if ephemeral is used with large number of columns, use views instead
                output = os.path.join("airbyte_views", self.schema, file)
        else:
            output = os.path.join("airbyte_tables", self.schema, file)
        tags = self.get_model_tags(is_intermediate)
        # The alias() macro configs a model's final table name.
        if file_name != table_name:
            header = jinja_call(f'config(alias="{table_name}", schema="{schema}", tags=[{tags}])')
        else:
            header = jinja_call(f'config(schema="{schema}", tags=[{tags}])')
        self.sql_outputs[
            output
        ] = f"""
{header}
{sql}
"""
        json_path = self.current_json_path()
        print(f"  Generating {output} from {json_path}")
        return ref_table(file_name)

    def get_model_tags(self, is_intermediate: bool) -> str:
        tags = ""
        if self.parent:
            tags += "nested"
        else:
            tags += "top-level"
        if is_intermediate:
            tags += "-intermediate"
        return f'"{tags}"'

    def get_schema(self, is_intermediate: bool) -> str:
        if is_intermediate:
            return self.raw_schema
        else:
            return self.schema

    def current_json_path(self) -> str:
        return "/".join(self.json_path)

    def normalized_stream_name(self) -> str:
        """
        This is the normalized name of this stream to be used as a table (different as referring it as a column).
        Note that it might not be the actual table name in case of collisions with other streams (see actual_table_name)...
        """
        return self.name_transformer.normalize_table_name(self.stream_name)

    def sql_table_comment(self, include_from_table: bool = False) -> str:
        result = f"-- {self.normalized_stream_name()}"
        if len(self.json_path) > 1:
            result += f" at {self.current_json_path()}"
        if include_from_table:
            from_table = jinja_call(self.from_table)
            result += f" from {from_table}"
        return result

    def hash_id(self) -> str:
        if self.parent:
            if self.normalized_stream_name() == self.parent.stream_name:
                level = len(self.json_path)
                return self.name_transformer.normalize_column_name(f"_airbyte_{self.normalized_stream_name()}_{level}_hashid")

        return self.name_transformer.normalize_column_name(f"_airbyte_{self.normalized_stream_name()}_hashid")


    def parent_hash_id(self) -> str:
        if self.parent:
            return self.parent.hash_id()
        return ""

    def unnesting_before_query(self) -> str:
        if self.parent and self.is_nested_array:
            parent_file_name = (
                f"'{self.tables_registry.get_file_name(self.parent.get_schema(False), self.parent.json_path, self.parent.stream_name, '')}'"
            )
            parent_stream_name = f"'{self.parent.normalized_stream_name()}'"
            quoted_field = self.name_transformer.normalize_column_name(self.stream_name, in_jinja=True)
            return jinja_call(f"unnest_cte({parent_file_name}, {parent_stream_name}, {quoted_field})")
        return ""

    def unnesting_after_query(self) -> str:
        result = ""
        if self.parent:
            cross_join = ""
            if self.is_nested_array:
                parent_stream_name = f"'{self.parent.normalized_stream_name()}'"
                quoted_field = self.name_transformer.normalize_column_name(self.stream_name, in_jinja=True)
                cross_join = jinja_call(f"cross_join_unnest({parent_stream_name}, {quoted_field})")
            column_name = self.name_transformer.normalize_column_name(self.stream_name)
            result = f"""
{cross_join}
where {column_name} is not null"""
        return result


# Static Functions


def ref_table(file_name: str) -> str:
    return f"ref('{file_name}')"


def find_properties_object(path: List[str], field: str, properties) -> Dict[str, Dict]:
    """
    This function is trying to look for a nested "properties" node under the current JSON node to
    identify all nested objects.

    @param path JSON path traversed so far to arrive to this node
    @param field is the current field being considered in the Json Tree
    @param properties is the child tree of properties of the current field being searched
    """
    result = {}
    current_path = path + [field]
    current = "_".join(current_path)
    if isinstance(properties, str) or isinstance(properties, int):
        return {}
    else:
        if "items" in properties:
            return find_properties_object(path, field, properties["items"])
        elif "properties" in properties:
            # we found a properties object
            return {current: properties["properties"]}
        elif "type" in properties and is_simple_property(properties["type"]):
            # we found a basic type
            return {current: {}}
        elif isinstance(properties, dict):
            for key in properties.keys():
                child = find_properties_object(path=current_path, field=key, properties=properties[key])
                if child:
                    result.update(child)
        elif isinstance(properties, list):
            for item in properties:
                child = find_properties_object(path=current_path, field=field, properties=item)
                if child:
                    result.update(child)
    return result
