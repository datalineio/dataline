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


import unicodedata as ud
from re import match, sub

from normalization.destination_type import DestinationType
from normalization.transform_catalog.reserved_keywords import is_reserved_keyword
from normalization.transform_catalog.utils import jinja_call

DESTINATION_SIZE_LIMITS = {
    DestinationType.BIGQUERY.value: 1024,
    DestinationType.SNOWFLAKE.value: 255,
    DestinationType.REDSHIFT.value: 127,
    DestinationType.POSTGRES.value: 63,
    DestinationType.ORACLE.value: 30,
}

# DBT also needs to generate suffix to table names, so we need to make sure it has enough characters to do so...
TRUNCATE_DBT_RESERVED_SIZE = 12
# we keep 4 characters for 1 underscore and 3 characters for suffix (_ab1, _ab2, etc)
# we keep 4 characters for 1 underscore and 3 characters hash (of the schema)
TRUNCATE_RESERVED_SIZE = 8


class DestinationNameTransformer:
    """
    Handles naming conventions in destinations for all kind of sql identifiers:
    - schema
    - table
    - column
    """

    def __init__(self, destination_type: DestinationType):
        """
        @param destination_type is the destination type of warehouse
        """
        self.destination_type: DestinationType = destination_type

    # Public methods

    def needs_quotes(self, input_name: str) -> bool:
        """
        @param input_name to test if it needs to manipulated with quotes or not
        """
        if is_reserved_keyword(input_name, self.destination_type):
            return True
        if self.destination_type.value == DestinationType.BIGQUERY.value:
            return False
        doesnt_start_with_alphaunderscore = match("[^A-Za-z_]", input_name[0]) is not None
        contains_non_alphanumeric = match(".*[^A-Za-z0-9_].*", input_name) is not None
        return doesnt_start_with_alphaunderscore or contains_non_alphanumeric

    def normalize_schema_name(self, schema_name: str, in_jinja: bool = False, truncate: bool = True) -> str:
        """
        @param schema_name is the schema to normalize
        @param in_jinja is a boolean to specify if the returned normalized will be used inside a jinja macro or not
        @param truncate force ignoring truncate operation on resulting normalized name. For example, if we don't
        control how the name would be normalized
        """
        return self.__normalize_non_column_identifier_name(input_name=schema_name, in_jinja=in_jinja, truncate=truncate)

    def normalize_table_name(self, table_name: str, in_jinja: bool = False, truncate: bool = True) -> str:
        """
        @param table_name is the table to normalize
        @param in_jinja is a boolean to specify if the returned normalized will be used inside a jinja macro or not
        @param truncate force ignoring truncate operation on resulting normalized name. For example, if we don't
        control how the name would be normalized
        """
        return self.__normalize_non_column_identifier_name(input_name=table_name, in_jinja=in_jinja, truncate=truncate)

    def normalize_column_name(self, column_name: str, in_jinja: bool = False, truncate: bool = True) -> str:
        """
        @param column_name is the column to normalize
        @param in_jinja is a boolean to specify if the returned normalized will be used inside a jinja macro or not
        @param truncate force ignoring truncate operation on resulting normalized name. For example, if we don't
        control how the name would be normalized
        """
        return self.__normalize_identifier_name(column_name=column_name, in_jinja=in_jinja, truncate=truncate)

    def truncate_identifier_name(self, input_name: str, custom_limit: int = -1) -> str:
        """
        @param input_name is the identifier name to middle truncate
        @param custom_limit uses a custom length as the max instead of the destination max length
        """
        limit = custom_limit - 1 if custom_limit > 0 else self.get_name_max_length()

        if limit < len(input_name):
            middle = round(limit / 2)
            # truncate in the middle to preserve prefix/suffix instead
            prefix = input_name[: limit - middle - 1]
            suffix = input_name[1 - middle :]
            # Add extra characters '__', signaling a truncate in identifier
            print(f"Truncating {input_name} (#{len(input_name)}) to {prefix}__{suffix} (#{2 + len(prefix) + len(suffix)})")
            input_name = f"{prefix}__{suffix}"

        return input_name

    def get_name_max_length(self):
        if self.destination_type.value in DESTINATION_SIZE_LIMITS:
            destination_limit = DESTINATION_SIZE_LIMITS[self.destination_type.value]
            return destination_limit - TRUNCATE_DBT_RESERVED_SIZE - TRUNCATE_RESERVED_SIZE
        else:
            raise KeyError(f"Unknown destination type {self.destination_type}")

    # Private methods

    def __normalize_non_column_identifier_name(self, input_name: str, in_jinja: bool = False, truncate: bool = True) -> str:
        # We force standard naming for non column names (see issue #1785)
        result = transform_standard_naming(input_name)
        result = self.__normalize_naming_conventions(result)
        if truncate:
            result = self.truncate_identifier_name(result)
        result = self.__normalize_identifier_case(result, is_quoted=False)
        return result

    def __normalize_identifier_name(self, column_name: str, in_jinja: bool = False, truncate: bool = True) -> str:
        result = self.__normalize_naming_conventions(column_name)
        if truncate:
            result = self.truncate_identifier_name(result)
        if self.needs_quotes(result):
            result = result.replace('"', '""')
            result = result.replace("'", "\\'")
            result = f"adapter.quote('{result}')"
            result = self.__normalize_identifier_case(result, is_quoted=True)
            if not in_jinja:
                result = jinja_call(result)
            return result
        else:
            result = self.__normalize_identifier_case(result, is_quoted=False)
        if in_jinja:
            # to refer to columns while already in jinja context, always quote
            return f"'{result}'"
        return result

    def __normalize_naming_conventions(self, input_name: str) -> str:
        result = input_name
        if self.destination_type.value == DestinationType.BIGQUERY.value:
            result = transform_standard_naming(result)
            doesnt_start_with_alphaunderscore = match("[^A-Za-z_]", result[0]) is not None
            if doesnt_start_with_alphaunderscore:
                result = f"_{result}"
        return result

    def __normalize_identifier_case(self, input_name: str, is_quoted: bool = False) -> str:
        result = input_name
        if self.destination_type.value == DestinationType.BIGQUERY.value:
            pass
        elif self.destination_type.value == DestinationType.REDSHIFT.value:
            # all tables (even quoted ones) are coerced to lowercase.
            result = input_name.lower()
        elif self.destination_type.value == DestinationType.POSTGRES.value:
            if not is_quoted and not self.needs_quotes(input_name):
                result = input_name.lower()
        elif self.destination_type.value == DestinationType.SNOWFLAKE.value:
            if not is_quoted and not self.needs_quotes(input_name):
                result = input_name.upper()
        elif self.destination_type.value == DestinationType.ORACLE.value:
            result = input_name.upper()
        else:
            raise KeyError(f"Unknown destination type {self.destination_type}")
        return result


# Static Functions


def transform_standard_naming(input_name: str) -> str:
    result = input_name.strip()
    result = strip_accents(result)
    result = sub(r"\s+", "_", result)
    result = sub(r"[^a-zA-Z0-9_]", "_", result)
    return result


def strip_accents(input_name: str) -> str:
    return "".join(c for c in ud.normalize("NFD", input_name) if ud.category(c) != "Mn")
