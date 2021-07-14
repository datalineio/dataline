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

import logging
from typing import Iterator, List, Tuple, Mapping, Any

from airbyte_cdk.models import AirbyteRecordMessage, ConfiguredAirbyteCatalog
from jsonschema import Draft4Validator, ValidationError


def is_nullable_schema(schema: Mapping[str, Any]) -> bool:
    """Tell if object schema allows it to be null"""
    obj_types = schema.get("type", [])
    if not isinstance(obj_types, List):
        obj_types = [obj_types]

    if "null" in obj_types:
        return True

    any_of = schema.get("anyOf")
    if any_of:
        for alt_type in any_of:
            if alt_type.get("type") == "null":
                return True

    return False


def verify_records_schema(
    records: List[AirbyteRecordMessage], catalog: ConfiguredAirbyteCatalog
) -> Iterator[Tuple[AirbyteRecordMessage, List[ValidationError]]]:
    """Check records against their schemas from the catalog, yield error messages.
    Only first record with error will be yielded for each stream.
    """
    validators = {}
    for stream in catalog.streams:
        stream_name = stream.stream.name
        schema = stream.stream.json_schema
        validators[stream_name] = Draft4Validator(schema)

        # we don't want top level schema to be nullable, because this will bypass all validation
        assert not is_nullable_schema(schema), f"Stream `{stream_name}`: nullable top level schema is not supported"

    for record in records:
        validator = validators.get(record.stream)
        if not validator:
            logging.error(f"Record from the {record.stream} stream that is not in the catalog.")
            continue
        errors = list(validator.iter_errors(record.data))
        if errors:
            yield record, sorted(errors, key=str)
