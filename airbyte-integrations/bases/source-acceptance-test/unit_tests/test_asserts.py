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

import pytest
from airbyte_cdk.models import (
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from source_acceptance_test.utils.asserts import verify_records_schema


@pytest.fixture(name="record_schema")
def record_schema_fixture():
    return {
        "properties": {
            "text_or_null": {"type": ["null", "string"]},
            "number_or_null": {"type": ["null", "number"]},
            "text": {"type": ["string"]},
            "number": {"type": ["number"]},
        },
        "type": ["object"],
    }


@pytest.fixture(name="configured_catalog")
def catalog_fixture(record_schema) -> ConfiguredAirbyteCatalog:
    stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(name="my_stream", json_schema=record_schema),
        sync_mode=SyncMode.full_refresh,
        destination_sync_mode=DestinationSyncMode.append,
    )

    return ConfiguredAirbyteCatalog(streams=[stream])


@pytest.fixture(name="configured_catalog_with_nullable_schema")
def configured_catalog_with_nullable_schema_fixture(configured_catalog) -> ConfiguredAirbyteCatalog:
    configured_catalog.streams[0].stream.json_schema["type"].append("null")
    return configured_catalog


def test_verify_records_schema(configured_catalog: ConfiguredAirbyteCatalog):
    """Test that correct records returned as records with errors, and verify specific error messages"""
    records = [
        {
            "text_or_null": 123,  # wrong format
            "number_or_null": 10.3,
            "text": "text",
            "number": "text",  # wrong format
        },
        {
            "text_or_null": "test",
            "number_or_null": None,
            "text": None,  # wrong value
            "number": None,  # wrong value
        },
        {
            "text_or_null": None,
            "number_or_null": None,
            "text": "text",
            "number": 77,
        },
        {
            "text_or_null": None,
            "number_or_null": None,
            "text": "text",
            "number": "text",  # wrong format
        },
    ]

    records = [AirbyteRecordMessage(stream="my_stream", data=record, emitted_at=0) for record in records]

    records_with_errors, record_errors = zip(*verify_records_schema(records, configured_catalog))
    errors = [[error.message for error in errors] for errors in record_errors]

    assert len(records_with_errors) == 3, "only 3 out of 4 records have errors"
    assert records_with_errors[0] == records[0], "1st record should have errors"
    assert records_with_errors[1] == records[1], "2nd record should have errors"
    assert records_with_errors[2] == records[3], "4th record should have errors"
    assert errors[0] == ["'text' is not of type 'number'", "123 is not of type 'null', 'string'"]
    assert errors[1] == ["None is not of type 'number'", "None is not of type 'string'"]
    assert errors[2] == ["'text' is not of type 'number'"]


def test_top_level_nullable_schema(configured_catalog_with_nullable_schema: ConfiguredAirbyteCatalog):
    """Test that we don't allow top level schema to be nullable"""
    records = []

    with pytest.raises(AssertionError):
        list(verify_records_schema(records, configured_catalog_with_nullable_schema))
