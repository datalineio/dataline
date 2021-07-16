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

import json
from typing import Any, Dict, List, Mapping

import pytest
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    SyncMode,
    Type,
)
from destination_kvdb import DestinationKvdb
from destination_kvdb.client import KvDbClient


@pytest.fixture(name="config")
def config_fixture() -> Mapping[str, Any]:
    with open("secrets/config.json", "r") as f:
        return json.loads(f.read())


@pytest.fixture(name="configured_catalog")
def configured_catalog_fixture() -> ConfiguredAirbyteCatalog:
    stream_schema = {"type": "object", "properties": {"string_col": {"type": "str"}, "int_col": {"type": "integer"}}}

    append_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(name="append_stream", json_schema=stream_schema),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append,
    )

    overwrite_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(name="overwrite_stream", json_schema=stream_schema),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.overwrite,
    )

    return ConfiguredAirbyteCatalog(streams=[append_stream, overwrite_stream])


@pytest.fixture(autouse=True)
def teardown(config: Mapping):
    yield
    client = KvDbClient(**config)
    client.delete(list(client.list_keys()))


@pytest.fixture(name="client")
def client_fixture(config) -> KvDbClient:
    return KvDbClient(**config)


def test_check_valid_config(config: Mapping):
    outcome = DestinationKvdb().check(AirbyteLogger(), config)
    assert outcome.status == Status.SUCCEEDED


def test_check_invalid_config():
    outcome = DestinationKvdb().check(AirbyteLogger(), {"bucket_id": "not_a_real_id"})
    assert outcome.status == Status.FAILED


def _state(data: Dict[str, Any]) -> AirbyteMessage:
    return AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(data=data))


def _record(stream: str, str_value: str, int_value: int) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.RECORD, record=AirbyteRecordMessage(stream=stream, data={"str_col": str_value, "int_col": int_value}, emitted_at=0)
    )


def retrieve_all_records(client: KvDbClient) -> List[AirbyteRecordMessage]:
    """retrieves and formats all records in kvdb as Airbyte messages"""
    all_records = client.list_keys(list_values=True)
    out = []
    for record in all_records:
        key = record[0]
        stream = key.split("__ab__")[0]
        value = record[1]
        out.append(_record(stream, value["str_col"], value["int_col"]))
    return out


def test_write(config: Mapping, configured_catalog: ConfiguredAirbyteCatalog, client: KvDbClient):
    """
    This test verifies that:
        1. writing a stream in "overwrite" mode overwrites any existing data for that stream
        2. writing a stream in "append" mode appends new records without deleting the old ones
        3. The correct state message is output by the connector at the end of the sync
    """
    append_stream, overwrite_stream = configured_catalog.streams[0].stream.name, configured_catalog.streams[1].stream.name
    first_state_message = _state({"state": "1"})
    first_record_chunk = [
        # make 5 records in each stream followed by state twice
        *[_record(append_stream, str(i), i) for i in range(5)],
        *[_record(overwrite_stream, str(i), i) for i in range(5)],
    ]

    second_state_message = _state({"state": "2"})
    second_record_chunk = [
        *[_record(append_stream, str(i), i) for i in range(5, 10)],
        *[_record(overwrite_stream, str(i), i) for i in range(5, 10)],
    ]

    destination = DestinationKvdb()

    expected_states = [first_state_message, second_state_message]
    output_states = list(
        destination.write(
            config, configured_catalog, [*first_record_chunk, first_state_message, *second_record_chunk, second_state_message]
        )
    )
    assert expected_states == output_states, "Checkpoint state messages were expected from the destination"

    expected_records = [*[_record(append_stream, str(i), i) for i in range(10)], *[_record(overwrite_stream, str(i), i) for i in range(10)]]
    records_in_destination = retrieve_all_records(client)
    assert expected_records == records_in_destination, "Records in destination should match records expected"

    # After this sync we expect the append stream to have 15 messages and the overwrite stream to have 5
    third_state_message = _state({"state": "3"})
    third_record_chunk = [
        *[_record(append_stream, str(i), i) for i in range(10, 15)],
        *[_record(overwrite_stream, str(i), i) for i in range(10, 15)],
    ]

    output_states = list(destination.write(config, configured_catalog, [*third_record_chunk, third_state_message]))
    assert [third_state_message] == output_states

    records_in_destination = retrieve_all_records(client)
    expected_records = [
        *[_record(append_stream, str(i), i) for i in range(15)],
        *[_record(overwrite_stream, str(i), i) for i in range(10, 15)],
    ]
    assert expected_records == records_in_destination
