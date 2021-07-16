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


import copy
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Iterator, List, Mapping, MutableMapping, Optional, Tuple

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    Status,
    SyncMode,
)
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.source import Source
from airbyte_cdk.sources.streams import Stream


class AbstractSource(Source, ABC):
    """
    Abstract base class for an Airbyte Source. Consumers should implement any abstract methods
    in this class to create an Airbyte Specification compliant Source.
    """

    @abstractmethod
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        """
        :param config: The user-provided configuration as specified by the source's spec. This usually contains information required to check connection e.g. tokens, secrets and keys etc.
        :return: A tuple of (boolean, error). If boolean is true, then the connection check is successful and we can connect to the underlying data
        source using the provided configuration.
        Otherwise, the input config cannot be used to connect to the underlying data source, and the "error" object should describe what went wrong.
        The error object will be cast to string to display the problem to the user.
        """

    @abstractmethod
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: The user-provided configuration as specified by the source's spec. Any stream construction related operation should happen here.
        :return: A list of the streams in this source connector.
        """

    @property
    def name(self) -> str:
        """Source name"""
        return self.__class__.__name__

    def discover(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteCatalog:
        """Implements the Discover operation from the Airbyte Specification. See https://docs.airbyte.io/architecture/airbyte-specification."""
        streams = [stream.as_airbyte_stream() for stream in self.streams(config=config)]
        return AirbyteCatalog(streams=streams)

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """Implements the Check Connection operation from the Airbyte Specification. See https://docs.airbyte.io/architecture/airbyte-specification."""
        try:
            check_succeeded, error = self.check_connection(logger, config)
            if not check_succeeded:
                return AirbyteConnectionStatus(status=Status.FAILED, message=repr(error))
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=repr(e))

        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def read(
        self, logger: AirbyteLogger, config: Mapping[str, Any], catalog: ConfiguredAirbyteCatalog, state: MutableMapping[str, Any] = None
    ) -> Iterator[AirbyteMessage]:
        """Implements the Read operation from the Airbyte Specification. See https://docs.airbyte.io/architecture/airbyte-specification."""
        connector_state = copy.deepcopy(state or {})
        logger.info(f"Starting syncing {self.name}")
        # TODO assert all streams exist in the connector
        # get the streams once in case the connector needs to make any queries to generate them
        stream_instances = {s.name: s for s in self.streams(config)}
        for configured_stream in catalog.streams:
            stream_instance = stream_instances.get(configured_stream.stream.name)
            if not stream_instance:
                raise KeyError(
                    f"The requested stream {configured_stream.stream.name} was not found in the source. Available streams: {stream_instances.keys()}"
                )

            try:
                yield from self._read_stream(
                    logger=logger, stream_instance=stream_instance, configured_stream=configured_stream, connector_state=connector_state
                )
            except Exception as e:
                logger.exception(f"Encountered an exception while reading stream {self.name}")
                raise e

        logger.info(f"Finished syncing {self.name}")

    def _read_stream(
        self,
        logger: AirbyteLogger,
        stream_instance: Stream,
        configured_stream: ConfiguredAirbyteStream,
        connector_state: MutableMapping[str, Any],
    ) -> Iterator[AirbyteMessage]:

        use_incremental = configured_stream.sync_mode == SyncMode.incremental and stream_instance.supports_incremental
        if use_incremental:
            record_iterator = self._read_incremental(logger, stream_instance, configured_stream, connector_state)
        else:
            record_iterator = self._read_full_refresh(stream_instance, configured_stream)

        record_counter = 0
        stream_name = configured_stream.stream.name
        logger.info(f"Syncing stream: {stream_name} ")
        for record in record_iterator:
            if record.type == MessageType.RECORD:
                record_counter += 1
            yield record

        logger.info(f"Read {record_counter} records from {stream_name} stream")

    def _read_incremental(
        self,
        logger: AirbyteLogger,
        stream_instance: Stream,
        configured_stream: ConfiguredAirbyteStream,
        connector_state: MutableMapping[str, Any],
    ) -> Iterator[AirbyteMessage]:
        stream_name = configured_stream.stream.name
        stream_state = connector_state.get(stream_name, {})
        if stream_state:
            logger.info(f"Setting state of {stream_name} stream to {stream_state}")

        checkpoint_interval = stream_instance.state_checkpoint_interval
        slices = stream_instance.stream_slices(
            cursor_field=configured_stream.cursor_field, sync_mode=SyncMode.incremental, stream_state=stream_state
        )
        for slice in slices:
            record_counter = 0
            records = stream_instance.read_records(
                sync_mode=SyncMode.incremental,
                stream_slice=slice,
                stream_state=stream_state,
                cursor_field=configured_stream.cursor_field or None,
            )
            for record_data in records:
                record_counter += 1
                yield self._as_airbyte_record(stream_name, record_data)
                stream_state = stream_instance.get_updated_state(stream_state, record_data)
                if checkpoint_interval and record_counter % checkpoint_interval == 0:
                    yield self._checkpoint_state(stream_name, stream_state, connector_state, logger)

            yield self._checkpoint_state(stream_name, stream_state, connector_state, logger)

    def _read_full_refresh(self, stream_instance: Stream, configured_stream: ConfiguredAirbyteStream) -> Iterator[AirbyteMessage]:
        slices = stream_instance.stream_slices(sync_mode=SyncMode.full_refresh, cursor_field=configured_stream.cursor_field)
        for slice in slices:
            records = stream_instance.read_records(
                stream_slice=slice, sync_mode=SyncMode.full_refresh, cursor_field=configured_stream.cursor_field
            )
            for record in records:
                yield self._as_airbyte_record(configured_stream.stream.name, record)

    def _checkpoint_state(self, stream_name, stream_state, connector_state, logger):
        logger.info(f"Setting state of {stream_name} stream to {stream_state}")
        connector_state[stream_name] = stream_state
        return AirbyteMessage(type=MessageType.STATE, state=AirbyteStateMessage(data=connector_state))

    def _as_airbyte_record(self, stream_name: str, data: Mapping[str, Any]):
        now_millis = int(datetime.now().timestamp()) * 1000
        message = AirbyteRecordMessage(stream=stream_name, data=data, emitted_at=now_millis)
        return AirbyteMessage(type=MessageType.RECORD, record=message)
