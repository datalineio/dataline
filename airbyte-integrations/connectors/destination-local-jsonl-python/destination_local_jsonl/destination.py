import io
import json
import sys
from pathlib import Path
from typing import Iterable, Mapping

from typing.io import TextIO

from airbyte_protocol import AirbyteMessage, ConfiguredAirbyteCatalog, AirbyteConnectionStatus, Status, AirbyteRecordMessage
from airbyte_protocol.models import DestinationSyncMode, Type
from base_python import Destination, AirbyteLogger


class DestinationLocalJsonL(Destination):
    # When running in Docker, this directory is mounted from the host system. So any files that should be retained outside of the Docker image
    # should be written to this directory and they will live on the host system once this container is done.

    def _create_directory_if_not_exists(self, directory: str):
        Path(directory).mkdir(parents=True, exist_ok=True)

    def _get_mounted_directory(self, output_dir):
        return output_dir  # TODO

    def check(self, logger: AirbyteLogger, config: Mapping[str, any]) -> AirbyteConnectionStatus:
        """
        Verify if the provided configuration can be used to run a sync.
        To do this we just verify if we can create the output destination. A failure will mean something like e.g: not having permissions.
        """
        output_directory = self._get_mounted_directory(config['output_directory'])
        try:
            self._create_directory_if_not_exists(output_directory)
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            msg = f"Cannot create output directory at {output_directory}. The following error occurred: {e}"
            return AirbyteConnectionStatus(status=Status.FAILED, message=msg)

    def _sync_mode_to_open_mode(self, mode: DestinationSyncMode):
        if mode == DestinationSyncMode.overwrite:
            return "w"
        elif mode == DestinationSyncMode.append:
            return "a"
        else:
            raise Exception(f"Unuspported sync mode: {mode}")

    def input_streams_to_file_handles(self, output_directory: str, configured_catalog: ConfiguredAirbyteCatalog) -> Mapping[str, TextIO]:
        stream_to_handle = {}
        root = Path(output_directory)
        for configured_stream in configured_catalog.streams:
            name = configured_stream.stream.name
            write_mode = self._sync_mode_to_open_mode(configured_stream.destination_sync_mode)
            full_path = root / f"{name}.json"
            handle = open(full_path, write_mode)
            stream_to_handle[name] = handle

        return stream_to_handle

    def read_record_messages(self, stdin: io.TextIOWrapper) -> Iterable[AirbyteRecordMessage]:
        for line in stdin:
            try:
                msg = AirbyteMessage.parse_raw(line)
                if msg.type == Type.RECORD:
                    yield msg.record
            except Exception:
                self.logger.info(f"ignoring non-record input: {line}")
        yield from []

    def write(self, logger: AirbyteLogger, config: Mapping[str, any], configured_catalog: ConfiguredAirbyteCatalog):
        stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
        output_directory = self._get_mounted_directory(config['output_directory'])
        stream_to_handle = self.input_streams_to_file_handles(output_directory, configured_catalog)
        record_counter = 0
        for record_message in self.read_record_messages(stdin):
            handle = stream_to_handle[record_message.stream]
            handle.write(json.dumps(record_message.data) + "\n")
            record_counter += 1

        for handle in stream_to_handle.values():
            handle.close()

        logger.info(f"Finished syncing {record_counter} records")
