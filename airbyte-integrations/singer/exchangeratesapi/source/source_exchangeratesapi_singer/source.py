import urllib.request
from typing import Generator

from airbyte_protocol import AirbyteCatalog
from airbyte_protocol import AirbyteCheckResponse
from airbyte_protocol.base import AirbyteEntrypoint
from airbyte_protocol import AirbyteMessage
from airbyte_protocol import AirbyteSpec
from airbyte_protocol import Source
from base_singer import SingerHelper


class SourceExchangeRatesApiSinger(Source):
    def __init__(self):
        pass

    def spec(self) -> AirbyteSpec:
        return SingerHelper.spec_from_file("/airbyte/exchangeratesapi-files/spec.json")

    def check(self, logger, config_container) -> AirbyteCheckResponse:
        code = urllib.request.urlopen("https://api.exchangeratesapi.io/").getcode()
        logger.info(f"Ping response code: {code}")
        return AirbyteCheckResponse(code == 200, {})

    def discover(self, logger, config_container) -> AirbyteCatalog:
        catalogs = SingerHelper.get_catalogs(logger,
                                             "tap-exchangeratesapi | grep '\"type\": \"SCHEMA\"' | head -1 | jq -c '{\"streams\":[{\"stream\": .stream, \"schema\": .schema}]}'")
        return catalogs.airbyte_catalog

    def read(self, logger, config_container, catalog_path, state=None) -> Generator[AirbyteMessage, None, None]:
        config_option = f"--config {config_container.rendered_config_path}"
        state_option = f"--state {state}" if state else ""
        return SingerHelper.read(logger, f"tap-exchangeratesapi {config_option} {state_option}")


if __name__ == "__main__":
    source = SourceExchangeRatesApiSinger()
    AirbyteEntrypoint(source).start()
