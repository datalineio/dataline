"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from airbyte_protocol import AirbyteConnectionStatus, Status
from base_python import AirbyteLogger, ConfigContainer
from base_singer import SingerSource

TAP_CMD = "tap-facebook"


class SourceFacebookMarketingApiSinger(SingerSource):
    def __init__(self):
        super().__init__()

    def transform_config(self, raw_config):
        return {
            "start_date": raw_config["start_date"],
            "account_id": raw_config["account_id"],
            "access_token": raw_config["access_token"],
            # tap-singer expects a string not a boolean
            "include_deleted": str(raw_config.get("include_deleted", False)),
        }

    def check(self, logger: AirbyteLogger, config_container: ConfigContainer) -> AirbyteConnectionStatus:
        try:
            self.discover(logger, config_container)
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            # TODO parse the exception message for a human readable error
            logger.error("Exception while connecting to the FB Marketing API")
            logger.error(str(e))
            return AirbyteConnectionStatus(
                status=Status.FAILED, message="Unable to connect to the FB Marketing API with the provided credentials. "
            )

    def discover_cmd(self, logger: AirbyteLogger, config_path: str) -> str:
        return f"{TAP_CMD} -c {config_path} --discover"

    def read_cmd(self, logger: AirbyteLogger, config_path: str, catalog_path: str, state_path: str = None) -> str:
        state_path = f"--state {state_path}" if state_path else ""
        return f"{TAP_CMD} -c {config_path} -p {catalog_path} {state_path}"

    def transform_config(self, raw_config):
        # todo (cgardens) - this is supposed to be handled in the ui and the api but neither of them are able to handle it right now. issue: https://github.com/airbytehq/airbyte/issues/892
        if "include_deleted" not in raw_config:
            raw_config["include_deleted"] = True

        return raw_config
