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

import FuelSDK
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import AirbyteConnectionStatus, Status
from airbyte_cdk.sources.singer import SingerSource
from suds.transport.https import HttpAuthenticated


class SourceSfMarketingcloudSinger(SingerSource):
    TAP_CMD = "tap-exacttarget"

    def check_config(self, logger: AirbyteLogger, config_path: str, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config_path: Path to the file containing the configuration json config
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        local_config = {
            "clientid": config["client_id"],
            "clientsecret": config["client_secret"],
            "tenant_subdomain": config["tenant_subdomain"],
            "start_date": config["start_date"],
            "request_timeout": "50000",
            "useOAuth2Authentication": "True",
            "authenticationurl": "",
        }
        # taken from line 52 - 80 in tap_exacttarget/client.py
        try:
            logger.info("Trying to authenticate using V1 endpoint")
            local_config["useOAuth2Authentication"] = "False"
            auth_stub = FuelSDK.ET_Client(params=local_config)
            transport = HttpAuthenticated(timeout=int(local_config.get("request_timeout", 900)))
            auth_stub.soap_client.set_options(transport=transport)
            logger.info("Success.")
        except Exception as e:
            logger.info("Failed to auth using V1 endpoint")
            if not local_config.get("tenant_subdomain"):
                logger.info("No tenant_subdomain found, will not attempt to auth with V2 endpoint")
                raise e

        # Next try V2
        # Move to OAuth2: https://help.salesforce.com/articleView?id=mc_rn_january_2019_platform_ip_remove_legacy_package_create_ability.htm&type=5
        try:
            logger.info("Trying to authenticate using V2 endpoint")
            local_config["useOAuth2Authentication"] = "True"

            local_config["authenticationurl"] = "https://{}.auth.marketingcloudapis.com".format(local_config["tenant_subdomain"])
            auth_stub = FuelSDK.ET_Client(params=local_config)
            transport = HttpAuthenticated(timeout=int(local_config.get("request_timeout", 900)))
            auth_stub.soap_client.set_options(transport=transport)
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            logger.info("Failed to auth using V2 endpoint")
            raise e
            logger.info("Login succeeded")
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover_cmd(self, logger: AirbyteLogger, config_path: str) -> str:
        """
        Return the string commands to invoke the tap with the --discover flag and the right configuration options
        """
        return f"tap-exacttarget --config {config_path} --discover"

    def read_cmd(self, logger: AirbyteLogger, config_path: str, catalog_path: str, state_path: str = None) -> str:
        """
        Return the string commands to invoke the tap with the right configuration options to read data from the source
        """
        # TODO update the command below if needed. Otherwise you're good to go
        config_option = f"--config {config_path}"
        properties_option = f"--properties {catalog_path}"
        state_option = f"--state {state_path}" if state_path else ""
        return f"{self.TAP_CMD} {config_option} {properties_option} {state_option}"
