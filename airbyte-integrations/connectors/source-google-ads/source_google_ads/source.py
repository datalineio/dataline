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


import json
from datetime import datetime
from typing import Dict, Generator

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.models.airbyte_protocol import SyncMode
from airbyte_cdk.sources import Source
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.oauth2.credentials import Credentials

class SourceGoogleAds(Source):
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            if(len(config['streams'] )== 0):
                raise Exception("No streams defined. Add at least one stream.")

            self.discover(logger, config)

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def _search(self, query, config: json):
        client = GoogleAdsClient.load_from_dict(config)
        ga_service = client.get_service("GoogleAdsService")

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = config['customer_id']
        search_request.query = query
        response = ga_service.search_stream(search_request)
#https://github.com/googleads/google-ads-python/issues/384
        response.service_reference = ga_service
        return response


    field_type_dict = {
        bool: "boolean",
        int: "integer",
        float: "number",
        str: "string"
    }
    def get_field(self, row, key):
        parts = key.split('.')
        obj = getattr(row, parts[0])
        fld = getattr(obj, parts[1])
        return fld

    def extract_schema(self, stream_config, response):
        props = {}
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": props,
        }
        for batch in response:
            for row in batch.results:
                error_keys = []
                for key in batch.field_mask.paths:
                    try:
                        fld = self.get_field(row, key)
                        typ = self.field_type_dict[type(fld)]
                        props[key] = {'type': typ}
                    except Exception as e:
                        error_keys.append(key)

                if(len(error_keys)>0):
                    raise Exception(f"The {stream_config['name']} GAQL contains invalid keys, try removing them from the GAQL. Keys: {str(error_keys)}")
                return json_schema

        raise Exception(f"The reponse has now rows. Please modify the {stream_config['name']} GAQL so that it returns at least one row") 


    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        streams = []

        for stream_config in config['streams']:
            query = stream_config['gaql']
            stream_name = stream_config['name']
            response = self._search(query, config)
            json_schema = self.extract_schema(stream_config, response)
            stream = AirbyteStream(name=stream_name, json_schema=json_schema)
            stream.supported_sync_modes = [SyncMode.full_refresh, SyncMode.incremental]
            streams.append(stream)

        return AirbyteCatalog(streams=streams)

    def read(
        self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.json file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """

        for stream_catalog in catalog.streams:
            has_emitted_state = False
            stream_name = stream_catalog.stream.name
            
            stream_config = next(cfg for cfg in config["streams"] if cfg["name"] == stream_name)

            if stream_config is None:
                #TODO Can we map the configured name back to original somehow?
                logger.error(f"Renaming of streams not supported. Please use the original stream name defined in the config. Configured name: {stream_name}")
                continue

            #TODO Can we annotate the gaql when using incremental sync, like add a WHERE clause?
            query = stream_config['gaql']
            response = self._search(query, config)

            max_cursor = None
            cursor_field_key = None
            if(stream_catalog.sync_mode == SyncMode.incremental):
                crs_fld = stream_catalog.cursor_field
                if(crs_fld is None):
                    logger.error(f"Incremental mode, but no cursor field defined for stream {stream_name}")
                    continue
                cursor_field_key = '.'.join(crs_fld)
                stream_state = state.get(stream_name)
                if(not stream_state is None):
                    max_cursor = stream_state[cursor_field_key]

            for batch in response:
                for row in batch.results:
                    data = {}
                    for key in batch.field_mask.paths:
                        fld = self.get_field(row, key)
                        data[key] = fld
                    if(stream_catalog.sync_mode == SyncMode.incremental):
                        data_cursor_value = data[cursor_field_key]
                        if(max_cursor and max_cursor > data_cursor_value):
                            continue
                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(stream=stream_name, data=data, emitted_at=int(datetime.now().timestamp()) * 1000),
                    )
                    if(stream_catalog.sync_mode == SyncMode.incremental and (not max_cursor or max_cursor < data[cursor_field_key])):
                        max_cursor = data_cursor_value
                        state[stream_name] = {cursor_field_key: max_cursor}
                        has_emitted_state = True
                        yield AirbyteMessage(
                            type=Type.STATE,
                            state=AirbyteRecordMessage(stream=stream_name, data=state, emitted_at=int(datetime.now().timestamp()) * 1000),
                        )

            if(not has_emitted_state):
                yield AirbyteMessage(
                    type=Type.STATE,
                    state=AirbyteRecordMessage(stream=stream_name, data=state, emitted_at=int(datetime.now().timestamp()) * 1000),
                )

