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


from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Tuple

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import AirbyteStream
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .zuora_auth import ZuoraAuthenticator
from .zuora_client import ZoqlExportClient
from .zuora_errors import ZOQLQueryCannotProcessObject, ZOQLQueryFieldCannotResolve


# Main class for Zuora Stream with overriden CDK methods
class ZuoraStream(Stream, ABC):
    # Define general primary key
    primary_key = "id"

    def __init__(self, api: ZoqlExportClient):
        self.api = api

    def get_cursor_from_schema(self, schema: Dict) -> str:
        """
        Get the cursor_field from the stream's schema,
        If the stream doesn't support 'updateddate', then we use 'createddate'.
        """
        return self.alt_cursor_field if self.cursor_field not in schema.keys() else self.cursor_field

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Get the stream's schema from Zuora Object's Data types,
        """
        schema = {}
        schema["properties"] = self.api._zuora_object_to_json_schema(self.name)
        return schema

    def as_airbyte_stream(self)-> AirbyteStream:
        """
        Override this method to set the default_cursor_field to the stream schema.
        """
        stream = super().as_airbyte_stream()
        stream.default_cursor_field = [self.get_cursor_from_schema(stream.json_schema["properties"])]
        return stream

    def _get_stream_state(self, stream_state: Mapping[str, Any] = None) -> Mapping[str, Any]:
        """
        Get the state of the stream for the default cursor_field = updateddate,
        If the stream doesn't have 'updateddate', then we use 'createddate'.
        If stream state == None, than we use start_date by default.
        """
        return stream_state.get(self.cursor_field, stream_state.get(self.alt_cursor_field)) if stream_state else self.api.start_date

    def read_records(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        # if stream_state is missing, we will use the start-date from config for a full refresh
        stream_state = self._get_stream_state(stream_state)
        try:
            yield from self.api._get_data_with_date_slice("select", self.name, self.cursor_field, stream_state, self.window_in_days)
        except ZOQLQueryFieldCannotResolve:
            self.cursor_field = self.alt_cursor_field
            yield from self.api._get_data_with_date_slice("select", self.name, self.cursor_field, stream_state, self.window_in_days)
        except ZOQLQueryCannotProcessObject:
            pass


# Incremental-refresh for Zuora Stream handels both full-refresh and incremental-refresh
class IncrementalZuoraStream(ZuoraStream):

    # Define default cursor field
    cursor_field = "updateddate"
    alt_cursor_field = "createddate"

    # setting limit of the date-slice for the data query job
    @property
    def window_in_days(self) -> int:
        return self.api.window_in_days

    # setting checkpoint interval to the limit of date-slice
    state_checkpoint_interval = window_in_days

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, ""), current_stream_state.get(self.cursor_field, ""))}


# Basic Connections Check
class SourceZuora(AbstractSource):
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        Testing connection availability for the connector by granting the token.
        """
        auth = ZuoraAuthenticator(config["is_sandbox"]).generateToken(config["client_id"], config["client_secret"])
        if auth.get("status") == 200:
            return True, None
        else:
            return False, auth["status"]

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Mapping a input config of the user input configuration as defined in the connector spec.
        Defining streams to run.
        """

        def create_stream_class_from_object_name(zuora_objects: List) -> List:
            """
            The function to produce the dynamic stream classes from the list of zuora objects names
            """
            # Define the bases
            cls_base = (IncrementalZuoraStream,)
            cls_props = {}
            # Build the streams
            streams = []
            for obj in zuora_objects:
                # List of zuora objects >> stream classes
                streams.append(type(obj, cls_base, cls_props))
            return streams

        # Make instance of ZuoraAuthenticator to get header with token, url_base
        auth_client = ZuoraAuthenticator(config["is_sandbox"])
        authenticator = auth_client.generateToken(config["client_id"], config["client_secret"]).get("header")
        url_base = auth_client.endpoint

        # Make set of arguments for the ZoqlExportClient
        args = {
            "authenticator": authenticator,
            "url_base": url_base,
            "start_date": config["start_date"],
            "window_in_days": config["window_in_days"],
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "is_sandbox": config["is_sandbox"],
        }

        # Making instance of Zuora API Client
        zuora_client = ZoqlExportClient(**args)

        # Get the list of available objects from Zuora
        zuora_objects = ["account", "subscription", "invoicehistory"]
        # zuora_objects = zuora_client._zuora_list_objects()
        # created the class for each object
        streams = create_stream_class_from_object_name(zuora_objects)
        # Return the list of stream classes with Zuora API Client as input
        return [streams[c](zuora_client) for c in range(len(streams))]
