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


import base64
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import parse_qsl, urlparse


import requests
from requests.auth import HTTPBasicAuth
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

# Basic full refresh stream
class DelightedStream(HttpStream, ABC):

    url_base = "https://api.delighted.com/v1/"

    # Page size
    limit = 100

    # Define primary key to all streams as primary key
    primary_key = "id"

    def __init__(self, since: int, api_key: str, **kwargs):
        super().__init__(**kwargs)
        self.since = since
        self.api_key = api_key
        self.since_id = 0

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # Getting next page link
        next_page = response.links.get("next", None)
        if next_page:
            return dict(parse_qsl(urlparse(next_page.get("url")).query))
        else:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            params = {"per_page": self.limit, **next_page_token}
        else:
            params = {"per_page": self.limit, "since": self.since}
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        records = response.json()
        yield from records


class IncrementalDelightedStream(DelightedStream, ABC):
    
    # Getting page size as 'limit' from parrent class
    @property
    def limit(self):
        return super().limit

    state_checkpoint_interval = limit

    @property
    def cursor_field(self) -> str:
        return "created_at"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, 0), current_stream_state.get(self.cursor_field, 0))}

    def request_params(self, stream_state=None, **kwargs):
        stream_state = stream_state or {}
        params = super().request_params(stream_state=stream_state, **kwargs)
        if stream_state:
            params["since"] = stream_state.get(self.cursor_field)
        return params


class People(IncrementalDelightedStream):

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "people.json"


class UnsubscribedPeople(IncrementalDelightedStream):
    cursor_field = "unsubscribed_at"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "unsubscribes.json"


class BouncedPeople(IncrementalDelightedStream):
    cursor_field = "bounced_at"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "bounces.json"


class SurveyResponses(IncrementalDelightedStream):
    cursor_field = "updated_at"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "survey_responses.json"

    def request_params(self, stream_state=None, **kwargs):
        stream_state = stream_state or {}
        params = super().request_params(stream_state=stream_state, **kwargs)
        if stream_state:
            params["updated_since"] = stream_state.get(self.cursor_field)
        return params


# Source
class SourceDelighted(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """

        Testing connection availability for the connector.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        api_key = config["api_key"]

        try:
            session = requests.get('https://api.delighted.com/v1/people.json', auth=HTTPBasicAuth(api_key, ''))
            session.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        token = base64.b64encode(f"{config['api_key']}:".encode("utf-8")).decode("utf-8")
        auth = TokenAuthenticator(token=token, auth_method="Basic")

        args = {"authenticator": auth, "since": config["since"], "api_key": config["api_key"]}

        return [People(**args),
                UnsubscribedPeople(**args),
                BouncedPeople(**args)]
