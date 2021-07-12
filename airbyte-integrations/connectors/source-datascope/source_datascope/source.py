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


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime, timedelta
import logging
import backoff

import requests
from requests.exceptions import ConnectionError
from requests.structures import CaseInsensitiveDict
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.sources.streams.http.auth import NoAuth



# Basic full refresh stream
class DatascopeStream(HttpStream, ABC):

    url_base = "https://mydatascope.com/api/external/"


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None
    @backoff.on_exception(backoff.expo, requests.exceptions.ConnectionError, max_tries=7)
    def _request(self, url: str, method: str = "GET", data: dict = None) -> List[dict]:
        response = requests.request(method, url, headers=self._headers, json=data)

        if response.status_code == 200:
            response_data = response.json()
            if isinstance(response_data, list):
                return response_data
            else:
                return [response_data]
        return []


class DatascopeIncrementalStream(DatascopeStream, ABC):

    primary_key = "form_answer_id"
    cursor_field = "updated_at"

    def __init__(self, token: str, form_id: str, start_date: str, schema_type: str, **kwargs):
        super().__init__()
        self.token = token
        self.form_id = form_id
        self.start_date = start_date
        self.schema_type = schema_type
        self.BASE_URL = "https://mydatascope.com"
        self._headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the base currency as a query param so we do that in this method
        return {'token': self.token, 'form_id': self.form_id, 'start': stream_slice['updated_at'], 'date_modified': True, 'order_date': True}

    def get_json_schema(self):
        schema = super().get_json_schema()
        if str(self.schema_type) == 'dynamic':
            return self._request(f"{self.BASE_URL}/api/external/v2/airbyte_schema?token={self.token}&form_id={self.form_id}")[0]
        else:
            return schema

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        return response.json()

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, any]:
        # This method is called once for each record returned from the API to compare the cursor field value in that record with the current state
        # we then return an updated state object. If this is the first time we run a sync or no state was passed, current_stream_state will be None.
        if current_stream_state is not None and 'updated_at' in current_stream_state:
            current_parsed_date = datetime.strptime(current_stream_state['updated_at'], '%Y-%m-%dT%H:%M:%S%z')
            last_date = latest_record['updated_at'].split('.')[0] + '+0000'
            if self.schema_type == 'dynamic':
                latest_record_date = datetime.strptime(last_date, '%d/%m/%Y %H:%M%z')
            else:
                latest_record_date = datetime.strptime(last_date, '%Y-%m-%dT%H:%M:%S%z')
            return {'updated_at': max(current_parsed_date, latest_record_date).strftime('%Y-%m-%dT%H:%M:%S%z')}
        else:
            return {'updated_at': self.start_date.strftime('%Y-%m-%dT%H:%M:%S%z')}

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now(start_date.tzinfo):
            dates.append({'updated_at': start_date.strftime('%Y-%m-%dT%H:%M:%S%z')})
            start_date += timedelta(days=7)
        return dates
    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[
        Optional[Mapping[str, any]]]:
        start_date = datetime.strptime(stream_state['updated_at'], '%Y-%m-%dT%H:%M:%S%z') if stream_state and 'updated_at' in stream_state else self.start_date
        return self._chunk_date_range(start_date)




class Forms(DatascopeIncrementalStream):
    primary_key = "form_answer_id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        if self.schema_type == 'dynamic':
            return "v2/airbyte"
        else:
            return "answers"

# Source
class SourceDatascope(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth()
        start_date = datetime.strptime(config['start_date'], '%Y-%m-%dT%H:%M:%S%z') 
        schema_type = config.get('schema_type', 'static')
        return [Forms(authenticator=auth, token=config['api_key'], form_id=config['form_id'], start_date=start_date, schema_type=schema_type)]
