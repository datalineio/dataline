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

import urllib.parse
from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream


class CartStream(HttpStream, ABC):
    primary_key = "id"

    def __init__(self, start_date: str, store_name: str, **kwargs):
        self._start_date = start_date
        self.store_name = store_name
        super().__init__(**kwargs)

    @property
    def url_base(self) -> str:
        return f"https://{self.store_name}/api/v1/"

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """
        We dont need to check the response.status_code == 429 since this header exists only in this case.
        """
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            return int(retry_after)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        if response_json.get("next_page"):
            next_query_string = urllib.parse.urlsplit(response_json.get("next_page")).query
            params = dict(urllib.parse.parse_qsl(next_query_string))
            return params

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        return {"Cache-Control": "no-cache", "Content-Type": "application/json"}

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any], **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        result = response_json.get(self.data_field, [])
        yield from result

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"count": 100, "sort": "updated_at"}
        if next_page_token:
            params.update(next_page_token)
        return params


class IncrementalCartStream(CartStream, ABC):

    state_checkpoint_interval = 1000
    cursor_field = "updated_at"

    def request_params(self, stream_state: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, **kwargs)
        cursor_value = stream_state.get(self.cursor_field) or self._start_date
        params[self.cursor_field] = f"gt:{max(cursor_value, self._start_date)}"
        return params

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state object
        and returning an updated state object.
        """
        latest_state = latest_record.get(self.cursor_field)
        current_state = current_stream_state.get(self.cursor_field) or latest_state
        if current_state:
            return {self.cursor_field: max(latest_state, current_state)}
        return {}


class CustomersCart(IncrementalCartStream):
    """
    TODO: Placeholder for documentation links
    """

    data_field = "customers"

    def path(self, **kwargs) -> str:
        return "customers"


class Orders(IncrementalCartStream):
    """
    TODO: Placeholder for documentation links
    """

    data_field = "orders"

    def path(self, **kwargs) -> str:
        return "orders"


class OrderPayments(IncrementalCartStream):
    """
    TODO: Placeholder for documentation links
    """

    data_field = "payments"

    def path(self, **kwargs) -> str:
        return "order_payments"


class Products(IncrementalCartStream):
    """
    TODO: Placeholder for documentation links
    """

    data_field = "products"

    def path(self, **kwargs) -> str:
        return "products"
