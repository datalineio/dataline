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

from typing import Any, Iterable, List, Mapping, Tuple, Union

import requests


class KvDbClient:
    base_url = "https://kvdb.io"
    PAGE_SIZE = 1000

    def __init__(self, bucket_id: str, secret_key: str = None):
        self.secret_key = secret_key
        self.bucket_id = bucket_id

    def write(self, key: str, value: Mapping[str, Any]):
        return self.batch_write([(key, value)])

    def batch_write(self, keys_and_values: List[Tuple[str, Mapping[str, Any]]]):
        """
        https://kvdb.io/docs/api/#execute-transaction
        """
        request_body = {"txn": [{"set": key, "value": value} for key, value in keys_and_values]}
        return self._request("POST", json=request_body)

    def list_keys(self, list_values: bool = False, prefix: str = None) -> Iterable[Union[str, List]]:
        """
        https://kvdb.io/docs/api/#list-keys
        """
        # TODO handle rate limiting
        pagination_complete = False
        offset = 0

        while not pagination_complete:
            response = self._request(
                "GET",
                params={
                    "limit": self.PAGE_SIZE,
                    "skip": offset,
                    "format": "json",
                    "prefix": prefix or "",
                    "values": "true" if list_values else "false",
                },
                endpoint="/",  # the "list" endpoint doesn't work without adding a trailing slash to the URL
            )

            response_json = response.json()
            yield from response_json

            pagination_complete = len(response_json) < self.PAGE_SIZE
            offset += self.PAGE_SIZE

    def delete(self, key: Union[str, List[str]]):
        """
        https://kvdb.io/docs/api/#execute-transaction
        """
        key_list = key if isinstance(key, List) else [key]
        request_body = {"txn": [{"delete": k} for k in key_list]}
        return self._request("POST", json=request_body)

    def _get_base_url(self) -> str:
        return f"{self.base_url}/{self.bucket_id}"

    def _get_auth_headers(self) -> Mapping[str, Any]:
        return {"Authorization": f"Bearer {self.secret_key}"} if self.secret_key else {}

    def _request(
        self, http_method: str, endpoint: str = None, params: Mapping[str, Any] = None, json: Mapping[str, Any] = None
    ) -> requests.Response:
        url = self._get_base_url() + (endpoint or "")
        headers = {"Accept": "application/json", **self._get_auth_headers()}

        response = requests.request(method=http_method, params=params, url=url, headers=headers, json=json)

        response.raise_for_status()
        return response
