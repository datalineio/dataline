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
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import pendulum
import requests
from airbyte_cdk.sources.streams.http import HttpStream

PIPEDRIVE_URL_BASE = "https://api.pipedrive.com/v1/"


class PipedriveStream(HttpStream, ABC):
    url_base = PIPEDRIVE_URL_BASE
    primary_key = "id"
    data_field = "data"
    page_size = 50

    def __init__(self, api_token: str, replication_start_date: pendulum.datetime = None, **kwargs):
        super().__init__(**kwargs)
        self._api_token = api_token
        self._replication_start_date = replication_start_date

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        if self._replication_start_date:
            return "update_time"
        return []

    def path(self, **kwargs) -> str:
        if self._replication_start_date:
            return "recents"

        class_name = self.__class__.__name__
        return f"{class_name[0].lower()}{class_name[1:]}"

    @property
    def path_param(self):
        return self.name[:-1]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query
                the next page in the response.
                If there are no more pages in the result, return None.
        """
        pagination_data = response.json().get("additional_data", {}).get("pagination", {})
        if pagination_data.get("more_items_in_collection") and pagination_data.get("start") is not None:
            start = pagination_data.get("start") + self.page_size
            return {"start": start}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        next_page_token = next_page_token or {}
        params = {"api_token": self._api_token, "limit": self.page_size, **next_page_token}

        replication_start_date = self._replication_start_date
        if replication_start_date:
            if stream_state.get(self.cursor_field):
                replication_start_date = max(pendulum.parse(stream_state[self.cursor_field]), replication_start_date)

            params.update(
                {
                    "items": self.path_param,
                    "since_timestamp": replication_start_date.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        records = response.json().get(self.data_field) or []
        for record in records:
            if record.get(self.data_field):
                yield record.get(self.data_field)
            else:
                yield record

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state object
        and returning an updated state object.
        """
        latest_benchmark = latest_record[self.cursor_field]
        if current_stream_state.get(self.cursor_field):
            return {self.cursor_field: max(latest_benchmark, current_stream_state[self.cursor_field])}
        return {self.cursor_field: latest_benchmark}


class Deals(PipedriveStream):
    """
    API docs: https://developers.pipedrive.com/docs/api/v1/Deals#getDeals,
    retrieved by https://developers.pipedrive.com/docs/api/v1/Recents#getRecents
    """


class Leads(PipedriveStream):
    """https://developers.pipedrive.com/docs/api/v1/Leads#getLeads"""


class Activities(PipedriveStream):
    """
    API docs: https://developers.pipedrive.com/docs/api/v1/Activities#getActivities,
    retrieved by https://developers.pipedrive.com/docs/api/v1/Recents#getRecents
    """

    path_param = "activity"


class ActivityFields(PipedriveStream):
    """https://developers.pipedrive.com/docs/api/v1/ActivityFields#getActivityFields"""


class Persons(PipedriveStream):
    """
    API docs: https://developers.pipedrive.com/docs/api/v1/Persons#getPersons,
    retrieved by https://developers.pipedrive.com/docs/api/v1/Recents#getRecents
    """


class Pipelines(PipedriveStream):
    """
    API docs: https://developers.pipedrive.com/docs/api/v1/Pipelines#getPipelines,
    retrieved by https://developers.pipedrive.com/docs/api/v1/Recents#getRecents
    """


class Stages(PipedriveStream):
    """
    API docs: https://developers.pipedrive.com/docs/api/v1/Stages#getStages,
    retrieved by https://developers.pipedrive.com/docs/api/v1/Recents#getRecents
    """


class Users(PipedriveStream):
    """
    API docs: https://developers.pipedrive.com/docs/api/v1/Users#getUsers,
    retrieved by https://developers.pipedrive.com/docs/api/v1/Recents#getRecents
    """

    cursor_field = "modified"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        record_gen = super().parse_response(response=response, **kwargs)
        for records in record_gen:
            yield from records
