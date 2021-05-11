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


from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import pendulum
import requests
from airbyte_cdk.sources.streams.http import HttpStream


class Stream(HttpStream):
    url_base = "https://www.zopim.com/api/v2/"
    primary_key = None

    data_field = None
    limit = 100

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        delay_time = response.headers.get("Retry-After")
        if delay_time:
            return int(delay_time)

    def path(self, **kwargs) -> str:
        return self.name

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return {}

    def request_params(
        self, stream_state: Mapping[str, Any], next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {"limit": self.limit}
        if next_page_token:
            params.update(next_page_token)

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_data = response.json()
        stream_data = self.get_stream_data(response_data)

        yield from stream_data

    def get_stream_data(self, response_data: Any) -> List[dict]:
        if self.data_field:
            response_data = response_data.get(self.data_field, [])

        if isinstance(response_data, list):
            return response_data
        elif isinstance(response_data, dict):
            return [response_data]
        else:
            raise Exception(f"Unsupported type of response data for stream {self.name}")


class BaseIncrementalStream(Stream, ABC):
    @property
    @abstractmethod
    def cursor_field(self) -> str:
        """
        Defining a cursor field indicates that a stream is incremental, so any incremental stream must extend this class
        and define a cursor field.
        """

    @abstractmethod
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state object
        and returning an updated state object.
        """

    @staticmethod
    def _field_to_datetime(value: Union[int, str]) -> pendulum.datetime:
        if isinstance(value, int):
            value = pendulum.from_timestamp(value / 1000.0)
        elif isinstance(value, str):
            value = pendulum.parse(value)
        else:
            raise ValueError(f"Unsupported type of datetime field {type(value)}")
        return value


class TimeIncrementalStream(BaseIncrementalStream, ABC):
    def __init__(self, start_date, **kwargs):
        super().__init__(**kwargs)
        self._start_date = pendulum.parse(start_date)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_data = response.json()
        if response_data["count"] == self.limit:
            return {"start_time": response_data["end_time"]}

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_benchmark = self._field_to_datetime(latest_record[self.cursor_field])
        if current_stream_state.get(self.cursor_field):
            return {self.cursor_field: str(max(latest_benchmark, self._field_to_datetime(current_stream_state[self.cursor_field])))}
        return {self.cursor_field: str(latest_benchmark)}

    def request_params(
        self, stream_state: Mapping[str, Any], next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, next_page_token=next_page_token)
        if next_page_token:
            params.update(next_page_token)
        else:
            start_datetime = self._start_date
            if stream_state.get(self.cursor_field):
                start_datetime = pendulum.parse(stream_state[self.cursor_field])

            params.update({"start_time": int(start_datetime.timestamp())})

        params.update({"fields": f"{self.name}(*)"})
        return params

    def path(self, **kwargs) -> str:
        return f"incremental/{self.name}"


class IdIncrementalStream(BaseIncrementalStream):
    cursor_field = "id"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_benchmark = latest_record[self.cursor_field]
        if current_stream_state.get(self.cursor_field):
            return {self.cursor_field: max(latest_benchmark, current_stream_state[self.cursor_field])}
        return {self.cursor_field: latest_benchmark}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        stream_data = self.get_stream_data(response.json())
        if len(stream_data) == self.limit:
            last_object_id = stream_data[-1]["id"]
            return {"since_id": last_object_id}

    def request_params(
        self, stream_state: Mapping[str, Any], next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, next_page_token=next_page_token)

        if next_page_token:
            params.update(next_page_token)
        elif stream_state.get(self.cursor_field):
            params.update({"since_id": stream_state[self.cursor_field]})

        return params


class Agents(IdIncrementalStream):
    """
    Agents Stream: https://developer.zendesk.com/rest_api/docs/chat/agents#list-agents
    """


class AgentTimelines(TimeIncrementalStream):
    """
    Agent Timelines Stream: https://developer.zendesk.com/rest_api/docs/chat/incremental_export#incremental-agent-timeline-export
    """

    cursor_field = "start_time"
    data_field = "agent_timeline"
    name = "agent_timeline"

    def request_params(self, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(**kwargs)
        if not kwargs.get("next_page_token"):
            params["start_time"] = params["start_time"] * 1000000
        return params


class Accounts(Stream):
    """
    Accounts Stream: https://developer.zendesk.com/rest_api/docs/chat/accounts#show-account
    """

    def path(self, **kwargs) -> str:
        return "account"


class Chats(Stream):
    """
    Chats Stream: https://developer.zendesk.com/rest_api/docs/chat/chats#list-chats
    """

    data_field = "chats"


class Shortcuts(Stream):
    """
    Shortcuts Stream: https://developer.zendesk.com/rest_api/docs/chat/shortcuts#list-shortcuts
    """


class Triggers(Stream):
    """
    Triggers Stream: https://developer.zendesk.com/rest_api/docs/chat/triggers#list-triggers
    """


class Bans(IdIncrementalStream):
    """
    Bans Stream: https://developer.zendesk.com/rest_api/docs/chat/bans#list-bans
    """

    def get_stream_data(self, response_data) -> List[dict]:
        bans = response_data["ip_address"] + response_data["visitor"]
        bans = sorted(bans, key=lambda x: pendulum.parse(x["created_at"]))
        return bans


class Departments(Stream):
    """
    Departments Stream: https://developer.zendesk.com/rest_api/docs/chat/departments#list-departments
    """


class Goals(Stream):
    """
    Goals Stream: https://developer.zendesk.com/rest_api/docs/chat/goals#list-goals
    """


class Skills(Stream):
    """
    Skills Stream: https://developer.zendesk.com/rest_api/docs/chat/skills#list-skills
    """


class Roles(Stream):
    """
    Roles Stream: https://developer.zendesk.com/rest_api/docs/chat/roles#list-roles
    """


class RoutingSettings(Stream):
    """
    Routing Settings Stream: https://developer.zendesk.com/rest_api/docs/chat/routing_settings#show-account-routing-settings
    """

    name = "routing_settings"
    data_field = "data"

    def path(self, **kwargs) -> str:
        return "routing_settings/account"
