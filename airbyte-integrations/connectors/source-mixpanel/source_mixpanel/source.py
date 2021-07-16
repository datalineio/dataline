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


import base64
import json
import time
from abc import ABC
from datetime import date, datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from urllib.parse import parse_qs, urlparse

import pendulum
import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator, TokenAuthenticator


class MixpanelStream(HttpStream, ABC):
    """
    Formatted API Rate Limit  (https://help.mixpanel.com/hc/en-us/articles/115004602563-Rate-Limits-for-API-Endpoints):
      A maximum of 5 concurrent queries
      400 queries per hour.
    """

    url_base = "https://mixpanel.com/api/2.0/"

    # https://help.mixpanel.com/hc/en-us/articles/115004602563-Rate-Limits-for-Export-API-Endpoints#api-export-endpoint-rate-limits
    reqs_per_hour_limit = 400  # 1 req in 9 secs

    def __init__(
        self,
        authenticator: HttpAuthenticator,
        start_date: Union[date, str] = None,
        end_date: Union[date, str] = None,
        date_window_size: int = 30,  # in days
        attribution_window: int = 0,  # in days
        select_properties_by_default: bool = True,
        **kwargs,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.date_window_size = date_window_size
        self.attribution_window = attribution_window
        self.additional_properties = select_properties_by_default

        super().__init__(authenticator=authenticator)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Define abstract method"""
        return None

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"Accept": "application/json"}

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        try:
            return super()._send_request(request, request_kwargs)
        except requests.exceptions.HTTPError as e:
            error_message = e.response.text
            if error_message:
                self.logger.error(f"Stream {self.name}: {e.response.status_code} {e.response.reason} - {error_message}")
            raise e

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()
        if self.data_field is not None:
            data = json_response.get(self.data_field, [])
        elif isinstance(json_response, list):
            data = json_response
        elif isinstance(json_response, dict):
            data = [json_response]

        for record in data:
            yield record

        # wait for X seconds to match API limitations
        time.sleep(3600 / self.reqs_per_hour_limit)


class IncrementalMixpanelStream(MixpanelStream, ABC):
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, any]:
        current_stream_state = current_stream_state or {}
        current_stream_state: str = current_stream_state.get("date", str(self.start_date))
        latest_record_date: str = latest_record.get(self.cursor_field, str(self.start_date))
        return {"date": max(current_stream_state, latest_record_date)}


class Cohorts(MixpanelStream):
    """Returns all of the cohorts in a given project.
    API Docs: https://developer.mixpanel.com/reference/cohorts
    Endpoint: https://mixpanel.com/api/2.0/cohorts/list

    [{
        "count": 150
        "is_visible": 1
        "description": "This cohort is visible, has an id = 1000, and currently has 150 users."
        "created": "2019-03-19 23:49:51"
        "project_id": 1
        "id": 1000
        "name": "Cohort One"
    },
    {
        "count": 25
        "is_visible": 0
        "description": "This cohort isn't visible, has an id = 2000, and currently has 25 users."
        "created": "2019-04-02 23:22:01"
        "project_id": 1
        "id": 2000
        "name": "Cohort Two"
    }
    ]

    """

    data_field = None
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "cohorts/list"


class FunnelsList(MixpanelStream):
    """List all funnels
    API Docs: https://developer.mixpanel.com/reference/funnels#funnels-list-saved
    Endpoint: https://mixpanel.com/api/2.0/funnels/list
    """

    primary_key = "funnel_id"
    data_field = None

    def path(self, **kwargs) -> str:
        return "funnels/list"


class DateSlicesMixin:
    def stream_slices(
        self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        date_slices = []

        # use the latest date between self.start_date and stream_state
        start_date = self.start_date
        if stream_state:
            # Remove time part from state because API accept 'from_date' param in date format only ('YYYY-MM-DD')
            # It also means that sync returns duplicated entries for the date from the state (date range is inclusive)
            stream_state_date = datetime.fromisoformat(stream_state["date"]).date()
            start_date = max(start_date, stream_state_date)

        # use the lowest date between start_date and self.end_date, otherwise API fails if start_date is in future
        start_date = min(start_date, self.end_date)

        # move start_date back <attribution_window> days to sync data since that time as well
        start_date = start_date - timedelta(days=self.attribution_window)

        while start_date <= self.end_date:
            end_date = start_date + timedelta(days=self.date_window_size - 1)  # -1 is needed because dates are inclusive
            date_slices.append(
                {
                    "start_date": str(start_date),
                    "end_date": str(min(end_date, self.end_date)),
                }
            )
            # add 1 additional day because date range is inclusive
            start_date = end_date + timedelta(days=1)

        # reset reqs_per_hour_limit if we expect less requests (1 req per stream) than it is allowed by API reqs_per_hour_limit
        if len(date_slices) < self.reqs_per_hour_limit:
            self.reqs_per_hour_limit = 3600  # 1 query per sec

        return date_slices

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "from_date": stream_slice["start_date"],
            "to_date": stream_slice["end_date"],
        }


class Funnels(DateSlicesMixin, IncrementalMixpanelStream):
    """List the funnels for a given date range.
    API Docs: https://developer.mixpanel.com/reference/funnels#funnels-query
    Endpoint: https://mixpanel.com/api/2.0/funnels
    """

    primary_key = ["funnel_id", "date"]
    data_field = "data"
    cursor_field = "date"
    min_date = "90"  # days

    def path(self, **kwargs) -> str:
        return "funnels"

    def funnel_slices(self, sync_mode) -> List[dict]:
        funnel_slices = FunnelsList(authenticator=self.authenticator).read_records(sync_mode=sync_mode)
        funnel_slices = list(funnel_slices)  # [{'funnel_id': <funnel_id1>, 'name': <name1>}, {...}]

        # save all funnels in dict(<funnel_id1>:<name1>, ...)
        self.funnels = dict((funnel["funnel_id"], funnel["name"]) for funnel in funnel_slices)

        return funnel_slices

    def stream_slices(
        self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Mapping[str, Any]]]]:
        """Return stream slices which is a combination of all funnel_ids and related date ranges, like:
        stream_slices = [
            {   'funnel_id': funnel_id1_int,
                'funnel_name': 'funnel_name1',
                'start_date': 'start_date_1'
                'end_date': 'end_date_1'
            },
            {   'funnel_id': 'funnel_id1_int',
                'funnel_name': 'funnel_name1',
                'start_date': 'start_date_2'
                'end_date': 'end_date_2'
            }
            ...
            {   'funnel_id': 'funnel_idX_int',
                'funnel_name': 'funnel_nameX',
                'start_date': 'start_date_1'
                'end_date': 'end_date_1'
            }
            ...
        ]

        # NOTE: funnel_id type:
        #    - int in funnel_slice
        #    - str in stream_state
        """
        stream_state = stream_state or {}

        # One stream slice is a combination of all funnel_slices and date_slices
        stream_slices = []
        funnel_slices = self.funnel_slices(sync_mode)
        # date_slices = self.date_slices(sync_mode, cursor_field=cursor_field, stream_state=stream_state)
        for funnel_slice in funnel_slices:
            # get single funnel state
            funnel_id = str(funnel_slice["funnel_id"])
            funnel_state = stream_state.get(funnel_id)
            date_slices = super().stream_slices(sync_mode, cursor_field=cursor_field, stream_state=funnel_state)
            for date_slice in date_slices:
                stream_slices.append({**funnel_slice, **date_slice})

        # reset reqs_per_hour_limit if we expect less requests (1 req per stream) than it is allowed by API reqs_per_hour_limit
        if len(stream_slices) < self.reqs_per_hour_limit:
            self.reqs_per_hour_limit = 3600  # queries per hour (1 query per sec)
        return stream_slices

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        # NOTE: funnel_id type:
        #    - int in stream_slice
        #    - str in stream_state
        funnel_id = str(stream_slice["funnel_id"])
        funnel_state = stream_state.get(funnel_id)

        params = super().request_params(funnel_state, stream_slice, next_page_token)
        params["funnel_id"] = stream_slice["funnel_id"]
        params["unit"] = "day"
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        response.json() example:
        {
            "meta": {
                "dates": [
                    "2016-09-12"
                    "2016-09-19"
                    "2016-09-26"
                ]
            }
            "data": {
                "2016-09-12": {
                    "steps": [...]
                    "analysis": {
                        "completion": 20524
                        "starting_amount": 32688
                        "steps": 2
                        "worst": 1
                    }
                }
                "2016-09-19": {
                    ...
                }
            }
        }
        :return an iterable containing each record in the response
        """
        # extract 'funnel_id' from internal request object
        query = urlparse(response.request.path_url).query
        params = parse_qs(query)
        funnel_id = int(params["funnel_id"][0])

        # read and transform records
        records = response.json().get(self.data_field, {})
        for date_entry in records:
            # for each record add funnel_id, name
            yield {
                "funnel_id": funnel_id,
                "name": self.funnels[funnel_id],
                "date": date_entry,
                **records[date_entry],
            }

    def get_updated_state(
        self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]
    ) -> Mapping[str, Mapping[str, str]]:
        """Update existing stream state for particular funnel_id
        stream_state = {
            'funnel_id1_str' = {'date': 'datetime_string1'},
            'funnel_id2_str' = {'date': 'datetime_string2'},
             ...
            'funnel_idX_str' = {'date': 'datetime_stringX'},
        }
        NOTE: funnel_id1 type:
            - int in latest_record
            - str in current_stream_state
        """
        funnel_id: str = str(latest_record["funnel_id"])

        latest_record_date: str = latest_record.get(self.cursor_field, str(self.start_date))
        stream_state_date: str = str(self.start_date)
        if current_stream_state and funnel_id in current_stream_state:
            stream_state_date = current_stream_state[funnel_id]["date"]

        # update existing stream state
        current_stream_state[funnel_id] = {"date": max(latest_record_date, stream_state_date)}

        return current_stream_state


class EngageSchema(MixpanelStream):
    """Engage helper stream for dynamic schema extraction"""

    primary_key = None
    data_field = "results"

    def path(self, **kwargs) -> str:
        return "engage/properties"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        response.json() example:
        {
            "results": {
                "$browser": {
                    "count": 124,
                    "type": "string"
                },
                "$browser_version": {
                    "count": 124,
                    "type": "string"
                },
                ...
                "_some_custom_property": {
                    "count": 124,
                    "type": "string"
                }
            }
        }
        """
        records = response.json().get(self.data_field, {})
        for property_name in records:
            yield {
                "name": property_name,
                "type": records[property_name]["type"],
            }


class Engage(MixpanelStream):
    """Return list of all users
    API Docs: https://developer.mixpanel.com/reference/engage
    Endpoint: https://mixpanel.com/api/2.0/engage
    """

    http_method = "POST"
    data_field = "results"
    primary_key = "distinct_id"
    page_size = 1000  # min 100
    _total = None

    def path(self, **kwargs) -> str:
        return "engage"

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        return {"include_all_users": True}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"page_size": self.page_size}
        if next_page_token:
            params.update(next_page_token)
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        page_number = decoded_response.get("page")
        total = decoded_response.get("total")  # exist only on first page
        if total:
            self._total = total

        if self._total and page_number is not None and self._total > self.page_size * (page_number + 1):
            return {
                "session_id": decoded_response.get("session_id"),
                "page": page_number + 1,
            }
        else:
            self._total = None
            return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        {
            "page": 0
            "page_size": 1000
            "session_id": "1234567890-EXAMPL"
            "status": "ok"
            "total": 1
            "results": [{
                "$distinct_id": "9d35cd7f-3f06-4549-91bf-198ee58bb58a"
                "$properties":{
                    "$browser":"Chrome"
                    "$browser_version":"83.0.4103.116"
                    "$city":"Leeds"
                    "$country_code":"GB"
                    "$region":"Leeds"
                    "$timezone":"Europe/London"
                    "unblocked":"true"
                    "$email":"nadine@asw.com"
                    "$first_name":"Nadine"
                    "$last_name":"Burzler"
                    "$name":"Nadine Burzler"
                    "id":"632540fa-d1af-4535-bc52-e331955d363e"
                    "$last_seen":"2020-06-28T12:12:31"
                    }
                },{
                ...
                }
            ]

        }
        """
        records = response.json().get(self.data_field, {})
        for record in records:
            item = {"distinct_id": record["$distinct_id"]}
            properties = record["$properties"]
            for property_name in properties:
                this_property_name = property_name
                if property_name.startswith("$"):
                    # Just remove leading '$' for 'reserved' mixpanel properties name, example:
                    # from API: '$browser'
                    # to stream: 'browser'
                    this_property_name = this_property_name[1:]
                item[this_property_name] = properties[property_name]
            yield item

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        :return: A dict of the JSON schema representing this stream.

        The default implementation of this method looks for a JSONSchema file with the same name as this stream's "name" property.
        Override as needed.
        """
        schema = super().get_json_schema()

        # Set whether to allow additional properties for engage and export endpoints
        # Event and Engage properties are dynamic and depend on the properties provided on upload,
        #   when the Event or Engage (user/person) was created.
        schema["additionalProperties"] = self.additional_properties

        types = {
            "boolean": {"type": ["null", "boolean"]},
            "number": {"type": ["null", "number"], "multipleOf": 1e-20},
            "datetime": {"type": ["null", "string"], "format": "date-time"},
            "object": {"type": ["null", "object"], "additionalProperties": True},
            "list": {"type": ["null", "array"], "required": False, "items": {}},
            "string": {"type": ["null", "string"]},
        }

        # read existing Engage schema from API
        schema_properties = EngageSchema(authenticator=self.authenticator).read_records(sync_mode=SyncMode.full_refresh)
        for property_entry in schema_properties:
            property_name: str = property_entry["name"]
            property_type: str = property_entry["type"]
            if property_name.startswith("$"):
                # Just remove leading '$' for 'reserved' mixpanel properties name, example:
                # from API: '$browser'
                # to stream: 'browser'
                property_name = property_name[1:]
            schema["properties"][property_name] = types.get(property_type, {"type": ["null", "string"]})

        return schema


class CohortMembers(Engage):
    """Return list of users grouped by cohort"""

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        # example: {"filter_by_cohort": {"id": 1343181}}
        return {"filter_by_cohort": stream_slice}

    def stream_slices(
        self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_slices = []
        cohorts = Cohorts(authenticator=self.authenticator).read_records(sync_mode=sync_mode)
        for cohort in cohorts:
            stream_slices.append({"id": cohort["id"]})

        return stream_slices


class Annotations(DateSlicesMixin, MixpanelStream):
    """List the annotations for a given date range.
    API Docs: https://developer.mixpanel.com/reference/annotations
    Endpoint: https://mixpanel.com/api/2.0/annotations

    Output example:
    {
        "annotations": [{
                "id": 640999
                "project_id": 2117889
                "date": "2021-06-16 00:00:00" <-- PLEASE READ A NOTE
                "description": "Looks good"
            }, {...}
        ]
    }

    NOTE: annotation date - is the date for which annotation was added, this is not the date when annotation was added
    That's why stream does not support incremental sync.
    """

    data_field = "annotations"
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "annotations"


class Revenue(DateSlicesMixin, IncrementalMixpanelStream):
    """Get data Revenue.
    API Docs: no docs! build based on singer source
    Endpoint: https://mixpanel.com/api/2.0/engage/revenue
    """

    data_field = "results"
    primary_key = "date"
    cursor_field = "date"

    def path(self, **kwargs) -> str:
        return "engage/revenue"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        response.json() example:
        {
            'computed_at': '2021-07-03T12:43:48.889421+00:00',
            'results': {
                '$overall': {       <-- should be skipped
                    'amount': 0.0,
                    'count': 124,
                    'paid_count': 0
                },
                '2021-06-01': {
                    'amount': 0.0,
                    'count': 124,
                    'paid_count': 0
                },
                '2021-06-02': {
                    'amount': 0.0,
                    'count': 124,
                    'paid_count': 0
                },
                ...
            },
            'session_id': '162...',
            'status': 'ok'
        }
        :return an iterable containing each record in the response
        """
        records = response.json().get(self.data_field, {})
        for date_entry in records:
            if date_entry != "$overall":
                yield {"date": date_entry, **records[date_entry]}


class ExportSchema(MixpanelStream):
    """Export helper stream for dynamic schema extraction"""

    primary_key = None
    data_field = None

    def path(self, **kwargs) -> str:
        return "events/properties/top"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[str]:
        """
        response.json() example:
        {
            "$browser": {
                "count": 6
            },
            "$browser_version": {
                "count": 6
            },
            "$current_url": {
                "count": 6
            },
            "mp_lib": {
                "count": 6
            },
            "noninteraction": {
                "count": 6
            },
            "$event_name": {
                "count": 6
            },
            "$duration_s": {},
            "$event_count": {},
            "$origin_end": {},
            "$origin_start": {}
        }
        """
        records = response.json()
        for property_name in records:
            yield property_name


class Export(DateSlicesMixin, IncrementalMixpanelStream):
    """Export event data as it is received and stored within Mixpanel, complete with all event properties
     (including distinct_id) and the exact timestamp the event was fired.

    API Docs: https://developer.mixpanel.com/reference/export
    Endpoint: https://data.mixpanel.com/api/2.0/export

    Raw Export API Rate Limit (https://help.mixpanel.com/hc/en-us/articles/115004602563-Rate-Limits-for-API-Endpoints):
     A maximum of 100 concurrent queries,
     3 queries per second and 60 queries per hour.
    """

    primary_key = None
    cursor_field = "time"
    reqs_per_hour_limit = 60  # 1 query per minute

    url_base = "https://data.mixpanel.com/api/2.0/"

    def path(self, **kwargs) -> str:
        return "export"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """Export API return response.text in JSONL format but each line is a valid JSON object
        Raw item example:
            {
                "event": "Viewed E-commerce Page",
                "properties": {
                    "time": 1623860880,
                    "distinct_id": "1d694fd9-31a5-4b99-9eef-ae63112063ed",
                    "$browser": "Chrome",                                           -> will be renamed to "browser"
                    "$browser_version": "91.0.4472.101",
                    "$current_url": "https://unblockdata.com/solutions/e-commerce/",
                    "$insert_id": "c5eed127-c747-59c8-a5ed-d766f48e39a4",
                    "$mp_api_endpoint": "api.mixpanel.com",
                    "mp_lib": "Segment: analytics-wordpress",
                    "mp_processing_time_ms": 1623886083321,
                    "noninteraction": true
                }
            }
        """

        for record_line in response.text.splitlines():
            record = json.loads(record_line)
            # transform record into flat dict structure
            item = {"event": record["event"]}
            properties = record["properties"]
            for property_name in properties:
                this_property_name = property_name
                if property_name.startswith("$"):
                    # Just remove leading '$' for 'reserved' mixpanel properties name, example:
                    # from API: '$browser'
                    # to stream: 'browser'
                    this_property_name = this_property_name[1:]
                # Convert all values to string (this is default property type)
                # because API does not provide properties type information
                item[this_property_name] = str(properties[property_name])

            # convert timestamp to datetime string
            if item.get("time") and item["time"].isdigit():
                item["time"] = datetime.fromtimestamp(int(item["time"])).isoformat()

            yield item

        # wait for X seconds to meet API limitation
        time.sleep(3600 / self.reqs_per_hour_limit)

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        :return: A dict of the JSON schema representing this stream.

        The default implementation of this method looks for a JSONSchema file with the same name as this stream's "name" property.
        Override as needed.
        """

        schema = super().get_json_schema()

        # Set whether to allow additional properties for engage and export endpoints
        # Event and Engage properties are dynamic and depend on the properties provided on upload,
        #   when the Event or Engage (user/person) was created.
        schema["additionalProperties"] = self.additional_properties

        # read existing Export schema from API
        schema_properties = ExportSchema(authenticator=self.authenticator).read_records(sync_mode=SyncMode.full_refresh)
        for property_entry in schema_properties:
            property_name: str = property_entry
            if property_name.startswith("$"):
                # Just remove leading '$' for 'reserved' mixpanel properties name, example:
                # from API: '$browser'
                # to stream: 'browser'
                property_name = property_name[1:]
            # Schema does not provide exact property type
            # string ONLY for event properties (no other datatypes)
            # Reference: https://help.mixpanel.com/hc/en-us/articles/360001355266-Event-Properties#field-size-character-limits-for-event-properties
            schema["properties"][property_name] = {"type": ["null", "string"]}

        return schema


class TokenAuthenticatorBase64(TokenAuthenticator):
    def __init__(self, token: str, auth_method: str = "Basic", **kwargs):
        token = base64.b64encode(token.encode("utf8")).decode("utf8")
        super().__init__(token=token, auth_method=auth_method, **kwargs)


class SourceMixpanel(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        authenticator = TokenAuthenticatorBase64(token=config["api_secret"])
        try:
            response = requests.request(
                "GET",
                url="https://mixpanel.com/api/2.0/funnels/list",
                headers={
                    "Accept": "application/json",
                    **authenticator.get_auth_header(),
                },
            )

            if response.status_code != 200:
                message = response.json()
                error_message = message.get("error")
                if error_message:
                    return False, error_message
                response.raise_for_status()
        except Exception as e:
            return False, e

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        tzone = pendulum.timezone(config.get("project_timezone", "US/Pacific"))
        now = datetime.now(tzone).date()

        start_date = config.get("start_date")
        if start_date and isinstance(start_date, str):
            start_date = pendulum.parse(config["start_date"]).date()
        year_ago = now - timedelta(days=365)
        # start_date can't be older than 1 year ago
        config["start_date"] = start_date if start_date and start_date >= year_ago else year_ago  # set to 1 year ago by default

        end_date = config.get("end_date")
        if end_date and isinstance(end_date, str):
            end_date = pendulum.parse(end_date).date()
        config["end_date"] = end_date or now  # set to now by default

        AirbyteLogger().log("INFO", f"Using start_date: {config['start_date']}, end_date: {config['end_date']}")

        auth = TokenAuthenticatorBase64(token=config["api_secret"])
        return [
            Annotations(authenticator=auth, **config),
            Cohorts(authenticator=auth, **config),
            CohortMembers(authenticator=auth, **config),
            Engage(authenticator=auth, **config),
            Export(authenticator=auth, **config),
            Funnels(authenticator=auth, **config),
            Revenue(authenticator=auth, **config),
        ]
