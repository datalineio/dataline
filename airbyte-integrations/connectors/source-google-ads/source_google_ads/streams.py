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
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import pendulum
from airbyte_cdk.sources.streams import Stream
from google.ads.googleads.v8.services.services.google_ads_service.pagers import SearchPager
from jsonschema import validate

from .google_ads import GoogleAds


def chunk_date_range(start_date: str, conversion_window: int, field: str, end_date: str = None) -> Iterable[Mapping[str, any]]:
    """
    Passing optional parameter end_date for testing
    Returns a list of the beginning and ending timetsamps of each month between the start date and now.
    The return value is a list of dicts {'date': str} which can be used directly with the Slack API
    """
    intervals = []
    end_date = pendulum.parse(end_date) if end_date else pendulum.now()
    start_date = pendulum.parse(start_date)

    # As in to return some state when state in abnormal
    if start_date > end_date:
        return [{field: start_date.to_date_string()}]

    # applying conversion window
    start_date = start_date.subtract(days=conversion_window)

    # Each stream_slice contains the beginning and ending timestamp for a 24 hour period
    while start_date < end_date:
        intervals.append({field: start_date.to_date_string()})
        start_date = start_date.add(months=1)

    return intervals


class GoogleAdsStream(Stream, ABC):
    def __init__(self, api: GoogleAds):
        self.google_ads_client = api

    def get_query(self, stream_slice: Mapping[str, Any]) -> str:
        query = GoogleAds.convert_schema_into_query(schema=self.get_json_schema(), report_name=self.name)
        return query

    def parse_response(self, response: SearchPager) -> Iterable[Mapping]:
        for result in response:
            record = self.google_ads_client.parse_single_result(self.get_json_schema(), result)
            validate(instance=record, schema=self.get_json_schema())
            yield record

    def read_records(self, sync_mode, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        response = self.google_ads_client.send_request(self.get_query(stream_slice))
        yield from self.parse_response(response)


class IncrementalGoogleAdsStream(GoogleAdsStream, ABC):
    def __init__(self, start_date: str, conversion_window_days: int, **kwargs):
        self.conversion_window_days = conversion_window_days
        self._start_date = start_date
        super().__init__(**kwargs)

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        stream_state = stream_state or {}
        start_date = stream_state.get(self.cursor_field) or self._start_date

        return chunk_date_range(start_date=start_date, conversion_window=self.conversion_window_days, field=self.cursor_field)

    @staticmethod
    def get_date_params(stream_slice: Mapping[str, Any], cursor_field: str, end_date: pendulum.datetime = None):
        end_date = end_date or pendulum.yesterday()
        start_date = pendulum.parse(stream_slice.get(cursor_field))
        if start_date > pendulum.now():
            return start_date.to_date_string(), start_date.add(days=1).to_date_string()

        end_date = min(end_date, pendulum.parse(stream_slice.get(cursor_field)).add(months=1))
        return start_date.add(days=1).to_date_string(), end_date.to_date_string()

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        current_stream_state = current_stream_state or {}

        # When state is none return date from latest record
        if current_stream_state.get(self.cursor_field) is None:
            current_stream_state[self.cursor_field] = latest_record[self.cursor_field]

            return current_stream_state

        date_in_current_stream = pendulum.parse(current_stream_state.get(self.cursor_field))
        date_in_latest_record = pendulum.parse(latest_record[self.cursor_field])

        current_stream_state[self.cursor_field] = (max(date_in_current_stream, date_in_latest_record)).to_date_string()

        return current_stream_state

    def get_query(self, stream_slice: Mapping[str, Any] = None) -> str:
        start_date, end_date = self.get_date_params(stream_slice, self.cursor_field)
        query = GoogleAds.convert_schema_into_query(
            schema=self.get_json_schema(), report_name=self.name, from_date=start_date, to_date=end_date, cursor_field=self.cursor_field
        )
        return query


class AdGroupAdReport(IncrementalGoogleAdsStream):
    cursor_field = "segments.date"
    primary_key = None


class Accounts(GoogleAdsStream):
    primary_key = "customer.id"


class Campaigns(GoogleAdsStream):
    primary_key = "campaign.id"


class AdGroups(GoogleAdsStream):
    primary_key = "ad_group.id"


class AdGroupAds(GoogleAdsStream):
    primary_key = "ad_group_ad.ad.id"
