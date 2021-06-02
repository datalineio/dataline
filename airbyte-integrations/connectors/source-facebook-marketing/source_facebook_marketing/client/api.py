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


import time
from abc import ABC, abstractmethod
from functools import partial
from typing import Any, Callable, Iterable, Iterator, Mapping, MutableMapping, Sequence

import backoff
import pendulum as pendulum

# FIXME (Eugene K): use standard logger
from airbyte_cdk.entrypoint import logger
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.api import FacebookAdsApiBatch, FacebookRequest, FacebookResponse
from facebook_business.exceptions import FacebookBadObjectError, FacebookRequestError

from .common import FacebookAPIException, JobTimeoutException, batch, deep_merge, retry_pattern

backoff_policy = retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)


class StreamAPI(ABC):
    result_return_limit = 100

    enable_deleted = False
    split_deleted_filter = True
    entity_prefix = None

    @property
    def name(self):
        """Name of the stream"""
        stream_name = self.__class__.__name__
        if stream_name.endswith("API"):
            stream_name = stream_name[:-3]
        return stream_name

    def __init__(self, api, include_deleted=False, **kwargs):
        super().__init__(**kwargs)
        self._api = api
        self._include_deleted = include_deleted if self.enable_deleted else False

    def _entity_status_filters(self) -> Iterator:
        """We split single request into multiple requests with few delivery_info values,
        Note: this logic originally taken from singer tap implementation, my guess is that when we
        query entities with all possible delivery_info values the API response time will be slow.
        """
        filt_values = [
            "active",
            "archived",
            "completed",
            "limited",
            "not_delivering",
            "deleted",
            "not_published",
            "pending_review",
            "permanently_deleted",
            "recently_completed",
            "recently_rejected",
            "rejected",
            "scheduled",
            "inactive",
        ]

        sub_list_length = 3 if self.split_deleted_filter else len(filt_values)
        for i in range(0, len(filt_values), sub_list_length):
            yield {
                "filtering": [
                    {"field": f"{self.entity_prefix}.delivery_info", "operator": "IN", "value": filt_values[i : i + sub_list_length]},
                ],
            }

    def read(self, getter: Callable, params: Mapping[str, Any] = None) -> Iterator:
        """Read entities using provided callable"""
        params = params or {}
        if self._include_deleted:
            for status_filter in self._entity_status_filters():
                yield from getter(params=self._build_params(deep_merge(params, status_filter)))
        else:
            yield from getter(params=self._build_params(params))

    def _build_params(self, params: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = params or {}
        return {"limit": self.result_return_limit, **params}

    @abstractmethod
    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        """Iterate over entities"""


class IncrementalStreamAPI(StreamAPI, ABC):
    buffer_days = -1

    @property
    @abstractmethod
    def state_pk(self):
        """Name of the field associated with the state"""

    @property
    def state(self):
        return {
            self.state_pk: str(self._state),
            "include_deleted": self._include_deleted,
        }

    @state.setter
    def state(self, value):
        potentially_new_records_in_the_past = self._include_deleted and not value.get("include_deleted", False)
        if potentially_new_records_in_the_past:
            logger.info(f"Ignoring bookmark for {self.name} because of enabled `include_deleted` option")
        else:
            self._state = pendulum.parse(value[self.state_pk])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._state = None

    def _build_params(self, params: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        """Build complete params for request"""
        params = params or {}
        return deep_merge(super()._build_params(params), self._state_filter())

    def _state_filter(self):
        """Additional filters associated with state if any set"""
        if self._state:
            return {
                "filtering": [
                    {
                        "field": f"{self.entity_prefix}.{self.state_pk}",
                        "operator": "GREATER_THAN",
                        "value": self._state.int_timestamp,
                    },
                ],
            }

        return {}

    def read(self, getter: Callable, params: Mapping[str, Any] = None) -> Iterator:
        """Apply state filter to set of records, update cursor(state) if necessary in the end"""
        params = params or {}
        latest_cursor = None
        for record in super().read(getter, params):
            cursor = pendulum.parse(record[self.state_pk])
            if self._state and self._state.subtract(days=self.buffer_days + 1) >= cursor:
                continue
            latest_cursor = max(cursor, latest_cursor) if latest_cursor else cursor
            yield record

        if latest_cursor:
            logger.info(f"Advancing bookmark for {self.name} stream from {self._state} to {latest_cursor}")
            self._state = max(latest_cursor, self._state) if self._state else latest_cursor


class AdCreativeAPI(StreamAPI):
    """AdCreative is not an iterable stream as it uses the batch endpoint
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup/adcreatives/
    """

    entity_prefix = "adcreative"
    batch_size = 50

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        # get pending requests for each creative
        requests = [creative.api_get(fields=fields, pending=True) for creative in self.read(getter=self._get_creatives)]
        for requests_batch in batch(requests, size=self.batch_size):
            yield from self.execute_in_batch(requests_batch)

    @backoff_policy
    def execute_in_batch(self, requests: Iterable[FacebookRequest]) -> Sequence[MutableMapping[str, Any]]:
        records = []

        def success(response: FacebookResponse):
            records.append(response.json())

        def failure(response: FacebookResponse):
            raise response.error()

        api_batch: FacebookAdsApiBatch = self._api._api.new_batch()
        for request in requests:
            api_batch.add_request(request, success=success, failure=failure)
        api_batch.execute()

        return records

    @backoff_policy
    def _get_creatives(self, params: Mapping[str, Any]) -> Iterator:
        return self._api.account.get_ad_creatives(params=params)


class AdsAPI(IncrementalStreamAPI):
    """ doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup """

    entity_prefix = "ad"
    enable_deleted = True
    state_pk = "updated_time"

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        for record in self.read(getter=self._get_ads):
            yield self._extend_record(record, fields=fields)

    @backoff_policy
    def _get_ads(self, params: Mapping[str, Any]):
        """
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """
        return self._api.account.get_ads(params=params, fields=[self.state_pk])

    @backoff_policy
    def _extend_record(self, ad: Ad, fields: Sequence[str]):
        return ad.api_get(fields=fields).export_all_data()


class AdSetsAPI(IncrementalStreamAPI):
    """ doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign """

    entity_prefix = "adset"
    enable_deleted = True
    state_pk = "updated_time"

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        for adset in self.read(getter=self._get_ad_sets):
            yield self._extend_record(adset, fields=fields)

    @backoff_policy
    def _get_ad_sets(self, params):
        """
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """
        return self._api.account.get_ad_sets(params={**params, **self._state_filter()}, fields=[self.state_pk])

    @backoff_policy
    def _extend_record(self, ad_set, fields):
        return ad_set.api_get(fields=fields).export_all_data()


class CampaignAPI(IncrementalStreamAPI):
    entity_prefix = "campaign"
    enable_deleted = True
    state_pk = "updated_time"

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        """Read available campaigns"""
        pull_ads = "ads" in fields
        fields = [k for k in fields if k != "ads"]
        for campaign in self.read(getter=self._get_campaigns):
            yield self._extend_record(campaign, fields=fields, pull_ads=pull_ads)

    @backoff_policy
    def _extend_record(self, campaign, fields, pull_ads):
        """Request additional attributes for campaign"""
        campaign_out = campaign.api_get(fields=fields).export_all_data()
        if pull_ads:
            campaign_out["ads"] = {"data": []}
            ids = [ad["id"] for ad in campaign.get_ads()]
            for ad_id in ids:
                campaign_out["ads"]["data"].append({"id": ad_id})
        return campaign_out

    @backoff_policy
    def _get_campaigns(self, params):
        """Separate method to request list of campaigns
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """
        return self._api.account.get_campaigns(params={**params, **self._state_filter()}, fields=[self.state_pk])


class AdsInsightAPI(IncrementalStreamAPI):
    entity_prefix = ""
    state_pk = "date_start"

    ALL_ACTION_ATTRIBUTION_WINDOWS = [
        "1d_click",
        "7d_click",
        "28d_click",
        "1d_view",
        "7d_view",
        "28d_view",
    ]

    ALL_ACTION_BREAKDOWNS = [
        "action_type",
        "action_target_id",
        "action_destination",
    ]

    # Some automatic fields (primary-keys) cannot be used as 'fields' query params.
    INVALID_INSIGHT_FIELDS = [
        "frequency_value",
        "device_platform",
        "hourly_stats_aggregated_by_audience_time_zone",
        "place_page_id",
        "product_id",
        "activity_recency",
        "hourly_stats_aggregated_by_advertiser_time_zone",
        "ad_format_asset",
        "impression_device",
        "publisher_platform",
        "platform_position",
        "age",
        "gender",
        "country",
        "placement",
        "region",
        "dma",
    ]

    MAX_WAIT_TO_START = pendulum.Interval(minutes=5)
    MAX_WAIT_TO_FINISH = pendulum.Interval(minutes=30)
    MAX_ASYNC_SLEEP = pendulum.Interval(minutes=5)

    action_breakdowns = ALL_ACTION_BREAKDOWNS
    level = "ad"
    action_attribution_windows = ALL_ACTION_ATTRIBUTION_WINDOWS
    time_increment = 1

    def __init__(self, api, start_date, breakdowns=None, buffer_days=28):
        super().__init__(api=api)
        self.start_date = start_date
        self.buffer_days = buffer_days
        self._state = start_date
        self.breakdowns = breakdowns

    @staticmethod
    def _get_job_result(job, **params) -> Iterator:
        for obj in job.get_result():
            yield obj.export_all_data()

    def list(self, fields: Sequence[str] = None) -> Iterator[dict]:
        for params in self._params(fields=fields):
            job = self._run_job_until_completion(params)
            yield from super().read(partial(self._get_job_result, job=job), params)

    @retry_pattern(
        backoff.expo, (FacebookRequestError, JobTimeoutException, FacebookBadObjectError, FacebookAPIException), max_tries=5, factor=4
    )
    def _run_job_until_completion(self, params) -> AdReportRun:
        # TODO parallelize running these jobs
        job = self._get_insights(params)
        logger.info(f"Created AdReportRun: {job} to sync insights with breakdown {self.breakdowns}")
        start_time = pendulum.now()
        sleep_seconds = 2
        while True:
            job = job.api_get()
            job_progress_pct = job["async_percent_completion"]
            logger.info(f"ReportRunId {job['report_run_id']} is {job_progress_pct}% complete")

            if job["async_status"] == "Job Completed":
                return job
            elif job["async_status"] == "Job Failed":
                raise FacebookAPIException(f"Job failed: {job}")

            runtime = pendulum.now() - start_time
            if runtime > self.MAX_WAIT_TO_START and job_progress_pct == 0:
                raise JobTimeoutException(
                    f"AdReportRun {job} did not start after {runtime.in_seconds()} seconds. This is an intermittent error which may be fixed by retrying the job. Aborting."
                )
            elif runtime > self.MAX_WAIT_TO_FINISH:
                raise JobTimeoutException(
                    f"AdReportRun {job} did not finish after {runtime.in_seconds()} seconds. This is an intermittent error which may be fixed by retrying the job. Aborting."
                )
            logger.info(f"Sleeping {sleep_seconds} seconds while waiting for AdReportRun: {job} to complete")
            time.sleep(sleep_seconds)
            if sleep_seconds < self.MAX_ASYNC_SLEEP.in_seconds():
                sleep_seconds *= 2

    def _params(self, fields: Sequence[str] = None) -> Iterator[dict]:
        # Facebook freezes insight data 28 days after it was generated, which means that all data
        # from the past 28 days may have changed since we last emitted it, so we retrieve it again.
        buffered_start_date = self._state.subtract(days=self.buffer_days)
        end_date = pendulum.now()

        fields = list(set(fields) - set(self.INVALID_INSIGHT_FIELDS))

        while buffered_start_date <= end_date:
            yield {
                "level": self.level,
                "action_breakdowns": self.action_breakdowns,
                "breakdowns": self.breakdowns,
                "limit": self.result_return_limit,
                "fields": fields,
                "time_increment": self.time_increment,
                "action_attribution_windows": self.action_attribution_windows,
                "time_ranges": [{"since": buffered_start_date.to_date_string(), "until": buffered_start_date.to_date_string()}],
            }
            buffered_start_date = buffered_start_date.add(days=1)

    @backoff_policy
    def _get_insights(self, params) -> AdReportRun:
        return self._api.account.get_insights(params=params, is_async=True)
