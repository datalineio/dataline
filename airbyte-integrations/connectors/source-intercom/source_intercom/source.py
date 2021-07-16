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
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from urllib.parse import parse_qsl, urlparse

import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator, TokenAuthenticator


class IntercomStream(HttpStream, ABC):
    url_base = "https://api.intercom.io/"

    # https://developers.intercom.com/intercom-api-reference/reference#rate-limiting
    queries_per_hour = 1000  # 1000 queries per hour == 1 req in 3,6 secs

    primary_key = "id"
    data_fields = ["data"]

    def __init__(
        self,
        authenticator: HttpAuthenticator,
        start_date: str = None,
        **kwargs,
    ):
        self.start_date = start_date

        super().__init__(authenticator=authenticator)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Abstract method of HttpStream - should be overwritten.
        Returning None means there are no more pages to read in response.
        """

        next_page = response.links.get("next", None)

        if next_page:
            return dict(parse_qsl(urlparse(next_page.get("url")).query))
        else:
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
                self.logger.error(f"Stream {self.name}: {e.response.status_code} " f"{e.response.reason} - {error_message}")
            raise e

    def get_data(self, response: requests.Response) -> List:
        data = response.json()

        for data_field in self.data_fields:
            if data and isinstance(data, dict):
                data = data.get(data_field, [])

        if isinstance(data, list):
            data = data
        elif isinstance(data, dict):
            data = [data]

        return data

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:

        data = self.get_data(response)

        for record in data:
            yield record

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        wait_time = 3600 / self.queries_per_hour  # wait for 3,6 seconds according to API limit
        return wait_time


class IncrementalIntercomStream(IntercomStream, ABC):
    cursor_field = "updated_at"

    def filter_by_state(self, stream_state: Mapping[str, Any] = None, record: Mapping[str, Any] = None) -> Iterable:
        """
        Endpoint does not provide query filtering params, but they provide us
        updated_at field in most cases, so we used that as incremental filtering
        during the slicing.
        """

        if not stream_state or record[self.cursor_field] >= stream_state.get(self.cursor_field):
            yield record

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:

        data = self.get_data(response)

        for record in data:
            updated_at = record.get(self.cursor_field)

            if updated_at:
                record[self.cursor_field] = datetime.fromtimestamp(
                    record[self.cursor_field]
                ).isoformat()  # convert timestamp to datetime string

            yield from self.filter_by_state(stream_state=stream_state, record=record)

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, any]:
        """
        This method is called once for each record returned from the API to
        compare the cursor field value in that record with the current state
        we then return an updated state object. If this is the first time we
        run a sync or no state was passed, current_stream_state will be None.
        """

        current_stream_state = current_stream_state or {}

        current_stream_state_date = current_stream_state.get(self.cursor_field, self.start_date)
        latest_record_date = latest_record.get(self.cursor_field, self.start_date)

        return {self.cursor_field: max(current_stream_state_date, latest_record_date)}


class StreamMixin:
    slicing_stream: Optional[IntercomStream] = None

    def stream_slices(self, sync_mode, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for item in self.slicing_stream(authenticator=self.authenticator, start_date=self.start_date).read_records(sync_mode=sync_mode):
            yield {"id": item["id"]}

        yield from []


class Admins(IntercomStream):
    """Return list of all admins.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-admins
    Endpoint: https://api.intercom.io/admins
    """

    data_fields = ["admins"]

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "admins"


class Companies(IncrementalIntercomStream):
    """Return list of all companies.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-companies
    Endpoint: https://api.intercom.io/companies
    """

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "companies"


class CompanySegments(StreamMixin, IncrementalIntercomStream):
    """Return list of all company segments.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-attached-segments-1
    Endpoint: https://api.intercom.io/companies/<id>/segments
    """

    slicing_stream = Companies

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"/companies/{stream_slice['id']}/segments"


class Conversations(IncrementalIntercomStream):
    """Return list of all conversations.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-conversations
    Endpoint: https://api.intercom.io/conversations
    """

    data_fields = ["conversations"]

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "conversations"


class ConversationParts(StreamMixin, IncrementalIntercomStream):
    """Return list of all conversation parts.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#retrieve-a-conversation
    Endpoint: https://api.intercom.io/conversations/<id>
    """

    data_fields = ["conversation_parts", "conversation_parts"]
    slicing_stream = Conversations

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"/conversations/{stream_slice['id']}"


class Segments(IncrementalIntercomStream):
    """Return list of all segments.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-segments
    Endpoint: https://api.intercom.io/segments
    """

    data_fields = ["segments"]

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "segments"


class Contacts(IncrementalIntercomStream):
    """Return list of all contacts.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-contacts
    Endpoint: https://api.intercom.io/contacts
    """

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "contacts"


class DataAttributes(IntercomStream):
    primary_key = "name"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "data_attributes"


class CompanyAttributes(DataAttributes):
    """Return list of all data attributes belonging to a workspace for companies.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-data-attributes
    Endpoint: https://api.intercom.io/data_attributes?model=company
    """

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {"model": "company"}


class ContactAttributes(DataAttributes):
    """Return list of all data attributes belonging to a workspace for contacts.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-data-attributes
    Endpoint: https://api.intercom.io/data_attributes?model=contact
    """

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {"model": "contact"}


class Tags(IntercomStream):
    """Return list of all tags.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-tags-for-an-app
    Endpoint: https://api.intercom.io/tags
    """

    primary_key = "name"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "tags"


class Teams(IntercomStream):
    """Return list of all teams.
    API Docs: https://developers.intercom.com/intercom-api-reference/reference#list-teams
    Endpoint: https://api.intercom.io/teams
    """

    primary_key = "name"
    data_fields = ["teams"]

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "teams"


class SourceIntercom(AbstractSource):
    """
    Source Intercom fetch data from messaging platform.
    """

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        authenticator = TokenAuthenticator(token=config["access_token"])
        try:
            url = f"{IntercomStream.url_base}/tags"
            auth_headers = {"Accept": "application/json", **authenticator.get_auth_header()}
            session = requests.get(url, headers=auth_headers)
            session.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        AirbyteLogger().log("INFO", f"Using start_date: {config['start_date']}")

        auth = TokenAuthenticator(token=config["access_token"])
        return [
            Admins(authenticator=auth, **config),
            Companies(authenticator=auth, **config),
            CompanySegments(authenticator=auth, **config),
            Conversations(authenticator=auth, **config),
            ConversationParts(authenticator=auth, **config),
            Contacts(authenticator=auth, **config),
            CompanyAttributes(authenticator=auth, **config),
            ContactAttributes(authenticator=auth, **config),
            Segments(authenticator=auth, **config),
            Tags(authenticator=auth, **config),
            Teams(authenticator=auth, **config),
        ]
