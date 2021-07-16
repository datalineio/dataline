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


import json as j
from datetime import datetime
from math import ceil
from typing import Dict, Iterable, List

import pendulum
import requests
from airbyte_cdk import AirbyteLogger

from .zuora_auth import ZuoraAuthenticator
from .zuora_errors import EndDateError, ZOQLQueryCannotProcessObject, ZOQLQueryFailed, ZOQLQueryFieldCannotResolve, ZOQLQueryNotValid


class ZoqlExportClient:

    """
    # TODO: Add the description about the ZOQL EXPORT + Links
    # TODO: Add description to class methods
    """

    logger = AirbyteLogger()

    def __init__(self, authenticator: Dict, url_base: str, start_date: str, window_in_days: int, client_id: str, client_secret: str, is_sandbox: bool):
        self.authenticator = authenticator
        self.url_base = url_base
        self.start_date = start_date
        self.window_in_days = window_in_days
        self.client_id = client_id
        self.client_secret = client_secret
        self.is_sandbox = is_sandbox
        self.retry_max = 3

    # MAKE try/except request warper with handling errors
    def _make_request(self, method: str = "GET", url: str = None, data: Dict = None) -> requests.Response:
        retry = 0
        while retry <= self.retry_max:
            try:
                request = requests.request(method=method, url=url, headers=self.authenticator, json=data)
                request.raise_for_status()
                return request
            except requests.exceptions.HTTPError as e:
                retry += 1
                if retry <= self.retry_max:
                    self._handle_requests_exceptions(e)
                    continue
                else:
                    raise Exception(e)

    # MAKE QUERY: for data_query_job
    def _make_dq_query(
        self, q_type: str = "select", obj: str = None, date_field: str = None, start_date: str = None, end_date: str = None
    ) -> Dict:
        # POTENTIALLY COULD BE REPLACED WITH request_parameters()
        valid_types = ["select", "describe", "show_tables"]
        base_query = {"compression": "NONE", "output": {"target": "S3"}, "outputFormat": "JSON", "query": ""}

        if q_type not in valid_types:
            raise ZOQLQueryNotValid(q_type, valid_types)
        elif q_type == "select":
            end_date = pendulum.now().to_datetime_string() if end_date is None else end_date
            base_query[
                "query"
            ] = f"select * from {obj} where {date_field} >= TIMESTAMP '{start_date}' and {date_field} <= TIMESTAMP '{end_date}'"
        elif q_type == "describe":
            base_query["query"] = f"DESCRIBE {obj}"
        elif q_type == "show_tables":
            base_query["query"] = "SHOW TABLES"
        return base_query

    # SUBMIT: data_query_job using data_query
    def _submit_dq_job(self, dq_query: Dict) -> str:
        submit_job = self._make_request(method="POST", url=f"{self.url_base}/query/jobs", data=dq_query)
        submit_job = submit_job.json()["data"]["id"]
        # self.logger.debug(submit_job)
        return submit_job

    # CHECK: the submited data_query_job status
    def _check_dq_job_status(self, dq_job_id: str, status: str = None) -> Dict:
        success = "completed"
        # Define the error status
        errors = ["failed", "canceled", "aborted"]
        # Define specific error msg when the cursor_field cannot be resolved
        cursor_error = "Column 'updateddate' cannot be resolved"
        obj_read_error = "failed to process object"

        while status != success:
            response = self._make_request(url=f"{self.url_base}/query/jobs/{dq_job_id}")
            dq_job_check = response.json()
            status = dq_job_check["data"]["queryStatus"]
            if status in errors and cursor_error in dq_job_check["data"]["errorMessage"]:
                raise ZOQLQueryFieldCannotResolve
            elif status in errors and obj_read_error in dq_job_check["data"]["errorMessage"]:
                raise ZOQLQueryCannotProcessObject
            elif status in errors:
                raise ZOQLQueryFailed(response)
        return dq_job_check

    # GET: data_query_job result
    def _get_data_from_dq_job(self, response: requests.Response) -> List:
        dq_job_data = requests.get(response["data"]["dataFile"])
        dq_job_data.raise_for_status()
        data = [j.loads(line) for line in dq_job_data.text.splitlines()]
        return data or []

    # Warpper function for `Query > Submit > Check > Get`
    def _get_data(
        self, q_type: str = "select", obj: str = None, date_field: str = None, start_date: str = None, end_date: str = None
    ) -> Iterable:
        query = self._make_dq_query(q_type, obj, date_field, start_date, end_date)
        # self.logger.debug(f"{query}")
        submit = self._submit_dq_job(query)
        check = self._check_dq_job_status(submit)
        get = self._get_data_from_dq_job(check)
        yield from get

    # Warper for _get_data using date-slices as pages
    def _get_data_with_date_slice(
        self, q_type: str, obj: str, date_field: str, start_date: str, window_days: int, end_date: str = None
    ) -> Iterable:
        # Parsing input dates
        s = pendulum.parse(start_date)
        tz = self._get_tz(s)
        # If there is no `end_date` as input - use now()
        e = pendulum.parse(pendulum.now().to_datetime_string()) if end_date is None else pendulum.parse(end_date)
        # check end_date bigger than start_date
        if s >= e:
            raise EndDateError
        else:
            # Get n of date-slices
            n = self._get_n_slices(s, e, window_days)
            # initiating slice_start/end for date slice
            slice_start = s.to_datetime_string() + tz
            # if slice_end is bigger than the input end_date, switch to the end_date
            slice_end = self._check_end_date(self._next_slice_end_date(slice_start, window_days), e) + tz
            # For multiple date-slices
            if n > 1:
                while n > 0:
                    yield from self._get_data(q_type, obj, date_field, slice_start, slice_end)
                    # make next date-slice
                    slice_start = slice_end
                    # if next date-slice end_date is bigger than the input end_date, we switch to the end_date
                    slice_end = self._check_end_date(self._next_slice_end_date(slice_end, window_days), e) + tz
                    # Modify iterator
                    n -= 1
            else:
                # For 1 date-slice
                yield from self._get_data(q_type, obj, date_field, slice_start, slice_end)

    # Check the end-date before assign next date-slice
    @staticmethod
    def _check_end_date(slice_end_date, global_end_date: str) -> str:
        return global_end_date.to_datetime_string() if pendulum.parse(slice_end_date) > global_end_date else slice_end_date

    # Get n of date-slices needed to fetch the data
    @staticmethod
    def _get_n_slices(start_date: datetime, end_date: datetime, window_days: int) -> int:
        # case where we have 1 sec difference between start and end dates would produce 0 slices, so return 1 instead
        d = pendulum.period(start_date, end_date).in_days()
        return 1 if ceil(d / window_days) <= 0 else ceil(d / window_days)

    # Creates the next-date-slice
    @staticmethod
    def _next_slice_end_date(end_date: str, window_days: int) -> str:
        # potentially could be replaced with next_page_token
        format = "YYYY-MM-DD HH:mm:ss Z"
        return pendulum.from_format(end_date, format).add(days=window_days).to_datetime_string()

    # Get/Set the timezone information from the date_field
    @staticmethod
    def _get_tz(start_date: datetime) -> str:
        return " +00:00" if start_date.tzname() == "UTC" else f" {start_date.tzname()}"

    # Compares strings for datatypes casting
    @staticmethod
    def _check_data_type(_str, _list):
        # Returns True if the Type is in the List of Types, else - False
        return any(_str in s for s in _list)

    # Convert Zuora DataTypes to JsonSchema Types
    def _cast_schema_types(self, _schema: List) -> Dict:
        casted_schema_types = {}

        for field in range(len(_schema)):
            field_name = _schema[field].get("field").lower()
            field_type = _schema[field].get("type")

            # Casting Zuora ZOQL Data Types into JsonSchema types
            if self._check_data_type(field_type, ["decimal(22,9)", "integer", "int", "bigint", "timestamp"]):
                field_type = ["number", "null"]
            elif self._check_data_type(field_type, ["date", "datetime", "timestamp with time zone", "picklist", "text", "varchar"]):
                field_type = ["string", "null"]
            elif self._check_data_type(field_type, ["zoql", "binary", "json", "xml", "blob"]):
                field_type = ["object", "null"]
            elif self._check_data_type(field_type, ["boolean"]):
                field_type = ["boolean", "null"]
            else:
                # if there is something else we don't cover, cast it as string
                field_type = ["string", "null"]

            casted_schema_types.update(**{field_name: {"type": field_type}})

        return casted_schema_types

    # Convert Zuora Fields data types to JSONSchema
    def _zuora_object_to_json_schema(self, obj: str) -> Dict:
        self.logger.info(f"Getting schema information for {obj}")
        raw_data_types = list(self._get_data(q_type="describe", obj=obj))
        json_schema = []
        # Get the raw field names and their types
        for field in range(len(raw_data_types)):
            json_schema.append({"field": raw_data_types[field]["Column"], "type": raw_data_types[field]["Type"]})
        # Cast the Zuora Field Types to JsonSchema Types
        json_schema = self._cast_schema_types(json_schema)
        return json_schema

    # Convert Zuora Fields data types to JSONSchema
    def _zuora_list_objects(self) -> List:
        self.logger.info("Retrieving the list of available Objects from Zuora")
        raw_data_types = list(self._get_data(q_type="show_tables"))
        object_list = []
        # Get the raw field names and their types
        for field in range(len(raw_data_types)):
            object_list.append(raw_data_types[field]["Table"])
        return object_list

    # Method to handle the errors
    def _handle_requests_exceptions(self, exception: requests.RequestException):
        exception_code = exception.response.status_code
        # print(exception_code)
        if exception_code in [500, 404]:
            # Bad Request - Server declined the request
            self.logger.error(f"Bad URL - please check the link, {exception}")
        elif exception_code == 400:
            # Bad Request - Server declined the request
            self.logger.error(f"Bad Request, please check the url query parameters, {exception}")
        elif exception_code == 401:
            # Unauthorised or Invalid Token
            self.logger.error(f"Unauthorised or Expired/Invalid Token, {exception}")
            # Attempt to refresh the token
            self.authenticator = ZuoraAuthenticator(self.is_sandbox).generateToken(self.client_id, self.client_secret).get("header")
            self.logger.info("Token is Refreshed")
        else:
            self.logger.fatal(f"Unrecognised Error Occured, please check traceback {exception}")
