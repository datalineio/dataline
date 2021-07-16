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

from enum import Enum
from typing import Any, List, Mapping

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.v8.services.types.google_ads_service import GoogleAdsRow, SearchGoogleAdsResponse
from proto.marshal.collections import Repeated, RepeatedComposite

REPORT_MAPPING = {
    "ad_group_ad_report": "ad_group_ad",
    "accounts": "customer",
    "campaigns": "campaign",
    "ad_groups": "ad_group",
    "ad_group_ads": "ad_group_ad",
}


class GoogleAds:
    DEFAULT_PAGE_SIZE = 1000

    def __init__(self, credentials: Mapping[str, Any], customer_id: str):
        self.client = GoogleAdsClient.load_from_dict(credentials)
        self.customer_id = customer_id
        self.ga_service = self.client.get_service("GoogleAdsService")

    def send_request(self, query: str) -> SearchGoogleAdsResponse:
        client = self.client
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = self.customer_id
        search_request.query = query
        search_request.page_size = self.DEFAULT_PAGE_SIZE

        return self.ga_service.search(search_request)

    @staticmethod
    def get_fields_from_schema(schema: Mapping[str, Any]) -> List[str]:
        properties = schema.get("properties")
        return [*properties]

    @staticmethod
    def convert_schema_into_query(
        schema: Mapping[str, Any], report_name: str, from_date: str = None, to_date: str = None, cursor_field: str = None
    ) -> str:
        from_category = REPORT_MAPPING[report_name]
        fields = GoogleAds.get_fields_from_schema(schema)
        fields = ",\n".join(fields)

        query_template = "SELECT {fields} FROM {from_category} "
        substitute_params = dict(fields=fields, from_category=from_category)

        if cursor_field:
            query_template += "WHERE {cursor_field} > '{from_date}' AND {cursor_field} < '{to_date}' ORDER BY {cursor_field}"
            substitute_params.update(dict(from_date=from_date, to_date=to_date, cursor_field=cursor_field))

        query = query_template.format(**substitute_params)
        return query

    @staticmethod
    def get_field_value(field_value: GoogleAdsRow, field: str) -> str:
        field_name = field.split(".")
        for level_attr in field_name:
            try:
                field_value = getattr(field_value, level_attr)
            except AttributeError:
                field_value = getattr(field_value, level_attr + "_", None)

            if isinstance(field_value, Enum):
                field_value = field_value.name
            elif isinstance(field_value, (Repeated, RepeatedComposite)):
                field_value = [str(value) for value in field_value]

        # Google Ads has a lot of entities inside itself and we cannot process them all separately, because:
        # 1. It will take a long time
        # 2. We have no way to get data on absolutely all entities to test.
        #
        # To prevent JSON from throwing an error during deserialization, we made such a hack.
        if not (isinstance(field_value, (list, int, float, str, bool, dict)) or field_value is None):
            field_value = str(field_value)

        return field_value

    @staticmethod
    def parse_single_result(schema: Mapping[str, Any], result: GoogleAdsRow):
        fields = GoogleAds.get_fields_from_schema(schema)
        single_record = {field: GoogleAds.get_field_value(result, field) for field in fields}
        return single_record
