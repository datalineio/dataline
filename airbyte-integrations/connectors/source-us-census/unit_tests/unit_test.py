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

from typing import List
from airbyte_cdk.sources.streams.http.exceptions import UserDefinedBackoffException
import pytest
import responses
import requests
from source_us_census.source import UsCensusStream
from airbyte_cdk.sources.streams.http.auth import NoAuth


@pytest.fixture
def us_census_stream():
    return UsCensusStream(
        query_params={},
        query_path="data/test",
        api_key="MY_API_KEY",
        authenticator=NoAuth(),
    )


simple_test = '[["name","id"],["A","1"],["B","2"]]'
example_from_docs_test = (
    '[["STNAME","POP","DATE_","state"],'
    '["Alabama","4849377","7","01"],'
    '["Alaska","736732","7","02"],'
    '["Arizona","6731484","7","04"],'
    '["Arkansas","2966369","7","05"],'
    '["California","38802500","7","06"]]'
)


@responses.activate
@pytest.mark.parametrize(
    "response, expected_result",
    [
        (
            simple_test,
            [{"name": "A", "id": "1"}, {"name": "B", "id": "2"}],
        ),
        (
            (
                example_from_docs_test,
                [
                    {
                        "STNAME": "Alabama",
                        "POP": "4849377",
                        "DATE_": "7",
                        "state": "01",
                    },
                    {"STNAME": "Alaska", "POP": "736732", "DATE_": "7", "state": "02"},
                    {
                        "STNAME": "Arizona",
                        "POP": "6731484",
                        "DATE_": "7",
                        "state": "04",
                    },
                    {
                        "STNAME": "Arkansas",
                        "POP": "2966369",
                        "DATE_": "7",
                        "state": "05",
                    },
                    {
                        "STNAME": "California",
                        "POP": "38802500",
                        "DATE_": "7",
                        "state": "06",
                    },
                ],
            )
        ),
        (
            '[["name","id"],["I have an escaped \\" quote","I have an embedded , comma"],["B","2"]]',
            [
                {
                    "name": 'I have an escaped " quote',
                    "id": "I have an embedded , comma",
                },
                {"name": "B", "id": "2"},
            ],
        ),
    ],
)
def test_parse_response(
    us_census_stream: UsCensusStream, response: str, expected_result: dict
):
    responses.add(
        responses.GET,
        us_census_stream.url_base,
        body=response,
    )
    resp = requests.get(us_census_stream.url_base)

    assert list(us_census_stream.parse_response(resp)) == expected_result


type_string = {"type": "string"}


@responses.activate
@pytest.mark.parametrize(
    "response, expected_schema",
    [
        (
            simple_test,
            {
                "name": type_string,
                "id": type_string,
            },
        ),
        (
            example_from_docs_test,
            {
                "STNAME": type_string,
                "POP": type_string,
                "DATE_": type_string,
                "state": type_string,
            },
        ),
    ],
)
def test_discover_schema(
    us_census_stream: UsCensusStream, response: str, expected_schema: dict
):
    responses.add(
        responses.GET,
        f"{us_census_stream.url_base}{us_census_stream.query_path}",
        body=response,
    )
    assert us_census_stream.get_json_schema().get("properties") == expected_schema
