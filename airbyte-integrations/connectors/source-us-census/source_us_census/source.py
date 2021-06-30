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


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.models.airbyte_protocol import SyncMode

import requests
from urllib.parse import parse_qs
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth


def prepare_request_params(query_params: str, api_key: str) -> dict:
    query_params = parse_qs(query_params)
    params = {**query_params, "key": api_key}
    return params


class UsCensusStream(HttpStream, ABC):
    """
    Generic stream to ingest US Census data.

    You should get an API key at https://api.census.gov/data/key_signup.html.
    """

    primary_key = ""
    url_base = "https://api.census.gov/"

    def __init__(
        self, query_params: Optional[str], query_path: str, api_key: str, **kwargs: dict
    ):
        super().__init__(**kwargs)
        if not query_path:
            raise ValueError("query_path is required!")

        if not api_key:
            raise ValueError("api_key is required!")

        self.query_params = query_params or ""
        self.query_path = query_path
        self.api_key = api_key

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        Adds request parameters and api key from the config.
        """
        return prepare_request_params(self.query_params, self.api_key)

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        Parses the response from the us census website.

        The US Census provides data in an atypical format,
        which motivated the creation of this source rather
        than using a generic http source.

        * Data are represented in a two-dimensional array
        * Square brackets [ ] hold arrays
        * Values are separated by a , (comma).

        e.g.
            [["STNAME","POP","DATE_","state"],

            ["Alabama","4849377","7","01"],

            ["Alaska","736732","7","02"],

            ["Arizona","6731484","7","04"],

            ["Arkansas","2966369","7","05"],

            ["California","38802500","7","06"]]
        """
        # Where we accumulate a "row" of data until we encounter ']'
        buffer = ""
        # The response is in a tabular format where the first list of strings
        # is the "header" of the table which we use as keys in the final dictionary
        # we produce
        header = []
        # Characters with special meanings which should not be added to the buffer
        # of values
        ignore_chars = [
            "[",
            "\n",
        ]
        # Context: save if previous value is an escape character
        is_prev_escape = False
        # Context: save if we are currently in double quotes
        is_in_quotes = False
        # Placeholder used to save position of commas that are
        # within values, so the .split(',') call does not produce
        # erroneous values
        comma_placeholder = "||comma_placeholder||"

        for response_chunk in response.iter_content(decode_unicode=True):
            if response_chunk == "\\":
                is_prev_escape = True
                continue
            elif response_chunk == '"' and not is_prev_escape:
                # If we are in quotes and encounter
                # closing quotes, we are not within quotes anymore
                # otherwise we are within quotes.
                is_in_quotes = not is_in_quotes
            elif response_chunk == "," and is_in_quotes:
                buffer += comma_placeholder
            elif response_chunk in ignore_chars and not is_prev_escape:
                pass
            elif response_chunk == "]":
                if not header:
                    header = buffer.split(",")
                elif buffer:
                    # Remove the first character from the values since
                    # it's a comma.
                    split_values = buffer.split(",")[1:]
                    # Add back commas originally embedded in values
                    split_values = map(
                        lambda x: x.replace(comma_placeholder, ","),
                        split_values,
                    )
                    # Zip the values we found with the "header"
                    yield dict(
                        zip(
                            header,
                            split_values,
                        )
                    )
                buffer = ""
            else:
                buffer += response_chunk
            is_prev_escape = False

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        The US Census website hosts many APIs: https://www.census.gov/data/developers/data-sets.html

        These APIs return data in a non standard format.
        We create the JSON schemas dynamically by reading the first "row" of data we get.

        In this implementation all records are of type "string", but this function could
        be changed to try and infer the data type based on the values it finds.
        """
        first_record = next(self.read_records(SyncMode.full_refresh))
        json_schema = {k: {"type": "string"} for (k, _) in first_record.items()}
        if first_record:
            return {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "additionalProperties": True,
                "type": "object",
                "properties": json_schema,
            }
        raise ValueError(
            "For schema discovery: the request must return at least one result"
        )

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        """
        Gets path from the config.
        """
        return self.query_path


# Source
class SourceUsCensus(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Tests the connection and the API key for the US census website.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            params = prepare_request_params(
                config.get("query_params"),
                config.get("api_key"),
            )
            resp = requests.get(
                f"{UsCensusStream.url_base}{config.get('query_path')}", params=params
            )
            status = resp.status_code
            logger.info(f"Ping response code: {status}")

            if status == 200:
                if "Invalid Key" in resp.text:
                    return False, RuntimeError(resp.text)
                return True, None
            return False, RuntimeError(resp.text)
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        The US Census website hosts many APIs: https://www.census.gov/data/developers/data-sets.html

        We provide one generic stream of all the US Census APIs rather than one stream per API.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return [
            UsCensusStream(
                query_params=config.get("query_params"),
                query_path=config.get("query_path"),
                api_key=config.get("api_key"),
                authenticator=NoAuth(),
            )
        ]
