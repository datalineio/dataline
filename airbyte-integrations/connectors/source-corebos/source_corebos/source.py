"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import json
from datetime import datetime
from typing import Dict, Generator
import requests
import hashlib

from airbyte_protocol import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from base_python import AirbyteLogger, Source


class SourceCorebos(Source):
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        try:
            params={"operation":"getchallenge","username":config["username"]}
            base_url="http://test.coreboscrm.com/denorm/webservice.php"
            url = base_url
            logger.info("GET {}".format(url))
            response = requests.get(url, params=params)
            if response.status_code == 200:
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message='An exception occurred: Status Code: {}, content: {}'.format(response.status_code, response.content),
                )
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {str(e)}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        streams = []

        stream_name = "retrieve" 
        json_schema = { 
            "type": "object",
            "properties": {
                "success": { "type": "boolean" },
                "result": {
                    "type": "object",
                        "properties": {
                         "user_name": { "type": "string" },
                          "message":{"type":"you will get the schema of the endpoint selected."}
          }
        }
      }
    }


        streams.append(AirbyteStream(name=stream_name, json_schema=json_schema))
        return AirbyteCatalog(streams=streams)

    def read(
        self, logger: AirbyteLogger, config: json, catalog:json, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:

        retrieve_stream = "retrieve"
        session = SourceCorebos.corebos_login(config)
        end_point=config["url"]
        logger.info("Attempting to retrieve: "+end_point)
        if config["http_method"] == "GET":
            data = requests.get(end_point)
        else:
            data = requests.post(end_point,data=config["body"])
        if data.status_code != 200:
            logger.info("Could not retrieve data from: "+end_point)
            return
    
        yield AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(stream=retrieve_stream, data=data.json(), emitted_at=int(datetime.now().timestamp()) * 1000),
        )


    def get_string_md5(val):
        harshed =  hashlib.md5(val.encode()).hexdigest()
        return harshed
    def corebos_login(config: json):
        parent_url ="http://test.coreboscrm.com/denorm/webservice.php?"
        login_request = parent_url+"operation=getchallenge&username="+config["username"]
        corebos_session = requests.Session()
        login_request_info = corebos_session.get(login_request).json()
        if login_request_info["success"] == True:
            corebos_session.headers.update({'content-Type': 'application/json'})
            new_var = login_request_info["result"]["token"] + config["access_token"]
            harshed_token = SourceCorebos.get_string_md5(new_var)
            login_body = {"operation" : "login", 'username' : config["username"], 'accessKey' : harshed_token}
            login_response=  corebos_session.post(parent_url, json=login_body).json()
            sessionName= login_response['result']['sessionName']
            return sessionName
        else:
            return
