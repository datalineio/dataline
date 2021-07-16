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


from typing import Dict

import requests


class ZuoraAuthenticator:
    def __init__(self, is_sandbox: bool = True):
        self.is_sandbox = is_sandbox

    @property
    def endpoint(self) -> str:
        if self.is_sandbox:
            return "https://rest.apisandbox.zuora.com"
        else:
            return "https://rest.zuora.com"

    # GENERATE TOKEN
    def generateToken(self, client_id: str, client_secret: str) -> Dict:
        endpoint = f"{self.endpoint}/oauth/token"
        header = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_id": f"{client_id}",
            "client_secret": f"{client_secret}",
            "grant_type": "client_credentials",
        }
        try:
            session = requests.post(endpoint, headers=header, data=data)
            session.raise_for_status()
            return {
                "status": session.status_code,
                "header": {
                    "Authorization": f"Bearer {session.json().get('access_token')}",
                    "Content-Type": "application/json",
                    # "X-Zuora-WSDL-Version": "107",
                },
            }
        except requests.exceptions.HTTPError as e:
            return {"status": e}
