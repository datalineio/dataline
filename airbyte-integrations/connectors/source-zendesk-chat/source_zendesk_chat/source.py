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


from typing import Any, List, Mapping, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

from .api import Accounts, Agents, AgentTimelines, Bans, Chats, Departments, Goals, Roles, RoutingSettings, Shortcuts, Skills, Triggers


class SourceZendeskChat(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            authenticator = TokenAuthenticator(token=config["access_token"])
            list(RoutingSettings(authenticator=authenticator).read_records(SyncMode.full_refresh))
            return True, None
        except Exception as error:
            return False, f"Unable to connect to Zendesk Chat API with the provided credentials - {error}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = TokenAuthenticator(token=config["access_token"])
        return [
            Agents(authenticator=authenticator),
            AgentTimelines(authenticator=authenticator, start_date=config["start_date"]),
            Accounts(authenticator=authenticator),
            Chats(authenticator=authenticator),
            Shortcuts(authenticator=authenticator),
            Triggers(authenticator=authenticator),
            Bans(authenticator=authenticator),
            Departments(authenticator=authenticator),
            Goals(authenticator=authenticator),
            Skills(authenticator=authenticator),
            Roles(authenticator=authenticator),
            RoutingSettings(authenticator=authenticator),
        ]
