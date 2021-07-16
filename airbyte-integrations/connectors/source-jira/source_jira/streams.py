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

import urllib.parse as urlparse
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional
from urllib.parse import parse_qs

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream

API_VERSION = 3


class JiraStream(HttpStream, ABC):
    """
    Jira API Reference: https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/
    """

    primary_key = "id"
    parse_response_root = None

    def __init__(self, domain: str, **kwargs):
        super(JiraStream, self).__init__(**kwargs)
        self._domain = domain

    @property
    def url_base(self) -> str:
        return f"https://{self._domain}/rest/api/{API_VERSION}/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        params = {}
        response_data = response.json()
        if "nextPage" in response_data:
            next_page = response_data["nextPage"]
            params = parse_qs(urlparse.urlparse(next_page).query)
        else:
            if all(paging_metadata in response_data for paging_metadata in ("startAt", "maxResults", "total")):
                start_at = response_data["startAt"]
                max_results = response_data["maxResults"]
                total = response_data["total"]
                end_at = start_at + max_results
                if not end_at > total:
                    params["startAt"] = end_at
                    params["maxResults"] = max_results
        return params

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {}

        if next_page_token:
            params.update(next_page_token)

        return params

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"Accept": "application/json"}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        records = response_json if not self.parse_response_root else response_json.get(self.parse_response_root, [])
        if isinstance(records, list):
            yield from records
        else:
            yield records


class IncrementalJiraStream(JiraStream, ABC):
    @property
    @abstractmethod
    def cursor_field(self) -> str:
        """
        Defining a cursor field indicates that a stream is incremental, so any incremental stream must extend this class
        and define a cursor field.
        """
        pass

    @abstractmethod
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent
        state object and returning an updated state object.
        """
        return {}


class ApplicationRoles(JiraStream):
    def path(self, **kwargs) -> str:
        return "applicationrole"


class Avatars(JiraStream):
    parse_response_root = "system"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        avatar_type = stream_slice["avatar_type"]
        return f"avatar/{avatar_type}/system"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        avatar_types = ("issuetype", "project", "user")
        for avatar_type in avatar_types:
            yield from super().read_records(stream_slice={"avatar_type": avatar_type}, **kwargs)


class Dashboards(JiraStream):
    parse_response_root = "dashboards"

    def path(self, **kwargs) -> str:
        return "dashboard"


class Filters(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "filter/search"


class FilterSharing(JiraStream):
    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        filter_id = stream_slice["filter_id"]
        return f"filter/{filter_id}/permission"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        filters_stream = Filters(authenticator=self.authenticator, domain=self._domain)
        for filters in filters_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"filter_id": filters["id"]}, **kwargs)


class Groups(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "group/bulk"


class Issues(IncrementalJiraStream):
    cursor_field = "updated"
    parse_response_root = "issues"

    def path(self, **kwargs) -> str:
        return "search"

    def request_params(self, stream_state=None, **kwargs):
        stream_state = stream_state or {}
        params = super().request_params(stream_state=stream_state, **kwargs)
        params["fields"] = ["attachment", "issuelinks", "security", "issuetype", "updated"]
        if stream_state.get(self.cursor_field):
            issues_state = pendulum.parse(stream_state.get(self.cursor_field))
            issues_state_row = issues_state.strftime("%Y/%m/%d %H:%M")
            params["jql"] = f"updated > '{issues_state_row}'"
        return params

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_record_date = pendulum.parse(latest_record.get("fields", {}).get(self.cursor_field))
        if current_stream_state:
            current_stream_state = current_stream_state.get(self.cursor_field)
            if current_stream_state:
                return {self.cursor_field: str(max(latest_record_date, pendulum.parse(current_stream_state)))}
        else:
            return {self.cursor_field: str(latest_record_date)}


class IssueComments(JiraStream):
    parse_response_root = "comments"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"issue/{key}/comment"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        issues_stream = Issues(authenticator=self.authenticator, domain=self._domain)
        for issue in issues_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"key": issue["key"]}, **kwargs)


class IssueFields(JiraStream):
    def path(self, **kwargs) -> str:
        return "field"


class IssueFieldConfigurations(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "fieldconfiguration"


class IssueCustomFieldContexts(JiraStream):
    parse_response_root = "values"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        field_id = stream_slice["field_id"]
        return f"field/{field_id}/context"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        fields_stream = IssueFields(authenticator=self.authenticator, domain=self._domain)
        for field in fields_stream.read_records(sync_mode=SyncMode.full_refresh):
            if field.get("custom", False):
                yield from super().read_records(stream_slice={"field_id": field["id"]}, **kwargs)


class IssueLinkTypes(JiraStream):
    parse_response_root = "issueLinkTypes"

    def path(self, **kwargs) -> str:
        return "issueLinkType"


class IssueNavigatorSettings(JiraStream):
    def path(self, **kwargs) -> str:
        return "settings/columns"


class IssueNotificationSchemes(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "notificationscheme"


class IssuePriorities(JiraStream):
    def path(self, **kwargs) -> str:
        return "priority"


class IssuePropertyKeys(JiraStream):
    parse_response_root = "key"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"issue/{key}/properties"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        issue_key = stream_slice["key"]
        yield from super().read_records(stream_slice={"key": issue_key}, **kwargs)


class IssueProperties(JiraStream):
    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        issue_key = stream_slice["issue_key"]
        return f"issue/{issue_key}/properties/{key}"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        issues_stream = Issues(authenticator=self.authenticator, domain=self._domain)
        issue_property_keys_stream = IssuePropertyKeys(authenticator=self.authenticator, domain=self._domain)
        for issue in issues_stream.read_records(sync_mode=SyncMode.full_refresh):
            for property_key in issue_property_keys_stream.read_records(stream_slice={"key": issue["key"]}, **kwargs):
                yield from super().read_records(stream_slice={"key": property_key["key"], "issue_key": issue["key"]}, **kwargs)


class IssueRemoteLinks(JiraStream):
    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"issue/{key}/remotelink"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        issues_stream = Issues(authenticator=self.authenticator, domain=self._domain)
        for issue in issues_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"key": issue["key"]}, **kwargs)


class IssueResolutions(JiraStream):
    def path(self, **kwargs) -> str:
        return "resolution"


class IssueSecuritySchemes(JiraStream):
    parse_response_root = "issueSecuritySchemes"

    def path(self, **kwargs) -> str:
        return "issuesecurityschemes"


class IssueTypeSchemes(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "issuetypescheme"


class IssueTypeScreenSchemes(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "issuetypescreenscheme"


class IssueVotes(JiraStream):
    parse_response_root = "voters"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"issue/{key}/votes"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        issues_stream = Issues(authenticator=self.authenticator, domain=self._domain)
        for issue in issues_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"key": issue["key"]}, **kwargs)


class IssueWatchers(JiraStream):
    parse_response_root = "watchers"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"issue/{key}/watchers"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        issues_stream = Issues(authenticator=self.authenticator, domain=self._domain)
        for issue in issues_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"key": issue["key"]}, **kwargs)


class IssueWorklogs(IncrementalJiraStream):
    cursor_field = "updated"
    parse_response_root = "worklogs"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"issue/{key}/worklog"

    def request_params(self, stream_state=None, **kwargs):
        stream_state = stream_state or {}
        params = super().request_params(stream_state=stream_state, **kwargs)
        if stream_state.get(self.cursor_field):
            issue_worklogs_state = pendulum.parse(stream_state.get(self.cursor_field))
            state_row = int(issue_worklogs_state.timestamp() * 1000)
            params[self.cursor_field] = state_row
        return params

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_record_date = pendulum.parse(latest_record.get("fields", {}).get(self.cursor_field))
        if current_stream_state:
            current_stream_state = current_stream_state.get(self.cursor_field)
            if current_stream_state:
                return {self.cursor_field: str(max(latest_record_date, pendulum.parse(current_stream_state)))}
        else:
            return {self.cursor_field: str(latest_record_date)}

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        issues_stream = Issues(authenticator=self.authenticator, domain=self._domain)
        for issue in issues_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"key": issue["key"]}, **kwargs)


class JiraSettings(JiraStream):
    def path(self, **kwargs) -> str:
        return "application-properties"


class Labels(JiraStream):
    def path(self, **kwargs) -> str:
        return "application-properties"


class Permissions(JiraStream):
    parse_response_root = "permissions"

    def path(self, **kwargs) -> str:
        return "permissions"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        records = response_json.get(self.parse_response_root, {}).values()
        yield from records


class PermissionSchemes(JiraStream):
    parse_response_root = "permissionSchemes"

    def path(self, **kwargs) -> str:
        return "permissionscheme"


class Projects(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "project/search"


class ProjectAvatars(JiraStream):
    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"project/{key}/avatars"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        for type_key, records in response_json.items():
            yield from records

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        projects_stream = Projects(authenticator=self.authenticator, domain=self._domain)
        for project in projects_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"key": project["key"]}, **kwargs)


class ProjectCategories(JiraStream):
    def path(self, **kwargs) -> str:
        return "projectCategory"


class ProjectComponents(JiraStream):
    parse_response_root = "values"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"project/{key}/component"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        projects_stream = Projects(authenticator=self.authenticator, domain=self._domain)
        for project in projects_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"key": project["key"]}, **kwargs)


class ProjectEmail(JiraStream):
    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        project_id = stream_slice["project_id"]
        return f"project/{project_id}/email"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        projects_stream = Projects(authenticator=self.authenticator, domain=self._domain)
        for project in projects_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"project_id": project["id"]}, **kwargs)


class ProjectPermissionSchemes(JiraStream):
    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"project/{key}/securitylevel"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        projects_stream = Projects(authenticator=self.authenticator, domain=self._domain)
        for project in projects_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"key": project["key"]}, **kwargs)


class ProjectTypes(JiraStream):
    def path(self, **kwargs) -> str:
        return "project/type"


class ProjectVersions(JiraStream):
    parse_response_root = "values"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        key = stream_slice["key"]
        return f"project/{key}/version"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        projects_stream = Projects(authenticator=self.authenticator, domain=self._domain)
        for project in projects_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from super().read_records(stream_slice={"key": project["key"]}, **kwargs)


class Screens(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "screens"


class ScreenTabs(JiraStream):
    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        screen_id = stream_slice["screen_id"]
        return f"screens/{screen_id}/tabs"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        screens_stream = Screens(authenticator=self.authenticator, domain=self._domain)
        for screen in screens_stream.read_records(sync_mode=SyncMode.full_refresh):
            yield from self.read_tab_records(stream_slice={"screen_id": screen["id"]}, **kwargs)

    def read_tab_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        screen_id = stream_slice["screen_id"]
        yield from super().read_records(stream_slice={"screen_id": screen_id}, **kwargs)


class ScreenTabFields(JiraStream):
    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        screen_id = stream_slice["screen_id"]
        tab_id = stream_slice["tab_id"]
        return f"screens/{screen_id}/tabs/{tab_id}/fields"

    def read_records(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        screens_stream = Screens(authenticator=self.authenticator, domain=self._domain)
        screen_tabs_stream = ScreenTabs(authenticator=self.authenticator, domain=self._domain)
        for screen in screens_stream.read_records(sync_mode=SyncMode.full_refresh):
            for tab in screen_tabs_stream.read_tab_records(stream_slice={"screen_id": screen["id"]}, **kwargs):
                yield from super().read_records(stream_slice={"screen_id": screen["id"], "tab_id": tab["id"]}, **kwargs)


class ScreenSchemes(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "screenscheme"


class TimeTracking(JiraStream):
    def path(self, **kwargs) -> str:
        return "configuration/timetracking/list"


class Users(JiraStream):
    def path(self, **kwargs) -> str:
        return "user/search?query="


class Workflows(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "workflow/search"


class WorkflowSchemes(JiraStream):
    parse_response_root = "values"

    def path(self, **kwargs) -> str:
        return "workflowscheme"


class WorkflowStatuses(JiraStream):
    def path(self, **kwargs) -> str:
        return "status"


class WorkflowStatusCategories(JiraStream):
    def path(self, **kwargs) -> str:
        return "statuscategory"
