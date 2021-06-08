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


from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils import casing


class GitlabStream(HttpStream, ABC):
    primary_key = "id"

    def __init__(self, api_url, **kwargs):
        super().__init__(**kwargs)
        self.api_url = api_url

    @property
    def url_base(self) -> str:
        """
        :return: URL base for the API endpoint
        """
        return f"https://{self.api_url}//api/v4/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_data = response.json()
        if isinstance(response_data, list):
            yield from response_data
        elif isinstance(response_data, dict):
            yield response_data
        else:
            Exception("TODO")


class GitlabSubStream(GitlabStream):
    def __init__(self, parent_stream: GitlabStream, parent_similar: bool = False, repo_url = False, **kwargs):
        super().__init__(**kwargs)
        self.parent_stream = parent_stream
        self.parent_similar = parent_similar
        self.repo_url = repo_url

    @property
    def path_template(self) -> str:
        """
        :return: sub stream path template
        """
        template = [self.parent_stream.name, "{main_id}"]
        if self.repo_url:
            template.append("repository")
        template.append(casing.camel_to_snake(self.__class__.__name__))
        return "/".join(template)

    @property
    def name(self) -> str:
        """
        :return: Stream name. By default this is the implementing class name, but it can be overridden as needed.
        """
        if self.parent_similar:
            return f"{self.parent_stream.name[:-1]}_{super().name}"
        return super().name

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for slice in self.parent_stream.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=slice):
                yield {"main_id": record["id"]}

    def path(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> str:
        return self.path_template.format(main_id=stream_slice["main_id"])


class Groups(GitlabStream):

    def __init__(self, group_ids, **kwargs):
        super().__init__(**kwargs)
        self.group_ids = group_ids

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"groups/{stream_slice['id']}"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for gid in self.group_ids:
            yield {"id": gid}


class Projects(GitlabStream):
    def __init__(self, project_ids, parent_stream=None, **kwargs):
        super().__init__(**kwargs)
        self.project_ids = project_ids
        self.parent_stream = parent_stream

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"projects/{stream_slice['id']}"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        if self.parent_stream:
            group_project_ids = set()
            for slice in self.parent_stream.stream_slices(sync_mode=SyncMode.full_refresh):
                for record in self.parent_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=slice):
                    group_project_ids.update({i["path_with_namespace"] for i in record["projects"]})
            for pid in self.project_ids:
                if pid in group_project_ids:
                    yield {"id": pid.replace("/", "%2F")}
        else:
            for pid in self.project_ids:
                if pid in self.parent_stream.project_ids:
                    yield {"id": pid.replace("/", "%2F")}


class Milestones(GitlabSubStream):
    pass


class Members(GitlabSubStream):
    pass


class Labels(GitlabSubStream):
    pass


class Branches(GitlabSubStream):
    pass


class Commits(GitlabSubStream):
    pass


class Issues(GitlabSubStream):
    pass


class MergeRequests(GitlabSubStream):
    pass


class Releases(GitlabSubStream):
    pass


class Tags(GitlabSubStream):
    pass


class Pipelines(GitlabSubStream):
    pass


class PipelinesExtended(GitlabSubStream):
    path_template = "projects/{project_id}/pipelines/{pipeline_id}"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for slice in self.parent_stream.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=slice):
                yield {"project_id": record["project_id"], "pipeline_id": record["id"]}

    def path(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> str:
        return self.path_template.format(project_id=stream_slice["project_id"], pipeline_id=stream_slice["pipeline_id"])


class Jobs(GitlabSubStream):
    path_template = "projects/{project_id}/pipelines/{pipeline_id}/jobs"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for slice in self.parent_stream.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=slice):
                yield {"project_id": record["project_id"], "pipeline_id": record["id"]}

    def path(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> str:
        return self.path_template.format(project_id=stream_slice["project_id"], pipeline_id=stream_slice["pipeline_id"])


class Users(GitlabSubStream):
    pass


# TODO: check this before merge
class Epics(GitlabSubStream):
    path_template = "groups/{parent_id}/epics"


# TODO: check this before merge
class EpicIssues(GitlabSubStream):
    path_template = "groups/{group_id}/epics/{epic_id}/issues"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for slice in self.parent_stream.stream_slices(sync_mode=SyncMode.full_refresh):
            for record in self.parent_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=slice):
                yield {"group_id": record["group_id"], "epic_id": record["id"]}

    def path(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> str:
        return self.path_template.format(project_id=stream_slice["project_id"], pipeline_id=stream_slice["pipeline_id"])
#
#
# class IncrementalGitlabStream(GitlabStream, ABC):
#     # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
#     state_checkpoint_interval = None
#
#     @property
#     def cursor_field(self) -> str:
#         """
#         TODO
#         Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
#         usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.
#
#         :return str: The name of the cursor field.
#         """
#         return []
#
#     def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
#         """
#         Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
#         the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
#         """
#         return {}
#
#
# class Employees(IncrementalGitlabStream):
#     """
#     TODO: Change class name to match the table/data source this stream corresponds to.
#     """
#
#     # TODO: Fill in the cursor_field. Required.
#     cursor_field = "start_date"
#
#     # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
#     primary_key = "employee_id"
#
#     def path(self, **kwargs) -> str:
#         """
#         TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
#         return "single". Required.
#         """
#         return "employees"
#
#     def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
#         """
#         TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.
#
#         Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
#         This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
#         section of the docs for more information.
#
#         The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
#         necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
#         This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.
#
#         An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
#         craft that specific request.
#
#         For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
#         this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
#         till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
#         the date query param.
#         """
#         raise NotImplementedError("Implement stream slices or delete this method!")
