# Copyright 2019, 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose, Aniek Roelofs, Tuan Chien


from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Optional

import pendulum


class CloudWorkspace:
    def __init__(
        self,
        *,
        project_id: str,
        download_bucket: str,
        transform_bucket: str,
        data_location: str,
        output_project_id: Optional[str] = None,
    ):
        """The CloudWorkspace settings used by workflows.

        project_id: the Google Cloud project id. input_project_id is an alias for project_id.
        download_bucket: the Google Cloud Storage bucket where downloads will be stored.
        transform_bucket: the Google Cloud Storage bucket where transformed data will be stored.
        data_location: the data location for storing information, e.g. where BigQuery datasets should be located.
        output_project_id: an optional Google Cloud project id when the outputs of a workflow should be stored in a
        different project to the inputs. If an output_project_id is not supplied, the project_id will be used.
        """

        self._project_id = project_id
        self._download_bucket = download_bucket
        self._transform_bucket = transform_bucket
        self._data_location = data_location
        self._output_project_id = output_project_id

    @property
    def project_id(self) -> str:
        return self._project_id

    @project_id.setter
    def project_id(self, project_id: str):
        self._project_id = project_id

    @property
    def download_bucket(self) -> str:
        return self._download_bucket

    @download_bucket.setter
    def download_bucket(self, download_bucket: str):
        self._download_bucket = download_bucket

    @property
    def transform_bucket(self) -> str:
        return self._transform_bucket

    @transform_bucket.setter
    def transform_bucket(self, transform_bucket: str):
        self._transform_bucket = transform_bucket

    @property
    def data_location(self) -> str:
        return self._data_location

    @data_location.setter
    def data_location(self, data_location: str):
        self._data_location = data_location

    @property
    def input_project_id(self) -> str:
        return self._project_id

    @input_project_id.setter
    def input_project_id(self, project_id: str):
        self._project_id = project_id

    @property
    def output_project_id(self) -> Optional[str]:
        if self._output_project_id is None:
            return self._project_id
        return self._output_project_id

    @output_project_id.setter
    def output_project_id(self, output_project_id: Optional[str]):
        self._output_project_id = output_project_id

    @staticmethod
    def from_dict(dict_: Dict) -> CloudWorkspace:
        """Constructs a CloudWorkspace instance from a dictionary.

        :param dict_: the dictionary.
        :return: the Workflow instance.
        """

        project_id = dict_.get("project_id")
        download_bucket = dict_.get("download_bucket")
        transform_bucket = dict_.get("transform_bucket")
        data_location = dict_.get("data_location")
        output_project_id = dict_.get("output_project_id")

        return CloudWorkspace(
            project_id=project_id,
            download_bucket=download_bucket,
            transform_bucket=transform_bucket,
            data_location=data_location,
            output_project_id=output_project_id,
        )

    def to_dict(self) -> Dict:
        """CloudWorkspace instance to dictionary.

        :return: the dictionary.
        """

        return dict(
            project_id=self._project_id,
            download_bucket=self._download_bucket,
            transform_bucket=self._transform_bucket,
            data_location=self._data_location,
            output_project_id=self.output_project_id,
        )

    @staticmethod
    def parse_cloud_workspaces(list: List) -> List[CloudWorkspace]:
        """Parse the cloud workspaces list object into a list of CloudWorkspace instances.

        :param list: the list.
        :return: a list of CloudWorkspace instances.
        """

        return [CloudWorkspace.from_dict(dict_) for dict_ in list]


@dataclass
class Workflow:
    """A Workflow configuration.

    Attributes:
        dag_id: the Airflow DAG identifier for the workflow.
        name: a user-friendly name for the workflow.
        class_name: the fully qualified class name for the workflow class.
        cloud_workspace: the Cloud Workspace to use when running the workflow.
        kwargs: a dictionary containing optional keyword arguments that are injected into the workflow constructor.
    """

    dag_id: str = None
    name: str = None
    class_name: str = None
    cloud_workspace: CloudWorkspace = None
    kwargs: Optional[Dict] = field(default_factory=lambda: dict())

    def to_dict(self) -> Dict:
        """Workflow instance to dictionary.

        :return: the dictionary.
        """

        cloud_workspace = self.cloud_workspace
        if self.cloud_workspace is not None:
            cloud_workspace = self.cloud_workspace.to_dict()

        return dict(
            dag_id=self.dag_id,
            name=self.name,
            class_name=self.class_name,
            cloud_workspace=cloud_workspace,
            kwargs=self.kwargs,
        )

    @staticmethod
    def from_dict(dict_: Dict) -> Workflow:
        """Constructs a Workflow instance from a dictionary.

        :param dict_: the dictionary.
        :return: the Workflow instance.
        """

        dag_id = dict_.get("dag_id")
        name = dict_.get("name")
        class_name = dict_.get("class_name")

        cloud_workspace = dict_.get("cloud_workspace")
        if cloud_workspace is not None:
            cloud_workspace = CloudWorkspace.from_dict(cloud_workspace)

        kwargs = dict_.get("kwargs", dict())

        return Workflow(dag_id, name, class_name, cloud_workspace, kwargs)

    @staticmethod
    def parse_workflows(list: List) -> List[Workflow]:
        """Parse the workflows list object into a list of Workflow instances.

        :param list: the list.
        :return: a list of Workflow instances.
        """

        return [Workflow.from_dict(dict_) for dict_ in list]


class PendulumDateTimeEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, pendulum.DateTime):
            return obj.isoformat()
        return super().default(obj)


def workflows_to_json_string(workflows: List[Workflow]) -> str:
    """Covnert a list of Workflow instances to a JSON string.

    :param workflows: the Workflow instances.
    :return: a JSON string.
    """

    data = [workflow.to_dict() for workflow in workflows]
    return json.dumps(data, cls=PendulumDateTimeEncoder)


def json_string_to_workflows(json_string: str) -> List[Workflow]:
    """Convert a JSON string into a list of Workflow instances.

    :param json_string: a JSON string version of a list of Workflow instances.
    :return: a list of Workflow instances.
    """

    def parse_datetime(obj):
        for key, value in obj.items():
            try:
                obj[key] = pendulum.parse(value)
            except (ValueError, TypeError):
                pass
        return obj

    data = json.loads(json_string, object_hook=parse_datetime)
    return Workflow.parse_workflows(data)


def parse_dict_to_list(dict_: Dict, cls: ClassVar) -> List[Any]:
    """Parse the key, value pairs in a dictionary into a list of class instances.

    :param dict_: the dictionary.
    :param cls: the type of class to construct.
    :return: a list of class instances.
    """

    parsed_items = []
    for key, val in dict_.items():
        parsed_items.append(cls(key, val))
    return parsed_items


def is_base64(text: bytes) -> bool:
    """Check if the string is base64.
    :param text: Text to check.
    :return: Whether it is base64.
    """

    try:
        base64.decodebytes(text)
    except:
        return False

    return True
