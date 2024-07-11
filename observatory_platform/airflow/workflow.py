# Copyright 2019-2024 Curtin University
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

import json
import logging
import os
import shutil
from dataclasses import dataclass, field
from pydoc import locate
from typing import Any, Dict, List, Optional

import pendulum
from airflow import AirflowException
from airflow.models import DagBag, Variable

from observatory_platform.airflow.airflow import delete_old_xcoms
from observatory_platform.config import AirflowVars


def get_data_path() -> str:
    """Grabs the DATA_PATH airflow vairable

    :raises AirflowException: Raised if the variable does not exist
    :return: DATA_PATH variable contents
    """

    # Try to get environment variable from environment variable first
    data_path = os.environ.get(AirflowVars.DATA_PATH)
    if data_path is not None:
        return data_path

    # Try to get from Airflow Variable
    data_path = Variable.get(AirflowVars.DATA_PATH)
    if data_path is not None:
        return data_path

    raise AirflowException("DATA_PATH variable could not be found.")


def fetch_workflows() -> List[Workflow]:
    """Fetches Workflow instances from the WORKFLOWS Airflow Variable.

    :return: a list of Workflow instances.
    """

    workflows = []
    workflows_str = Variable.get(AirflowVars.WORKFLOWS)
    logging.info(f"workflows_str: {workflows_str}")

    if workflows_str is not None and workflows_str.strip() != "":
        try:
            workflows = json_string_to_workflows(workflows_str)
            logging.info(f"workflows: {workflows}")
        except json.decoder.JSONDecodeError as e:
            e.msg = f"workflows_str: {workflows_str}\n\n{e.msg}"

    return workflows


def load_dags_from_config(use_params: bool = False):
    """Loads DAGs from a workflow config file, stored in the WORKFLOWS Airflow Variable.

    :param use_params: whether the workflow has a Params class with which to load with
    :return: None.
    """

    for workflow in fetch_workflows():
        dag_id = workflow.dag_id
        logging.info(f"Making Workflow: {workflow.name}, dag_id={dag_id}")
        if use_params:
            dag = make_dag_from_params(workflow)
        else:
            dag = make_dag(workflow)

        logging.info(f"Adding DAG: dag_id={dag_id}, dag={dag}")
        globals()[dag_id] = dag


def make_dag_from_params(workflow: Workflow):
    """Make a DAG instance from a Workflow config. Will attempt to built the dag with its Params class

    :param workflow: the workflow configuration.
    :return: the workflow instance.
    """
    param_cls = locate(workflow.class_name + ".Params")
    if param_cls is None:
        raise ModuleNotFoundError(
            f"dag_id={workflow.dag_id}: could not locate Param class_name={workflow.class_name}.Params"
        )

    cls = locate(workflow.class_name + ".create_dag")
    if cls is None:
        raise ModuleNotFoundError(
            f"dag_id={workflow.dag_id}: could not locate class_name={workflow.class_name}.create_dag"
        )

    return cls(params=param_cls(dag_id=workflow.dag_id, cloud_workspace=workflow.cloud_workspace, **workflow.kwargs))


def make_dag(workflow: Workflow):
    """Make a DAG instance from a Workflow config.

    :param workflow: the workflow configuration.
    :return: the workflow instance.
    """

    cls = locate(workflow.class_name + ".create_dag")
    if cls is None:
        raise ModuleNotFoundError(
            f"dag_id={workflow.dag_id}: could not locate class_name={workflow.class_name}.create_dag"
        )

    return cls(dag_id=workflow.dag_id, cloud_workspace=workflow.cloud_workspace, **workflow.kwargs)


def make_workflow_folder(dag_id: str, run_id: str, *subdirs: str) -> str:
    """Return the path to this dag release's workflow folder. Will also create it if it doesn't exist

    :param dag_id: The ID of the dag. This is used to find/create the workflow folder
    :param run_id: The Airflow DAGs run ID. Examples: "scheduled__2023-03-26T00:00:00+00:00" or "manual__2023-03-26T00:00:00+00:00".
    :param subdirs: The folder path structure (if any) to create inside the workspace. e.g. 'download' or 'transform'
    :return: the path of the workflow folder
    """

    path = os.path.join(get_data_path(), dag_id, run_id, *subdirs)
    os.makedirs(path, exist_ok=True)
    return path


def fetch_dag_bag(path: str, include_examples: bool = False) -> DagBag:
    """Load a DAG Bag from a given path.

    :param path: the path to the DAG bag.
    :param include_examples: whether to include example DAGs or not.
    :return: None.
    """
    logging.info(f"Loading DAG bag from path: {path}")
    dag_bag = DagBag(path, include_examples=include_examples)

    if dag_bag is None:
        raise Exception(f"DagBag could not be loaded from path: {path}")

    if len(dag_bag.import_errors):
        # Collate loading errors as single string and raise it as exception
        results = []
        for path, exception in dag_bag.import_errors.items():
            results.append(f"DAG import exception: {path}\n{exception}\n\n")
        raise Exception("\n".join(results))

    return dag_bag


def cleanup(dag_id: str, execution_date: str, workflow_folder: str = None, retention_days=31) -> None:
    """Delete all files, folders and XComs associated from a release.

    :param dag_id: The ID of the DAG to remove XComs
    :param execution_date: The execution date of the DAG run
    :param workflow_folder: The top-level workflow folder to clean up
    :param retention_days: How many days of Xcom messages to retain
    """
    if workflow_folder:
        try:
            shutil.rmtree(workflow_folder)
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {workflow_folder}: {e}")

    delete_old_xcoms(dag_id=dag_id, execution_date=execution_date, retention_days=retention_days)


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
        class_name: the fully qualified class name for the workflow class. e.g. myproject.myworkflows.my_workflow
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
    def parse_workflows(list: Dict) -> List[Workflow]:
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
