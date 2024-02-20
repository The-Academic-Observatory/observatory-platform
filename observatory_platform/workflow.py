# Copyright 2020 Curtin University
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

# Author: Aniek Roelofs, James Diprose, Tuan Chien

from __future__ import annotations

import logging
import shutil
from typing import Callable, Dict

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol
import os
import pendulum

from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from observatory_platform.airflow import (
    delete_old_xcoms,
)
from observatory_platform.airflow import get_data_path
from observatory_platform.observatory_config import CloudWorkspace

DATE_TIME_FORMAT = "YYYY-MM-DD_HH:mm:ss"


def make_snapshot_date(**kwargs) -> pendulum.DateTime:
    """Make a snapshot date"""

    return kwargs["data_interval_end"]


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


def check_workflow_inputs(workflow: Workflow, check_cloud_workspace=True) -> None:
    """Checks a Workflow object for validity

    :param workflow: The Workflow object
    :param check_cloud_workspace: Whether to check the CloudWorkspace field, defaults to True
    :raises AirflowException: Raised if there are invalid fields
    """
    invalid_fields = []
    if not workflow.dag_id or not isinstance(workflow.dag_id, str):
        invalid_fields.append("dag_id")

    if check_cloud_workspace:
        cloud_workspace = workflow.cloud_workspace
        if not isinstance(cloud_workspace, CloudWorkspace):
            invalid_fields.append("cloud_workspace")
        else:
            required_fields = {"project_id": str, "data_location": str, "download_bucket": str, "transform_bucket": str}
            for field_name, field_type in required_fields.items():
                field_value = getattr(cloud_workspace, field_name, None)
                if not isinstance(field_value, field_type) or not field_value:
                    invalid_fields.append(f"cloud_workspace.{field_name}")

            if cloud_workspace.output_project_id is not None:
                if not isinstance(cloud_workspace.output_project_id, str) or not cloud_workspace.output_project_id:
                    invalid_fields.append("cloud_workspace.output_project_id")

    if invalid_fields:
        raise AirflowException(f"Workflow input fields invalid: {invalid_fields}")


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


class WorkflowBashOperator(BashOperator):
    def __init__(self, workflow: Workflow, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.workflow = workflow

    def render_template(self, content, context, *args, **kwargs):
        # Make release and set in context
        obj = self.workflow.make_release(**context)
        if isinstance(obj, list):
            context["releases"] = obj
        elif isinstance(obj, Release):
            context["release"] = obj

        # Add workflow to context
        if self.workflow is not None:
            context["workflow"] = self.workflow

        else:
            raise AirflowException(
                f"WorkflowBashOperator.render_template: self.make_release returned an object of an invalid type (should be a list of Releases, or a single Release object): {type(obj)}"
            )

        return super().render_template(content, context, *args, **kwargs)


class Release:
    def __init__(self, *, dag_id: str, run_id: str):
        """Construct a Release instance

        :param dag_id: the DAG ID.
        :param run_id: the DAG's run ID.
        """

        self.dag_id = dag_id
        self.run_id = run_id

    @property
    def workflow_folder(self):
        return make_workflow_folder(self.dag_id, self.run_id)

    @property
    def release_folder(self):
        raise NotImplementedError("self.release_folder should be implemented by subclasses")

    @property
    def download_folder(self):
        path = os.path.join(self.release_folder, "download")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def extract_folder(self):
        path = os.path.join(self.release_folder, "extract")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def transform_folder(self):
        path = os.path.join(self.release_folder, "transform")
        os.makedirs(path, exist_ok=True)
        return path

    def __str__(self):
        return f"Release(dag_id={self.dag_id}, run_id={self.run_id})"


class SnapshotRelease(Release):
    def __init__(
            self,
            *,
            dag_id: str,
            run_id: str,
            snapshot_date: pendulum.DateTime,
    ):
        """Construct a SnapshotRelease instance

        :param dag_id: the DAG ID.
        :param run_id: the DAG's run ID.
        :param snapshot_date: the release date of the snapshot.
        """

        super().__init__(dag_id=dag_id, run_id=run_id)
        self.snapshot_date = snapshot_date

    @property
    def release_folder(self):
        return make_workflow_folder(self.dag_id, self.run_id, f"snapshot_{self.snapshot_date.format(DATE_TIME_FORMAT)}")

    def __str__(self):
        return f"SnapshotRelease(dag_id={self.dag_id}, run_id={self.run_id}, snapshot_date={self.snapshot_date})"


class PartitionRelease(Release):
    def __init__(
            self,
            *,
            dag_id: str,
            run_id: str,
            partition_date: pendulum.DateTime,
    ):
        """Construct a PartitionRelease instance

        :param dag_id: the DAG ID.
        :param run_id: the DAG's run ID.
        :param partition_date: the release date of the partition.
        """

        super().__init__(dag_id=dag_id, run_id=run_id)
        self.partition_date = partition_date

    @property
    def release_folder(self):
        return make_workflow_folder(
            self.dag_id, self.run_id, f"partition_{self.partition_date.format(DATE_TIME_FORMAT)}"
        )

    def __str__(self):
        return f"PartitionRelease(dag_id={self.dag_id}, run_id={self.run_id}, partition_date={self.partition_date})"


class ChangefileRelease(Release):
    def __init__(
            self,
            *,
            dag_id: str,
            run_id: str,
            start_date: pendulum.DateTime = None,
            end_date: pendulum.DateTime = None,
            sequence_start: int = None,
            sequence_end: int = None,
    ):
        """Construct a ChangefileRelease instance

        :param dag_id: the DAG ID.
        :param run_id: the DAG's run ID.
        :param start_date: the date of the first changefile processed in this release.
        :param end_date: the date of the last changefile processed in this release.
        :param sequence_start: the starting sequence number of files that make up this release.
        :param sequence_end: the end sequence number of files that make up this release.
        """

        super().__init__(dag_id=dag_id, run_id=run_id)
        self.start_date = start_date
        self.end_date = end_date
        self.sequence_start = sequence_start
        self.sequence_end = sequence_end

    @property
    def release_folder(self):
        return make_workflow_folder(
            self.dag_id,
            self.run_id,
            f"changefile_{self.start_date.format(DATE_TIME_FORMAT)}_to_{self.end_date.format(DATE_TIME_FORMAT)}",
        )

    def __str__(self):
        return (
            f"Release(dag_id={self.dag_id}, run_id={self.run_id}, start_date={self.start_date}, "
            f"end_date={self.end_date}, sequence_start={self.sequence_start}, sequence_end={self.sequence_end})"
        )


def set_task_state(success: bool, task_id: str, release: Release = None):
    """Update the state of the Airflow task.
    :param success: whether the task was successful or not.
    :param task_id: the task id.
    :param release: the release being processed. Optional.
    :return: None.
    """

    if success:
        msg = f"{task_id}: success"
        if release is not None:
            msg += f" {release}"
        logging.info(msg)
    else:
        msg_failed = f"{task_id}: failed"
        if release is not None:
            msg_failed += f" {release}"
        logging.error(msg_failed)
        raise AirflowException(msg_failed)


def make_task_id(func: Callable, kwargs: Dict) -> str:
    """Set a task_id from a func or kwargs.

    :param func: the task function.
    :param kwargs: the task kwargs parameter.
    :return: the task id.
    """

    task_id_key = "task_id"

    if task_id_key in kwargs:
        task_id = kwargs["task_id"]
    else:
        task_id = func.__name__

    return task_id
