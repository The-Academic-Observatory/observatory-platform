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
import json
import os
import tempfile
import uuid
from typing import Optional, Union, List

import pendulum
from airflow.exceptions import AirflowException

from observatory_platform.airflow.workflow import make_workflow_folder
from observatory_platform.google.gcs import gcs_upload_file, gcs_read_blob

DATE_TIME_FORMAT = "YYYY-MM-DD_HH:mm:ss"


def make_snapshot_date(**kwargs) -> pendulum.DateTime:
    """Make a snapshot date"""

    return kwargs["data_interval_end"]


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


def release_blob(id: str) -> str:
    """Generates the blob for a release object"""

    return f"releases/{id}.json"


def release_to_bucket(data: Union[dict, List[dict]], bucket: str, id: Optional[str] = None) -> str:
    """Uploads a release object to a bucket in json format. Will put it in {bucket}/releases.

    :param data: The release object
    :param bucket: The name of the bucket to upload to
    :param id: The id to use as an identifier. Will be generated if not supplied.
    :return: The id as a string
    """

    if not id:
        id = str(uuid.uuid4())

    with tempfile.NamedTemporaryFile(mode="w") as f:
        f.write(json.dumps(data))
        f.flush()  # Force write stream to file
        success, _ = gcs_upload_file(bucket_name=bucket, blob_name=release_blob(id), file_path=f.name)
    if not success:
        raise RuntimeError(f"Release could not be uploaded to gs://{bucket}/{id}.json")

    return id


def release_from_bucket(bucket: str, id: str) -> Union[dict, List[dict]]:
    """Downloads a release from a bucket.

    :param bucket: The name of the bucket containing the release
    :param id: The id of the release
    :return: The content of the release as a json dictionary or list of dicts
    """

    blob_name = release_blob(id)
    content, success = gcs_read_blob(bucket_name=bucket, blob_name=blob_name)
    if not success:
        raise RuntimeError(f"Release at gs://{bucket}/{blob_name} could not be downloaded")

    return json.loads(content)


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
        """Get the path to the workflow folder, namespaced to a DAG run. Can contain multiple release folders.

        :return: path to folder.
        """

        return make_workflow_folder(self.dag_id, self.run_id)

    @property
    def release_folder(self):
        """Get the path to the release folder, which resides inside the workflow folder.

        :return: path to folder.
        """

        raise NotImplementedError("self.release_folder should be implemented by subclasses")

    @property
    def download_folder(self):
        """Get the path to the download folder, which contains downloaded files. Resides in a release folder.

        :return: path to folder.
        """

        path = os.path.join(self.release_folder, "download")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def extract_folder(self):
        """Get the path to the extract folder, which contains extracted files. Resides in a release folder.

        :return: path to folder.
        """

        path = os.path.join(self.release_folder, "extract")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def transform_folder(self):
        """Get the path to the transform folder, which contains transformed files. Resides in a release folder.

        :return: path to folder.
        """

        path = os.path.join(self.release_folder, "transform")
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def from_dict(_dict: dict):
        """Converts the release dict to its object equivalent"""
        raise NotImplementedError("from_dict() not implemented for this Release object")

    def to_dict(self) -> dict:
        """Transforms the release to its dictionary equivalent"""
        raise NotImplementedError("to_dict() not implemented for this Release object")

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
        """Get the path to the release folder, which resides inside the workflow folder.

        :return: path to folder.
        """

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
        """Get the path to the release folder, which resides inside the workflow folder.

        :return: path to folder.
        """

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
        """Get the path to the release folder, which resides inside the workflow folder.

        :return: path to folder.
        """

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
