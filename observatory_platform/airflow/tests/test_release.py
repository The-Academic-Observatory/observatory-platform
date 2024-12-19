# Copyright 2019-2024 Curtin Universityrelease_blob(id)
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

import os
from random import randint
import json
import uuid
import tempfile

import pendulum
from airflow.exceptions import AirflowException

from observatory_platform.airflow.release import (
    Release,
    set_task_state,
    make_snapshot_date,
    release_to_bucket,
    release_from_bucket,
)
from observatory_platform.google.gcs import gcs_list_blobs, gcs_upload_file
from observatory_platform.sandbox.test_utils import SandboxTestCase
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment


class _MyRelease(Release):
    def __init__(self, dag_id: str, run_id: str, my_int: int, my_time: pendulum.DateTime):
        super().__init__(dag_id=dag_id, run_id=run_id)
        self.my_int = my_int
        self.my_time = my_time

    def to_dict(self):
        return dict(dag_id=self.dag_id, run_id=self.run_id, my_int=self.my_int, my_time=self.my_time.timestamp())

    @staticmethod
    def from_dict(dict_: dict):
        return _MyRelease(dict_["dag_id"], dict_["run_id"], dict_["my_int"], pendulum.from_timestamp(dict_["my_time"]))

    def __str__(self):
        return f"{self.dag_id}, {self.run_id}, {self.my_int}, {self.my_time.timestamp()}"

    @staticmethod
    def _gen_release():
        return _MyRelease(
            dag_id="test_dag",
            run_id=str(uuid.uuid4()),
            my_int=randint(-10e9, 10e9),
            my_time=pendulum.datetime(randint(0, 2000), 1, 1),
        )


class TestGCSFunctions(SandboxTestCase):

    gcp_project_id = os.getenv("TEST_GCP_PROJECT_ID")
    gcp_data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_release_to_bucket(self):
        env = SandboxEnvironment(project_id=self.gcp_project_id, data_location=self.gcp_data_location)
        bucket = env.add_bucket()
        release = _MyRelease._gen_release()
        with env.create():
            id = release_to_bucket(release.to_dict(), bucket)
            blobs = [b.name for b in gcs_list_blobs(bucket)]
            self.assertIn(f"releases/{id}.json", blobs)

    def test_release_from_bucket(self):
        env = SandboxEnvironment(project_id=self.gcp_project_id, data_location=self.gcp_data_location)
        bucket = env.add_bucket()
        id = "test_release"
        release = _MyRelease._gen_release()
        with env.create():
            with tempfile.NamedTemporaryFile(mode="w") as f:
                f.write(json.dumps(release.to_dict()))
                f.flush()  # Force write stream to file
                gcs_upload_file(bucket_name=bucket, blob_name=f"releases/{id}.json", file_path=f.name)
            dl_release = release_from_bucket(bucket, id)
            self.assertEqual(str(release), str(_MyRelease.from_dict(dl_release)))


class TestWorkflow(SandboxTestCase):
    def test_make_snapshot_date(self):
        """Test make_table_name"""

        data_interval_end = pendulum.datetime(2021, 11, 11)
        expected_date = pendulum.datetime(2021, 11, 11)
        actual_date = make_snapshot_date(**{"data_interval_end": data_interval_end})
        self.assertEqual(expected_date, actual_date)

    def test_set_task_state(self):
        """Test set_task_state"""

        task_id = "test_task"
        set_task_state(True, task_id)
        with self.assertRaises(AirflowException):
            set_task_state(False, task_id)
