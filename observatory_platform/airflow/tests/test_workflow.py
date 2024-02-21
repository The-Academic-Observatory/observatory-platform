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

# Author: James Diprose, Aniek Roelofs, Tuan-Chien

import os
import shutil
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pendulum
from airflow.exceptions import AirflowException

from observatory_platform.airflow.workflow import (
    Workflow,
    workflows_to_json_string,
    json_string_to_workflows,
)
from observatory_platform.airflow.workflow import get_data_path, fetch_dag_bag, make_workflow_folder
from observatory_platform.config import module_file_path
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase


class TestWorkflow(SandboxTestCase):

    def __init__(self, *args, **kwargs):
        super(TestWorkflow, self).__init__(*args, **kwargs)
        self.fixtures_path = module_file_path("observatory_platform.airflow.tests.fixtures")

    @patch("observatory_platform.airflow.workflow.Variable.get")
    def test_get_data_path(self, mock_variable_get):
        """Tests the function that retrieves the data_path airflow variable"""
        # 1 - no variable available
        mock_variable_get.return_value = None
        self.assertRaises(AirflowException, get_data_path)

        # 2 - available in Airflow variable
        mock_variable_get.return_value = "env_return"
        self.assertEqual("env_return", get_data_path())

    def test_fetch_dag_bag(self):
        """Test fetch_dag_bag"""

        env = SandboxEnvironment()
        with env.create() as t:
            # No DAGs found
            dag_bag = fetch_dag_bag(t)
            print(f"DAGS found on path: {t}")
            for dag_id in dag_bag.dag_ids:
                print(f"  {dag_id}")
            self.assertEqual(0, len(dag_bag.dag_ids))

            # Bad DAG
            src = os.path.join(self.fixtures_path, "bad_dag.py")
            shutil.copy(src, os.path.join(t, "dags.py"))
            with self.assertRaises(Exception):
                fetch_dag_bag(t)

            # Copy Good DAGs to folder
            src = os.path.join(self.fixtures_path, "good_dag.py")
            shutil.copy(src, os.path.join(t, "dags.py"))

            # DAGs found
            expected_dag_ids = {"hello", "world"}
            dag_bag = fetch_dag_bag(t)
            actual_dag_ids = set(dag_bag.dag_ids)
            self.assertSetEqual(expected_dag_ids, actual_dag_ids)

    @patch("observatory_platform.airflow.workflow.Variable.get")
    def test_make_workflow_folder(self, mock_get_variable):
        """Tests the make_workflow_folder function"""
        with TemporaryDirectory() as tempdir:
            mock_get_variable.return_value = tempdir
            run_id = "scheduled__2023-03-26T00:00:00+00:00"  # Also can look like: "manual__2023-03-26T00:00:00+00:00"
            path = make_workflow_folder("test_dag", run_id, "sub_folder", "subsub_folder")
            self.assertEqual(
                path,
                os.path.join(tempdir, f"test_dag/scheduled__2023-03-26T00:00:00+00:00/sub_folder/subsub_folder"),
            )

    def test_workflows_to_json_string(self):
        workflows = [
            Workflow(
                dag_id="my_dag",
                name="My DAG",
                class_name="observatory_platform.workflows.vm_workflow.VmCreateWorkflow",
                kwargs=dict(dt=pendulum.datetime(2021, 1, 1)),
            )
        ]
        json_string = workflows_to_json_string(workflows)
        self.assertEqual(
            '[{"dag_id": "my_dag", "name": "My DAG", "class_name": "observatory_platform.workflows.vm_workflow.VmCreateWorkflow", "cloud_workspace": null, "kwargs": {"dt": "2021-01-01T00:00:00+00:00"}}]',
            json_string,
        )

    def test_json_string_to_workflows(self):
        json_string = '[{"dag_id": "my_dag", "name": "My DAG", "class_name": "observatory_platform.workflows.vm_workflow.VmCreateWorkflow", "cloud_workspace": null, "kwargs": {"dt": "2021-01-01T00:00:00+00:00"}}]'
        actual_workflows = json_string_to_workflows(json_string)
        self.assertEqual(
            [
                Workflow(
                    dag_id="my_dag",
                    name="My DAG",
                    class_name="observatory_platform.workflows.vm_workflow.VmCreateWorkflow",
                    kwargs=dict(dt=pendulum.datetime(2021, 1, 1)),
                )
            ],
            actual_workflows,
        )
