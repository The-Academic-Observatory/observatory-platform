# Copyright 2021 Curtin University
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

# Author: Tuan Chien, Aniek Roelofs

import os
from datetime import datetime, timezone
from functools import partial
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock
from copy import deepcopy

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowNotFoundException, AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from observatory_platform.airflow.workflow import CloudWorkspace, cleanup
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase, find_free_port

from observatory_platform.airflow.release import (
    Release,
    make_snapshot_date,
    set_task_state,
    check_workflow_inputs,
)



class TestAirflow(unittest.TestCase):
    @patch("observatory_platform.airflow.airflow.Variable.get")
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
            src = test_fixtures_path("utils", "bad_dag.py")
            shutil.copy(src, os.path.join(t, "dags.py"))
            with self.assertRaises(Exception):
                fetch_dag_bag(t)

            # Copy Good DAGs to folder
            src = test_fixtures_path("utils", "good_dag.py")
            shutil.copy(src, os.path.join(t, "dags.py"))

            # DAGs found
            expected_dag_ids = {"hello", "world"}
            dag_bag = fetch_dag_bag(t)
            actual_dag_ids = set(dag_bag.dag_ids)
            self.assertSetEqual(expected_dag_ids, actual_dag_ids)
#
# class MockWorkflow(Workflow):
#     """
#     Generic Workflow telescope for running tasks.
#     """
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#
#     def make_release(self, **kwargs) -> Release:
#         return Release(dag_id=self.dag_id, run_id=kwargs["run_id"])
#
#     def setup_task(self, **kwargs) -> bool:
#         return True
#
#     def task(self, release: Release, **kwargs):
#         pass
#
#     def task5(self, release: Release, **kwargs):
#         pass
#
#     def task6(self, release: Release, **kwargs):
#         pass
#
#
# class TestCallbackWorkflow(Workflow):
#     def __init__(
#         self, dag_id: str, start_date: pendulum.DateTime, schedule: str, max_retries: int, airflow_conns: list
#     ):
#         super().__init__(dag_id, start_date, schedule, max_retries=max_retries, airflow_conns=airflow_conns)
#         self.add_setup_task(self.check_dependencies)
#
#     def make_release(self, **kwargs):
#         return


class TestWorkflowFunctions(SandboxTestCase):
    def test_set_task_state(self):
        """Test set_task_state"""

        task_id = "test_task"
        set_task_state(True, task_id)
        with self.assertRaises(AirflowException):
            set_task_state(False, task_id)

    @patch("observatory_platform.airflow.airflow.Variable.get")
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

    def test_make_snapshot_date(self):
        """Test make_table_name"""

        data_interval_end = pendulum.datetime(2021, 11, 11)
        expected_date = pendulum.datetime(2021, 11, 11)
        actual_date = make_snapshot_date(**{"data_interval_end": data_interval_end})
        self.assertEqual(expected_date, actual_date)

    def test_cleanup(self):
        """
        Tests the cleanup function.
        Creates a task and pushes and Xcom. Also creates a fake workflow directory.
        Both the Xcom and the directory should be deleted by the cleanup() function
        """

        def create_xcom(**kwargs):
            ti = kwargs["ti"]
            execution_date = kwargs["execution_date"]
            ti.xcom_push("topic", {"snapshot_date": execution_date.format("YYYYMMDD"), "something": "info"})

        env = SandboxEnvironment(enable_api=False)
        with env.create():
            execution_date = pendulum.datetime(2023, 1, 1)
            with DAG(
                dag_id="test_dag",
                schedule="@daily",
                default_args={"owner": "airflow", "start_date": execution_date},
                catchup=True,
            ) as dag:
                kwargs = {"task_id": "create_xcom"}
                op = PythonOperator(python_callable=create_xcom, **kwargs)

            with TemporaryDirectory() as workflow_dir:
                # Create some files in the workflow folder
                subdir = os.path.join(workflow_dir, "test_directory")
                os.mkdir(subdir)

                # DAG Run
                with env.create_dag_run(dag=dag, execution_date=execution_date):
                    ti = env.run_task("create_xcom")
                    self.assertEqual("success", ti.state)
                    msgs = ti.xcom_pull(key="topic", task_ids="create_xcom", include_prior_dates=True)
                    self.assertIsInstance(msgs, dict)
                    cleanup("test_dag", execution_date, workflow_folder=workflow_dir, retention_days=0)
                    msgs = ti.xcom_pull(key="topic", task_ids="create_xcom", include_prior_dates=True)
                    self.assertEqual(msgs, None)
                    self.assertEqual(os.path.isdir(subdir), False)
                    self.assertEqual(os.path.isdir(workflow_dir), False)


class TestWorkflow(SandboxTestCase):
    """Tests the Telescope."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super().__init__(*args, **kwargs)
        self.dag_id = "dag_id"
        self.start_date = pendulum.datetime(2020, 1, 1)
        self.schedule = "@weekly"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.host = "localhost"
        self.port = find_free_port()

    def test_make_task_id(self):
        """Test make_task_id"""

        def test_func():
            pass

        # task_id is specified as kwargs
        expected_task_id = "hello"
        actual_task_id = make_task_id(test_func, {"task_id": expected_task_id})
        self.assertEqual(expected_task_id, actual_task_id)

        # task_id not specified in kwargs
        expected_task_id = "test_func"
        actual_task_id = make_task_id(test_func, {})
        self.assertEqual(expected_task_id, actual_task_id)

    def dummy_func(self):
        pass

    def test_add_operator(self):
        workflow = MockWorkflow(
            dag_id="1", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule="@daily"
        )
        op1 = ExternalTaskSensor(
            external_dag_id="1", task_id="test", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )
        op2 = ExternalTaskSensor(
            external_dag_id="1", task_id="test2", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )

        with workflow.parallel_tasks():
            workflow.add_operator(op1)
            workflow.add_operator(op2)
        workflow.add_task(self.dummy_func)
        dag = workflow.make_dag()

        self.assert_dag_structure({"dummy_func": [], "test": ["dummy_func"], "test2": ["dummy_func"]}, dag)

    def test_workflow_tags(self):
        workflow = MockWorkflow(
            dag_id="1",
            start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc),
            schedule="@daily",
            tags=["oaebu"],
        )

        self.assertEqual(workflow.dag.tags, ["oaebu"])

    def test_make_dag(self):
        """Test making DAG"""
        # Test adding tasks from Telescope methods
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule)
        telescope.add_setup_task(telescope.setup_task)
        telescope.add_task(telescope.task)
        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(2, len(dag.tasks))
        for task in dag.tasks:
            self.assertIsInstance(task, BaseOperator)

        # Test adding tasks from partial Telescope methods
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule)
        for i in range(2):
            setup_task = partial(telescope.task, somearg="test")
            setup_task.__name__ = f"setup_task_{i}"
            telescope.add_setup_task(setup_task)
        for i in range(2):
            task = partial(telescope.task, somearg="test")
            task.__name__ = f"task_{i}"
            telescope.add_task(task)
        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(4, len(dag.tasks))
        for task in dag.tasks:
            self.assertIsInstance(task, BaseOperator)

        # Test adding tasks with custom kwargs
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule)
        telescope.add_setup_task(telescope.setup_task, trigger_rule="none_failed")
        telescope.add_task(telescope.task, trigger_rule="none_failed")
        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(2, len(dag.tasks))
        for task in dag.tasks:
            self.assertIsInstance(task, BaseOperator)
            self.assertEqual("none_failed", task.trigger_rule)

        # Test adding tasks with custom operator
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule)
        task_id = "bash_task"
        telescope.add_operator(BashOperator(task_id=task_id, bash_command="echo 'hello'"))
        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(1, len(dag.tasks))
        for task in dag.tasks:
            self.assertEqual(task_id, task.task_id)
            self.assertIsInstance(task, BashOperator)

        # Test adding parallel tasks
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule)
        telescope.add_setup_task(telescope.setup_task)
        with telescope.parallel_tasks():
            telescope.add_task(telescope.task, task_id="task1")
            telescope.add_task(telescope.task, task_id="task2")
        telescope.add_task(telescope.task, task_id="join1")
        with telescope.parallel_tasks():
            telescope.add_task(telescope.task, task_id="task3")
            telescope.add_task(telescope.task, task_id="task4")
        telescope.add_task(telescope.task, task_id="join2")
        with telescope.parallel_tasks():
            telescope.add_task(telescope.task5)
            telescope.add_task(telescope.task6)

        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(9, len(dag.tasks))
        self.assert_dag_structure(
            {
                "setup_task": ["task1", "task2"],
                "task1": ["join1"],
                "task2": ["join1"],
                "join1": ["task3", "task4"],
                "task3": ["join2"],
                "task4": ["join2"],
                "join2": ["task5", "task6"],
                "task5": [],
                "task6": [],
            },
            dag,
        )

        # Test parallel tasks function
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule)
        self.assertFalse(telescope._parallel_tasks)
        with telescope.parallel_tasks():
            self.assertTrue(telescope._parallel_tasks)
        self.assertFalse(telescope._parallel_tasks)

    def test_telescope(self):
        """Basic test to make sure that the Workflow class can execute in an Airflow environment."""
        # Setup Observatory environment
        env = SandboxEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            task1 = "task1"
            task2 = "task2"
            expected_date = "success"

            workflow = MockWorkflow(self.dag_id, self.start_date, self.schedule)
            workflow.add_setup_task(workflow.setup_task)
            workflow.add_task(workflow.task, task_id=task1)
            workflow.add_operator(BashOperator(task_id=task2, bash_command="echo 'hello'"))

            dag = workflow.make_dag()
            with env.create_dag_run(dag, self.start_date):
                ti = env.run_task(workflow.setup_task.__name__)
                self.assertEqual(expected_date, ti.state)

                ti = env.run_task(task1)
                self.assertEqual(expected_date, ti.state)

                ti = env.run_task(task2)
                self.assertEqual(expected_date, ti.state)

    @patch("observatory_platform.airflow.airflow.send_slack_msg")
    def test_callback(self, mock_send_slack_msg):
        """Test that the on_failure_callback function is successfully called in a production environment when a task
        fails

        :param mock_send_slack_msg: Mock send_slack_msg function
        :return: None.
        """
        # mock_send_slack_msg.return_value = Mock(spec=SlackWebhookHook)

        # Setup Observatory environment
        env = SandboxEnvironment(self.project_id, self.data_location)

        # Setup Workflow with 0 retries and missing airflow variable, so it will fail the task
        execution_date = pendulum.datetime(2020, 1, 1)
        conn_id = "orcid_bucket"
        workflow = TestCallbackWorkflow(
            "test_callback",
            execution_date,
            self.schedule,
            max_retries=0,
            airflow_conns=[conn_id],
        )
        dag = workflow.make_dag()

        # Create the Observatory environment and run task, expecting slack webhook call in production environment
        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):
                with self.assertRaises(AirflowNotFoundException):
                    env.run_task(workflow.check_dependencies.__name__)

                _, callkwargs = mock_send_slack_msg.call_args
                self.assertTrue(
                    "airflow.exceptions.AirflowNotFoundException: The conn_id `orcid_bucket` isn't defined"
                    in callkwargs["comments"]
                )

        # Reset mock
        mock_send_slack_msg.reset_mock()

        # Add orcid_bucket connection and test that Slack Web Hook did not get triggered
        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):
                env.add_connection(Connection(conn_id=conn_id, uri="https://orcid.org/"))
                env.run_task(workflow.check_dependencies.__name__)
                mock_send_slack_msg.assert_not_called()


# Copyright 2019 Curtin University. All Rights Reserved.
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

# Author: James Diprose, Aniek Roelofs

import random
import string
import unittest

import pendulum
import yaml

from observatory_platform.observatory_config import (
    Workflow,
    workflows_to_json_string,
    json_string_to_workflows,
)


class TestObservatoryConfigValidator(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = dict()
        self.schema["google_cloud"] = {
            "required": True,
            "type": "dict",
            "schema": {"credentials": {"required": True, "type": "string", "google_application_credentials": True}},
        }

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


def tmp_config_file(dict_: dict) -> str:
    """
    Dumps dict into a yaml file that is saved in a randomly named file. Used to as config file to create
    ObservatoryConfig instance.
    :param dict_: config dict
    :return: path of temporary file
    """
    content = yaml.safe_dump(dict_).replace("'!", "!").replace("':", ":")
    file_name = "".join(random.choices(string.ascii_lowercase, k=10))
    with open(file_name, "w") as f:
        f.write(content)
    return file_name
