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
from unittest.mock import ANY, Mock, patch

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowNotFoundException
from airflow.models.baseoperator import BaseOperator
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.sensors.external_task import ExternalTaskSensor
from observatory.platform.observatory_config import Environment
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
)
from observatory.platform.workflows.workflow import Release, Workflow, make_task_id
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.telescope import Telescope
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.table_type import TableType
from observatory.api.client.model.telescope_type import TelescopeType


class MockWorkflow(Workflow):
    """
    Generic Workflow telescope for running tasks.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dag_id = "dag_id"
        self.release_id = "20210101"

    def make_release(self, **kwargs) -> Release:
        return Release(self.dag_id, self.release_id)

    def setup_task(self, **kwargs) -> bool:
        return True

    def task(self, release: Release, **kwargs):
        pass

    def task5(self, release: Release, **kwargs):
        pass

    def task6(self, release: Release, **kwargs):
        pass


class TestCallbackWorkflow(Workflow):
    def __init__(
        self, dag_id: str, start_date: pendulum.DateTime, schedule_interval: str, max_retries: int, airflow_conns: list
    ):
        super().__init__(dag_id, start_date, schedule_interval, max_retries=max_retries, airflow_conns=airflow_conns)
        self.add_setup_task(self.check_dependencies)

    def make_release(self, **kwargs):
        return


class TestWorkflow(ObservatoryTestCase):
    """Tests the Telescope."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super().__init__(*args, **kwargs)
        self.dag_id = "dag_id"
        self.start_date = pendulum.datetime(2020, 1, 1)
        self.schedule_interval = "@weekly"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"
        self.telescope_id = 1

    def setup_api(self):
        org = Organisation(name=self.org_name)
        result = self.api.put_organisation(org)
        self.assertIsInstance(result, Organisation)

        tele_type = TelescopeType(type_id="tele_type", name="My Telescope")
        result = self.api.put_telescope_type(tele_type)
        self.assertIsInstance(result, TelescopeType)

        telescope = Telescope(organisation=Organisation(id=1), telescope_type=TelescopeType(id=1))
        result = self.api.put_telescope(telescope)
        self.assertIsInstance(result, Telescope)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            name="My dataset type",
            type_id="type id",
            table_type=TableType(id=1),
        )

        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="My dataset",
            service="bigquery",
            address="project.dataset.table",
            connection=Telescope(id=1),
            dataset_type=DatasetType(id=1),
        )
        result = self.api.put_dataset(dataset)
        self.assertIsInstance(result, Dataset)

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

    def test_make_dag(self):
        """Test making DAG"""
        # Test adding tasks from Telescope methods
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule_interval)
        telescope.add_setup_task(telescope.setup_task)
        telescope.add_task(telescope.task)
        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(2, len(dag.tasks))
        for task in dag.tasks:
            self.assertIsInstance(task, BaseOperator)

        # Test adding tasks from partial Telescope methods
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule_interval)
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
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule_interval)
        telescope.add_setup_task(telescope.setup_task, trigger_rule="none_failed")
        telescope.add_task(telescope.task, trigger_rule="none_failed")
        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(2, len(dag.tasks))
        for task in dag.tasks:
            self.assertIsInstance(task, BaseOperator)
            self.assertEqual("none_failed", task.trigger_rule)

        # Test adding tasks with custom operator
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule_interval)
        task_id = "bash_task"
        telescope.add_operator(BashOperator(task_id=task_id, bash_command="echo 'hello'"))
        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(1, len(dag.tasks))
        for task in dag.tasks:
            self.assertEqual(task_id, task.task_id)
            self.assertIsInstance(task, BashOperator)

        # Test adding parallel tasks
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule_interval)
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
            telescope.add_task_chain([telescope.task5, telescope.task6])

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
        telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule_interval)
        self.assertFalse(telescope._parallel_tasks)
        with telescope.parallel_tasks():
            self.assertTrue(telescope._parallel_tasks)
        self.assertFalse(telescope._parallel_tasks)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    @patch("observatory.platform.workflows.workflow.get_datasets")
    def test_add_new_dataset_releases(self, m_get_datasets, m_makeapi):
        m_makeapi.return_value = self.api

        with self.env.create():
            self.setup_api()
            dataset = Dataset(
                id=1,
                name="My dataset",
                service="bigquery",
                address="project.dataset.table",
                connection=Telescope(id=1),
                dataset_type=DatasetType(id=1),
            )
            m_get_datasets.return_value = [dataset]

            telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule_interval)
            self.assertRaises(Exception, telescope.add_new_dataset_releases, None)

            telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule_interval, workflow_id=1)

            # No releases
            telescope.add_new_dataset_releases([])

            # Single 'release'
            release = Release(dag_id="dag_id", release_id="20220101")
            telescope.add_new_dataset_releases(release)

            # Single 'releases'
            release = [Release(dag_id="dag_id", release_id="20220101")]
            telescope.add_new_dataset_releases(release)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_telescope(self, m_makeapi):
        """Basic test to make sure that the Workflow class can execute in an Airflow environment.
        :return: None.
        """

        m_makeapi.return_value = self.api

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location, api_host=self.host, api_port=self.port)

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            self.setup_api()

            task1 = "task1"
            task2 = "task2"
            expected_date = "success"

            telescope = MockWorkflow(self.dag_id, self.start_date, self.schedule_interval, workflow_id=1)
            telescope.add_setup_task(telescope.setup_task)
            telescope.add_task(telescope.task, task_id=task1)
            telescope.add_operator(BashOperator(task_id=task2, bash_command="echo 'hello'"))
            telescope.add_task(telescope.add_new_dataset_releases)

            dag = telescope.make_dag()
            with env.create_dag_run(dag, self.start_date):
                ti = env.run_task(telescope.setup_task.__name__)
                self.assertEqual(expected_date, ti.state)

                ti = env.run_task(task1)
                self.assertEqual(expected_date, ti.state)

                ti = env.run_task(task2)
                self.assertEqual(expected_date, ti.state)

                ti = env.run_task(telescope.add_new_dataset_releases.__name__)
                self.assertEqual(expected_date, ti.state)
                releases = self.api.get_dataset_releases(limit=1000)
                self.assertEqual(len(releases), 1)
                self.assertEqual(pendulum.instance(releases[0].start_date), pendulum.datetime(2021, 1, 1))

    @patch("observatory.platform.utils.workflow_utils.send_slack_msg")
    def test_callback(self, mock_send_slack_msg):
        """Test that the on_failure_callback function is successfully called in a production environment when a task
        fails

        :param mock_send_slack_msg: Mock send_slack_msg function
        :return: None.
        """
        # mock_send_slack_msg.return_value = Mock(spec=SlackWebhookHook)

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)

        # Setup Telescope with 0 retries and missing airflow variable, so it will fail the task
        execution_date = pendulum.datetime(2020, 1, 1)
        telescope = TestCallbackWorkflow(
            "test_callback",
            execution_date,
            self.schedule_interval,
            max_retries=0,
            airflow_conns=[AirflowVars.ORCID_BUCKET],
        )
        dag = telescope.make_dag()

        # Create the Observatory environment and run task, expecting slack webhook call in production environment
        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):
                env.add_variable(Variable(key=AirflowVars.ENVIRONMENT, val=Environment.production.value))
                with self.assertRaises(AirflowNotFoundException):
                    env.run_task(telescope.check_dependencies.__name__)

                _, callkwargs = mock_send_slack_msg.call_args
                self.assertEqual(
                    callkwargs["comments"],
                    "Task failed, exception:\nairflow.exceptions.AirflowNotFoundException: The conn_id `orcid_bucket` isn't defined",
                )
                self.assertEqual(callkwargs["project_id"], self.project_id)

        # Reset mock
        mock_send_slack_msg.reset_mock()

        # Create the Observatory environment and run task, expecting no slack webhook call in develop environment
        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):
                env.add_variable(Variable(key=AirflowVars.ENVIRONMENT, val=Environment.develop.value))
                with self.assertRaises(AirflowNotFoundException):
                    env.run_task(telescope.check_dependencies.__name__)
                mock_send_slack_msg.assert_not_called()


class TestAddOperatorsTelescope(ObservatoryTestCase):
    """Tests the operator interface."""

    def dummy_func(self):
        pass

    def test_add_operator(self):
        mt = MockWorkflow(
            dag_id="1", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule_interval="daily"
        )
        tds = ExternalTaskSensor(
            external_dag_id="1", task_id="test", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )
        tds2 = ExternalTaskSensor(
            external_dag_id="1", task_id="test2", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )

        with mt.parallel_tasks():
            mt.add_operator(tds)
            mt.add_operator(tds2)
        mt.add_task(self.dummy_func)
        dag = mt.make_dag()

        self.assert_dag_structure({"dummy_func": [], "test": ["dummy_func"], "test2": ["dummy_func"]}, dag)

    def test_add_operators(self):
        mt = MockWorkflow(
            dag_id="1", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule_interval="daily"
        )
        tds = ExternalTaskSensor(
            external_dag_id="1", task_id="test", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )
        tds2 = ExternalTaskSensor(
            external_dag_id="1", task_id="test2", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )

        with mt.parallel_tasks():
            mt.add_operator_chain([tds, tds2])
        mt.add_task(self.dummy_func)
        dag = mt.make_dag()

        self.assert_dag_structure({"dummy_func": [], "test": ["dummy_func"], "test2": ["dummy_func"]}, dag)

    def test_add_operators_empty(self):
        mt = MockWorkflow(
            dag_id="1", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule_interval="daily"
        )
        mt.add_task(self.dummy_func)
        mt.add_operator_chain([])
        dag = mt.make_dag()

        self.assert_dag_structure({"dummy_func": []}, dag)
