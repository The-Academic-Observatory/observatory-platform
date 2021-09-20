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
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.sensors.external_task import ExternalTaskSensor
from observatory.platform.observatory_config import Environment
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase
from observatory.platform.workflows.workflow import Operator, Release, Workflow, make_task_id


class MockTelescope(Workflow):
    """
    Generic Workflow telescope for running tasks.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dag_id = "dag_id"
        self.release_id = "release_id"

    def make_release(self) -> Release:
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
    def __init__(self, dag_id: str, start_date: pendulum.DateTime, schedule_interval: str, max_retries: int,
                 airflow_conns: list):
        super().__init__(dag_id, start_date, schedule_interval, max_retries=max_retries,
                         airflow_conns=airflow_conns)
        self.add_setup_task(self.check_dependencies)

    def make_release(self, **kwargs):
        return


class TestTelescope(ObservatoryTestCase):
    """Tests the Telescope."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super().__init__(*args, **kwargs)
        self.dag_id = "dag_id"
        self.start_date = datetime(2020, 1, 1)
        self.schedule_interval = "@weekly"
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

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
        telescope = MockTelescope(self.dag_id, self.start_date, self.schedule_interval)
        telescope.add_setup_task(telescope.setup_task)
        telescope.add_task(telescope.task)
        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(2, len(dag.tasks))
        for task in dag.tasks:
            self.assertIsInstance(task, BaseOperator)

        # Test adding tasks from partial Telescope methods
        telescope = MockTelescope(self.dag_id, self.start_date, self.schedule_interval)
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
        telescope = MockTelescope(self.dag_id, self.start_date, self.schedule_interval)
        telescope.add_setup_task(telescope.setup_task, trigger_rule="none_failed")
        telescope.add_task(telescope.task, trigger_rule="none_failed")
        dag = telescope.make_dag()
        self.assertIsInstance(dag, DAG)
        self.assertEqual(2, len(dag.tasks))
        for task in dag.tasks:
            self.assertIsInstance(task, BaseOperator)
            self.assertEqual("none_failed", task.trigger_rule)

        # Test adding parallel tasks
        telescope = MockTelescope(self.dag_id, self.start_date, self.schedule_interval)
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
        telescope = MockTelescope(self.dag_id, self.start_date, self.schedule_interval)
        self.assertFalse(telescope._parallel_tasks)
        with telescope.parallel_tasks():
            self.assertTrue(telescope._parallel_tasks)
        self.assertFalse(telescope._parallel_tasks)

        # to_python_operators
        telescope = MockTelescope(self.dag_id, self.start_date, self.schedule_interval)
        operators = [
            Operator(telescope.task5, {}),
            Operator(telescope.task6, {}),
            Operator(telescope.task, {"task_id": "task7"}),
        ]
        expected_python_operator_ids = ["task5", "task6", "task7"]
        actual_python_operators = telescope.to_python_operators(operators)
        self.assertEqual(len(expected_python_operator_ids), len(actual_python_operators))
        for e_task_id, op, actual_op in zip(expected_python_operator_ids, operators, actual_python_operators):
            self.assertEqual(e_task_id, actual_op.task_id)
            self.assertEqual(op.func, actual_op.python_callable.args[0])

    @patch("observatory.platform.utils.workflow_utils.create_slack_webhook")
    def test_callback(self, mock_create_slack_webhook):
        """ Test that the on_failure_callback function is successfully called in a production environment when a task
        fails

        :param mock_create_slack_webhook: Mock create_slack_webhook function
        :return: None.
        """
        mock_create_slack_webhook.return_value = Mock(spec=SlackWebhookHook)

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)

        # Setup Telescope with 0 retries and missing airflow variable, so it will fail the task
        telescope = TestCallbackWorkflow("test_callback", pendulum.datetime(2020, 1, 1), self.schedule_interval,
                                         max_retries=0,
                                         airflow_conns=[AirflowVars.ORCID_BUCKET])
        dag = telescope.make_dag()

        # Create the Observatory environment and run task, expecting slack webhook call in production environment
        with env.create(task_logging=True):
            env.add_variable(Variable(key=AirflowVars.ENVIRONMENT, val=Environment.production.value))
            with self.assertRaises(AirflowNotFoundException):
                env.run_task(telescope.check_dependencies.__name__, dag, pendulum.datetime(2020, 1, 1))
            mock_create_slack_webhook.assert_called_once_with(
                "Task failed, exception:\nairflow.exceptions.AirflowNotFoundException: The conn_id `orcid_bucket` isn\'t defined",
                self.project_id, ANY
            )
        # Reset mock
        mock_create_slack_webhook.reset_mock()

        # Create the Observatory environment and run task, expecting no slack webhook call in develop environment
        with env.create(task_logging=True):
            env.add_variable(Variable(key=AirflowVars.ENVIRONMENT, val=Environment.develop.value))
            with self.assertRaises(AirflowNotFoundException):
                env.run_task(telescope.check_dependencies.__name__, dag, pendulum.datetime(2020, 1, 1))
            mock_create_slack_webhook.assert_not_called()


class TestAddSensorsTelescope(ObservatoryTestCase):
    """Tests the sensor interface."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super().__init__(*args, **kwargs)

    def dummy_func(self):
        pass

    def test_add_sensor(self):
        mt = MockTelescope(
            dag_id="1", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule_interval="daily"
        )
        mt.add_task(self.dummy_func)
        tds = ExternalTaskSensor(
            external_dag_id="1", task_id="test", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )

        tds2 = ExternalTaskSensor(
            external_dag_id="1", task_id="test2", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )

        mt.add_sensor(tds)
        mt.add_sensor(tds2)
        dag = mt.make_dag()

        self.assert_dag_structure({"dummy_func": [], "test": ["dummy_func"], "test2": ["dummy_func"]}, dag)

    def test_add_sensors(self):
        mt = MockTelescope(
            dag_id="1", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule_interval="daily"
        )
        mt.add_task(self.dummy_func)
        tds = ExternalTaskSensor(
            external_dag_id="1", task_id="test", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )

        tds2 = ExternalTaskSensor(
            external_dag_id="1", task_id="test2", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
        )

        mt.add_sensor_chain([tds, tds2])
        dag = mt.make_dag()

        self.assert_dag_structure({"dummy_func": [], "test": ["dummy_func"], "test2": ["dummy_func"]}, dag)

    def test_add_sensors_empty(self):
        mt = MockTelescope(
            dag_id="1", start_date=datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule_interval="daily"
        )
        mt.add_task(self.dummy_func)
        mt.add_sensor_chain([])
        dag = mt.make_dag()

        self.assert_dag_structure({"dummy_func": []}, dag)
