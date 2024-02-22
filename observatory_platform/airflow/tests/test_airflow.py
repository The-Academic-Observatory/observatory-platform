# Copyright 2021 Curtin University. All Rights Reserved.
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

import datetime
import os
import textwrap
import unittest
from unittest.mock import MagicMock, patch

import pendulum
from airflow.decorators import dag
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.models.xcom import XCom
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.utils.state import State

from observatory_platform.airflow.airflow import (
    get_airflow_connection_login,
    get_airflow_connection_password,
    get_airflow_connection_url,
    send_slack_msg,
    delete_old_xcoms,
    on_failure_callback,
    normalized_schedule_interval,
    is_first_dag_run,
)
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment


class MockConnection:
    def __init__(self, url):
        self.url = url
        self.login = "login"

    def get_uri(self):
        return self.url

    def get_password(self):
        return "password"


class TestAirflow(unittest.TestCase):
    @patch("observatory_platform.airflow.airflow.SlackWebhookHook")
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_send_slack_msg(self, mock_get_connection, m_slack):
        slack_webhook_conn_id = "slack_conn"
        mock_get_connection.return_value = Connection(
            conn_id=slack_webhook_conn_id, uri=f"https://:key@https%3A%2F%2Fhooks.slack.com%2Fservices"
        )

        class MockTI:
            def __init__(self):
                self.task_id = "task_id"
                self.dag_id = "dag_id"
                self.log_url = "log_url"

        ti = MockTI()
        execution_date = pendulum.now()

        send_slack_msg(
            ti=ti,
            execution_date=execution_date,
            comments="comment",
            slack_conn_id=slack_webhook_conn_id,
        )

        message = textwrap.dedent(
            """
            :red_circle: Task Alert.
            *Task*: task_id
            *Dag*: dag_id
            *Execution Time*: {exec_date}
            *Log Url*: log_url
            *Comments*: comment
            """
        ).format(exec_date=execution_date.isoformat())

        m_slack.assert_called_once_with(slack_webhook_conn_id=slack_webhook_conn_id)
        m_slack.return_value.send_text.assert_called_once_with(message)

    def test_get_airflow_connection_url_invalid(self):
        with patch("observatory_platform.airflow.airflow.BaseHook") as m_basehook:
            m_basehook.get_connection = MagicMock(return_value=MockConnection(""))
            self.assertRaises(AirflowException, get_airflow_connection_url, "some_connection")

            m_basehook.get_connection = MagicMock(return_value=MockConnection("http://invalidurl"))
            self.assertRaises(AirflowException, get_airflow_connection_url, "some_connection")

    def test_get_airflow_connection_url_valid(self):
        expected_url = "http://localhost/"
        fake_conn = "some_connection"

        with patch("observatory_platform.airflow.airflow.BaseHook") as m_basehook:
            # With trailing /
            input_url = "http://localhost/"
            m_basehook.get_connection = MagicMock(return_value=MockConnection(input_url))
            url = get_airflow_connection_url(fake_conn)
            self.assertEqual(url, expected_url)

            # Without trailing /
            input_url = "http://localhost"
            m_basehook.get_connection = MagicMock(return_value=MockConnection(input_url))
            url = get_airflow_connection_url(fake_conn)
            self.assertEqual(url, expected_url)

    def test_get_airflow_connection_password(self):
        env = SandboxEnvironment()
        with env.create():
            # Assert that we can get a connection password
            conn_id = "conn_1"
            env.add_connection(Connection(conn_id=conn_id, conn_type="http", password="password", login="login"))
            password = get_airflow_connection_password(conn_id)
            self.assertEqual("password", password)

            # Assert that an AirflowException is raised when the password is None
            conn_id = "conn_2"
            env.add_connection(Connection(conn_id=conn_id, conn_type="http", password=None, login="login"))
            with self.assertRaises(AirflowException):
                get_airflow_connection_password(conn_id)

    def test_get_airflow_connection_login(self):
        env = SandboxEnvironment()
        with env.create():
            # Assert that we can get a connection login
            conn_id = "conn_1"
            env.add_connection(Connection(conn_id=conn_id, conn_type="http", password="password", login="login"))
            login = get_airflow_connection_login(conn_id)
            self.assertEqual("login", login)

            # Assert that an AirflowException is raised when the login is None
            conn_id = "conn_2"
            env.add_connection(Connection(conn_id=conn_id, conn_type="http", password="password", login=None))
            with self.assertRaises(AirflowException):
                get_airflow_connection_login(conn_id)

    def test_normalized_schedule_interval(self):
        """Test normalized_schedule_interval"""
        schedule_intervals = [
            (None, None),
            ("@daily", "0 0 * * *"),
            ("@weekly", "0 0 * * 0"),
            ("@monthly", "0 0 1 * *"),
            ("@quarterly", "0 0 1 */3 *"),
            ("@yearly", "0 0 1 1 *"),
            ("@once", None),
            (datetime.timedelta(days=1), datetime.timedelta(days=1)),
        ]
        for test in schedule_intervals:
            schedule = test[0]
            expected_n_schedule_interval = test[1]
            actual_n_schedule_interval = normalized_schedule_interval(schedule)

            self.assertEqual(expected_n_schedule_interval, actual_n_schedule_interval)

    def test_delete_old_xcom_all(self):
        """Test deleting all XCom messages."""

        def create_xcom(**kwargs):
            ti = kwargs["ti"]
            execution_date = kwargs["execution_date"]
            ti.xcom_push("topic", {"snapshot_date": execution_date.format("YYYYMMDD"), "something": "info"})

        env = SandboxEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 5)
            with DAG(
                dag_id="hello_world_dag",
                schedule="@daily",
                default_args={"owner": "airflow", "start_date": execution_date},
                catchup=True,
            ) as dag:
                kwargs = {"task_id": "create_xcom"}
                op = PythonOperator(python_callable=create_xcom, **kwargs)

            # DAG Run
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("create_xcom")
                self.assertEqual("success", ti.state)
                msgs = ti.xcom_pull(key="topic", task_ids="create_xcom", include_prior_dates=True)
                self.assertIsInstance(msgs, dict)
                delete_old_xcoms(dag_id="hello_world_dag", execution_date=execution_date, retention_days=0)
                msgs = ti.xcom_pull(key="topic", task_ids="create_xcom", include_prior_dates=True)
                self.assertEqual(msgs, None)

    def test_delete_old_xcom_older(self):
        """Test deleting old XCom messages."""

        def create_xcom(**kwargs):
            ti = kwargs["ti"]
            execution_date = kwargs["execution_date"]
            ti.xcom_push("topic", {"snapshot_date": execution_date.format("YYYYMMDD"), "something": "info"})

        @provide_session
        def get_xcom(session=None, dag_id=None, task_id=None, key=None, execution_date=None):
            msgs = XCom.get_many(
                execution_date=execution_date,
                key=key,
                dag_ids=dag_id,
                task_ids=task_id,
                include_prior_dates=True,
                session=session,
            ).with_entities(XCom.value)
            return msgs.all()

        env = SandboxEnvironment()
        with env.create():
            first_execution_date = pendulum.datetime(2021, 9, 5)
            with DAG(
                dag_id="hello_world_dag",
                schedule="@daily",
                default_args={"owner": "airflow", "start_date": first_execution_date},
                catchup=True,
            ) as dag:
                kwargs = {"task_id": "create_xcom"}
                PythonOperator(python_callable=create_xcom, **kwargs)

            # First DAG Run
            with env.create_dag_run(dag=dag, execution_date=first_execution_date):
                ti = env.run_task("create_xcom")
                self.assertEqual("success", ti.state)
                msg = ti.xcom_pull(key="topic", task_ids="create_xcom", include_prior_dates=True)
                self.assertEqual(msg["snapshot_date"], first_execution_date.format("YYYYMMDD"))

            # Second DAG Run
            second_execution_date = pendulum.datetime(2021, 9, 15)
            with env.create_dag_run(dag=dag, execution_date=second_execution_date):
                ti = env.run_task("create_xcom")
                self.assertEqual("success", ti.state)
                msg = ti.xcom_pull(key="topic", task_ids="create_xcom", include_prior_dates=True)
                self.assertEqual(msg["snapshot_date"], second_execution_date.format("YYYYMMDD"))

                # Check there are two xcoms in the db
                xcoms = get_xcom(
                    dag_id="hello_world_dag", task_id="create_xcom", key="topic", execution_date=second_execution_date
                )
                self.assertEqual(len(xcoms), 2)

                # Delete old xcoms
                delete_old_xcoms(dag_id="hello_world_dag", execution_date=second_execution_date, retention_days=1)

                # Check result
                xcoms = get_xcom(
                    dag_id="hello_world_dag", task_id="create_xcom", key="topic", execution_date=second_execution_date
                )
                self.assertEqual(len(xcoms), 1)
                msg = XCom.deserialize_value(xcoms[0])
                self.assertEqual(msg["snapshot_date"], second_execution_date.format("YYYYMMDD"))

    @patch("observatory_platform.airflow.airflow.send_slack_msg")
    def test_on_failure_callback(self, mock_send_slack_msg):
        # Fake Airflow ti instance
        class MockTI:
            def __init__(self):
                self.task_id = "id"
                self.dag_id = "dag"
                self.log_url = "logurl"

        execution_date = pendulum.now()
        ti = MockTI()
        context = {"exception": AirflowException("Exception message"), "ti": ti, "execution_date": execution_date}
        # Check that hasn't been called
        mock_send_slack_msg.assert_not_called()

        # Call function
        on_failure_callback(context)

        # Check that called with correct parameters
        mock_send_slack_msg.assert_called_once_with(
            ti=ti,
            execution_date=execution_date,
            comments="Task failed, exception:\n" "airflow.exceptions.AirflowException: Exception message",
            slack_conn_id="slack",
        )

    @patch("observatory_platform.airflow.airflow.send_slack_msg")
    def test_callback(self, mock_send_slack_msg):
        """Test that the on_failure_callback function is successfully called in a production environment when a task
        fails

        :param mock_send_slack_msg: Mock send_slack_msg function
        :return: None.
        """

        def create_dag(dag_id: str, start_date: pendulum.DateTime, schedule: str, retries: int, airflow_conns: list):
            @dag(
                dag_id=dag_id,
                start_date=start_date,
                schedule=schedule,
                default_args=dict(retries=retries, on_failure_callback=on_failure_callback),
            )
            def callback_test_dag():
                check_dependencies(airflow_conns=airflow_conns)
            return callback_test_dag()

        # Setup Observatory environment
        project_id = os.getenv("TEST_GCP_PROJECT_ID")
        data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        env = SandboxEnvironment(project_id, data_location)

        # Setup Workflow with 0 retries and missing airflow variable, so it will fail the task
        execution_date = pendulum.datetime(2020, 1, 1)
        conn_id = "orcid_bucket"
        my_dag = create_dag(
            "test_callback",
            execution_date,
            "@weekly",
            retries=0,
            airflow_conns=[conn_id],
        )

        # Create the Observatory environment and run task, expecting slack webhook call in production environment
        with env.create(task_logging=True):
            with env.create_dag_run(my_dag, execution_date):
                with self.assertRaises(AirflowNotFoundException):
                    env.run_task("check_dependencies")

                _, callkwargs = mock_send_slack_msg.call_args
                self.assertTrue(
                    "airflow.exceptions.AirflowNotFoundException: Required variables or connections are missing"
                    in callkwargs["comments"]
                )

        # Reset mock
        mock_send_slack_msg.reset_mock()

        # Add orcid_bucket connection and test that Slack Web Hook did not get triggered
        with env.create(task_logging=True):
            with env.create_dag_run(my_dag, execution_date):
                env.add_connection(Connection(conn_id=conn_id, uri="https://orcid.org/"))
                env.run_task("check_dependencies")
                mock_send_slack_msg.assert_not_called()

    def test_is_first_dag_run(self):
        """Test is_first_dag_run"""

        env = SandboxEnvironment()
        with env.create():
            first_execution_date = pendulum.datetime(2021, 9, 5)
            with DAG(
                dag_id="hello_world_dag",
                schedule="@daily",
                default_args={"owner": "airflow", "start_date": first_execution_date},
                catchup=True,
            ) as dag:
                task = BashOperator(task_id="task", bash_command="echo 'hello'")

            # First DAG Run
            with env.create_dag_run(dag=dag, execution_date=first_execution_date) as first_dag_run:
                # Should be true the first DAG run. Check before and after a task.
                is_first = is_first_dag_run(first_dag_run)
                self.assertTrue(is_first)

                ti = env.run_task("task")
                self.assertEqual(ti.state, State.SUCCESS)

                is_first = is_first_dag_run(first_dag_run)
                self.assertTrue(is_first)

            # Second DAG Run
            second_execution_date = pendulum.datetime(2021, 9, 12)
            with env.create_dag_run(dag=dag, execution_date=second_execution_date) as second_dag_run:
                # Should be false on second DAG Run, check before and after a task.
                is_first = is_first_dag_run(second_dag_run)
                self.assertFalse(is_first)

                ti = env.run_task("task")
                self.assertEqual(ti.state, State.SUCCESS)

                is_first = is_first_dag_run(second_dag_run)
                self.assertFalse(is_first)