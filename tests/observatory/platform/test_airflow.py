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
import shutil
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.models.xcom import XCom
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session

from observatory.platform.airflow import (
    get_airflow_connection_login,
    get_airflow_connection_password,
    get_airflow_connection_url,
    send_slack_msg,
    fetch_dags_modules,
    fetch_dag_bag,
    delete_old_xcoms,
    on_failure_callback,
    get_data_path,
    normalized_schedule_interval,
)
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    test_fixtures_path,
)


class MockConnection:
    def __init__(self, url):
        self.url = url
        self.login = "login"

    def get_uri(self):
        return self.url

    def get_password(self):
        return "password"


class TestAirflow(unittest.TestCase):
    @patch("observatory.platform.airflow.Variable.get")
    def test_get_data_path(self, mock_variable_get):
        """Tests the function that retrieves the data_path airflow variable"""
        # 1 - no variable available
        mock_variable_get.return_value = None
        self.assertRaises(AirflowException, get_data_path)

        # 2 - available in Airflow variable
        mock_variable_get.return_value = "env_return"
        self.assertEqual("env_return", get_data_path())

    def test_fetch_dags_modules(self):
        """Test fetch_dags_modules"""

        dags_module_names_val = '["academic_observatory_workflows.dags", "oaebu_workflows.dags"]'
        expected = ["academic_observatory_workflows.dags", "oaebu_workflows.dags"]
        env = ObservatoryEnvironment(enable_api=False, enable_elastic=False)
        with env.create():
            # Test when no variable set
            with self.assertRaises(KeyError):
                fetch_dags_modules()

            # Test when using an Airflow Variable exists
            env.add_variable(Variable(key="dags_module_names", val=dags_module_names_val))
            actual = fetch_dags_modules()
            self.assertEqual(expected, actual)

        with ObservatoryEnvironment(enable_api=False, enable_elastic=False).create():
            # Set environment variable
            new_env = env.new_env
            new_env["AIRFLOW_VAR_DAGS_MODULE_NAMES"] = dags_module_names_val
            os.environ.update(new_env)

            # Test when using an Airflow Variable set with an environment variable
            actual = fetch_dags_modules()
            self.assertEqual(expected, actual)

    def test_fetch_dag_bag(self):
        """Test fetch_dag_bag"""

        env = ObservatoryEnvironment(enable_api=False, enable_elastic=False)
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

    @patch("observatory.platform.airflow.SlackWebhookHook")
    @patch("airflow.hooks.base_hook.BaseHook.get_connection")
    def test_send_slack_msg(self, mock_get_connection, m_slack):
        mock_get_connection.return_value = Connection(uri=f"https://:key@https%3A%2F%2Fhooks.slack.com%2Fservices")

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
        )

        expected_message = """
    :red_circle: Task Alert.
    *Task*: {task}
    *Dag*: {dag}
    *Execution Time*: {exec_date}
    *Log Url*: {log_url}
    *Comments*: {comments}
    """.format(
            task="task_id",
            dag="dag_id",
            exec_date=execution_date.isoformat(),
            log_url="log_url",
            comments="comment",
            project_id="project-id",
        )

        m_slack.assert_called_once_with(http_conn_id=None, webhook_token="key", message=expected_message)

    def test_get_airflow_connection_url_invalid(self):
        with patch("observatory.platform.airflow.BaseHook") as m_basehook:
            m_basehook.get_connection = MagicMock(return_value=MockConnection(""))
            self.assertRaises(AirflowException, get_airflow_connection_url, "some_connection")

            m_basehook.get_connection = MagicMock(return_value=MockConnection("http://invalidurl"))
            self.assertRaises(AirflowException, get_airflow_connection_url, "some_connection")

    def test_get_airflow_connection_url_valid(self):
        expected_url = "http://localhost/"
        fake_conn = "some_connection"

        with patch("observatory.platform.airflow.BaseHook") as m_basehook:
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
        env = ObservatoryEnvironment(enable_api=False, enable_elastic=False)
        with env.create():
            # Assert that we can get a connection password
            conn_id = "conn_1"
            env.add_connection(Connection(
                conn_id=conn_id, conn_type="http", password="password", login="login"
            ))
            password = get_airflow_connection_password(conn_id)
            self.assertEqual("password", password)

            # Assert that an AirflowException is raised when the password is None
            conn_id = "conn_2"
            env.add_connection(Connection(
                conn_id=conn_id, conn_type="http", password=None, login="login"
            ))
            with self.assertRaises(AirflowException):
                get_airflow_connection_password(conn_id)

    def test_get_airflow_connection_login(self):
        env = ObservatoryEnvironment(enable_api=False, enable_elastic=False)
        with env.create():
            # Assert that we can get a connection login
            conn_id = "conn_1"
            env.add_connection(Connection(
                conn_id=conn_id, conn_type="http", password="password", login="login"
            ))
            login = get_airflow_connection_login(conn_id)
            self.assertEqual("login", login)

            # Assert that an AirflowException is raised when the login is None
            conn_id = "conn_2"
            env.add_connection(Connection(
                conn_id=conn_id, conn_type="http", password="password", login=None
            ))
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
            schedule_interval = test[0]
            expected_n_schedule_interval = test[1]
            actual_n_schedule_interval = normalized_schedule_interval(schedule_interval)

            self.assertEqual(expected_n_schedule_interval, actual_n_schedule_interval)

    def test_delete_old_xcom_all(self):
        """Test deleting all XCom messages."""

        def create_xcom(**kwargs):
            ti = kwargs["ti"]
            execution_date = kwargs["execution_date"]
            ti.xcom_push("topic", {"snapshot_date": execution_date.format("YYYYMMDD"), "something": "info"})

        env = ObservatoryEnvironment(enable_api=False, enable_elastic=False)
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 5)
            with DAG(
                dag_id="hello_world_dag",
                schedule_interval="@daily",
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

        env = ObservatoryEnvironment(enable_api=False, enable_elastic=False)
        with env.create():
            first_execution_date = pendulum.datetime(2021, 9, 5)
            with DAG(
                dag_id="hello_world_dag",
                schedule_interval="@daily",
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

    @patch("observatory.platform.airflow.send_slack_msg")
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
            slack_conn_id='slack'
        )
