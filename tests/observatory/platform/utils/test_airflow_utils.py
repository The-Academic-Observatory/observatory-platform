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

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from observatory.platform.utils.airflow_utils import (
    AirflowConns,
    get_airflow_connection_login,
    get_airflow_connection_password,
    get_airflow_connection_url,
    send_slack_msg,
    set_task_state,
)


class MockConnection:
    def __init__(self, url):
        self.url = url
        self.login = "login"

    def get_uri(self):
        return self.url

    def get_password(self):
        return "password"


class TestAirflowUtils(unittest.TestCase):
    @patch("observatory.platform.utils.airflow_utils.SlackWebhookHook")
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
            project_id="project-id",
        )

        expected_message = """
    :red_circle: Task Alert.
    *Task*: {task}
    *Dag*: {dag}
    *Execution Time*: {exec_date}
    *Log Url*: {log_url}
    *Project id*: {project_id}
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

    def test_set_task_state(self):
        """Test set_task_state"""

        task_id = "test_task"
        set_task_state(True, task_id)
        with self.assertRaises(AirflowException):
            set_task_state(False, task_id)

    def test_get_airflow_connection_url_invalid(self):
        with patch("observatory.platform.utils.airflow_utils.BaseHook") as m_basehook:
            m_basehook.get_connection = MagicMock(return_value=MockConnection(""))
            self.assertRaises(AirflowException, get_airflow_connection_url, "some_connection")

            m_basehook.get_connection = MagicMock(return_value=MockConnection("http://invalidurl"))
            self.assertRaises(AirflowException, get_airflow_connection_url, "some_connection")

    def test_get_airflow_connection_url_valid(self):
        expected_url = "http://localhost/"
        fake_conn = "some_connection"

        with patch("observatory.platform.utils.airflow_utils.BaseHook") as m_basehook:
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
        with patch("observatory.platform.utils.airflow_utils.BaseHook") as m_basehook:
            m_basehook.get_connection = MagicMock(return_value=MockConnection(""))
            password = get_airflow_connection_password("")
            self.assertEqual(password, "password")

    def test_get_airflow_connection_login(self):
        with patch("observatory.platform.utils.airflow_utils.BaseHook") as m_basehook:
            m_basehook.get_connection = MagicMock(return_value=MockConnection(""))
            login = get_airflow_connection_login("")
            self.assertEqual(login, "login")
