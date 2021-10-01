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
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.models import DagRun, TaskInstance, BaseOperator, DAG
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from observatory.platform.utils.airflow_utils import (
    create_slack_webhook,
    get_airflow_connection_password,
    get_airflow_connection_url,
    get_prev_start_date_success_task,
    set_task_state,
)


class MockConnection:
    def __init__(self, url):
        self.url = url

    def get_uri(self):
        return self.url

    def get_password(self):
        return "password"


class TestAirflowUtils(unittest.TestCase):
    @patch("airflow.hooks.base_hook.BaseHook.get_connection")
    def test_create_slack_webhook(self, mock_get_connection):
        mock_get_connection.return_value = Connection(uri=f"https://:key@https%3A%2F%2Fhooks.slack.com%2Fservices")
        slack_hook = create_slack_webhook(
            comments="comment",
            project_id="project-id",
            context={
                "ti": SimpleNamespace(task_id="task_id", dag_id="dag_id", log_url="log_url"),
                "execution_date": "2020-01-01",
            },
        )
        self.assertIsInstance(slack_hook, SlackWebhookHook)
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
            exec_date="2020-01-01",
            log_url="log_url",
            comments="comment",
            project_id="project-id",
        )
        self.assertEqual(expected_message, slack_hook.message)

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

    def test_get_prev_start_date_success_task(self):
        """Test the get_prev_start_date_success_task function

        :return: None.
        """
        execution_date = datetime(2021, 1, 1, tzinfo=timezone.utc)
        dag_run = DagRun("dag_id", execution_date=execution_date)

        # Test that start date is None when there is no previous dag run
        with patch.object(DagRun, "get_previous_dagrun", return_value=None):
            start_date = get_prev_start_date_success_task(dag_run, "task_id")
            self.assertIsNone(start_date)

        # Create two task instances with different states
        with DAG("dag_id", start_date=datetime(2020, 1, 1)):
            task = BaseOperator(task_id="task_id")
        ti_skipped = TaskInstance(task, execution_date=execution_date)
        ti_skipped.start_date = datetime(2020, 8, 1)
        ti_skipped.state = "skipped"

        ti_success = TaskInstance(task, execution_date=execution_date)
        ti_success.start_date = datetime(2021, 1, 1)
        ti_success.state = "success"

        # Test that the start date is the start date of the successful task
        with patch.object(DagRun, "get_previous_dagrun", return_value=DagRun):
            with patch.object(DagRun, "get_task_instance", side_effect=[ti_skipped, ti_success]):
                start_date = get_prev_start_date_success_task(dag_run, "task_id")
                self.assertEqual(ti_success.start_date, start_date)
