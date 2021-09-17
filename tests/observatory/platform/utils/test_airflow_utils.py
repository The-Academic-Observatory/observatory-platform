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

# Author: James Diprose


import unittest
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from observatory.platform.utils.airflow_utils import (
    get_airflow_connection_url,
    set_task_state,
)


class TestAirflowUtils(unittest.TestCase):
    def test_set_task_state(self):
        """Test set_task_state"""

        task_id = "test_task"
        set_task_state(True, task_id)
        with self.assertRaises(AirflowException):
            set_task_state(False, task_id)

    def test_get_airflow_connection_url_invalid(self):
        with patch("observatory.platform.utils.airflow_utils.BaseHook") as m_basehook:
            m_basehook.get_connection = MagicMock(return_value="")
            self.assertRaises(AirflowException, get_airflow_connection_url, "some_connection")

            m_basehook.get_connection = MagicMock(return_value="http://invalidurl")
            self.assertRaises(AirflowException, get_airflow_connection_url, "some_connection")

    def test_get_airflow_connection_url_valid(self):
        expected_url = "http://localhost/"
        fake_conn = "some_connection"

        with patch("observatory.platform.utils.airflow_utils.BaseHook") as m_basehook:
            # With trailing /
            input_url = "http://localhost/"
            m_basehook.get_connection = MagicMock(return_value=input_url)
            url = get_airflow_connection_url(fake_conn)
            self.assertEqual(url, expected_url)

            # Without trailing /
            input_url = "http://localhost"
            m_basehook.get_connection = MagicMock(return_value=input_url)
            url = get_airflow_connection_url(fake_conn)
            self.assertEqual(url, expected_url)
