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

import unittest
from unittest.mock import patch
from urllib.parse import quote

import paramiko
import pysftp
from airflow.models.connection import Connection

from observatory_platform.sftp import make_sftp_connection


class TestSFTP(unittest.TestCase):
    @patch.object(pysftp, "Connection")
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_make_sftp_connection(self, mock_airflow_conn, mock_pysftp_connection):
        """Test that sftp connection is initialized correctly"""

        # set up variables
        username = "username"
        password = "password"
        host = "host"
        host_key = quote(paramiko.RSAKey.generate(512).get_base64(), safe="")

        # mock airflow sftp service conn
        mock_airflow_conn.return_value = Connection(uri=f"ssh://{username}:{password}@{host}?host_key={host_key}")

        # run function
        sftp = make_sftp_connection("pysftp_connection")

        # confirm sftp server was initialised with correct username, password and cnopts
        call_args = mock_pysftp_connection.call_args

        self.assertEqual(1, len(call_args[0]))
        self.assertEqual(host, call_args[0][0])

        self.assertEqual(4, len(call_args[1]))
        self.assertEqual(username, call_args[1]["username"])
        self.assertEqual(password, call_args[1]["password"])
        self.assertIsInstance(call_args[1]["cnopts"], pysftp.CnOpts)
        self.assertIsNone(call_args[1]["port"])
