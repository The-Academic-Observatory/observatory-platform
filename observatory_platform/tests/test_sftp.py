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
from unittest.mock import MagicMock

# from urllib.parse import quote

# import paramiko
# from airflow.models.connection import Connection

from observatory_platform.sftp import make_sftp_connection


class TestSFTP(unittest.TestCase):

    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("observatory_platform.sftp.paramiko.SSHClient")
    def test_make_sftp_connection(self, mock_sshclient_cls, mock_get_connection):
        # Setup mock connection returned by BaseHook.get_connection
        mock_conn = MagicMock()
        mock_conn.host = "example.com"
        mock_conn.port = 2222
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_get_connection.return_value = mock_conn

        # Setup mock SSH client and its methods
        mock_sshclient = MagicMock()
        mock_sftp = MagicMock()
        mock_sshclient.open_sftp.return_value = mock_sftp
        mock_sshclient_cls.return_value = mock_sshclient

        with make_sftp_connection("my_conn_id") as sftp:
            self.assertEqual(sftp, mock_sftp)
            mock_sshclient.connect.assert_called_once_with("example.com", port=2222, username="user", password="pass")
            mock_sshclient.open_sftp.assert_called_once()

        # Ensure cleanup was called
        mock_sftp.close.assert_called_once()
        mock_sshclient.close.assert_called_once()
