# Copyright 2021-2024 Curtin University
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

from __future__ import annotations

import contextlib
import os
import unittest
from ftplib import FTP

from click.testing import CliRunner

from observatory_platform.sandbox.ftp_server import FtpServer
from observatory_platform.sandbox.test_utils import find_free_port


class TestFtpServer(unittest.TestCase):
    def setUp(self) -> None:
        self.host = "localhost"
        self.port = find_free_port()

    @contextlib.contextmanager
    def test_server(self):
        """Test that the FTP server can be connected to"""

        with CliRunner().isolated_filesystem() as tmp_dir:
            server = FtpServer(directory=tmp_dir, host=self.host, port=self.port)
            with server.create() as root_dir:
                # Connect to FTP server anonymously
                ftp_conn = FTP()
                ftp_conn.connect(host=self.host, port=self.port)
                ftp_conn.login()

                # Check that there are no files
                files = ftp_conn.nlst()
                self.assertFalse(len(files))

                # Add a file and check that it exists
                expected_file_name = "textfile.txt"
                file_path = os.path.join(root_dir, expected_file_name)
                with open(file_path, mode="w") as f:
                    f.write("hello world")
                files = ftp_conn.nlst()
                self.assertEqual(1, len(files))
                self.assertEqual(expected_file_name, files[0])

    @contextlib.contextmanager
    def test_user_permissions(self):
        "Test the level of permissions of the root and anonymous users."

        with CliRunner().isolated_filesystem() as tmp_dir:
            server = FtpServer(
                directory=tmp_dir, host=self.host, port=self.port, root_username="root", root_password="pass"
            )
            with server.create() as root_dir:
                # Add a file onto locally hosted server.
                expected_file_name = "textfile.txt"
                file_path = os.path.join(root_dir, expected_file_name)
                with open(file_path, mode="w") as f:
                    f.write("hello world")

                # Connect to FTP server anonymously.
                ftp_conn = FTP()
                ftp_conn.connect(host=self.host, port=self.port)
                ftp_conn.login()

                # Make sure that anonymoous user has read-only permissions
                ftp_repsonse = ftp_conn.sendcmd(f"MLST {expected_file_name}")
                self.assertTrue(";perm=r;size=11;type=file;" in ftp_repsonse)

                ftp_conn.close()

                # Connect to FTP server as root user.
                ftp_conn = FTP()
                ftp_conn.connect(host=self.host, port=self.port)
                ftp_conn.login(user="root", passwd="pass")

                # Make sure that root user has all available read/write/modification permissions.
                ftp_repsonse = ftp_conn.sendcmd(f"MLST {expected_file_name}")
                self.assertTrue(";perm=radfwMT;size=11;type=file;" in ftp_repsonse)

                ftp_conn.close()
