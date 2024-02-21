# Copyright 2019-2024 Curtin University
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

import os
import unittest

import pysftp

from observatory_platform.sandbox.stfp_server import SftpServer
from observatory_platform.sandbox.test_utils import find_free_port


class TestSftpServer(unittest.TestCase):
    def setUp(self) -> None:
        self.host = "localhost"
        self.port = find_free_port()

    def test_server(self):
        """Test that the SFTP server can be connected to"""

        server = SftpServer(host=self.host, port=self.port)
        with server.create() as root_dir:
            # Connect to SFTP server and disable host key checking
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            sftp = pysftp.Connection(self.host, port=self.port, username="", password="", cnopts=cnopts)

            # Check that there are no files
            files = sftp.listdir(".")
            self.assertFalse(len(files))

            # Add a file and check that it exists
            expected_file_name = "onix.xml"
            file_path = os.path.join(root_dir, expected_file_name)
            with open(file_path, mode="w") as f:
                f.write("hello world")
            files = sftp.listdir(".")
            self.assertEqual(1, len(files))
            self.assertEqual(expected_file_name, files[0])
