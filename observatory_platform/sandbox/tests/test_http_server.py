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
from unittest.mock import patch

import timeout_decorator
from click.testing import CliRunner

from observatory_platform.http_download import (
    DownloadInfo,
    download_file,
    download_files,
)
from observatory_platform.sandbox.http_server import HttpServer
from observatory_platform.sandbox.test_utils import test_fixtures_path, SandboxTestCase


class TestHttpserver(SandboxTestCase):
    def test_serve(self):
        """Make sure the server can be constructed."""
        with patch("observatory_platform.sandbox.http_server.ThreadingHTTPServer.serve_forever") as m_serve:
            server = HttpServer(directory=".")
            server.serve_(("localhost", 10000), ".")
            self.assertEqual(m_serve.call_count, 1)

    @timeout_decorator.timeout(1)
    def test_stop_before_start(self):
        """Make sure there's no deadlock if we try to stop before a start."""

        server = HttpServer(directory=".")
        server.stop()

    @timeout_decorator.timeout(1)
    def test_start_twice(self):
        """Make sure there's no funny business if we try to stop before a start."""

        server = HttpServer(directory=".")
        server.start()
        server.start()
        server.stop()

    def test_server(self):
        """Test the webserver can serve a directory"""

        directory = test_fixtures_path("utils")
        server = HttpServer(directory=directory)
        server.start()

        test_file = "http_testfile.txt"
        expected_hash = "d8e8fca2dc0f896fd7cb4cb0031ba249"
        algorithm = "md5"

        url = f"{server.url}{test_file}"

        with CliRunner().isolated_filesystem() as tmpdir:
            dst_file = os.path.join(tmpdir, "testfile.txt")

            download_files(download_list=[DownloadInfo(url=url, filename=dst_file)])

            self.assert_file_integrity(dst_file, expected_hash, algorithm)

        server.stop()

    def test_context_manager(self):
        directory = test_fixtures_path("utils")
        server = HttpServer(directory=directory)

        with server.create():
            test_file = "http_testfile.txt"
            expected_hash = "d8e8fca2dc0f896fd7cb4cb0031ba249"
            algorithm = "md5"

            url = f"{server.url}{test_file}"

            with CliRunner().isolated_filesystem() as tmpdir:
                dst_file = os.path.join(tmpdir, "testfile.txt")
                download_file(url=url, filename=dst_file)
                self.assert_file_integrity(dst_file, expected_hash, algorithm)
