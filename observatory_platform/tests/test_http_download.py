# Copyright 2019 Curtin University
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

# Author: Tuan Chien

import os
import shutil
from unittest.mock import patch

from click.testing import CliRunner

from observatory_platform.config import module_file_path
from observatory_platform.http_download import (
    DownloadInfo,
    download_file,
    download_files,
)
from observatory_platform.sandbox.http_server import HttpServer
from observatory_platform.sandbox.test_utils import SandboxTestCase
from observatory_platform.url_utils import get_observatory_http_header


class MockVersionData:
    @classmethod
    def get(self, attribute):
        if attribute == "Version":
            return "1"
        if attribute == "Home-page":
            return "http://test.test"
        if attribute == "Author-email":
            return "test@test"


class TestAsyncHttpFileDownloader(SandboxTestCase):
    def test_download_files(self):
        # Spin up http server
        directory = module_file_path("observatory_platform.tests.fixtures")
        http_server = HttpServer(directory=directory)
        with http_server.create():
            file1 = "http_testfile.txt"
            file2 = "http_testfile2.txt"
            hash1 = "d8e8fca2dc0f896fd7cb4cb0031ba249"
            hash2 = "126a8a51b9d1bbd07fddc65819a542c3"
            algorithm = "md5"

            url1 = f"{http_server.url}{file1}"
            url2 = f"{http_server.url}{file2}"

            # Empty list
            with CliRunner().isolated_filesystem() as tmpdir:
                download_files(download_list=[])
                files = os.listdir(tmpdir)
                self.assertEqual(len(files), 0)

            # URL only
            with CliRunner().isolated_filesystem() as tmpdir:
                download_list = [url1, url2]
                download_files(download_list=download_list)
                self.assert_file_integrity(file1, hash1, algorithm)
                self.assert_file_integrity(file2, hash2, algorithm)

            # URL only with observatory user agent
            with CliRunner().isolated_filesystem() as tmpdir:
                with patch("observatory_platform.url_utils.metadata", return_value=MockVersionData):
                    headers = get_observatory_http_header(package_name="observatory-platform")
                    download_list = [url1, url2]
                    download_files(download_list=download_list, headers=headers)
                    self.assert_file_integrity(file1, hash1, algorithm)
                    self.assert_file_integrity(file2, hash2, algorithm)

            # Dictionary list
            with CliRunner().isolated_filesystem() as tmpdir:
                dst1 = "test1.txt"
                dst2 = "test2.txt"

                download_list = [
                    DownloadInfo(url=url1, filename=dst1),
                    DownloadInfo(url=url2, filename=dst2),
                ]
                download_files(download_list=download_list)
                self.assert_file_integrity(dst1, hash1, algorithm)
                self.assert_file_integrity(dst2, hash2, algorithm)

            # Single download
            with CliRunner().isolated_filesystem() as tmpdir:
                download_file(url=url1)
                download_file(url=url2, filename=file2)
                self.assert_file_integrity(file1, hash1, algorithm)
                self.assert_file_integrity(file2, hash2, algorithm)

            # Assert that filepaths are correct
            with CliRunner().isolated_filesystem() as tmpdir:
                success, download_info = download_file(url=url1)
                self.assertTrue(success)
                self.assertEqual(file1, download_info.file_path)

                success, download_info = download_file(url=url1, prefix_dir=tmpdir)
                self.assertTrue(success)
                self.assertEqual(os.path.join(tmpdir, file1), download_info.file_path)

                success, download_info = download_file(url=url1, prefix_dir=tmpdir, filename=file2)
                self.assertTrue(success)
                self.assertEqual(os.path.join(tmpdir, file2), download_info.file_path)

            # Single download with  (prefix dir)
            with CliRunner().isolated_filesystem() as tmpdir:
                dinfo = DownloadInfo(url=url1, filename=file1, prefix_dir="invalid")
                download_files(download_list=[dinfo], prefix_dir=tmpdir)
                self.assert_file_integrity(file1, hash1, algorithm)

            # Retry and timeout
            download_list = [f"{http_server.url}does_not_exist"]
            success = download_files(download_list=download_list)
            self.assertFalse(success)

            # File exists, good hash
            with CliRunner().isolated_filesystem() as tmpdir:
                src_file = os.path.join(directory, file1)
                dst_file = os.path.join(tmpdir, file1)
                shutil.copyfile(src_file, dst_file)

                hash = "d8e8fca2dc0f896fd7cb4cb0031ba249"
                download_file(url=url1, hash=hash, hash_algorithm="md5")
                self.assert_file_integrity(file1, hash1, algorithm)

            # File exists, bad hash
            with CliRunner().isolated_filesystem() as tmpdir:
                src_file = os.path.join(directory, file1)
                dst_file = os.path.join(tmpdir, file1)
                shutil.copyfile(src_file, dst_file)

                hash = "garbage2dc0f896fd7cb4cb0031ba249"
                success, download_info = download_file(url=url1, hash=hash, hash_algorithm="md5")
                self.assertFalse(success)

            # File exists, bad hash (prefix dir)
            with CliRunner().isolated_filesystem() as tmpdir:
                src_file = os.path.join(directory, file1)
                dst_file = os.path.join(tmpdir, file1)
                shutil.copyfile(src_file, dst_file)

                hash = "garbage2dc0f896fd7cb4cb0031ba249"
                success, download_info = download_file(url=url1, hash=hash, hash_algorithm="md5", prefix_dir=tmpdir)
                self.assertFalse(success)

            # File does not exist, bad hash
            with CliRunner().isolated_filesystem() as tmpdir:
                hash = "garbage2dc0f896fd7cb4cb0031ba249"
                success, download_info = download_file(url=url1, hash=hash, hash_algorithm="md5")
                self.assertFalse(success)

            # File does not exist, good hash
            with CliRunner().isolated_filesystem() as tmpdir:
                success, download_info = download_file(url=url1, hash=hash1, hash_algorithm="md5")
                self.assertTrue(success)
                self.assert_file_integrity(file1, hash1, algorithm)

                # Skip download because exists
                with patch("observatory_platform.http_download.download_http_file_") as m_down:
                    success = download_file(url=url1, filename=file1, hash=hash1, hash_algorithm="md5")
                    self.assertTrue(success)
                    self.assert_file_integrity(file1, hash1, algorithm)
                    self.assertEqual(m_down.call_count, 0)

                # Skip download because exists (with prefix dir)
                with patch("observatory_platform.http_download.download_http_file_") as m_down:
                    success, download_info = download_file(
                        url=url1, filename=file1, hash=hash1, hash_algorithm="md5", prefix_dir=tmpdir
                    )
                    self.assertTrue(success)
                    self.assert_file_integrity(file1, hash1, algorithm)
                    self.assertEqual(m_down.call_count, 0)

            # Get filename from Content-Disposition
            with CliRunner().isolated_filesystem() as tmpdir:
                with patch("observatory_platform.http_download.parse_header") as m_header:
                    m_header.return_value = (None, {"filename": "testfile"})
                    success, download_info = download_file(url=url1, hash=hash1, hash_algorithm="md5")
                    self.assertTrue(success)
                    self.assert_file_integrity("testfile", hash1, algorithm)

    def test_download_files_bad_input(self):
        self.assertRaises(Exception, download_files, download_list=[{"url": "myurl", "filename": "myfilename"}])
