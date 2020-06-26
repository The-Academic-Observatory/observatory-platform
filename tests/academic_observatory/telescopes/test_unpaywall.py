# Copyright 2020 Curtin University
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

# Author: Aniek Roelofs

import datetime
import logging
import os
import shutil
import unittest
import vcr
from click.testing import CliRunner
from typing import List
from unittest.mock import patch

from academic_observatory.telescopes.unpaywall import(
    UnpaywallTelescope,
    decompress_release,
    filepath_download,
    filepath_extract,
    filepath_transform,
    list_releases,
    release_date,
    transform_release
)
from academic_observatory.utils.config_utils import telescope_path, SubFolder
from academic_observatory.utils.data_utils import _hash_file
from tests.academic_observatory.config import test_fixtures_path


class TestUnpaywall(unittest.TestCase):
    """ Tests for the functions used by the unpaywall telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestUnpaywall, self).__init__(*args, **kwargs)

        # Paths
        self.vcr_cassettes_path = os.path.join(test_fixtures_path(), 'vcr_cassettes')
        self.list_unpaywall_releases_path = os.path.join(self.vcr_cassettes_path, 'list_unpaywall_releases.yaml')
        self.list_unpaywall_releases_hash = '78d1a129cb0aba072ca49e2599f60c10'

        # Unpaywall Test Release
        self.unpaywall_test_url = UnpaywallTelescope.TELESCOPE_DEBUG_URL
        self.unpaywall_test_path = UnpaywallTelescope.DEBUG_FILE_PATH
        self.unpaywall_test_date = '3000-01-27'
        self.unpaywall_test_download_hash = '90051478f7b6689838d58edfc2450cb3'
        self.unpaywall_test_decompress_hash = 'fe4e72ce54c4bb236802ddbb3dbee905'
        self.unpaywall_test_transform_hash = '62cbb5af5a78d2e0769a28d976971cba'
        self.unpaywall_test_download_file_name = 'unpaywall_3000_01_27.jsonl.gz'
        self.unpaywall_test_decompress_file_name = 'unpaywall_3000_01_27.jsonl'
        self.unpaywall_test_transform_file_name = 'unpaywall_3000_01_27.jsonl'

        logging.info("Check that test fixtures exist")
        self.assertTrue(os.path.isfile(self.list_unpaywall_releases_path))
        self.assertTrue(os.path.isfile(self.unpaywall_test_path))
        self.assertTrue(self.list_unpaywall_releases_hash, _hash_file(self.list_unpaywall_releases_path, algorithm='md5'))
        self.assertTrue(self.unpaywall_test_download_hash, _hash_file(self.unpaywall_test_path, algorithm='md5'))

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def test_list_releases(self):
        """ Test that list releases returns a list of string with urls.

        :return: None.
        """
        with vcr.use_cassette(self.list_unpaywall_releases_path):
            releases = list_releases(UnpaywallTelescope.TELESCOPE_URL)
            self.assertIsInstance(releases, List)
            for release in releases:
                self.assertIsInstance(release, str)

    def test_release_date(self):
        """ Test that date obtained from url is string and in correct format.

        :return: None.
        """
        with vcr.use_cassette(self.list_unpaywall_releases_path):
            releases = list_releases(UnpaywallTelescope.TELESCOPE_URL)
            for release in releases:
                date = release_date(release)
                self.assertIsInstance(date, str)
                self.assertTrue(datetime.datetime.strptime(date, "%Y-%m-%d"))

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_download(self, home_mock):
        """ Test that path of downloaded file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                file_path_download = filepath_download(self.unpaywall_test_url)
                path = telescope_path(UnpaywallTelescope.DAG_ID, SubFolder.downloaded)
                self.assertEqual(os.path.join(path, self.unpaywall_test_download_file_name), file_path_download)

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_extract(self, home_mock):
        """ Test that path of decompressed/extracted file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                file_path_extract = filepath_extract(self.unpaywall_test_url)
                path = telescope_path(UnpaywallTelescope.DAG_ID, SubFolder.extracted)
                self.assertEqual(os.path.join(path, self.unpaywall_test_decompress_file_name), file_path_extract)

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_transform(self, home_mock):
        """ Test that path of transformed file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                file_path_transform = filepath_transform(self.unpaywall_test_url)
                path = telescope_path(UnpaywallTelescope.DAG_ID, SubFolder.transformed)
                self.assertEqual(os.path.join(path, self.unpaywall_test_transform_file_name), file_path_transform)

    def test_download_release_date(self):
        """ Test that the test url contains the correct date.

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            self.assertEqual(self.unpaywall_test_date, release_date(self.unpaywall_test_url))

    def test_decompress_release(self):
        """ Test that the release is decompressed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # 'download' release
            shutil.copyfile(self.unpaywall_test_path, filepath_download(self.unpaywall_test_url))

            decompress_file_path = decompress_release(self.unpaywall_test_url)
            decompress_file_name = os.path.basename(decompress_file_path)

            self.assertTrue(os.path.exists(decompress_file_path))
            self.assertEqual(self.unpaywall_test_decompress_file_name, decompress_file_name)
            self.assertEqual(self.unpaywall_test_decompress_hash, _hash_file(decompress_file_path, algorithm='md5'))

    def test_transform_release(self):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            shutil.copyfile(self.unpaywall_test_path, filepath_download(self.unpaywall_test_url))

            decompress_file_path = decompress_release(self.unpaywall_test_url)
            transform_file_path = transform_release(self.unpaywall_test_url)
            transform_file_name = os.path.basename(transform_file_path)

            self.assertTrue(os.path.exists(transform_file_path))
            self.assertEqual(self.unpaywall_test_transform_file_name, transform_file_name)
            self.assertEqual(self.unpaywall_test_transform_hash, _hash_file(transform_file_path, algorithm='md5'))