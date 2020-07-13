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

from academic_observatory.telescopes.unpaywall import (
    UnpaywallRelease,
    UnpaywallTelescope,
    extract_release,
    list_releases,
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

        # Unpaywall releases list
        self.list_unpaywall_releases_path = os.path.join(test_fixtures_path(), 'vcr_cassettes',
                                                         'list_unpaywall_releases.yaml')
        self.list_unpaywall_releases_hash = '78d1a129cb0aba072ca49e2599f60c10'

        # Unpaywall test release
        self.unpaywall_test_path = os.path.join(test_fixtures_path(), 'telescopes', 'unpaywall.jsonl.gz')
        self.unpaywall_test_url = UnpaywallTelescope.TELESCOPE_DEBUG_URL
        self.unpaywall_test_date = '3000-01-27'
        self.unpaywall_test_download_file_name = 'unpaywall_3000_01_27.jsonl.gz'
        self.unpaywall_test_decompress_file_name = 'unpaywall_3000_01_27.jsonl'
        self.unpaywall_test_transform_file_name = 'unpaywall_3000_01_27.jsonl'
        self.unpaywall_test_download_hash = '90051478f7b6689838d58edfc2450cb3'
        self.unpaywall_test_decompress_hash = 'fe4e72ce54c4bb236802ddbb3dbee905'
        self.unpaywall_test_transform_hash = '62cbb5af5a78d2e0769a28d976971cba'

        logging.info("Check that test fixtures exist")
        self.assertTrue(os.path.isfile(self.list_unpaywall_releases_path))
        self.assertTrue(os.path.isfile(self.unpaywall_test_path))
        self.assertTrue(self.list_unpaywall_releases_hash,
                        _hash_file(self.list_unpaywall_releases_path, algorithm='md5'))
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

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_release_date(self, mock_variable_get):
        """ Test that date obtained from url is string and in correct format.

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create data path and mock getting data path
            data_path = 'data'
            mock_variable_get.return_value = data_path
            os.makedirs(data_path, exist_ok=True)

            with vcr.use_cassette(self.list_unpaywall_releases_path):
                releases = list_releases(UnpaywallTelescope.TELESCOPE_URL)
                for release_url in releases:
                    unpaywall_release = UnpaywallRelease(release_url)
                    date = unpaywall_release.date
                    self.assertIsInstance(date, str)
                    self.assertTrue(datetime.datetime.strptime(date, "%Y-%m-%d"))

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_download(self, mock_variable_get):
        """ Test that path of downloaded file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create data path and mock getting data path
            data_path = 'data'
            mock_variable_get.return_value = data_path
            os.makedirs(data_path, exist_ok=True)

            with CliRunner().isolated_filesystem():
                unpaywall_release = UnpaywallRelease(self.unpaywall_test_url)
                file_path_download = unpaywall_release.filepath_download
                path = telescope_path(SubFolder.downloaded, UnpaywallTelescope.DAG_ID)
                self.assertEqual(os.path.join(path, self.unpaywall_test_download_file_name), file_path_download)

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_extract(self, mock_variable_get):
        """ Test that path of decompressed/extracted file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create data path and mock getting data path
            data_path = 'data'
            mock_variable_get.return_value = data_path
            os.makedirs(data_path, exist_ok=True)

            with CliRunner().isolated_filesystem():
                unpaywall_release = UnpaywallRelease(self.unpaywall_test_url)
                file_path_extract = unpaywall_release.filepath_extract
                path = telescope_path(SubFolder.extracted, UnpaywallTelescope.DAG_ID)
                self.assertEqual(os.path.join(path, self.unpaywall_test_decompress_file_name), file_path_extract)

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_transform(self, mock_variable_get):
        """ Test that path of transformed file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create data path and mock getting data path
            data_path = 'data'
            mock_variable_get.return_value = data_path
            os.makedirs(data_path, exist_ok=True)

            with CliRunner().isolated_filesystem():
                unpaywall_release = UnpaywallRelease(self.unpaywall_test_url)
                file_path_transform = unpaywall_release.filepath_transform
                path = telescope_path(SubFolder.transformed, UnpaywallTelescope.DAG_ID)
                self.assertEqual(os.path.join(path, self.unpaywall_test_transform_file_name), file_path_transform)

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_download_release_date(self, mock_variable_get):
        """ Test that the test url contains the correct date.

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create data path and mock getting data path
            data_path = 'data'
            mock_variable_get.return_value = data_path
            os.makedirs(data_path, exist_ok=True)

            unpaywall_release = UnpaywallRelease(self.unpaywall_test_url)
            self.assertEqual(self.unpaywall_test_date, unpaywall_release.date)

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_decompress_release(self, mock_variable_get):
        """ Test that the release is decompressed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Create data path and mock getting data path
            data_path = 'data'
            mock_variable_get.return_value = data_path
            os.makedirs(data_path, exist_ok=True)

            unpaywall_release = UnpaywallRelease(self.unpaywall_test_url)
            # 'download' release
            shutil.copyfile(self.unpaywall_test_path, unpaywall_release.filepath_download)

            decompress_file_path = extract_release(unpaywall_release)
            decompress_file_name = os.path.basename(decompress_file_path)

            self.assertTrue(os.path.exists(decompress_file_path))
            self.assertEqual(self.unpaywall_test_decompress_file_name, decompress_file_name)
            self.assertEqual(self.unpaywall_test_decompress_hash, _hash_file(decompress_file_path, algorithm='md5'))

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_transform_release(self, mock_variable_get):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Create data path and mock getting data path
            data_path = 'data'
            mock_variable_get.return_value = data_path
            os.makedirs(data_path, exist_ok=True)

            unpaywall_release = UnpaywallRelease(self.unpaywall_test_url)
            shutil.copyfile(self.unpaywall_test_path, unpaywall_release.filepath_download)

            extract_release(unpaywall_release)
            transform_file_path = transform_release(unpaywall_release)
            transform_file_name = os.path.basename(transform_file_path)

            self.assertTrue(os.path.exists(transform_file_path))
            self.assertEqual(self.unpaywall_test_transform_file_name, transform_file_name)
            self.assertEqual(self.unpaywall_test_transform_hash, _hash_file(transform_file_path, algorithm='md5'))
