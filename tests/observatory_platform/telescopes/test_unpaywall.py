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

import logging
import os
import shutil
import unittest
from typing import List
from unittest.mock import patch

import pendulum
import vcr
from click.testing import CliRunner

from observatory_platform.telescopes.unpaywall import (
    UnpaywallRelease,
    UnpaywallTelescope,
    extract_release,
    list_releases,
    transform_release
)
from observatory_platform.utils.config_utils import telescope_path, SubFolder
from observatory_platform.utils.data_utils import _hash_file
from tests.observatory_platform.config import test_fixtures_path


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
        self.unpaywall_test_file = 'unpaywall_snapshot_3000-01-27T153236.jsonl.gz'
        self.unpaywall_test_url = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_3000-01-27T153236.jsonl.gz'
        self.unpaywall_test_date = pendulum.datetime(year=3000, month=1, day=27)
        self.unpaywall_test_download_file_name = 'unpaywall_3000_01_27.jsonl.gz'
        self.unpaywall_test_decompress_file_name = 'unpaywall_3000_01_27.jsonl'
        self.unpaywall_test_transform_file_name = 'unpaywall_3000_01_27.jsonl'
        self.unpaywall_test_download_hash = '90051478f7b6689838d58edfc2450cb3'
        self.unpaywall_test_decompress_hash = 'fe4e72ce54c4bb236802ddbb3dbee905'
        self.unpaywall_test_transform_hash = '62cbb5af5a78d2e0769a28d976971cba'
        self.start_date = pendulum.datetime(year=2018, month=3, day=29)
        self.end_date = pendulum.datetime(year=2020, month=4, day=29)

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_list_releases(self, mock_variable_get):
        """ Test that list releases returns a list of string with urls.

        :return: None.
        """

        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.list_unpaywall_releases_path):
                releases = list_releases(self.start_date, self.end_date)
                self.assertIsInstance(releases, List)
                for release in releases:
                    self.assertIsInstance(release, UnpaywallRelease)
                self.assertEqual(13, len(releases))

    def test_parse_release_date(self):
        """ Test that date obtained from url is string and in correct format.

        :return: None.
        """

        release_date = UnpaywallRelease.parse_release_date(self.unpaywall_test_file)
        self.assertEqual(self.unpaywall_test_date, release_date)

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_download(self, mock_variable_get):
        """ Test that path of downloaded file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Create data path and mock getting data path
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(self.unpaywall_test_file, self.unpaywall_test_date, self.unpaywall_test_date)
            path = telescope_path(SubFolder.downloaded, UnpaywallTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.unpaywall_test_download_file_name), release.filepath_download)

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_extract(self, mock_variable_get):
        """ Test that path of decompressed/extracted file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Create data path and mock getting data path
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(self.unpaywall_test_file, self.unpaywall_test_date, self.unpaywall_test_date)
            path = telescope_path(SubFolder.extracted, UnpaywallTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.unpaywall_test_decompress_file_name), release.filepath_extract)

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_transform(self, mock_variable_get):
        """ Test that path of transformed file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Create data path and mock getting data path
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(self.unpaywall_test_file, self.unpaywall_test_date, self.unpaywall_test_date)
            path = telescope_path(SubFolder.transformed, UnpaywallTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.unpaywall_test_transform_file_name), release.filepath_transform)

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_extract_release(self, mock_variable_get):
        """ Test that the release is decompressed as expected.

        :return: None.
        """

        # Create data path and mock getting data path
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(self.unpaywall_test_file, self.unpaywall_test_date, self.unpaywall_test_date)
            # 'download' release
            shutil.copyfile(self.unpaywall_test_path, release.filepath_download)

            decompress_file_path = extract_release(release)
            decompress_file_name = os.path.basename(decompress_file_path)

            self.assertTrue(os.path.exists(decompress_file_path))
            self.assertEqual(self.unpaywall_test_decompress_file_name, decompress_file_name)
            self.assertEqual(self.unpaywall_test_decompress_hash, _hash_file(decompress_file_path, algorithm='md5'))

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_transform_release(self, mock_variable_get):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        # Create data path and mock getting data path
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = UnpaywallRelease(self.unpaywall_test_file, self.unpaywall_test_date, self.unpaywall_test_date)
            shutil.copyfile(self.unpaywall_test_path, release.filepath_download)

            extract_release(release)
            transform_file_path = transform_release(release)
            transform_file_name = os.path.basename(transform_file_path)

            self.assertTrue(os.path.exists(transform_file_path))
            self.assertEqual(self.unpaywall_test_transform_file_name, transform_file_name)
            self.assertEqual(self.unpaywall_test_transform_hash, _hash_file(transform_file_path, algorithm='md5'))
