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

import glob
import logging
import os
import shutil
import unittest
from unittest.mock import patch

import pendulum
import vcr
from click.testing import CliRunner
from natsort import natsorted

from observatory.dags.telescopes.crossref_metadata import (
    CrossrefMetadataRelease,
    CrossrefMetadataTelescope,
    extract_release,
    transform_release
)
from observatory.platform.utils.config_utils import telescope_path, SubFolder
from observatory.platform.utils.data_utils import _hash_file
from tests.observatory.test_utils import test_fixtures_path


class TestCrossrefMetadata(unittest.TestCase):
    """ Tests for the functions used by the crossref metadata telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefMetadata, self).__init__(*args, **kwargs)

        self.crossref_release_exists_path = os.path.join(test_fixtures_path(), 'vcr_cassettes',
                                                         'crossref_release_exists.yaml')
        self.test_release_path = os.path.join(test_fixtures_path(), 'telescopes', 'crossref_metadata.json.tar.gz')
        self.year = 3000
        self.month = 1
        self.date = pendulum.datetime(year=3000, month=1, day=1)
        self.download_file_name = 'crossref_metadata_3000_01_01.json.tar.gz'
        self.extract_folder = 'crossref_metadata_3000_01_01'
        self.transform_folder = 'crossref_metadata_3000_01_01'
        self.num_files = 5
        self.download_hash = 'ddcbcca0f8d63ce57906e76c787f243d'
        self.extract_hashes = ['d7acd01f729d62fbbaffa50b534025b2', '634724e4c9f773ca3b5a81b20f0ff005',
                               '2392895c2dacd6176b140435e97e1840', 'd1157cf952d4694d4f4c08ee36a18116',
                               '4128088d14446682bde8a1a1bc5e15b5']
        self.transform_hashes = ['5f27af4d3080b56484fa5567230aaf92', '10717291388b1c232416e720f6ec3f3d',
                                 '92faa40aa9353c832cd66349d32950ad', 'c98c07927b6b978696ffaf81ee632bbb',
                                 '92a3ac8f3110da83fcaede58e30f6ec3']

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_list_releases(self, mock_variable_get):
        """ Test that list releases returns a list of string with urls.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.crossref_release_exists_path):
                # Test a release that exists
                release = CrossrefMetadataRelease(2020, 6)
                self.assertTrue(release.exists())

                # Test a release that should not exist
                release = CrossrefMetadataRelease(self.year, self.month)
                self.assertFalse(release.exists())

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_download_path(self, mock_variable_get):
        """ Test that path of downloaded file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = CrossrefMetadataRelease(self.year, self.month)
            path = telescope_path(SubFolder.downloaded, CrossrefMetadataTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.download_file_name), release.download_path)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_extract_path(self, mock_variable_get):
        """ Test that path of decompressed/extracted file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = CrossrefMetadataRelease(self.year, self.month)
            path = telescope_path(SubFolder.extracted, CrossrefMetadataTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.extract_folder), release.extract_path)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_transform_path(self, mock_variable_get):
        """ Test that path of transformed file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = CrossrefMetadataRelease(self.year, self.month)
            path = os.path.join(telescope_path(SubFolder.transformed, CrossrefMetadataTelescope.DAG_ID),
                                self.transform_folder)
            self.assertEqual(path, release.transform_path)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_extract_release(self, mock_variable_get):
        """ Test that the release is decompressed as expected.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = CrossrefMetadataRelease(self.year, self.month)

            # 'download' release
            shutil.copyfile(self.test_release_path, release.download_path)

            success = extract_release(release)
            self.assertTrue(success)
            self.assertTrue(os.path.exists(release.extract_path))

            file_paths = natsorted(glob.glob(f'{release.extract_path}/*.json'))
            self.assertEqual(self.num_files, len(file_paths))

            for md5sum, file_path in zip(self.extract_hashes, file_paths):
                self.assertEqual(md5sum, _hash_file(file_path, algorithm='md5'))

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_transform_release(self, mock_variable_get):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = CrossrefMetadataRelease(self.year, self.month)

            # 'download' release
            shutil.copyfile(self.test_release_path, release.download_path)

            # Extract release
            success = extract_release(release)
            self.assertTrue(success)
            self.assertTrue(os.path.exists(release.extract_path))

            # Transform release
            success = transform_release(release)
            self.assertTrue(success)
            self.assertTrue(os.path.exists(release.transform_path))

            # Check files are correct
            file_paths = natsorted(glob.glob(f'{release.transform_path}/*.jsonl'))
            self.assertEqual(self.num_files, len(file_paths))

            for md5sum, file_path in zip(self.transform_hashes, file_paths):
                self.assertEqual(md5sum, _hash_file(file_path, algorithm='md5'))
