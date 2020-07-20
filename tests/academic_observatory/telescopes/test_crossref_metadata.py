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
from unittest.mock import patch

import pendulum
import vcr
from click.testing import CliRunner

from academic_observatory.telescopes.crossref_metadata import (
    CrossrefMetadataRelease,
    CrossrefMetadataTelescope,
    extract_release,
    transform_release
)
from academic_observatory.utils.config_utils import telescope_path, SubFolder
from academic_observatory.utils.data_utils import _hash_file
from tests.academic_observatory.config import test_fixtures_path


class TestCrossrefMetadata(unittest.TestCase):
    """ Tests for the functions used by the crossref metadata telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefMetadata, self).__init__(*args, **kwargs)

        # Crossref releases list
        self.crossref_release_exists_path = os.path.join(test_fixtures_path(), 'vcr_cassettes',
                                                         'crossref_release_exists.yaml')
        self.list_crossref_releases_hash = 'aa9b6b37a4abafd5760301d903373594'

        # Crossref test release
        self.crossref_test_path = os.path.join(test_fixtures_path(), 'telescopes', 'crossref_metadata.json.tar.gz')
        # self.crossref_test_url = CrossrefMetadataTelescope.TELESCOPE_DEBUG_URL.format(year=3000, month=1)

        self.crossref_test_year = 3000
        self.crossref_test_month = 1
        self.crossref_test_date = pendulum.datetime(year=3000, month=1, day=1)
        self.crossref_test_download_file_name = 'crossref_metadata_3000_01_01.json.tar.gz'
        self.crossref_test_extracted_folder = 'crossref_metadata_3000_01_01'
        self.crossref_test_extracted_folder = 'crossref_metadata_3000_01_01'
        self.crossref_test_download_hash = 'ddcbcca0f8d63ce57906e76c787f243d'
        self.crossref_test_decompress_hash = 'df9543823bfdedac5b43685bc1fb62fa'
        self.crossref_test_transform_hash = '48262de7b058994e698083d64d4a6148'

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_list_releases(self, mock_variable_get):
        """ Test that list releases returns a list of string with urls.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.crossref_release_exists_path):
                # Mock data variable
                data_path = 'data'
                mock_variable_get.return_value = data_path

                # Test a release that exists
                release = CrossrefMetadataRelease(2020, 6)
                self.assertTrue(release.exists())

                # Test a release that should not exist
                release = CrossrefMetadataRelease(self.crossref_test_year, self.crossref_test_month)
                self.assertFalse(release.exists())

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_download_path(self, mock_variable_get):
        """ Test that path of downloaded file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Mock data variable
            data_path = 'data'
            mock_variable_get.return_value = data_path

            release = CrossrefMetadataRelease(self.crossref_test_year, self.crossref_test_month)
            path = telescope_path(SubFolder.downloaded, CrossrefMetadataTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.crossref_test_download_file_name), release.download_path)

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_extract_path(self, mock_variable_get):
        """ Test that path of decompressed/extracted file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Mock data variable
            data_path = 'data'
            mock_variable_get.return_value = data_path

            release = CrossrefMetadataRelease(self.crossref_test_year, self.crossref_test_month)
            path = telescope_path(SubFolder.extracted, CrossrefMetadataTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.crossref_test_extracted_folder), release.extract_path)

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_transform_path(self, mock_variable_get):
        """ Test that path of transformed file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Mock data variable
            data_path = 'data'
            mock_variable_get.return_value = data_path

            release = CrossrefMetadataRelease(self.crossref_test_year, self.crossref_test_month)
            path = os.path.join(telescope_path(SubFolder.transformed, CrossrefMetadataTelescope.DAG_ID),
                                self.crossref_test_extracted_folder)
            self.assertEqual(path, release.transform_path)

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_extract_release(self, mock_variable_get):
        """ Test that the release is decompressed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Mock data variable
            data_path = 'data'
            mock_variable_get.return_value = data_path

            release = CrossrefMetadataRelease(self.crossref_test_year, self.crossref_test_month)
            # 'download' release
            shutil.copyfile(self.crossref_test_path, release.filepath_download)

            decompress_file_path = extract_release(release)
            decompress_file_name = os.path.basename(decompress_file_path)

            self.assertTrue(os.path.exists(decompress_file_path))
            self.assertEqual(self.crossref_test_extracted_folder, decompress_file_name)
            self.assertEqual(self.crossref_test_decompress_hash, _hash_file(decompress_file_path, algorithm='md5'))

    @patch('academic_observatory.utils.config_utils.airflow.models.Variable.get')
    def test_transform_release(self, mock_variable_get):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Mock data variable
            data_path = 'data'
            mock_variable_get.return_value = data_path

            release = CrossrefMetadataRelease(self.crossref_test_year, self.crossref_test_month)
            shutil.copyfile(self.crossref_test_path, release.filepath_download)

            extract_release(release)
            transform_file_path = transform_release(release)
            transform_file_name = os.path.basename(transform_file_path)

            self.assertTrue(os.path.exists(transform_file_path))
            self.assertEqual(self.crossref_test_extracted_folder, transform_file_name)
            self.assertEqual(self.crossref_test_transform_hash, _hash_file(transform_file_path, algorithm='md5'))
