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
from observatory.platform.utils.config_utils import telescope_path, SubFolder
from observatory.platform.utils.data_utils import _hash_file
from observatory.platform.utils.test_utils import gzip_file_crc

from observatory.dags.telescopes.geonames import (
    GeonamesRelease,
    GeonamesTelescope,
    transform_release,
    first_sunday_of_month
)
from observatory.dags.telescopes.geonames import fetch_release_date
from tests.observatory.config import test_fixtures_path


class TestGeonames(unittest.TestCase):
    """ Tests for the functions used by the geonames telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestGeonames, self).__init__(*args, **kwargs)

        # GeoNames test release
        self.geonames_test_path = os.path.join(test_fixtures_path(), 'telescopes', 'geonames.txt')
        self.fetch_release_date_path = os.path.join(test_fixtures_path(), 'vcr_cassettes',
                                                    'geonames_fetch_release_date.yaml')

        self.geonames_test_date = pendulum.datetime(year=3000, month=1, day=1)
        self.geonames_test_download_file_name = 'geonames_3000_01_01.zip'
        self.geonames_test_decompress_file_name = 'geonames_3000_01_01.txt'
        self.geonames_test_transform_file_name = 'geonames_3000_01_01.csv.gz'
        self.geonames_test_decompress_hash = 'de1bf005df4840d16faf598999d72051'
        self.geonames_test_transform_crc = '26c14e16'

        logging.info("Check that test fixtures exist")
        self.assertTrue(os.path.isfile(self.geonames_test_path))
        self.assertTrue(self.geonames_test_decompress_hash, _hash_file(self.geonames_test_path, algorithm='md5'))

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def test_first_sunday_of_month(self):
        # Test when the date is later in the month
        datetime = pendulum.datetime(year=2020, month=7, day=28)
        expected_datetime = pendulum.datetime(year=2020, month=7, day=5)
        actual_datetime = first_sunday_of_month(datetime)
        self.assertEqual(expected_datetime, actual_datetime)

        # Test a date when the current date is a Sunday
        datetime = pendulum.datetime(year=2020, month=11, day=1)
        expected_datetime = pendulum.datetime(year=2020, month=11, day=1)
        actual_datetime = first_sunday_of_month(datetime)
        self.assertEqual(expected_datetime, actual_datetime)

    def test_fetch_release_date(self):
        """ Test that fetch_release_date function works.

        :return: None.
        """

        with vcr.use_cassette(self.fetch_release_date_path):
            date = fetch_release_date()
            self.assertEqual(date, pendulum.datetime(year=2020, month=7, day=16, hour=1, minute=22, second=15))

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_download(self, mock_variable_get):
        """ Test that path of downloaded file is correct.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = GeonamesRelease(self.geonames_test_date)
            file_path_download = release.filepath_download
            path = telescope_path(SubFolder.downloaded, GeonamesTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.geonames_test_download_file_name), file_path_download)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_decompress(self, mock_variable_get):
        """ Test that path of downloaded file is correct.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = GeonamesRelease(self.geonames_test_date)
            file_path_decompress = release.filepath_extract
            path = telescope_path(SubFolder.extracted, GeonamesTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.geonames_test_decompress_file_name), file_path_decompress)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_extract(self, mock_variable_get):
        """ Test that path of decompressed/extracted file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = GeonamesRelease(self.geonames_test_date)
            file_path_extract = release.filepath_extract
            path = telescope_path(SubFolder.extracted, GeonamesTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.geonames_test_decompress_file_name), file_path_extract)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_transform(self, mock_variable_get):
        """ Test that path of transformed file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = GeonamesRelease(self.geonames_test_date)
            file_path_transform = release.filepath_transform
            path = telescope_path(SubFolder.transformed, GeonamesTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.geonames_test_transform_file_name), file_path_transform)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_get_blob_name(self, mock_variable_get):
        """ Test get_blob_name.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = GeonamesRelease(self.geonames_test_date)
            self.assertEqual(release.get_blob_name(SubFolder.downloaded),
                             'telescopes/geonames/geonames_3000_01_01.zip')
            self.assertEqual(release.get_blob_name(SubFolder.transformed),
                             'telescopes/geonames/geonames_3000_01_01.csv.gz')

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_transform_release(self, mock_variable_get):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = GeonamesRelease(self.geonames_test_date)
            shutil.copyfile(self.geonames_test_path, release.filepath_extract)

            transform_file_path = transform_release(release)
            transform_file_name = os.path.basename(transform_file_path)

            self.assertTrue(os.path.exists(transform_file_path))
            self.assertEqual(self.geonames_test_transform_file_name, transform_file_name)
            gzip_crc = gzip_file_crc(transform_file_path)
            self.assertEqual(self.geonames_test_transform_crc, gzip_crc)
