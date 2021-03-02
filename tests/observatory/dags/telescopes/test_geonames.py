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

from observatory.dags.telescopes.geonames import fetch_release_date, GeonamesRelease, first_sunday_of_month
from observatory.platform.utils.file_utils import _hash_file, gzip_file_crc
from tests.observatory.test_utils import test_fixtures_path


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

    @patch('observatory.platform.utils.template_utils.AirflowVariable.get')
    def test_transform_release(self, mock_variable_get):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = GeonamesRelease('geonames', self.geonames_test_date)
            shutil.copyfile(self.geonames_test_path, release.extract_path)

            # Transform
            release.transform()

            # Assert file is as expected
            self.assertTrue(os.path.exists(release.transform_path))
            gzip_crc = gzip_file_crc(release.transform_path)
            self.assertEqual(self.geonames_test_transform_crc, gzip_crc)
