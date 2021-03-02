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
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pendulum
import vcr
from click.testing import CliRunner
from observatory.dags.telescopes.ucl_discovery import UclDiscoveryRelease, UclDiscoveryTelescope
from observatory.platform.utils.file_utils import _hash_file, gzip_file_crc

from tests.observatory.test_utils import test_fixtures_path


def mock_airflow_variable(arg):
    values = {
        'project_id': 'project',
        'download_bucket_name': 'download-bucket',
        'transform_bucket_name': 'transform-bucket',
        'data_path': 'data',
        'data_location': 'US'
    }
    return values[arg]


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code):
            self.content = content
            self.status_code = status_code

    if args[0] == 'test_status_code':
        return MockResponse(b'no content', 404)
    elif args[0] == 'test_empty_csv':
        return MockResponse(b'"eprintid","rev_number","eprint_status","userid","importid","source"', 200)


@patch('observatory.platform.utils.template_utils.AirflowVariable.get')
class TestUclDiscovery(unittest.TestCase):
    """ Tests for the functions used by the UclDiscovery telescope """

    def __init__(self, *args, **kwargs, ):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestUclDiscovery, self).__init__(*args, **kwargs)

        # Paths
        self.vcr_cassettes_path = os.path.join(test_fixtures_path(), 'vcr_cassettes')
        self.download_path = os.path.join(self.vcr_cassettes_path, 'ucl_discovery_2008-02-01.yaml')
        self.country_report_path = os.path.join(self.vcr_cassettes_path,
                                                'ucl_discovery_country_2008-02-01.yaml')

        # Telescope instance
        self.ucl_discovery = UclDiscoveryTelescope()

        # Dag run info
        self.start_date = pendulum.parse('2021-01-01')
        self.end_date = pendulum.parse('2021-02-01')
        self.download_hash = 'ed054db8c4221b7e8055507c4718b7f2'
        self.transform_crc = '12444a7d'

        # Create release instance that is used to test download/transform
        with patch('observatory.platform.utils.template_utils.AirflowVariable.get') as mock_variable_get:
            mock_variable_get.side_effect = mock_airflow_variable
            self.release = UclDiscoveryRelease(self.ucl_discovery.dag_id, self.start_date, self.end_date)

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def test_make_release(self, mock_variable_get):
        """ Check that make_release returns a list with one UclDiscoveryRelease instance.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = mock_airflow_variable

        schedule_interval = '0 0 1 * *'
        execution_date = self.start_date
        releases = self.ucl_discovery.make_release(dag=SimpleNamespace(normalized_schedule_interval=schedule_interval),
                                                   dag_run=SimpleNamespace(execution_date=execution_date))
        self.assertEqual(1, len(releases))
        self.assertIsInstance(releases, list)
        self.assertIsInstance(releases[0], UclDiscoveryRelease)

    def test_download_release(self, mock_variable_get):
        """ Download release to check it has the expected md5 sum and test unsuccessful mocked responses.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return:
        """
        mock_variable_get.side_effect = mock_airflow_variable

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.download_path):
                success = self.release.download()

                # Check that download is successful
                self.assertTrue(success)

                # Check that file has expected hash
                self.assertEqual(1, len(self.release.download_files))
                self.assertEqual(self.release.download_path, self.release.download_files[0])
                self.assertTrue(os.path.exists(self.release.download_path))
                self.assertEqual(self.download_hash, _hash_file(self.release.download_path, algorithm='md5'))

            with patch('observatory.platform.utils.url_utils.requests.Session.get') as mock_requests_get:
                # mock response status code is not 200
                mock_requests_get.side_effect = mocked_requests_get
                self.release.eprint_metadata_url = 'test_status_code'
                success = self.release.download()
                self.assertFalse(success)

                # mock response content is empty CSV file (only headers)
                self.release.eprint_metadata_url = 'test_empty_csv'
                success = self.release.download()
                self.assertFalse(success)

    def test_transform_release(self, mock_variable_get):
        """ Test that the release is transformed as expected.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = mock_airflow_variable

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.download_path):
                self.release.download()

            # use three eprintids for transform test, for first one the country downloads is empty
            with open(self.release.download_path, 'r') as f_in:
                lines = f_in.readlines()
            with open(self.release.download_path, 'w') as f_out:
                f_out.writelines(lines[0:1] + lines[23:36] + lines[36:61] + lines[61:88])

            with vcr.use_cassette(self.country_report_path):
                self.release.transform()

            # Check that file has expected crc
            self.assertEqual(1, len(self.release.transform_files))
            self.assertEqual(self.release.transform_path, self.release.transform_files[0])
            self.assertTrue(os.path.exists(self.release.transform_path))
            self.assertEqual(self.transform_crc, gzip_file_crc(self.release.transform_path))
