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
from unittest.mock import patch

import pendulum
import vcr
from click.testing import CliRunner
from observatory.dags.workflows.oapen_metadata_telescope import (
    OapenMetadataRelease,
    OapenMetadataTelescope,
)
from observatory.platform.utils.file_utils import _hash_file, gzip_file_crc
from tests.observatory.test_utils import test_fixtures_path


class MockTaskInstance:
    def __init__(self, start_date: pendulum.DateTime, end_date: pendulum.DateTime, first_release: bool):
        """Construct a MockTaskInstance. This mocks the airflow TaskInstance and is passed as a keyword arg to the
        make_release function.
        :param start_date: Start date of dag run
        :param end_date: End date of dag run
        :param first_release: Whether first time a release is processed
        """
        self.start_date = start_date
        self.end_date = end_date
        self.first_release = first_release

    def xcom_pull(self, key: str, include_prior_dates: bool):
        """Mock xcom_pull method of airflow TaskInstance.
        :param key: -
        :param include_prior_dates: -
        :return: Records list
        """
        return self.start_date, self.end_date, self.first_release


def side_effect(arg):
    values = {
        "project_id": "project",
        "download_bucket_name": "download-bucket",
        "transform_bucket_name": "transform-bucket",
        "data_path": "data",
        "data_location": "US",
    }
    return values[arg]


@patch("observatory.platform.utils.workflow_utils.AirflowVariable.get")
class TestOapenMetadataTelescope(unittest.TestCase):
    """Tests for the functions used by the OapenMetadata telescope"""

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestOapenMetadataTelescope, self).__init__(*args, **kwargs)
        # Paths
        self.vcr_cassettes_path = os.path.join(test_fixtures_path(), "vcr_cassettes")
        self.download_path = os.path.join(self.vcr_cassettes_path, "oapen_metadata_2021-02-19.yaml")

        # Telescope instance
        self.oapen_metadata = OapenMetadataTelescope()

        # Dag run info
        self.start_date = pendulum.parse("2021-02-12")
        self.end_date = pendulum.parse("2021-02-19")
        self.download_hash = "4c261bbfaceafde1854e102d31fcbc0e"
        self.transform_crc = "415144d7"

        # Create release instance that is used to test download/transform
        with patch("observatory.platform.utils.workflow_utils.AirflowVariable.get") as mock_variable_get:
            mock_variable_get.side_effect = side_effect

            self.release = OapenMetadataRelease(
                self.oapen_metadata.dag_id, self.start_date, self.end_date, first_release=True
            )

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def test_make_release(self, mock_variable_get):
        """Check that make_release returns a list of GridRelease instances.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = side_effect

        first_release = self.oapen_metadata.make_release(ti=MockTaskInstance(self.start_date, self.end_date, True))
        later_release = self.oapen_metadata.make_release(ti=MockTaskInstance(self.start_date, self.end_date, False))
        for release in [first_release, later_release]:
            self.assertIsInstance(release, OapenMetadataRelease)
        self.assertTrue(first_release.first_release)
        self.assertFalse(later_release.first_release)

    def test_download_release(self, mock_variable_get):
        """Download release and check it has the expected md5 sum.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return:
        """
        mock_variable_get.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.download_path):
                success = self.release.download()

                # Check that download is successful
                self.assertTrue(success)

                # Check that file has expected hash
                self.assertEqual(1, len(self.release.download_files))
                file_path = self.release.download_files[0]
                self.assertTrue(os.path.exists(file_path))
                self.assertEqual(self.download_hash, _hash_file(file_path, algorithm="md5"))

    def test_transform_release(self, mock_variable_get):
        """Test that the release is transformed as expected.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.download_path):
                self.release.download()
                self.release.transform()

                # Check that file has expected crc
                self.assertEqual(1, len(self.release.transform_files))
                file_path = self.release.transform_files[0]
                self.assertTrue(os.path.exists(file_path))
                self.assertEqual(self.transform_crc, gzip_file_crc(file_path))
