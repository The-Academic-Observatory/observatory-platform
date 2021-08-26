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

# Author: James Diprose, Aniek Roelofs

import logging
import os
import unittest
from unittest.mock import patch

import pendulum
import vcr
from click.testing import CliRunner

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.grid_telescope import (
    GridRelease,
    GridTelescope,
    list_grid_records,
)
from observatory.platform.utils.file_utils import _hash_file, gzip_file_crc


class MockTaskInstance:
    def __init__(self, records):
        """Construct a MockTaskInstance. This mocks the airflow TaskInstance and is passed as a keyword arg to the
        make_release function.
        :param records: List of record info, returned as value during xcom_pull
        """
        self.records = records

    def xcom_pull(self, key: str, task_ids: str, include_prior_dates: bool):
        """Mock xcom_pull method of airflow TaskInstance.
        :param key: -
        :param task_ids: -
        :param include_prior_dates: -
        :return: Records list
        """
        return self.records


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
class TestGridTelescope(unittest.TestCase):
    """Tests for the functions used by the GRID telescope"""

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestGridTelescope, self).__init__(*args, **kwargs)
        # Telescope instance
        self.grid = GridTelescope()

        # Paths
        self.vcr_cassettes_path = test_fixtures_folder("grid")
        self.list_grid_records_path = os.path.join(self.vcr_cassettes_path, "list_grid_releases.yaml")

        # Contains GRID releases 2015-09-22 and 2015-10-09 (format for both is .csv and .json files)
        with patch("observatory.platform.utils.workflow_utils.AirflowVariable.get") as mock_variable_get:
            mock_variable_get.side_effect = side_effect
            self.grid_run_2015_10_18 = {
                "start_date": pendulum.datetime(2015, 10, 11),
                "end_date": pendulum.datetime(2015, 10, 18),
                "records": [
                    {"article_ids": [1570968, 1570967], "release_date": pendulum.parse("2015-10-09T00:00:00+00:00")},
                    {"article_ids": [1553266, 1553267], "release_date": pendulum.parse("2015-09-22T00:00:00+00:00")},
                ],
                # there are 2 releases in this run, but use only 1 for testing
                "release": GridRelease(
                    self.grid.dag_id, ["1553266", "1553267"], pendulum.parse("2015-09-22T00:00:00+00:00")
                ),
                "download_path": os.path.join(self.vcr_cassettes_path, "grid_2015-09-22.yaml"),
                "download_hash": "c6fd33fd31b6699a2f19622f0283f4f1",
                "extract_hash": "c6fd33fd31b6699a2f19622f0283f4f1",
                "transform_crc": "eb66ae78",
            }

            # Contains GRID release 2020-03-15 (format is a .zip file, which is more common)
            self.grid_run_2020_03_27 = {
                "start_date": pendulum.datetime(2020, 3, 20),
                "end_date": pendulum.datetime(2020, 3, 27),
                "records": [{"article_ids": [12022722], "release_date": pendulum.parse("2020-03-15T00:00:00+00:00")}],
                "release": GridRelease(self.grid.dag_id, ["12022722"], pendulum.parse("2020-03-15T00:00:00+00:00")),
                "download_path": os.path.join(self.vcr_cassettes_path, "grid_2020-03-15.yaml"),
                "download_hash": "3d300affce1666ac50b8d945c6ca4c5a",
                "extract_hash": "5aff68e9bf72e846a867e91c1fa206a0",
                "transform_crc": "77bc8585",
            }

        self.grid_runs = [self.grid_run_2015_10_18, self.grid_run_2020_03_27]

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def test_list_grid_records(self, mock_variable_get):
        """Check that list grid records returns a list of dictionaries with records in the correct format.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        with vcr.use_cassette(self.list_grid_records_path):
            start_date = self.grid_run_2015_10_18["start_date"]
            end_date = self.grid_run_2015_10_18["end_date"]
            records = list_grid_records(start_date, end_date, GridTelescope.GRID_DATASET_URL)
            self.assertEqual(self.grid_run_2015_10_18["records"], records)

    def test_make_release(self, mock_variable_get):
        """Check that make_release returns a list of GridRelease instances.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = side_effect

        for run in self.grid_runs:
            records = run["records"]

            releases = self.grid.make_release(ti=MockTaskInstance(records))
            self.assertIsInstance(releases, list)
            for release in releases:
                self.assertIsInstance(release, GridRelease)

    def test_download_release(self, mock_variable_get):
        """Download two specific GRID releases and check they have the expected md5 sum.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return:
        """
        mock_variable_get.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            for run in self.grid_runs:
                release = run["release"]
                with vcr.use_cassette(run["download_path"]):
                    downloads = release.download()
                    # Check that returned downloads has correct length
                    self.assertEqual(1, len(downloads))

                    # Check that file has expected hash
                    file_path = downloads[0]
                    self.assertTrue(os.path.exists(file_path))
                    self.assertEqual(run["download_hash"], _hash_file(file_path, algorithm="md5"))

    def test_extract_release(self, mock_variable_get):
        """Test that the GRID releases are extracted as expected, both for an unzipped json file and a zip file.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            for run in self.grid_runs:
                release = run["release"]
                with vcr.use_cassette(run["download_path"]):
                    release.download()
                    release.extract()

                    self.assertEqual(1, len(release.extract_files))
                    self.assertEqual(run["extract_hash"], _hash_file(release.extract_files[0], algorithm="md5"))

    def test_transform_release(self, mock_variable_get):
        """Test that the GRID releases are transformed as expected.

        :param mock_variable_get: Mock result of airflow's Variable.get() function
        :return: None.
        """
        mock_variable_get.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            for run in self.grid_runs:
                release = run["release"]
                with vcr.use_cassette(run["download_path"]):
                    release.download()
                    release.extract()
                    release.transform()

                    self.assertEqual(1, len(release.transform_files))
                    self.assertEqual(run["transform_crc"], gzip_file_crc(release.transform_files[0]))
