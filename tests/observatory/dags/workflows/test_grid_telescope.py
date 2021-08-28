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

# Author: James Diprose, Aniek Roelofs, Tuan Chien

import logging
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pendulum
import vcr
from airflow.exceptions import AirflowException
from click.testing import CliRunner
from observatory.dags.workflows.grid_telescope import (
    GridRelease,
    GridTelescope,
    list_grid_records,
)
from observatory.platform.utils.file_utils import _hash_file, gzip_file_crc
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    test_fixtures_path,
)
from observatory.platform.utils.workflow_utils import blob_name, table_ids_from_path


class MockResponse:
    def __init__(self):
        self.text = '[{"published_date": "20210101", "id":12345, "title":"no date in here"}]'


class MockSession:
    def get(self, *args, **kwargs):
        return MockResponse()


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
        self.vcr_cassettes_path = os.path.join(test_fixtures_path(), "vcr_cassettes")
        self.list_grid_records_path = os.path.join(self.vcr_cassettes_path, "list_grid_releases.yaml")

        # Contains GRID releases 2015-09-22 and 2015-10-09 (format for both is .csv and .json files)
        with patch("observatory.platform.utils.workflow_utils.AirflowVariable.get") as mock_variable_get:
            mock_variable_get.side_effect = side_effect
            self.grid_run_2015_10_18 = {
                "start_date": pendulum.datetime(2015, 10, 11),
                "end_date": pendulum.datetime(2015, 10, 18),
                "records": [
                    {"article_ids": [1570968, 1570967], "release_date": "2015-10-09"},
                    {"article_ids": [1553266, 1553267], "release_date": "2015-09-22"},
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
                "records": [{"article_ids": [12022722], "release_date": "2020-03-15T00:00:00+00:00"}],
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

    def test_ctor(self, mock_variable_get):
        """Cover case where airflow_vars is given."""
        telescope = GridTelescope(airflow_vars=[])
        self.assertEqual(telescope.airflow_vars, list(["transform_bucket"]))

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

    def test_list_grid_records_bad_title(self, mock_variable_get):
        """Check exception raised when invalid title given."""

        with patch("observatory.dags.workflows.grid_telescope.retry_session", return_value=MockSession()) as _:
            start_date = pendulum.datetime(2020, 1, 1)
            end_date = pendulum.datetime(2022, 1, 1)
            self.assertRaises(ValueError, list_grid_records, start_date, end_date, "")

    def test_list_releases(self, mock_variable_get):
        """Test list_releases."""

        ti = MagicMock()
        with patch("observatory.dags.workflows.grid_telescope.list_grid_records") as m_list_grid_records:
            m_list_grid_records.return_value = []
            telescope = GridTelescope()
            result = telescope.list_releases(execution_date=pendulum.now(), next_execution_date=pendulum.now())
            self.assertEqual(result, False)

            m_list_grid_records.return_value = [1]
            telescope = GridTelescope()
            result = telescope.list_releases(execution_date=pendulum.now(), next_execution_date=pendulum.now(), ti=ti)
            self.assertEqual(result, True)

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


class TestGridRelease(unittest.TestCase):
    @patch("observatory.dags.workflows.grid_telescope.GridRelease.extract_folder", new_callable=PropertyMock)
    @patch("observatory.dags.workflows.grid_telescope.GridRelease.download_files", new_callable=PropertyMock)
    def test_extract_not_zip_file(self, m_download_files, m_extract_folder):
        with CliRunner().isolated_filesystem():
            Path("file.zip").touch()

            release = GridRelease(dag_id="dag", article_ids=[], release_date=pendulum.now())
            m_download_files.return_value = ["file.zip"]
            m_extract_folder.return_value = "."
            release.extract()

    @patch("observatory.dags.workflows.grid_telescope.GridRelease.extract_files", new_callable=PropertyMock)
    def test_transform_multiple_extract(self, m_extract_files):
        m_extract_files.return_value = ["1", "2"]

        with CliRunner().isolated_filesystem():
            release = GridRelease(dag_id="dag", article_ids=[], release_date=pendulum.now())
            self.assertRaises(AirflowException, release.transform)


class TestGridTelescopeDag(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super().__init__(*args, **kwargs)

        self.project_id = os.environ["TEST_GCP_PROJECT_ID"]
        self.data_location = os.environ["TEST_GCP_DATA_LOCATION"]

        # Paths
        self.vcr_cassettes_path = os.path.join(test_fixtures_path(), "vcr_cassettes")
        self.list_grid_records_path = os.path.join(self.vcr_cassettes_path, "list_grid_releases.yaml")
        self.grid_path = os.path.join(self.vcr_cassettes_path, "grid_2015-09-22.yaml")

    def setup_observatory_environment(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        self.dataset_id = env.add_dataset()
        return env

    def test_dag_structure(self):
        """Test that the GRID DAG has the correct structure.

        :return: None
        """
        telescope = GridTelescope()
        dag = telescope.make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["list_releases"],
                "list_releases": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["extract"],
                "extract": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the GRID DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "grid_telescope.py")
            self.assert_dag_load("grid", dag_file)

    def test_telescope(self):
        """Test running the telescope. Functional test."""

        env = self.setup_observatory_environment()
        telescope = GridTelescope(dag_id="grid", dataset_id=self.dataset_id)
        dag = telescope.make_dag()
        execution_date = pendulum.datetime(year=2015, month=9, day=22)

        # Create the Observatory environment and run tests
        with env.create():
            with env.create_dag_run(dag, execution_date):
                # Check dependencies
                env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

                # List releases
                with patch("observatory.dags.workflows.grid_telescope.list_grid_records") as m_list_grid_records:
                    m_list_grid_records.return_value = [
                        {"article_ids": [1553266, 1553267], "release_date": "2015-10-09"},
                    ]
                    ti = env.run_task(telescope.list_releases.__name__, dag, execution_date)

                # Test list releases
                available_releases = ti.xcom_pull(
                    key=GridTelescope.RELEASE_INFO,
                    task_ids=telescope.list_releases.__name__,
                    include_prior_dates=False,
                )
                self.assertEqual(len(available_releases), 1)

                # Download
                with vcr.use_cassette(self.grid_path):
                    env.run_task(telescope.download.__name__, dag, execution_date)

                # Test download
                release = GridRelease(
                    dag_id="grid",
                    article_ids=[1553266, 1553267],
                    release_date=pendulum.datetime(2015, 10, 9),
                )
                self.assertEqual(len(release.download_files), 1)

                # upload_downloaded
                env.run_task(telescope.upload_downloaded.__name__, dag, execution_date)

                # Test upload_downloaded
                for file in release.download_files:
                    self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                # extract
                env.run_task(telescope.extract.__name__, dag, execution_date)

                # Test extract
                self.assertEqual(len(release.extract_files), 1)

                # transform
                env.run_task(telescope.transform.__name__, dag, execution_date)

                # Test transform
                self.assertEqual(len(release.transform_files), 1)

                # upload_transformed
                env.run_task(telescope.upload_transformed.__name__, dag, execution_date)

                # Test upload_transformed
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # bq_load
                env.run_task(telescope.bq_load.__name__, dag, execution_date)

                # Test bq_load
                # Will only check table exists rather than validate data.
                for file in release.transform_files:
                    table_id, _ = table_ids_from_path(file)
                    suffix = release.release_date.format("YYYYMMDD")
                    table_id = f"{self.project_id}.{self.dataset_id}.{table_id}{suffix}"
                    expected_rows = 48987
                    self.assert_table_integrity(table_id, expected_rows)

                # cleanup
                env.run_task(telescope.cleanup.__name__, dag, execution_date)

                # Test cleanup
                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__, dag, execution_date)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)
