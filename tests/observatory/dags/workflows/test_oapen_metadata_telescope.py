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

# Author: Aniek Roelofs, Tuan Chien

import logging
import os
import unittest
from unittest.mock import patch

import pendulum
import vcr
from airflow.exceptions import AirflowException
from click.testing import CliRunner
from observatory.dags.workflows.oapen_metadata_telescope import (
    OapenMetadataRelease,
    OapenMetadataTelescope,
    transform_dict,
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
        self.status_code = 404
        self.text = "test text"


class MockSession:
    def get(self, *args, **kwargs):
        return MockResponse()


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
        self.start_date = "2021-02-12"
        self.end_date = "2021-02-19"
        self.download_hash = "4c261bbfaceafde1854e102d31fcbc0e"
        self.transform_crc = "415144d7"

        # Create release instance that is used to test download/transform
        with patch("observatory.platform.utils.workflow_utils.AirflowVariable.get") as mock_variable_get:
            mock_variable_get.side_effect = side_effect

            self.release = OapenMetadataRelease(
                self.oapen_metadata.dag_id,
                pendulum.parse(self.start_date),
                pendulum.parse(self.end_date),
                first_release=True,
            )

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def test_ctor(self, mock_variable_get):
        """Cover case when airflow_vars is given."""

        telescope = OapenMetadataTelescope(airflow_vars=list())
        self.assertEqual(telescope.airflow_vars, ["transform_bucket"])

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

    def test_download_bad_response(self, mock_variable_get):
        """Validate handling when status code is not 200."""

        with patch("observatory.dags.workflows.oapen_metadata_telescope.retry_session") as m:
            m.return_value = MockSession()
            release = OapenMetadataRelease(
                dag_id="dag", start_date=pendulum.now(), end_date=pendulum.now(), first_release=False
            )
            self.assertRaises(AirflowException, release.download)

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

    def test_transform_dict_invalid(self, mock_variable_get):
        """Check transform_dict handling of invalid case."""
        result = transform_dict(None, None, None, None)
        self.assertEqual(result, None)


class TestOapenMetadataTelescopeDag(ObservatoryTestCase):
    """Tests for the Oapen Metadata telescope DAG"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # Paths
        self.vcr_cassettes_path = os.path.join(test_fixtures_path(), "vcr_cassettes")
        self.download_path = os.path.join(self.vcr_cassettes_path, "oapen_metadata_2021-02-19.yaml")

        # Telescope instance
        self.oapen_metadata = OapenMetadataTelescope()

        # Dag run info
        self.start_date = pendulum.parse("2021-02-12")
        self.end_date = pendulum.parse("2021-02-19")

    def setup_environment(self) -> ObservatoryEnvironment:
        """Setup observatory environment"""
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        self.dataset_id = env.add_dataset()
        return env

    def test_dag_structure(self):
        """Test that the Oapen Metadata DAG has the correct structure.

        :return: None
        """
        dag = OapenMetadataTelescope().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["get_release_info"],
                "get_release_info": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["bq_delete_old"],
                "bq_delete_old": ["bq_append_new"],
                "bq_append_new": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the OapenMetadata DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "oapen_metadata_telescope.py")
            self.assert_dag_load("oapen_metadata", dag_file)

    def test_telescope(self):
        """Test telescope task execution."""

        env = self.setup_environment()
        telescope = OapenMetadataTelescope(dataset_id=self.dataset_id)
        dag = telescope.make_dag()
        execution_date = self.start_date
        start_date = pendulum.datetime(2018, 5, 14)
        end_date = pendulum.datetime(2021, 2, 13)
        release = OapenMetadataRelease(
            dag_id="oapen_metadata", start_date=start_date, end_date=end_date, first_release=True
        )

        with env.create():
            with env.create_dag_run(dag, execution_date):
                with CliRunner().isolated_filesystem():
                    # Test that all dependencies are specified: no error should be thrown
                    env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

                    env.run_task(telescope.get_release_info.__name__, dag, execution_date)

                    # Test download
                    with vcr.use_cassette(self.download_path):
                        env.run_task(telescope.download.__name__, dag, execution_date)
                    self.assertEqual(len(release.download_files), 1)

                    # Test upload_downloaded
                    env.run_task(telescope.upload_downloaded.__name__, dag, execution_date)
                    for file in release.download_files:
                        self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                    # Test download
                    env.run_task(telescope.transform.__name__, dag, execution_date)
                    self.assertEqual(len(release.transform_files), 1)

                    # Test upload_transformed
                    env.run_task(telescope.upload_transformed.__name__, dag, execution_date)

                    for file in release.transform_files:
                        self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                    # Test bq_load partition
                    ti = env.run_task(telescope.bq_load_partition.__name__, dag, execution_date)
                    self.assertEqual(ti.state, "skipped")

                    # Test bq_delete_old
                    ti = env.run_task(telescope.bq_delete_old.__name__, dag, execution_date)
                    self.assertEqual(ti.state, "success")

                    # Test bq_append_new
                    env.run_task(telescope.bq_append_new.__name__, dag, execution_date)

                    partition_table_id = f"{release.dag_id}"
                    table_id = f"{self.project_id}.{telescope.dataset_id}.metadata"
                    expected_rows = 15310
                    self.assert_table_integrity(table_id, expected_rows)

                    # Test cleanup
                    download_folder, extract_folder, transform_folder = (
                        release.download_folder,
                        release.extract_folder,
                        release.transform_folder,
                    )

                    env.run_task(telescope.cleanup.__name__, dag, execution_date)

                    self.assert_cleanup(download_folder, extract_folder, transform_folder)
