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

import os
from typing import List
from unittest.mock import patch

import httpretty
import pendulum
import vcr
from airflow.utils.state import State
from click.testing import CliRunner

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.crossref_fundref_telescope import (
    CrossrefFundrefRelease,
    CrossrefFundrefTelescope,
    list_releases,
    strip_whitespace,
)
from observatory.platform.utils.file_utils import _hash_file
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import (
    bigquery_sharded_table_id,
    blob_name,
)


class TestCrossrefFundrefTelescope(ObservatoryTestCase):
    """Tests for the CrossrefFundref telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefFundrefTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.download_path = test_fixtures_folder("crossref_fundref", "crossref_fundref_v1.34.tar.gz")
        self.download_hash = "0cd65042"
        self.extract_hash = "559aa89d41a85ff84d705084c1caeb8d"
        self.transform_hash = "632b453a"

    def test_dag_structure(self):
        """Test that the CrossrefFundref DAG has the correct structure.

        :return: None
        """
        # mock create_pool to prevent querying non existing airflow db
        with patch("academic_observatory_workflows.workflows.crossref_fundref_telescope.create_pool"):
            dag = CrossrefFundrefTelescope().make_dag()
            self.assert_dag_structure(
                {
                    "check_dependencies": ["get_release_info"],
                    "get_release_info": ["download"],
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
        """Test that the CrossrefFundref DAG can be loaded from a DAG bag.

        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "crossref_fundref_telescope.py")
            self.assert_dag_load("crossref_fundref", dag_file)

    def test_telescope(self):
        """Test the CrossrefFundref telescope end to end.

        :return: None.
        """

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests
        with env.create():
            # Setup Telescope inside env, so pool can be created
            execution_date = pendulum.datetime(year=2021, month=6, day=1)
            telescope = CrossrefFundrefTelescope(dataset_id=dataset_id)
            dag = telescope.make_dag()

            with env.create_dag_run(dag, execution_date):
                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(telescope.check_dependencies.__name__, dag, execution_date)
                self.assertEqual(ti.state, State.SUCCESS)

                # Test list releases task
                release_info = [
                    {
                        "url": "https://gitlab.com/crossref/open_funder_registry/-/archive/v1.34/open_funder_registry-v1.34.tar.gz",
                        "date": pendulum.parse("2021-05-19T09:34:09.898000+00:00"),
                    }
                ]
                with patch("academic_observatory_workflows.workflows.crossref_fundref_telescope.list_releases") as mock_list_releases:
                    mock_list_releases.return_value = release_info
                    ti = env.run_task(telescope.get_release_info.__name__, dag, execution_date)

                actual_release_info = ti.xcom_pull(
                    key=CrossrefFundrefTelescope.RELEASE_INFO,
                    task_ids=telescope.get_release_info.__name__,
                    include_prior_dates=False,
                )
                self.assertEqual(release_info, actual_release_info)

                # Create release instance to check results from other tasks
                release = CrossrefFundrefRelease(
                    telescope.dag_id, pendulum.parse(release_info[0]["date"]), release_info[0]["url"]
                )

                # Test download task
                with httpretty.enabled():
                    self.setup_mock_file_download(release.url, self.download_path)
                    env.run_task(telescope.download.__name__, dag, execution_date)
                self.assertEqual(1, len(release.download_files))
                self.assert_file_integrity(release.download_path, self.download_hash, "gzip_crc")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__, dag, execution_date)
                self.assert_blob_integrity(env.download_bucket, blob_name(release.download_path), release.download_path)

                # Test that file extracted
                env.run_task(telescope.extract.__name__, dag, execution_date)
                self.assertEqual(1, len(release.extract_files))
                self.assert_file_integrity(release.extract_path, self.extract_hash, "md5")

                # Test that file transformed
                env.run_task(telescope.transform.__name__, dag, execution_date)
                self.assertEqual(1, len(release.transform_files))
                self.assert_file_integrity(release.transform_path, self.transform_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__, dag, execution_date)
                self.assert_blob_integrity(
                    env.transform_bucket, blob_name(release.transform_path), release.transform_path
                )

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load.__name__, dag, execution_date)
                table_id = (
                    f"{self.project_id}.{dataset_id}."
                    f"{bigquery_sharded_table_id(telescope.dag_id, release.release_date)}"
                )
                expected_rows = 27949
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__, dag, execution_date)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

    def test_list_releases(self):
        """Test that list releases returns a list with dictionaries of release info.

        :return: None.
        """
        cassette_path = test_fixtures_folder("crossref_fundref", "list_fundref_releases.yaml")
        with vcr.use_cassette(cassette_path):
            releases = list_releases(pendulum.datetime(2014, 3, 1), pendulum.datetime(2020, 6, 1))
            self.assertIsInstance(releases, List)
            self.assertEqual(39, len(releases))
            for release in releases:
                self.assertIsInstance(release, dict)
                self.assertIsInstance(release["url"], str)
                self.assertIsInstance(pendulum.parse(release["date"]), pendulum.DateTime)

    def test_strip_whitespace(self):
        with CliRunner().isolated_filesystem():
            # Create file with space
            file_with_space = "file1.txt"
            with open(file_with_space, "w") as f:
                f.write(" ")
                f.write("test")

            # Create file without space and store hash
            file_without_space = "file2.txt"
            with open(file_without_space, "w") as f:
                f.write("test")
            expected_hash = _hash_file(file_without_space, algorithm="md5")

            # Strip whitespace and check that files are now the same
            strip_whitespace(file_with_space)
            self.assert_file_integrity(file_with_space, expected_hash, "md5")

            # Check that file stays the same when first line is not a space
            strip_whitespace(file_without_space)
            self.assert_file_integrity(file_without_space, expected_hash, "md5")
