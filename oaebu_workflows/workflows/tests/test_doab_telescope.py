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
from datetime import timedelta

import httpretty
import pendulum

from oaebu_workflows.config import test_fixtures_folder
from oaebu_workflows.workflows.doab_telescope import DoabRelease, DoabTelescope
from observatory.platform.utils.file_utils import _hash_file
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import blob_name, table_ids_from_path


class TestDoabTelescope(ObservatoryTestCase):
    """Tests for the DOAB telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestDoabTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.first_download_path = test_fixtures_folder("doab", "doab1.csv")
        self.first_execution_date = pendulum.datetime(year=2021, month=2, day=1)

        self.second_download_path = test_fixtures_folder("doab", "doab2.csv")
        self.second_execution_date = pendulum.datetime(year=2021, month=3, day=1)

    def test_dag_structure(self):
        """Test that the DOAB DAG has the correct structure.
        :return: None
        """

        dag = DoabTelescope().make_dag()
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
        """Test that the DOAB DAG can be loaded from a DAG bag.
        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path("oaebu_workflows.dags"), "doab_telescope.py")
            self.assert_dag_load("doab", dag_file)

    def test_telescope(self):
        """Test the DOAB telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        telescope = DoabTelescope(dataset_id=dataset_id)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # first run
            with env.create_dag_run(dag, self.first_execution_date):
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test list releases task with files available
                ti = env.run_task(telescope.get_release_info.__name__)
                start_date, end_date, first_release = ti.xcom_pull(
                    key=DoabTelescope.RELEASE_INFO,
                    task_ids=telescope.get_release_info.__name__,
                    include_prior_dates=False,
                )
                self.assertEqual(pendulum.parse(start_date), dag.default_args["start_date"])
                self.assertEqual(pendulum.parse(end_date), pendulum.today("UTC") - timedelta(days=1))
                self.assertTrue(first_release)

                # use release info for other tasks
                release = DoabRelease(
                    telescope.dag_id, pendulum.parse(start_date), pendulum.parse(end_date), first_release
                )

                # Test download task
                with httpretty.enabled():
                    self.setup_mock_file_download(DoabTelescope.CSV_URL, self.first_download_path)
                    env.run_task(telescope.download.__name__)

                self.assertEqual(1, len(release.download_files))
                download_path = release.download_files[0]
                expected_file_hash = _hash_file(self.first_download_path, algorithm="md5")
                self.assert_file_integrity(download_path, expected_file_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                self.assert_blob_integrity(env.download_bucket, blob_name(download_path), download_path)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)

                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                expected_file_hash = "ed36b4ac"
                self.assert_file_integrity(transform_path, expected_file_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                self.assert_blob_integrity(env.transform_bucket, blob_name(transform_path), transform_path)

                # Test that load partition task is skipped for the first release
                ti = env.run_task(telescope.bq_load_partition.__name__)
                self.assertEqual(ti.state, "skipped")

                # Test delete old task is in success state, without doing anything
                ti = env.run_task(telescope.bq_delete_old.__name__)
                self.assertEqual(ti.state, "success")

                # Test append new creates table
                env.run_task(telescope.bq_append_new.__name__)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                expected_rows = 4
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

            # second run
            with env.create_dag_run(dag, self.second_execution_date):
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test list releases task with files available
                ti = env.run_task(telescope.get_release_info.__name__)
                start_date, end_date, first_release = ti.xcom_pull(
                    key=DoabTelescope.RELEASE_INFO,
                    task_ids=telescope.get_release_info.__name__,
                    include_prior_dates=False,
                )
                self.assertEqual(release.end_date + timedelta(days=1), pendulum.parse(start_date))
                self.assertEqual(pendulum.today("UTC") - timedelta(days=1), pendulum.parse(end_date))
                self.assertFalse(first_release)

                # use release info for other tasks
                release = DoabRelease(
                    telescope.dag_id, pendulum.parse(start_date), pendulum.parse(end_date), first_release
                )

                # Test download task
                with httpretty.enabled():
                    self.setup_mock_file_download(DoabTelescope.CSV_URL, self.second_download_path)
                    env.run_task(telescope.download.__name__)

                self.assertEqual(1, len(release.download_files))
                download_path = release.download_files[0]
                expected_file_hash = _hash_file(self.second_download_path, algorithm="md5")
                self.assert_file_integrity(download_path, expected_file_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                self.assert_blob_integrity(env.download_bucket, blob_name(download_path), download_path)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)

                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                expected_file_hash = "b3e604ba"
                self.assert_file_integrity(transform_path, expected_file_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                self.assert_blob_integrity(env.transform_bucket, blob_name(transform_path), transform_path)

                # Test that load partition task creates partition
                env.run_task(telescope.bq_load_partition.__name__)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{partition_table_id}"
                expected_rows = 4
                self.assert_table_integrity(table_id, expected_rows)

                # Test task deleted rows from main table
                env.run_task(telescope.bq_delete_old.__name__)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                expected_rows = 3
                self.assert_table_integrity(table_id, expected_rows)

                # Test append new adds rows to table
                env.run_task(telescope.bq_append_new.__name__)
                table_id = f"{self.project_id}.{telescope.dataset_id}.{main_table_id}"
                expected_rows = 7
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)
