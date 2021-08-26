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

# Author: Aniek Roelofs, James Diprose

import os

import httpretty
import pendulum
import vcr

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.geonames_telescope import (
    GeonamesRelease,
    GeonamesTelescope,
    fetch_release_date,
    first_sunday_of_month,
)
from observatory.platform.utils.file_utils import _hash_file
from observatory.platform.utils.gc_utils import bigquery_sharded_table_id
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.utils.workflow_utils import (
    SubFolder,
    blob_name,
    workflow_path,
)


class TestGeonamesTelescope(ObservatoryTestCase):
    """Tests for the Geonames telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestGeonamesTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.all_countries_path = test_fixtures_folder("geonames", "allCountries.zip")
        self.fetch_release_date_path = test_fixtures_folder("geonames", "fetch_release_date.yaml")
        self.list_releases_path = test_fixtures_folder("geonames", "list_releases.yaml")

    def test_dag_structure(self):
        """Test that the Geonames DAG has the correct structure.

        :return: None
        """

        dag = GeonamesTelescope().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["fetch_release_date"],
                "fetch_release_date": ["download"],
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
        """Test that the Geonames DAG can be loaded from a DAG bag.

        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path("academic_observatory_workflows.dags"), "geonames_telescope.py")
            self.assert_dag_load("geonames", dag_file)

    def test_first_sunday_of_month(self):
        """Test first_sunday_of_month function.

        :return: None.
        """

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
        """Test fetch_release_date function.

        :return: None.
        """

        with vcr.use_cassette(self.fetch_release_date_path):
            date = fetch_release_date()
            self.assertEqual(date, pendulum.datetime(year=2020, month=7, day=16, hour=1, minute=22, second=15))

    def test_telescope(self):
        """Test the Geonames telescope end to end.

        :return: None.
        """

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = GeonamesTelescope(dataset_id=dataset_id)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            with env.create_dag_run(dag, execution_date):
                # Release settings
                release_date = pendulum.datetime(year=2021, month=3, day=5, hour=1, minute=34, second=32)
                release_id = f'{telescope.dag_id}_{release_date.strftime("%Y_%m_%d")}'
                download_folder = workflow_path(SubFolder.downloaded, telescope.dag_id, release_id)
                extract_folder = workflow_path(SubFolder.extracted, telescope.dag_id, release_id)
                transform_folder = workflow_path(SubFolder.transformed, telescope.dag_id, release_id)

                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

                # Test list releases task
                with vcr.use_cassette(self.list_releases_path):
                    ti = env.run_task(telescope.fetch_release_date.__name__, dag, execution_date)

                pulled_release_date = ti.xcom_pull(
                    key=GeonamesTelescope.RELEASE_INFO,
                    task_ids=telescope.fetch_release_date.__name__,
                    include_prior_dates=False,
                )
                self.assertIsInstance(pendulum.parse(pulled_release_date), pendulum.DateTime)
                self.assertEqual(release_date.date(), pendulum.parse(pulled_release_date).date())

                # Test download task
                with httpretty.enabled():
                    self.setup_mock_file_download(GeonamesRelease.DOWNLOAD_URL, self.all_countries_path)
                    env.run_task(telescope.download.__name__, dag, execution_date)

                download_file_path = os.path.join(download_folder, f"{telescope.dag_id}.zip")
                expected_file_hash = _hash_file(self.all_countries_path, algorithm="md5")
                self.assert_file_integrity(download_file_path, expected_file_hash, "md5")

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__, dag, execution_date)
                self.assert_blob_integrity(env.download_bucket, blob_name(download_file_path), download_file_path)

                # Test that file extracted
                env.run_task(telescope.extract.__name__, dag, execution_date)
                extracted_file_path = os.path.join(extract_folder, "allCountries.txt")
                expected_file_hash = "de1bf005df4840d16faf598999d72051"
                self.assert_file_integrity(extracted_file_path, expected_file_hash, "md5")

                # Test that file transformed
                env.run_task(telescope.transform.__name__, dag, execution_date)
                transformed_file_path = os.path.join(transform_folder, f"{telescope.dag_id}.csv.gz")
                expected_file_hash = "26c14e16"
                self.assert_file_integrity(transformed_file_path, expected_file_hash, "gzip_crc")

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__, dag, execution_date)
                self.assert_blob_integrity(
                    env.transform_bucket, blob_name(transformed_file_path), transformed_file_path
                )

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load.__name__, dag, execution_date)
                table_id = f"{self.project_id}.{dataset_id}.{bigquery_sharded_table_id(telescope.dag_id, release_date)}"
                expected_rows = 50
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                env.run_task(telescope.cleanup.__name__, dag, execution_date)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)
