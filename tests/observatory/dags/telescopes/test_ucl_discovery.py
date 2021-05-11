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



import gzip
import json
import os
from datetime import datetime
from unittest.mock import patch

import pendulum
from airflow.models.connection import Connection
from croniter import croniter
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence
from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.client.model.organisation import Organisation
from observatory.api.server import orm
from observatory.dags.telescopes.google_analytics import GoogleAnalyticsRelease, GoogleAnalyticsTelescope
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.template_utils import bigquery_partitioned_table_id, blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase, module_file_path


class TestUclDiscovery(ObservatoryTestCase):
    """ Tests for the Ucl Discovery telescope """

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestUclDiscovery, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.organisation_name = "ucl_press"
        self.host = "localhost"
        self.api_port = 5000

        self.metadata_cassette = os.path.join(test_fixtures_path("vcr_cassettes", "ucl_discovery"),
                                              'metadata.yaml')
        self.country_cassette = os.path.join(test_fixtures_path("vcr_cassettes", "ucl_discovery"),
                                              'country.yaml')
        self.download_hash = '8ae68aa5a455a1835fd906665746ee8c'
        self.transform_hash = '89704b88'

    def test_dag_structure(self):
        """Test that the UCL Discovery DAG has the correct structure.
        :return: None
        """
        organisation = Organisation(name=self.organisation_name)
        dag = UclDiscoveryTelescope(organisation).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download"],
                "download": ["upload_downloaded"],
                "upload_downloaded": ["transform"],
                "transform": ["upload_transformed"],
                "upload_transformed": ["bq_load"],
                "bq_load": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the UCL Discovery DAG can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            # Add Observatory API connection
            conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.api_port}")
            env.add_connection(conn)

            # Add a telescope
            dt = pendulum.utcnow()
            telescope_type = orm.TelescopeType(
                name="UCL Discovery Telescope", type_id=TelescopeTypes.ucl_discovery, created=dt, modified=dt
            )
            env.api_session.add(telescope_type)
            organisation = orm.Organisation(name="UCL Press", created=dt, modified=dt)
            env.api_session.add(organisation)
            telescope = orm.Telescope(
                name="UCL Press UCL Discovery Telescope",
                telescope_type=telescope_type,
                organisation=organisation,
                modified=dt,
                created=dt
            )
            env.api_session.add(telescope)
            env.api_session.commit()

            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "ucl_discovery.py")
            self.assert_dag_load("ucl_discovery_ucl_press", dag_file)

    @patch('observatory.dags.telescopes.ucl_discovery.get_downloads_per_country')
    def test_telescope(self, mock_downloads_per_country):
        """Test the UCL Discovery telescope end to end.
        :return: None.
        """

        mock_downloads_per_country.return_value = [{'country_code': 'MX', 'country_name': 'Mexico',
                                                    'download_count': 10},
                                                   {'country_code': 'US', 'country_name': 'United States',
                                                    'download_count': 8},
                                                   {'country_code': 'GB', 'country_name': 'United Kingdom',
                                                    'download_count': 6},
                                                   {'country_code': 'BR', 'country_name': 'Brazil',
                                                    'download_count': 1}], 25

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2021, month=4, day=1)
        organisation = Organisation(
            name=self.organisation_name,
            gcp_project_id=self.project_id,
            gcp_download_bucket=env.download_bucket,
            gcp_transform_bucket=env.transform_bucket,
        )
        telescope = UclDiscoveryTelescope(organisation=organisation, dataset_id=dataset_id)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # Add OAEBU service account connection connection
            conn = Connection(
                conn_id=AirflowConns.OAEBU_SERVICE_ACCOUNT,
                uri=f"google-cloud-platform://?type=service_account&private_key_id=private_key_id"
                f"&private_key=private_key"
                f"&client_email=client_email"
                f"&client_id=client_id",
            )
            env.add_connection(conn)

            # Test that all dependencies are specified: no error should be thrown
            env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

            # Use release to check tasks
            cron_schedule = dag.normalized_schedule_interval
            cron_iter = croniter(cron_schedule, execution_date)
            end_date = pendulum.instance(cron_iter.get_next(datetime))
            release = UclDiscoveryRelease(telescope.dag_id, execution_date, end_date, organisation)

            # Test download
            with vcr.use_cassette(self.metadata_cassette):
                env.run_task(telescope.download.__name__, dag, execution_date)
            self.assertEqual(1, len(release.download_files))
            for file in release.download_files:
                self.assert_file_integrity(file, self.download_hash, "md5")

            # Test upload downloaded
            env.run_task(telescope.upload_downloaded.__name__, dag, execution_date)
            for file in release.download_files:
                self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

            # Test that file transformed
            # with vcr.use_cassette(self.country_cassette):
            env.run_task(telescope.transform.__name__, dag, execution_date)
            self.assertEqual(1, len(release.transform_files))
            for file in release.transform_files:
                self.assert_file_integrity(file, self.transform_hash, "gzip_crc")

            # Test that transformed file uploaded
            env.run_task(telescope.upload_transformed.__name__, dag, execution_date)
            for file in release.transform_files:
                self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

            # Test that data loaded into BigQuery
            env.run_task(telescope.bq_load.__name__, dag, execution_date)
            for file in release.transform_files:
                table_id, _ = table_ids_from_path(file)
                table_id = f"{self.project_id}.{telescope.dataset_id}." \
                           f"{bigquery_partitioned_table_id(telescope.DAG_ID_PREFIX, release.end_date)}"

                expected_rows = 519
                self.assert_table_integrity(table_id, expected_rows)

            # Test that all telescope data deleted
            download_folder, extract_folder, transform_folder = (
                release.download_folder,
                release.extract_folder,
                release.transform_folder,
            )
            env.run_task(telescope.cleanup.__name__, dag, execution_date)
            self.assert_cleanup(download_folder, extract_folder, transform_folder)


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

#
# @patch('observatory.platform.utils.template_utils.AirflowVariable.get')
# class TestUclDiscovery(unittest.TestCase):
#     """ Tests for the functions used by the UclDiscovery telescope """
#
#     def __init__(self, *args, **kwargs, ):
#         """ Constructor which sets up variables used by tests.
#
#         :param args: arguments.
#         :param kwargs: keyword arguments.
#         """
#
#         super(TestUclDiscovery, self).__init__(*args, **kwargs)
#
#         # Paths
#         self.vcr_cassettes_path = os.path.join(test_fixtures_path(), 'vcr_cassettes')
#         self.download_path = os.path.join(self.vcr_cassettes_path, 'ucl_discovery_2008-02-01.yaml')
#         self.country_report_path = os.path.join(self.vcr_cassettes_path,
#                                                 'ucl_discovery_country_2008-02-01.yaml')
#
#         # Telescope instance
#         self.ucl_discovery = UclDiscoveryTelescope()
#
#         # Dag run info
#         self.start_date = pendulum.parse('2021-01-01')
#         self.end_date = pendulum.parse('2021-02-01')
#         self.download_hash = 'ed054db8c4221b7e8055507c4718b7f2'
#         self.transform_crc = '12444a7d'
#
#         # Create release instance that is used to test download/transform
#         with patch('observatory.platform.utils.template_utils.AirflowVariable.get') as mock_variable_get:
#             mock_variable_get.side_effect = mock_airflow_variable
#             self.release = UclDiscoveryRelease(self.ucl_discovery.dag_id, self.start_date, self.end_date)
#
#         # Turn logging to warning because vcr prints too much at info level
#         logging.basicConfig()
#         logging.getLogger().setLevel(logging.WARNING)
#
#     def test_make_release(self, mock_variable_get):
#         """ Check that make_release returns a list with one UclDiscoveryRelease instance.
#
#         :param mock_variable_get: Mock result of airflow's Variable.get() function
#         :return: None.
#         """
#         mock_variable_get.side_effect = mock_airflow_variable
#
#         schedule_interval = '0 0 1 * *'
#         execution_date = self.start_date
#         releases = self.ucl_discovery.make_release(dag=SimpleNamespace(normalized_schedule_interval=schedule_interval),
#                                                    dag_run=SimpleNamespace(execution_date=execution_date))
#         self.assertEqual(1, len(releases))
#         self.assertIsInstance(releases, list)
#         self.assertIsInstance(releases[0], UclDiscoveryRelease)
#
#     def test_download_release(self, mock_variable_get):
#         """ Download release to check it has the expected md5 sum and test unsuccessful mocked responses.
#
#         :param mock_variable_get: Mock result of airflow's Variable.get() function
#         :return:
#         """
#         mock_variable_get.side_effect = mock_airflow_variable
#
#         with CliRunner().isolated_filesystem():
#             with vcr.use_cassette(self.download_path):
#                 success = self.release.download()
#
#                 # Check that download is successful
#                 self.assertTrue(success)
#
#                 # Check that file has expected hash
#                 self.assertEqual(1, len(self.release.download_files))
#                 self.assertEqual(self.release.download_path, self.release.download_files[0])
#                 self.assertTrue(os.path.exists(self.release.download_path))
#                 self.assertEqual(self.download_hash, _hash_file(self.release.download_path, algorithm='md5'))
#
#             with patch('observatory.platform.utils.url_utils.requests.Session.get') as mock_requests_get:
#                 # mock response status code is not 200
#                 mock_requests_get.side_effect = mocked_requests_get
#                 self.release.eprint_metadata_url = 'test_status_code'
#                 success = self.release.download()
#                 self.assertFalse(success)
#
#                 # mock response content is empty CSV file (only headers)
#                 self.release.eprint_metadata_url = 'test_empty_csv'
#                 success = self.release.download()
#                 self.assertFalse(success)
#
#     def test_transform_release(self, mock_variable_get):
#         """ Test that the release is transformed as expected.
#
#         :param mock_variable_get: Mock result of airflow's Variable.get() function
#         :return: None.
#         """
#         mock_variable_get.side_effect = mock_airflow_variable
#
#         with CliRunner().isolated_filesystem():
#             with vcr.use_cassette(self.download_path):
#                 self.release.download()
#
#             # use three eprintids for transform test, for first one the country downloads is empty
#             with open(self.release.download_path, 'r') as f_in:
#                 lines = f_in.readlines()
#             with open(self.release.download_path, 'w') as f_out:
#                 f_out.writelines(lines[0:1] + lines[23:36] + lines[36:61] + lines[61:88])
#
#             with vcr.use_cassette(self.country_report_path):
#                 self.release.transform()
#
#             # Check that file has expected crc
#             self.assertEqual(1, len(self.release.transform_files))
#             self.assertEqual(self.release.transform_path, self.release.transform_files[0])
#             self.assertTrue(os.path.exists(self.release.transform_path))
#             self.assertEqual(self.transform_crc, gzip_file_crc(self.release.transform_path))
