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

import json
import os
from unittest.mock import patch, MagicMock, ANY
from urllib.parse import quote

import httpretty
import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.connection import Connection
from click.testing import CliRunner
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence
from requests import Response

from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.client.model.organisation import Organisation
from observatory.api.server import orm
from observatory.dags.telescopes.oapen_irus_uk import (
    OapenIrusUkRelease,
    OapenIrusUkTelescope,
    call_cloud_function,
    cloud_function_exists,
    create_cloud_function,
    upload_source_code_to_bucket,
)
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.gc_utils import upload_file_to_cloud_storage
from observatory.platform.utils.template_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    random_id,
    test_fixtures_path,
)


class TestOapenIrusUk(ObservatoryTestCase):
    """ Tests for the Oapen Irus Uk telescope """

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestOapenIrusUk, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.organisation_name = "ucl_press"
        self.extra = {"publisher_id": quote("UCL Press")}
        self.host = "localhost"
        self.api_port = 5000
        self.download_path = test_fixtures_path("telescopes", "oapen_irus_uk", "download.jsonl.gz")
        self.transform_hash = "0b111b2f"

    def test_dag_structure(self):
        """Test that the Oapen Irus Uk DAG has the correct structure.
        :return: None
        """
        organisation = Organisation(name=self.organisation_name)
        dag = OapenIrusUkTelescope(organisation, self.extra.get("publisher_id")).make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["create_cloud_function"],
                "create_cloud_function": ["call_cloud_function"],
                "call_cloud_function": ["transfer"],
                "transfer": ["download_transform"],
                "download_transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Oapen Irus Uk DAG can be loaded from a DAG bag.
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
                name="OAPEN Irus UK Telescope", type_id=TelescopeTypes.oapen_irus_uk, created=dt, modified=dt
            )
            env.api_session.add(telescope_type)
            organisation = orm.Organisation(name="UCL Press", created=dt, modified=dt)
            env.api_session.add(organisation)
            telescope = orm.Telescope(
                name="UCL Press OAPEN Irus UK Telescope",
                telescope_type=telescope_type,
                organisation=organisation,
                modified=dt,
                created=dt,
                extra=self.extra,
            )
            env.api_session.add(telescope)
            env.api_session.commit()

            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "oapen_irus_uk.py")
            self.assert_dag_load("oapen_irus_uk_ucl_press", dag_file)

    @patch("observatory.dags.telescopes.oapen_irus_uk.build")
    @patch("observatory.dags.telescopes.oapen_irus_uk.ServiceAccountCredentials")
    @patch("observatory.dags.telescopes.oapen_irus_uk.AuthorizedSession.post")
    def test_telescope(self, mock_authorized_session, mock_account_credentials, mock_build):
        """Test the Oapen Irus Uk telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2021, month=2, day=14)
        organisation = Organisation(
            name=self.organisation_name,
            gcp_project_id=self.project_id,
            gcp_download_bucket=env.download_bucket,
            gcp_transform_bucket=env.transform_bucket,
        )
        telescope = OapenIrusUkTelescope(
            organisation=organisation, publisher_id=self.extra.get("publisher_id"), dataset_id=dataset_id
        )
        # Fake oapen project and bucket
        OapenIrusUkTelescope.OAPEN_PROJECT_ID = env.project_id
        OapenIrusUkTelescope.OAPEN_BUCKET = random_id()

        # Mock the Google Cloud Functions API service
        mock_account_credentials.from_json_keyfile_dict.return_value = ""
        http = HttpMockSequence(
            [
                (
                    {"status": "200"},
                    json.dumps(
                        {
                            "functions": [
                                {
                                    "name": f"projects/{OapenIrusUkTelescope.OAPEN_PROJECT_ID}/locations/"
                                            f"{OapenIrusUkTelescope.FUNCTION_REGION}/functions/"
                                            f"{OapenIrusUkTelescope.FUNCTION_NAME}"
                                }
                            ]
                        }
                    ),
                ),
                (
                    {"status": "200"},
                    json.dumps({
                        "name":
                            "operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc",
                        "done": True,
                        "response": {"message": "response"},
                        }
                    ),
                ),
            ]
        )
        mock_build.return_value = build("cloudfunctions", "v1", http=http)

        dag = telescope.make_dag()

        # Use release to check results from tasks
        release = OapenIrusUkRelease(telescope.dag_id, execution_date.end_of('month'), organisation)

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            # Add airflow connections
            conn = Connection(conn_id=AirflowConns.GEOIP_LICENSE_KEY, uri="mysql://email_address:password@")
            env.add_connection(conn)
            conn = Connection(conn_id=AirflowConns.OAPEN_IRUS_UK_API, uri="mysql://requestor_id:api_key@")
            env.add_connection(conn)
            conn = Connection(conn_id=AirflowConns.OAPEN_IRUS_UK_LOGIN, uri="mysql://user_id:license_key@")
            env.add_connection(conn)

            # Test that all dependencies are specified: no error should be thrown
            env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

            # Test create cloud function task: no error should be thrown
            env.run_task(telescope.create_cloud_function.__name__, dag, execution_date)

            # Test call cloud function task: no error should be thrown
            with httpretty.enabled():
                # mock response of getting publisher uuid
                url = f"https://library.oapen.org/rest/search?query=publisher.name:{release.organisation_id}" \
                      f"&expand=metadata"
                httpretty.register_uri(httpretty.GET, url, body='[{"uuid":"df73bf94-b818-494c-a8dd-6775b0573bc2"}]')
                # mock response of cloud function
                mock_authorized_session.return_value = MagicMock(spec=Response, status_code=200,
                                                                 json=lambda: {'entries': 100,
                                                                               'unprocessed_publishers': None},
                                                                 reason="unit test")
                url = f"https://{OapenIrusUkTelescope.FUNCTION_REGION}-{OapenIrusUkTelescope.OAPEN_PROJECT_ID}." \
                      f"cloudfunctions.net/{OapenIrusUkTelescope.FUNCTION_NAME}"
                httpretty.register_uri(httpretty.POST, url, body="")
                env.run_task(telescope.call_cloud_function.__name__, dag, execution_date)

            # Test transfer task
            upload_file_to_cloud_storage(OapenIrusUkTelescope.OAPEN_BUCKET, release.blob_name, self.download_path)
            env.run_task(telescope.transfer.__name__, dag, execution_date)
            self.assert_blob_integrity(env.download_bucket, release.blob_name, self.download_path)

            # Test download_transform task
            env.run_task(telescope.download_transform.__name__, dag, execution_date)
            self.assertEqual(1, len(release.transform_files))
            for file in release.transform_files:
                self.assert_file_integrity(file, self.transform_hash, "gzip_crc")

            # Test that transformed file uploaded
            env.run_task(telescope.upload_transformed.__name__, dag, execution_date)
            for file in release.transform_files:
                self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

            # Test that data loaded into BigQuery
            env.run_task(telescope.bq_load_partition.__name__, dag, execution_date)
            for file in release.transform_files:
                table_id, _ = table_ids_from_path(file)
                table_id = f'{self.project_id}.{dataset_id}.{table_id}${release.release_date.strftime("%Y%m")}'
                expected_rows = 2
                self.assert_table_integrity(table_id, expected_rows)

            # Test that all telescope data deleted
            download_folder, extract_folder, transform_folder = (
                release.download_folder,
                release.extract_folder,
                release.transform_folder,
            )
            env.run_task(telescope.cleanup.__name__, dag, execution_date)
            self.assert_cleanup(download_folder, extract_folder, transform_folder)

            # Delete oapen bucket
            env._delete_bucket(OapenIrusUkTelescope.OAPEN_BUCKET)

    @patch('observatory.platform.utils.template_utils.AirflowVariable.get')
    @patch('observatory.dags.telescopes.oapen_irus_uk.upload_source_code_to_bucket')
    @patch('observatory.dags.telescopes.oapen_irus_uk.cloud_function_exists')
    @patch('observatory.dags.telescopes.oapen_irus_uk.create_cloud_function')
    def test_release_create_cloud_function(self, mock_create_function, mock_function_exists, mock_upload,
                                           mock_variable_get):
        """ Test the create_cloud_function method of the OapenIrusUkRelease

        :param mock_variable_get: Mock Airflow Variable 'data'
        :return: None.
        """
        def reset_mocks():
            mock_upload.reset_mock()
            mock_function_exists.reset_mock()
            mock_create_function.reset_mock()

        def assert_mocks(create: bool, update: bool):
            mock_upload.assert_called_once_with(telescope.FUNCTION_SOURCE_URL, telescope.OAPEN_PROJECT_ID,
                                                telescope.OAPEN_BUCKET, telescope.FUNCTION_BLOB_NAME)
            mock_function_exists.assert_called_once_with(ANY, location, full_name)
            if create or update:
                mock_create_function.assert_called_once_with(ANY, location, full_name, telescope.OAPEN_BUCKET,
                                                             telescope.FUNCTION_BLOB_NAME, telescope.max_active_runs,
                                                             update)
            else:
                mock_create_function.assert_not_called()

        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.path.join(os.getcwd(), 'data')
            # Create release instance
            org = Organisation(
                name=self.organisation_name,
                gcp_project_id=self.project_id,
                gcp_download_bucket='download_bucket',
                gcp_transform_bucket='transform_bucket'
            )
            telescope = OapenIrusUkTelescope(org, publisher_id='publisher', dataset_id='dataset_id')
            release = OapenIrusUkRelease(telescope.dag_id, pendulum.parse('2020-02-01'), org)

            location = f"projects/{telescope.OAPEN_PROJECT_ID}/locations/{telescope.FUNCTION_REGION}"
            full_name = f"{location}/functions/{telescope.FUNCTION_NAME}"

            # Test when source code upload was unsuccessful
            mock_upload.return_value = False, False
            with self.assertRaises(AirflowException):
                release.create_cloud_function(telescope.max_active_runs)

            # Test when cloud function does not exist
            reset_mocks()
            mock_upload.return_value = True, True
            mock_function_exists.return_value = False
            mock_create_function.return_value = True, "response"
            release.create_cloud_function(telescope.max_active_runs)
            assert_mocks(create=True, update=False)

            # Test when cloud function exists, but source code has changed
            reset_mocks()
            mock_upload.return_value = True, True
            mock_function_exists.return_value = True
            mock_create_function.return_value = True, "response"
            release.create_cloud_function(telescope.max_active_runs)
            assert_mocks(create=False, update=True)

            # Test when cloud function exists and source code has not changed
            reset_mocks()
            mock_upload.return_value = True, False
            mock_function_exists.return_value = True
            release.create_cloud_function(telescope.max_active_runs)
            assert_mocks(create=False, update=False)

            # Test when create cloud function was unsuccessful
            reset_mocks()
            mock_upload.return_value = True, True
            mock_function_exists.return_value = True
            mock_create_function.return_value = False, "response"
            with self.assertRaises(AirflowException):
                release.create_cloud_function(telescope.max_active_runs)

    @patch('observatory.platform.utils.template_utils.AirflowVariable.get')
    @patch('observatory.dags.telescopes.oapen_irus_uk.BaseHook.get_connection')
    @patch('observatory.dags.telescopes.oapen_irus_uk.get_publisher_uuid')
    @patch('observatory.dags.telescopes.oapen_irus_uk.call_cloud_function')
    def test_release_call_cloud_function(self, mock_call_function, mock_get_publisher, mock_conn_get,
                                         mock_variable_get):
        """ Test the call_cloud_function method of the OapenIrusUkRelease

        :param mock_variable_get: Mock Airflow Variable 'data'
        :return: None.
        """
        connections = {AirflowConns.GEOIP_LICENSE_KEY: Connection(AirflowConns.GEOIP_LICENSE_KEY,
                                                                  uri='http://user_id:key@'),
                       AirflowConns.OAPEN_IRUS_UK_API: Connection(AirflowConns.OAPEN_IRUS_UK_API,
                                                                  uri='mysql://requestor_id:api_key@'),
                       AirflowConns.OAPEN_IRUS_UK_LOGIN: Connection(AirflowConns.OAPEN_IRUS_UK_LOGIN,
                                                                    uri='mysql://email:password@')}
        mock_conn_get.side_effect = lambda x: connections[x]
        mock_get_publisher.return_value = 'publisher_uuid'

        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.path.join(os.getcwd(), 'data')
            org = Organisation(
                name=self.organisation_name,
                gcp_project_id=self.project_id,
                gcp_download_bucket='download_bucket',
                gcp_transform_bucket='transform_bucket'
            )

            # Test new platform and old platform
            for date in ['2020-03', '2020-04']:
                # Test for a given publisher name and the 'oapen' publisher
                for publisher_id in ['publisher', 'oapen']:
                    mock_call_function.reset_mock()

                    telescope = OapenIrusUkTelescope(org, publisher_id=publisher_id, dataset_id='dataset_id')
                    release = OapenIrusUkRelease(telescope.dag_id, pendulum.parse(date + '-01'), org)
                    function_url = f"https://{telescope.FUNCTION_REGION}-{telescope.OAPEN_PROJECT_ID}" \
                                   f".cloudfunctions.net/{telescope.FUNCTION_NAME}"

                    release.call_cloud_function(telescope.publisher_id)

                    # Test that the call function is called with the correct args
                    if date == '2020-04':
                        username = 'requestor_id'
                        password = 'api_key'
                        if publisher_id == "oapen":
                            publisher_uuid = ""
                        else:
                            publisher_uuid = 'publisher_uuid'
                    else:
                        username = 'email'
                        password = 'password'
                        publisher_uuid = "NA"
                        if publisher_id == "oapen":
                            publisher_id = ""
                    mock_call_function.assert_called_once_with(function_url, date, username, password,
                                                               'key', publisher_id, publisher_uuid,
                                                               telescope.OAPEN_BUCKET, release.blob_name)

    @patch("observatory.dags.telescopes.oapen_irus_uk.upload_file_to_cloud_storage")
    @patch("observatory.dags.telescopes.oapen_irus_uk.create_cloud_storage_bucket")
    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_upload_source_code_to_bucket(self, mock_variable_get, mock_create_bucket, mock_upload_to_bucket):
        """Test getting source code from oapen irus uk release and uploading to storage bucket.
        Test expected results both when md5 hashes match and when they don't.

        :return: None.
        """
        mock_create_bucket.return_value = True
        mock_upload_to_bucket.return_value = True, True
        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.getcwd()
            success, upload = upload_source_code_to_bucket(
                OapenIrusUkTelescope.FUNCTION_SOURCE_URL,
                OapenIrusUkTelescope.OAPEN_PROJECT_ID,
                OapenIrusUkTelescope.OAPEN_BUCKET,
                OapenIrusUkTelescope.FUNCTION_BLOB_NAME,
            )
            self.assertEqual(success, True)
            self.assertEqual(upload, True)

            OapenIrusUkTelescope.FUNCTION_MD5_HASH = "different"
            with self.assertRaises(AirflowException):
                upload_source_code_to_bucket(
                    OapenIrusUkTelescope.FUNCTION_SOURCE_URL,
                    OapenIrusUkTelescope.OAPEN_PROJECT_ID,
                    OapenIrusUkTelescope.OAPEN_BUCKET,
                    OapenIrusUkTelescope.FUNCTION_BLOB_NAME,
                )

    def test_cloud_function_exists(self):
        """Test the function that checks whether the cloud function exists
        :return: None.
        """
        http = HttpMockSequence(
            [
                (
                    {"status": "200"},
                    json.dumps(
                        {"functions": [{"name": "projects/project-id/locations/us-central1/functions/function-1"}]}
                    ),
                ),
                (
                    {"status": "200"},
                    json.dumps(
                        {"functions": [{"name": "projects/project-id/locations/us-central1/functions/function-2"}]}
                    ),
                ),
            ]
        )
        service = build("cloudfunctions", "v1", http=http)
        location = "projects/project-id/locations/us-central1"
        full_name = "projects/project-id/locations/us-central1/functions/function-2"
        # With http where cloud function does not exists
        exists = cloud_function_exists(service, location=location, full_name=full_name)
        self.assertFalse(exists)

        # With http where cloud function exists
        exists = cloud_function_exists(service, location=location, full_name=full_name)
        self.assertTrue(exists)

    def test_create_cloud_function(self):
        """ Test the function that creates the cloud function
        :return: None.
        """
        location = "projects/project-id/locations/us-central1"
        full_name = "projects/project-id/locations/us-central1/functions/function-2"
        source_bucket = "oapen_bucket"
        blob_name = "source_code.zip"
        max_active_runs = 1

        # Test creating cloud function, no error
        http = HttpMockSequence(
            [
                (
                    {"status": "200"},
                    json.dumps(
                        {
                            "name":
                                "operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc"
                        }
                    ),
                ),
                (
                    {"status": "200"},
                    json.dumps({
                        "name":
                            "operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc",
                        "done": True,
                        "response": {"message": "response"},
                        }
                    ),
                ),
            ]
        )
        service = build("cloudfunctions", "v1", http=http)
        success, msg = create_cloud_function(
            service, location, full_name, source_bucket, blob_name, max_active_runs, update=False
        )
        self.assertTrue(success)
        self.assertDictEqual({"message": "response"}, msg)

        # Test updating/patching cloud function, no error
        http = HttpMockSequence(
            [
                (
                    {"status": "200"},
                    json.dumps({
                        "name": "operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc"
                        }
                    ),
                ),
                (
                    {"status": "200"},
                    json.dumps({
                        "name":
                            "operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc",
                        "done": True,
                        "response": {"message": "response"},
                        }
                    ),
                ),
            ]
        )
        service = build("cloudfunctions", "v1", http=http)
        success, msg = create_cloud_function(
            service, location, full_name, source_bucket, blob_name, max_active_runs, update=True
        )
        self.assertTrue(success)
        self.assertDictEqual({"message": "response"}, msg)

        # Test creating cloud function, error
        http = HttpMockSequence(
            [
                (
                    {"status": "200"},
                    json.dumps({
                        "name": "operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc"
                        }
                    ),
                ),
                (
                    {"status": "200"},
                    json.dumps({
                        "name":
                            "operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc",
                        "done": True,
                        "error": {"message": "error"},
                        }
                    ),
                ),
            ]
        )
        service = build("cloudfunctions", "v1", http=http)
        success, msg = create_cloud_function(
            service, location, full_name, source_bucket, blob_name, max_active_runs, update=False
        )
        self.assertFalse(success)
        self.assertDictEqual({"message": "error"}, msg)

    @patch("observatory.dags.telescopes.oapen_irus_uk.AuthorizedSession.post")
    def test_call_cloud_function(self, mock_authorized_session):
        """ Test the function that calls the cloud function
        :return: None.
        """

        function_url = "function_url"
        release_date = "2020-01-01"
        username = "username"
        password = "password"
        geoip_license_key = "key"
        publisher_name = "publisher_name"
        publisher_uuid = "publisher_uuid"
        bucket_name = "bucket"
        blob_name = "blob"

        # Set responses for consequential calls
        mock_authorized_session.side_effect = [MagicMock(spec=Response, status_code=200, reason="unit test",
                                                         json=lambda: {'entries': 100,
                                                                       'unprocessed_publishers': ['publisher1',
                                                                                                  'publisher2']}),
                                               MagicMock(spec=Response, status_code=200, reason="unit test",
                                                         json=lambda: {'entries': 200, 'unprocessed_publishers': None}),
                                               MagicMock(spec=Response, status_code=200, reason="unit test",
                                                         json=lambda: {'entries': 0, 'unprocessed_publishers': None}),
                                               MagicMock(spec=Response, status_code=400, reason="unit test")
                                               ]
        # Test when there are unprocessed publishers (first 2 responses from side effect)
        call_cloud_function(function_url, release_date, username, password, geoip_license_key, publisher_name,
                            publisher_uuid, bucket_name, blob_name)
        self.assertEqual(2, mock_authorized_session.call_count)

        # Test when entries is 0 (3rd response from side effect)
        with self.assertRaises(AirflowSkipException):
            call_cloud_function(function_url, release_date, username, password, geoip_license_key, publisher_name,
                                publisher_uuid, bucket_name, blob_name)

        # Test when response status code is not 200 (last response from side effect)
        with self.assertRaises(AirflowException):
            call_cloud_function(function_url, release_date, username, password, geoip_license_key, publisher_name,
                                publisher_uuid, bucket_name, blob_name)
