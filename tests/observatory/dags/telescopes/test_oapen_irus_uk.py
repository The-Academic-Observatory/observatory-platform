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
from unittest.mock import patch

import httpretty
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from click.testing import CliRunner
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence
from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.client.model.organisation import Organisation
from observatory.api.server import orm
from observatory.dags.telescopes.oapen_irus_uk import OapenIrusUkRelease, OapenIrusUkTelescope, cloud_function_exists, \
    create_cloud_function, upload_source_code_to_bucket
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.gc_utils import upload_file_to_cloud_storage
from observatory.platform.utils.template_utils import bigquery_partitioned_table_id, blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase, module_file_path, \
    random_id, test_fixtures_path


class TestOapenIrusUk(ObservatoryTestCase):
    """ Tests for the Oapen Irus Uk telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestOapenIrusUk, self).__init__(*args, **kwargs)
        self.project_id = os.getenv('TESTS_GOOGLE_CLOUD_PROJECT_ID')
        self.data_location = os.getenv('TESTS_DATA_LOCATION')
        self.organisation_name = 'ucl_press'
        self.host = "localhost"
        self.api_port = 5000
        self.download_path = test_fixtures_path('telescopes', 'oapen_irus_uk', 'download_2021_02.jsonl.gz')
        self.transform_hash = '07d2dd2e'

    def test_dag_structure(self):
        """ Test that the Oapen Irus Uk DAG has the correct structure.
        :return: None
        """
        organisation = Organisation(name=self.organisation_name)
        dag = OapenIrusUkTelescope(organisation).make_dag()
        self.assert_dag_structure({
            'check_dependencies': ['create_cloud_function'],
            'create_cloud_function': ['call_cloud_function'],
            'call_cloud_function': ['transfer'],
            'transfer': ['download_transform'],
            'download_transform': ['upload_transformed'],
            'upload_transformed': ['bq_load'],
            'bq_load': ['cleanup'],
            'cleanup': []
        }, dag)

    def test_dag_load(self):
        """ Test that the Oapen Irus Uk DAG can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            # Add Observatory API connection
            conn = Connection(conn_id=AirflowConns.OBSERVATORY_API,
                              uri=f'http://:password@{self.host}:{self.api_port}')
            env.add_connection(conn)

            # Add a telescope
            dt = pendulum.utcnow()
            telescope_type = orm.TelescopeType(name='OAPEN Irus UK Telescope',
                                               type_id=TelescopeTypes.oapen_irus_uk,
                                               created=dt,
                                               modified=dt)
            env.api_session.add(telescope_type)
            organisation = orm.Organisation(name='UCL Press',
                                            created=dt,
                                            modified=dt)
            env.api_session.add(organisation)
            telescope = orm.Telescope(name='UCL Press OAPEN Irus UK Telescope',
                                      telescope_type=telescope_type,
                                      organisation=organisation,
                                      modified=dt,
                                      created=dt)
            env.api_session.add(telescope)
            env.api_session.commit()

            dag_file = os.path.join(module_file_path('observatory.dags.dags'), 'oapen_irus_uk.py')
            self.assert_dag_load('oapen_irus_uk_ucl_press', dag_file)

    @patch('observatory.dags.telescopes.oapen_irus_uk.build')
    @patch('observatory.dags.telescopes.oapen_irus_uk.ServiceAccountCredentials')
    @patch('observatory.dags.telescopes.oapen_irus_uk.AuthorizedSession.post')
    def test_telescope(self, mock_authorized_session, mock_account_credentials, mock_build):
        """ Test the Oapen Irus Uk telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2021, month=2, day=1)
        organisation = Organisation(name=self.organisation_name,
                                    gcp_project_id=self.project_id,
                                    gcp_download_bucket=env.download_bucket,
                                    gcp_transform_bucket=env.transform_bucket)
        telescope = OapenIrusUkTelescope(organisation=organisation, dataset_id=dataset_id)
        # Fake oapen project and bucket
        telescope.oapen_project_id = env.project_id
        telescope.oapen_bucket = random_id()

        # Mock the Google Cloud Functions API service
        mock_account_credentials.from_json_keyfile_dict.return_value = ''
        http = HttpMockSequence([({'status': '200'}, json.dumps({'functions': [{
            'name': f'projects/{telescope.oapen_project_id}/locations/{telescope.function_region}/functions/{telescope.function_name}'}]})),
                                 ({'status': '200'}, json.dumps({
                                     'name': 'operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc',
                                     'done': True, 'response': {'message': 'response'}}))
                                 ])
        mock_build.return_value = build('cloudfunctions', 'v1', http=http)

        dag = telescope.make_dag()

        # Use release to check results from tasks
        release = OapenIrusUkRelease(telescope.dag_id, execution_date, organisation)

        # Create the Observatory environment and run tests
        with env.create():
            # Add airflow connections
            conn = Connection(conn_id=AirflowConns.GEOIP_LICENSE_KEY,
                              uri='mysql://email_address:password@')
            env.add_connection(conn)
            conn = Connection(conn_id=AirflowConns.OAPEN_IRUS_UK_API,
                              uri='mysql://requestor_id:api_key@')
            env.add_connection(conn)
            conn = Connection(conn_id=AirflowConns.OAPEN_IRUS_UK_LOGIN,
                              uri='mysql://user_id:license_key@')
            env.add_connection(conn)

            # Test that all dependencies are specified: no error should be thrown
            env.run_task(dag, telescope.check_dependencies.__name__, execution_date)

            # Test create cloud function task: no error should be thrown
            env.run_task(dag, telescope.create_cloud_function.__name__, execution_date)

            # Test call cloud function task: no error should be thrown
            with httpretty.enabled():
                # mock response of getting publisher uuid
                url = f'https://library.oapen.org/rest/search?query=publisher.name:{release.organisation_id}&expand=metadata'
                httpretty.register_uri(httpretty.GET, url,
                                       body='[{"uuid":"df73bf94-b818-494c-a8dd-6775b0573bc2"}]')
                # mock response of cloud function
                mock_authorized_session.return_value.status_code = 200
                mock_authorized_session.return_value.reason = 'unit test'
                url = f'https://{telescope.function_region}-{telescope.oapen_project_id}.cloudfunctions.net/{telescope.function_name}'
                httpretty.register_uri(httpretty.POST, url,
                                       body="")
                env.run_task(dag, telescope.call_cloud_function.__name__, execution_date)

            # Test transfer task
            upload_file_to_cloud_storage(telescope.oapen_bucket, release.blob_name, self.download_path)
            env.run_task(dag, telescope.transfer.__name__, execution_date)
            self.assert_blob_integrity(env.download_bucket, release.blob_name, self.download_path)

            # Test download_transform task
            env.run_task(dag, telescope.download_transform.__name__, execution_date)
            for file in release.transform_files:
                self.assert_file_integrity(file, self.transform_hash, 'gzip_crc')

            # Test that transformed file uploaded
            env.run_task(dag, telescope.upload_transformed.__name__, execution_date)
            for file in release.transform_files:
                self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

            # Test that data loaded into BigQuery
            env.run_task(dag, telescope.bq_load.__name__, execution_date)
            for file in release.transform_files:
                table_id, _ = table_ids_from_path(file)
                table_id = f'{self.project_id}.{telescope.dataset_id}.' \
                           f'{bigquery_partitioned_table_id(telescope.DAG_ID_PREFIX, release.release_date)}'
                expected_rows = 4
                self.assert_table_integrity(table_id, expected_rows)

            # Test that all telescope data deleted
            download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                release.transform_folder
            env.run_task(dag, telescope.cleanup.__name__, execution_date)
            self.assert_cleanup(download_folder, extract_folder, transform_folder)

            # Delete oapen bucket
            env._delete_bucket(telescope.oapen_bucket)

    @patch('observatory.dags.telescopes.oapen_irus_uk.upload_file_to_cloud_storage')
    @patch('observatory.dags.telescopes.oapen_irus_uk.create_cloud_storage_bucket')
    @patch('observatory.platform.utils.template_utils.AirflowVariable.get')
    def test_upload_source_code_to_bucket(self, mock_variable_get, mock_create_bucket, mock_upload_to_bucket):
        """ Test getting source code from oapen irus uk release and uploading to storage bucket.
        Test expected results both when md5 hashes match and when they don't.

        :return: None.
        """
        mock_create_bucket.return_value = True
        mock_upload_to_bucket.return_value = True, True
        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = os.getcwd()
            success, upload = upload_source_code_to_bucket('oapen_project_id', 'bucket_name',
                                                           'cloud_function_source_code.zip')
            self.assertEqual(success, True)
            self.assertEqual(upload, True)

            OapenIrusUkTelescope.CLOUD_FUNCTION_MD5_HASH = 'different'
            with self.assertRaises(AirflowException):
                upload_source_code_to_bucket('oapen_project_id', 'bucket_name', 'cloud_function_source_code.zip')

    def test_cloud_function_exists(self):
        """ Test the function that checks whether the cloud function exists
        :return: None.
        """
        http = HttpMockSequence([({'status': '200'}, json.dumps(
            {'functions': [{'name': 'projects/project-id/locations/us-central1/functions/function-1'}]})),
                                 ({'status': '200'}, json.dumps({'functions': [
                                     {'name': 'projects/project-id/locations/us-central1/functions/function-2'}]}))])
        service = build('cloudfunctions', 'v1', http=http)
        location = 'projects/project-id/locations/us-central1'
        full_name = 'projects/project-id/locations/us-central1/functions/function-2'
        # With http where cloud function does not exists
        exists = cloud_function_exists(service, location=location, full_name=full_name)
        self.assertFalse(exists)

        # With http where cloud function exists
        exists = cloud_function_exists(service, location=location, full_name=full_name)
        self.assertTrue(exists)

    def test_create_cloud_function(self):
        location = 'projects/project-id/locations/us-central1'
        full_name = 'projects/project-id/locations/us-central1/functions/function-2'
        source_bucket = 'oapen_bucket'
        blob_name = 'source_code.zip'
        max_active_runs = 1

        # Test creating cloud function, no error
        http = HttpMockSequence([({'status': '200'}, json.dumps(
            {'name': 'operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc'})),
                                 ({'status': '200'}, json.dumps({
                                     'name': 'operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc',
                                     'done': True,
                                     'response': {'message': 'response'}}))
                                 ])
        service = build('cloudfunctions', 'v1', http=http)
        success, msg = create_cloud_function(service, location, full_name, source_bucket, blob_name, max_active_runs,
                                             update=False)
        self.assertTrue(success)
        self.assertDictEqual({'message': 'response'}, msg)

        # Test updating/patching cloud function, no error
        http = HttpMockSequence([({'status': '200'}, json.dumps(
            {'name': 'operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc'})),
                                 ({'status': '200'}, json.dumps({
                                     'name': 'operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc',
                                     'done': True,
                                     'response': {'message': 'response'}}))
                                 ])
        service = build('cloudfunctions', 'v1', http=http)
        success, msg = create_cloud_function(service, location, full_name, source_bucket, blob_name, max_active_runs,
                                             update=True)
        self.assertTrue(success)
        self.assertDictEqual({'message': 'response'}, msg)

        # Test creating cloud function, error
        http = HttpMockSequence([({'status': '200'}, json.dumps(
            {'name': 'operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc'})),
                                 ({'status': '200'}, json.dumps({
                                     'name': 'operations/d29ya2Zsb3dzLWRldi91cy1jZW50cmFsMS9vYXBlbl9hY2Nlc3Nfc3RhdHMvWnlmSEdWZDBHTGc',
                                     'done': True,
                                     'error': {'message': 'error'}}))
                                 ])
        service = build('cloudfunctions', 'v1', http=http)
        success, msg = create_cloud_function(service, location, full_name, source_bucket, blob_name, max_active_runs,
                                             update=False)
        self.assertFalse(success)
        self.assertDictEqual({'message': 'error'}, msg)
