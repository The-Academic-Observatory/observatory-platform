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
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpretty
import pendulum
from airflow.models.connection import Connection
from click.testing import CliRunner
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence

from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.client.model.organisation import Organisation
from observatory.api.server import orm
from observatory.dags.telescopes.oapen_cloud_function.main import download, download_access_stats_new, \
    download_access_stats_old, download_geoip, list_to_jsonl_gz, replace_ip_address
from observatory.dags.telescopes.oapen_irus_uk import OapenIrusUkRelease, OapenIrusUkTelescope, cloud_function_exists, \
    create_cloud_function
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.file_utils import gzip_file_crc
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
                # register uri to get publisher uuid
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


class MockGeoipClient:
    def __init__(self, latitude, longitude, city, country, country_iso_code):
        self.latitude = latitude
        self.longitude = longitude
        self.city_name = city
        self.country = country
        self.country_iso_code = country_iso_code

    def city(self, client_ip: str):
        return SimpleNamespace(location=SimpleNamespace(latitude=self.latitude, longitude=self.longitude),
                               city=SimpleNamespace(name=self.city_name),
                               country=SimpleNamespace(name=self.country,
                                                       iso_code=self.country_iso_code))


class TestCloudFunction(unittest.TestCase):
    def __init__(self, *args, **kwargs, ):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCloudFunction, self).__init__(*args, **kwargs)

        self.download_path_old = test_fixtures_path('telescopes', 'oapen_irus_uk', 'download_2020_03.tsv')
        self.download_hash_old = '361294f7'
        self.download_path_new = test_fixtures_path('telescopes', 'oapen_irus_uk', 'download_2020_04.json')
        self.download_hash_new = '1a293f45'

    @patch('observatory.dags.telescopes.oapen_cloud_function.main.download_geoip')
    @patch('observatory.dags.telescopes.oapen_cloud_function.main.geoip2.database.Reader')
    @patch('observatory.dags.telescopes.oapen_cloud_function.main.download_access_stats_new')
    @patch('observatory.dags.telescopes.oapen_cloud_function.main.download_access_stats_old')
    @patch('observatory.dags.telescopes.oapen_cloud_function.main.upload_file_to_storage_bucket')
    def test_download(self, mock_upload_blob, mock_download_old, mock_download_new, mock_geoip_reader,
                      mock_download_geoip):
        """ Test downloading OAPEN Irus UK access stats """
        # download older version
        mock_upload_blob.return_value = True
        mock_download_old.return_value = None
        mock_download_new.return_value = None
        mock_geoip_reader.return_value = 'geoip_client'
        mock_download_geoip.return_value = None

        data = {'release_date': '2020-03',
                'username': 'username',
                'password': 'password',
                'geoip_license_key': 'geoip_license_key',
                'publisher_name': 'publisher_name',
                'publisher_uuid': 'publisher_uuid',
                'bucket_name': 'bucket_name',
                'blob_name': 'blob_name'}
        request = Mock(get_json=Mock(return_value=data), args=data)
        download(request)
        # assert mocked functions are called correctly
        mock_upload_blob.assert_called_once_with('/tmp/oapen_access_stats.jsonl.gz', data['bucket_name'],
                                                 data['blob_name'])
        mock_download_old.assert_called_once_with('/tmp/oapen_access_stats.jsonl.gz', data['release_date'],
                                                  data['username'], data['password'], data['publisher_name'],
                                                  'geoip_client')
        mock_download_new.assert_not_called()
        mock_geoip_reader.assert_called_once_with('/tmp/geolite_city.mmdb')
        mock_download_geoip.assert_called_once_with(data['geoip_license_key'], '/tmp/geolite_city.tar.gz',
                                                    '/tmp/geolite_city.mmdb')

        # download newer version
        mock_upload_blob.reset_mock()
        mock_download_old.reset_mock()
        mock_download_new.reset_mock()
        mock_geoip_reader.reset_mock()
        mock_download_geoip.reset_mock()

        data['release_date'] = '2020-04'
        download(request)
        # assert mocked functions are called correctly
        mock_upload_blob.assert_called_once_with('/tmp/oapen_access_stats.jsonl.gz', data['bucket_name'],
                                                 data['blob_name'])
        mock_download_old.assert_not_called()
        mock_download_new.assert_called_once_with('/tmp/oapen_access_stats.jsonl.gz', data['release_date'],
                                                  data['username'], data['password'], data['publisher_uuid'],
                                                  'geoip_client')
        mock_geoip_reader.assert_called_once_with('/tmp/geolite_city.mmdb')
        mock_download_geoip.assert_called_once_with(data['geoip_license_key'], '/tmp/geolite_city.tar.gz',
                                                    '/tmp/geolite_city.mmdb')

    def test_download_geoip(self):
        """ Test downloading geolite database """
        geoip_license_key = 'license_key'
        url = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=' \
              f'{geoip_license_key}&suffix=tar.gz'
        download_path = 'geolite_city.tar.gz'
        extract_path = 'geolite_city.mmdb'
        with CliRunner().isolated_filesystem():
            with httpretty.enabled():
                httpretty.register_uri(httpretty.GET, uri=url, body='success', content_type="application/gzip")
                download_geoip(geoip_license_key, download_path, extract_path)
            self.assertTrue(os.path.isfile(download_path))
            self.assertTrue(os.path.isfile(extract_path))

    @patch('observatory.dags.telescopes.oapen_cloud_function.main.replace_ip_address')
    def test_download_access_stats_old(self, mock_replace_ip):
        """ Test downloading access stats before April 2020 """
        mock_replace_ip.return_value = ('23.1194', '-82.392', 'Suva', 'Peru', 'PE')
        with CliRunner().isolated_filesystem():
            file_path = 'oapen_access_stats.jsonl.gz'
            publisher_name = 'publisher%20name'
            release_date = '2020-03'
            start_date = release_date + "-01"
            end_date = release_date + "-31"
            with httpretty.enabled():
                httpretty.register_uri(httpretty.POST,
                                       uri='https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/?action=login',
                                       body='After you have finished your session please remember to')

                with open(self.download_path_old, 'rb') as f:
                    body = f.read()
                httpretty.register_uri(httpretty.GET, uri=f'https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/br1b/'
                                                          f'?frmRepository=1%7COAPEN+Library&frmPublisher='
                                                          f'{publisher_name}&frmFrom={start_date}&frmTo={end_date}'
                                                          f'&frmFormat=TSV&Go=Generate+Report',
                                       body=body)
                download_access_stats_old(file_path, release_date, 'username', 'password', publisher_name,
                                          'geoip_client')
                actual_hash = gzip_file_crc(file_path)
                self.assertEqual(self.download_hash_old, actual_hash)

    @patch('observatory.dags.telescopes.oapen_cloud_function.main.replace_ip_address')
    def test_download_access_stats_new(self, mock_replace_ip):
        """ Test downloading access stats since April 2020 """

        mock_replace_ip.return_value = ('23.1194', '-82.392', 'Suva', 'Peru', 'PE')
        with CliRunner().isolated_filesystem():
            file_path = 'oapen_access_stats.jsonl.gz'
            publisher_uuid = 'publisher_uuid'
            release_date = '2020-04'
            requestor_id = 'requestor_id'
            api_key = 'api_key'

            with httpretty.enabled():
                with open(self.download_path_new, 'rb') as f:
                    body = f.read()
                httpretty.register_uri(httpretty.GET,
                                       uri=f'https://irus.jisc.ac.uk/sushiservice/oapen/reports/oapen_ir/?requestor_id={requestor_id}' \
                                           f'&platform=215&begin_date={release_date}&end_date={release_date}&formatted&api_key={api_key}' \
                                           f'&attributes_to_show=Client_IP%7CCountry&publisher={publisher_uuid}',
                                       body=body)
                download_access_stats_new(file_path, release_date, requestor_id, api_key, publisher_uuid,
                                          'geoip_client')
                actual_hash = gzip_file_crc(file_path)
                self.assertEqual(self.download_hash_new, actual_hash)

    def test_replace_ip_address(self):
        """ Test replacing ip adresss with geographical information mocking the geolite database """
        latitude = '23.1194',
        longitude = '-82.392',
        city = 'Suva',
        country = 'Peru',
        country_iso_code = 'PE'

        geoip_client = MockGeoipClient(latitude, longitude, city, country, country_iso_code)
        client_lat, client_lon, client_city, client_country, client_country_code = replace_ip_address('100.229.139.139',
                                                                                                      geoip_client)
        self.assertEqual(latitude, client_lat)
        self.assertEqual(longitude, client_lon)
        self.assertEqual(city, client_city)
        self.assertEqual(country, client_country)
        self.assertEqual(country_iso_code, client_country_code)

    def test_list_to_jsonl_gz(self):
        """ Test writing list of dicts to jsonl.gz file """
        list_of_dicts = [{'k1a': 'v1a', 'k2a': 'v2a'},
                         {'k1b': 'v1b', 'k2b': 'v2b'}
                         ]
        file_path = 'list.jsonl.gz'
        expected_file_hash = 'e608cfeb'
        with CliRunner().isolated_filesystem():
            list_to_jsonl_gz(file_path, list_of_dicts)
            self.assertTrue(os.path.isfile(file_path))
            actual_file_hash = gzip_file_crc(file_path)
            self.assertEqual(expected_file_hash, actual_file_hash)

    def test_upload_file_to_storage_bucket(self):
        """ Test that file is uploaded to storage bucket """
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create file
            upload_file_name = f'{random_id()}.txt'
            with open(upload_file_name, 'w') as f:
                f.write('hello world')
            expected_crc32c = 'yZRlqg=='

            # Create client for blob
            gc_bucket_name: str = os.getenv('TESTS_GOOGLE_CLOUD_BUCKET_NAME')
            blob_name = 'blob'
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(gc_bucket_name)
            blob = bucket.blob(blob_name)

            try:
                success = upload_file_to_cloud_storage(gc_bucket_name, blob_name, upload_file_name)
                self.assertTrue(success)
                self.assertTrue(blob.exists())
                blob.reload()
                self.assertEqual(expected_crc32c, blob.crc32c)

            finally:
                if blob.exists():
                    blob.delete()
