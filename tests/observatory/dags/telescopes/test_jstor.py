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

import base64
import json
import os
from unittest.mock import patch

import httpretty
import pendulum
from airflow.models.connection import Connection
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence
from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.client.model.organisation import Organisation
from observatory.api.server import orm
from observatory.dags.telescopes.jstor import (JstorRelease, JstorTelescope, get_label_id)
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.template_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import (ObservatoryEnvironment, ObservatoryTestCase, module_file_path,
                                                   test_fixtures_path)


class TestJstor(ObservatoryTestCase):
    """ Tests for the Jstor telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestJstor, self).__init__(*args, **kwargs)
        self.project_id = os.getenv('TEST_GCP_PROJECT_ID')
        self.data_location = os.getenv('TEST_GCP_DATA_LOCATION')
        self.organisation_name = 'ANU Press'
        self.extra = {'publisher_id': 'anupress'}
        self.host = "localhost"
        self.api_port = 5000

        self.release_date = pendulum.parse('20210301').end_of('month')
        publisher_id = self.extra.get('publisher_id')
        self.country_report = {'path': test_fixtures_path('telescopes', 'jstor', 'country_20210401.tsv'),
                               'url': 'https://www.jstor.org/admin/reports/download/249192019',
                               'headers': {'Content-Disposition': f'attachment; filename=PUB_{publisher_id}_PUBBCU_'
                                                                  f'{self.release_date.strftime("%Y%m%d")}.tsv'},
                               'download_hash': '9c5eff69085457758e3743d229ec46a1',
                               'transform_hash': '9b197a54',
                               'table_rows': 10}
        self.institution_report = {'path': test_fixtures_path('telescopes', 'jstor', 'institution_20210401.tsv'),
                                   'url': 'https://www.jstor.org/admin/reports/download/129518301',
                                   'headers': {'Content-Disposition': f'attachment; filename=PUB_{publisher_id}_PUBBIU_'
                                                                      f'{self.release_date.strftime("%Y%m%d")}.tsv'},
                                   'download_hash': '793ee70d9102d8dca3cace65cb00ecc3',
                                   'transform_hash': '4a664f4d',
                                   'table_rows': 3}

    def test_dag_structure(self):
        """ Test that the Jstor DAG has the correct structure.

        :return: None
        """
        organisation = Organisation(name=self.organisation_name)
        dag = JstorTelescope(organisation, self.extra.get('publisher_id')).make_dag()
        self.assert_dag_structure({
            'check_dependencies': ['list_reports'],
            'list_reports': ['download_reports'],
            'download_reports': ['upload_downloaded'],
            'upload_downloaded': ['transform'],
            'transform': ['upload_transformed'],
            'upload_transformed': ['bq_load_partition'],
            'bq_load_partition': ['cleanup'],
            'cleanup': []
        }, dag)

    def test_dag_load(self):
        """ Test that the Jstor DAG can be loaded from a DAG bag.

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
            telescope_type = orm.TelescopeType(name='Jstor Telescope',
                                               type_id=TelescopeTypes.jstor,
                                               created=dt,
                                               modified=dt)
            env.api_session.add(telescope_type)
            organisation = orm.Organisation(name='Curtin Press',
                                            created=dt,
                                            modified=dt)
            env.api_session.add(organisation)
            telescope = orm.Telescope(name='Curtin Press Jstor Telescope',
                                      telescope_type=telescope_type,
                                      organisation=organisation,
                                      modified=dt,
                                      created=dt,
                                      extra=self.extra)
            env.api_session.add(telescope)
            env.api_session.commit()

            dag_file = os.path.join(module_file_path('observatory.dags.dags'), 'jstor.py')
            self.assert_dag_load('jstor_curtin_press', dag_file)

    @patch('observatory.dags.telescopes.jstor.build')
    @patch('observatory.dags.telescopes.jstor.Credentials')
    def test_telescope(self, mock_account_credentials, mock_build):
        """ Test the Jstor telescope end to end.

        :return: None.
        """

        mock_account_credentials.from_json_keyfile_dict.return_value = ''

        http = HttpMockSequence(create_http_mock_sequence(self.country_report['url'], self.institution_report['url']))
        mock_build.return_value = build('gmail', 'v1', http=http)

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        organisation = Organisation(name=self.organisation_name,
                                    gcp_project_id=self.project_id,
                                    gcp_download_bucket=env.download_bucket,
                                    gcp_transform_bucket=env.transform_bucket)
        telescope = JstorTelescope(organisation=organisation, publisher_id=self.extra.get('publisher_id'),
                                   dataset_id=dataset_id)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # add gmail connection
            conn = Connection(conn_id=AirflowConns.GMAIL_API,
                              uri='google-cloud-platform://?token=123&refresh_token=123'
                                  '&client_id=123.apps.googleusercontent.com&client_secret=123')
            env.add_connection(conn)

            # Test that all dependencies are specified: no error should be thrown
            env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

            # Test list releases task with files available
            with httpretty.enabled():
                for report in [self.country_report, self.institution_report]:
                    self.setup_mock_file_download(report['url'], report['path'], headers=report['headers'],
                                                  method=httpretty.HEAD)
                ti = env.run_task(telescope.list_reports.__name__, dag, execution_date)
            available_reports = ti.xcom_pull(key=JstorTelescope.REPORTS_INFO,
                                             task_ids=telescope.list_reports.__name__, include_prior_dates=False)
            self.assertIsInstance(available_reports, list)
            expected_reports_info = [{'type': 'country',
                                      'url': self.country_report['url'],
                                      'id': '1788ec9e91f3de62'},
                                     {'type': 'institution',
                                      'url': self.institution_report['url'],
                                      'id': '1788ebe4ecbab055'}]
            self.assertListEqual(expected_reports_info, available_reports)

            # Test download_reports task
            with httpretty.enabled():
                for report in [self.country_report, self.institution_report]:
                    self.setup_mock_file_download(report['url'], report['path'], headers=report['headers'])
                ti = env.run_task(telescope.download_reports.__name__, dag, execution_date)

            # use release info for other tasks
            available_releases = ti.xcom_pull(key=JstorTelescope.RELEASE_INFO,
                                              task_ids=telescope.download_reports.__name__, include_prior_dates=False)
            self.assertIsInstance(available_releases, dict)
            self.assertEqual(1, len(available_releases))
            for release_date, reports_info in available_releases.items():
                self.assertEqual(self.release_date, release_date)
                self.assertIsInstance(reports_info, list)
                self.assertListEqual(expected_reports_info, reports_info)
            release = JstorRelease(telescope.dag_id, release_date, reports_info, organisation)

            self.assertEqual(2, len(release.download_files))
            for file in release.download_files:
                if 'country' in file:
                    expected_file_hash = self.country_report['download_hash']
                else:
                    expected_file_hash = self.institution_report['download_hash']
                self.assert_file_integrity(file, expected_file_hash, 'md5')

            # Test that file uploaded
            env.run_task(telescope.upload_downloaded.__name__, dag, execution_date)
            for file in release.download_files:
                self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

            # Test that file transformed
            env.run_task(telescope.transform.__name__, dag, execution_date)
            self.assertEqual(2, len(release.transform_files))
            for file in release.transform_files:
                if 'country' in file:
                    expected_file_hash = self.country_report['transform_hash']
                else:
                    expected_file_hash = self.institution_report['transform_hash']
                self.assert_file_integrity(file, expected_file_hash, 'gzip_crc')

            # Test that transformed file uploaded
            env.run_task(telescope.upload_transformed.__name__, dag, execution_date)
            for file in release.transform_files:
                self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

            # Test that data loaded into BigQuery
            env.run_task(telescope.bq_load_partition.__name__, dag, execution_date)
            for file in release.transform_files:
                table_id, _ = table_ids_from_path(file)
                table_id = f'{self.project_id}.{dataset_id}.{table_id}${release.release_date.strftime("%Y%m")}'
                if 'country' in file:
                    expected_rows = self.country_report['table_rows']
                else:
                    expected_rows = self.institution_report['table_rows']
                self.assert_table_integrity(table_id, expected_rows)

            # Test that all telescope data deleted
            download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                release.transform_folder
            env.run_task(telescope.cleanup.__name__, dag, execution_date)
            self.assert_cleanup(download_folder, extract_folder, transform_folder)

    def test_get_label_id(self):
        """ Test getting label id both when label already exists and does not exist yet.

        :return: None.
        """
        list_labels_no_match = {'labels': [{'id': 'CHAT',
                                            'name': 'CHAT',
                                            'messageListVisibility': 'hide',
                                            'labelListVisibility': 'labelHide',
                                            'type': 'system'},
                                           {'id': 'SENT',
                                            'name': 'SENT',
                                            'type': 'system'}]}
        create_label = {'id': 'created_label',
                        'name': JstorTelescope.PROCESSED_LABEL_NAME,
                        'messageListVisibility': 'show',
                        'labelListVisibility': 'labelShow'}
        list_labels_match = {'labels': [{'id': 'CHAT',
                                         'name': 'CHAT',
                                         'messageListVisibility': 'hide',
                                         'labelListVisibility': 'labelHide',
                                         'type': 'system'},
                                        {'id': 'existing_label',
                                         'name': JstorTelescope.PROCESSED_LABEL_NAME,
                                         'messageListVisibility': 'show',
                                         'labelListVisibility': 'labelShow',
                                         'type': 'user'}]}
        http = HttpMockSequence([
            ({'status': '200'}, json.dumps(list_labels_no_match)),
            ({'status': '200'}, json.dumps(create_label)),
            ({'status': '200'}, json.dumps(list_labels_match)),
        ])
        service = build('gmail', 'v1', http=http)

        # call function without match for label, so label is created
        label_id = get_label_id(service, JstorTelescope.PROCESSED_LABEL_NAME)
        self.assertEqual('created_label', label_id)

        # call function with match for label
        label_id = get_label_id(service, JstorTelescope.PROCESSED_LABEL_NAME)
        self.assertEqual('existing_label', label_id)


def create_http_mock_sequence(country_report_url: str, institution_report_url: str) -> list:
    """ Create a list with mocked http responses

    :param country_report_url: URL to country report
    :param institution_report_url: URL to institution report
    :return: List with http responses
    """
    list_labels = {'labels': [{'id': 'CHAT', 'name': 'CHAT', 'messageListVisibility': 'hide',
                               'labelListVisibility': 'labelHide', 'type': 'system'},
                              {'id': 'Label_1', 'name': JstorTelescope.PROCESSED_LABEL_NAME,
                               'messageListVisibility': 'show', 'labelListVisibility': 'labelShow', 'type': 'user'}]}
    list_messages = {'messages': [{'id': '1788ec9e91f3de62', 'threadId': '1788e9b0a848236a'},
                                  {'id': '1788ebe4ecbab055', 'threadId': '1788e9b0a848236a'}],
                     'resultSizeEstimate': 4}
    get_message1 = {'id': '1788ec9e91f3de62', 'threadId': '1788e9b0a848236a',
                    'labelIds': ['CATEGORY_PERSONAL', 'INBOX'],
                    'snippet': 'JSTOR JSTOR Usage Reports Report Complete Twitter Facebook Tumblr Dear OAEBU Service '
                               'Account, Your usage report &quot;Book Usage by Country&quot; is now available to '
                               'download. Download Completed Report',
                    'payload': {'partId': '',
                                'mimeType': 'text/html',
                                'filename': '',
                                'headers': [{'name': 'Delivered-To', 'value': 'accountname@gmail.com'}],
                                'body': {'size': 12313, 'data': base64.urlsafe_b64encode(
                                    f'<a href="{country_report_url}">Download Completed Report</a>'.encode()).decode()}},
                    'sizeEstimate': 17939, 'historyId': '2302', 'internalDate': '1617303299000'}
    get_message2 = {'id': '1788ebe4ecbab055', 'threadId': '1788e9b0a848236a',
                    'labelIds': ['CATEGORY_PERSONAL', 'INBOX'],
                    'snippet': 'JSTOR JSTOR Usage Reports Report Complete Twitter Facebook Tumblr Dear OAEBU Service '
                               'Account, Your usage report &quot;Book Usage by Country&quot; is now available to '
                               'download. Download Completed Report',
                    'payload': {'partId': '',
                                'mimeType': 'text/html',
                                'filename': '',
                                'headers': [{'name': 'Delivered-To', 'value': 'accountname@gmail.com'}],
                                'body': {'size': 12313, 'data': base64.urlsafe_b64encode(
                                    f'<a href="{institution_report_url}">Download Completed Report</a>'.encode()).decode()}},
                    'sizeEstimate': 17939, 'historyId': '2302', 'internalDate': '1617303299000'}
    modify_message1 = {'id': '1788ec9e91f3de62', 'threadId': '1788e9b0a848236a',
                       'labelIds': ['Label_1', 'CATEGORY_PERSONAL', 'INBOX']}
    modify_message2 = {'id': '1788ebe4ecbab055', 'threadId': '1788e9b0a848236a',
                       'labelIds': ['Label_1', 'CATEGORY_PERSONAL', 'INBOX']}
    http_mock_sequence = [({'status': '200'}, json.dumps(list_labels)),
                          ({'status': '200'}, json.dumps(list_messages)),
                          ({'status': '200'}, json.dumps(get_message1)),
                          ({'status': '200'}, json.dumps(get_message2)),
                          ({'status': '200'}, json.dumps(list_labels)),
                          ({'status': '200'}, json.dumps(modify_message1)),
                          ({'status': '200'}, json.dumps(modify_message2))]

    return http_mock_sequence
