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
import os
from typing import Any
from unittest.mock import Mock, patch

import httpretty
import pendulum
from airflow.models.connection import Connection
from observatory.dags.telescopes.jstor import (JstorRelease, JstorTelescope)
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.template_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase

from tests.observatory.test_utils import test_fixtures_path


class MockGmailService(Mock):
    """
    Mock the gmail api service
    """
    def __init__(self, messages_dict: dict, message: dict, label_dict: dict, **kwargs: Any):
        super().__init__(**kwargs)
        self.messages_dict = messages_dict
        self.message = message
        self.label_dict = label_dict

    def users(self):
        return Users(self.messages_dict, self.message, self.label_dict)


class Users:
    def __init__(self, messages_dict: dict, message: dict, label_dict: dict):
        self.messages_dict = messages_dict
        self.message = message
        self.label_dict = label_dict

    def messages(self):
        return Messages(self.messages_dict, self.message)

    def labels(self):
        return Labels(self.label_dict)


class Labels:
    def __init__(self, label_dict: dict):
        self.label_dict = label_dict

    def list(self, userId: str):
        return Request(self.label_dict)

    def create(self):
        return Request()


class Messages:
    def __init__(self, messages_dict: dict, message: dict):
        self.messages_dict = messages_dict
        self.message = message

    def list(self, userId: str, q: str):
        return Request(self.messages_dict)

    def get(self, userId: str, id: str):
        return Request(self.message[id])

    def modify(self, userId: str, id: str, body: dict):
        return Request()


class Request:
    def __init__(self, result=None):
        self.result = result

    def execute(self):
        return self.result


class TestJstor(ObservatoryTestCase):
    """ Tests for the Jstor telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestJstor, self).__init__(*args, **kwargs)
        self.project_id = os.getenv('TESTS_GOOGLE_CLOUD_PROJECT_ID')
        self.data_location = os.getenv('TESTS_DATA_LOCATION')
        self.gmail_api_conn = 'google-cloud-platform://?token=123&refresh_token=123' \
                              '&client_id=123.apps.googleusercontent.com&client_secret=123'

        self.release_date = pendulum.parse('20210301')

        self.country_report_path = test_fixtures_path('telescopes', 'jstor', 'country_20210301.tsv')
        self.country_report_url = 'https://www.jstor.org/admin/reports/download/12345'
        self.headers = {
            'Content-Disposition': f'attachment; filename=PUB_anupress_PUBBCU_'
                                   f'{self.release_date.strftime("%Y%m%d")}.tsv'
        }

        self.download_hash = '9c5eff69085457758e3743d229ec46a1'
        self.transform_hash = 'e772a805'

    def test_dag_structure(self):
        """ Test that the Jstor DAG has the correct structure.

        :return: None
        """

        dag = JstorTelescope().make_dag()
        self.assert_dag_structure({
            'check_dependencies': ['list_releases'],
            'list_releases': ['download'],
            'download': ['upload_downloaded'],
            'upload_downloaded': ['transform'],
            'transform': ['upload_transformed'],
            'upload_transformed': ['bq_load'],
            'bq_load': ['cleanup'],
            'cleanup': []
        }, dag)

    def test_dag_load(self):
        """ Test that the Jstor DAG can be loaded from a DAG bag.

        :return: None
        """

        with ObservatoryEnvironment().create():
            self.assert_dag_load('jstor')

    @patch('googleapiclient.discovery.Resource')
    def test_telescope(self, mock_gmail_service):
        """ Test the Jstor telescope end to end.

        :return: None.
        """
        mock_gmail_service.return_value = MockGmailService(messages_dict={
            'messages': [{
                             'id': '1785b19cf37d3044',
                             'threadId': '1785b19cf37d3044'
                         }]
        }, message={
            '1785b19cf37d3044': {
                'id': '1785b19cf37d3044',
                'threadId': '1785b19cf37d3044',
                'labelIds': ['IMPORTANT', 'Label_3', 'CATEGORY_UPDATES', 'INBOX'],
                'snippet': 'JSTOR JSTOR Usage Reports Report Complete Twitter Facebook Tumblr Dear OAEBU Service '
                           'Account, Your usage report &quot;Book Usage by Country&quot; is now available to '
                           'download. Download Completed Report',
                'payload': {
                    'body': {
                        'data': base64.urlsafe_b64encode(
                            f'<a href="{self.country_report_url}">Download Completed Report</a>'.encode())
                    }
                }
            }
        }, label_dict={
            'labels': [{
                           'id': 'CHAT',
                           'name': 'CHAT',
                           'messageListVisibility': 'hide',
                           'labelListVisibility': 'labelHide',
                           'type': 'system'
                       }, {
                           'id': 'Label_1',
                           'name': JstorTelescope.PROCESSED_LABEL_NAME,
                           'messageListVisibility': 'show',
                           'labelListVisibility': 'labelShow',
                           'type': 'user'
                       }]
        })

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = JstorTelescope(dataset_id=dataset_id)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # add gmail connection
            conn = Connection(conn_id=AirflowConns.GMAIL_API)
            conn.parse_from_uri(self.gmail_api_conn)
            env.add_connection(conn)

            # Test that all dependencies are specified: no error should be thrown
            env.run_task(dag, telescope.check_dependencies.__name__, execution_date)

            # Test list releases task with files available
            with httpretty.enabled():
                self.setup_mock_file_download(self.country_report_url, self.country_report_path, headers=self.headers,
                                              method=httpretty.HEAD)
                ti = env.run_task(dag, telescope.list_releases.__name__, execution_date)
            available_releases = ti.xcom_pull(key=JstorTelescope.RELEASE_INFO,
                                              task_ids=telescope.list_releases.__name__, include_prior_dates=False)
            self.assertIsInstance(available_releases, dict)
            releases = []
            for release_date, reports_info in available_releases.items():
                self.assertEqual(self.release_date, release_date)
                self.assertIsInstance(reports_info, list)
                self.assertEqual(1, len(reports_info))
                for report in reports_info:
                    self.assertDictEqual({
                                             'type': 'country',
                                             'url': 'https://www.jstor.org/admin/reports/download/12345',
                                             'id': '1785b19cf37d3044'
                                         }, report)

                # use release info for other tasks
                release = JstorRelease(telescope.dag_id, release_date, reports_info)
                releases.append(release)

            # Test download task
            with httpretty.enabled():
                self.setup_mock_file_download(self.country_report_url, self.country_report_path, headers=self.headers)
                env.run_task(dag, telescope.download.__name__, execution_date)
            for release in releases:
                self.assertEqual(1, len(release.download_files))
                for file in release.download_files:
                    expected_file_hash = self.download_hash
                    self.assert_file_integrity(file, expected_file_hash, 'md5')

            # Test that file uploaded
            env.run_task(dag, telescope.upload_downloaded.__name__, execution_date)
            for release in releases:
                for file in release.download_files:
                    self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

            # Test that file transformed
            env.run_task(dag, telescope.transform.__name__, execution_date)
            for release in releases:
                self.assertEqual(1, len(release.transform_files))
                for file in release.transform_files:
                    transformed_file_hash = self.transform_hash
                    self.assert_file_integrity(file, transformed_file_hash, 'gzip_crc')

            # Test that transformed file uploaded
            env.run_task(dag, telescope.upload_transformed.__name__, execution_date)
            for release in releases:
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

            # Test that data loaded into BigQuery
            env.run_task(dag, telescope.bq_load.__name__, execution_date)
            for release in releases:
                for file in release.transform_files:
                    table_id, _ = table_ids_from_path(file)
                    table_id = f'{self.project_id}.{telescope.dataset_id}.{table_id}{release_date.strftime("%Y%m%d")}'
                    expected_rows = 10
                    self.assert_table_integrity(table_id, expected_rows)

            # Test that all telescope data deleted
            download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                release.transform_folder
            env.run_task(dag, telescope.cleanup.__name__, execution_date)
            self.assert_cleanup(download_folder, extract_folder, transform_folder)
