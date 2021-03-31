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
from typing import Any, Dict
from unittest.mock import Mock, patch

import pendulum
from airflow.models.connection import Connection
from observatory.dags.telescopes.google_analytics import (GoogleAnalyticsTelescope, GoogleAnalyticsRelease)
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.template_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase
from pendulum import Pendulum
from croniter import croniter
from datetime import datetime

class MockSftpService(Mock):
    def __init__(self, files: Dict[str, list], **kwargs: Any):
        super().__init__(**kwargs)
        self.files = files

    def listdir(self, sftp_folder: str):
        return self.files[sftp_folder]

    def get(self, file: str, localpath: str):
        pass

    def remove(self, file: str):
        pass

    def close(self):
        pass


class TestGoogleAnalytics(ObservatoryTestCase):
    """ Tests for the Jstor telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestGoogleAnalytics, self).__init__(*args, **kwargs)
        self.project_id = os.getenv('TESTS_GOOGLE_CLOUD_PROJECT_ID')
        self.data_location = os.getenv('TESTS_DATA_LOCATION')
        self.oaebu_account_conn = os.getenv('TESTS_OAEBU_ACCOUNT_CONN')
        self.download_hashes = {
        }
        self.transform_hashes = {
        }

    def test_dag_structure(self):
        """ Test that the Jstor DAG has the correct structure.
        :return: None
        """

        dag = GoogleAnalyticsTelescope().make_dag()
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

    def test_telescope(self):
        """ Test the Jstor telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = GoogleAnalyticsTelescope(dataset_id=dataset_id)
        telescope.sftp_folder = '/unittests/jstor'
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # add gmail connection
            conn = Connection(conn_id=AirflowConns.OAEBU_SERVICE_ACCOUNT)
            conn.parse_from_uri(self.oaebu_account_conn)
            env.add_connection(conn)

            # Test that all dependencies are specified: no error should be thrown
            env.run_task(dag, telescope.check_dependencies.__name__, execution_date)

            # Use release to check tasks
            cron_schedule = dag.normalized_schedule_interval
            cron_iter = croniter(cron_schedule, execution_date)
            end_date = pendulum.instance(cron_iter.get_next(datetime))
            release = GoogleAnalyticsRelease(telescope.dag_id, execution_date, end_date)

            # Test download task
            env.run_task(dag, telescope.download.__name__, execution_date)
            # for release in releases:
            #     self.assertEqual(2, len(release.download_files))
            #
            #     for file in release.download_files:
            #         expected_file_hash = self.download_hashes[os.path.basename(file)]
            #         self.assert_file_integrity(file, expected_file_hash, 'md5')

            # # Test that file uploaded
            # env.run_task(dag, telescope.upload_downloaded.__name__, execution_date)
            # for release in releases:
            #     for file in release.download_files:
            #         self.assert_blob_integrity(env.download_bucket, blob_name(file), file)
            #
            # # Test that file transformed
            # env.run_task(dag, telescope.transform.__name__, execution_date)
            # for release in releases:
            #     for file in release.transform_files:
            #         transformed_file_hash = self.transform_hashes[os.path.basename(file)]
            #         self.assert_file_integrity(file, transformed_file_hash, 'gzip_crc')
            #
            # # Test that transformed file uploaded
            # env.run_task(dag, telescope.upload_transformed.__name__, execution_date)
            # for release in releases:
            #     for file in release.transform_files:
            #         self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)
            #
            # # Test that data loaded into BigQuery
            # env.run_task(dag, telescope.bq_load.__name__, execution_date)
            # for release in releases:
            #     for file in release.transform_files:
            #         table_id, _ = table_ids_from_path(file)
            #         table_id = f'{self.project_id}.{telescope.dataset_id}.{table_id}{release_date.strftime("%Y%m%d")}'
            #         expected_rows = 9
            #         self.assert_table_integrity(table_id, expected_rows)
            #
            # # Test that all telescope data deleted
            # download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
            #                                                     release.transform_folder
            # # mock pysftp, so unittest files are not deleted from server
            # with patch('pysftp.Connection') as mock_pysftp:
            #     mock_pysftp.return_value = MockSftpService({
            #                                                    telescope.sftp_folder: ['']
            #                                                })
            #     env.run_task(dag, telescope.cleanup.__name__, execution_date)
            # self.assert_cleanup(download_folder, extract_folder, transform_folder)