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
from observatory.dags.telescopes.jstor import (JstorRelease, JstorTelescope)
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.template_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase
from pendulum import Pendulum


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
        self.sftp_service_conn = os.getenv('TESTS_SFTP_SERVICE_CONN')
        self.release_date = pendulum.parse('20210311')
        self.download_hashes = {
            'jstor_country.tsv': '23462a6264198b75faa56584bae0bc3d',
            'jstor_institution.tsv': '191273d0b5230201125a6296eff336f6'
        }
        self.transform_hashes = {
            'jstor_country.jsonl.gz': 'd40dfc62',
            'jstor_institution.jsonl.gz': '1939ee8a'
        }

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

    def test_telescope(self):
        """ Test the Jstor telescope end to end.

        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = JstorTelescope(dataset_id=dataset_id)
        telescope.sftp_folder = '/unittests/jstor'
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # add gmail connection
            conn = Connection(conn_id=AirflowConns.SFTP_SERVICE)
            conn.parse_from_uri(self.sftp_service_conn)
            env.add_connection(conn)

            # Test that all dependencies are specified: no error should be thrown
            env.run_task(dag, telescope.check_dependencies.__name__, execution_date)

            # TODO
            # # Test list releases task when no files are available
            # with patch('pysftp.Connection') as mock_pysftp:
            #     mock_pysftp.return_value = MockSftpService({telescope.sftp_folder: ['']})
            #     ti = env.run_task(dag, telescope.list_releases.__name__, execution_date)

            # Test list releases task with files available
            ti = env.run_task(dag, telescope.list_releases.__name__, execution_date)
            reports_info = ti.xcom_pull(key=JstorTelescope.RELEASE_INFO, task_ids=telescope.list_releases.__name__,
                                        include_prior_dates=False)
            self.assertIsInstance(reports_info, dict)
            releases = []
            for release_date, sftp_files in reports_info.items():
                self.assertIsInstance(release_date, Pendulum)
                self.assertEqual(self.release_date, release_date)
                self.assertEqual([os.path.join(telescope.sftp_folder, 'PUB_anupress_PUBBCU_20210311.tsv'),
                                  os.path.join(telescope.sftp_folder, 'PUB_anupress_PUBBIU_20210311.tsv')], sftp_files)

                # use release info for other tasks
                release = JstorRelease(telescope.dag_id, release_date, sftp_files)
                releases.append(release)

            # Test download task
            env.run_task(dag, telescope.download.__name__, execution_date)
            for release in releases:
                self.assertEqual(2, len(release.download_files))

                for file in release.download_files:
                    expected_file_hash = self.download_hashes[os.path.basename(file)]
                    self.assert_file_integrity(file, expected_file_hash, 'md5')

            # Test that file uploaded
            env.run_task(dag, telescope.upload_downloaded.__name__, execution_date)
            for release in releases:
                for file in release.download_files:
                    self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

            # Test that file transformed
            env.run_task(dag, telescope.transform.__name__, execution_date)
            for release in releases:
                for file in release.transform_files:
                    transformed_file_hash = self.transform_hashes[os.path.basename(file)]
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
                    expected_rows = 9
                    self.assert_table_integrity(table_id, expected_rows)

            # Test that all telescope data deleted
            download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                release.transform_folder
            # mock pysftp, so unittest files are not deleted from server
            with patch('pysftp.Connection') as mock_pysftp:
                mock_pysftp.return_value = MockSftpService({
                                                               telescope.sftp_folder: ['']
                                                           })
                env.run_task(dag, telescope.cleanup.__name__, execution_date)
            self.assert_cleanup(download_folder, extract_folder, transform_folder)
