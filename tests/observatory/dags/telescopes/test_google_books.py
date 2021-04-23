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
import shutil

import observatory.api.server.orm as orm
import pendulum
from airflow.models.connection import Connection
from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.client.model.organisation import Organisation
from observatory.dags.telescopes.google_books import GoogleBooksRelease, GoogleBooksTelescope
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.telescope_utils import SftpFolders
from observatory.platform.utils.template_utils import bigquery_partitioned_table_id, blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import (ObservatoryEnvironment, ObservatoryTestCase, SftpServer,
                                                   module_file_path, test_fixtures_path)

from tests.observatory.test_utils import test_fixtures_path


class TestGoogleBooks(ObservatoryTestCase):
    """ Tests for the GoogleBooks telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestGoogleBooks, self).__init__(*args, **kwargs)
        self.host = "localhost"
        self.api_port = 5000
        self.sftp_port = 3373
        self.project_id = os.getenv('TEST_GCP_PROJECT_ID')
        self.data_location = os.getenv('TEST_GCP_DATA_LOCATION')
        self.organisation_name = 'anu-press'
        self.organisation_folder = 'anu-press'

        self.test_files = {'GoogleBooksTrafficReport_2020_02.csv':
                               os.path.join(test_fixtures_path('telescopes', 'google_books'),
                                            'GoogleBooksTrafficReport_2020_02.csv'),
                           'GoogleSalesTransactionReport_2020_02.csv':
                               os.path.join(test_fixtures_path('telescopes', 'google_books'),
                                            'GoogleSalesTransactionReport_2020_02.csv')}
        self.traffic_download_hash = 'db4dca44d5231e0c4e2ad95db41b79b6'
        self.traffic_transform_hash = '63d7f678'
        self.sales_download_hash = '9d1981aaffcb0249ee9a625a879d2f95'
        self.sales_transform_hash = 'dc177c5a'

    def test_dag_structure(self):
        """ Test that the Google Books DAG has the correct structure.
        :return: None
        """

        organisation = Organisation(name=self.organisation_name)
        dag = GoogleBooksTelescope(organisation).make_dag()
        self.assert_dag_structure({
            'check_dependencies': ['list_release_info'],
            'list_release_info': ['move_files_to_in_progress'],
            'move_files_to_in_progress': ['download'],
            'download': ['upload_downloaded'],
            'upload_downloaded': ['transform'],
            'transform': ['upload_transformed'],
            'upload_transformed': ['bq_load'],
            'bq_load': ['move_files_to_finished'],
            'move_files_to_finished': ['cleanup'],
            'cleanup': []
        }, dag)

    def test_dag_load(self):
        """ Test that the Google Books DAG can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location,
                                     api_host=self.host, api_port=self.api_port)
        with env.create():
            # Add Observatory API connection
            conn = Connection(conn_id=AirflowConns.OBSERVATORY_API,
                              uri=f'http://:password@{self.host}:{self.api_port}')
            env.add_connection(conn)

            # Add a Google Books telescope
            dt = pendulum.utcnow()
            telescope_type = orm.TelescopeType(name='Google Books Telescope',
                                               type_id=TelescopeTypes.google_books,
                                               created=dt,
                                               modified=dt)
            env.api_session.add(telescope_type)
            organisation = orm.Organisation(name='anu-press',
                                            created=dt,
                                            modified=dt)
            env.api_session.add(organisation)
            telescope = orm.Telescope(name='anu-press Google Books Telescope',
                                      telescope_type=telescope_type,
                                      organisation=organisation,
                                      modified=dt,
                                      created=dt)
            env.api_session.add(telescope)
            env.api_session.commit()

            dag_file = os.path.join(module_file_path('observatory.dags.dags'), 'google_books.py')
            self.assert_dag_load('google_books_anu-press', dag_file)

    def test_telescope(self):
        """ Test the Google Books telescope end to end.

        :return: None.
        """

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        sftp_server = SftpServer(host=self.host, port=self.sftp_port)
        dataset_id = env.add_dataset()

        # Create the Observatory environment and run tests
        with env.create():
            with sftp_server.create() as sftp_root:
                # Setup Telescope
                execution_date = pendulum.datetime(year=2021, month=3, day=31)
                org = Organisation(name=self.organisation_name,
                                   gcp_project_id=self.project_id,
                                   gcp_download_bucket=env.download_bucket,
                                   gcp_transform_bucket=env.transform_bucket)
                telescope = GoogleBooksTelescope(org, dataset_id=dataset_id)
                dag = telescope.make_dag()

                # Add SFTP connection
                conn = Connection(conn_id=AirflowConns.SFTP_SERVICE,
                                  uri=f'ssh://:password@{self.host}:{self.sftp_port}')
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

                # Add file to SFTP server
                local_sftp_folders = SftpFolders(telescope.dag_id, self.organisation_name, sftp_root)
                os.makedirs(local_sftp_folders.upload, exist_ok=True)
                for file_name, file_path in self.test_files.items():
                    upload_file = os.path.join(local_sftp_folders.upload, file_name)
                    shutil.copy(file_path, upload_file)

                # Get release info from SFTP server and check that the correct release info is returned via Xcom
                ti = env.run_task(telescope.list_release_info.__name__, dag, execution_date)
                release_info = ti.xcom_pull(key=GoogleBooksTelescope.RELEASE_INFO,
                                            task_ids=telescope.list_release_info.__name__,
                                            include_prior_dates=False)

                from collections import defaultdict
                expected_release_files = []
                for file_name, file_path in self.test_files.items():
                    expected_release_date = pendulum.strptime(file_name[-11:].strip('.csv'), '%Y_%m')
                    expected_release_files.append(os.path.join(telescope.sftp_folders.in_progress, file_name))
                expected_release_info = defaultdict(list, {expected_release_date: expected_release_files})
                self.assertEqual(expected_release_info, release_info)

                # use release info for other tasks
                releases = []
                for release_date, sftp_files in release_info.items():
                    releases.append(GoogleBooksRelease(telescope.dag_id, release_date, sftp_files, org))

                # Test move file to in progress
                env.run_task(telescope.move_files_to_in_progress.__name__, dag, execution_date)
                for release in releases:
                    for file in release.sftp_files:
                        file_name = os.path.basename(file)
                        upload_file = os.path.join(local_sftp_folders.upload, file_name)
                        self.assertFalse(os.path.isfile(upload_file))

                        in_progress_file = os.path.join(local_sftp_folders.in_progress, file_name)
                        self.assertTrue(os.path.isfile(in_progress_file))

                # Test download
                env.run_task(telescope.download.__name__, dag, execution_date)
                for release in releases:
                    for file in release.download_files:
                        if 'traffic' in file:
                            expected_file_hash = self.traffic_download_hash
                        else:
                            expected_file_hash = self.sales_download_hash
                        self.assert_file_integrity(file, expected_file_hash, 'md5')

                # Test upload downloaded
                env.run_task(telescope.upload_downloaded.__name__, dag, execution_date)
                for release in releases:
                    for file in release.download_files:
                        self.assert_blob_integrity(env.download_bucket, blob_name(file), file)

                # Test that file transformed
                env.run_task(telescope.transform.__name__, dag, execution_date)
                for release in releases:
                    for file in release.transform_files:
                        if 'traffic' in file:
                            expected_file_hash = self.traffic_transform_hash
                        else:
                            expected_file_hash = self.sales_transform_hash
                        self.assert_file_integrity(file, expected_file_hash, 'gzip_crc')

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__, dag, execution_date)
                for release in releases:
                    for file in release.transform_files:
                        self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load.__name__, dag, execution_date)
                for release in releases:
                    for file in release.transform_files:
                        table_id, _ = table_ids_from_path(file)
                        table_id = f'{self.project_id}.{telescope.dataset_id}.{bigquery_partitioned_table_id(table_id, release.release_date)}'
                        expected_rows = 4
                        self.assert_table_integrity(table_id, expected_rows)

                # Test move files to finished
                env.run_task(telescope.move_files_to_finished.__name__, dag, execution_date)
                for release in releases:
                    for file in release.sftp_files:
                        file_name = os.path.basename(file)
                        in_progress_file = os.path.join(local_sftp_folders.in_progress, file_name)
                        self.assertFalse(os.path.isfile(in_progress_file))

                        finished_file = os.path.join(local_sftp_folders.finished, file_name)
                        self.assertTrue(os.path.isfile(finished_file))

                # Test cleanup
                download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                    release.transform_folder
                env.run_task(telescope.cleanup.__name__, dag, execution_date)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)
