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

import io
import os
from datetime import timedelta
from unittest.mock import patch

import pendulum
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from botocore.response import StreamingBody
from observatory.dags.telescopes.orcid import (OrcidRelease, OrcidTelescope)
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.file_utils import _hash_file
from observatory.platform.utils.gc_utils import upload_file_to_cloud_storage, upload_files_to_cloud_storage
from observatory.platform.utils.template_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase, module_file_path, \
    test_fixtures_path


class TestOrcid(ObservatoryTestCase):
    """ Tests for the Orcid telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestOrcid, self).__init__(*args, **kwargs)
        self.project_id = os.getenv('TEST_GCP_PROJECT_ID')
        self.data_location = os.getenv('TEST_GCP_DATA_LOCATION')

        self.first_execution_date = pendulum.datetime(year=2021, month=2, day=1)
        self.second_execution_date = pendulum.datetime(year=2021, month=3, day=1)

        # orcid records
        self.records = {}
        for file in ['0000-0002-9227-8514.xml', '0000-0002-9228-8514.xml', '0000-0002-9229-8514.xml']:
            self.records[file] = {'blob': f"514/{file}",
                                  'path': os.path.join(test_fixtures_path("telescopes", "orcid"), file)}
        # last modified file
        self.last_modified_path = os.path.join(test_fixtures_path("telescopes", "orcid"), 'last_modified.csv.tar')

    def test_dag_structure(self):
        """ Test that the ORCID DAG has the correct structure.
        :return: None
        """

        dag = OrcidTelescope().make_dag()
        self.assert_dag_structure({
            'check_dependencies': ['get_release_info'],
            'get_release_info': ['transfer'],
            'transfer': ['download_transferred'],
            'download_transferred': ['transform'],
            'transform': ['upload_transformed'],
            'upload_transformed': ['bq_load_partition'],
            'bq_load_partition': ['bq_delete_old'],
            'bq_delete_old': ['bq_append_new'],
            'bq_append_new': ['cleanup'],
            'cleanup': []
        }, dag)

    def test_dag_load(self):
        """ Test that the ORCID DAG can be loaded from a DAG bag.
        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path('observatory.dags.dags'), 'orcid.py')
            self.assert_dag_load('orcid', dag_file)

    @patch('observatory.dags.telescopes.orcid.aws_to_google_cloud_storage_transfer')
    @patch("observatory.dags.telescopes.orcid.boto3.client")
    def test_telescope(self, mock_client, mock_transfer):
        """ Test the ORCID telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Set up google cloud sync bucket
        orcid_bucket = env.add_bucket()

        # Setup Telescope
        telescope = OrcidTelescope(dataset_id=dataset_id)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # Add connection
            conn = Connection(conn_id=AirflowConns.ORCID,
                              uri="aws://UWLA41aAhdja:AJLD91saAJSKAL0AjAhkaka@")
            # uri=self.orcid_conn)
            env.add_connection(conn)

            # Add variable
            var = Variable(key=AirflowVars.ORCID_BUCKET, val=orcid_bucket)  # type: ignore
            env.add_variable(var)

            # first run
            with env.create_dag_run(dag, self.first_execution_date):

                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test list releases task with files available
                ti = env.run_task(telescope.get_release_info.__name__)
                start_date, end_date, first_release = ti.xcom_pull(key=OrcidTelescope.RELEASE_INFO,
                                                                   task_ids=telescope.get_release_info.__name__,
                                                                   include_prior_dates=False)
                self.assertEqual(start_date, dag.default_args['start_date'])
                self.assertEqual(end_date, pendulum.today('UTC') - timedelta(days=1))
                self.assertTrue(first_release)

                # use release info for other tasks
                release = OrcidRelease(telescope.dag_id, start_date, end_date, first_release)

                # Test transfer task
                mock_transfer.return_value = True, 2
                env.run_task(telescope.transfer.__name__)
                mock_transfer.assert_called_once()
                try:
                    self.assertTupleEqual(mock_transfer.call_args[0], (conn.login, conn.password))
                except AssertionError:
                    raise AssertionError("AWS key id and secret not passed correctly to transfer function")
                self.assertDictEqual(mock_transfer.call_args[1], {'aws_bucket': OrcidTelescope.SUMMARIES_BUCKET,
                                                                  'include_prefixes': [],
                                                                  'gc_project_id': self.project_id,
                                                                  'gc_bucket': orcid_bucket,
                                                                  'description': 'Transfer ORCID data from airflow '
                                                                                 'telescope',
                                                                  'last_modified_since': None,
                                                                  'last_modified_before': release.end_date})
                # Upload files to bucket, to mock transfer
                record1 = self.records['0000-0002-9227-8514.xml']
                record2 = self.records['0000-0002-9228-8514.xml']
                upload_files_to_cloud_storage(orcid_bucket, [record1['blob'], record2['blob']],
                                              [record1['path'], record2['path']])
                self.assert_blob_integrity(orcid_bucket, record1['blob'], record1['path'])
                self.assert_blob_integrity(orcid_bucket, record2['blob'], record2['path'])

                # Test that file was downloaded
                env.run_task(telescope.download_transferred.__name__)
                self.assertEqual(2, len(release.download_files))
                for file in release.download_files:
                    expected_file_hash = _hash_file(self.records[os.path.basename(file)]['path'], algorithm='md5')
                    self.assert_file_integrity(file, expected_file_hash, 'md5')

                # Test that files transformed
                env.run_task(telescope.transform.__name__)
                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                expected_file_hash = '5f8c7477'
                self.assert_file_integrity(transform_path, expected_file_hash, 'gzip_crc')

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                self.assert_blob_integrity(env.transform_bucket, blob_name(transform_path), transform_path)

                # Test that load partition task is skipped for the first release
                ti = env.run_task(telescope.bq_load_partition.__name__)
                self.assertEqual(ti.state, 'skipped')

                # Test delete old task is in success state, without doing anything
                ti = env.run_task(telescope.bq_delete_old.__name__)
                self.assertEqual(ti.state, 'success')

                # Test append new creates table
                env.run_task(telescope.bq_append_new.__name__)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_id = f'{self.project_id}.{telescope.dataset_id}.{main_table_id}'
                expected_rows = 2
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                    release.transform_folder
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

            # second run
            with env.create_dag_run(dag, self.second_execution_date):
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test list releases task with files available
                ti = env.run_task(telescope.get_release_info.__name__)
                start_date, end_date, first_release = ti.xcom_pull(key=OrcidTelescope.RELEASE_INFO,
                                                                   task_ids=telescope.get_release_info.__name__,
                                                                   include_prior_dates=False)
                self.assertEqual(start_date, release.end_date)
                self.assertEqual(end_date, pendulum.today('UTC') - timedelta(days=1))
                self.assertFalse(first_release)

                # use release info for other tasks
                release = OrcidRelease(telescope.dag_id, start_date, end_date, first_release)

                # Test transfer task
                mock_transfer.return_value = True, 2
                mock_transfer.reset_mock()
                env.run_task(telescope.transfer.__name__)
                mock_transfer.assert_called_once()
                try:
                    self.assertTupleEqual(mock_transfer.call_args[0], (conn.login, conn.password))
                except AssertionError:
                    raise AssertionError("AWS key id and secret not passed correctly to transfer function")
                self.assertDictEqual(mock_transfer.call_args[1], {'aws_bucket': OrcidTelescope.SUMMARIES_BUCKET,
                                                                  'include_prefixes': [],
                                                                  'gc_project_id': self.project_id,
                                                                  'gc_bucket': orcid_bucket,
                                                                  'description': 'Transfer ORCID data from airflow telescope',
                                                                  'last_modified_since': release.start_date,
                                                                  'last_modified_before': release.end_date})
                # Upload files to bucket, to mock transfer
                record3 = self.records['0000-0002-9229-8514.xml']
                upload_file_to_cloud_storage(orcid_bucket, record3['blob'], record3['path'])
                self.assert_blob_integrity(orcid_bucket, record3['blob'], record3['path'])

                # Mock response of get_object on last_modified file
                with open(self.last_modified_path, 'rb') as f:
                    file_bytes = f.read()
                    mock_client().get_object.return_value = {'Body': StreamingBody(
                        io.BytesIO(file_bytes),
                        len(file_bytes)
                    )}
                # Test that file was downloaded
                env.run_task(telescope.download_transferred.__name__)
                self.assertEqual(2, len(release.download_files))
                for file in release.download_files:
                    expected_file_hash = _hash_file(self.records[os.path.basename(file)]['path'], algorithm='md5')
                    self.assert_file_integrity(file, expected_file_hash, 'md5')

                # Test that files transformed
                env.run_task(telescope.transform.__name__)
                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                expected_file_hash = 'a6e04bc3'
                self.assert_file_integrity(transform_path, expected_file_hash, 'gzip_crc')

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                self.assert_blob_integrity(env.transform_bucket, blob_name(transform_path), transform_path)

                # Test that load partition task creates partition
                env.run_task(telescope.bq_load_partition.__name__)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_id = f'{self.project_id}.{telescope.dataset_id}.{partition_table_id}'
                expected_rows = 2
                self.assert_table_integrity(table_id, expected_rows)

                # Test task deleted rows from main table
                env.run_task(telescope.bq_delete_old.__name__)
                table_id = f'{self.project_id}.{telescope.dataset_id}.{main_table_id}'
                expected_rows = 1
                self.assert_table_integrity(table_id, expected_rows)

                # Test append new adds rows to table
                env.run_task(telescope.bq_append_new.__name__)
                table_id = f'{self.project_id}.{telescope.dataset_id}.{main_table_id}'
                expected_rows = 3
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                    release.transform_folder
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)
