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

import httpretty
import pendulum
from airflow.models.connection import Connection

from observatory.dags.telescopes.crossref_metadata import CrossrefMetadataRelease, CrossrefMetadataTelescope
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.gc_utils import bigquery_partitioned_table_id
from observatory.platform.utils.template_utils import blob_name
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase, module_file_path
from tests.observatory.test_utils import test_fixtures_path


class TestCrossrefMetadata(ObservatoryTestCase):
    """ Tests for the Crossref Metadata telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefMetadata, self).__init__(*args, **kwargs)
        self.project_id = os.getenv('TEST_GCP_PROJECT_ID')
        self.data_location = os.getenv('TEST_GCP_DATA_LOCATION')
        self.download_path = test_fixtures_path('telescopes', 'crossref_metadata', 'crossref_metadata.json.tar.gz')
        self.extract_file_hashes = ['4a55065d90aaa58c69bc5f5a54da3006', 'c45901a52154789470410aad51485e9c',
                                    '4c0fd617224a557b9ef04313cca0bd4a', 'd93dc613e299871925532d906c3a44a1',
                                    'dd1ab247c55191a14bcd1bf32719c337']

    def test_dag_structure(self):
        """ Test that the Crossref Metadata DAG has the correct structure.

        :return: None
        """

        dag = CrossrefMetadataTelescope().make_dag()
        self.assert_dag_structure({
            'check_dependencies': ['check_release_exists'],
            'check_release_exists': ['download'],
            'download': ['upload_downloaded'],
            'upload_downloaded': ['extract'],
            'extract': ['transform'],
            'transform': ['upload_transformed'],
            'upload_transformed': ['bq_load'],
            'bq_load': ['cleanup'],
            'cleanup': []
        }, dag)

    def test_dag_load(self):
        """ Test that the Crossref Metadata DAG can be loaded from a DAG bag.

        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path('observatory.dags.dags'), 'crossref_metadata.py')
            self.assert_dag_load('crossref_metadata', dag_file)

    def test_telescope(self):
        """ Test the Crossref Metadata telescope end to end.

        :return: None.
        """

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = CrossrefMetadataTelescope(dataset_id=dataset_id)
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # Add Crossref Metadata connection
            env.add_connection(Connection(conn_id=AirflowConns.CROSSREF, uri=" mysql://:crossref-token@"))

            # Test that all dependencies are specified: no error should be thrown
            env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

            # Test check release exists task, next tasks should not be skipped
            with httpretty.enabled():
                url = CrossrefMetadataTelescope.TELESCOPE_URL.format(year=execution_date.year,
                                                                     month=execution_date.month)
                httpretty.register_uri(httpretty.HEAD, url, body="", status=302)
                ti = env.run_task(telescope.check_release_exists.__name__, dag, execution_date)

            release = CrossrefMetadataRelease(telescope.dag_id, execution_date)

            # Test download task
            with httpretty.enabled():
                self.setup_mock_file_download(release.url, self.download_path)
                env.run_task(telescope.download.__name__, dag, execution_date)
            self.assertEqual(1, len(release.download_files))
            expected_file_hash = '10210c33936f9ba6b7e053f6f457591b'
            self.assert_file_integrity(release.download_path, expected_file_hash, 'md5')

            # Test that file uploaded
            env.run_task(telescope.upload_downloaded.__name__, dag, execution_date)
            self.assert_blob_integrity(env.download_bucket, blob_name(release.download_path), release.download_path)

            # Test that file extracted
            env.run_task(telescope.extract.__name__, dag, execution_date)
            self.assertEqual(5, len(release.extract_files))
            for i, file in enumerate(release.extract_files):
                expected_file_hash = self.extract_file_hashes[i]
                self.assert_file_integrity(file, expected_file_hash, 'md5')

            # Test that file transformed
            env.run_task(telescope.transform.__name__, dag, execution_date)
            self.assertEqual(1, len(release.transform_files))
            expected_file_hash = '25e7768e'
            self.assert_file_integrity(release.transform_path, expected_file_hash, 'gzip_crc')

            # Test that transformed file uploaded
            env.run_task(telescope.upload_transformed.__name__, dag, execution_date)
            self.assert_blob_integrity(env.transform_bucket, blob_name(release.transform_path), release.transform_path)

            # Test that data loaded into BigQuery
            env.run_task(telescope.bq_load.__name__, dag, execution_date)
            table_id = f'{self.project_id}.{dataset_id}.' \
                       f'{bigquery_partitioned_table_id(telescope.dag_id, release.release_date)}'
            expected_rows = 20
            self.assert_table_integrity(table_id, expected_rows)

            # Test that all telescope data deleted
            download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                release.transform_folder
            env.run_task(telescope.cleanup.__name__, dag, execution_date)
            self.assert_cleanup(download_folder, extract_folder, transform_folder)
