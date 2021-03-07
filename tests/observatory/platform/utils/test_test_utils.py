# Copyright 2021 Curtin University
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

# Author: James Diprose

from __future__ import annotations

import datetime
import os
import unittest
from typing import Union, List

import httpretty
import pendulum
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from click.testing import CliRunner
from google.cloud.exceptions import NotFound

from observatory.platform.telescopes.telescope import Telescope, AbstractRelease
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.gc_utils import create_bigquery_dataset
from observatory.platform.utils.test_utils import ObservatoryEnvironment, random_id, ObservatoryTestCase
from observatory.platform.utils.url_utils import retry_session

DAG_ID = 'telescope-test'
MY_VAR_ID = 'my-variable'
MY_CONN_ID = 'my-connection'
DAG_FILE_CONTENT = """
# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from tests.observatory.platform.utils.test_test_utils import TelescopeTest

telescope = TelescopeTest()
globals()['test-telescope'] = telescope.make_dag()
"""


class TelescopeTest(Telescope):
    """ A telescope for testing purposes """

    def __init__(self, dag_id: str = DAG_ID,
                 start_date: datetime = datetime.datetime(2020, 9, 1),
                 schedule_interval: str = '@weekly'):
        airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                        AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET, MY_VAR_ID]
        airflow_conns = [MY_CONN_ID]
        super().__init__(dag_id, start_date, schedule_interval, airflow_vars=airflow_vars, airflow_conns=airflow_conns)
        self.add_setup_task(self.check_dependencies)

    def make_release(self, **kwargs) -> Union[AbstractRelease, List[AbstractRelease]]:
        pass


class TestObservatoryEnvironment(unittest.TestCase):
    """ Test the ObservatoryEnvironment """

    def __init__(self, *args, **kwargs):
        super(TestObservatoryEnvironment, self).__init__(*args, **kwargs)
        self.project_id = os.getenv('TESTS_GOOGLE_CLOUD_PROJECT_ID')
        self.data_location = os.getenv('TESTS_DATA_LOCATION')

    def test_add_bucket(self):
        """ Test the add_bucket method """

        env = ObservatoryEnvironment(self.project_id, self.data_location)

        # The download and transform buckets are added in the constructor
        self.assertEqual(2, len(env.buckets))
        self.assertEqual(env.download_bucket, env.buckets[0])
        self.assertEqual(env.transform_bucket, env.buckets[1])

        # Test that calling add bucket adds a new bucket to the buckets list
        name = env.add_bucket()
        self.assertEqual(name, env.buckets[-1])

    def test_create_delete_bucket(self):
        """ Test _create_bucket and _delete_bucket """

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        bucket_id = random_id()

        # Create bucket
        env._create_bucket(bucket_id)
        bucket = env.storage_client.bucket(bucket_id)
        self.assertTrue(bucket.exists())

        # Delete bucket
        env._delete_bucket(bucket_id)
        self.assertFalse(bucket.exists())

    def test_delete_dataset(self):
        """ Test _delete_dataset """

        # Create dataset
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = random_id()
        create_bigquery_dataset(self.project_id, dataset_id, self.data_location)

        # Check that dataset exists: should not raise NotFound exception
        env.bigquery_client.get_dataset(dataset_id)

        # Delete dataset
        env._delete_dataset(dataset_id)

        # Check that dataset doesn't exist
        with self.assertRaises(NotFound):
            env.bigquery_client.get_dataset(dataset_id)

    def test_create(self):
        """ Tests create, add_variable, add_connection and run_task """

        env = ObservatoryEnvironment(self.project_id, self.data_location)

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = TelescopeTest()
        dag = telescope.make_dag()

        with env.create():
            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, val='hello'))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID)
            conn.parse_from_uri('mysql://login:password@host:8080/schema?param1=val1&param2=val2')
            env.add_connection(conn)

            # Test run task
            env.run_task(dag, telescope.check_dependencies.__name__, execution_date)


class TestObservatoryTestCase(unittest.TestCase):
    """ Test the ObservatoryTestCase class """

    def __init__(self, *args, **kwargs):
        super(TestObservatoryTestCase, self).__init__(*args, **kwargs)

    def test_assert_dag_structure(self):
        """ Test assert_dag_structure """

        test_case = ObservatoryTestCase()
        telescope = TelescopeTest()
        dag = telescope.make_dag()

        # No assertion error
        expected = {'check_dependencies': []}
        test_case.assert_dag_structure(expected, dag)

        # Raise assertion error
        with self.assertRaises(AssertionError):
            expected = {'check_dependencies': ['list_releases'],
                        'list_releases': []}
            test_case.assert_dag_structure(expected, dag)

    def test_assert_dag_load(self):
        """ Test assert_dag_load """

        test_case = ObservatoryTestCase()
        with CliRunner().isolated_filesystem() as temp_dir:
            # Write DAG into temp_dir
            file_path = os.path.join(temp_dir, f'telescope_test.py')
            with open(file_path, mode='w') as f:
                f.write(DAG_FILE_CONTENT)

            # DAG loaded successfully: should be no errors
            test_case.assert_dag_load(DAG_ID, dag_folder=temp_dir)

            # Remove DAG from temp_dir
            os.unlink(file_path)

            # DAG not loaded
            with self.assertRaises(AssertionError):
                test_case.assert_dag_load(DAG_ID, dag_folder=temp_dir)

    def test_assert_blob_integrity(self):
        pass

    def test_assert_table_integrity(self):
        pass

    def test_assert_file_integrity(self):
        pass

    def test_assert_cleanup(self):
        """ Test assert_cleanup """

        with CliRunner().isolated_filesystem() as temp_dir:
            download = os.path.join(temp_dir, 'download')
            extract = os.path.join(temp_dir, 'extract')
            transform = os.path.join(temp_dir, 'transform')

            # Make download, extract and transform folders
            os.makedirs(download)
            os.makedirs(extract)
            os.makedirs(transform)

            # Check that assertion is raised when folders exist
            test_case = ObservatoryTestCase()
            with self.assertRaises(AssertionError):
                test_case.assert_cleanup(download, extract, transform)

            # Delete folders
            os.rmdir(download)
            os.rmdir(extract)
            os.rmdir(transform)

            # No error when folders deleted
            test_case.assert_cleanup(download, extract, transform)

    def test_setup_mock_file_download(self):
        """ Test mocking a file download """

        with CliRunner().isolated_filesystem() as temp_dir:
            # Write data into temp_dir
            expected_data = "Hello World!"
            file_path = os.path.join(temp_dir, f'content.txt')
            with open(file_path, mode='w') as f:
                f.write(expected_data)

            # Check that content was downloaded from test file
            test_case = ObservatoryTestCase()
            url = "https://example.com"
            with httpretty.enabled():
                test_case.setup_mock_file_download(url, file_path)
                response = retry_session().get(url)
                self.assertEqual(expected_data, response.content.decode("utf-8"))
