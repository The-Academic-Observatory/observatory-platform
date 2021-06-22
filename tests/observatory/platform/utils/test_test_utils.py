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

# Author: James Diprose, Aniek Roelofs

from __future__ import annotations

import datetime
import os
import unittest
from typing import Union, List

import croniter
import httpretty
import pendulum
import pysftp
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from click.testing import CliRunner
from google.cloud.bigquery import SourceFormat
from google.cloud.exceptions import NotFound
from observatory.platform.telescopes.telescope import Telescope, AbstractRelease
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.gc_utils import (
    create_bigquery_dataset,
    upload_file_to_cloud_storage,
    load_bigquery_table,
)
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    random_id,
    ObservatoryTestCase,
    test_fixtures_path,
    SftpServer,
)
from observatory.platform.utils.url_utils import retry_session

DAG_ID = "telescope-test"
MY_VAR_ID = "my-variable"
MY_CONN_ID = "my-connection"
DAG_FILE_CONTENT = """
# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from tests.observatory.platform.utils.test_test_utils import TelescopeTest

telescope = TelescopeTest()
globals()['test-telescope'] = telescope.make_dag()
"""


class TelescopeTest(Telescope):
    """ A telescope for testing purposes """

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: datetime = datetime.datetime(2020, 9, 1),
        schedule_interval: str = "@weekly",
    ):
        airflow_vars = [
            AirflowVars.DATA_PATH,
            AirflowVars.PROJECT_ID,
            AirflowVars.DATA_LOCATION,
            AirflowVars.DOWNLOAD_BUCKET,
            AirflowVars.TRANSFORM_BUCKET,
            MY_VAR_ID,
        ]
        airflow_conns = [MY_CONN_ID]
        super().__init__(dag_id, start_date, schedule_interval, airflow_vars=airflow_vars, airflow_conns=airflow_conns)
        self.add_setup_task(self.check_dependencies)

    def make_release(self, **kwargs) -> Union[AbstractRelease, List[AbstractRelease]]:
        pass


class TestObservatoryEnvironment(unittest.TestCase):
    """ Test the ObservatoryEnvironment """

    def __init__(self, *args, **kwargs):
        super(TestObservatoryEnvironment, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

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

        # No Google Cloud variables raises error
        with self.assertRaises(AssertionError):
            ObservatoryEnvironment().add_bucket()

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

        # No Google Cloud variables raises error
        bucket_id = random_id()
        with self.assertRaises(AssertionError):
            ObservatoryEnvironment()._create_bucket(bucket_id)
        with self.assertRaises(AssertionError):
            ObservatoryEnvironment()._delete_bucket(bucket_id)

    def test_add_delete_dataset(self):
        """ Test add_dataset and _delete_dataset """

        # Create dataset
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()
        create_bigquery_dataset(self.project_id, dataset_id, self.data_location)

        # Check that dataset exists: should not raise NotFound exception
        env.bigquery_client.get_dataset(dataset_id)

        # Delete dataset
        env._delete_dataset(dataset_id)

        # Check that dataset doesn't exist
        with self.assertRaises(NotFound):
            env.bigquery_client.get_dataset(dataset_id)

        # No Google Cloud variables raises error
        with self.assertRaises(AssertionError):
            ObservatoryEnvironment().add_dataset()
        with self.assertRaises(AssertionError):
            ObservatoryEnvironment()._delete_dataset(random_id())

    def test_create(self):
        """ Tests create, add_variable, add_connection and run_task """

        env = ObservatoryEnvironment(self.project_id, self.data_location)

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = TelescopeTest()
        dag = telescope.make_dag()

        with env.create():
            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID)
            conn.parse_from_uri("mysql://login:password@host:8080/schema?param1=val1&param2=val2")
            env.add_connection(conn)

            # Test run task
            ti = env.run_task(telescope.check_dependencies.__name__, dag, execution_date)
            self.assertFalse(ti.logger.propagate)

        # Test environment with logging enabled
        with env.create(task_logging=True):
            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID)
            conn.parse_from_uri("mysql://login:password@host:8080/schema?param1=val1&param2=val2")
            env.add_connection(conn)

            # Test run task
            ti = env.run_task(telescope.check_dependencies.__name__, dag, execution_date)
            self.assertTrue(ti.logger.propagate)

    def test_create_dagrun(self):
        """ Tests create_dag_run """
        env = ObservatoryEnvironment(self.project_id, self.data_location)

        # Setup Telescope
        first_execution_date = pendulum.datetime(year=2020, month=11, day=1)
        second_execution_date = pendulum.datetime(year=2020, month=12, day=1)
        telescope = TelescopeTest()
        dag = telescope.make_dag()

        # Get start dates outside of
        first_start_date = croniter.croniter(dag.normalized_schedule_interval, first_execution_date).get_next(
            datetime.datetime
        )
        second_start_date = croniter.croniter(dag.normalized_schedule_interval, second_execution_date).get_next(
            datetime.datetime
        )

        # Use DAG run with freezing time
        with env.create():
            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID)
            conn.parse_from_uri("mysql://login:password@host:8080/schema?param1=val1&param2=val2")
            env.add_connection(conn)

            self.assertIsNone(env.dag_run)
            # First DAG Run
            with env.create_dag_run(dag, first_execution_date):
                # Test DAG Run is set and has frozen start date
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(first_start_date.date(), env.dag_run.start_date.date())

                ti1 = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual("success", ti1.state)
                self.assertIsNone(ti1.previous_ti)

            with env.create_dag_run(dag, second_execution_date):
                # Test DAG Run is set and has frozen start date
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(second_start_date.date(), env.dag_run.start_date.date())

                ti2 = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual("success", ti2.state)
                # Test previous ti is set
                self.assertEqual(ti1.job_id, ti2.previous_ti.job_id)

        # Use DAG run without freezing time
        with env.create():
            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID)
            conn.parse_from_uri("mysql://login:password@host:8080/schema?param1=val1&param2=val2")
            env.add_connection(conn)

            # First DAG Run
            with env.create_dag_run(dag, first_execution_date, freeze=False):
                # Test DAG Run is set and has today as start date
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(pendulum.now().date(), env.dag_run.start_date.date())

                ti1 = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual("success", ti1.state)
                self.assertIsNone(ti1.previous_ti)

            # Second DAG Run
            with env.create_dag_run(dag, second_execution_date, freeze=False):
                # Test DAG Run is set and has today as start date
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(pendulum.now().date(), env.dag_run.start_date.date())

                ti2 = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual("success", ti2.state)
                # Test previous ti is set
                self.assertEqual(ti1.job_id, ti2.previous_ti.job_id)


class TestObservatoryTestCase(unittest.TestCase):
    """ Test the ObservatoryTestCase class """

    def __init__(self, *args, **kwargs):
        super(TestObservatoryTestCase, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_assert_dag_structure(self):
        """ Test assert_dag_structure """

        test_case = ObservatoryTestCase()
        telescope = TelescopeTest()
        dag = telescope.make_dag()

        # No assertion error
        expected = {"check_dependencies": []}
        test_case.assert_dag_structure(expected, dag)

        # Raise assertion error
        with self.assertRaises(AssertionError):
            expected = {"check_dependencies": ["list_releases"], "list_releases": []}
            test_case.assert_dag_structure(expected, dag)

    def test_assert_dag_load(self):
        """ Test assert_dag_load """

        test_case = ObservatoryTestCase()
        with ObservatoryEnvironment().create() as temp_dir:
            # Write DAG into temp_dir
            file_path = os.path.join(temp_dir, f"telescope_test.py")
            with open(file_path, mode="w") as f:
                f.write(DAG_FILE_CONTENT)

            # DAG loaded successfully: should be no errors
            test_case.assert_dag_load(DAG_ID, file_path)

            # Remove DAG from temp_dir
            os.unlink(file_path)

            # DAG not loaded
            with self.assertRaises(AssertionError):
                test_case.assert_dag_load(DAG_ID, file_path)

    def test_assert_blob_integrity(self):
        """ Test assert_blob_integrity """

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            # Upload file to download bucket and check gzip-crc
            file_name = "people.csv"
            file_path = os.path.join(test_fixtures_path("utils", "gc_utils"), file_name)
            result, upload = upload_file_to_cloud_storage(env.download_bucket, file_name, file_path)
            self.assertTrue(result)

            # Check that blob exists
            test_case = ObservatoryTestCase()
            test_case.assert_blob_integrity(env.download_bucket, file_name, file_path)

            # Check that blob doesn't exist
            with self.assertRaises(AssertionError):
                test_case.assert_blob_integrity(env.transform_bucket, file_name, file_path)

    def test_assert_table_integrity(self):
        """ Test assert_table_integrity """

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            # Upload file to download bucket and check gzip-crc
            file_name = "people.jsonl"
            file_path = os.path.join(test_fixtures_path("utils", "gc_utils"), file_name)
            result, upload = upload_file_to_cloud_storage(env.download_bucket, file_name, file_path)
            self.assertTrue(result)

            # Create dataset
            dataset_id = env.add_dataset()
            create_bigquery_dataset(self.project_id, dataset_id, self.data_location)

            # Test loading JSON newline table
            table_name = random_id()
            schema_path = os.path.join(test_fixtures_path("utils", "gc_utils"), "people_schema.json")
            uri = f"gs://{env.download_bucket}/{file_name}"
            result = load_bigquery_table(
                uri,
                dataset_id,
                self.data_location,
                table_name,
                schema_file_path=schema_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            self.assertTrue(result)

            # Check BigQuery table exists and has expected rows
            test_case = ObservatoryTestCase()
            table_id = f"{dataset_id}.{table_name}"
            expected_rows = 5
            test_case.assert_table_integrity(table_id, expected_rows)

            # Check that BigQuery table doesn't exist
            with self.assertRaises(AssertionError):
                table_id = f"{dataset_id}.{random_id()}"
                test_case.assert_table_integrity(table_id, expected_rows)

            # Check that BigQuery table has incorrect rows
            with self.assertRaises(AssertionError):
                table_id = f"{dataset_id}.{table_name}"
                expected_rows = 20
                test_case.assert_table_integrity(table_id, expected_rows)

    def test_assert_file_integrity(self):
        """ Test assert_file_integrity """

        test_case = ObservatoryTestCase()
        tests_path = test_fixtures_path("utils", "gc_utils")

        # Test md5
        file_path = os.path.join(tests_path, "people.csv")
        expected_hash = "ad0d7ad3dc3434337cebd5fb543420e7"
        algorithm = "md5"
        test_case.assert_file_integrity(file_path, expected_hash, algorithm)

        # Test gzip-crc
        file_path = os.path.join(tests_path, "people.csv.gz")
        expected_hash = "3beea5ac"
        algorithm = "gzip_crc"
        test_case.assert_file_integrity(file_path, expected_hash, algorithm)

    def test_assert_cleanup(self):
        """ Test assert_cleanup """

        with CliRunner().isolated_filesystem() as temp_dir:
            download = os.path.join(temp_dir, "download")
            extract = os.path.join(temp_dir, "extract")
            transform = os.path.join(temp_dir, "transform")

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
            file_path = os.path.join(temp_dir, f"content.txt")
            with open(file_path, mode="w") as f:
                f.write(expected_data)

            # Check that content was downloaded from test file
            test_case = ObservatoryTestCase()
            url = "https://example.com"
            with httpretty.enabled():
                test_case.setup_mock_file_download(url, file_path)
                response = retry_session().get(url)
                self.assertEqual(expected_data, response.content.decode("utf-8"))


class TestSftpServer(unittest.TestCase):
    def setUp(self) -> None:
        self.host = "localhost"
        self.port = 3373

    def test_server(self):
        """ Test that the SFTP server can be connected to """

        server = SftpServer(host=self.host, port=self.port)
        with server.create() as root_dir:
            # Connect to SFTP server and disable host key checking
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            sftp = pysftp.Connection(self.host, port=self.port, username="", password="", cnopts=cnopts)

            # Check that there are no files
            files = sftp.listdir(".")
            self.assertFalse(len(files))

            # Add a file and check that it exists
            expected_file_name = "onix.xml"
            file_path = os.path.join(root_dir, expected_file_name)
            with open(file_path, mode="w") as f:
                f.write("hello world")
            files = sftp.listdir(".")
            self.assertEqual(1, len(files))
            self.assertEqual(expected_file_name, files[0])
