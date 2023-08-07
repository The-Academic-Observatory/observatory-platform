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

import contextlib
import logging
import os
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Union
from unittest.mock import patch
from ftplib import FTP
import re

import croniter
import httpretty
import pendulum
import pysftp
import timeout_decorator
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from click.testing import CliRunner
from google.cloud.bigquery import SourceFormat
from google.cloud.exceptions import NotFound

from observatory.platform.bigquery import bq_create_dataset, bq_load_table, bq_table_id
from observatory.platform.config import AirflowVars
from observatory.platform.gcs import gcs_upload_file, gcs_blob_uri
from observatory.platform.observatory_environment import (
    HttpServer,
    ObservatoryEnvironment,
    ObservatoryTestCase,
    SftpServer,
    FtpServer,
    random_id,
    test_fixtures_path,
    find_free_port,
)
from observatory.platform.utils.http_download import (
    DownloadInfo,
    download_file,
    download_files,
)
from observatory.platform.utils.url_utils import retry_session
from observatory.platform.workflows.workflow import Workflow, Release

DAG_ID = "telescope-test"
MY_VAR_ID = "my-variable"
MY_CONN_ID = "my-connection"
DAG_FILE_CONTENT = """
# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from tests.observatory.platform.test_observatory_environment import TelescopeTest

telescope = TelescopeTest()
globals()['test-telescope'] = telescope.make_dag()
"""


class TelescopeTest(Workflow):
    """A telescope for testing purposes"""

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 9, 1, tz="UTC"),
        schedule: str = "@weekly",
        setup_task_result: bool = True,
    ):
        airflow_vars = [
            AirflowVars.DATA_PATH,
            MY_VAR_ID,
        ]
        airflow_conns = [MY_CONN_ID]
        super().__init__(dag_id, start_date, schedule, airflow_vars=airflow_vars, airflow_conns=airflow_conns)
        self.setup_task_result = setup_task_result
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.setup_task)
        self.add_task(self.my_task)

    def make_release(self, **kwargs) -> Union[Release, List[Release]]:
        return None

    def setup_task(self, **kwargs):
        logging.info("setup_task success")
        return self.setup_task_result

    def my_task(self, release, **kwargs):
        logging.info("my_task success")


class TestObservatoryEnvironment(unittest.TestCase):
    """Test the ObservatoryEnvironment"""

    def __init__(self, *args, **kwargs):
        super(TestObservatoryEnvironment, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_add_bucket(self):
        """Test the add_bucket method"""

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
        """Test _create_bucket and _delete_bucket"""

        env = ObservatoryEnvironment(self.project_id, self.data_location)

        bucket_id = "obsenv_tests_" + random_id()

        # Create bucket
        env._create_bucket(bucket_id)
        bucket = env.storage_client.bucket(bucket_id)
        self.assertTrue(bucket.exists())

        # Delete bucket
        env._delete_bucket(bucket_id)
        self.assertFalse(bucket.exists())

        # Test double delete is handled gracefully
        env._delete_bucket(bucket_id)

        # No Google Cloud variables raises error
        bucket_id = "obsenv_tests_" + random_id()
        with self.assertRaises(AssertionError):
            ObservatoryEnvironment()._create_bucket(bucket_id)
        with self.assertRaises(AssertionError):
            ObservatoryEnvironment()._delete_bucket(bucket_id)

    def test_add_delete_dataset(self):
        """Test add_dataset and _delete_dataset"""

        # Create dataset
        env = ObservatoryEnvironment(self.project_id, self.data_location)

        dataset_id = env.add_dataset()
        bq_create_dataset(project_id=self.project_id, dataset_id=dataset_id, location=self.data_location)

        # Check that dataset exists: should not raise NotFound exception
        dataset_id = f"{self.project_id}.{dataset_id}"
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
        """Tests create, add_variable, add_connection and run_task"""

        expected_state = "success"

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = TelescopeTest()
        dag = telescope.make_dag()

        # Test that previous tasks have to be finished to run next task
        env = ObservatoryEnvironment(self.project_id, self.data_location)

        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):
                # Add_variable
                env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

                # Add connection
                conn = Connection(
                    conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2"
                )
                env.add_connection(conn)

                # Test run task when dependencies are not met
                ti = env.run_task(telescope.setup_task.__name__)
                self.assertIsNone(ti.state)

                # Try again when dependencies are met
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(expected_state, ti.state)

                ti = env.run_task(telescope.setup_task.__name__)
                self.assertEqual(expected_state, ti.state)

                ti = env.run_task(telescope.my_task.__name__)
                self.assertEqual(expected_state, ti.state)

        # Test that tasks are skipped when setup task returns False
        telescope = TelescopeTest(setup_task_result=False)
        dag = telescope.make_dag()
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):
                # Add_variable
                env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

                # Add connection
                conn = Connection(
                    conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2"
                )
                env.add_connection(conn)

                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual(expected_state, ti.state)

                ti = env.run_task(telescope.setup_task.__name__)
                self.assertEqual(expected_state, ti.state)

                expected_state = "skipped"
                ti = env.run_task(telescope.my_task.__name__)
                self.assertEqual(expected_state, ti.state)

    def test_task_logging(self):
        """Test task logging"""

        expected_state = "success"
        env = ObservatoryEnvironment(self.project_id, self.data_location)

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        telescope = TelescopeTest()
        dag = telescope.make_dag()

        # Test environment without logging enabled
        with env.create():
            with env.create_dag_run(dag, execution_date):
                # Test add_variable
                env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

                # Test add_connection
                conn = Connection(
                    conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2"
                )
                env.add_connection(conn)

                # Test run task
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertFalse(ti.log.propagate)
                self.assertEqual(expected_state, ti.state)

        # Test environment with logging enabled
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create(task_logging=True):
            with env.create_dag_run(dag, execution_date):
                # Test add_variable
                env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

                # Test add_connection
                conn = Connection(
                    conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2"
                )
                env.add_connection(conn)

                # Test run task
                ti = env.run_task(telescope.check_dependencies.__name__)
                self.assertTrue(ti.log.propagate)
                self.assertEqual(expected_state, ti.state)

    def test_create_dagrun(self):
        """Tests create_dag_run"""

        env = ObservatoryEnvironment(self.project_id, self.data_location)

        # Setup Telescope
        first_execution_date = pendulum.datetime(year=2020, month=11, day=1, tz="UTC")
        second_execution_date = pendulum.datetime(year=2020, month=12, day=1, tz="UTC")
        telescope = TelescopeTest()
        dag = telescope.make_dag()

        # Get start dates outside of
        first_start_date = croniter.croniter(dag.normalized_schedule_interval, first_execution_date).get_next(
            pendulum.DateTime
        )
        second_start_date = croniter.croniter(dag.normalized_schedule_interval, second_execution_date).get_next(
            pendulum.DateTime
        )

        # Use DAG run with freezing time
        with env.create():
            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2")
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
                self.assertEqual(second_start_date, env.dag_run.start_date)

                ti2 = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual("success", ti2.state)
                # Test previous ti is set
                self.assertEqual(ti1.job_id, ti2.previous_ti.job_id)

        # Use DAG run without freezing time
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2")
            env.add_connection(conn)

            # First DAG Run
            with env.create_dag_run(dag, first_execution_date):
                # Test DAG Run is set and has today as start date
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(first_start_date, env.dag_run.start_date)

                ti1 = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual("success", ti1.state)
                self.assertIsNone(ti1.previous_ti)

            # Second DAG Run
            with env.create_dag_run(dag, second_execution_date):
                # Test DAG Run is set and has today as start date
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(second_start_date, env.dag_run.start_date)

                ti2 = env.run_task(telescope.check_dependencies.__name__)
                self.assertEqual("success", ti2.state)
                # Test previous ti is set
                self.assertEqual(ti1.job_id, ti2.previous_ti.job_id)

    def test_create_dag_run_timedelta(self):
        env = ObservatoryEnvironment(self.project_id, self.data_location)

        telescope = TelescopeTest(schedule=timedelta(days=1))
        dag = telescope.make_dag()
        execution_date = pendulum.datetime(2021, 1, 1)
        expected_dag_date = pendulum.datetime(2021, 1, 2)
        with env.create():
            with env.create_dag_run(dag, execution_date):
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(expected_dag_date, env.dag_run.start_date)


class TestObservatoryTestCase(unittest.TestCase):
    """Test the ObservatoryTestCase class"""

    def __init__(self, *args, **kwargs):
        super(TestObservatoryTestCase, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_assert_dag_structure(self):
        """Test assert_dag_structure"""

        test_case = ObservatoryTestCase()
        telescope = TelescopeTest()
        dag = telescope.make_dag()

        # No assertion error
        expected = {"check_dependencies": ["setup_task"], "setup_task": ["my_task"], "my_task": []}
        test_case.assert_dag_structure(expected, dag)

        # Raise assertion error
        with self.assertRaises(AssertionError):
            expected = {"check_dependencies": ["list_releases"], "list_releases": []}
            test_case.assert_dag_structure(expected, dag)

    def test_assert_dag_load(self):
        """Test assert_dag_load"""

        test_case = ObservatoryTestCase()
        env = ObservatoryEnvironment()
        with env.create() as temp_dir:
            # Write DAG into temp_dir
            file_path = os.path.join(temp_dir, f"telescope_test.py")
            with open(file_path, mode="w") as f:
                f.write(DAG_FILE_CONTENT)

            # DAG loaded successfully: should be no errors
            test_case.assert_dag_load(DAG_ID, file_path)

            # Remove DAG from temp_dir
            os.unlink(file_path)

            # DAG not loaded
            with self.assertRaises(Exception):
                test_case.assert_dag_load(DAG_ID, file_path)

            # DAG not found
            with self.assertRaises(Exception):
                test_case.assert_dag_load("dag not found", file_path)

            # Import errors
            with self.assertRaises(AssertionError):
                test_case.assert_dag_load("no dag found", test_fixtures_path("utils", "bad_dag.py"))

            # No dag
            with self.assertRaises(AssertionError):
                empty_filename = os.path.join(temp_dir, "empty_dag.py")
                Path(empty_filename).touch()
                test_case.assert_dag_load("invalid_dag_id", empty_filename)

    def test_assert_blob_integrity(self):
        """Test assert_blob_integrity"""

        env = ObservatoryEnvironment(self.project_id, self.data_location)

        with env.create():
            # Upload file to download bucket and check gzip-crc
            blob_name = "people.csv"
            file_path = test_fixtures_path("utils", blob_name)
            result, upload = gcs_upload_file(bucket_name=env.download_bucket, blob_name=blob_name, file_path=file_path)
            self.assertTrue(result)

            # Check that blob exists
            test_case = ObservatoryTestCase()
            test_case.assert_blob_integrity(env.download_bucket, blob_name, file_path)

            # Check that blob doesn't exist
            with self.assertRaises(AssertionError):
                test_case.assert_blob_integrity(env.transform_bucket, blob_name, file_path)

    def test_assert_table_integrity(self):
        """Test assert_table_integrity"""

        env = ObservatoryEnvironment(self.project_id, self.data_location)

        with env.create():
            # Upload file to download bucket and check gzip-crc
            blob_name = "people.jsonl"
            file_path = test_fixtures_path("utils", blob_name)
            result, upload = gcs_upload_file(bucket_name=env.download_bucket, blob_name=blob_name, file_path=file_path)
            self.assertTrue(result)

            # Create dataset
            dataset_id = env.add_dataset()
            bq_create_dataset(project_id=self.project_id, dataset_id=dataset_id, location=self.data_location)

            # Test loading JSON newline table
            table_name = random_id()
            schema_path = test_fixtures_path("utils", "people_schema.json")
            uri = gcs_blob_uri(env.download_bucket, blob_name)
            table_id = bq_table_id(self.project_id, dataset_id, table_name)
            result = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=schema_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            self.assertTrue(result)

            # Check BigQuery table exists and has expected rows
            test_case = ObservatoryTestCase()
            table_id = f"{self.project_id}.{dataset_id}.{table_name}"
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

    def test_assert_table_content(self):
        """Test assert table content

        :return: None.
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location)

        with env.create():
            # Upload file to download bucket and check gzip-crc
            blob_name = "people.jsonl"
            file_path = test_fixtures_path("utils", blob_name)
            result, upload = gcs_upload_file(bucket_name=env.download_bucket, blob_name=blob_name, file_path=file_path)
            self.assertTrue(result)

            # Create dataset
            dataset_id = env.add_dataset()
            bq_create_dataset(project_id=self.project_id, dataset_id=dataset_id, location=self.data_location)

            # Test loading JSON newline table
            table_name = random_id()
            schema_path = test_fixtures_path("utils", "people_schema.json")
            uri = gcs_blob_uri(env.download_bucket, blob_name)
            table_id = bq_table_id(self.project_id, dataset_id, table_name)
            result = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=schema_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            self.assertTrue(result)

            # Check BigQuery table exists and has expected rows
            test_case = ObservatoryTestCase()
            table_id = f"{self.project_id}.{dataset_id}.{table_name}"
            expected_content = [
                {"first_name": "Gisella", "last_name": "Derya", "dob": datetime(1997, 7, 1).date()},
                {"first_name": "Adelaida", "last_name": "Melis", "dob": datetime(1980, 9, 3).date()},
                {"first_name": "Melanie", "last_name": "Magomedkhan", "dob": datetime(1990, 3, 1).date()},
                {"first_name": "Octavia", "last_name": "Tomasa", "dob": datetime(1970, 1, 8).date()},
                {"first_name": "Ansgar", "last_name": "Zorion", "dob": datetime(2001, 2, 1).date()},
            ]
            test_case.assert_table_content(table_id, expected_content, "first_name")

            # Check that BigQuery table doesn't exist
            with self.assertRaises(AssertionError):
                table_id = f"{self.project_id}.{dataset_id}.{random_id()}"
                test_case.assert_table_content(table_id, expected_content, "first_name")

            # Check that BigQuery table has extra rows
            with self.assertRaises(AssertionError):
                table_id = f"{dataset_id}.{table_name}"
                expected_content = [
                    {"first_name": "Gisella", "last_name": "Derya", "dob": datetime(1997, 7, 1).date()},
                    {"first_name": "Adelaida", "last_name": "Melis", "dob": datetime(1980, 9, 3).date()},
                    {"first_name": "Octavia", "last_name": "Tomasa", "dob": datetime(1970, 1, 8).date()},
                    {"first_name": "Ansgar", "last_name": "Zorion", "dob": datetime(2001, 2, 1).date()},
                ]
                test_case.assert_table_content(table_id, expected_content, "first_name")

            # Check that BigQuery table has missing rows
            with self.assertRaises(AssertionError):
                table_id = f"{self.project_id}.{dataset_id}.{table_name}"
                expected_content = [
                    {"first_name": "Gisella", "last_name": "Derya", "dob": datetime(1997, 7, 1).date()},
                    {"first_name": "Adelaida", "last_name": "Melis", "dob": datetime(1980, 9, 3).date()},
                    {"first_name": "Melanie", "last_name": "Magomedkhan", "dob": datetime(1990, 3, 1).date()},
                    {"first_name": "Octavia", "last_name": "Tomasa", "dob": datetime(1970, 1, 8).date()},
                    {"first_name": "Ansgar", "last_name": "Zorion", "dob": datetime(2001, 2, 1).date()},
                    {"first_name": "Extra", "last_name": "Row", "dob": datetime(2001, 2, 1).date()},
                ]
                test_case.assert_table_content(table_id, expected_content, "first_name")

    def test_assert_file_integrity(self):
        """Test assert_file_integrity"""

        test_case = ObservatoryTestCase()
        tests_path = test_fixtures_path("utils")

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
        """Test assert_cleanup"""

        with CliRunner().isolated_filesystem() as temp_dir:
            workflow = os.path.join(temp_dir, "workflow")

            # Make download, extract and transform folders
            os.makedirs(workflow)

            # Check that assertion is raised when folders exist
            test_case = ObservatoryTestCase()
            with self.assertRaises(AssertionError):
                test_case.assert_cleanup(workflow)

            # Delete folders
            os.rmdir(workflow)

            # No error when folders deleted
            test_case.assert_cleanup(workflow)

    def test_setup_mock_file_download(self):
        """Test mocking a file download"""

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
        self.port = find_free_port()

    def test_server(self):
        """Test that the SFTP server can be connected to"""

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


class TestFtpServer(unittest.TestCase):
    def setUp(self) -> None:
        self.host = "localhost"
        self.port = find_free_port()

    @contextlib.contextmanager
    def test_server(self):
        """Test that the FTP server can be connected to"""

        with CliRunner().isolated_filesystem() as tmp_dir:
            server = FtpServer(directory=tmp_dir, host=self.host, port=self.port)
            with server.create() as root_dir:
                # Connect to FTP server anonymously
                ftp_conn = FTP()
                ftp_conn.connect(host=self.host, port=self.port)
                ftp_conn.login()

                # Check that there are no files
                files = ftp_conn.nlst()
                self.assertFalse(len(files))

                # Add a file and check that it exists
                expected_file_name = "textfile.txt"
                file_path = os.path.join(root_dir, expected_file_name)
                with open(file_path, mode="w") as f:
                    f.write("hello world")
                files = ftp_conn.nlst()
                self.assertEqual(1, len(files))
                self.assertEqual(expected_file_name, files[0])

    @contextlib.contextmanager
    def test_user_permissions(self):
        "Test the level of permissions of the root and anonymous users."

        with CliRunner().isolated_filesystem() as tmp_dir:
            server = FtpServer(
                directory=tmp_dir, host=self.host, port=self.port, root_username="root", root_password="pass"
            )
            with server.create() as root_dir:
                # Add a file onto locally hosted server.
                expected_file_name = "textfile.txt"
                file_path = os.path.join(root_dir, expected_file_name)
                with open(file_path, mode="w") as f:
                    f.write("hello world")

                # Connect to FTP server anonymously.
                ftp_conn = FTP()
                ftp_conn.connect(host=self.host, port=self.port)
                ftp_conn.login()

                # Make sure that anonymoous user has read-only permissions
                ftp_repsonse = ftp_conn.sendcmd(f"MLST {expected_file_name}")
                self.assertTrue(";perm=r;size=11;type=file;" in ftp_repsonse)

                ftp_conn.close()

                # Connect to FTP server as root user.
                ftp_conn = FTP()
                ftp_conn.connect(host=self.host, port=self.port)
                ftp_conn.login(user="root", passwd="pass")

                # Make sure that root user has all available read/write/modification permissions.
                ftp_repsonse = ftp_conn.sendcmd(f"MLST {expected_file_name}")
                self.assertTrue(";perm=radfwMT;size=11;type=file;" in ftp_repsonse)

                ftp_conn.close()


class TestHttpserver(ObservatoryTestCase):
    def test_serve(self):
        """Make sure the server can be constructed."""
        with patch("observatory.platform.observatory_environment.ThreadingHTTPServer.serve_forever") as m_serve:
            server = HttpServer(directory=".")
            server.serve_(("localhost", 10000), ".")
            self.assertEqual(m_serve.call_count, 1)

    @timeout_decorator.timeout(1)
    def test_stop_before_start(self):
        """Make sure there's no deadlock if we try to stop before a start."""

        server = HttpServer(directory=".")
        server.stop()

    @timeout_decorator.timeout(1)
    def test_start_twice(self):
        """Make sure there's no funny business if we try to stop before a start."""

        server = HttpServer(directory=".")
        server.start()
        server.start()
        server.stop()

    def test_server(self):
        """Test the webserver can serve a directory"""

        directory = test_fixtures_path("utils")
        server = HttpServer(directory=directory)
        server.start()

        test_file = "http_testfile.txt"
        expected_hash = "d8e8fca2dc0f896fd7cb4cb0031ba249"
        algorithm = "md5"

        url = f"{server.url}{test_file}"

        with CliRunner().isolated_filesystem() as tmpdir:
            dst_file = os.path.join(tmpdir, "testfile.txt")

            download_files(download_list=[DownloadInfo(url=url, filename=dst_file)])

            self.assert_file_integrity(dst_file, expected_hash, algorithm)

        server.stop()

    def test_context_manager(self):
        directory = test_fixtures_path("utils")
        server = HttpServer(directory=directory)

        with server.create():
            test_file = "http_testfile.txt"
            expected_hash = "d8e8fca2dc0f896fd7cb4cb0031ba249"
            algorithm = "md5"

            url = f"{server.url}{test_file}"

            with CliRunner().isolated_filesystem() as tmpdir:
                dst_file = os.path.join(tmpdir, "testfile.txt")
                download_file(url=url, filename=dst_file)
                self.assert_file_integrity(dst_file, expected_hash, algorithm)
