# Copyright 2019-2024 Curtin University
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


from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import httpretty
import pendulum
from google.cloud.bigquery import SourceFormat

from observatory_platform.config import module_file_path
from observatory_platform.google.bigquery import bq_create_dataset, bq_load_table, bq_table_id
from observatory_platform.google.gcs import gcs_blob_uri, gcs_upload_file
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import load_and_parse_json, random_id, SandboxTestCase
from observatory_platform.sandbox.tests.test_sandbox_environment import create_dag
from observatory_platform.url_utils import retry_session

DAG_ID = "my-dag"
DAG_FILE_CONTENT = """
# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from observatory_platform.sandbox.tests.test_sandbox_environment import create_dag

dag_id = "my-dag"
globals()[dag_id] = create_dag(dag_id=dag_id)
"""


class TestSandboxTestCase(unittest.TestCase):
    """Test the SandboxTestCase class"""

    def __init__(self, *args, **kwargs):
        super(TestSandboxTestCase, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.test_fixtures_path = module_file_path("observatory_platform.sandbox.tests.fixtures")

    def test_assert_dag_structure(self):
        """Test assert_dag_structure"""

        test_case = SandboxTestCase()
        dag = create_dag()

        # No assertion error
        expected = {"check_dependencies": ["task2"], "task2": ["task3"], "task3": []}
        test_case.assert_dag_structure(expected, dag)

        # Raise assertion error
        with self.assertRaises(AssertionError):
            expected = {"check_dependencies": ["list_releases"], "list_releases": []}
            test_case.assert_dag_structure(expected, dag)

    def test_assert_dag_load(self):
        """Test assert_dag_load"""

        test_case = SandboxTestCase()
        env = SandboxEnvironment()
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
                test_case.assert_dag_load("no dag found", os.path.join(self.test_fixtures_path, "bad_dag.py"))

            # No dag
            with self.assertRaises(AssertionError):
                empty_filename = os.path.join(temp_dir, "empty_dag.py")
                Path(empty_filename).touch()
                test_case.assert_dag_load("invalid_dag_id", empty_filename)

    def test_assert_blob_integrity(self):
        """Test assert_blob_integrity"""

        env = SandboxEnvironment(self.project_id, self.data_location)

        with env.create():
            # Upload file to download bucket and check gzip-crc
            blob_name = "people.csv"
            file_path = os.path.join(self.test_fixtures_path, blob_name)
            result, upload = gcs_upload_file(bucket_name=env.download_bucket, blob_name=blob_name, file_path=file_path)
            self.assertTrue(result)

            # Check that blob exists
            test_case = SandboxTestCase()
            test_case.assert_blob_integrity(env.download_bucket, blob_name, file_path)

            # Check that blob doesn't exist
            with self.assertRaises(AssertionError):
                test_case.assert_blob_integrity(env.transform_bucket, blob_name, file_path)

    def test_assert_table_integrity(self):
        """Test assert_table_integrity"""

        env = SandboxEnvironment(self.project_id, self.data_location)

        with env.create():
            # Upload file to download bucket and check gzip-crc
            blob_name = "people.jsonl"
            file_path = os.path.join(self.test_fixtures_path, blob_name)
            result, upload = gcs_upload_file(bucket_name=env.download_bucket, blob_name=blob_name, file_path=file_path)
            self.assertTrue(result)

            # Create dataset
            dataset_id = env.add_dataset()
            bq_create_dataset(project_id=self.project_id, dataset_id=dataset_id, location=self.data_location)

            # Test loading JSON newline table
            table_name = random_id()
            schema_path = os.path.join(self.test_fixtures_path, "people_schema.json")
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
            test_case = SandboxTestCase()
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

        env = SandboxEnvironment(self.project_id, self.data_location)

        with env.create():
            # Upload file to download bucket and check gzip-crc
            blob_name = "people.jsonl"
            file_path = os.path.join(self.test_fixtures_path, blob_name)
            result, upload = gcs_upload_file(bucket_name=env.download_bucket, blob_name=blob_name, file_path=file_path)
            self.assertTrue(result)

            # Create dataset
            dataset_id = env.add_dataset()
            bq_create_dataset(project_id=self.project_id, dataset_id=dataset_id, location=self.data_location)

            # Test loading JSON newline table
            table_name = random_id()
            schema_path = os.path.join(self.test_fixtures_path, "people_schema.json")
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
            test_case = SandboxTestCase()
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

        test_case = SandboxTestCase()

        # Test md5
        file_path = os.path.join(self.test_fixtures_path, "people.csv")
        expected_hash = "ad0d7ad3dc3434337cebd5fb543420e7"
        algorithm = "md5"
        test_case.assert_file_integrity(file_path, expected_hash, algorithm)

        # Test gzip-crc
        file_path = os.path.join(self.test_fixtures_path, "people.csv.gz")
        expected_hash = "3beea5ac"
        algorithm = "gzip_crc"
        test_case.assert_file_integrity(file_path, expected_hash, algorithm)

    def test_assert_cleanup(self):
        """Test assert_cleanup"""

        with tempfile.TemporaryDirectory() as temp_dir:
            workflow = os.path.join(temp_dir, "workflow")

            # Make download, extract and transform folders
            os.makedirs(workflow)

            # Check that assertion is raised when folders exist
            test_case = SandboxTestCase()
            with self.assertRaises(AssertionError):
                test_case.assert_cleanup(workflow)

            # Delete folders
            os.rmdir(workflow)

            # No error when folders deleted
            test_case.assert_cleanup(workflow)

    def test_setup_mock_file_download(self):
        """Test mocking a file download"""

        with tempfile.TemporaryDirectory() as temp_dir:
            # Write data into temp_dir
            expected_data = "Hello World!"
            file_path = os.path.join(temp_dir, f"content.txt")
            with open(file_path, mode="w") as f:
                f.write(expected_data)

            # Check that content was downloaded from test file
            test_case = SandboxTestCase()
            url = "https://example.com"
            with httpretty.enabled():
                test_case.setup_mock_file_download(url, file_path)
                response = retry_session().get(url)
                self.assertEqual(expected_data, response.content.decode("utf-8"))


class TestLoadAndParseJson(unittest.TestCase):
    def test_load_and_parse_json(self):
        # Create a temporary JSON file
        with tempfile.NamedTemporaryFile() as temp_file:
            # Create the data dictionary and write to temp file
            data = {
                "date1": "2022-01-01",
                "timestamp1": "2022-01-01 12:00:00.100000 UTC",
                "date2": "20230101",
                "timestamp2": "2023-01-01 12:00:00",
            }
            with open(temp_file.name, "w") as f:
                json.dump(data, f)

            # Test case 1: Parsing date fields with default date formats. Not specifying timestamp fields
            expected_result = data.copy()
            expected_result["date1"] = datetime(2022, 1, 1).date()
            expected_result["date2"] = datetime(2023, 1, 1).date()  # Should be converted by pendulum
            result = load_and_parse_json(temp_file.name, date_fields=["date1", "date2"], date_formats=["%Y-%m-%d"])
            self.assertEqual(result, expected_result)

            # Test case 2: Parsing timestamp fields with custom timestamp format, not specifying date field
            expected_result = data.copy()
            expected_result["timestamp1"] = datetime(2022, 1, 1, 12, 0, 0, 100000)
            expected_result["timestamp2"] = datetime(
                2023, 1, 1, 12, 0, 0, tzinfo=pendulum.tz.timezone("UTC")
            )  # Converted by pendulum
            result = load_and_parse_json(
                temp_file.name,
                timestamp_fields=["timestamp1", "timestamp2"],
                timestamp_formats=["%Y-%m-%d %H:%M:%S.%f %Z"],
            )
            self.assertEqual(result, expected_result)

            # Test case 3: Default date and timestamp formats
            expected_result = {
                "date1": datetime(2022, 1, 1).date(),
                "date2": "20230101",
                "timestamp1": datetime(2022, 1, 1, 12, 0, 0, 100000),
                "timestamp2": "2023-01-01 12:00:00",
            }
            result = load_and_parse_json(temp_file.name, date_fields=["date1"], timestamp_fields=["timestamp1"])
            self.assertEqual(result, expected_result)
