# Copyright 2021-2024 Curtin University
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


import contextlib
import datetime
import json
import logging
import os
import shutil
import socket
import socketserver
import unittest
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Set

import boto3
import httpretty
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import DagBag
from airflow.operators.empty import EmptyOperator
from click.testing import CliRunner
from deepdiff import DeepDiff
from google.cloud import bigquery, storage
from google.cloud.bigquery import SourceFormat
from google.cloud.exceptions import NotFound
from pendulum import DateTime

from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.files import crc32c_base64_hash, get_file_hash, gzip_file_crc, save_jsonl_gz
from observatory_platform.google.bigquery import bq_sharded_table_id, bq_load_table, bq_table_id, bq_create_dataset
from observatory_platform.google.gcs import gcs_blob_uri, gcs_upload_files


class SandboxTestCase(unittest.TestCase):
    """Common test functions for testing Observatory Platform DAGs"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(SandboxTestCase, self).__init__(*args, **kwargs)
        self.storage_client = storage.Client()
        self.bigquery_client = bigquery.Client()

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        vcr_log = logging.getLogger("vcr")
        vcr_log.setLevel(logging.WARNING)

    @property
    def fake_cloud_workspace(self):
        return CloudWorkspace(
            project_id="project-id",
            download_bucket="download_bucket",
            transform_bucket="transform_bucket",
            data_location="us",
        )

    def assert_dag_structure(self, expected: Dict, dag: DAG):
        """Assert the DAG structure.

        :param expected: a dictionary of DAG task ids as keys and values which should be a list of downstream task ids.
        :param dag: the DAG.
        :return: None.
        """

        expected_keys = expected.keys()
        actual_keys = dag.task_dict.keys()
        self.assertEqual(expected_keys, actual_keys)

        for task_id, downstream_list in expected.items():
            self.assertTrue(dag.has_task(task_id))
            task = dag.get_task(task_id)
            self.assertEqual(set(downstream_list), task.downstream_task_ids)

    def assert_dag_load(self, dag_id: str, dag_file: str):
        """Assert that the given DAG loads from a DagBag.

        :param dag_id: the DAG id.
        :param dag_file: the path to the DAG file.
        :return: None.
        """

        with CliRunner().isolated_filesystem() as dag_folder:
            if not os.path.exists(dag_file):
                raise Exception(f"{dag_file} does not exist.")

            shutil.copy(dag_file, os.path.join(dag_folder, os.path.basename(dag_file)))

            dag_bag = DagBag(dag_folder=dag_folder)

            if dag_bag.import_errors != {}:
                logging.error(f"DagBag errors: {dag_bag.import_errors}")
            self.assertEqual({}, dag_bag.import_errors, dag_bag.import_errors)

            dag = dag_bag.get_dag(dag_id=dag_id)

            if dag is None:
                logging.error(
                    f"DAG not found in the database. Make sure the DAG ID is correct, and the dag file contains the words 'airflow' and 'DAG'."
                )
            self.assertIsNotNone(dag)

            self.assertGreaterEqual(len(dag.tasks), 1)

    def assert_dag_load_from_config(self, dag_id: str, dag_file: str):
        """Assert that the given DAG loads from a config file.

        :param dag_id: the DAG id.
        :param dag_file: the path to the dag loader
        :return: None.
        """

        self.assert_dag_load(dag_id, dag_file)

    def assert_blob_exists(self, bucket_id: str, blob_name: str):
        """Assert whether a blob exists or not.

        :param bucket_id: the Google Cloud storage bucket id.
        :param blob_name: the blob name (full path except for bucket)
        :return: None.
        """

        # Get blob
        bucket = self.storage_client.get_bucket(bucket_id)
        blob = bucket.blob(blob_name)
        self.assertTrue(blob.exists())

    def assert_blob_integrity(self, bucket_id: str, blob_name: str, local_file_path: str):
        """Assert whether the blob uploaded and that it has the expected hash.

        :param blob_name: the Google Cloud Blob name, i.e. the entire path to the blob on the Cloud Storage bucket.
        :param bucket_id: the Google Cloud Storage bucket id.
        :param local_file_path: the path to the local file.
        :return: whether the blob uploaded and that it has the expected hash.
        """

        # Get blob
        bucket = self.storage_client.get_bucket(bucket_id)
        blob = bucket.blob(blob_name)
        result = blob.exists()

        # Check that blob hash matches if it exists
        if result:
            # Get blob hash
            blob.reload()
            expected_hash = blob.crc32c

            # Check actual file
            actual_hash = crc32c_base64_hash(local_file_path)
            result = expected_hash == actual_hash

        self.assertTrue(result)

    def assert_table_integrity(self, table_id: str, expected_rows: int = None):
        """Assert whether a BigQuery table exists and has the expected number of rows.

        :param table_id: the BigQuery table id.
        :param expected_rows: the expected number of rows.
        :return: whether the table exists and has the expected number of rows.
        """

        table = None
        actual_rows = None
        try:
            table = self.bigquery_client.get_table(table_id)
            actual_rows = table.num_rows
        except NotFound:
            pass

        self.assertIsNotNone(table)
        if expected_rows is not None:
            self.assertEqual(expected_rows, actual_rows)

    def assert_table_content(self, table_id: str, expected_content: List[dict], primary_key: str):
        """Assert whether a BigQuery table has any content and if expected content is given whether it matches the
        actual content. The order of the rows is not checked, only whether all rows in the expected content match
        the rows in the actual content.
        The expected content should be a list of dictionaries, where each dictionary represents one row of the table,
        the keys are fieldnames and values are values.

        :param table_id: the BigQuery table id.
        :param expected_content: the expected content.
        :param primary_key: the primary key to use to compare.
        :return: whether the table has content and the expected content is correct
        """

        logging.info(
            f"assert_table_content: {table_id}, len(expected_content)={len(expected_content), }, primary_key={primary_key}"
        )
        rows = None
        actual_content = None
        try:
            rows = list(self.bigquery_client.list_rows(table_id))
            actual_content = [dict(row) for row in rows]
        except NotFound:
            pass
        self.assertIsNotNone(rows)
        self.assertIsNotNone(actual_content)
        results = compare_lists_of_dicts(expected_content, actual_content, primary_key)
        assert results, "Rows in actual content do not match expected content"

    def assert_table_bytes(self, table_id: str, expected_bytes: int):
        """Assert whether the given bytes from a BigQuery table matches the expected bytes.

        :param table_id: the BigQuery table id.
        :param expected_bytes: the expected number of bytes.
        :return: whether the table exists and the expected bytes match
        """

        table = None
        try:
            table = self.bigquery_client.get_table(table_id)
        except NotFound:
            pass

        self.assertIsNotNone(table)
        self.assertEqual(expected_bytes, table.num_bytes)

    def assert_file_integrity(self, file_path: str, expected_hash: str, algorithm: str):
        """Assert that a file exists and it has the correct hash.

        :param file_path: the path to the file.
        :param expected_hash: the expected hash.
        :param algorithm: the algorithm to use when hashing, either md5 or gzip crc
        :return: None.
        """

        self.assertTrue(os.path.isfile(file_path))

        if algorithm == "gzip_crc":
            actual_hash = gzip_file_crc(file_path)
        else:
            actual_hash = get_file_hash(file_path=file_path, algorithm=algorithm)

        self.assertEqual(expected_hash, actual_hash)

    def assert_cleanup(self, workflow_folder: str):
        """Assert that the download, extracted and transformed folders were cleaned up.

        :param workflow_folder: the path to the DAGs download folder.
        :return: None.
        """

        self.assertFalse(os.path.exists(workflow_folder))

    def setup_mock_file_download(
        self, uri: str, file_path: str, headers: Dict = None, method: str = httpretty.GET
    ) -> None:
        """Use httpretty to mock a file download.

        This function must be called from within an httpretty.enabled() block, for instance:

        with httpretty.enabled():
            self.setup_mock_file_download('https://example.com/file.zip', path_to_file)

        :param uri: the URI of the file download to mock.
        :param file_path: the path to the file on the local system.
        :param headers: the response headers.
        :return: None.
        """

        if headers is None:
            headers = {}

        with open(file_path, "rb") as f:
            body = f.read()

        httpretty.register_uri(method, uri, adding_headers=headers, body=body)


def random_id():
    """Generate a random id for bucket name.

    When code is pushed to a branch and a pull request is open, Github Actions runs the unit tests workflow
    twice, one for the push and one for the pull request. However, the uuid4 function, which calls os.urandom(16),
    generates the same sequence of values for each workflow run. We have also used the hostname of the machine
    in the construction of the random id to ensure sure that the ids are different on both workflow runs.

    :return: a random string id.
    """
    return str(uuid.uuid5(uuid.uuid4(), socket.gethostname())).replace("-", "")


def find_free_port(host: str = "localhost") -> int:
    """Find a free port.

    :param host: the host.
    :return: the free port number
    """

    with socketserver.TCPServer((host, 0), None) as tcp_server:
        return tcp_server.server_address[1]


def save_empty_file(path: str, file_name: str) -> str:
    """Save empty file and return path.

    :param path: the file directory.
    :param file_name: the file name.
    :return: the full file path.
    """

    file_path = os.path.join(path, file_name)
    open(file_path, "a").close()

    return file_path


@contextlib.contextmanager
def bq_dataset_test_env(*, project_id: str, location: str, prefix: str):
    client = bigquery.Client()
    dataset_id = prefix + "_" + random_id()
    try:
        bq_create_dataset(project_id=project_id, dataset_id=dataset_id, location=location)
        yield dataset_id
    finally:
        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@contextlib.contextmanager
def aws_bucket_test_env(*, prefix: str, region_name: str, expiration_days=1) -> str:
    # Create an S3 client
    s3 = boto3.Session().client("s3", region_name=region_name)
    bucket_name = f"obs-test-{prefix}-{random_id()}"
    try:
        s3.create_bucket(Bucket=bucket_name)  # CreateBucketConfiguration={"LocationConstraint": region_name}
        # Set up the lifecycle configuration
        lifecycle_configuration = {
            "Rules": [
                {"ID": "ExpireObjects", "Status": "Enabled", "Filter": {}, "Expiration": {"Days": expiration_days}}
            ]
        }
        # Apply the lifecycle configuration to the bucket
        s3.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle_configuration)
        yield bucket_name
    except Exception as e:
        raise e
    finally:
        # Get a reference to the bucket
        s3_resource = boto3.Session().resource("s3")
        bucket = s3_resource.Bucket(bucket_name)

        # Delete all objects and versions in the bucket
        bucket.objects.all().delete()
        bucket.object_versions.all().delete()

        # Delete the bucket
        bucket.delete()

        print(f"Bucket {bucket_name} deleted")


def load_and_parse_json(
    file_path: str,
    date_fields: Set[str] = None,
    timestamp_fields: Set[str] = None,
    date_formats: Set[str] = None,
    timestamp_formats: str = None,
):
    """Load a JSON file for testing purposes. It parses string dates and datetimes into date and datetime instances.

    :param file_path: the path to the JSON file.
    :param date_fields: The fields to parse as a date.
    :param timestamp_fields: The fields to parse as a timestamp.
    :param date_formats: The date formats to use. If none, will use [%Y-%m-%d, %Y%m%d].
    :param timestamp_formats: The timestamp formats to use. If none, will use [%Y-%m-%d %H:%M:%S.%f %Z].
    """

    if date_fields is None:
        date_fields = set()

    if timestamp_fields is None:
        timestamp_fields = set()

    if date_formats is None:
        date_formats = {"%Y-%m-%d", "%Y%m%d"}

    if timestamp_formats is None:
        timestamp_formats = {"%Y-%m-%d %H:%M:%S.%f %Z"}

    def parse_datetime(obj):
        for key, value in obj.items():
            # Try to parse into a date or datetime
            if key in date_fields:
                if isinstance(value, str):
                    format_found = False
                    for format in date_formats:
                        try:
                            obj[key] = datetime.strptime(value, format).date()
                            format_found = True
                            break
                        except (ValueError, TypeError):
                            pass
                    if not format_found:
                        try:
                            dt = pendulum.parse(value)
                            dt = datetime(
                                dt.year,
                                dt.month,
                                dt.day,
                                dt.hour,
                                dt.minute,
                                dt.second,
                                dt.microsecond,
                                tzinfo=dt.tzinfo,
                            ).date()
                            obj[key] = dt
                        except (ValueError, TypeError):
                            pass

            if key in timestamp_fields:
                if isinstance(value, str):
                    format_found = False
                    for format in timestamp_formats:
                        try:
                            obj[key] = datetime.strptime(value, format)
                            format_found = True
                            break
                        except (ValueError, TypeError):
                            pass
                    if not format_found:
                        try:
                            dt = pendulum.parse(value)
                            dt = datetime(
                                dt.year,
                                dt.month,
                                dt.day,
                                dt.hour,
                                dt.minute,
                                dt.second,
                                dt.microsecond,
                                tzinfo=dt.tzinfo,
                            )
                            obj[key] = dt
                        except (ValueError, TypeError):
                            pass

        return obj

    with open(file_path, mode="r") as f:
        rows = json.load(f, object_hook=parse_datetime)
    return rows


def compare_lists_of_dicts(expected: List[Dict], actual: List[Dict], primary_key: str) -> bool:
    """Compare two lists of dictionaries, using a primary_key as the basis for the top level comparisons.

    :param expected: the expected data.
    :param actual: the actual data.
    :param primary_key: the primary key.
    :return: whether the expected and actual match.
    """

    expected_dict = {item[primary_key]: item for item in expected}
    actual_dict = {item[primary_key]: item for item in actual}

    if set(expected_dict.keys()) != set(actual_dict.keys()):
        logging.error("Primary keys don't match:")
        logging.error(f"Only in expected: {set(expected_dict.keys()) - set(actual_dict.keys())}")
        logging.error(f"Only in actual: {set(actual_dict.keys()) - set(expected_dict.keys())}")
        return False

    all_matched = True
    for key in expected_dict:
        diff = DeepDiff(expected_dict[key], actual_dict[key], ignore_order=True)
        logging.info(f"primary_key: {key}")
        for diff_type, changes in diff.items():
            all_matched = False
            log_diff(diff_type, changes)

    return all_matched


def log_diff(diff_type, changes):
    """Log the DeepDiff changes.

    :param diff_type: the diff type.
    :param changes: the changes.
    :return: None.
    """

    if diff_type == "values_changed":
        for key_path, change in changes.items():
            logging.error(
                f"(expected) != (actual) {key_path}: {change['old_value']} (expected) != (actual) {change['new_value']}"
            )
    elif diff_type == "dictionary_item_added":
        for change in changes:
            logging.error(f"dictionary_item_added: {change}")
    elif diff_type == "dictionary_item_removed":
        for change in changes:
            logging.error(f"dictionary_item_removed: {change}")
    elif diff_type == "type_changes":
        for key_path, change in changes.items():
            logging.error(
                f"(expected) != (actual) {key_path}: {change['old_type']} (expected) != (actual) {change['new_type']}"
            )


def make_dummy_dag(dag_id: str, execution_date: pendulum.DateTime) -> DAG:
    """A Dummy DAG for testing purposes.

    :param dag_id: the DAG id.
    :param execution_date: the DAGs execution date.
    :return: the DAG.
    """

    with DAG(
        dag_id=dag_id,
        schedule="@weekly",
        default_args={"owner": "airflow", "start_date": execution_date},
        catchup=False,
    ) as dag:
        task1 = EmptyOperator(task_id="dummy_task")

    return dag


@dataclass
class Table:
    """A table to be loaded into Elasticsearch.

    :param table_name: the table name.
    :param is_sharded: whether the table is sharded or not.
    :param dataset_id: the dataset id.
    :param records: the records to load.
    :param schema_file_path: the schema file path.
    """

    table_name: str
    is_sharded: bool
    dataset_id: str
    records: List[Dict]
    schema_file_path: str


def bq_load_tables(
    *,
    project_id: str,
    tables: List[Table],
    bucket_name: str,
    snapshot_date: DateTime,
):
    """Load the fake Observatory Dataset in BigQuery.

    :param project_id: GCP project id.
    :param tables: the list of tables and records to load.
    :param bucket_name: the Google Cloud Storage bucket name.
    :param snapshot_date: the release date for the observatory dataset.
    :return: None.
    """

    with CliRunner().isolated_filesystem() as t:
        files_list = []
        blob_names = []

        # Save to JSONL
        for table in tables:
            blob_name = f"{table.dataset_id}-{table.table_name}.jsonl.gz"
            file_path = os.path.join(t, blob_name)
            save_jsonl_gz(file_path, table.records)
            files_list.append(file_path)
            blob_names.append(blob_name)

        # Upload to Google Cloud Storage
        success = gcs_upload_files(bucket_name=bucket_name, file_paths=files_list, blob_names=blob_names)
        assert success, "Data did not load into BigQuery"

        # Save to BigQuery tables
        for blob_name, table in zip(blob_names, tables):
            if table.schema_file_path is None:
                logging.error(
                    f"No schema found with search parameters: analysis_schema_path={table.schema_file_path}, "
                    f"table_name={table.table_name}, snapshot_date={snapshot_date}"
                )
                exit(os.EX_CONFIG)

            if table.is_sharded:
                table_id = bq_sharded_table_id(project_id, table.dataset_id, table.table_name, snapshot_date)
            else:
                table_id = bq_table_id(project_id, table.dataset_id, table.table_name)

            # Load BigQuery table
            uri = gcs_blob_uri(bucket_name, blob_name)
            logging.info(f"URI: {uri}")
            success = bq_load_table(
                uri=uri,
                table_id=table_id,
                schema_file_path=table.schema_file_path,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            if not success:
                raise AirflowException("bq_load task: data failed to load data into BigQuery")
