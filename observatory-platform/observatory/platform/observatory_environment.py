# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Sources:
# * https://github.com/apache/airflow/blob/ffb472cf9e630bd70f51b74b0d0ea4ab98635572/airflow/cli/commands/task_command.py
# * https://github.com/apache/airflow/blob/master/docs/apache-airflow/best-practices.rst

# Copyright (c) 2011-2017 Ruslan Spivak
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Sources:
# * https://github.com/rspivak/sftpserver/blob/master/src/sftpserver/__init__.py

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

import contextlib
import datetime
import json
import logging
import os
import shutil
import socket
import socketserver
import threading
import time
import unittest
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from multiprocessing import Process
from typing import Dict, List, Optional, Set

import boto3
import croniter
import google
import httpretty
import paramiko
import pendulum
import requests
from airflow import DAG, settings
from airflow.exceptions import AirflowException
from airflow.models import DagBag
from airflow.models.connection import Connection
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils import db
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from click.testing import CliRunner
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from dateutil.relativedelta import relativedelta
from deepdiff import DeepDiff
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from pendulum import DateTime
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import ThreadedFTPServer
from sftpserver.stub_sftp import StubServer, StubSFTPServer

from observatory.api.testing import ObservatoryApiEnvironment
from observatory.platform.bigquery import bq_create_dataset
from observatory.platform.bigquery import (
    bq_sharded_table_id,
    bq_load_table,
    bq_table_id,
    SourceFormat,
    bq_delete_old_datasets_with_prefix,
)
from observatory.platform.config import module_file_path, AirflowVars, AirflowConns
from observatory.platform.files import crc32c_base64_hash, get_file_hash, gzip_file_crc, save_jsonl_gz
from observatory.platform.gcs import (
    gcs_blob_uri,
    gcs_upload_files,
    gcs_delete_old_buckets_with_prefix,
)
from observatory.platform.observatory_config import Workflow, CloudWorkspace, workflows_to_json_string


def random_id():
    """Generate a random id for bucket name.

    When code is pushed to a branch and a pull request is open, Github Actions runs the unit tests workflow
    twice, one for the push and one for the pull request. However, the uuid4 function, which calls os.urandom(16),
    generates the same sequence of values for each workflow run. We have also used the hostname of the machine
    in the construction of the random id to ensure sure that the ids are different on both workflow runs.

    :return: a random string id.
    """
    return str(uuid.uuid5(uuid.uuid4(), socket.gethostname())).replace("-", "")


def test_fixtures_path(*subdirs) -> str:
    """Get the path to the Observatory Platform test data directory.

    :return: he Observatory Platform test data directory.
    """

    base_path = module_file_path("tests.fixtures")
    return os.path.join(base_path, *subdirs)


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


class ObservatoryEnvironment:
    OBSERVATORY_HOME_KEY = "OBSERVATORY_HOME"

    def __init__(
        self,
        project_id: str = None,
        data_location: str = None,
        api_host: str = "localhost",
        api_port: int = 5000,
        enable_api: bool = True,
        prefix: Optional[str] = "obsenv_tests",
        age_to_delete: int = 12,
        workflows: List[Workflow] = None,
    ):
        """Constructor for an Observatory environment.

        To create an Observatory environment:
        env = ObservatoryEnvironment()
        with env.create():
            pass

        :param project_id: the Google Cloud project id.
        :param data_location: the Google Cloud data location.
        :param api_host: the Observatory API host.
        :param api_port: the Observatory API port.
        :param enable_api: whether to enable the observatory API or not.
        :param prefix: prefix for buckets and datsets created for the testing environment.
        :param age_to_delete: age of buckets and datasets to delete that share the same prefix, in hours
        """

        self.project_id = project_id
        self.data_location = data_location
        self.api_host = api_host
        self.api_port = api_port
        self.buckets = []
        self.datasets = []
        self.data_path = None
        self.session = None
        self.temp_dir = None
        self.api_env = None
        self.api_session = None
        self.enable_api = enable_api
        self.dag_run: DagRun = None
        self.prefix = prefix
        self.age_to_delete = age_to_delete
        self.workflows = workflows

        if self.create_gcp_env:
            self.download_bucket = self.add_bucket()
            self.transform_bucket = self.add_bucket()
            self.storage_client = storage.Client()
            self.bigquery_client = bigquery.Client()
        else:
            self.download_bucket = None
            self.transform_bucket = None
            self.storage_client = None
            self.bigquery_client = None

    @property
    def cloud_workspace(self) -> CloudWorkspace:
        return CloudWorkspace(
            project_id=self.project_id,
            download_bucket=self.download_bucket,
            transform_bucket=self.transform_bucket,
            data_location=self.data_location,
        )

    @property
    def create_gcp_env(self) -> bool:
        """Whether to create the Google Cloud project environment.

        :return: whether to create Google Cloud project environ,ent
        """

        return self.project_id is not None and self.data_location is not None

    def assert_gcp_dependencies(self):
        """Assert that the Google Cloud project dependencies are met.

        :return: None.
        """

        assert self.create_gcp_env, "Please specify the Google Cloud project_id and data_location"

    def add_bucket(self, prefix: Optional[str] = None) -> str:
        """Add a Google Cloud Storage Bucket to the Observatory environment.

        The bucket will be created when create() is called and deleted when the Observatory
        environment is closed.

        :param prefix: an optional additional prefix for the bucket.
        :return: returns the bucket name.
        """

        self.assert_gcp_dependencies()
        parts = []
        if self.prefix:
            parts.append(self.prefix)
        if prefix:
            parts.append(prefix)
        parts.append(random_id())
        bucket_name = "_".join(parts)

        if len(bucket_name) > 63:
            raise Exception(f"Bucket name cannot be longer than 63 characters: {bucket_name}")
        else:
            self.buckets.append(bucket_name)

        return bucket_name

    def _create_bucket(self, bucket_id: str) -> None:
        """Create a Google Cloud Storage Bucket.

        :param bucket_id: the bucket identifier.
        :return: None.
        """

        self.assert_gcp_dependencies()
        self.storage_client.create_bucket(bucket_id, location=self.data_location)
        logging.info(f"Created bucket with name: {bucket_id}")

    def _create_dataset(self, dataset_id: str) -> None:
        """Create a BigQuery dataset.

        :param dataset_id: the dataset identifier.
        :return: None.
        """

        self.assert_gcp_dependencies()
        dataset = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
        dataset.location = self.data_location
        self.bigquery_client.create_dataset(dataset, exists_ok=True)
        logging.info(f"Created dataset with name: {dataset_id}")

    def _delete_bucket(self, bucket_id: str) -> None:
        """Delete a Google Cloud Storage Bucket.

        :param bucket_id: the bucket identifier.
        :return: None.
        """

        self.assert_gcp_dependencies()
        try:
            bucket = self.storage_client.get_bucket(bucket_id)
            bucket.delete(force=True)
        except requests.exceptions.ReadTimeout:
            pass
        except google.api_core.exceptions.NotFound:
            logging.warning(
                f"Bucket {bucket_id} not found. Did you mean to call _delete_bucket on the same bucket twice?"
            )

    def add_dataset(self, prefix: Optional[str] = None) -> str:
        """Add a BigQuery dataset to the Observatory environment.

        The BigQuery dataset will be deleted when the Observatory environment is closed.

        :param prefix: an optional additional prefix for the dataset.
        :return: the BigQuery dataset identifier.
        """

        self.assert_gcp_dependencies()
        parts = []
        if self.prefix:
            parts.append(self.prefix)
        if prefix:
            parts.append(prefix)
        parts.append(random_id())
        dataset_id = "_".join(parts)
        self.datasets.append(dataset_id)
        return dataset_id

    def _delete_dataset(self, dataset_id: str) -> None:
        """Delete a BigQuery dataset.

        :param dataset_id: the BigQuery dataset identifier.
        :return: None.
        """

        self.assert_gcp_dependencies()
        try:
            self.bigquery_client.delete_dataset(dataset_id, not_found_ok=True, delete_contents=True)
        except requests.exceptions.ReadTimeout:
            pass

    def add_variable(self, var: Variable) -> None:
        """Add an Airflow variable to the Observatory environment.

        :param var: the Airflow variable.
        :return: None.
        """

        self.session.add(var)
        self.session.commit()

    def add_connection(self, conn: Connection):
        """Add an Airflow connection to the Observatory environment.

        :param conn: the Airflow connection.
        :return: None.
        """

        self.session.add(conn)
        self.session.commit()

    def run_task(self, task_id: str) -> TaskInstance:
        """Run an Airflow task.

        :param task_id: the Airflow task identifier.
        :return: None.
        """

        assert self.dag_run is not None, "with create_dag_run must be called before run_task"

        dag = self.dag_run.dag
        run_id = self.dag_run.run_id
        task = dag.get_task(task_id=task_id)
        ti = TaskInstance(task, run_id=run_id)
        ti.refresh_from_db()
        ti.run(ignore_ti_state=True)

        return ti

    def get_task_instance(self, task_id: str) -> TaskInstance:
        """Get an up-to-date TaskInstance.

        :param task_id: the task id.
        :return: up-to-date TaskInstance instance.
        """

        assert self.dag_run is not None, "with create_dag_run must be called before get_task_instance"

        run_id = self.dag_run.run_id
        task = self.dag_run.dag.get_task(task_id=task_id)
        ti = TaskInstance(task, run_id=run_id)
        ti.refresh_from_db()
        return ti

    @contextlib.contextmanager
    def create_dag_run(
        self,
        dag: DAG,
        execution_date: pendulum.DateTime,
        run_type: DagRunType = DagRunType.SCHEDULED,
    ):
        """Create a DagRun that can be used when running tasks.
        During cleanup the DAG run state is updated.

        :param dag: the Airflow DAG instance.
        :param execution_date: the execution date of the DAG.
        :param run_type: what run_type to use when running the DAG run.
        :return: None.
        """

        # Get start date, which is one schedule interval after execution date
        if isinstance(dag.normalized_schedule_interval, (timedelta, relativedelta)):
            start_date = (
                datetime.fromtimestamp(execution_date.timestamp(), pendulum.tz.UTC) + dag.normalized_schedule_interval
            )
        else:
            start_date = croniter.croniter(dag.normalized_schedule_interval, execution_date).get_next(pendulum.DateTime)

        try:
            self.dag_run = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=execution_date,
                start_date=start_date,
                run_type=run_type,
            )
            yield self.dag_run
        finally:
            self.dag_run.update_state()

    @contextlib.contextmanager
    def create(self, task_logging: bool = False):
        """Make and destroy an Observatory isolated environment, which involves:

        * Creating a temporary directory.
        * Setting the OBSERVATORY_HOME environment variable.
        * Initialising a temporary Airflow database.
        * Creating download and transform Google Cloud Storage buckets.
        * Creating default Airflow Variables: AirflowVars.DATA_PATH,
          AirflowVars.DOWNLOAD_BUCKET and AirflowVars.TRANSFORM_BUCKET.
        * Cleaning up all resources when the environment is closed.

        :param task_logging: display airflow task logging
        :yield: Observatory environment temporary directory.
        """

        with CliRunner().isolated_filesystem() as temp_dir:
            # Set temporary directory
            self.temp_dir = temp_dir

            # Prepare environment
            self.new_env = {self.OBSERVATORY_HOME_KEY: os.path.join(self.temp_dir, ".observatory")}
            prev_env = dict(os.environ)

            try:
                # Update environment
                os.environ.update(self.new_env)

                # Create Airflow SQLite database
                settings.DAGS_FOLDER = os.path.join(self.temp_dir, "airflow", "dags")
                os.makedirs(settings.DAGS_FOLDER, exist_ok=True)
                airflow_db_path = os.path.join(self.temp_dir, "airflow.db")
                settings.SQL_ALCHEMY_CONN = f"sqlite:///{airflow_db_path}"
                logging.info(f"SQL_ALCHEMY_CONN: {settings.SQL_ALCHEMY_CONN}")
                settings.configure_orm(disable_connection_pool=True)
                self.session = settings.Session
                db.initdb()

                # Setup Airflow task logging
                original_log_level = logging.getLogger().getEffectiveLevel()
                if task_logging:
                    # Set root logger to INFO level, it seems that custom 'logging.info()' statements inside a task
                    # come from root
                    logging.getLogger().setLevel(20)
                    # Propagate logging so it is displayed
                    logging.getLogger("airflow.task").propagate = True

                # Create buckets and datasets
                if self.create_gcp_env:
                    for bucket_id in self.buckets:
                        self._create_bucket(bucket_id)

                    for dataset_id in self.datasets:
                        self._create_dataset(dataset_id)

                # Deletes old test buckets and datasets from the project thats older than 2 hours.
                gcs_delete_old_buckets_with_prefix(prefix=self.prefix, age_to_delete=self.age_to_delete)
                bq_delete_old_datasets_with_prefix(prefix=self.prefix, age_to_delete=self.age_to_delete)

                # Add default Airflow variables
                self.data_path = os.path.join(self.temp_dir, "data")
                self.add_variable(Variable(key=AirflowVars.DATA_PATH, val=self.data_path))

                if self.workflows is not None:
                    var = workflows_to_json_string(self.workflows)
                    self.add_variable(Variable(key=AirflowVars.WORKFLOWS, val=var))

                # Reset dag run
                self.dag_run: DagRun = None

                # Create ObservatoryApiEnvironment
                if self.enable_api:
                    # Add Observatory API connection
                    conn = Connection(
                        conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:@{self.api_host}:{self.api_port}"
                    )
                    self.add_connection(conn)

                    # Create API environment
                    self.api_env = ObservatoryApiEnvironment(host=self.api_host, port=self.api_port)
                    with self.api_env.create():
                        self.api_session = self.api_env.session
                        yield self.temp_dir
                else:
                    yield self.temp_dir
            finally:
                # Set logger settings back to original settings
                logging.getLogger().setLevel(original_log_level)
                logging.getLogger("airflow.task").propagate = False

                # Revert environment
                os.environ.clear()
                os.environ.update(prev_env)

                if self.create_gcp_env:
                    # Remove Google Cloud Storage buckets
                    for bucket_id in self.buckets:
                        self._delete_bucket(bucket_id)

                    # Remove BigQuery datasets
                    for dataset_id in self.datasets:
                        self._delete_dataset(dataset_id)


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

    return all_matched


class ObservatoryTestCase(unittest.TestCase):
    """Common test functions for testing Observatory Platform DAGs"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(ObservatoryTestCase, self).__init__(*args, **kwargs)
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

    def assert_dag_load_from_config(self, dag_id: str):
        """Assert that the given DAG loads from a config file.

        :param dag_id: the DAG id.
        :return: None.
        """

        self.assert_dag_load(dag_id, os.path.join(module_file_path("observatory.platform.dags"), "load_workflows.py"))

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


class SftpServer:
    """A Mock SFTP server for testing purposes"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3373,
        level: str = "INFO",
        backlog: int = 10,
        startup_wait_secs: int = 1,
        socket_timeout: int = 10,
    ):
        """Create a Mock SftpServer instance.

        :param host: the host name.
        :param port: the port.
        :param level: the log level.
        :param backlog: ?
        :param startup_wait_secs: time in seconds to wait before returning from create to give the server enough
        time to start before connecting to it.
        """

        self.host = host
        self.port = port
        self.level = level
        self.backlog = backlog
        self.startup_wait_secs = startup_wait_secs
        self.is_shutdown = True
        self.tmp_dir = None
        self.root_dir = None
        self.private_key_path = None
        self.server_thread = None
        self.socket_timeout = socket_timeout

    def _generate_key(self):
        """Generate a private key.

        :return: the filepath to the private key.
        """

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())

        private_key_path = os.path.join(self.tmp_dir, "test_rsa.key")
        with open(private_key_path, "wb") as f:
            f.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        return private_key_path

    def _start_server(self):
        paramiko_level = getattr(paramiko.common, self.level)
        paramiko.common.logging.basicConfig(level=paramiko_level)

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.settimeout(self.socket_timeout)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        server_socket.bind((self.host, self.port))
        server_socket.listen(self.backlog)

        while not self.is_shutdown:
            try:
                conn, addr = server_socket.accept()
                transport = paramiko.Transport(conn)
                transport.add_server_key(paramiko.RSAKey.from_private_key_file(self.private_key_path))
                transport.set_subsystem_handler("sftp", paramiko.SFTPServer, StubSFTPServer)

                server = StubServer()
                transport.start_server(server=server)

                channel = transport.accept()
                while transport.is_active() and not self.is_shutdown:
                    time.sleep(1)

            except socket.timeout:
                # Timeout must be set for socket otherwise it will wait for a connection forever and block
                # the thread from exiting. At: conn, addr = server_socket.accept()
                pass

    @contextlib.contextmanager
    def create(self):
        """Make and destroy a test SFTP server.

        :yield: None.
        """

        with CliRunner().isolated_filesystem() as tmp_dir:
            # Override the root directory of the SFTP server, which is set as the cwd at import time
            self.tmp_dir = tmp_dir
            self.root_dir = os.path.join(tmp_dir, "home")
            os.makedirs(self.root_dir, exist_ok=True)
            StubSFTPServer.ROOT = self.root_dir

            # Generate private key
            self.private_key_path = self._generate_key()

            try:
                self.is_shutdown = False
                self.server_thread = threading.Thread(target=self._start_server)
                self.server_thread.start()

                # Wait a little bit to give the server time to grab the socket
                time.sleep(self.startup_wait_secs)

                yield self.root_dir
            finally:
                # Stop server and wait for server thread to join
                self.is_shutdown = True
                if self.server_thread is not None:
                    self.server_thread.join()


class FtpServer:
    """
    Create a Mock FTPServer instance.

    :param directory: The directory that is hosted on FTP server.
    :param host: Hostname of the server.
    :param port: The port number.
    :param startup_wait_secs: time in seconds to wait before returning from create to give the server enough
    time to start before connecting to it.
    """

    def __init__(
        self,
        directory: str = "/",
        host: str = "localhost",
        port: int = 21,
        startup_wait_secs: int = 1,
        root_username: str = "root",
        root_password: str = "pass",
    ):
        self.host = host
        self.port = port
        self.directory = directory
        self.startup_wait_secs = startup_wait_secs

        self.root_username = root_username
        self.root_password = root_password

        self.is_shutdown = True
        self.server_thread = None

    @contextlib.contextmanager
    def create(self):
        """Make and destroy a test FTP server.

        :yield: self.directory.
        """

        # Set up the FTP server with root and anonymous users.
        authorizer = DummyAuthorizer()
        authorizer.add_user(
            username=self.root_username, password=self.root_password, homedir=self.directory, perm="elradfmwMT"
        )
        authorizer.add_anonymous(self.directory)
        handler = FTPHandler
        handler.authorizer = authorizer

        try:
            # Start server in separate thread.
            self.server = ThreadedFTPServer((self.host, self.port), handler)
            self.server_thread = threading.Thread(target=self.server.serve_forever)
            self.server_thread.daemon = True
            self.server_thread.start()

            # Wait a little bit to give the server time to grab the socket
            time.sleep(self.startup_wait_secs)

            yield self.directory

        finally:
            # Stop server and wait for server thread to join
            self.is_shutdown = True
            if self.server_thread is not None:
                self.server.close_all()
                self.server_thread.join()


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


class HttpServer:
    """Simple HTTP server for testing. Serves files from a directory to http://locahost:port/filename"""

    def __init__(self, directory: str, host: str = "localhost", port: int = None):
        """Initialises the server.

        :param directory: Directory to serve.
        """

        self.directory = directory
        self.process = None

        self.host = host
        if port is None:
            port = find_free_port(host=self.host)
        self.port = port
        self.address = (self.host, self.port)
        self.url = f"http://{self.host}:{self.port}/"

    @staticmethod
    def serve_(address, directory):
        """Entry point for a new process to run HTTP server.

        :param address: Address (host, port) to bind server to.
        :param directory: Directory to serve.
        """

        os.chdir(directory)
        server = ThreadingHTTPServer(address, SimpleHTTPRequestHandler)
        server.serve_forever()

    def start(self):
        """Spin the server up in a new process."""

        # Don't try to start it twice.
        if self.process is not None and self.process.is_alive():
            return

        self.process = Process(
            target=HttpServer.serve_,
            args=(
                self.address,
                self.directory,
            ),
        )
        self.process.start()

    def stop(self):
        """Shutdown the server."""

        if self.process is not None and self.process.is_alive():
            self.process.kill()
            self.process.join()

    @contextlib.contextmanager
    def create(self):
        """Spin up a server for the duration of the session."""
        self.start()

        try:
            yield self.process
        finally:
            self.stop()
