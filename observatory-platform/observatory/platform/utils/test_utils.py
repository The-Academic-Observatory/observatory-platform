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
import logging
import os
import shutil
import socket
import threading
import time
import unittest
import uuid
from functools import partial
from typing import Dict

import croniter
import datetime
import httpretty
import paramiko
import pendulum
from airflow import settings
from airflow.models import DagBag
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.utils import db
from airflow.utils.state import State
from click.testing import CliRunner
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from freezegun import freeze_time
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from sftpserver.stub_sftp import StubServer, StubSFTPServer
from unittest.mock import patch

from observatory.api.testing import ObservatoryApiEnvironment
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.file_utils import _hash_file, crc32c_base64_hash, gzip_file_crc
from observatory.platform.utils.template_utils import reset_variables


def random_id():
    """Generate a random id for bucket name.

    :return: a random string id.
    """
    return str(uuid.uuid4()).replace("-", "")


def test_fixtures_path(*subdirs) -> str:
    """Get the path to the Observatory Platform test data directory.

    :return: he Observatory Platform test data directory.
    """

    base_path = module_file_path("tests.fixtures")
    return os.path.join(base_path, *subdirs)


class ObservatoryEnvironment:
    OBSERVATORY_HOME_KEY = "OBSERVATORY_HOME"

    def __init__(
        self, project_id: str = None, data_location: str = None, api_host: str = "localhost", api_port: int = 5000
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
        self.dag_run: DagRun = None

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

    def add_bucket(self) -> str:
        """Add a Google Cloud Storage Bucket to the Observatory environment.

        The bucket will be created when create() is called and deleted when the Observatory
        environment is closed.

        :return: returns the bucket name.
        """

        self.assert_gcp_dependencies()
        bucket_name = random_id()
        self.buckets.append(bucket_name)
        return bucket_name

    def _create_bucket(self, bucket_id: str) -> None:
        """Create a Google Cloud Storage Bucket.

        :param bucket_id: the bucket identifier.
        :return: None.
        """

        self.assert_gcp_dependencies()
        self.storage_client.create_bucket(bucket_id, location=self.data_location)

    def _delete_bucket(self, bucket_id: str) -> None:
        """Delete a Google Cloud Storage Bucket.

        :param bucket_id: the bucket identifier.
        :return: None.
        """

        self.assert_gcp_dependencies()
        bucket = self.storage_client.get_bucket(bucket_id)
        bucket.delete(force=True)

    def add_dataset(self) -> str:
        """Add a BigQuery dataset to the Observatory environment.

        The BigQuery dataset will be deleted when the Observatory environment is closed.

        :return: the BigQuery dataset identifier.
        """

        self.assert_gcp_dependencies()
        dataset_id = random_id()
        self.datasets.append(dataset_id)
        return dataset_id

    def _delete_dataset(self, dataset_id: str) -> None:
        """Delete a BigQuery dataset.

        :param dataset_id: the BigQuery dataset identifier.
        :return: None.
        """

        self.assert_gcp_dependencies()
        self.bigquery_client.delete_dataset(dataset_id, not_found_ok=True, delete_contents=True)

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

    def run_task(self, task_id: str, dag: DAG = None, execution_date: pendulum.Pendulum = None) -> TaskInstance:
        """Run an Airflow task.

        :param dag: the Airflow DAG instance.
        :param task_id: the Airflow task identifier.
        :param execution_date: the execution date of the DAG.
        :return: None.
        """
        # If dag or execution date are not set, get corresponding values from dag run
        if not (dag and execution_date):
            if self.dag_run is None:
                raise TypeError(
                    "Either dag and execution date should be set, or task should be run within a DagRun environment."
                )
            dag = self.dag_run.dag
            execution_date = self.dag_run.execution_date
        task = dag.get_task(task_id=task_id)
        ti = TaskInstance(task, execution_date)
        ti.refresh_from_db()
        ti.init_run_context(raw=True)
        ti.run()
        return ti

    @contextlib.contextmanager
    def create_dag_run(self, dag: DAG, execution_date: pendulum.Pendulum, freeze: bool = True):
        """Create a DagRun that can be used when running tasks.
        During cleanup the DAG run state is updated.

        :param dag: the Airflow DAG instance.
        :param execution_date: the execution date of the DAG.
        :param freeze: whether to freeze time to the start date of the DAG run.
        :return: None.
        """
        # Get start date, which is one schedule interval after execution date
        start_date = croniter.croniter(dag.normalized_schedule_interval, execution_date).get_next(datetime.datetime)
        frozen_time = freeze_time(start_date, tick=True)

        run_id = "manual__{0}".format(execution_date.isoformat())

        # Make sure google auth uses real datetime and not freezegun fake time
        with patch("google.auth._helpers.utcnow", wraps=datetime.datetime.utcnow) as mock_utc_now:
            try:
                if freeze:
                    frozen_time.start()
                state = State.RUNNING
                self.dag_run = dag.create_dagrun(run_id=run_id, state=state, execution_date=execution_date)
                yield
            finally:
                self.dag_run.update_state()
                if freeze:
                    frozen_time.stop()

    @contextlib.contextmanager
    def create(self):
        """Make and destroy an Observatory isolated environment, which involves:

        * Creating a temporary directory.
        * Setting the OBSERVATORY_HOME environment variable.
        * Initialising a temporary Airflow database.
        * Creating download and transform Google Cloud Storage buckets.
        * Creating default Airflow Variables: AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
          AirflowVars.DOWNLOAD_BUCKET and AirflowVars.TRANSFORM_BUCKET.
        * Cleaning up all resources when the environment is closed.

        :yield: Observatory environment temporary directory.
        """

        with CliRunner().isolated_filesystem() as temp_dir:
            # Set temporary directory
            self.temp_dir = temp_dir

            # Reset Airflow variables
            reset_variables()

            # Prepare environment
            new_env = {self.OBSERVATORY_HOME_KEY: os.path.join(self.temp_dir, ".observatory")}
            prev_env = dict(os.environ)

            try:
                # Update environment
                os.environ.update(new_env)

                # Create Airflow SQLite database
                settings.DAGS_FOLDER = os.path.join(self.temp_dir, "airflow", "dags")
                os.makedirs(settings.DAGS_FOLDER, exist_ok=True)
                airflow_db_path = os.path.join(self.temp_dir, "airflow.db")
                settings.SQL_ALCHEMY_CONN = f"sqlite:///{airflow_db_path}"
                logging.info(f"SQL_ALCHEMY_CONN: {settings.SQL_ALCHEMY_CONN}")
                settings.configure_orm(disable_connection_pool=True)
                self.session = settings.Session
                db.initdb()

                # Create buckets
                if self.create_gcp_env:
                    for bucket_id in self.buckets:
                        self._create_bucket(bucket_id)

                # Add default Airflow variables
                self.data_path = os.path.join(self.temp_dir, "data")
                self.add_variable(Variable(key=AirflowVars.DATA_PATH, val=self.data_path))

                # Add Google Cloud environment related Airflow variables
                if self.create_gcp_env:
                    self.add_variable(Variable(key=AirflowVars.PROJECT_ID, val=self.project_id))
                    self.add_variable(Variable(key=AirflowVars.DATA_LOCATION, val=self.data_location))
                    self.add_variable(Variable(key=AirflowVars.DOWNLOAD_BUCKET, val=self.download_bucket))
                    self.add_variable(Variable(key=AirflowVars.TRANSFORM_BUCKET, val=self.transform_bucket))

                # Create ObservatoryApiEnvironment
                self.api_env = ObservatoryApiEnvironment(host=self.api_host, port=self.api_port)
                with self.api_env.create():
                    self.api_session = self.api_env.session
                    yield self.temp_dir
            finally:
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


class ObservatoryTestCase(unittest.TestCase):
    """ Common test functions for testing Observatory Platform DAGs """

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
        logging.getLogger().setLevel(logging.WARNING)

    def assert_dag_structure(self, expected: Dict, dag: DAG):
        """Assert the DAG structure.

        :param expected: a dictionary of DAG task ids as keys and values which should be a list of downstream task ids.
        :param dag: the DAG.
        :return: None.
        """

        self.assertEqual(expected.keys(), dag.task_dict.keys())

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
            if os.path.exists(dag_file):
                shutil.copy(dag_file, os.path.join(dag_folder, os.path.basename(dag_file)))
            dag_bag = DagBag(dag_folder=dag_folder)
            dag = dag_bag.get_dag(dag_id=dag_id)
            self.assertEqual({}, dag_bag.import_errors)
            self.assertIsNotNone(dag)
            self.assertGreaterEqual(len(dag.tasks), 1)

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

    def assert_table_integrity(self, table_id: str, expected_rows: int):
        """Assert whether a BigQuery table exists and has the expected number of rows.

        :param table_id: the BigQuery table id.
        :param expected_rows: the expected number of rows.
        :return: whether the table exists and has the expected number of rows.
        """

        result = False
        try:
            table = self.bigquery_client.get_table(table_id)
            result = expected_rows == table.num_rows
        except NotFound:
            pass

        self.assertTrue(result)

    def assert_file_integrity(self, file_path: str, expected_hash: str, algorithm: str):
        """Assert that a file exists and it has the correct hash.

        :param file_path: the path to the file.
        :param expected_hash: the expected hash.
        :param algorithm: the algorithm to use when hashing, either md5 or gzip crc
        :return: None.
        """

        self.assertTrue(os.path.isfile(file_path))

        if algorithm == "md5":
            hash_func = partial(_hash_file, algorithm="md5")
        elif algorithm == "gzip_crc":
            hash_func = gzip_file_crc
        else:
            raise ValueError(f"Unknown hash algorithm: {algorithm}")

        actual_hash = hash_func(file_path)
        self.assertEqual(expected_hash, actual_hash)

    def assert_cleanup(self, download_folder: str, extract_folder: str, transform_folder: str):
        """Assert that the download, extracted and transformed folders were cleaned up.

        :param download_folder: the path to the DAGs download folder.
        :param extract_folder: the path to the DAGs extract folder.
        :param transform_folder: the path to the DAGs transform folder.
        :return: None.
        """

        self.assertFalse(os.path.exists(download_folder))
        self.assertFalse(os.path.exists(extract_folder))
        self.assertFalse(os.path.exists(transform_folder))

    def setup_mock_file_download(self, uri: str, file_path: str, headers: Dict = None, method: str = httpretty.GET) -> \
            None:
        """ Use httpretty to mock a file download.

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

        httpretty.register_uri(method, uri,
                               adding_headers=headers,
                               body=body)


class SftpServer:
    """ A Mock SFTP server for testing purposes """

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
