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

# Author: James Diprose, Aniek Roelofs

from __future__ import annotations

import logging
import os
import unittest
from unittest.mock import patch

import pendulum
from airflow.sdk import dag, task, task_group, Connection, Variable
from airflow.models import DagRun
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import TaskInstanceState
from google.cloud.exceptions import NotFound

from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.config import AirflowVars
from observatory_platform.google.bigquery import bq_create_dataset
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import random_id

DAG_ID = "dag-test"
MY_VAR_ID = "my-variable"
MY_CONN_ID = "my-connection"


def create_dag(
    dag_id: str = DAG_ID,
    start_date: pendulum.DateTime = pendulum.datetime(2020, 9, 1, tz="UTC"),
    schedule="@weekly",
):
    # Define the DAG (workflow)
    @dag(dag_id=dag_id, schedule=schedule, start_date=start_date)
    def my_dag():
        @task()
        def task2():
            logging.info("task 2!")

        @task()
        def task3():
            logging.info("task 3!")

        t1 = check_dependencies(
            airflow_vars=[
                AirflowVars.DATA_PATH,
                MY_VAR_ID,
            ],
            airflow_conns=[MY_CONN_ID],
        )
        t2 = task2()
        t3 = task3()
        t1 >> t2 >> t3

    return my_dag()


def create_dynamic_task_dag(
    *,
    dag_id: str,
    start_date: pendulum.DateTime,
    schedule: str = "@weekly",
    catchup: bool = False,
):
    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=["example_tag"],
    )
    def example_workflow():
        @task
        def fetch_releases(**context):
            releases = [0, 1]
            if not releases:
                raise AirflowSkipException("No new releases found, skipping")
            return releases

        @task_group(group_id="process_release")
        def process_release(data, **context):
            @task
            def download(release: dict, **context):
                print(f"Downloading {release}")

            @task
            def bq_load(release: dict, **context):
                print(f"Loading to BigQuery {release}")

            # Connects tasks
            download(data) >> bq_load(data)

        # Fetches releases
        xcom_releases = fetch_releases()

        # Using `.expand()` to dynamically create tasks for each release
        process_release_task_group = process_release.expand(data=xcom_releases)

        (xcom_releases >> process_release_task_group)

    return example_workflow()


class TestSandboxEnvironment(unittest.TestCase):
    """Test the SandboxEnvironment"""

    def __init__(self, *args, **kwargs):
        super(TestSandboxEnvironment, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_add_variable_uppsercased(self):
        """Test the add_variable method properly uppercases an input"""
        env = SandboxEnvironment(self.project_id, self.data_location)
        var = Variable(key="mIxEdCaSe", value="v")

        with patch.object(env, "_set_env_var") as mock_set:
            env.add_variable(var)

        env._release_env_vars()  # remove env vars from environment
        mock_set.assert_called_once_with("AIRFLOW_VAR_MIXED_CASE", "v")

    def test_add_variable_sets_env(self):
        """Test the add_variable method adds variable to os.environ"""
        env = SandboxEnvironment(self.project_id, self.data_location)
        var = Variable(key="k", value="v")

        env.add_variable(var)

        self.assertEqual(os.environ.get("AIRFLOW_VAR_K", "v"))
        env._release_env_vars()  # remove env vars from environment

    def test_add_connection_uppercased(self):
        """Test the add_connection method properly uppercases an input"""
        env = SandboxEnvironment(self.project_id, self.data_location)
        conn = Connection(conn_id="mIxEdCaSe", conn_type="http", host="example.com")

        with patch.object(env, "_set_env_var") as mock_set:
            env.add_conenction(conn)

        env._release_env_vars()  # remove env vars from environment
        mock_set.assert_called_once_with("AIRFLOW_CONN_MIXED_CASE", conn.get_uri())

    def test_add_connection_sets_env(self):
        """Test the add_connection method adds connetion to os.environ"""
        env = SandboxEnvironment(self.project_id, self.data_location)
        conn = Connection(conn_id="foo", conn_type="http", host="example.com")

        env.add_connection(conn)

        self.assertEqual(os.environ.get("AIRFLOW_CONN_FOO", conn.get_uri()))
        env._release_env_vars()  # remove env vars from environment

    def test_add_bucket(self):
        """Test the add_bucket method"""

        env = SandboxEnvironment(self.project_id, self.data_location)

        # The download and transform buckets are added in the constructor
        buckets = list(env.buckets.keys())
        self.assertEqual(2, len(buckets))
        self.assertEqual(env.download_bucket, buckets[0])
        self.assertEqual(env.transform_bucket, buckets[1])

        # Test that calling add bucket adds a new bucket to the buckets list
        name = env.add_bucket()
        buckets = list(env.buckets.keys())
        self.assertEqual(name, buckets[-1])

        # No Google Cloud variables raises error
        with self.assertRaises(AssertionError):
            SandboxEnvironment().add_bucket()

    def test_create_delete_bucket(self):
        """Test _create_bucket and _delete_bucket"""

        env = SandboxEnvironment(self.project_id, self.data_location)

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

        # Test create a bucket with a set of roles
        roles = {"roles/storage.objectViewer", "roles/storage.legacyBucketWriter"}
        env._create_bucket(bucket_id, roles=roles)
        bucket = env.storage_client.bucket(bucket_id)
        bucket_policy = bucket.get_iam_policy()
        for role in roles:
            self.assertTrue({"role": role, "members": {"allUsers"}} in bucket_policy)

        # No Google Cloud variables raises error
        bucket_id = "obsenv_tests_" + random_id()
        with self.assertRaises(AssertionError):
            SandboxEnvironment()._create_bucket(bucket_id)
        with self.assertRaises(AssertionError):
            SandboxEnvironment()._delete_bucket(bucket_id)

    def test_add_delete_dataset(self):
        """Test add_dataset and _delete_dataset"""

        # Create dataset
        env = SandboxEnvironment(self.project_id, self.data_location)

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
            SandboxEnvironment().add_dataset()
        with self.assertRaises(AssertionError):
            SandboxEnvironment()._delete_dataset(random_id())

    def test_create(self):
        """Tests create, add_variable, add_connection and run_task"""

        # Setup Telescope
        my_dag = create_dag()

        # Test that previous tasks have to be finished to run next task
        env = SandboxEnvironment(self.project_id, self.data_location)

        with env.create(task_logging=True):
            env.serialize_dag(my_dag)
            # Add_variable
            env.add_variable(Variable(key=MY_VAR_ID, value="hello"))

            # Add connection
            conn = Connection(conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2")
            env.add_connection(conn)

            dagrun: DagRun = my_dag.test()

            # Test run task when dependencies are not met
            ti = dagrun.get_task_instance(task_id="task2")
            self.assertIsNone(ti.state)

            # Try again when dependencies are met
            ti = dagrun.get_task_instance(task_id="check_dependencies")
            self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

            ti = dagrun.get_task_instance(task_id="task2")
            self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

            ti = dagrun.get_task_instance(task_id="task3")
            self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

    def test_task_logging(self):
        """Test task logging"""

        env = SandboxEnvironment(self.project_id, self.data_location)

        # Setup Telescope
        my_dag = create_dag()

        # Test environment without logging enabled
        with env.create():
            env.serialize_dag(my_dag)

            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, value="hello"))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2")
            env.add_connection(conn)

            # Test run task
            dag_run = my_dag.test()
            ti = dag_run.get_task_instance(task_id="check_dependencies")
            self.assertFalse(ti.log.propagate)
            self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

        # Test environment with logging enabled
        env = SandboxEnvironment(self.project_id, self.data_location)
        with env.create(task_logging=True):
            env.serialize_dag(my_dag)
            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, value="hello"))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2")
            env.add_connection(conn)

            # Test run task
            dag_run = my_dag.test()
            ti = dag_run.get_task_instance(task_id="check_dependencies")
            self.assertTrue(ti.log.propagate)
            self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

    def test_map_index(self):
        env = SandboxEnvironment(self.project_id, self.data_location)
        logical_date = pendulum.datetime(2024, 1, 1)
        my_dag = create_dynamic_task_dag(dag_id="dynamic_task_dag", start_date=logical_date)
        with env.create():
            env.serialize_dag(my_dag)
            dag_run = my_dag.test()

            ti = dag_run.get_task_instance(task_id="fetch_releases")
            self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

            for map_index in range(2):
                ti = dag_run.get_task_instance(task_id="process_release.download", map_index=map_index)
                self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

                ti = dag_run.get_task_instance(task_id="process_release.bq_load", map_index=map_index)
                self.assertEqual(TaskInstanceState.SUCCESS, ti.state)
