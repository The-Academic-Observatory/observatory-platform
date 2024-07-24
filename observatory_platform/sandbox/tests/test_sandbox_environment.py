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
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.models.connection import Connection
from airflow.models.dag import ScheduleArg
from airflow.models.variable import Variable
from airflow.timetables.base import DataInterval
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
    schedule: ScheduleArg = "@weekly",
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
        schedule_interval=schedule,
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
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        my_dag = create_dag()

        # Test that previous tasks have to be finished to run next task
        env = SandboxEnvironment(self.project_id, self.data_location)

        with env.create(task_logging=True):
            with env.create_dag_run(my_dag, execution_date):
                # Add_variable
                env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

                # Add connection
                conn = Connection(
                    conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2"
                )
                env.add_connection(conn)

                # Test run task when dependencies are not met
                ti = env.run_task("task2")
                self.assertIsNone(ti.state)

                # Try again when dependencies are met
                ti = env.run_task("check_dependencies")
                self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

                ti = env.run_task("task2")
                self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

                ti = env.run_task("task3")
                self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

    def test_task_logging(self):
        """Test task logging"""

        env = SandboxEnvironment(self.project_id, self.data_location)

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=11, day=1)
        my_dag = create_dag()

        # Test environment without logging enabled
        with env.create():
            with env.create_dag_run(my_dag, execution_date):
                # Test add_variable
                env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

                # Test add_connection
                conn = Connection(
                    conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2"
                )
                env.add_connection(conn)

                # Test run task
                ti = env.run_task("check_dependencies")
                self.assertFalse(ti.log.propagate)
                self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

        # Test environment with logging enabled
        env = SandboxEnvironment(self.project_id, self.data_location)
        with env.create(task_logging=True):
            with env.create_dag_run(my_dag, execution_date):
                # Test add_variable
                env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

                # Test add_connection
                conn = Connection(
                    conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2"
                )
                env.add_connection(conn)

                # Test run task
                ti = env.run_task("check_dependencies")
                self.assertTrue(ti.log.propagate)
                self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

    def test_create_dagrun(self):
        """Tests create_dag_run"""

        env = SandboxEnvironment(self.project_id, self.data_location)

        # Setup Telescope
        first_execution_date = pendulum.datetime(year=2020, month=11, day=1, tz="UTC")  # Sunday
        second_execution_date = pendulum.datetime(year=2020, month=12, day=1, tz="UTC")  # Tuesday
        third_data_interval = DataInterval(
            pendulum.datetime(year=2021, month=1, day=1, tz="UTC"),
            pendulum.datetime(year=2021, month=1, day=3, tz="UTC"),
        )
        my_dag = create_dag()

        # Use DAG run with freezing time
        with env.create():
            # Test add_variable
            env.add_variable(Variable(key=MY_VAR_ID, val="hello"))

            # Test add_connection
            conn = Connection(conn_id=MY_CONN_ID, uri="mysql://login:password@host:8080/schema?param1=val1&param2=val2")
            env.add_connection(conn)

            self.assertIsNone(env.dag_run)
            # First DAG Run
            with env.create_dag_run(my_dag, first_execution_date):
                # Test DAG Run is set and has frozen start date
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(first_execution_date.date(), env.dag_run.start_date.date())
                self.assertEqual(env.dag_run.data_interval_start.date(), first_execution_date.date())
                self.assertEqual(env.dag_run.data_interval_end.date(), first_execution_date.date() + timedelta(days=7))

                ti1 = env.run_task("check_dependencies")
                self.assertEqual(TaskInstanceState.SUCCESS, ti1.state)
                self.assertIsNone(ti1.previous_ti)

            # Second DAG Run
            with env.create_dag_run(my_dag, second_execution_date):
                # Test DAG Run is set and has frozen start date
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(second_execution_date.date(), env.dag_run.start_date.date())
                self.assertEqual(env.dag_run.data_interval_start.date(), second_execution_date.date())
                self.assertEqual(env.dag_run.data_interval_end.date(), second_execution_date.date() + timedelta(days=5))

                ti2 = env.run_task("check_dependencies")
                self.assertEqual(TaskInstanceState.SUCCESS, ti2.state)
                # Test previous ti is set
                self.assertEqual(ti1.job_id, ti2.previous_ti.job_id)

            # Third DAG Run
            with env.create_dag_run(my_dag, data_interval=third_data_interval):
                # Test DAG Run is set and has frozen start date
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(third_data_interval.start, env.dag_run.data_interval_start)
                self.assertEqual(third_data_interval.end, env.dag_run.data_interval_end)

                ti3 = env.run_task("check_dependencies")
                self.assertEqual(TaskInstanceState.SUCCESS, ti3.state)
                # Test previous ti is set
                self.assertEqual(ti2.job_id, ti3.previous_ti.job_id)

    def test_create_dag_run_timedelta(self):
        env = SandboxEnvironment(self.project_id, self.data_location)

        my_dag = create_dag(schedule=timedelta(days=1))
        execution_date = pendulum.datetime(2021, 1, 1)
        with env.create():
            with env.create_dag_run(my_dag, execution_date):
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(execution_date, env.dag_run.start_date)
                execution_date = env.dag_run.data_interval_end

            expected_dag_date = pendulum.datetime(2021, 1, 2)
            with env.create_dag_run(my_dag, execution_date):
                self.assertIsNotNone(env.dag_run)
                self.assertEqual(expected_dag_date, env.dag_run.start_date)

    def test_create_dag_run_raises_error(self):
        env = SandboxEnvironment(self.project_id, self.data_location)

        my_dag = create_dag(schedule=timedelta(days=1))
        with env.create():
            with self.assertRaisesRegex(ValueError, "Must provide one of"):
                with env.create_dag_run(my_dag):
                    pass

    def test_map_index(self):
        env = SandboxEnvironment(self.project_id, self.data_location)
        logical_date = pendulum.datetime(2024, 1, 1)
        my_dag = create_dynamic_task_dag(dag_id="dynamic_task_dag", start_date=logical_date)
        with env.create():
            with env.create_dag_run(my_dag, logical_date):
                self.assertIsNotNone(env.dag_run)
                ti = env.run_task("fetch_releases")
                self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

                for map_index in range(2):
                    ti = env.run_task("process_release.download", map_index=map_index)
                    self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

                    ti = env.run_task("process_release.bq_load", map_index=map_index)
                    self.assertEqual(TaskInstanceState.SUCCESS, ti.state)
