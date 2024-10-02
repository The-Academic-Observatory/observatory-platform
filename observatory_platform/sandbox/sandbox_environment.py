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
import logging
import os
from typing import List, Optional, Set, Union

from airflow import DAG, settings
from airflow.models.connection import Connection
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.timetables.base import DataInterval
from airflow.utils import db
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from click.testing import CliRunner
import google
from google.cloud import bigquery, storage
import pendulum
import requests

from observatory_platform.airflow.workflow import Workflow, CloudWorkspace, workflows_to_json_string
from observatory_platform.config import AirflowVars
from observatory_platform.google.bigquery import bq_delete_old_datasets_with_prefix
from observatory_platform.google.gcs import gcs_delete_old_buckets_with_prefix
from observatory_platform.sandbox.test_utils import random_id


class SandboxEnvironment:
    OBSERVATORY_HOME_KEY = "OBSERVATORY_HOME"

    def __init__(
        self,
        project_id: str = None,
        data_location: str = None,
        prefix: Optional[str] = "obsenv_tests",
        age_to_delete: int = 12,
        workflows: List[Workflow] = None,
        gcs_bucket_roles: Union[Set[str], str] = None,
        env_vars: dict = None,
    ):
        """Constructor for an Observatory environment.

        To create an Observatory environment:
        env = SandboxEnvironment()
        with env.create():
            pass

        :param project_id: the Google Cloud project id.
        :param data_location: the Google Cloud data location.
        :param prefix: prefix for buckets and datsets created for the testing environment.
        :param age_to_delete: age of buckets and datasets to delete that share the same prefix, in hours
        :param env_vars: Any environment variables to set upon environment creation.
        """

        self.project_id = project_id
        self.data_location = data_location
        self.buckets = {}
        self.datasets = []
        self.data_path = None
        self.session = None
        self.temp_dir = None
        self.api_env = None
        self.api_session = None
        self.dag_run: DagRun = None
        self.prefix = prefix
        self.age_to_delete = age_to_delete
        self.workflows = workflows
        self.env_vars = env_vars

        if self.create_gcp_env:
            self.download_bucket = self.add_bucket(roles=gcs_bucket_roles)
            self.transform_bucket = self.add_bucket(roles=gcs_bucket_roles)
            self.storage_client = storage.Client()
            self.bigquery_client = bigquery.Client()
        else:
            self.download_bucket = None
            self.transform_bucket = None
            self.storage_client = None
            self.bigquery_client = None

    @property
    def cloud_workspace(self) -> CloudWorkspace:
        """Return a default CloudWorkspace instance constructed from the sandbox project_id, download_bucket,
        transform_bucket and data_location variables.

        :return: the CloudWorkspace instance.
        """

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

    def add_bucket(self, prefix: Optional[str] = None, roles: Optional[Union[Set[str], str]] = None) -> str:
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
            self.buckets[bucket_name] = roles

        return bucket_name

    def _create_bucket(self, bucket_id: str, roles: Optional[Union[str, Set[str]]] = None) -> None:
        """Create a Google Cloud Storage Bucket.

        :param bucket_id: the bucket identifier.
        :param roles: Create bucket with custom roles if required.
        :return: None.
        """

        self.assert_gcp_dependencies()
        bucket = self.storage_client.create_bucket(bucket_id, location=self.data_location)
        logging.info(f"Created bucket with name: {bucket_id}")

        if roles:
            roles = set(roles) if isinstance(roles, str) else roles

            # Get policy of bucket and add roles.
            policy = bucket.get_iam_policy()
            for role in roles:
                policy.bindings.append({"role": role, "members": {"allUsers"}})
            bucket.set_iam_policy(policy)
            logging.info(f"Added permission {role} to bucket {bucket_id} for allUsers.")

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

    def run_task(self, task_id: str, map_index: int = -1) -> TaskInstance:
        """Run an Airflow task.

        :param task_id: the Airflow task identifier.
        :param map_index: the map index if the task is a daynamic task
        :return: None.
        """

        assert self.dag_run is not None, "with create_dag_run must be called before run_task"

        dag = self.dag_run.dag
        run_id = self.dag_run.run_id
        task = dag.get_task(task_id=task_id)
        ti = TaskInstance(task, run_id=run_id, map_index=map_index)
        ti.refresh_from_db()

        # TODO: remove this when this issue fixed / PR merged: https://github.com/apache/airflow/issues/34023#issuecomment-1705761692
        # https://github.com/apache/airflow/pull/36462
        ignore_task_deps = False
        if map_index > -1:
            ignore_task_deps = True

        ti.run(ignore_task_deps=ignore_task_deps)

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
        logical_date: pendulum.DateTime = None,
        data_interval: DataInterval = None,
        run_type: DagRunType = DagRunType.SCHEDULED,
    ):
        """Create a DagRun that can be used when running tasks.
        During cleanup the DAG run state is updated.

        :param dag: the Airflow DAG instance.
        :param logical_date: the logical date of the DAG.
        :param run_type: what run_type to use when running the DAG run.
        :return: None.
        """

        if data_interval:
            start_date = data_interval.start
            if not logical_date:
                logical_date = data_interval.start
        elif logical_date:
            data_interval = dag.infer_automated_data_interval(logical_date=logical_date)
            start_date = data_interval.start
        else:
            raise ValueError("Must provide one of `data_inerval` or `logical_date`")

        try:
            self.dag_run = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=logical_date,
                start_date=start_date,
                run_type=run_type,
                data_interval=data_interval,
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
                if self.env_vars:
                    os.environ.update(self.env_vars)

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
                else:
                    logging.getLogger("airflow.task").propagate = False

                # Create buckets and datasets
                if self.create_gcp_env:
                    for bucket_id, roles in self.buckets.items():
                        self._create_bucket(bucket_id, roles)

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
                    for bucket_id, roles in self.buckets.items():
                        self._delete_bucket(bucket_id)

                    # Remove BigQuery datasets
                    for dataset_id in self.datasets:
                        self._delete_dataset(dataset_id)
