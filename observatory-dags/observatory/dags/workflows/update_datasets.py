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

import logging
import re
from typing import List
from typing import Optional

import google.cloud.bigquery as bigquery
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from pendulum import Pendulum

from observatory.dags.workflows.doi import select_table_suffixes
from observatory.platform.telescopes.telescope import Telescope
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.gc_utils import bigquery_partitioned_table_id, copy_bigquery_table
from observatory.platform.utils.telescope_utils import make_dag_id

MAX_RESULTS = 10000


class UpdateDatasetsRelease:
    def __init__(
        self, src_gcp_project_id: str, dst_gcp_project_id: str, gcp_data_location: str, release_date: Pendulum
    ):
        self.src_gcp_project_id = src_gcp_project_id
        self.dst_gcp_project_id = dst_gcp_project_id
        self.gcp_data_location = gcp_data_location
        self.release_date = release_date


def extract_table_id(table_id: str):
    results = re.search(r"\d{8}$", table_id)

    if results is None:
        return table_id, None

    return table_id[:-8], results.group(0)


class UpdateDatasetsWorkflow(Telescope):
    SENSOR_DAG_IDS = ["doi"]
    DATASET_IDS = [
        "clarivate",
        "coki",
        "crossref",
        "digital_science",
        "elsevier",
        "geonames",
        "iso",
        "mag",
        "open_citations",
        "orcid",
        "our_research",
    ]

    def __init__(
        self,
        *,
        dst_gcp_project_id: str,
        start_date: Optional[Pendulum] = Pendulum(2020, 8, 30),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        airflow_vars: List = None,
    ):
        """ Create the DoiWorkflow.
        :param dst_gcp_project_id: the Google Cloud project that data will be copied to.
        :param start_date: the start date.
        :param schedule_interval: the schedule interval.
        :param catchup: whether to catchup.
        :param airflow_vars: the required Airflow Variables.
        """

        # Make sure that this isn't the academic-observatory project
        assert dst_gcp_project_id != "academic-observatory"

        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION]

        # Initialise Telesecope base class
        super().__init__(
            dag_id=make_dag_id("update_datasets", dst_gcp_project_id),
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
        )
        self.dst_gcp_project_id = dst_gcp_project_id

        # Add sensors
        for ext_dag_id in self.SENSOR_DAG_IDS:
            sensor = ExternalTaskSensor(task_id=f"{ext_dag_id}_sensor", external_dag_id=ext_dag_id, mode="reschedule")
            self.add_sensor(sensor)

        # Add tasks
        self.add_setup_task(self.check_dependencies)
        self.add_task(self.delete_datasets)
        self.add_task(self.create_datasets)
        self.add_task(self.copy_latest_tables)

    def make_release(self, **kwargs):
        """Make a release instance. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.
        :param kwargs: the context passed from the Airflow Operator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None
        """

        src_gcp_project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        return UpdateDatasetsRelease(src_gcp_project_id, self.dst_gcp_project_id, data_location, release_date)

    def delete_datasets(self, release: UpdateDatasetsRelease, **kwargs):
        """ Delete all datasets and their contents in the project.

        :param release: None.
        :param kwargs: the context passed from the Airflow Operator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        client = bigquery.Client(project=release.dst_gcp_project_id)
        for dataset_id in self.DATASET_IDS:
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    def create_datasets(self, release: UpdateDatasetsRelease, **kwargs):
        """ Create all datasets in the project, with no contents.

        :param release: None.
        :param kwargs: the context passed from the Airflow Operator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        client = bigquery.Client(project=release.dst_gcp_project_id)
        for dataset_id in self.DATASET_IDS:
            client.create_dataset(dataset_id, exists_ok=True)

    def copy_latest_tables(self, release: UpdateDatasetsRelease, **kwargs):
        """ Copy the latest tables to the new datasets.

        :param release: None.
        :param kwargs: the context passed from the Airflow Operator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        src_client = bigquery.Client(project=release.src_gcp_project_id)

        for dataset_id in self.DATASET_IDS:
            # Find all tables in the dataset, split into non-sharded and sharded
            table_ids = set()
            table_ids_sharded = set()
            tables = src_client.list_tables(dataset_id, max_results=MAX_RESULTS)
            for table in tables:
                table_id, shard_date = extract_table_id(table.table_id)
                if shard_date is not None:
                    table_ids_sharded.add(table_id)
                else:
                    table_ids.add(table_id)

            # For all sharded tables, find the latest version and add it to table_ids
            for table_id in table_ids_sharded:
                table_dates = select_table_suffixes(
                    release.src_gcp_project_id, dataset_id, table_id, release.release_date
                )
                table_ids.add(bigquery_partitioned_table_id(table_id, table_dates[0]))

            # Copy tables to dst project
            results = []
            for table_id in table_ids:
                src_table_id = f"{release.src_gcp_project_id}.{dataset_id}.{table_id}"
                dst_table_id = f"{release.dst_gcp_project_id}.{dataset_id}.{table_id}"
                success = copy_bigquery_table(src_table_id, dst_table_id, release.gcp_data_location)
                if not success:
                    logging.error(f"Issue copying table: {src_table_id} to {dst_table_id}")
                results.append(success)

            if not all(results):
                raise AirflowException("Problem copying tables")
