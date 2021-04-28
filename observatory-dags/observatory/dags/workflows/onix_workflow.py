# Copyright 2020 Curtin University
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
#
#
# Author: Tuan Chien

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pendulum
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from google.cloud.bigquery import SourceFormat
from jinja2 import Environment, PackageLoader
from observatory.dags.telescopes.onix import OnixTelescope
from observatory.dags.workflows.onix_work_aggregation import (
    BookWorkAggregator,
    BookWorkFamilyAggregator,
)
from observatory.platform.telescopes.telescope import AbstractRelease, Telescope
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.gc_utils import (
    run_bigquery_query,
    select_table_suffixes,
    upload_files_to_cloud_storage,
)
from observatory.platform.utils.telescope_utils import (
    list_to_jsonl_gz,
    make_dag_id,
    write_to_file,
)
from observatory.platform.utils.template_utils import (
    blob_name,
    bq_load_shard_v2,
    table_ids_from_path,
    upload_files_from_list,
)


class OnixWorkflowRelease(AbstractRelease):
    """
    Release information for OnixWorkflow.
    """

    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.Pendulum,
        gcp_project_id: str,
        onix_dataset_id: str = "onix",
        onix_table_id: str = "onix",
        gcp_bucket_name: str,
    ):
        """
        :param dag_id: DAG ID.
        :param release_date: The release date. It's the current execution date.
        :param gcp_project_id: GCP Project ID.
        :param gcp_bucket_name: GCP bucket name.
        :param onix_dataset_id: GCP dataset ID for the onix data.
        :param onix_table_id: GCP table ID for the onix data.
        """

        self.dag_id = dag_id
        self.release_date = release_date

        # Prepare filesystem
        self.worksid_table = "onix_workid_isbn"
        self.worksid_error_table = "onix_workid_isbn_errors"
        self.workfamilyid_table = "onix_workfamilyid_isbn"

        self.workslookup_filename = os.path.join(self.transform_folder, f"{self.worksid_table}.jsonl.gz")
        self.workslookup_errors_filename = os.path.join(self.transform_folder, f"{self.worksid_error_table}.jsonl.gz")
        self.worksfamilylookup_filename = os.path.join(self.transform_folder, f"{self.workfamilyid_table}.jsonl.gz")

        # GCP parameters
        self.workflow_dataset_id = "onix_workflow"
        self.project_id = gcp_project_id
        self.onix_dataset_id = onix_dataset_id
        self.dataset_location = "us"
        self.dataset_description = "ONIX workflow tables"

        # ONIX release info
        self.onix_table_id = onix_table_id
        self.bucket_name = gcp_bucket_name

        Path(".", self.transform_folder).mkdir(exist_ok=True, parents=True)

    @property
    def transform_bucket(self) -> str:
        """
        :return: The transform bucket name.
        """
        return self.bucket_name

    @property
    def transform_folder(self) -> str:
        """The transform folder path. This is the folder hierarchy locally and on GCP.
        :return: The transform folder path.
        """

        folder = os.path.join(str(self.dag_id), self.release_date.strftime("%Y%m%d"))

        return folder

    @property
    def transform_files(self) -> List[str]:
        """
        :return: List of transformed files.
        """
        return [self.workslookup_filename, self.workslookup_errors_filename, self.worksfamilylookup_filename]

    @property
    def download_bucket(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    @property
    def download_files(self) -> List[str]:
        """Not used.
        :return: Empty list.
        """
        return list()

    @property
    def extract_files(self) -> List[str]:
        """Not used.
        :return: Empty list.
        """
        return list()

    @property
    def download_folder(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    @property
    def extract_folder(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    def cleanup(self):
        """Delete all files and folders associated with this release.
        :return: None.
        """
        shutil.rmtree(self.transform_folder)


class OnixWorkflow(Telescope):
    """This workflow telescope:
    1. [Not implemented] Creates an ISBN13-> internal identifier lookup table.
    2. Creates an ISBN13 -> WorkID lookup table.
      a. Aggregates Product records into Work clusters.
      b. Writes the lookup table to BigQuery.
      c. Writes an error table to BigQuery.
    3. Create an ISBN13 -> Work Family ID lookup table.  Clusters editions together.
      a. Aggregate Works into Work Families.
      b. Writes the lookup table to BigQuery.
    """

    DAG_ID_PREFIX = "onix_workflow"

    def __init__(
        self,
        *,
        org_name: str,
        gcp_project_id: str,
        gcp_bucket_name: str,
        telescope_sensor: ExternalTaskSensor,
        onix_dataset_id: str = "onix",
        onix_table_id: str = "onix",
        dag_id: Optional[str] = None,
        start_date: Optional[datetime] = datetime(2021, 3, 28),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
    ):
        """Initialises the workflow object.
        :param org_name: Organisation name.
        :param gcp_project_id: Project ID in GCP.
        :param gcp_bucket_name: GCP bucket name to store files.
        :param telescope_sensor: Telescope sensor to monitor.
        :param onix_dataset_id: GCP dataset ID of the onix data.
        :param onix_table_id: GCP table ID of the onix data.
        :param dag_id: DAG ID.
        :param start_date: Start date of the DAG.
        :param schedule_interval: Scheduled interval for running the DAG.
        :param catchup: Whether to catch up missed DAG runs.
        """

        self.dag_id = dag_id
        if dag_id is None:
            self.dag_id = make_dag_id(self.DAG_ID_PREFIX, org_name)

        self.org_name = org_name
        self.gcp_project_id = gcp_project_id
        self.gcp_bucket_name = gcp_bucket_name
        self.onix_dataset_id = onix_dataset_id
        self.onix_table_id = onix_table_id

        # Initialise Telesecope base class
        super().__init__(
            dag_id=self.dag_id, start_date=start_date, schedule_interval=schedule_interval, catchup=catchup
        )

        # Wait on ONIX telescope to finish
        self.add_sensor(telescope_sensor)

        # Aggregate Works
        self.add_task(self.aggregate_works)
        self.add_task(self.upload_aggregation_tables)
        self.add_task(self.bq_load_workid_lookup)
        self.add_task(self.bq_load_workid_lookup_errors)
        self.add_task(self.bq_load_workfamilyid_lookup)

        # Cleanup tasks
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> OnixWorkflowRelease:
        """Creates a release object.
        :param kwargs: From Airflow. Contains the execution_date.
        """

        release_date = kwargs["execution_date"]
        release = OnixWorkflowRelease(
            dag_id=self.dag_id,
            release_date=release_date,
            gcp_project_id=self.gcp_project_id,
            gcp_bucket_name=self.gcp_bucket_name,
            onix_dataset_id=self.onix_dataset_id,
            onix_table_id=self.onix_table_id,
        )

        return release

    def get_onix_records(self, project_id: str, dataset_id, table_id) -> List[dict]:
        """Fetch the latest onix snapshot from BigQuery.
        :param project_id: Project ID.
        :param dataset_id: Dataset ID.
        :param table_id: Table ID.
        :return: List of onix product records.
        """

        sql = f"SELECT * FROM {project_id}.{dataset_id}.{table_id}"
        records = run_bigquery_query(sql)
        return records

    def aggregate_works(self, release: OnixWorkflowRelease, **kwargs):
        """Fetches the ONIX product records from our ONIX database, aggregates them into works, workfamilies,
        and outputs it into jsonl files.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        print("===========================================================")
        print(
            f"Attempting to fetch suffixes for proj: {self.gcp_project_id}, dataset: {self.onix_dataset_id}, table_id: {self.onix_table_id}, end_date: {release.release_date}"
        )
        print("===========================================================")

        onix_release_date = select_table_suffixes(
            project_id=self.gcp_project_id,
            dataset_id=self.onix_dataset_id,
            table_id=self.onix_table_id,
            end_date=release.release_date,
        )[0]

        # Fetch ONIX data
        table_id = release.onix_table_id + onix_release_date.strftime("%Y%m%d")
        products = self.get_onix_records(release.project_id, release.onix_dataset_id, table_id)

        # Aggregate into works
        agg = BookWorkAggregator(products)
        works = agg.aggregate()
        lookup_table = agg.get_works_lookup_table()
        list_to_jsonl_gz(release.workslookup_filename, lookup_table)

        # Save errors from aggregation
        error_table = [{"Error": error} for error in agg.errors]
        list_to_jsonl_gz(release.workslookup_errors_filename, error_table)

        # Aggregate work families
        agg = BookWorkFamilyAggregator(works)
        agg.aggregate()
        lookup_table = agg.get_works_family_lookup_table()
        list_to_jsonl_gz(release.worksfamilylookup_filename, lookup_table)

    def upload_aggregation_tables(self, release: OnixWorkflowRelease, **kwargs):
        """Upload the aggregation tables and error tables to a GCP bucket in preparation for BQ loading.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        files = release.transform_files
        blobs = [os.path.join(release.transform_folder, os.path.basename(file)) for file in files]

        upload_files_to_cloud_storage(bucket_name=release.transform_bucket, blob_names=blobs, file_paths=files)

    def bq_load_workid_lookup(self, release: OnixWorkflowRelease, **kwargs):
        """Load the WorkID lookup table into BigQuery.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        blob = os.path.join(release.transform_folder, os.path.basename(release.workslookup_filename))
        table_id, _ = table_ids_from_path(release.workslookup_filename)
        bq_load_shard_v2(
            project_id=release.project_id,
            transform_bucket=release.transform_bucket,
            transform_blob=blob,
            dataset_id=release.workflow_dataset_id,
            dataset_location=release.dataset_location,
            table_id=table_id,
            release_date=release.release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            dataset_description=release.dataset_description,
            **{},
        )

    def bq_load_workid_lookup_errors(self, release: OnixWorkflowRelease, **kwargs):
        """Load the WorkID lookup table errors into BigQuery.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        blob = os.path.join(release.transform_folder, os.path.basename(release.workslookup_errors_filename))

        table_id, _ = table_ids_from_path(release.workslookup_errors_filename)

        bq_load_shard_v2(
            project_id=release.project_id,
            transform_bucket=release.transform_bucket,
            transform_blob=blob,
            dataset_id=release.workflow_dataset_id,
            dataset_location=release.dataset_location,
            table_id=table_id,
            release_date=release.release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            prefix="",
            schema_version="",
            dataset_description=release.dataset_description,
            **{},
        )

    def bq_load_workfamilyid_lookup(self, release: OnixWorkflowRelease, **kwargs):
        """Load the WorkFamilyID lookup table into BigQuery.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        blob = os.path.join(release.transform_folder, os.path.basename(release.worksfamilylookup_filename))

        table_id, _ = table_ids_from_path(release.worksfamilylookup_filename)

        bq_load_shard_v2(
            project_id=release.project_id,
            transform_bucket=release.transform_bucket,
            transform_blob=blob,
            dataset_id=release.workflow_dataset_id,
            dataset_location=release.dataset_location,
            table_id=table_id,
            release_date=release.release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            prefix="",
            schema_version="",
            dataset_description=release.dataset_description,
            **{},
        )

    def cleanup(self, release: OnixWorkflowRelease, **kwargs):
        """Cleanup temporary files.

        :param release: Workflow release object.
        :param kwargs: Unused.
        """

        release.cleanup()
