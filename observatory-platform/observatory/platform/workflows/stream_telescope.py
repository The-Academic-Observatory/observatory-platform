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

# Author: Aniek Roelofs, James Diprose, Tuan Chien

import logging
from typing import Dict, Tuple, List

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models.dagrun import DagRun
from croniter import croniter
from google.cloud.bigquery import SourceFormat
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.workflow_utils import (
    batch_blob_name,
    blob_name,
    bq_append_from_file,
    bq_append_from_partition,
    bq_delete_old,
    bq_load_ingestion_partition,
    delete_old_xcoms,
    is_first_dag_run,
    table_ids_from_path,
    upload_files_from_list,
)
from observatory.platform.workflows.workflow import Release, Workflow


class StreamRelease(Release):
    def __init__(
        self,
        dag_id: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        first_release: bool,
        download_files_regex: str = None,
        extract_files_regex: str = None,
        transform_files_regex: str = None,
    ):
        """Construct a StreamRelease instance

        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        :param download_files_regex: regex pattern that is used to find files in download folder
        :param extract_files_regex: regex pattern that is used to find files in extract folder
        :param transform_files_regex: regex pattern that is used to find files in transform folder
        """
        self.start_date = start_date
        self.end_date = end_date
        self.first_release = first_release
        release_id = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")
        super().__init__(dag_id, release_id, download_files_regex, extract_files_regex, transform_files_regex)


def get_data_interval(
    execution_date: pendulum.DateTime, schedule_interval: str
) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
    """Get the data interval for a DAG Run. TODO: replace this in Airflow 2.2 when data intervals are part of
    the DAG model"""

    schedule_interval = croniter(schedule_interval, execution_date)
    end_date = pendulum.from_timestamp(schedule_interval.get_next())
    return execution_date, end_date


class StreamTelescope(Workflow):
    def __init__(
        self,
        dag_id: str,
        start_date: pendulum.DateTime,
        schedule_interval: str,
        dataset_id: str,
        merge_partition_field: str,
        schema_folder: str,
        catchup: bool = False,
        queue: str = "default",
        max_retries: int = 3,
        max_active_runs: int = 1,
        source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_prefix: str = "",
        schema_version: str = None,
        load_bigquery_table_kwargs: Dict = None,
        dataset_description: str = "",
        table_descriptions: Dict[str, str] = None,
        batch_load: bool = False,
        airflow_vars: list = None,
        airflow_conns: list = None,
    ):
        """Construct a StreamTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param schema_folder: the path to the SQL schema folder.
        :param catchup: whether to catchup the DAG or not.
        :param queue: the Airflow queue name.
        :param max_retries: the number of times to retry each task.
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param source_format: the format of the data to load into BigQuery.
        :param schema_prefix: the prefix used to find the schema path
        :param schema_version: the version used to find the schema path
        :param load_bigquery_table_kwargs: the customisation parameters for loading data into a BigQuery table.
        :param dataset_description: description for the BigQuery dataset.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions
        :param batch_load: whether all files in the transform folder are loaded into 1 table at once
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        """

        # Set transform_bucket_name as required airflow variable
        if not airflow_vars:
            airflow_vars = []
        airflow_vars = list(set([AirflowVars.TRANSFORM_BUCKET] + airflow_vars))

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            catchup=catchup,
            queue=queue,
            max_retries=max_retries,
            max_active_runs=max_active_runs,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
        )

        self.schema_prefix = schema_prefix
        self.schema_version = schema_version
        self.dataset_id = dataset_id
        self.source_format = source_format
        self.merge_partition_field = merge_partition_field
        self.schema_folder = schema_folder
        self.load_bigquery_table_kwargs = load_bigquery_table_kwargs if load_bigquery_table_kwargs else dict()
        self.dataset_description = dataset_description
        self.table_descriptions = table_descriptions if table_descriptions else dict()
        self.batch_load = batch_load

    def get_release_info(self, **kwargs) -> Tuple[pendulum.DateTime, pendulum.DateTime, bool]:
        """Return release information with the start and end date.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """

        # Check if first release or not
        dag_run: DagRun = kwargs["dag_run"]
        first_release = is_first_dag_run(dag_run)
        if first_release:
            # When first release, set start date to the start of the DAG
            start_date = pendulum.instance(kwargs["dag"].default_args["start_date"]).start_of("day")
        else:
            # When not first release, set start date to be the end date of the previous DAG run
            prev_dag_run: DagRun = dag_run.get_previous_dagrun()
            _, prev_end_date = get_data_interval(prev_dag_run.execution_date, self.schedule_interval)
            start_date = prev_end_date.start_of("day")

        # Set end date to end of time period, subtract 1 day, because next execution date is the date
        # the DAG will actually run
        end_date = kwargs["next_execution_date"].subtract(days=1).start_of("day")
        logging.info(f"Start date: {start_date}, end date: {end_date}, first release: {first_release}")

        return start_date, end_date, first_release

    def get_bq_load_info(self, release: StreamRelease) -> List[Tuple[str, str, str]]:
        """Get (a list of) the transform blob, main table id and partition table id that are used to load data into
        BigQuery.
        When the batch_load property of the telescope is set to True, all blobs in the transform folder will be loaded
        into 1 table at once.
        When the batch_load property of the telescope is set to False, each blob in the transform folder will be used to
        create/update an individual table.

        :param release: The release object.
        :return: List with tuples of transform_blob, main_table_id and partition_table_id
        """
        bq_info = []
        if self.batch_load:
            if release.transform_files:
                transform_blob = batch_blob_name(release.transform_folder)
                main_table_id, partition_table_id = (self.dag_id, f"{self.dag_id}_partitions")
                bq_info = [(transform_blob, main_table_id, partition_table_id)]
        else:
            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                bq_info.append((transform_blob, main_table_id, partition_table_id))
        return bq_info

    def download(self, release: StreamRelease, **kwargs):
        """Task to download the StreamRelease release.

        :param release: a StreamRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.download()

    def upload_downloaded(self, release: StreamRelease, **kwargs):
        """Upload the downloaded files for the given release.

        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        upload_files_from_list(release.download_files, release.download_bucket)

    def extract(self, release: StreamRelease, **kwargs):
        """Task to extract the StreamRelease release.

        :param release: a StreamRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.extract()

    def transform(self, release: StreamRelease, **kwargs):
        """Task to transform the StreamRelease release.

        :param release: a StreamRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.transform()

    def upload_transformed(self, release: StreamRelease, **kwargs):
        """Upload the transformed files for the given release.

        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        upload_files_from_list(release.transform_files, release.transform_bucket)

    def bq_load_partition(self, release: StreamRelease, **kwargs):
        """For each file listed in transform_files, create a time based partition in BigQuery

        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        if release.first_release:
            # The main table is the same as the partition, so no need to upload the partition as well. Especially
            # because a first release can be relatively big in size.
            raise AirflowSkipException("Skipped, because first release")

        bq_load_info = self.get_bq_load_info(release)
        for transform_blob, main_table_id, partition_table_id in bq_load_info:
            table_description = self.table_descriptions.get(main_table_id, "")
            bq_load_ingestion_partition(
                self.schema_folder,
                release.end_date,
                transform_blob,
                self.dataset_id,
                main_table_id,
                partition_table_id,
                self.source_format,
                self.schema_prefix,
                self.schema_version,
                self.dataset_description,
                table_description=table_description,
                **self.load_bigquery_table_kwargs,
            )

    def bq_delete_old(self, release: StreamRelease, **kwargs):
        """Delete old rows from the 'main' table, based on rows that are in a partition of the 'partitions' table.

        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        if release.first_release:
            # Nothing to delete, because first release
            raise AirflowSkipException("Skipped, because first release")

        logging.info(f"Deleting old data from main table using partition with ingestion date of {release.end_date}")
        bytes_budget = kwargs.get("bytes_budget", None)
        bq_load_info = self.get_bq_load_info(release)
        for _, main_table_id, partition_table_id in bq_load_info:
            bq_delete_old(
                release.end_date,
                self.dataset_id,
                main_table_id,
                partition_table_id,
                self.merge_partition_field,
                bytes_budget=bytes_budget,
            )

    def bq_append_new(self, release: StreamRelease, **kwargs):
        """Append rows to the 'main' table, based on rows that are in a partition of the 'partitions' table.

        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        bq_load_info = self.get_bq_load_info(release)

        if release.first_release:
            for transform_blob, main_table_id, partition_table_id in bq_load_info:
                table_description = self.table_descriptions.get(main_table_id, "")
                bq_append_from_file(
                    self.schema_folder,
                    release.end_date,
                    transform_blob,
                    self.dataset_id,
                    main_table_id,
                    self.source_format,
                    self.schema_prefix,
                    self.schema_version,
                    self.dataset_description,
                    table_description=table_description,
                    **self.load_bigquery_table_kwargs,
                )
            return

        logging.info(f"Appending data to main table from partition with ingestion date of {release.end_date}")
        for transform_blob, main_table_id, partition_table_id in bq_load_info:
            bq_append_from_partition(
                release.end_date,
                self.dataset_id,
                main_table_id,
                partition_table_id,
            )

    def cleanup(self, release: StreamRelease, **kwargs):
        """Delete downloaded, extracted and transformed files of the release. Deletes old xcoms.

        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.cleanup()

        execution_date = kwargs["execution_date"]
        delete_old_xcoms(dag_id=self.dag_id, execution_date=execution_date)
