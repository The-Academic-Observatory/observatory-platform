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

# Author: Aniek Roelofs

from typing import Dict, List

import pendulum
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
from observatory.api.client.model.organisation import Organisation
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.workflow_utils import (
    blob_name,
    bq_load_partition,
    delete_old_xcoms,
    table_ids_from_path,
    upload_files_from_list,
)
from observatory.platform.workflows.workflow import Release, Workflow


class OrganisationRelease(Release):
    def __init__(
        self,
        dag_id: str,
        release_date: pendulum.DateTime,
        organisation: Organisation,
        download_files_regex: str = None,
        extract_files_regex: str = None,
        transform_files_regex: str = None,
    ):
        """Construct an OrganisationRelease instance.

        :param dag_id: the id of the DAG.
        :param release_date: the release date (used to construct release_id).
        :param organisation: the Organisation created with the Observatory API.
        :param download_files_regex: regex pattern that is used to find files in download folder
        :param extract_files_regex: regex pattern that is used to find files in extract folder
        :param transform_files_regex: regex pattern that is used to find files in transform folder
        """
        self.release_date = release_date
        release_id = f'{dag_id}_{self.release_date.strftime("%Y_%m_%d")}'
        super().__init__(dag_id, release_id, download_files_regex, extract_files_regex, transform_files_regex)
        self.organisation = organisation

    @property
    def download_bucket(self):
        """The download bucket name.

        :return: the download bucket name.
        """
        return self.organisation.gcp_download_bucket

    @property
    def transform_bucket(self):
        """The transform bucket name.

        :return: the transform bucket name.
        """
        return self.organisation.gcp_transform_bucket


class OrganisationTelescope(Workflow):
    def __init__(
        self,
        organisation: Organisation,
        dag_id: str,
        start_date: pendulum.DateTime,
        schedule_interval: str,
        dataset_id: str,
        schema_folder: str,
        catchup: bool,
        queue: str = "default",
        max_retries: int = 3,
        max_active_runs: int = 1,
        source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_prefix: str = "",
        schema_version: str = None,
        load_bigquery_table_kwargs: Dict = None,
        dataset_description: str = "",
        table_descriptions: Dict[str, str] = None,
        airflow_vars: list = None,
        airflow_conns: list = None,
    ):
        """Construct a OrganisationTelescope instance.

        :param organisation: the Organisation created with the Observatory API.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param schema_folder: the path to the SQL schema folder.
        :param catchup: whether to catchup the DAG or not.
        :param queue: the Airflow queue name.
        :param max_retries: the number of times to retry each task.
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param source_format: the format of the data to load into BigQuery.
        :param schema_prefix: the prefix used to find the schema path.
        :param schema_version: the version used to find the schema path.
        :param load_bigquery_table_kwargs: the customisation parameters for loading data into a BigQuery table.
        :param dataset_description: description for the BigQuery dataset.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions
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
            catchup,
            queue,
            max_retries,
            max_active_runs,
            airflow_vars,
            airflow_conns,
        )
        self.organisation = organisation
        self.project_id = organisation.gcp_project_id
        self.dataset_id = dataset_id
        self.dataset_location = "us"  # TODO: add to API
        self.source_format = source_format
        self.schema_folder = schema_folder
        self.schema_prefix = schema_prefix
        self.schema_version = schema_version
        self.load_bigquery_table_kwargs = load_bigquery_table_kwargs if load_bigquery_table_kwargs else dict()
        self.dataset_description = dataset_description
        self.table_descriptions = table_descriptions if table_descriptions else dict()

    def upload_downloaded(self, releases: List[OrganisationRelease], **kwargs):
        """Task to upload each downloaded release to a Google Cloud bucket.

        :param releases: a list of releases.
        :param kwargs: the context passed from the PythonOperator.
        :return: None.
        """
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def upload_transformed(self, releases: List[OrganisationRelease], **kwargs):
        """Task to upload each transformed release to a Google Cloud bucket.

        :param releases: a list of releases.
        :param kwargs: the context passed from the PythonOperator.
        :return: None.
        """
        for release in releases:
            upload_files_from_list(release.transform_files, release.transform_bucket)

    def bq_load_partition(self, releases: List[OrganisationRelease], **kwargs):
        """Task to load each transformed release to BigQuery.
        The table_id is set to the file name without the extension.

        :param releases: a list of releases.
        :param kwargs: the context passed from the PythonOperator.
        :return: None.
        """
        for release in releases:
            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)
                table_description = self.table_descriptions.get(table_id, "")
                bq_load_partition(
                    self.schema_folder,
                    self.project_id,
                    release.transform_bucket,
                    transform_blob,
                    self.dataset_id,
                    self.dataset_location,
                    table_id,
                    release.release_date,
                    self.source_format,
                    bigquery.table.TimePartitioningType.MONTH,
                    prefix=self.schema_prefix,
                    schema_version=self.schema_version,
                    dataset_description=self.dataset_description,
                    table_description=table_description,
                    **self.load_bigquery_table_kwargs,
                )

    def cleanup(self, releases: List[OrganisationRelease], **kwargs):
        """Delete files of downloaded, extracted and transformed release. Deletes old xcoms.

        :param releases: a list of releases.
        :param kwargs:
        :return: None.
        """
        for release in releases:
            release.cleanup()

        execution_date = kwargs["execution_date"]
        delete_old_xcoms(dag_id=self.dag_id, execution_date=execution_date)
