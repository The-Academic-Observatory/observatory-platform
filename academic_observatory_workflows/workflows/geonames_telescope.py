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

# Author: Aniek Roelofs, James Diprose


from __future__ import annotations

import gzip
import logging
import os
import shutil
from typing import Dict, List
from zipfile import ZipFile

import pendulum
import requests
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.workflow_utils import upload_files_from_list
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)


def fetch_release_date() -> pendulum.DateTime:
    """Fetch the Geonames release date.

    :return: the release date.
    """

    response = requests.head(GeonamesRelease.DOWNLOAD_URL)
    date_str = response.headers["Last-Modified"]
    date: pendulum.DateTime = pendulum.from_format(date_str, "ddd, DD MMM YYYY HH:mm:ss z")
    return date


def first_sunday_of_month(datetime: pendulum.DateTime) -> pendulum.DateTime:
    """Get the first Sunday of the month based on a given datetime.

    :param datetime: the datetime.
    :return: the first Sunday of the month.
    """

    return datetime.start_of("month").first_of("month", day_of_week=7)


class GeonamesRelease(SnapshotRelease):
    DOWNLOAD_URL = "https://download.geonames.org/export/dump/allCountries.zip"

    def __init__(self, dag_id: str, release_date: pendulum.DateTime):
        """Create a GeonamesRelease instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        """

        download_file_name = f"{dag_id}.zip"
        extract_file_name = f"allCountries.txt"
        transform_file_name = f"{dag_id}.csv.gz"
        super().__init__(dag_id, release_date, download_file_name, extract_file_name, transform_file_name)

    @property
    def download_path(self) -> str:
        """Get the path to the downloaded file.

        :return: the file path.
        """

        return os.path.join(self.download_folder, self.download_files_regex)

    @property
    def extract_path(self) -> str:
        """Get the path to the extracted file.

        :return: the file path.
        """

        return os.path.join(self.extract_folder, self.extract_files_regex)

    @property
    def transform_path(self) -> str:
        """Get the path to the transformed file.

        :return: the file path.
        """

        return os.path.join(self.transform_folder, self.transform_files_regex)

    def download(self):
        """Downloads geonames dump file containing country data. The file is in zip format and will be extracted
        after downloading, saving the unzipped content.

        :return: None
        """

        file_path, updated = get_file(
            fname=self.download_path, origin=GeonamesRelease.DOWNLOAD_URL, cache_subdir="", extract=False
        )

        logging.info(f"Downloaded file: {file_path}")

    def extract(self):
        """Extract a downloaded Geonames release.

        :return: None
        """

        with ZipFile(self.download_path) as zip_file:
            zip_file.extractall(self.extract_folder)

    def transform(self):
        """Transforms release by storing file content in gzipped csv format.

        :return: None
        """

        with open(self.extract_path, "rb") as file_in:
            with gzip.open(self.transform_path, "wb") as file_out:
                shutil.copyfileobj(file_in, file_out)


class GeonamesTelescope(SnapshotTelescope):
    """
    A Telescope that harvests the GeoNames geographical database: https://www.geonames.org/

    Saved to the BigQuery table: <project_id>.geonames.geonamesYYYYMMDD
    """

    DAG_ID = "geonames"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 9, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = "geonames",
        schema_folder: str = default_schema_folder(),
        source_format: str = SourceFormat.CSV,
        dataset_description: str = "The GeoNames geographical database: https://www.geonames.org/",
        load_bigquery_table_kwargs: Dict = None,
        table_descriptions: Dict = None,
        catchup: bool = False,
        airflow_vars: List = None,
    ):

        """The Geonames telescope.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
        :param source_format: the format of the data to load into BigQuery.
        :param dataset_description: description for the BigQuery dataset.
        :param load_bigquery_table_kwargs: the customisation parameters for loading data into a BigQuery table.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param catchup:  whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        """

        if load_bigquery_table_kwargs is None:
            load_bigquery_table_kwargs = {"csv_field_delimiter": "\t", "csv_quote_character": ""}

        if table_descriptions is None:
            table_descriptions = {dag_id: "The GeoNames table."}

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            source_format=source_format,
            load_bigquery_table_kwargs=load_bigquery_table_kwargs,
            dataset_description=dataset_description,
            table_descriptions=table_descriptions,
            catchup=catchup,
            airflow_vars=airflow_vars,
        )
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.fetch_release_date)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[GeonamesRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: a list of GeonamesRelease instances.
        """

        ti: TaskInstance = kwargs["ti"]
        release_date = ti.xcom_pull(
            key=GeonamesTelescope.RELEASE_INFO, task_ids=self.fetch_release_date.__name__, include_prior_dates=False
        )

        return [GeonamesRelease(self.dag_id, pendulum.parse(release_date))]

    def fetch_release_date(self, **kwargs):
        """Get the Geonames release for a given month and publishes the release_date as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: whether to keep executing the DAG.
        """

        # Check if first Sunday of month
        execution_date = kwargs["execution_date"]
        run_date = first_sunday_of_month(execution_date)
        logging.info(f"execution_date={execution_date}, run_date={run_date}")

        # If first Sunday of month get current release date and push for processing
        continue_dag = execution_date == run_date
        if continue_dag:
            # Fetch release date
            release_date = fetch_release_date()

            # Push messages
            ti: TaskInstance = kwargs["ti"]
            ti.xcom_push(GeonamesTelescope.RELEASE_INFO, release_date.format("YYYYMMDD"), execution_date)

        return continue_dag

    def download(self, releases: List[GeonamesRelease], **kwargs):
        """Task to download the GeonamesRelease release for a given month.

        :param releases: the list of GeonamesRelease instances.
        :return: None.
        """

        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[GeonamesRelease], **kwargs):
        """Task to upload the downloaded GeonamesRelease release for a given month.

        :param releases: the list of GeonamesRelease instances.
        :return: None.
        """

        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def extract(self, releases: List[GeonamesRelease], **kwargs):
        """Task to extract the GeonamesRelease release for a given month.

        :param release: GeonamesRelease.
        :return: None.
        """

        for release in releases:
            release.extract()

    def transform(self, releases: List[GeonamesRelease], **kwargs):
        """Task to transform the GeonamesRelease release for a given month.

        :param releases: the list of GeonamesRelease instances.
        :return: None.
        """

        for release in releases:
            release.transform()
