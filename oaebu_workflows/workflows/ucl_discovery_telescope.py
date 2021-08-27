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


import csv
import logging
import os
from datetime import timedelta
from typing import List, Optional, Tuple

import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from google.cloud import bigquery

from observatory.api.client.model.organisation import Organisation
from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.file_utils import list_to_jsonl_gz
from observatory.platform.utils.url_utils import retry_session
from observatory.platform.utils.workflow_utils import add_partition_date, make_dag_id
from observatory.platform.utils.workflow_utils import (
    blob_name,
    bq_load_partition,
    table_ids_from_path,
    upload_files_from_list,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)


class UclDiscoveryRelease(SnapshotRelease):
    def __init__(
        self, dag_id: str, start_date: pendulum.DateTime, end_date: pendulum.DateTime, organisation: Organisation
    ):
        """Construct a UclDiscoveryRelease instance.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the download period.
        :param end_date: the end date of the download period, also used as release date for BigQuery table and
        file paths.
        :param organisation: the Organisation of which data is processed.
        """
        super().__init__(dag_id, end_date)

        self.dag_id_prefix = UclDiscoveryTelescope.DAG_ID_PREFIX
        self.organisation = organisation
        self.start_date = start_date
        self.end_date = end_date

        self.eprint_metadata_url = (
            "https://discovery.ucl.ac.uk/cgi/search/archive/advanced/export_discovery_CSV.csv?"
            "screen=Search&dataset=archive&_action_export=1&output=CSV"
            "&exp=0|1|-date/creators_name/title|archive|-|date:date:ALL:EQ:-"
            f'{self.end_date.strftime("%Y")}|primo:primo:ANY:EQ:open'
            "|type:type:ANY:EQ:book|-|eprint_status:eprint_status:ANY:EQ:archive"
            "|metadata_visibility:metadata_visibility:ANY:EQ:show"
        )
        self.countries_url = (
            "https://discovery.ucl.ac.uk/cgi/stats/get?from="
            f'{self.start_date.strftime("%Y%m%d")}&to='
            f'{self.end_date.strftime("%Y%m%d")}&irs2report=eprint&datatype=countries&top=countries'
            f"&view=Table&limit=all&set_name=eprint&export=CSV&set_value="
        )

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

    @property
    def download_path(self) -> str:
        """Creates path to store the downloaded UCL discovery data
        :return: Full path to the download file
        """
        return os.path.join(self.download_folder, f"{self.dag_id_prefix}.txt")

    @property
    def transform_path(self) -> str:
        """Creates path to store the transformed and gzipped UCL discovery data
        :return: Full path to the transform file
        """
        return os.path.join(self.transform_folder, f"{self.dag_id_prefix}.jsonl.gz")

    def download(self):
        """Download metadata for all eprints that are published before a specific date

        :return: None.
        """
        logging.info(f"Downloading metadata from {self.eprint_metadata_url}")
        response = retry_session(num_retries=5).get(self.eprint_metadata_url)
        if response.status_code == 200:
            response_content = response.content.decode("utf-8")
            csv_reader = csv.DictReader(response_content.splitlines())
            try:
                next(csv_reader)
            except StopIteration:
                raise AirflowSkipException("No metadata available for the year of this release date.")
            logging.info(f"Saving metadata to file: {self.download_path}")
            with open(self.download_path, "w") as f:
                f.write(response_content)
        else:
            raise AirflowException(
                f"Could not download metadata, response status code: {response.status_code} "
                f"reason: {response.reason}"
            )

    def transform(self):
        """Parse the csv file and for each eprint id store the relevant metadata in a dictionary and get the downloads
        per country (between begin_date and end_date). The list of dictionaries is stored in a gzipped json lines file.
        There might be multiple rows for 1 eprint id. Some columns only have a value in the first row, some columns
        have values in multiple rows.

        :return: None.
        """
        begin_date = self.start_date.strftime("%Y-%m-%d")
        end_date = self.end_date.strftime("%Y-%m-%d")
        total_downloads = 0
        downloads_per_country = []
        single_row_columns = {}
        with open(self.download_path, "r") as f:
            csv_reader = csv.DictReader(f)

            previous_id = None
            results = []
            multi_row_columns = {
                "creators_name_family": [],
                "creators_name_given": [],
                "subjects": [],
                "divisions": [],
                "lyricists_name_family": [],
                "lyricists_name_given": [],
                "editors_name_family": [],
                "editors_name_given": [],
            }
            for row in csv_reader:
                eprintid = row["eprintid"]
                # row with a new eprint id
                if previous_id != eprintid:
                    # add results of previous eprint id
                    if previous_id:
                        result = create_result_dict(
                            begin_date,
                            end_date,
                            total_downloads,
                            downloads_per_country,
                            multi_row_columns,
                            single_row_columns,
                        )
                        results.append(result)
                        for column in multi_row_columns:
                            multi_row_columns[column] = []

                    # store results of current eprint id
                    single_row_columns = {
                        "eprintid": row["eprintid"],
                        "book_title": row["title"],
                        "ispublished": row["ispublished"],
                        "keywords": row["keywords"].split(", "),
                        "abstract": row["abstract"],
                        "date": row["date"],
                        "publisher": row["publisher"],
                        "official_url": row["official_url"],
                        "oa_status": row["oa_status"],
                        "language": row["language"],
                        "doi": row["doi"],
                        "isbn": row["isbn_13"],
                        "language_elements": row["language_elements"],
                        "series": row["series"],
                        "pagerange": row["pagerange"],
                        "pages": row["pages"],
                    }

                    downloads_per_country, total_downloads = get_downloads_per_country(self.countries_url + eprintid)

                # append results of current eprint id
                for column in multi_row_columns:
                    # For 'name' type columns, don't add empty strings as a name, but make sure that  a value is added
                    # for both family and given, even if only 1 of the columns has a value.
                    start_column_name = column.split("_")[0]  # 'creators' when column = 'creators_name_family'
                    if start_column_name in ["creators", "lyricists", "editors"]:
                        name_family = row[start_column_name + "_name.family"]
                        name_given = row[start_column_name + "_name.given"]

                        name = name_family + name_given
                        if name:
                            column_name = ".".join(column.rsplit("_", 1))  # 'creators_name.family' when column =
                            # 'creators_name_family'
                            multi_row_columns[column].append(row[column_name])
                    else:
                        if row[column]:
                            multi_row_columns[column].append(row[column])

                previous_id = eprintid

            # append results of last rows/eprint id
            result = create_result_dict(
                begin_date, end_date, total_downloads, downloads_per_country, multi_row_columns, single_row_columns
            )
            results.append(result)

        results = add_partition_date(results, self.release_date, bigquery.TimePartitioningType.MONTH)
        # Write list into gzipped JSON Lines file
        list_to_jsonl_gz(self.transform_path, results)


class UclDiscoveryTelescope(SnapshotTelescope):
    """The UCL Discovery telescope."""

    DAG_ID_PREFIX = "ucl_discovery"

    def __init__(
        self,
        organisation: Organisation,
        dag_id: Optional[str] = None,
        start_date: pendulum.DateTime = pendulum.datetime(2008, 1, 1),
        schedule_interval: str = "@monthly",
        dataset_id: str = "ucl",
        schema_folder: str = default_schema_folder(),
        airflow_vars: list = None,
        max_active_runs: int = 10,
    ):
        """Construct a UclDiscoveryTelescope instance.
        :param organisation: the Organisation of which data is processed.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the name of the dataset in BigQuery.
        :param schema_folder: the SQL schema path.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        :param max_active_runs: the maximum number of DAG runs to execute in parallel.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]
        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation.name)

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            airflow_vars=airflow_vars,
            max_active_runs=max_active_runs,
        )
        self.organisation = organisation
        self.project_id = organisation.gcp_project_id
        self.dataset_location = "us"  # TODO: add to API
        self.add_setup_task(self.check_dependencies)
        self.add_task_chain(
            [
                self.download,
                self.upload_downloaded,
                self.transform,
                self.upload_transformed,
                self.bq_load_partition,
                self.cleanup,
            ]
        )

    def make_release(self, **kwargs) -> List[UclDiscoveryRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'. There will only be 1 release, but it is passed on as a list so the
        SnapshotTelescope template methods can be used.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list with one ucldiscovery release instance.
        """

        # Get start and end date (end_date = release_date)
        start_date = kwargs["execution_date"]
        end_date = kwargs["next_execution_date"] - timedelta(days=1)

        logging.info(f"Start date: {start_date}, end date:{end_date}, release date: {end_date}")
        releases = [UclDiscoveryRelease(self.dag_id, start_date, end_date, self.organisation)]
        return releases

    def download(self, releases: List[UclDiscoveryRelease], **kwargs):
        """Task to download the ucldiscovery release for a given month.
        :param releases: a list with the ucldiscovery release.
        :return: None.
        """
        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[UclDiscoveryRelease], **kwargs):
        """Task to upload the downloaded ucldiscovery release for a given month.
        :param releases: a list with the ucldiscovery release.
        :return: None.
        """
        # Upload each downloaded release
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[UclDiscoveryRelease], **kwargs):
        """Task to transform the ucldiscovery release for a given month.
        :param releases: a list with the ucldiscovery release.
        :return: None.
        """
        # Transform each release
        for release in releases:
            release.transform()

    def bq_load_partition(self, releases: List[UclDiscoveryRelease], **kwargs):
        """Task to load each transformed release to BigQuery.
        The table_id is set to the file name without the extension.
        :param releases: a list of releases.
        :return: None.
        """

        # Load each transformed release
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


def get_downloads_per_country(countries_url: str) -> Tuple[List[dict], int]:
    """Requests info on downloads per country for a specific eprint id

    :param countries_url: The url to the downloads per country info
    :return: Number of total downloads and list of downloads per country, country code and country name.
    """
    response = retry_session(num_retries=5).get(countries_url)
    response_content = response.content.decode("utf-8")
    if response_content == "\n":
        return [], 0
    response_csv = csv.DictReader(response_content.splitlines())
    results = []
    total_downloads = 0
    for row in response_csv:
        download_count = int(row["count"].strip('="'))
        country_code = row["value"]
        country_name = row["description"].split("</span>")[0].split(">")[-1]
        results.append({"country_code": country_code, "country_name": country_name, "download_count": download_count})
        total_downloads += download_count

    return results, total_downloads


def create_result_dict(
    begin_date: str,
    end_date: str,
    total_downloads: int,
    downloads_per_country: List[dict],
    multi_row_columns: dict,
    single_row_columns: dict,
) -> dict:
    """Create one result dictionary with info on downloads for a specific eprint id in a given time period.

    :param begin_date: The begin date of download period
    :param end_date: The end date of download period
    :param total_downloads: Total of downloads in that period
    :param downloads_per_country: List of downloads per country
    :param multi_row_columns: Dict of column names & values for columns that have values over multiple rows of an
    eprintid
    :param single_row_columns: Dict of column names & values for columns that have values only in the first row of an eprint id
    :return: Results dictionary
    """
    result = dict(
        begin_date=begin_date,
        end_date=end_date,
        total_downloads=total_downloads,
        downloads_per_country=downloads_per_country,
        **multi_row_columns,
        **single_row_columns,
    )
    # change empty strings to None so they don't show up in BigQuery table
    for k, v in result.items():
        result[k] = v if v != "" else None
    return result
