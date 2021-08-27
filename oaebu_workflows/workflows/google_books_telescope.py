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
import os
import re
from collections import OrderedDict, defaultdict
from typing import List, Optional

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud import bigquery

from observatory.api.client.model.organisation import Organisation
from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.file_utils import list_to_jsonl_gz
from observatory.platform.utils.workflow_utils import (
    SftpFolders,
    add_partition_date,
    convert,
    make_dag_id,
    make_sftp_connection,
)
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


class GoogleBooksRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: pendulum.DateTime, sftp_files: List[str], organisation: Organisation):
        """Construct a GoogleBooksRelease.

        :param dag_id: the DAG id.
        :param release_date: the release date, corresponds to the last day of the month being processed.
        :param sftp_files: List of full filepaths to download from sftp service (incl. in_progress folder)
        :param organisation: the Organisation.
        """
        self.dag_id = dag_id
        self.release_date = release_date

        download_files_regex = f"^{GoogleBooksTelescope.DAG_ID_PREFIX}_(sales|traffic)\.csv$"
        transform_files_regex = f"^{GoogleBooksTelescope.DAG_ID_PREFIX}_(sales|traffic)\.jsonl\.gz$"
        super().__init__(
            self.dag_id,
            release_date,
            download_files_regex=download_files_regex,
            transform_files_regex=transform_files_regex,
        )
        self.organisation = organisation
        self.sftp_files = sftp_files

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

    def download_path(self, remote_path: str) -> str:
        """Creates full download path

        :param remote_path: filepath of remote sftp tile
        :return: Download path
        """
        report_type = "sales" if "Sales" in remote_path else "traffic"
        return os.path.join(self.download_folder, f"{GoogleBooksTelescope.DAG_ID_PREFIX}_{report_type}.csv")

    def transform_path(self, path: str) -> str:
        """Creates full transform path

        :param path: filepath of download file
        :return: Transform path
        """
        report_type = "sales" if "sales" in path else "traffic"
        return os.path.join(self.transform_folder, f"{GoogleBooksTelescope.DAG_ID_PREFIX}_{report_type}.jsonl.gz")

    def download(self):
        """Downloads Google Books reports.

        :return: the paths on the system of the downloaded files.
        """
        with make_sftp_connection() as sftp:
            for file in self.sftp_files:
                sftp.get(file, localpath=self.download_path(file))

    def transform(self):
        """Transforms sales and traffic report. For both reports it transforms the csv into a jsonl file and
        replaces spaces in the keys with underscores.

        :return: None
        """
        for file in self.download_files:
            results = []
            with open(file, encoding="utf-16") as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter="\t")
                for row in csv_reader:
                    transformed_row = OrderedDict((convert(k.replace("%", "Perc")), v) for k, v in row.items())
                    if "sales" in file:
                        transaction_date = pendulum.from_format(transformed_row["Transaction_Date"], "MM/DD/YY")

                        # sanity check that transaction date is in month of release date
                        if self.release_date.start_of("month") <= transaction_date <= self.release_date.end_of("month"):
                            pass
                        else:
                            raise AirflowException(
                                "Transaction date does not fall within release month. "
                                f"Transaction date: {transaction_date.strftime('%Y-%m-%d')}, "
                                f"release month: {self.release_date.strftime('%Y-%m')}"
                            )

                        # transform to valid date format
                        transformed_row["Transaction_Date"] = transaction_date.strftime("%Y-%m-%d")

                        # remove percentage sign
                        transformed_row["Publisher_Revenue_Perc"] = transformed_row["Publisher_Revenue_Perc"].strip("%")
                        # this field is not present for some publishers (UCL Press), for ANU Press the field value is
                        # “E-Book”
                        try:
                            transformed_row["Line_of_Business"]
                        except KeyError:
                            transformed_row["Line_of_Business"] = None
                    else:
                        # remove percentage sign
                        transformed_row["Buy_Link_CTR"] = transformed_row["Buy_Link_CTR"].strip("%")

                    results.append(transformed_row)
            results = add_partition_date(results, self.release_date, bigquery.TimePartitioningType.MONTH)
            list_to_jsonl_gz(self.transform_path(file), results)


class GoogleBooksTelescope(SnapshotTelescope):
    """The Google Books telescope."""

    DAG_ID_PREFIX = "google_books"

    def __init__(
        self,
        organisation: Organisation,
        dag_id: Optional[str] = None,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 1, 1),
        schedule_interval: str = "@monthly",
        dataset_id: str = "google",
        schema_folder: str = default_schema_folder(),
        catchup: bool = False,
        airflow_vars=None,
        airflow_conns=None,
    ):
        """Construct a GoogleBooksTelescope instance.

        :param organisation: the Organisation the DAG will process.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param schema_folder: the SQL schema path.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        """
        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]
        if airflow_conns is None:
            airflow_conns = [AirflowConns.SFTP_SERVICE]

        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation.name)

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
        )
        self.organisation = organisation
        self.project_id = organisation.gcp_project_id
        self.dataset_location = "us"  # TODO: add to API
        self.sftp_folders = SftpFolders(dag_id, organisation.name)
        self.sftp_regex = r"^Google(SalesTransaction|BooksTraffic)Report_\d{4}_\d{2}.csv$"

        self.add_setup_task_chain([self.check_dependencies, self.list_release_info])
        self.add_task_chain(
            [
                self.move_files_to_in_progress,
                self.download,
                self.upload_downloaded,
                self.transform,
                self.upload_transformed,
                self.bq_load_partition,
                self.move_files_to_finished,
                self.cleanup,
            ]
        )

    def make_release(self, **kwargs) -> List[GoogleBooksRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of google books release instances
        """
        ti: TaskInstance = kwargs["ti"]
        reports_info = ti.xcom_pull(
            key=GoogleBooksTelescope.RELEASE_INFO, task_ids=self.list_release_info.__name__, include_prior_dates=False
        )
        releases = []
        for release_date, sftp_files in reports_info.items():
            releases.append(
                GoogleBooksRelease(self.dag_id, pendulum.parse(release_date), sftp_files, self.organisation)
            )
        return releases

    def list_release_info(self, **kwargs):
        """Lists all Google Books releases available on the SFTP server and publishes sftp file paths and
        release_date's as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        release_info = defaultdict(list)

        with make_sftp_connection() as sftp:
            files = sftp.listdir(self.sftp_folders.upload)
            for file_name in files:
                if re.match(self.sftp_regex, file_name):
                    date_str = file_name[-11:].strip(".csv")
                    release_date = pendulum.from_format(date_str, "YYYY_MM").end_of("month")
                    release_date = release_date.format("YYYYMMDD")
                    sftp_file = os.path.join(self.sftp_folders.in_progress, file_name)
                    release_info[release_date].append(sftp_file)

        continue_dag = len(release_info)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs["ti"]
            ti.xcom_push(GoogleBooksTelescope.RELEASE_INFO, release_info)

        return continue_dag

    def move_files_to_in_progress(self, releases: List[GoogleBooksRelease], **kwargs):
        """Move Google Books files to SFTP in-progress folder.

        :param releases: a list of Google Books releases.
        :return: None.
        """

        for release in releases:
            self.sftp_folders.move_files_to_in_progress(release.sftp_files)

    def download(self, releases: List[GoogleBooksRelease], **kwargs):
        """Task to download the Google Books releases for a given month.

        :param releases: a list of Google Books releases.
        :return: None.
        """
        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[GoogleBooksRelease], **kwargs):
        """Task to upload the downloaded Google Books releases for a given month.

        :param releases: a list of Google Books releases.
        :return: None.
        """
        # Upload each downloaded release
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[GoogleBooksRelease], **kwargs):
        """Task to transform the Google Books releases for a given month.

        :param releases: a list of Google Books releases.
        :return: None.
        """
        # Transform each release
        for release in releases:
            release.transform()

    def bq_load_partition(self, releases: List[GoogleBooksRelease], **kwargs):
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

    def move_files_to_finished(self, releases: List[GoogleBooksRelease], **kwargs):
        """Move Google Books files to SFTP finished folder.

        :param releases: a list of Google Books releases.
        :return: None.
        """

        for release in releases:
            self.sftp_folders.move_files_to_finished(release.sftp_files)
