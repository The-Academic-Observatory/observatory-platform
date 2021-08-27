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
# Author: Aniek Roelofs

from __future__ import annotations

import base64
import csv
import logging
import os
import os.path
import shutil
from collections import OrderedDict
from typing import List, Optional

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.taskinstance import TaskInstance
from bs4 import BeautifulSoup, SoupStrainer
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed

from observatory.api.client.model.organisation import Organisation
from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.file_utils import list_to_jsonl_gz
from observatory.platform.utils.url_utils import get_user_agent
from observatory.platform.utils.workflow_utils import (
    SubFolder,
    blob_name,
    bq_load_partition,
    table_ids_from_path,
    workflow_path,
    upload_files_from_list,
)
from observatory.platform.utils.workflow_utils import (
    add_partition_date,
    convert,
    make_dag_id,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)


class JstorRelease(SnapshotRelease):
    def __init__(
        self, dag_id: str, release_date: pendulum.DateTime, reports_info: List[dict], organisation: Organisation
    ):
        """Construct a JstorRelease.

        :param release_date: the release date, corresponds to the last day of the month being processed..
        :param reports_info: list with report_type (country or institution) and url of reports
        """

        self.reports_info = reports_info
        download_files_regex = f"^{JstorTelescope.DAG_ID_PREFIX}_(country|institution)\.tsv$"
        transform_files_regex = f"^{JstorTelescope.DAG_ID_PREFIX}_(country|institution)\.jsonl.gz"

        super().__init__(dag_id, release_date, download_files_regex, transform_files_regex)
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

    def download_path(self, report_type: str) -> str:
        """Creates full download path

        :param report_type: The report type (country or institution)
        :return: Download path
        """
        return os.path.join(self.download_folder, f"{JstorTelescope.DAG_ID_PREFIX}_{report_type}.tsv")

    def transform_path(self, report_type: str) -> str:
        """Creates full transform path

        :param report_type: The report type (country or institution)
        :return: Transform path
        """
        return os.path.join(self.transform_folder, f"{JstorTelescope.DAG_ID_PREFIX}_{report_type}.jsonl.gz")

    def transform(self):
        """Transform a Jstor release into json lines format and gzip the result.

        :return: None.
        """
        for file in self.download_files:
            results = []
            with open(file) as tsv_file:
                csv_reader = csv.DictReader(tsv_file, delimiter="\t")
                for row in csv_reader:
                    transformed_row = OrderedDict((convert(k), v) for k, v in row.items())
                    results.append(transformed_row)

            results = add_partition_date(results, self.release_date, bigquery.TimePartitioningType.MONTH)
            report_type = "country" if "country" in file else "institution"
            list_to_jsonl_gz(self.transform_path(report_type), results)

    def cleanup(self) -> None:
        """Delete files of downloaded, extracted and transformed release. Add to parent method cleanup and assign a
        label to the gmail messages that have been processed.

        :return: None.
        """
        super().cleanup()

        service = create_gmail_service()
        label_id = get_label_id(service, JstorTelescope.PROCESSED_LABEL_NAME)
        for report in self.reports_info:
            message_id = report["id"]
            body = {"addLabelIds": [label_id]}
            response = service.users().messages().modify(userId="me", id=message_id, body=body).execute()
            try:
                message_id = response["id"]
                logging.info(
                    f"Added label '{JstorTelescope.PROCESSED_LABEL_NAME}' to GMAIL message, message_id: "
                    f"{message_id}"
                )
            except KeyError:
                raise AirflowException(f"Unsuccessful adding label to GMAIL message, message_id: {message_id}")


class JstorTelescope(SnapshotTelescope):
    """
    The JSTOR telescope.

    Saved to the BigQuery tables: <project_id>.jstor.jstor_countryYYYYMMDD and
    <project_id>.jstor.jstor_institutionYYYYMMDD
    """

    REPORTS_INFO = "reports_info"
    DAG_ID_PREFIX = "jstor"
    PROCESSED_LABEL_NAME = "processed_report"

    # download settings
    MAX_ATTEMPTS = 3
    FIXED_WAIT = 20  # seconds
    MAX_WAIT_TIME = 60 * 10  # seconds
    EXP_BASE = 3
    MULTIPLIER = 10

    def __init__(
        self,
        organisation: Organisation,
        publisher_id: str,
        dag_id: Optional[str] = None,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 1, 1),
        schedule_interval: str = "@monthly",
        dataset_id: str = "jstor",
        schema_folder: str = default_schema_folder(),
        source_format: SourceFormat = SourceFormat.NEWLINE_DELIMITED_JSON,
        dataset_description: str = "",
        catchup: bool = False,
        airflow_vars: List = None,
        airflow_conns: List = None,
        max_active_runs: int = 1,
    ):
        """Construct a JstorTelescope instance.
        :param organisation: the Organisation of which data is processed.
        :param publisher_id: the publisher ID, obtained from the 'extra' info from the API regarding the telescope.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
        :param source_format: the format of the data to load into BigQuery.
        :param dataset_description: description for the BigQuery dataset.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
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
            airflow_conns = [AirflowConns.GMAIL_API]

        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation.name)

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            source_format=source_format,
            dataset_description=dataset_description,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            max_active_runs=max_active_runs,
        )
        self.organisation = organisation
        self.project_id = organisation.gcp_project_id
        self.dataset_location = "us"  # TODO: add to API
        self.publisher_id = publisher_id

        self.add_setup_task_chain([self.check_dependencies, self.list_reports, self.download_reports])
        self.add_task_chain(
            [self.upload_downloaded, self.transform, self.upload_transformed, self.bq_load_partition, self.cleanup]
        )

    def make_release(self, **kwargs) -> List[JstorRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of grid release instances
        """

        ti: TaskInstance = kwargs["ti"]
        available_releases = ti.xcom_pull(
            key=JstorTelescope.RELEASE_INFO, task_ids=self.download_reports.__name__, include_prior_dates=False
        )
        releases = []
        for release_date in available_releases:
            reports_info = available_releases[release_date]
            releases.append(JstorRelease(self.dag_id, pendulum.parse(release_date), reports_info, self.organisation))
        return releases

    def check_dependencies(self, **kwargs) -> bool:
        """Check dependencies of DAG. Add to parent method to additionally check for a publisher id
        :return: True if dependencies are valid.
        """
        super().check_dependencies()

        if self.publisher_id is None:
            expected_extra = {"publisher_id": "jstor_publisher_id"}
            raise AirflowException(f"Publisher ID is not set in 'extra' of telescope, extra example: {expected_extra}")
        return True

    def list_reports(self, **kwargs) -> bool:
        """Lists all Jstor releases for a given month and publishes their report_type, download_url and
        release_date's as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: Whether to continue the DAG
        """

        service = create_gmail_service()
        label_id = get_label_id(service, self.PROCESSED_LABEL_NAME)
        available_reports = list_reports(service, self.publisher_id, label_id)

        continue_dag = len(available_reports) > 0
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs["ti"]
            ti.xcom_push(JstorTelescope.REPORTS_INFO, available_reports)

        return continue_dag

    def download_reports(self, **kwargs) -> bool:
        """Download the JSTOR reports based on the list with available reports.
        The release date for each report is only known after downloading the report. Therefore they are first
        downloaded to a temporary location, afterwards the release info can be pushed as an xcom and the report is
        moved to the correct location.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: Whether to continue the DAG (always True)
        """
        ti: TaskInstance = kwargs["ti"]
        available_reports = ti.xcom_pull(
            key=JstorTelescope.REPORTS_INFO, task_ids=self.list_reports.__name__, include_prior_dates=False
        )
        reports_folder = workflow_path(SubFolder.downloaded.value, self.dag_id, "tmp_reports")
        available_releases = {}
        for report in available_reports:
            # Download report to temporary file
            url = report["url"]
            tmp_download_path = os.path.join(reports_folder, "report.tsv")
            download_report(url, tmp_download_path)

            # Get the release date
            release_date = get_release_date(tmp_download_path)

            # Create temporarily release and move report to correct path
            release = JstorRelease(self.dag_id, release_date, [report], self.organisation)
            shutil.move(tmp_download_path, release.download_path(report["type"]))

            release_date = release_date.format("YYYYMMDD")

            # Add reports to list with available releases
            try:
                available_releases[release_date].append(report)
            except KeyError:
                available_releases[release_date] = [report]

        ti.xcom_push(JstorTelescope.RELEASE_INFO, available_releases)
        return True

    def upload_downloaded(self, releases: List[JstorRelease], **kwargs):
        """Task to upload the downloaded Jstor releases for a given month.

        :param releases: a list of Jstor releases.
        :return: None.
        """

        # Upload each downloaded release
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[JstorRelease], **kwargs):
        """Task to transform the Jstor releases for a given month.

        :param releases: a list of Jstor releases.
        :return: None.
        """

        # Transform each release
        for release in releases:
            release.transform()

    def bq_load_partition(self, releases: List[JstorRelease], **kwargs):
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


def create_headers() -> dict:
    """Create a headers dict that can be used to make a request

    :return: headers dictionary
    """

    headers = {"User-Agent": get_user_agent(package_name="oaebu_workflows")}
    return headers


@retry(
    stop=stop_after_attempt(JstorTelescope.MAX_ATTEMPTS),
    reraise=True,
    wait=wait_fixed(JstorTelescope.FIXED_WAIT)
    + wait_exponential(
        multiplier=JstorTelescope.MULTIPLIER, exp_base=JstorTelescope.EXP_BASE, max=JstorTelescope.MAX_WAIT_TIME
    ),
)
def get_header_info(url: str) -> [str, str]:
    """Get header info from url and parse for filename and extension of file.

    :param url: Download url
    :return: Filename and file extension
    """
    logging.info(
        f"Getting HEAD of report: {url}, "
        f'attempt: {get_header_info.retry.statistics["attempt_number"]}, '
        f'idle for: {get_header_info.retry.statistics["idle_for"]}'
    )
    response = requests.head(url, allow_redirects=True, headers=create_headers())
    if response.status_code != 200:
        raise AirflowException(
            f"Could not get HEAD of report download url, reason: {response.reason}, "
            f"status_code: {response.status_code}"
        )
    filename, extension = response.headers["Content-Disposition"].split("=")[1].split(".")
    return filename, extension


@retry(
    stop=stop_after_attempt(JstorTelescope.MAX_ATTEMPTS),
    reraise=True,
    wait=wait_fixed(JstorTelescope.FIXED_WAIT)
    + wait_exponential(
        multiplier=JstorTelescope.MULTIPLIER, exp_base=JstorTelescope.EXP_BASE, max=JstorTelescope.MAX_WAIT_TIME
    ),
)
def download_report(url: str, download_path: str):
    """Download report from url to a file.

    :param url: Download url
    :param download_path: Path to download data to
    :return: Whether download was successful
    """
    logging.info(
        f"Downloading report: {url}, "
        f"to: {download_path}, "
        f'attempt: {download_report.retry.statistics["attempt_number"]}, '
        f'idle for: {download_report.retry.statistics["idle_for"]}'
    )
    response = requests.get(url, headers=create_headers())
    if response.status_code != 200:
        raise AirflowException(
            f"Could not download content from report url, reason: {response.reason}, "
            f"status_code: {response.status_code}"
        )

    content = response.content.decode("utf-8")
    with open(download_path, "w") as f:
        f.write(content)


def get_release_date(report_path: str) -> pendulum.DateTime:
    """Get the release date from the "Usage Month" column in the first row of the report.
    Also checks if the reports contains data from the same month only.

    :param report_path: The path to the JSTOR report
    :return: The release date, defaults to end of the month
    """
    # Load report data into list of dicts
    with open(report_path) as tsv_file:
        csv_list = list(csv.DictReader(tsv_file, delimiter="\t"))

    # get the first and last usage month
    first_usage_month = csv_list[0]["Usage Month"]
    last_usage_month = csv_list[-1]["Usage Month"]

    # check that month in first and last row are the same
    if first_usage_month != last_usage_month:
        logging.info(
            f"Report contains data from more than 1 month, start month: {first_usage_month}, "
            f"end month: {last_usage_month}"
        )

    # get the release date from the last usage month
    release_date = pendulum.from_format(last_usage_month, "YYYY-MM").end_of("month")

    return release_date


def create_gmail_service() -> Resource:
    """Build the gmail service.

    :return: Gmail service instance
    """
    gmail_api_conn = BaseHook.get_connection(AirflowConns.GMAIL_API)
    scopes = ["https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.modify"]
    creds = Credentials.from_authorized_user_info(gmail_api_conn.extra_dejson, scopes=scopes)

    service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    return service


def get_label_id(service: Resource, label_name: str) -> str:
    """Get the id of a label based on the label name.

    :param service: Gmail service
    :param label_name: The name of the label
    :return: The label id
    """
    existing_labels = service.users().labels().list(userId="me").execute()["labels"]
    label_id = [label["id"] for label in existing_labels if label["name"] == label_name]
    if label_id:
        label_id = label_id[0]
    else:
        # create label
        label_body = {
            "name": label_name,
            "messageListVisibility": "show",
            "labelListVisibility": "labelShow",
            "type": "user",
        }
        result = service.users().labels().create(userId="me", body=label_body).execute()
        label_id = result["id"]
    return label_id


def message_has_label(message: dict, label_id: str) -> bool:
    """Checks if a message has the given label

    :param message: A Gmail message
    :param label_id: The label id
    :return: True if the message has the label
    """
    label_ids = message["labelIds"]
    for label in label_ids:
        if label == label_id:
            return True


def list_reports(service: Resource, publisher_id: str, processed_label_id: str) -> List[dict]:
    """List the available releases by going through the messages of a gmail account and looking for a specific pattern.

    If a message has been processed previously it has a specific label, messages with this label will be skipped.
    The message should include a download url. The head of this download url contains the filename, from which the
    release date and publisher can be derived.

    :param service: Gmail service
    :param publisher_id: Id of the publisher
    :param processed_label_id: Id of the 'processed_reports' label
    :return: Dictionary with release dates as key and reports info as value, where reports info is a list of country
    and/or institution reports.
    """

    available_reports = []
    # list messages with specific query
    results = (
        service.users()
        .messages()
        .list(userId="me", q='subject:"JSTOR Publisher Report Available"', labelIds=["INBOX"])
        .execute()
    )
    for message_info in results["messages"]:
        message_id = message_info["id"]
        message = service.users().messages().get(userId="me", id=message_id).execute()

        # check if message has processed label id
        if message_has_label(message, processed_label_id):
            continue

        # get download url
        download_url = None
        message_data = base64.urlsafe_b64decode(message["payload"]["body"]["data"])
        for link in BeautifulSoup(message_data, "html.parser", parse_only=SoupStrainer("a")):
            if link.text == "Download Completed Report":
                download_url = link["href"]
                break
        if download_url is None:
            raise AirflowException(f"Can't find download link for report in e-mail, message snippet: {message.snippet}")

        # get filename and extension from head
        filename, extension = get_header_info(download_url)

        # get publisher
        report_publisher = filename.split("_")[1]
        if report_publisher != publisher_id:
            continue

        # get report_type
        report_mapping = {"PUBBCU": "country", "PUBBIU": "institution"}
        report_type = report_mapping.get(filename.split("_")[2])
        if report_type is None:
            logging.info(f"Skipping unrecognized report type, filename {filename}")

        # check format
        original_email = f"https://mail.google.com/mail/u/0/#all/{message_id}"
        if extension == "tsv":
            # add report info
            logging.info(
                f"Adding report. Report type: {report_type}, url: {download_url}, " f"original email: {original_email}."
            )
            available_reports.append({"type": report_type, "url": download_url, "id": message_id})
        else:
            logging.warning(
                f'Excluding file "{filename}.{extension}", as it does not have ".tsv" extension. '
                f"Original email: {original_email}"
            )

    return available_reports
