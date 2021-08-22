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

# Author: Tuan Chien


import calendar
import json
import logging
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Empty, Queue
from threading import Event
from time import sleep
from typing import Any, Dict, List, Tuple, Type, Union
from urllib.error import HTTPError
from urllib.parse import quote_plus

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat, WriteDisposition
from ratelimit import limits, sleep_and_retry

from observatory.dags.config import schema_path
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable
from observatory.platform.utils.airflow_utils import AirflowVars, check_variables
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.file_utils import write_to_file, zip_files
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    create_bigquery_dataset,
    load_bigquery_table,
)
from observatory.platform.utils.url_utils import get_ao_user_agent
from observatory.platform.utils.workflow_utils import SubFolder, workflow_path
from observatory.platform.utils.workflow_utils import (
    build_schedule,
    delete_msg_files,
    get_as_list,
    get_entry_or_none,
    json_to_db,
    upload_telescope_file_list,
    validate_date,
)


class ScopusRelease:
    """Used to store info on a given SCOPUS release."""

    def __init__(
        self,
        inst_id: str,
        scopus_inst_id: List[str],
        release_date: pendulum.DateTime,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        project_id: str,
        download_bucket_name: str,
        transform_bucket_name: str,
        data_location: str,
        schema_ver: str,
    ):
        """Constructor.

        :param inst_id: institution id from the airflow connection (minus the scopus_)
        :param scopus_inst_id: List of institution ids to use in the SCOPUS query.
        :param release_date: Release date (currently the execution date).
        :param start_date: Start date of the dag where to start pulling records from.
        :param end_date: End of records to pull.
        :param project_id: The project id to use.
        :param download_bucket_name: Download bucket name to use for storing downloaded files in the cloud.
        :param transform_bucket_name: Transform bucket name to use for storing transformed files in the cloud.
        :param data_location: Location of the data servers
        """

        self.inst_id = inst_id
        self.scopus_inst_id = sorted(scopus_inst_id)
        self.release_date = release_date
        self.start_date = start_date
        self.end_date = end_date
        self.download_path = workflow_path(SubFolder.downloaded, ScopusTelescope.DAG_ID)
        self.transform_path = workflow_path(SubFolder.transformed, ScopusTelescope.DAG_ID)
        self.telescope_path = f"telescopes/{ScopusTelescope.DAG_ID}/{release_date.date()}"
        self.project_id = project_id
        self.download_bucket_name = download_bucket_name
        self.transform_bucket_name = transform_bucket_name
        self.data_location = data_location
        self.schema_ver = schema_ver


class ScopusTelescope:
    """A container for holding the constants and static functions for the SCOPUS telescope."""

    DAG_ID = "scopus"
    ID_STRING_OFFSET = len(DAG_ID) + 1
    DESCRIPTION = "SCOPUS: https://www.scopus.com"

    # Elsevier's example call
    API_SERVER_CHECK_URL = (
        "https://api.elsevier.com/content/search/scopus?query=all(gene)&" "apiKey=7f59af901d2d86f78a1fd60c1bf9426a"
    )

    SCHEDULE_INTERVAL = "@monthly"
    QUEUE = "default"
    RETRIES = 3
    DATASET_ID = "elsevier"
    SCHEMA_PATH = "telescopes"
    TABLE_NAME = DAG_ID
    SCHEMA_VER = "scopus1"  # Internal version. SCOPUS returns no version in response.

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_CHECK_API_SERVER = "check_api_server"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_UPLOAD_DOWNLOADED = "upload_downloaded"
    TASK_ID_TRANSFORM_DB_FORMAT = "transform_db_format"
    TASK_ID_UPLOAD_TRANSFORMED = "upload_transformed"
    TASK_ID_BQ_LOAD = "bq_load"
    TASK_ID_CLEANUP = "cleanup"

    XCOM_RELEASES = "releases"
    XCOM_DOWNLOAD_PATH = "download_path"
    XCOM_UPLOAD_ZIP_PATH = "download_zip_path"
    XCOM_JSON_PATH = "json_path"
    XCOM_JSON_HARVEST = "json_harvest"
    XCOM_JSONL_PATH = "jsonl_path"
    XCOM_JSONL_ZIP_PATH = "jsonl_zip_path"
    XCOM_JSONL_BLOB_PATH = "jsonl_blob_path"

    DOWNLOAD_MODE = "sequential"  # Valid options: ['sequential', 'parallel']

    @staticmethod
    def check_dependencies(**kwargs):
        """Check that all variables exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        vars_valid = check_variables(
            AirflowVars.DATA_PATH,
            AirflowVars.PROJECT_ID,
            AirflowVars.DATA_LOCATION,
            AirflowVars.DOWNLOAD_BUCKET,
            AirflowVars.TRANSFORM_BUCKET,
        )
        if not vars_valid:
            raise AirflowException("Required variables are missing")

        conn = kwargs["conn"]

        # Validate extra field is set correctly.
        logging.info(f"Validating json in extra field of {conn}")
        extra = conn.extra
        try:
            extra_dict = json.loads(extra)
        except Exception as e:
            raise AirflowException(f"Error processing json extra fields in {conn} connection id profile: {e}")

        logging.info(f"Validating extra field keys for {conn}")

        # Check date is ok
        start_date = extra_dict["start_date"]
        if not validate_date(start_date):
            raise AirflowException(f"Invalid date string for {conn}: {start_date}")

        # Check institution id is set
        if "id" not in extra_dict:
            raise AirflowException(f'The "id" field is not set for {conn}.')

        # Check API keys are present
        if "api_keys" not in extra_dict or len(extra_dict["api_keys"]) == 0:
            raise AirflowException(f"No API keys are set for {conn}.")

        logging.info(f"Checking for airflow override variables of {conn}")
        # Set project id override
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        if "project_id" in extra_dict:
            project_id = extra_dict["project_id"]
            logging.info(f"Override for project_id found. Using: {project_id}")

        # Set download bucket name override
        download_bucket_name = Variable.get(AirflowVars.DOWNLOAD_BUCKET)
        if "download_bucket_name" in extra_dict:
            download_bucket_name = extra_dict["download_bucket_name"]
            logging.info(f"Override for download_bucket_name found. Using: {download_bucket_name}")

        # Set transform bucket name override
        transform_bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET)
        if "transform_bucket_name" in extra_dict:
            transform_bucket_name = extra_dict["transform_bucket_name"]
            logging.info(f"Override for transform_bucket_name found. Using: {transform_bucket_name}")

        # Set data location override
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        if "data_location" in extra_dict:
            data_location = extra_dict["data_location"]
            logging.info(f"Override for data_location found. Using: {data_location}")

        # Push release information for other tasks
        scopus_inst_id = get_as_list(extra_dict, "id")
        release = ScopusRelease(
            inst_id=kwargs["institution"],
            scopus_inst_id=scopus_inst_id,
            release_date=kwargs["execution_date"].date(),
            start_date=pendulum.parse(start_date).date(),
            end_date=pendulum.parse(kwargs["dag_start"]).date(),
            project_id=project_id,
            download_bucket_name=download_bucket_name,
            transform_bucket_name=transform_bucket_name,
            data_location=data_location,
            schema_ver=ScopusTelescope.SCHEMA_VER,
        )

        logging.info(
            f"ScopusRelease contains:\ndownload_bucket_name: {release.download_bucket_name}, transform_bucket_name: "
            + f"{release.transform_bucket_name}, data_location: {release.data_location}"
        )

        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(ScopusTelescope.XCOM_RELEASES, release)

    @staticmethod
    def check_api_server():
        """Check that the API server is still contactable.

        :return: None.
        """

        http_ok = 200

        try:
            http_code = urllib.request.urlopen(ScopusTelescope.API_SERVER_CHECK_URL).getcode()
        except HTTPError as e:
            raise ValueError(f"Failed to fetch url because of: {e}")

        if http_code != http_ok:
            raise ValueError(f"HTTP response code {http_code} received.")

    @staticmethod
    def download(**kwargs):
        """Task to download the SCOPUS snapshots.

        Pushes the following xcom:
            download_path (str): the path to a pickled file containing the list of xml responses in a month query.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: ScopusRelease = ScopusTelescope.pull_release(ti)

        # Prepare paths
        extra = json.loads(kwargs["conn"].extra)
        api_keys = get_as_list(extra, "api_keys")
        download_files = ScopusUtility.download_snapshot(api_keys, release, "sequential")

        # Notify next task of the files downloaded.
        ti.xcom_push(ScopusTelescope.XCOM_DOWNLOAD_PATH, download_files)

    @staticmethod
    def upload_downloaded(**kwargs):
        """Task to upload the downloaded SCOPUS snapshots.

        Pushes the following xcom:
            upload_zip_path (str): the path to pickle zip file of downloaded response.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: ScopusRelease = ScopusTelescope.pull_release(ti)

        # Pull messages
        download_list = ti.xcom_pull(
            key=ScopusTelescope.XCOM_DOWNLOAD_PATH, task_ids=ScopusTelescope.TASK_ID_DOWNLOAD, include_prior_dates=False
        )

        # Upload each snapshot
        logging.info("upload_downloaded: zipping and uploading downloaded files")
        zip_list = zip_files(download_list)
        upload_telescope_file_list(release.download_bucket_name, release.inst_id, release.telescope_path, zip_list)

        json_harvest = list()
        for file in download_list:
            file_name = os.path.basename(file)
            end = file_name.find("_")
            harvest_datetime = file_name[:end]
            logging.info(f"Harvest date time: {harvest_datetime}")
            json_harvest.append(harvest_datetime)

        # Notify next task of the files downloaded.
        ti.xcom_push(ScopusTelescope.XCOM_JSON_PATH, download_list)
        ti.xcom_push(ScopusTelescope.XCOM_UPLOAD_ZIP_PATH, zip_list)
        ti.xcom_push(ScopusTelescope.XCOM_JSON_HARVEST, json_harvest)

    @staticmethod
    def transform_db_format(**kwargs):
        """Task to transform the json into db field format (and in jsonlines form).

        Pushes the following xcom:
            version (str): the version of the SCOPUS release.
            json_gz_file_name (str): the file name for the transformed SCOPUS release.
            json_gz_file_path (str): the path to the transformed SCOPUS release (including file name).

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: ScopusRelease = ScopusTelescope.pull_release(ti)

        # Pull messages
        json_files = ti.xcom_pull(
            key=ScopusTelescope.XCOM_JSON_PATH,
            task_ids=ScopusTelescope.TASK_ID_UPLOAD_DOWNLOADED,
            include_prior_dates=False,
        )

        harvest_times = ti.xcom_pull(
            key=ScopusTelescope.XCOM_JSON_HARVEST,
            task_ids=ScopusTelescope.TASK_ID_UPLOAD_DOWNLOADED,
            include_prior_dates=False,
        )

        json_harvest_pair = list(zip(json_files, harvest_times))
        json_harvest_pair.sort()  # Sort by institution and date
        print(f"json harvest pairs are:\n{json_harvest_pair}")

        # Apply field extraction and transformation to jsonlines
        logging.info("transform_db_format: parsing and transforming into db format")
        path_prefix = os.path.join(release.transform_path, release.release_date.isoformat(), release.inst_id)
        jsonl_list = json_to_db(
            json_harvest_pair,
            release.release_date.isoformat(),
            ScopusJsonParser.parse_json,
            release.scopus_inst_id,
            path_prefix,
        )

        # Notify next task
        ti.xcom_push(ScopusTelescope.XCOM_JSONL_PATH, jsonl_list)

    @staticmethod
    def upload_transformed(**kwargs):
        """Task to upload the transformed SCOPUS data into jsonlines files.

        Pushes the following xcom:
            release_date (str): the release date of the SCOPUS release.
            blob_name (str): the name of the blob on the Google Cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: ScopusRelease = ScopusTelescope.pull_release(ti)

        # Pull messages
        jsonl_paths = ti.xcom_pull(
            key=ScopusTelescope.XCOM_JSONL_PATH,
            task_ids=ScopusTelescope.TASK_ID_TRANSFORM_DB_FORMAT,
            include_prior_dates=False,
        )

        # Upload each snapshot
        logging.info("upload_transformed: zipping and uploading jsonlines to cloud")
        zip_list = zip_files(jsonl_paths)
        blob_list = upload_telescope_file_list(
            release.transform_bucket_name, release.inst_id, release.telescope_path, zip_list
        )

        # Notify next task of the files downloaded.
        ti.xcom_push(ScopusTelescope.XCOM_JSONL_BLOB_PATH, blob_list)
        ti.xcom_push(ScopusTelescope.XCOM_JSONL_ZIP_PATH, zip_list)

    @staticmethod
    def bq_load(**kwargs):
        """Task to load the transformed SCOPUS snapshot into BigQuery.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: ScopusRelease = ScopusTelescope.pull_release(ti)

        jsonl_zip_blobs = ti.xcom_pull(
            key=ScopusTelescope.XCOM_JSONL_BLOB_PATH,
            task_ids=ScopusTelescope.TASK_ID_UPLOAD_TRANSFORMED,
            include_prior_dates=False,
        )

        # Create dataset
        create_bigquery_dataset(
            release.project_id, ScopusTelescope.DATASET_ID, release.data_location, ScopusTelescope.DESCRIPTION
        )

        # Create table id
        table_id = bigquery_sharded_table_id(ScopusTelescope.TABLE_NAME, release.release_date)

        # Load into BigQuery
        for file in jsonl_zip_blobs:
            schema_file_path = find_schema(
                schema_path(), ScopusTelescope.TABLE_NAME, release.release_date, "", release.schema_ver
            )
            if schema_file_path is None:
                logging.error(
                    f"No schema found with search parameters: analysis_schema_path={ScopusTelescope.SCHEMA_PATH}, "
                    f"table_name={ScopusTelescope.TABLE_NAME}, release_date={release.release_date}, "
                    f"schema_ver={release.schema_ver}"
                )
                exit(os.EX_CONFIG)

            # Load BigQuery table
            uri = f"gs://{release.transform_bucket_name}/{file}"
            logging.info(f"URI: {uri}")

            load_bigquery_table(
                uri,
                ScopusTelescope.DATASET_ID,
                release.data_location,
                table_id,
                schema_file_path,
                SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=WriteDisposition.WRITE_APPEND,
            )

    @staticmethod
    def cleanup(**kwargs):
        """Delete files of downloaded, extracted and transformed releases.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]

        delete_msg_files(ti, ScopusTelescope.XCOM_DOWNLOAD_PATH, ScopusTelescope.TASK_ID_DOWNLOAD)
        delete_msg_files(ti, ScopusTelescope.XCOM_UPLOAD_ZIP_PATH, ScopusTelescope.TASK_ID_UPLOAD_DOWNLOADED)
        delete_msg_files(ti, ScopusTelescope.XCOM_JSONL_PATH, ScopusTelescope.TASK_ID_TRANSFORM_DB_FORMAT)
        delete_msg_files(ti, ScopusTelescope.XCOM_JSONL_ZIP_PATH, ScopusTelescope.TASK_ID_UPLOAD_TRANSFORMED)

    @staticmethod
    def pull_release(ti: TaskInstance):
        """Get the ScopusRelease object from XCOM message."""
        return ti.xcom_pull(
            key=ScopusTelescope.XCOM_RELEASES,
            task_ids=ScopusTelescope.TASK_ID_CHECK_DEPENDENCIES,
            include_prior_dates=False,
        )


class ScopusClientThrottleLimits:
    """API throttling constants for ScopusClient."""

    CALL_LIMIT = 1  # SCOPUS allows 2 api calls / second.
    CALL_PERIOD = 1  # seconds


class ScopusClient:
    """Handles URL fetching of SCOPUS search."""

    RESULTS_PER_PAGE = 25
    MAX_RESULTS = 5000  # Upper limit on number of results returned
    QUOTA_EXCEED_ERROR_PREFIX = "QuotaExceeded. Resets at: "

    def __init__(self, api_key: str, view: str = "standard"):
        """Constructor.

        :param api_key: API key.
        :param view: The 'view' access level. Can be 'standard' or 'complete'.
        """

        self._headers = {"X-ELS-APIKey": api_key, "Accept": "application/json", "User-Agent": get_ao_user_agent()}

        self._view = view

    @staticmethod
    def get_reset_date_from_error(msg: str) -> int:
        """Get the reset date timestamp in seconds from the exception message.

        :param msg: exception message.
        :return: Reset date timestamp in seconds.
        """

        ts_offset = len(ScopusClient.QUOTA_EXCEED_ERROR_PREFIX)
        return int(msg[ts_offset:]) / 1000  # Elsevier docs says reports seconds, but headers report milliseconds.

    @staticmethod
    def get_next_page_url(links: str) -> Union[None, str]:
        """Get the URL for the next result page.

        :param links: The list of links returned from the last query.
        :return None if next page not found, otherwise string to next page's url.
        """

        for link in links:
            if link["@ref"] == "next":
                return link["@href"]
        return None

    @sleep_and_retry
    @limits(calls=ScopusClientThrottleLimits.CALL_LIMIT, period=ScopusClientThrottleLimits.CALL_PERIOD)
    def retrieve(self, query: str) -> Tuple[List[Dict[str, Any]], int, int]:
        """Execute the query.

        :param query: Query string.
        :return: (results of query, quota remaining, quota reset date timestamp in seconds)
        """

        http_ok = 200
        http_quota_exceeded = 429

        url = f"https://api.elsevier.com/content/search/scopus?view={self._view}&query={quote_plus(query)}"
        request = urllib.request.Request(url, headers=self._headers)
        results = list()

        while len(results) < ScopusClient.MAX_RESULTS:
            response = urllib.request.urlopen(request)
            quota_remaining = response.getheader("X-RateLimit-Remaining")
            quota_reset = response.getheader("X-RateLimit-Reset")

            request_code = response.getcode()
            if request_code == http_quota_exceeded:
                raise Exception(f"{ScopusClient.QUOTA_EXCEED_ERROR_PREFIX}{quota_reset}")

            response_dict = json.loads(response.read().decode("utf-8"))

            if request_code != http_ok:
                raise Exception(f"HTTP {request_code}:{response_dict}")

            if "search-results" not in response_dict:
                break

            results += response_dict["search-results"]["entry"]

            total_results = int(response_dict["search-results"]["opensearch:totalResults"])
            if len(results) == total_results:
                break

            url = ScopusClient.get_next_page_url(response_dict["search-results"]["link"])
            if url is None:
                return results, quota_remaining, quota_reset

            request = urllib.request.Request(url, headers=self._headers)

        return results, quota_remaining, quota_reset


class ScopusUtilWorker:
    """Worker class"""

    DEFAULT_KEY_QUOTA = 20000  # API key query limit default per 7 days.
    QUEUE_WAIT_TIME = 20  # Wait time for Queue.get() call

    def __init__(self, client_id: int, client: ScopusClient, quota_reset_date: pendulum.DateTime, quota_remaining: int):
        """Constructor.

        :param client_id: Client id to use for debug messages so we don't leak the API key.
        :param client: ElsClient object for an API key.
        :param quota_reset_date: Date at which the quota will reset.
        """

        self.client_id = client_id
        self.client = client
        self.quota_reset_date = quota_reset_date
        self.quota_remaining = quota_remaining


class ScopusUtility:
    """Handles the SCOPUS interactions."""

    @staticmethod
    def build_query(scopus_inst_id: List[str], period: Type[pendulum.Period]) -> str:
        """Build a SCOPUS API query.

        :param scopus_inst_id: List of Institutional ID to query, e.g, ["60031226"] (Curtin University)
        :param period: A schedule period.
        :return: Constructed web query.
        """

        tail_offset = -4  # To remove ' or ' and ' OR ' from tail of string

        organisations = str()
        for i, inst in enumerate(scopus_inst_id):
            organisations += f"AF-ID({inst}) OR "
        organisations = organisations[:tail_offset]

        # Build publication date range
        search_months = str()

        for point in period.range("months"):
            month_name = calendar.month_name[point.month]
            search_months += f'"{month_name} {point.year}" or '
        search_months = search_months[:tail_offset]

        query = f"({organisations}) AND PUBDATETXT({search_months})"
        return query

    @staticmethod
    def download_scopus_period(
        worker: ScopusUtilWorker, conn: str, period: Type[pendulum.Period], inst_id: List[str], download_path: str
    ) -> str:
        """Download records for a stated date range.
        The elsapy package currently has a cap of 5000 results per query. So in the unlikely event any institution has
        more than 5000 entries per month, this will present a problem.

        :param worker: Worker that will do the downloading.
        :param conn: Connection ID from Airflow (minus scopus_)
        :param period: Period to download.
        :param inst_id: List of institutions to query concurrently.
        :param download_path: Path to save downloaded files to.
        :return: Saved file name.
        """

        timestamp = pendulum.now("UTC").isoformat()
        save_file = os.path.join(download_path, f"{timestamp}_{period.start}_{period.end}.json")
        logging.info(f"{conn} worker {worker.client_id}: retrieving period {period.start} - {period.end}")
        query = ScopusUtility.build_query(inst_id, period)
        result, num_results = ScopusUtility.make_query(worker, query)

        if num_results >= ScopusClient.MAX_RESULTS:
            logging.warning(
                f"{conn}: Result limit {ScopusClient.MAX_RESULTS} reached for {period.start} - {period.end}"
            )

        write_to_file(result, save_file)
        return save_file

    @staticmethod
    def sleep_if_needed(reset_date: pendulum.DateTime, conn: str):
        """Sleep until reset_date.

        :param reset_date: Date(time) to sleep to.
        :param conn: Connection id from Airflow.
        """

        now = pendulum.now("UTC")
        sleep_time = (reset_date - now).seconds + 1
        if sleep_time > 0:
            logging.info(f"{conn}: Sleeping for {sleep_time} seconds until a worker is ready.")
            sleep(sleep_time)

    @staticmethod
    def qe_worker_maintenance(qe_workers: List[ScopusUtilWorker], workerq: Queue, conn: str) -> List[ScopusUtilWorker]:
        """Add workers back that have quotas and maintain list of timed out workers.

        :param qe_workers: List of timed out workers.
        :param workerq: Worker queue to add ready workers back to.
        :param conn: Connection id from Airflow.
        :return: Updated list of timed out workers.
        """

        now = pendulum.now("UTC")
        for worker in qe_workers:
            if now >= worker.quota_reset_date:
                logging.info(f"{conn}: adding client {worker.client_id} back to queue.")
                workerq.put(worker)
        return [x for x in qe_workers if now < x.quota_reset_date]

    @staticmethod
    def update_reset_date(
        conn: str, error_msg: str, worker: ScopusUtilWorker, curr_reset_date: pendulum.DateTime
    ) -> pendulum.DateTime:
        """Update the reset date to closest date that will make a worker available.

        :param conn: Airflow connection ID.
        :param error_msg: Error message from quota exceeded exception.
        :param worker: Worker that will do the downloading.
        :param curr_reset_date: The current closest reset date.
        :return: Updated closest reset date.
        """

        now = pendulum.now("UTC")
        renews_ts = ScopusClient.get_reset_date_from_error(error_msg)
        worker.quota_reset_date = pendulum.from_timestamp(renews_ts)

        logging.warning(f"{conn} worker {worker.client_id}: quoted exceeded. New reset date: {worker.quota_reset_date}")

        if now > curr_reset_date:
            return worker.quota_reset_date

        return min(worker.quota_reset_date, curr_reset_date)

    @staticmethod
    def download_sequential(
        workers: List[ScopusUtilWorker], taskq: Queue, conn: str, inst_id: List[str], download_path: str
    ) -> List[str]:
        """Download SCOPUS snapshot sequentially. Tasks will be distributed in a round robin to the available keys.
        Block each task until it's done or failed.

        :param workers: list of available workers.
        :param taskq: task queue.
        :param conn: Airflow connection ID.
        :param inst_id: List of institutions to query concurrently.
        :param download_path: Path to save downloaded files to.
        """

        qe_workers = list()  # Quota exceeded workers
        saved_files = list()
        reset_date = pendulum.now("UTC")
        workerq = Queue()

        for worker in workers:
            workerq.put(worker)

        while True:
            # Update worker queue and timed out worker list
            qe_workers = ScopusUtility.qe_worker_maintenance(qe_workers, workerq, conn)

            try:
                logging.info(f"{conn}: attempting to get task")
                task = taskq.get(block=True, timeout=ScopusUtilWorker.QUEUE_WAIT_TIME)
            except Empty:
                logging.info(f"{conn}: task queue empty. All work done.")
                break

            try:
                logging.info(f"{conn}: attempting to get free worker")
                worker = workerq.get(block=True, timeout=ScopusUtilWorker.QUEUE_WAIT_TIME)

            except Empty:
                logging.info(f"{conn}: no free workers. Sleeping until worker available.")
                ScopusUtility.sleep_if_needed(reset_date, conn)
                taskq.put(task)  # Got task but no worker, so put task back and try again.
                continue

            try:  # Got task and worker. Try to download.
                saved_file = ScopusUtility.download_scopus_period(worker, conn, task, inst_id, download_path)
                saved_files.append(saved_file)
                workerq.put(worker)
            except Exception as e:
                error_msg = str(e)
                if error_msg.startswith(ScopusClient.QUOTA_EXCEED_ERROR_PREFIX):
                    reset_date = ScopusUtility.update_reset_date(conn, error_msg, worker, reset_date)
                    taskq.put(task)
                    qe_workers.append(worker)
                    continue  # Quota exceeded while trying task with worker, so put task back an try again
                else:
                    raise AirflowException(
                        f"{conn}: uncaught exception processing worker {worker.client_id} and task {task} "
                        f"with message {error_msg}"
                    )

        return saved_files

    @staticmethod
    def download_worker(
        worker: ScopusUtilWorker, exit_event: Event, taskq: Queue, conn: str, inst_id: List[str], download_path: str
    ) -> List[str]:
        """Download worker method used by parallel downloader. Pulls tasks from queue when ready. No
            load balance guarantee.

        :param worker: worker to use.
        :param exit_event: exit event to monitor.
        :param taskq: tasks queue.
        :param conn: Airflow connection ID.
        :param inst_id: List of institutions to query concurrently.
        :param download_path: Path to save downloaded files to.
        :return: List of downloaded files.
        """

        saved_files = list()

        while True:
            ScopusUtility.sleep_if_needed(worker.quota_reset_date, conn)

            try:
                logging.info(f"{conn} worker {worker.client_id}: attempting to get a task")
                task = taskq.get(block=True, timeout=ScopusUtilWorker.QUEUE_WAIT_TIME)
            except Empty:
                if exit_event.is_set():
                    logging.info(f"{conn} worker {worker.client_id}: received exit event. Returning results.")
                    break
                logging.info(f"{conn} worker {worker.client_id}: get task timeout. Retrying.")
                continue

            try:  # Got task. Try to download.
                saved_file = ScopusUtility.download_scopus_period(worker, conn, task, inst_id, download_path)
                saved_files.append(saved_file)
                taskq.task_done()
            except Exception as e:
                error_msg = str(e)
                if error_msg.startswith(ScopusClient.QUOTA_EXCEED_ERROR_PREFIX):
                    reset_date = ScopusUtility.update_reset_date(conn, error_msg, worker, worker.quota_reset_date)
                    taskq.put(task)
                    taskq.task_done()
                    continue

        return saved_files

    @staticmethod
    def download_parallel(
        workers: List[ScopusUtilWorker], taskq: Queue, conn: str, inst_id: List[str], download_path: str
    ) -> List[str]:
        """Download SCOPUS snapshot with parallel sessions. Tasks will be distributed in parallel to the available
        keys. Each key will independently fetch a task from the queue when it's free so there's no guarantee of load
        balance.

        :param workers: List of workers available.
        :param taskq: tasks queue.
        :param conn: Airflow connection ID.
        :param inst_id: List of institutions to query concurrently.
        :param download_path: Path to save downloaded files to.
        :return: List of files downloaded.
        """

        saved_files = list()
        sessions = len(workers)
        with ThreadPoolExecutor(max_workers=sessions) as executor:
            futures = list()
            thread_exit = Event()

            for worker in workers:
                futures.append(
                    executor.submit(
                        ScopusUtility.download_worker, worker, thread_exit, taskq, conn, inst_id, download_path
                    )
                )

            taskq.join()  # Wait until all tasks done
            logging.info(f"{conn}: all tasks fetched. Signalling threads to exit.")
            thread_exit.set()  # Notify for threads to return results

            for future in as_completed(futures):
                saved_files += future.result()

        return saved_files

    @staticmethod
    def download_snapshot(api_keys: List[str], release: ScopusRelease, mode: str) -> List[str]:
        """Download snapshot from SCOPUS for the given institution.

        :param api_keys: List of API keys to use for downloading.
        :param release: Release information.
        :param mode: Download mode.
        :return: List of files downloaded.
        """

        download_path = os.path.join(release.download_path, release.release_date.isoformat(), release.inst_id)

        logging.info(f"Downloading snapshot with {mode} method.")
        schedule = build_schedule(release.start_date, release.end_date)
        task_queue = Queue()
        for period in schedule:
            task_queue.put(period)

        now = pendulum.now("UTC")
        workers = list()

        for i, apikey in enumerate(api_keys):
            view = "standard"
            if "view" in apikey:
                view = apikey["view"]
            key = apikey["key"]
            worker = ScopusUtilWorker(i, ScopusClient(key, view), now, ScopusUtilWorker.DEFAULT_KEY_QUOTA)
            workers.append(worker)

        if mode == "sequential":
            return ScopusUtility.download_sequential(
                workers, task_queue, release.inst_id, release.scopus_inst_id, download_path
            )
        if mode == "parallel":
            return ScopusUtility.download_parallel(
                workers, task_queue, release.inst_id, release.scopus_inst_id, download_path
            )

        raise AirflowException(f"Unsupported mode {mode} received")

    @staticmethod
    def make_query(worker: ScopusUtilWorker, query: str) -> Tuple[str, int]:
        """Throttling wrapper for the API call. This is a global limit for this API when called from a program on the
        same machine. Limits specified in ScopusUtilConst class.

        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param worker: ScopusUtilWorker object.
        :param query: Query object.
        :returns: Query results.
        """

        results, quota_remaining, quota_reset = worker.client.retrieve(query)
        worker.quota_reset_date = quota_reset
        worker.quota_remaining = quota_remaining
        return json.dumps(results), len(results)


class ScopusJsonParser:
    """Helper methods to process the json from SCOPUS into desired structure."""

    @staticmethod
    def get_affiliations(data: Dict[str, Any]) -> Union[None, List[Dict[str, Any]]]:
        """Get the affiliation field.

        :param data: json response from SCOPUS.
        :return list of affiliation details.
        """

        affiliations = list()
        if "affiliation" not in data:
            return None

        for affiliation in data["affiliation"]:
            affil = dict()
            affil["name"] = get_entry_or_none(affiliation, "affilname")
            affil["city"] = get_entry_or_none(affiliation, "affiliation-city")
            affil["country"] = get_entry_or_none(affiliation, "affiliation-country")

            # Available in complete view
            affil["id"] = get_entry_or_none(affiliation, "afid")
            affil["name_variant"] = get_entry_or_none(affiliation, "name-variant")
            affiliations.append(affil)

        if len(affiliations) == 0:
            return None

        return affiliations

    @staticmethod
    def get_authors(data: Dict[str, Any]) -> Union[None, List[Dict[str, Any]]]:
        """Get the author field. Won't know if this parser is going to throw error unless we get access to api key
            with complete view access.

        :param data: json response from SCOPUS.
        :return list of authors' details.
        """

        author_list = list()
        if "author" not in data:
            return None

        # Assuming there's a list given the doc says complete author list
        authors = data["author"]
        for author in authors:
            ad = dict()
            ad["authid"] = get_entry_or_none(author, "authid")  # Not sure what this is or how it's diff to afid
            ad["orcid"] = get_entry_or_none(author, "orcid")
            ad["full_name"] = get_entry_or_none(author, "authname")  # Taking a guess that this is what it is
            ad["first_name"] = get_entry_or_none(author, "given-name")
            ad["last_name"] = get_entry_or_none(author, "surname")
            ad["initials"] = get_entry_or_none(author, "initials")
            ad["afid"] = get_entry_or_none(author, "afid")
            author_list.append(ad)

        if len(author_list) == 0:
            return None

        return author_list

    @staticmethod
    def get_identifier_list(data: dict, id_type: str) -> Union[None, List[str]]:
        """Get the list of document identifiers or null of it does not exist.  This string/list behaviour was observed
        for ISBNs so using it for other identifiers just in case.

        :param data: json response from SCOPUS.
        :param id_type: type of identifier, e.g., 'isbn'
        :return: List of identifiers.
        """

        identifier = list()
        if id_type not in data:
            return None

        id_data = data[id_type]
        if isinstance(id_data, str):
            identifier.append(id_data)
        else:  # Only other observed case is list
            for entry in id_data:
                identifier.append(entry["$"])  # This is what showed up in ISBN example in list situation

        if len(identifier) == 0:
            return None

        return identifier

    @staticmethod
    def parse_json(data: dict, harvest_datetime: str, release_date: str, institutes: List[str]) -> dict:
        """Turn json data into db schema format.

        :param data: json response from SCOPUS.
        :param harvest_datetime: isoformat string of time the fetch took place.
        :param release_date: DAG execution date.
        :param institutes: List of institution ids used in the query.
        :return: dict of data in right field format.
        """

        entry = dict()
        entry["harvest_datetime"] = harvest_datetime  # Time of harvest (datetime string)
        entry["release_date"] = release_date  # Release date (date string)
        entry["institution_ids"] = institutes

        entry["title"] = get_entry_or_none(data, "dc:title")  # Article title
        entry["identifier"] = get_entry_or_none(data, "dc:identifier")  # Scopus ID
        entry["creator"] = get_entry_or_none(data, "dc:creator")  # First author name
        entry["publication_name"] = get_entry_or_none(data, "prism:publicationName")  # Source title
        entry["cover_date"] = get_entry_or_none(data, "prism:coverDate")  # Publication date
        entry["doi"] = ScopusJsonParser.get_identifier_list(data, "prism:doi")  # DOI
        entry["eissn"] = ScopusJsonParser.get_identifier_list(data, "prism:eIssn")  # Electronic ISSN
        entry["issn"] = ScopusJsonParser.get_identifier_list(data, "prism:issn")  # ISSN
        entry["isbn"] = ScopusJsonParser.get_identifier_list(data, "prism:isbn")  # ISBN
        entry["aggregation_type"] = get_entry_or_none(data, "prism:aggregationType")  # Source type
        entry["pubmed_id"] = get_entry_or_none(data, "pubmed-id")  # MEDLINE identifier
        entry["pii"] = get_entry_or_none(data, "pii")  # PII Publisher item identifier
        entry["eid"] = get_entry_or_none(data, "eid")  # Electronic ID
        entry["subtype_description"] = get_entry_or_none(data, "subtypeDescription")  # Document Type description
        entry["open_access"] = get_entry_or_none(data, "openaccess", int)  # Open access status. (Integer)
        entry["open_access_flag"] = get_entry_or_none(data, "openaccessFlag")  # Open access status. (Boolean)
        entry["citedby_count"] = get_entry_or_none(data, "citedby-count", int)  # Cited by count (integer)
        entry["source_id"] = get_entry_or_none(data, "source-id", int)  # Source ID (integer)
        entry["affiliations"] = ScopusJsonParser.get_affiliations(data)  # Affiliations
        entry["orcid"] = get_entry_or_none(data, "orcid")  # ORCID

        # Available in complete view
        entry["authors"] = ScopusJsonParser.get_authors(data)  # List of authors
        entry["abstract"] = get_entry_or_none(data, "dc:description")  # Abstract
        entry["keywords"] = get_as_list(data, "authkeywords")  # Assuming it's a list of strings.
        entry["article_number"] = get_entry_or_none(data, "article-number")  # Article number (unclear if int or str)
        entry["fund_agency_ac"] = get_entry_or_none(data, "fund-acr")  # Funding agency acronym
        entry["fund_agency_id"] = get_entry_or_none(data, "fund-no")  # Funding agency identification
        entry["fund_agency_name"] = get_entry_or_none(data, "fund-sponsor")  # Funding agency name

        return entry
