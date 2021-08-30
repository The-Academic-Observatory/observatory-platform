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


import json
import logging
import os
import urllib.request
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from typing import Any, Dict, List, Type, Union
from urllib.error import URLError

import backoff
import pendulum
import xmltodict
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat, WriteDisposition
from ratelimit import limits, sleep_and_retry
from suds import WebFault
from wos import WosClient

from academic_observatory_workflows.config import schema_folder
from observatory.platform.utils.airflow_utils import AirflowVars, check_variables
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.file_utils import write_to_file, zip_files
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    create_bigquery_dataset,
    load_bigquery_table,
)
from observatory.platform.utils.workflow_utils import SubFolder, workflow_path
from observatory.platform.utils.workflow_utils import (
    build_schedule,
    delete_msg_files,
    get_as_list,
    get_as_list_or_none,
    get_entry_or_none,
    json_to_db,
    upload_telescope_file_list,
    validate_date,
    write_xml_to_json,
)


class WosUtilConst:
    """Class containing some WosUtility constants. Makes these values accessible by decorators."""

    # WoS limits as a guide for reference. Throttle limits more conservative than this.
    RESULT_LIMIT = 100  # Return 100 results max per query.
    CALL_LIMIT = 1  # WoS says they can do 2 api calls / second, but we're setting 1 per 2 seconds.
    CALL_PERIOD = 2  # seconds
    SESSION_CALL_LIMIT = 5  # 5 calls.
    SESSION_CALL_PERIOD = 360  # 6 minutes. [Actual WoS limit is 5 mins]
    RETRIES = 3


class WosUtility:
    """Handles the interaction with Web of Science"""

    @staticmethod
    def build_query(wos_inst_id: List[str], period: Type[pendulum.Period]) -> OrderedDict:
        """Build a WoS API query.

        :param wos_inst_id: List of Institutional ID to query, e.g, "Curtin University"
        :param period: A tuple containing start and end dates.
        :return: Constructed web query.
        """
        start_date = period.start.isoformat()
        end_date = period.end.isoformat()

        organisations = str()
        n_institutes = len(wos_inst_id)
        for i, inst in enumerate(wos_inst_id):
            organisations += inst
            organisations += " OR "

        organisations = organisations[:-4]  # Remove last ' OR '

        query_str = f"OG=({organisations})"

        logging.info(f"Query string: {query_str}")
        query = OrderedDict(
            [
                ("query", query_str),
                ("count", WosUtilConst.RESULT_LIMIT),
                ("offset", 1),
                ("timeSpan", {"begin": start_date, "end": end_date}),
            ]
        )
        return query

    @staticmethod
    def parse_query(records: Any) -> (dict, str):
        """Parse XML tree record into a dict.

        :param records: XML tree returned by the web query.
        :return: Dictionary version of the web response and a schema version string.
        """

        try:
            records_dict = xmltodict.parse(records)
        except Exception as e:
            raise AirflowException(f"XML to dict parsing failed: {e}")

        records = records_dict["records"]
        schema_string = records["@xmlns"]

        prefix_len = len(WosTelescope.SCHEMA_ID_PREFIX)
        prefix_loc = schema_string.find(WosTelescope.SCHEMA_ID_PREFIX)
        if prefix_loc == -1:
            logging.warning(
                f"WOS schema has changed.\nExpecting prefix: {WosTelescope.SCHEMA_ID_PREFIX}\n"
                f"Received: {schema_string}"
            )
            return None

        ver_start = prefix_len
        ver_end = schema_string.find("/", ver_start)
        schema_ver = schema_string[ver_start:ver_end]
        logging.info(f"Found schema version: {schema_ver}")

        if "REC" not in records:
            logging.warning(f"No record found for query. Please double check parameters, e.g., institution id.")
            return None, schema_ver

        return get_as_list(records, "REC"), schema_ver

    @staticmethod
    def download_wos_period(
        client: WosClient, conn: str, period: pendulum.Period, wos_inst_id: List[str], download_path: str
    ) -> List[str]:
        """Download records for a stated date range.

        :param client: WebClient object.
        :param conn: file name for saved response as a pickle file.
        :param period: Period tuple containing (start date, end date).
        :param wos_inst_id: List of Institutional ID to query, e.g, "Curtin University"
        :param download_path: Path to download files to.
        """

        timestamp = pendulum.now().isoformat()
        inst_str = conn[WosTelescope.ID_STRING_OFFSET :]
        save_file_prefix = os.path.join(
            download_path, period.start.isoformat(), inst_str, f"{period.start}-{period.end}_{timestamp}"
        )
        query = WosUtility.build_query(wos_inst_id, period)
        result = WosUtility.make_query(client, query)
        logging.info(f"{conn} with session id {client._SID}: retrieving period {period.start} - {period.end}")

        counter = 0
        saved_files = list()
        for entry in result:
            save_file = f"{save_file_prefix}-{counter}.xml"
            write_to_file(entry, save_file)
            counter += 1
            saved_files.append(save_file)

        return saved_files

    @staticmethod
    @backoff.on_exception(
        backoff.constant, WebFault, max_tries=WosUtilConst.RETRIES, interval=WosUtilConst.SESSION_CALL_PERIOD
    )
    def download_wos_batch(
        login: str, password: str, batch: list, conn: str, wos_inst_id: List[str], download_path: str
    ) -> List[str]:
        """Download one batch of WoS snapshots. Throttling limits are more conservative than WoS limits.
        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param login: login.
        :param password: password.
        :param batch: List of tuples of (start_date, end_date) to fetch.
        :param conn: connection_id string from Airflow variable.
        :param wos_inst_id: List of Institutional ID to query, e.g, "Curtin University"
        :param download_path: download path to save response to.
        :return: List of saved files from this batch.
        """

        file_list = list()
        with WosClient(login, password) as client:
            for period in batch:
                files = WosUtility.download_wos_period(client, conn, period, wos_inst_id, download_path)
                file_list += files
        return file_list

    @staticmethod
    def download_wos_parallel(
        login: str, password: str, schedule: list, conn: str, wos_inst_id: List[str], download_path: str
    ) -> List[str]:
        """Download WoS snapshot with parallel sessions. Using threads.

        :param login: WoS login
        :param password: WoS password
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param wos_inst_id: List of Institutional ID to query, e.g, "Curtin University"
        :param download_path: Path to download to.
        :return: List of files downloaded.
        """

        sessions = WosUtilConst.SESSION_CALL_LIMIT
        batch_size = ceil(len(schedule) / sessions)

        # Evenly distribute schedule among the session batches. Last session gets whatever the remaining tail is.
        batches = [schedule[i * batch_size : (i + 1) * batch_size] for i in range(sessions - 1)]
        batches.append(schedule[(sessions - 1) * batch_size :])  # Last session

        file_list = []
        with ThreadPoolExecutor(max_workers=sessions) as executor:
            futures = []
            for i in range(sessions):
                futures.append(
                    executor.submit(
                        WosUtility.download_wos_batch, login, password, batches[i], conn, wos_inst_id, download_path
                    )
                )
            for future in as_completed(futures):
                files = future.result()
                file_list = file_list + files

        return file_list

    @staticmethod
    def download_wos_sequential(
        login: str, password: str, schedule: list, conn: str, wos_inst_id: List[str], download_path: str
    ) -> List[str]:
        """Download WoS snapshot sequentially.

        :param login: WoS login
        :param password: WoS password
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param wos_inst_id: List of Institutional ID to query, e.g, "Curtin University"
        :param download_path: Path to download to.
        :return: List of files downloaded.
        """

        return WosUtility.download_wos_batch(login, password, schedule, conn, wos_inst_id, download_path)

    @staticmethod
    def download_wos_snapshot(
        download_path: str, conn, wos_inst_id: List[str], end_date: pendulum.DateTime, mode: str
    ) -> List[str]:
        """Download snapshot from Web of Science for the given institution.

        :param download_path: the directory where the downloaded wos snapshot should be saved.
        :param conn: Airflow connection object.
        :param wos_inst_id: List of wos institution ids to use in query.
        :param end_date: end date of schedule. Usually the DAG start date of this DAG run.
        :param mode: Download mode to use. 'sequential' or 'parallel'
        """

        extra = json.loads(conn.extra)
        login = conn.login
        password = conn.password
        start_date = pendulum.parse(extra["start_date"])
        end_date = end_date
        schedule = build_schedule(start_date, end_date)

        if mode == "sequential" or len(schedule) <= WosUtilConst.SESSION_CALL_LIMIT:
            logging.info("Downloading snapshot with sequential method")
            return WosUtility.download_wos_sequential(login, password, schedule, str(conn), wos_inst_id, download_path)

        if mode == "parallel":
            logging.info("Downloading snapshot with parallel method")
            return WosUtility.download_wos_parallel(login, password, schedule, str(conn), wos_inst_id, download_path)

    @staticmethod
    def make_query(client: WosClient, query: OrderedDict) -> List[Any]:
        """Make the API calls to retrieve information from Web of Science.

        :param client: WosClient object.
        :param query: Constructed search query from use build_query.

        :return: List of XML responses.
        """

        results = WosUtility.wos_search(client, query)
        num_results = int(results.recordsFound)
        record_list = [results.records]

        if num_results > WosUtilConst.RESULT_LIMIT:
            for offset in range(2, num_results, WosUtilConst.RESULT_LIMIT):
                query["offset"] = offset
                record_list.append(WosUtility.wos_search(client, query).records)

        return record_list

    @staticmethod
    @sleep_and_retry
    @limits(calls=WosUtilConst.CALL_LIMIT, period=WosUtilConst.CALL_PERIOD)
    def wos_search(client: WosClient, query: OrderedDict) -> Any:
        """Throttling wrapper for the API call. This is a global limit for this API when called from a program on the
        same machine. If you are throttled, it will throw a WebFault and the exception message will contain the phrase
        'Request denied by Throttle server'

        Limiting to 1 call per second even though theoretical limit is 2 per second just in case.
        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param client: WosClient object.
        :param query: Query object.
        :returns: Query results.
        """

        return client.search(**query)


class WosRelease:
    """Used to store info on a given WoS release.

    :param inst_id: institution id from the airflow connection (minus the wos_)
    :param wos_inst_id: List of institution ids to use in the WoS query.
    :param release_date: Release date (currently the execution date).
    :param dag_start: Start date of the dag (not execution date).
    :param project_id: The project id to use.
    :param download_bucket_name: Download bucket name to use for storing downloaded files in the cloud.
    :param transform_bucket_name: Transform bucket name to use for storing transformed files in the cloud.
    :param data_location: Location of the data servers
    """

    def __init__(
        self,
        inst_id: str,
        wos_inst_id: List[str],
        release_date: pendulum.DateTime,
        dag_start: pendulum.DateTime,
        project_id: str,
        download_bucket_name: str,
        transform_bucket_name: str,
        data_location: str,
        schema_ver: str,
    ):
        self.inst_id = inst_id
        self.wos_inst_id = wos_inst_id
        self.release_date = release_date
        self.dag_start = dag_start
        self.download_path = workflow_path(SubFolder.downloaded, WosTelescope.DAG_ID)
        self.transform_path = workflow_path(SubFolder.transformed, WosTelescope.DAG_ID)
        self.telescope_path = f"telescopes/{WosTelescope.DAG_ID}/{release_date}"
        self.project_id = project_id
        self.download_bucket_name = download_bucket_name
        self.transform_bucket_name = transform_bucket_name
        self.data_location = data_location
        self.schema_ver = schema_ver


class WosTelescope:
    """A container for holding the constants and static functions for the Web of Science telescope."""

    DAG_ID = "webofscience"
    ID_STRING_OFFSET = len(DAG_ID) + 1
    DESCRIPTION = "Web of Science: https://www.clarivate.com/webofsciencegroup"
    SCHEDULE_INTERVAL = "@monthly"
    QUEUE = "default"
    RETRIES = 3
    DATASET_ID = "clarivate"
    SCHEMA_PATH = "telescopes"
    TABLE_NAME = DAG_ID
    SCHEMA_VER = "wok5.4"

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_CHECK_API_SERVER = "check_api_server"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_UPLOAD_DOWNLOADED = "upload_downloaded"
    TASK_ID_TRANSFORM_XML = "transform_xml"
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

    # Implement multiprocessing later if needed. WosClient seems to be synchronous i/o so asyncio doesn't help.
    DOWNLOAD_MODE = "sequential"  # Valid options: ['sequential', 'parallel']

    API_SERVER = "http://scientific.thomsonreuters.com"
    SCHEMA_ID_PREFIX = "http://scientific.thomsonreuters.com/schema/"

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
        extra = conn.extra

        logging.info(f"Validating json in extra field of {conn}")
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

        # Check login is set
        if len(conn.login) == 0:
            raise AirflowException(f'The "login" field is not set for {conn}.')

        # Check password is set
        if len(conn.password) == 0:
            raise AirflowException(f'The "password" field is not set for {conn}.')

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
        wos_inst_id = get_as_list(extra_dict, "id")
        release = WosRelease(
            inst_id=kwargs["institution"],
            wos_inst_id=wos_inst_id,
            release_date=kwargs["execution_date"],
            dag_start=pendulum.parse(kwargs["dag_start"]),
            project_id=project_id,
            download_bucket_name=download_bucket_name,
            transform_bucket_name=transform_bucket_name,
            data_location=data_location,
            schema_ver=WosTelescope.SCHEMA_VER,
        )

        logging.info(
            f"WosRelease contains:\ndownload_bucket_name: {release.download_bucket_name}, transform_bucket_name: "
            + f"{release.transform_bucket_name}, data_location: {release.data_location}"
        )

        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(WosTelescope.XCOM_RELEASES, release)

    @staticmethod
    def check_api_server():
        """Check that http://scientific.thomsonreuters.com is still contactable.

        :return: the identifier of the task to execute next.
        """

        http_code_ok = 200

        logging.info(f"Checking API server {WosTelescope.API_SERVER} is up.")

        try:
            http_code = urllib.request.urlopen(WosTelescope.API_SERVER).getcode()
        except URLError as e:
            raise ValueError(f"Failed to fetch url because of: {e}")

        if http_code != http_code_ok:
            raise ValueError(f"HTTP response code {http_code} received.")

    @staticmethod
    def download(**kwargs):
        """Task to download the WoS snapshots.

        Pushes the following xcom:
            download_path (str): the path to a pickled file containing the list of xml responses in a month query.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: WosRelease = WosTelescope.pull_release(ti)

        # Prepare paths
        download_files = WosUtility.download_wos_snapshot(
            release.download_path, kwargs["conn"], release.wos_inst_id, release.dag_start, WosTelescope.DOWNLOAD_MODE
        )

        # Notify next task of the files downloaded.
        ti.xcom_push(WosTelescope.XCOM_DOWNLOAD_PATH, download_files)

    @staticmethod
    def upload_downloaded(**kwargs):
        """Task to upload the downloaded WoS snapshots.

        Pushes the following xcom:
            upload_zip_path (str): the path to pickle zip file of downloaded response.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: WosRelease = WosTelescope.pull_release(ti)

        # Pull messages
        download_list = ti.xcom_pull(
            key=WosTelescope.XCOM_DOWNLOAD_PATH, task_ids=WosTelescope.TASK_ID_DOWNLOAD, include_prior_dates=False
        )

        # Upload each snapshot
        logging.info("upload_downloaded: zipping and uploading downloaded files")
        zip_list = zip_files(download_list)
        upload_telescope_file_list(release.download_bucket_name, release.inst_id, release.telescope_path, zip_list)

        # Notify next task of the files downloaded.
        ti.xcom_push(WosTelescope.XCOM_UPLOAD_ZIP_PATH, zip_list)

    @staticmethod
    def transform_xml(**kwargs):
        """Task to transform the XML from the query to json.

        Pushes the following xcom:
            json_path (str): the path to json file of a converted xml response.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        xml_files = ti.xcom_pull(
            key=WosTelescope.XCOM_DOWNLOAD_PATH, task_ids=WosTelescope.TASK_ID_DOWNLOAD, include_prior_dates=False
        )
        release: WosRelease = WosTelescope.pull_release(ti)

        # Process each xml file
        logging.info("transform_xml: transforming xml to dict and writing to json")
        json_file_list, schema_vers = write_xml_to_json(
            release.transform_path, release.release_date.isoformat(), release.inst_id, xml_files, WosUtility.parse_query
        )

        # Check we received consistent schema versions, and update release information.
        if schema_vers and schema_vers.count(WosTelescope.SCHEMA_VER) == len(schema_vers):
            release.schema_ver = schema_vers[0]
        else:
            raise AirflowException(f"Inconsistent schema versions received in response.")

        # Notify next task of the converted json files
        json_path = list()
        json_harvest = list()
        for file in json_file_list:
            file_name = os.path.basename(file)
            start = file_name.find("_") + 1
            end = file_name.rfind("-")
            harvest_datetime = file_name[start:end]
            json_path.append(file)
            json_harvest.append(harvest_datetime)
        ti.xcom_push(WosTelescope.XCOM_JSON_PATH, json_path)
        ti.xcom_push(WosTelescope.XCOM_JSON_HARVEST, json_harvest)

    @staticmethod
    def transform_db_format(**kwargs):
        """Task to transform the json into db field format (and in jsonlines form).

        Pushes the following xcom:
            version (str): the version of the GRID release.
            json_gz_file_name (str): the file name for the transformed GRID release.
            json_gz_file_path (str): the path to the transformed GRID release (including file name).

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: WosRelease = WosTelescope.pull_release(ti)

        # Pull messages
        json_files = ti.xcom_pull(
            key=WosTelescope.XCOM_JSON_PATH, task_ids=WosTelescope.TASK_ID_TRANSFORM_XML, include_prior_dates=False
        )

        harvest_times = ti.xcom_pull(
            key=WosTelescope.XCOM_JSON_HARVEST, task_ids=WosTelescope.TASK_ID_TRANSFORM_XML, include_prior_dates=False
        )

        json_harvest_pair = list(zip(json_files, harvest_times))
        json_harvest_pair.sort()  # Sort by institution and date
        print(f"json harvest pairs are:\n{json_harvest_pair}")

        # Apply field extraction and transformation to jsonlines
        logging.info("transform_db_format: parsing and transforming into db format")
        jsonl_list = json_to_db(
            json_harvest_pair, release.release_date.isoformat(), WosJsonParser.parse_json, release.wos_inst_id
        )

        # Notify next task
        ti.xcom_push(WosTelescope.XCOM_JSONL_PATH, jsonl_list)

    @staticmethod
    def upload_transformed(**kwargs):
        """Task to upload the transformed WoS data into jsonlines files.

        Pushes the following xcom:
            release_date (str): the release date of the GRID release.
            blob_name (str): the name of the blob on the Google Cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: WosRelease = WosTelescope.pull_release(ti)

        # Pull messages
        jsonl_paths = ti.xcom_pull(
            key=WosTelescope.XCOM_JSONL_PATH,
            task_ids=WosTelescope.TASK_ID_TRANSFORM_DB_FORMAT,
            include_prior_dates=False,
        )

        # Upload each snapshot
        logging.info("upload_transformed: zipping and uploading jsonlines to cloud")
        zip_list = zip_files(jsonl_paths)
        blob_list = upload_telescope_file_list(
            release.transform_bucket_name, release.inst_id, release.telescope_path, zip_list
        )

        # Notify next task of the files downloaded.
        ti.xcom_push(WosTelescope.XCOM_JSONL_BLOB_PATH, blob_list)
        ti.xcom_push(WosTelescope.XCOM_JSONL_ZIP_PATH, zip_list)

    @staticmethod
    def bq_load(**kwargs):
        """Task to load the transformed WoS snapshot into BigQuery.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs["ti"]
        release: WosRelease = WosTelescope.pull_release(ti)

        jsonl_zip_blobs = ti.xcom_pull(
            key=WosTelescope.XCOM_JSONL_BLOB_PATH,
            task_ids=WosTelescope.TASK_ID_UPLOAD_TRANSFORMED,
            include_prior_dates=False,
        )

        # Create dataset
        create_bigquery_dataset(
            release.project_id, WosTelescope.DATASET_ID, release.data_location, WosTelescope.DESCRIPTION
        )

        # Create table id
        table_id = bigquery_sharded_table_id(WosTelescope.TABLE_NAME, release.release_date)

        # Load into BigQuery
        analysis_schema_path = schema_folder()

        for file in jsonl_zip_blobs:
            schema_file_path = find_schema(
                analysis_schema_path, WosTelescope.TABLE_NAME, release.release_date, "", release.schema_ver
            )
            if schema_file_path is None:
                logging.error(
                    f"No schema found with search parameters: analysis_schema_path={WosTelescope.SCHEMA_PATH}, "
                    f"table_name={WosTelescope.TABLE_NAME}, release_date={release.release_date}, "
                    f"schema_ver={release.schema_ver}"
                )
                exit(os.EX_CONFIG)

            # Load BigQuery table
            uri = f"gs://{release.transform_bucket_name}/{file}"
            logging.info(f"URI: {uri}")

            load_bigquery_table(
                uri,
                WosTelescope.DATASET_ID,
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

        delete_msg_files(ti, WosTelescope.XCOM_DOWNLOAD_PATH, WosTelescope.TASK_ID_DOWNLOAD)
        delete_msg_files(ti, WosTelescope.XCOM_UPLOAD_ZIP_PATH, WosTelescope.TASK_ID_UPLOAD_DOWNLOADED)
        delete_msg_files(ti, WosTelescope.XCOM_JSON_PATH, WosTelescope.TASK_ID_TRANSFORM_XML)
        delete_msg_files(ti, WosTelescope.XCOM_JSONL_PATH, WosTelescope.TASK_ID_TRANSFORM_DB_FORMAT)
        delete_msg_files(ti, WosTelescope.XCOM_JSONL_ZIP_PATH, WosTelescope.TASK_ID_UPLOAD_TRANSFORMED)

    @staticmethod
    def pull_release(ti: TaskInstance):
        """Get the WosRelease object from XCOM message."""
        return ti.xcom_pull(
            key=WosTelescope.XCOM_RELEASES, task_ids=WosTelescope.TASK_ID_CHECK_DEPENDENCIES, include_prior_dates=False
        )


class WosNameAttributes:
    """Helper class for parsing name attributes."""

    def __init__(self, data: dict):
        self._contribs = WosNameAttributes._get_contribs(data)

    @staticmethod
    def _get_contribs(data: dict) -> dict:
        """Helper function to parse the contributors structure to aid extraction of fields.

        :param data: dictionary to query.
        :return: Dictionary of attributes keyed by full_name string.
        """

        contrib_dict = dict()

        if "contributors" in data["static_data"]:
            contributors = get_as_list(data["static_data"]["contributors"], "contributor")
            for contributor in contributors:
                name_field = contributor["name"]
                first_name = name_field["first_name"]
                last_name = name_field["last_name"]
                attrib = dict()
                full_name = f"{first_name} {last_name}"
                if "@r_id" in name_field:
                    attrib["r_id"] = name_field["@r_id"]
                if "@orcid_id" in name_field:
                    attrib["orcid"] = name_field["@orcid_id"]
                contrib_dict[full_name] = attrib

        return contrib_dict

    def get_orcid(self, full_name: str) -> str:
        """Get the orcid id of a person. Note that full name must be the combination of first and last name.
        This is not necessarily the full_name field.

        :param full_name: The 'first_name last_name' string.
        :return: orcid id.
        """
        if full_name is None or full_name not in self._contribs or "orcid" not in self._contribs[full_name]:
            return None
        return self._contribs[full_name]["orcid"]

    def get_r_id(self, full_name: str) -> str:
        """Get the r_id of a person. Note that full name must be the combination of first and last name.
        This is not necessarily the full_name field.

        :param full_name: The 'first_name last_name' string.
        :return: r_id.
        """

        if full_name is None or full_name not in self._contribs or "r_id" not in self._contribs[full_name]:
            return None
        return self._contribs[full_name]["r_id"]


class WosJsonParser:
    """Helper methods to process the the converted json from Web of Science."""

    @staticmethod
    def get_identifiers(data: dict) -> Dict[str, Any]:
        """Extract identifier information.

        :param data: dictionary of web response.
        :return: Identifier record.
        """

        recognised_types = {
            "issn",
            "eissn",
            "isbn",
            "eisbn",
            "art_no",
            "meeting_abs",
            "xref_doi",
            "parent_book_doi",
            "doi",
        }
        field = dict()
        field["uid"] = data["UID"]

        for thing in recognised_types:
            field[thing] = None

        if "dynamic_data" not in data:
            return field

        if "cluster_related" not in data["dynamic_data"]:
            return field

        if "identifiers" not in data["dynamic_data"]["cluster_related"]:
            return field

        if data["dynamic_data"]["cluster_related"]["identifiers"] is None:
            return field

        if "identifier" not in data["dynamic_data"]["cluster_related"]["identifiers"]:
            return field

        identifiers = data["dynamic_data"]["cluster_related"]["identifiers"]
        identifier = get_as_list(identifiers, "identifier")

        for entry in identifier:
            type_ = entry["@type"]
            value = entry["@value"]

            if type_ in recognised_types:
                field[type_] = value

        return field

    @staticmethod
    def get_pub_info(data: dict) -> Dict[str, Any]:
        """Extract publication information fields.

        :param data: dictionary of web response.
        :return: Publication info record.
        """

        summary = data["static_data"]["summary"]

        field = dict()
        field["sort_date"] = None
        field["pub_type"] = None
        field["page_count"] = None
        field["source"] = None
        field["doc_type"] = None
        field["publisher"] = None
        field["publisher_city"] = None

        if "pub_info" in summary:
            pub_info = summary["pub_info"]
            field["sort_date"] = pub_info["@sortdate"]
            field["pub_type"] = pub_info["@pubtype"]
            field["page_count"] = int(pub_info["page"]["@page_count"])

        if "publishers" in summary and "publisher" in summary["publishers"]:
            publisher = summary["publishers"]["publisher"]
            field["publisher"] = publisher["names"]["name"]["full_name"]
            field["publisher_city"] = publisher["address_spec"]["city"]

        if "titles" in summary and "title" in summary["titles"]:
            titles = get_as_list(summary["titles"], "title")
            for title in titles:
                if title["@type"] == "source":
                    field["source"] = title["#text"]
                    break

        if "doctypes" in summary:
            doctypes = get_as_list(summary["doctypes"], "doctype")
            field["doc_type"] = doctypes[0]

        return field

    @staticmethod
    def get_title(data: dict) -> Union[None, str]:
        """Extract title. May raise exception on error.

        :param data: dictionary of web response.
        :return: String of title or None if not found.
        """
        if "titles" not in data["static_data"]["summary"]:
            return None

        for entry in data["static_data"]["summary"]["titles"]["title"]:
            if "@type" in entry and entry["@type"] == "item" and "#text" in entry:
                return entry["#text"]

        raise AirflowException("Schema change detected in title field. Please review.")

    @staticmethod
    def get_names(data: dict) -> List[Dict[str, Any]]:
        """Extract names fields.

        :param data: dictionary of web response.
        :return: List of name records.
        """

        field = list()
        if "names" not in data["static_data"]["summary"]:
            return field

        names = data["static_data"]["summary"]["names"]
        if "name" not in names:
            return field

        attrib = WosNameAttributes(data)
        names = get_as_list(data["static_data"]["summary"]["names"], "name")

        for name in names:
            entry = dict()
            entry["seq_no"] = int(get_entry_or_none(name, "@seq_no"))
            entry["role"] = get_entry_or_none(name, "@role")
            entry["first_name"] = get_entry_or_none(name, "first_name")
            entry["last_name"] = get_entry_or_none(name, "last_name")
            entry["wos_standard"] = get_entry_or_none(name, "wos_standard")
            entry["daisng_id"] = get_entry_or_none(name, "@daisng_id")
            entry["full_name"] = get_entry_or_none(name, "full_name")

            # Get around errors / booby traps for name retrieval
            first_name = entry["first_name"]
            last_name = entry["last_name"]
            full_name = f"{first_name} {last_name}"
            entry["orcid"] = attrib.get_orcid(full_name)
            entry["r_id"] = attrib.get_r_id(full_name)
            field.append(entry)

        return field

    @staticmethod
    def get_languages(data: dict) -> List[Dict[str, str]]:
        """Extract language fields.

        :param data: dictionary of web response.
        :return: List of language records.
        """

        lang_list = list()
        if "languages" not in data["static_data"]["fullrecord_metadata"]:
            return lang_list

        if "language" not in data["static_data"]["fullrecord_metadata"]["languages"]:
            return lang_list

        languages = get_as_list(data["static_data"]["fullrecord_metadata"]["languages"], "language")
        for entry in languages:
            lang_list.append({"type": entry["@type"], "name": entry["#text"]})
        return lang_list

    @staticmethod
    def get_refcount(data: dict) -> Union[int, None]:
        """Extract reference count.

        :param data: dictionary of web response.
        :return: Reference count.
        """

        if "refs" not in data["static_data"]["fullrecord_metadata"]:
            return None

        if "@count" not in data["static_data"]["fullrecord_metadata"]["refs"]:
            return None

        return int(data["static_data"]["fullrecord_metadata"]["refs"]["@count"])

    @staticmethod
    def get_abstract(data: dict) -> List[str]:
        """Extract abstracts.

        :param data: dictionary of web response.
        :return: List of abstracts.
        """

        abstract_list = list()
        if "abstracts" not in data["static_data"]["fullrecord_metadata"]:
            return abstract_list
        if "abstract" not in data["static_data"]["fullrecord_metadata"]["abstracts"]:
            return abstract_list

        abstracts = get_as_list(data["static_data"]["fullrecord_metadata"]["abstracts"], "abstract")
        for abstract in abstracts:
            texts = get_as_list(abstract["abstract_text"], "p")
            for text in texts:
                abstract_list.append(text)
        return abstract_list

    @staticmethod
    def get_keyword(data: dict) -> List[str]:
        """Extract keywords. Will also get the keywords from keyword plus if they are available.

        :param data: dictionary of web response.
        :return: List of keywords.
        """

        keywords = list()
        if "keywords" not in data["static_data"]["fullrecord_metadata"]:
            return keywords
        if "keyword" not in data["static_data"]["fullrecord_metadata"]["keywords"]:
            return keywords

        keywords = get_as_list(data["static_data"]["fullrecord_metadata"]["keywords"], "keyword")
        if "item" in data["static_data"] and "keywords_plus" in data["static_data"]["item"]:
            plus = get_as_list(data["static_data"]["item"]["keywords_plus"], "keyword")
            keywords = keywords + plus
        return keywords

    @staticmethod
    def get_conference(data: dict) -> List[Dict[str, Any]]:
        """Extract conference information.

        :param data: dictionary of web response.
        :return: List of conferences.
        """

        conferences = list()
        if "conferences" not in data["static_data"]["summary"]:
            return conferences
        if "conference" not in data["static_data"]["summary"]["conferences"]:
            return conferences

        conf_list = get_as_list(data["static_data"]["summary"]["conferences"], "conference")
        for conf in conf_list:
            conference = dict()
            conference["id"] = get_entry_or_none(conf, "@conf_id")
            if conference["id"] is not None:
                conference["id"] = int(conference["id"])
            conference["name"] = None

            if "conf_titles" in conf and "conf_title" in conf["conf_titles"]:
                titles = get_as_list(conf["conf_titles"], "conf_title")
                conference["name"] = titles[0]
            conferences.append(conference)
        return conferences

    @staticmethod
    def get_orgs(data: dict) -> list:
        """Extract the organisation information.

        :param data: dictionary of web response.
        :return: list of organisations or None
        """

        orgs = list()

        if "addresses" not in data["static_data"]["fullrecord_metadata"]:
            return orgs

        addr_list = get_as_list(data["static_data"]["fullrecord_metadata"]["addresses"], "address_name")
        if addr_list is None:
            return orgs

        for addr in addr_list:
            spec = addr["address_spec"]
            org = dict()

            org["city"] = get_entry_or_none(spec, "city")
            org["state"] = get_entry_or_none(spec, "state")
            org["country"] = get_entry_or_none(spec, "country")

            if "organizations" not in addr["address_spec"]:
                orgs.append(org)
                return orgs

            org_list = get_as_list(addr["address_spec"]["organizations"], "organization")
            for entry in org_list:
                if "@pref" in entry and entry["@pref"] == "Y":
                    org["org_name"] = entry["#text"]
                    break
            else:
                org["org_name"] = org_list[0]
                if not isinstance(org["org_name"], str):
                    raise AirflowException("Schema parsing error for org.")

            if "suborganizations" in addr["address_spec"]:
                org["suborgs"] = get_as_list(addr["address_spec"]["suborganizations"], "suborganization")

            if "names" in addr and "name" in addr["names"]:
                names = get_as_list(addr["names"], "name")
                names_list = list()
                for name in names:
                    entry = dict()
                    entry["first_name"] = get_entry_or_none(name, "first_name")
                    entry["last_name"] = get_entry_or_none(name, "last_name")
                    entry["daisng_id"] = get_entry_or_none(name, "@daisng_id")
                    entry["full_name"] = get_entry_or_none(name, "full_name")
                    entry["wos_standard"] = get_entry_or_none(name, "wos_standard")
                    names_list.append(entry)
                org["names"] = names_list
            orgs.append(org)
        return orgs

    @staticmethod
    def get_fund_ack(data: dict) -> dict:
        """Extract funding acknowledgements.

        :param data: dictionary of web response.
        :return: Funding acknowledgement information.
        """

        fund_ack = dict()
        fund_ack["text"] = list()
        fund_ack["grants"] = list()

        if "fund_ack" not in data["static_data"]["fullrecord_metadata"]:
            return fund_ack

        entry = data["static_data"]["fullrecord_metadata"]["fund_ack"]
        if "fund_text" in entry and "p" in entry["fund_text"]:
            fund_ack["text"] = get_as_list(entry["fund_text"], "p")

        if "grants" not in entry:
            return fund_ack

        grants = get_as_list(entry["grants"], "grant")
        for grant in grants:
            grant_info = dict()
            grant_info["agency"] = get_entry_or_none(grant, "grant_agency")
            grant_info["ids"] = list()
            if "grant_ids" in grant:
                grant_info["ids"] = get_as_list(grant["grant_ids"], "grant_id")
            fund_ack["grants"].append(grant_info)
        return fund_ack

    @staticmethod
    def get_categories(data: dict) -> dict:
        """Extract categories.

        :param data: dictionary of web response.
        :return: categories dictionary.
        """

        category_info = dict()
        if "category_info" not in data["static_data"]["fullrecord_metadata"]:
            return category_info

        entry = data["static_data"]["fullrecord_metadata"]["category_info"]
        category_info["headings"] = get_as_list_or_none(entry, "headings", "heading")
        category_info["subheadings"] = get_as_list_or_none(entry, "subheadings", "subheading")

        subject_list = list()
        subjects = get_as_list_or_none(entry, "subjects", "subject")
        for subject in subjects:
            subject_dict = dict()
            subject_dict["ascatype"] = get_entry_or_none(subject, "@ascatype")
            subject_dict["code"] = get_entry_or_none(subject, "@code")
            subject_dict["text"] = get_entry_or_none(subject, "#text")
            subject_list.append(subject_dict)
        category_info["subjects"] = subject_list

        return category_info

    @staticmethod
    def parse_json(data: dict, harvest_datetime: str, release_date: str, institutes: List[str]) -> dict:
        """Turn json data into db schema format.

        :param data: dictionary of web response.
        :param harvest_datetime: isoformat string of time the fetch took place.
        :param release_date: DAG execution date.
        :param institutes: List of institution ids used in the query.
        :return: dict of data in right field format.
        """

        entry = dict()
        entry["harvest_datetime"] = harvest_datetime
        entry["release_date"] = release_date
        entry["identifiers"] = WosJsonParser.get_identifiers(data)
        entry["pub_info"] = WosJsonParser.get_pub_info(data)
        entry["title"] = WosJsonParser.get_title(data)
        entry["names"] = WosJsonParser.get_names(data)
        entry["languages"] = WosJsonParser.get_languages(data)
        entry["ref_count"] = WosJsonParser.get_refcount(data)
        entry["abstract"] = WosJsonParser.get_abstract(data)
        entry["keywords"] = WosJsonParser.get_keyword(data)
        entry["conferences"] = WosJsonParser.get_conference(data)
        entry["fund_ack"] = WosJsonParser.get_fund_ack(data)
        entry["categories"] = WosJsonParser.get_categories(data)
        entry["orgs"] = WosJsonParser.get_orgs(data)
        entry["institution_ids"] = institutes

        return entry
