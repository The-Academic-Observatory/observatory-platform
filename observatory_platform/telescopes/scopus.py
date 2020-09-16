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


import backoff
import json
import logging
import pendulum
import os
import urllib.request
import xmltodict

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch
from google.cloud.bigquery import SourceFormat
from typing import List
from math import ceil
from ratelimit import limits, sleep_and_retry
from requests import HTTPError

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable

# Remove these later
import calendar
import pathlib
import jsonlines
import gzip
import shutil
import pickle


class ScopusTelescope:
    """ A container for holding the constants and static functions for the SCOPUS telescope."""

    DAG_ID = 'scopus'
    SUBDAG_ID_DOWNLOAD = 'download'
    DESCRIPTION = 'SCOPUS: https://www.scopus.com'
    SCHEDULE_INTERVAL = '@monthly'
    RELEASES_TOPIC_NAME = 'releases'
    QUEUE = 'remote_queue'
    RETRIES = 3

    TASK_ID_CHECK_DEPENDENCIES = 'check_dependencies'
    TASK_CHECK_API_SERVER = 'check_api_server'
    TASK_ID_DOWNLOAD = 'download'
    TASK_ID_UPLOAD_DOWNLOADED = 'upload_downloaded'
    TASK_ID_TRANSFORM_DB_FORMAT = 'transform_db_format'
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD = 'bq_load'
    TASK_ID_CLEANUP = 'cleanup'
    TASK_ID_STOP = 'stop_dag'

    XCOM_DOWNLOAD_PATH = 'download_path'
    XCOM_UPLOAD_ZIP_PATH = 'download_zip_path'
    XCOM_HARVEST_DATETIME = 'harvest_datetime'
    XCOM_JSONL_PATH = 'jsonl_path'
    XCOM_JSONL_ZIP_PATH = 'jsonl_zip_path'
    XCOM_JSONL_BLOB_PATH = 'jsonl_blob_path'

    # Implement multiprocessing later if needed. WosClient seems to be synchronous i/o so asyncio doesn't help.
    DOWNLOAD_MODE = 'parallel'  # Valid options: ['sequential', 'parallel']

    API_SERVER = ''

    @staticmethod
    def check_dependencies(**kwargs):
        """ Check that all variables exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def check_api_server(**kwargs):
        """ Check that the API server is still contactable.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

    @staticmethod
    def download(**kwargs):
        """ Task to download the WoS snapshots.

        Pushes the following xcom:
            download_path (str): the path to a pickled file containing the list of xml responses in a month query.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Task to upload the downloaded SCOPUS snapshots.

        Pushes the following xcom:
            upload_zip_path (str): the path to pickle zip file of downloaded response.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def transform_db_format(**kwargs):
        """ Task to transform the json into db field format (and in jsonlines form).

        Pushes the following xcom:
            version (str): the version of the GRID release.
            json_gz_file_name (str): the file name for the transformed GRID release.
            json_gz_file_path (str): the path to the transformed GRID release (including file name).

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def upload_transformed(**kwargs):
        """ Task to upload the transformed WoS data into jsonlines files.

        Pushes the following xcom:
            release_date (str): the release date of the GRID release.
            blob_name (str): the name of the blob on the Google Cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def bq_load(**kwargs):
        """ Task to load the transformed WoS snapshot into BigQuery.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed releases.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """


class ScopusUtilConst:
    """ Constants for the SCOPUS utility class. """

    # WoS limits. Not sure if necessary but imposing to be safe.
    RESULT_LIMIT = 100  # Return 100 results max per query.
    CALL_LIMIT = 1  # WoS says they can do 2 api calls / second.
    CALL_PERIOD = 1  # seconds
    SESSION_CALL_LIMIT = 5  # 5 calls per 5 min.
    SESSION_CALL_PERIOD = 300  # 5 minutes.
    API_KEY_QUERY_QUOTA = 20000  # API key limit for the Curtin scopus keys.
    RETRIES = 3


class ScopusUtility:
    """ Handles the SCOPUS interactions. """

    @staticmethod
    def build_query(inst_id: str, period: tuple) -> str:
        """ Build a SCOPUS API query.

        :param inst_id: Institution ID needed to filter the search results.
        :param period: A tuple containing start and end dates.
        :return: Constructed web query.
        """

        search_month = calendar.month_name(period[0].month)
        search_year = period[0].year

        query = f'(AF-ID({inst_id})) AND PUBDATETXT({search_month} AND {search_year})'

        return query

    @staticmethod
    def parse_query(records) -> dict:
        """ Parse XML tree record into a dict.

        :param records: XML tree returned by the web query.
        :return: Dictionary version of the web response.
        """

    @staticmethod
    def download_scopus_period(client: ElsClient, conn: str, period: tuple, inst_id: str, download_path: str) -> str:
        """ Download records for a stated date range.

        :param client: WebClient object.
        :param conn: file name for saved response as a pickle file.
        :param period: Period to fetch (start_date, end_date). Will fetch up to
        :param inst_id: Institutioanl ID to query, e.g, "60031226" (Curtin University)
        :param download_path: Path to download to.
         """

    @staticmethod
    def download_scopus_sequential(api_keys: List[str], schedule: list, conn: str, inst_id: str, download_path: str):
        """ Download SCOPUS snapshot sequentially. Tasks will be distributed in a round robin to the available keys.

        :param api_keys: List of API keys used to access SCOPUS service.
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param inst_id: Institutioanl ID to query, e.g, "60031226" (Curtin University)
        :param download_path: Path to download to.
        :return: List of files downloaded.
        """

    @staticmethod
    def download_scopus_parallel(api_keys: List[str], schedule: list, conn: str, inst_id: str, download_path: str):
        """ Download SCOPUS snapshot with parallel sessions. Tasks will be distributed in parallel to the available
        keys.

        :param api_keys: List of API keys used to access SCOPUS service.
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param inst_id: Institutioanl ID to query, .e.g, "Curtin University"
        :param download_path: Path to download to.
        :return: List of files downloaded.
        """

    @staticmethod
    def download_scopus_batch(api_key: str, batch: list, conn: str, inst_id: str, download_path: str):
        """ Download one batch of SCOPUS snapshots. Handle quota backoff in here.

            quota_exceeded = True if fetch_log["error"].startswith("HTTPError('QuotaExceeded") else False
            if quota_exceeded:
                renews_at = str(fetch_log["error"])[37:-2]

                fetch_log["quota_renews_at"] = renews_at

        :param api_key: API key used to access SCOPUS service.
        :param batch: List of tuples of (start_date, end_date) to fetch.
        :param conn: connection_id string from Airflow variable.
        :param inst_id: institution id to query.
        :param download_path: download path to save response to.
        :return: List of saved files from this batch.
        """

    @staticmethod
    def make_query(client: ElsClient, query: str):
        """Make the API calls to retrieve information from SCOPUS.

        :param client: ElsClient object.
        :param query: Constructed search query from use build_query.

        :return: List of XML responses.
        """

    @staticmethod
    @sleep_and_retry
    @limits(calls=ScopusUtilConst.CALL_LIMIT, period=ScopusUtilConst.CALL_PERIOD)
    def scopus_search(client: ElsClient, query: str):
        """ Throttling wrapper for the API call. This is a global limit for this API when called from a program on the
        same machine. Limits specified in WosUtilConst class.

        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param client: ElsClient object.
        :param query: Query object.
        :returns: Query results.
        """


class ScopusJsonParser:
    """ Helper methods to process the json from SCOPUS into desired structure. """


    @staticmethod
    def parse_json(data: dict, harvest_datetime: str, release_date: str) -> dict:
        """ Turn json data into db schema format.

        :param data: json response from SCOPUS.
        :param harvest_datetime: isoformat string of time the fetch took place.
        :param release_date: DAG execution date.
        :return: dict of data in right field format.
        """



################################
# Remove this after WoS branch merged

def build_schedule(sched_start_date, sched_end_date):
    """ Useful for API based data sources.
    Create a fetch schedule to specify what date ranges to use for each API call. Will default to once a month
    for now, but in the future if we are minimising API calls, this can be a more complicated scheme.

    :param sched_start_date: the schedule start date.
    :param sched_end_date: the end date of the schedule.
    :return: list of (section_start_date, section_end_date) pairs from start_date to current Airflow DAG start date.
    """

    schedule = []
    for year in range(sched_start_date.year, sched_end_date.year + 1, 1):
        for month in range(1, 13):
            start_date = pendulum.date(year, month, 1)
            last_day_start_month = calendar.monthrange(year, month)[1]
            end_date = pendulum.date(year, month, last_day_start_month)
            if sched_start_date <= start_date <= sched_end_date:
                if end_date > sched_end_date:
                    end_date = sched_end_date
                schedule.append((start_date, end_date))
    return schedule


def delete_msg_files(ti: TaskInstance, topic: str, task_id: str, msg_key: str, dag_id):
    """ Pull messages from a topic and delete the relevant paths.

    :param ti: TaskInstance.
    :param topic: Message topic.
    :param task_id: Task ID who sent message.
    :param msg_key: Key of specific messages.
    :param dag_id: DAG ID.
    """

    msgs = ti.xcom_pull(key=topic, task_ids=task_id, include_prior_dates=True, dag_id=dag_id)
    keyed_files = filter(lambda x: msg_key in x, msgs)
    files = [msg[msg_key] for msg in keyed_files]

    for file in files:
        try:
            pathlib.Path(file).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {file}: {e}")


def json_to_db(json_list: List[str], release_date: str, parser) -> List[str]:
    """ Transform json from query into database format.

    :param json_list: json data to transform.
    :param release_date: release date of the snapshot.
    :param parser: Parser function accepting (json entry, harvest date, release date), and returning schema conforming
                    data structure.
    :return: Saved file name.
    """

    jsonlines_files = list()
    if len(json_list) == 0:
        return jsonlines_files

    first_file = json_list[0][0]
    harvest_date = json_list[0][1]
    end_boundary = first_file.find('-')
    inst_tag = first_file[:end_boundary]
    save_file = f'{inst_tag}_{release_date}_{harvest_date}.jsonl'
    jsonlines_files.append(save_file)

    with jsonlines.open(save_file, mode='w') as writer:
        for (file, harvest_date) in json_list:
            with open(file, 'r') as f:
                data = json.load(f)

            parsed_entries = list()
            for entry in data:
                parsed_entry = parser(entry, harvest_date, release_date)
                parsed_entries.append(parsed_entry)

            for entry in parsed_entries:
                writer.write(entry)

    return jsonlines_files


def load_pickle(file_name: str):
    """ Load a pickle file.

    :param file_name: file to load.
    :return: contents of pickle file.
    """

    with open(file_name, 'rb') as f:
        return pickle.load(f)


def validate_date(date_string):
    """ Validate a date string is pendulum parsable.

    :param date_string: date string to check.
    :return: True if parsable, false otherwise.
    """
    try:
        pendulum.parse(date_string)
    except:
        return False
    return True


def write_json(record, file_name: str):
    """ Write a structure to json file.

    :param record: Structure to write.
    :param file_name: File name to write to.
    """
    with open(file_name, 'w') as f:
        f.write(record)


def write_pickle(record, file_name: str):
    """ Write out a pickle file of a python structure.

    :param record: Structure to write out.
    :param file_name: File name to write to.
    """

    with open(file_name, 'wb') as f:
        pickle.dump(record, f)


def write_pickled_xml_to_json(pickle_files, parser):
    """ Write a list of pickled web responses to json.

    :param pickle_files: list of pickled web response files.
    :param parser: Parsing function that parses the response into json compatible data.
    :return: List of json files written to.
    """

    json_file_list = list()

    for file in pickle_files:
        xml_list = load_pickle(file)

        parsed_list = list()
        for record in xml_list:
            parsed_record = parser(record)
            parsed_list = parsed_list + parsed_record

        json_file = f'{file[:-3]}json'
        json_file_list.append(json_file)
        json_record = json.dumps(parsed_list)
        write_json(json_record, json_file)

    return json_file_list


def zip_files(file_list: List[str]):
    """ GZip up the list of files.

    :param file_list: List of files to zip up.
    :return: List of zipped up file names.
    """

    zip_list = list()
    for file_path in file_list:
        zip_file = f'{file_path}.gz'
        zip_list.append(zip_file)
        with open(file_path, 'rb') as f_in:
            with gzip.open(zip_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    return zip_list
