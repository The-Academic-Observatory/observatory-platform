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
from google.cloud.bigquery import SourceFormat
from typing import List
from math import ceil
from ratelimit import limits, sleep_and_retry
from urllib.error import URLError

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable


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
    TASK_ID_TRANSFORM_XML = 'transform_xml'
    TASK_ID_UPLOAD_JSON = 'upload_json'
    TASK_ID_TRANSFORM_DB_FORMAT = 'transform_db_format'
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD = 'bq_load'
    TASK_ID_CLEANUP = 'cleanup'
    TASK_ID_STOP = 'stop_dag'

    XCOM_DOWNLOAD_PATH = 'download_path'
    XCOM_UPLOAD_ZIP_PATH = 'download_zip_path'
    XCOM_JSON_PATH = 'json_path'
    XCOM_JSON_ZIP_PATH = 'json_zip_path'
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
    def transform_xml(**kwargs):
        """ Task to transform the XML from the query to json.

        Pushes the following xcom:
            json_path (str): the path to json file of a converted xml response.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def upload_json(**kwargs):
        """ Task to upload the transformed json files.

        Pushes the following xcom:
            upload_json_zip_path (str): the path to json zip file of a converted xml response.

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