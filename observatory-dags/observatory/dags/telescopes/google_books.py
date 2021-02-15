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


import glob
import gzip
import io
import json
import logging
import os
import pathlib
import shutil
from shutil import copyfile
from typing import Dict, List, Tuple
from zipfile import BadZipFile, ZipFile

import jsonlines
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from observatory.dags.config import schema_path
from observatory.platform.utils.airflow_utils import AirflowVariable
from observatory.platform.utils.config_utils import AirflowVars, AirflowConns, SubFolder, find_schema, telescope_path, \
    check_variables
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 upload_file_to_cloud_storage)
from observatory.platform.utils.url_utils import retry_session
from selenium import webdriver
import time
from datetime import datetime
import requests
from observatory.platform.utils.url_utils import get_ao_user_agent
import jsonlines
import logging
import os
import pathlib
import pendulum
import re
import shutil
import subprocess
import tarfile
import xmltodict
from datetime import datetime, timedelta
from functools import partial
from io import BytesIO
from multiprocessing import cpu_count, Pool
from subprocess import Popen
from typing import Tuple
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from observatory.platform.utils.config_utils import (AirflowConns,
                                                     AirflowVars,
                                                     SubFolder,
                                                     check_connections,
                                                     check_variables,
                                                     find_schema,
                                                     telescope_path)
from observatory.platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 copy_bigquery_table,
                                                 create_bigquery_dataset,
                                                 download_blobs_from_cloud_storage,
                                                 load_bigquery_table,
                                                 run_bigquery_query,
                                                 upload_file_to_cloud_storage)
from observatory.platform.utils.jinja2_utils import (make_sql_jinja2_filename, render_template)
from observatory.platform.utils.proc_utils import stream_process
import csv
import io


def google_login(driver, service_account: str, password: str, login_url: str):
    driver.get(login_url)
    driver.find_element_by_id("identifierId").send_keys(service_account)
    driver.find_element_by_id("identifierNext").click()

    time.sleep(3)
    driver.find_element_by_name("password").send_keys(password)
    driver.find_element_by_id("passwordNext").click()
    time.sleep(4)
    return driver


def get_report(report_url: str, user_agent: str, cookies) -> str:
    headers = {
        'user-agent': user_agent
    }
    res = requests.get(report_url, headers=headers, cookies=cookies)
    if not res.status_code == 200 or not res.headers['content-type'] == 'text/csv; charset=utf-16le':
        # These are not the reports we're looking for
        raise ValueError(res.text)
    content = res.content.decode('utf-16')
    return content


def download_reports(release: 'GoogleBooksRelease'):
    user_agent = get_ao_user_agent()
    # User agent needs to include 'Chrome' bit, otherwise can't find correct element
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 " \
                 "Safari/537.36"

    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument(f'user-agent={user_agent}')

    #TODO install chromedriver in /usr/local/bin and remove executable path
    executable_path = '/Users/284060a/Downloads/chromedriver'
    driver = webdriver.Chrome(executable_path, chrome_options=options)

    service_account = BaseHook.get_connection(AirflowConns.OAEBU_SERVICE_ACCOUNT)
    password = BaseHook.get_connection(AirflowConns.OAEBU_PASSWORD)
    login_url = GoogleBooksTelescope.LOGIN_URL

    driver = google_login(driver, service_account, password, login_url)
    for report, url in release.report_urls.items():
        try:
            logging.info(f'Downloading report: {report}, url: {url}')
            report_content = get_report(url, user_agent, driver.get_cookies())
        except (UnicodeDecodeError, ValueError):
            raise AirflowException(f'Failed to retrieve report. This is likely to be caused by an authentication issue')
        finally:
            driver.close()

        csv_reader = csv.DictReader(io.StringIO(report_content))


def transform_reports(release: 'GoogleBooksRelease'):
    pass


def bq_load_shards(release: 'GoogleBooksRelease'):
    # Get variables
    project_id = AirflowVariable.get(AirflowVars.PROJECT_ID)
    bucket_name = AirflowVariable.get(AirflowVars.TRANSFORM_BUCKET)
    data_location = AirflowVariable.get(AirflowVars.DATA_LOCATION)

    # Create dataset
    dataset_id = GoogleBooksTelescope.DATASET_ID
    create_bigquery_dataset(project_id, dataset_id, data_location)

    for report in release.report_urls:
        # Select schema file based on release date
        table_id = 'books' + report
        release_date = release.end_date

        analysis_schema_path = schema_path()
        schema_file_path = find_schema(analysis_schema_path, table_id, release_date)
        if schema_file_path is None:
            exit(os.EX_CONFIG)

        # Create table id
        table_id = bigquery_partitioned_table_id(table_id, release_date)

        # Load BigQuery table
        blob_name = release.blob_name(release.transform_path(report))
        uri = f"gs://{bucket_name}/{blob_name}"
        logging.info(f"URI: {uri}")
        success = load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path,
                                      SourceFormat.NEWLINE_DELIMITED_JSON)
        if not success:
            raise AirflowException()


def cleanup_dirs(release: 'GoogleBooksRelease'):
    try:
        shutil.rmtree(release.subdir(SubFolder.downloaded))
    except FileNotFoundError as e:
        logging.warning(f"No such file or directory {release.subdir(SubFolder.downloaded)}: {e}")

    try:
        shutil.rmtree(release.subdir(SubFolder.transformed))
    except FileNotFoundError as e:
        logging.warning(f"No such file or directory {release.subdir(SubFolder.transformed)}: {e}")


class GoogleBooksRelease:
    """ Used to store info on a given google books release """

    def __init__(self, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, account_number: str):
        self.start_date = start_date
        self.end_date = end_date

        fmt = '%Y,%-m,%-d'
        report_types = ['TrafficReport', 'SalesTransactionReport']
        self.report_urls = {}
        for report in report_types:
            self.report_urls[report] = GoogleBooksTelescope.REPORT_URL.format(account_number=account_number,
                                                                              report=report,
                                                                              start=start_date.strftime(fmt),
                                                                              end=end_date.strftime(fmt))

    @property
    def download_dir(self) -> str:
        download_dir = self.subdir(SubFolder.downloaded.value)
        if not os.path.exists(download_dir):
            os.makedirs(download_dir, exist_ok=True)

        return download_dir

    def download_path(self, report_type: str) -> str:
        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")

        file_name = f"{GoogleBooksTelescope.DAG_ID}_{report_type}_{date_str}.csv"
        return self.get_path(SubFolder.downloaded.value, file_name)

    def transform_path(self, report_type: str) -> str:
        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")

        file_name = f"{GoogleBooksTelescope.DAG_ID}_{report_type}_{date_str}.json"
        return self.get_path(SubFolder.transformed.value, file_name)

    def blob_name(self, path: str) -> str:
        """
        Returns blob name that is used to determine path inside google cloud storage bucket
        :return: The blob name
        """
        file_name = os.path.basename(path)
        blob_name = f'telescopes/{GoogleBooksTelescope.DAG_ID}/{file_name}'

        return blob_name

    def subdir(self, sub_folder: SubFolder):
        """
        Path to subdirectory of a specific release for either downloaded/transformed files.
        :param sub_folder:
        :return:
        """
        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")
        return os.path.join(telescope_path(sub_folder, GoogleBooksTelescope.DAG_ID), date_str)

    def get_path(self, sub_folder: SubFolder, name: str) -> str:
        """
        Gets path to file based on subfolder and name. Will also create the subfolder if it doesn't exist yet.
        # :param start_date: Start date of this release
        :param sub_folder: Name of the subfolder
        :param name: File base name including extension
        :return: The file path.
        """
        release_subdir = self.subdir(sub_folder)
        if not os.path.exists(release_subdir):
            os.makedirs(release_subdir, exist_ok=True)

        path = os.path.join(release_subdir, name)
        return path


def pull_release(ti: TaskInstance) -> GoogleBooksRelease:
    return ti.xcom_pull(key=GoogleBooksTelescope.RELEASE_INFO, task_ids=GoogleBooksTelescope.TASK_ID_CREATE_RELEASE,
                        include_prior_dates=False)


class GoogleBooksTelescope:
    """ A container for holding the constants and static functions for the google books telescope. """

    DAG_ID = 'google_books'
    DATASET_ID = 'google'
    PARTITION_FIELD = ''
    DESCRIPTION = ''
    RELEASE_INFO = "releases"
    LAST_MODIFIED_XCOM = "start_date"
    QUEUE = 'remote_queue'
    MAX_RETRIES = 3
    MAX_PROCESSES = cpu_count()

    LOGIN_URL = "https://accounts.google.com/ServiceLogin"
    REPORT_URL = "https://play.google.com/books/publish/a/{account_number}/download{report}?f.req=[[null,{start}],[null,{end}],2]&hl=en-US&token="

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_CREATE_RELEASE = "create_release"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_EXTRACT = "extract"
    TASK_ID_TRANSFORM = "transform_release"
    TASK_ID_UPLOAD_TRANSFORMED = "upload_transformed"
    TASK_ID_BQ_LOAD_SHARD = "bq_load_shard"
    TASK_ID_CLEANUP = "cleanup"

    @staticmethod
    def check_dependencies():
        """ Check that all variables exist that are required to run the DAG.
        :return: None.
        """

        vars_valid = check_variables(AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID,
                                     AirflowVars.DATA_LOCATION, AirflowVars.DOWNLOAD_BUCKET,
                                     AirflowVars.TRANSFORM_BUCKET)
        conns_valid = check_connections(AirflowConns.OAEBU_PASSWORD, AirflowConns.OAEBU_SERVICE_ACCOUNT,
                                        AirflowConns.GOOGLE_BOOKS_ACCOUNT_NUMBER)

        service_account = BaseHook.get_connection(AirflowConns.OAEBU_SERVICE_ACCOUNT)
        password = BaseHook.get_connection(AirflowConns.OAEBU_PASSWORD)
        account_number = BaseHook.get_connection(AirflowConns.GOOGLE_BOOKS_ACCOUNT_NUMBER)
        logging.info(f"test connections: {service_account}, {password}, {account_number}")
        if not vars_valid or not conns_valid:
            raise AirflowException('Required variables or connections are missing')

    @staticmethod
    def create_release(**kwargs) -> bool:
        """ Create a release instance and update the xcom value with the last start date.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']

        try:
            start_date = pendulum.parse(kwargs['prev_ds_nodash'])
        except TypeError:
            start_date = pendulum.instance(kwargs['dag'].default_args['start_date']).start_of('day')
        end_date = pendulum.parse(kwargs['ds_nodash'])
        logging.info(f'Start date: {start_date}, end date: {end_date}')

        account_number = BaseHook.get_connection(AirflowConns.GOOGLE_BOOKS_ACCOUNT_NUMBER)
        release = GoogleBooksRelease(start_date, end_date, account_number)
        ti.xcom_push(GoogleBooksTelescope.RELEASE_INFO, release)
        return True

    @staticmethod
    def download(**kwargs):
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        download_reports(release)

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Upload the downloaded files to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        gcp_bucket = Variable.get(AirflowVars.DOWNLOAD_BUCKET)
        for report in release.report_urls:
            file_path = release.download_path(report)
            upload_file_to_cloud_storage(gcp_bucket, release.blob_name(file_path), file_path)

    @staticmethod
    def transform(**kwargs):
        """ Transform release with sed command and save to new file.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Transform release
        transform_reports(release)

    @staticmethod
    def upload_transformed(**kwargs):
        """ Upload transformed release to a Google Cloud Storage bucket.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        gcp_bucket = Variable.get(AirflowVars.TRANSFORM_BUCKET)
        for report in release.report_urls:
            file_path = release.transform_path(report)
            upload_file_to_cloud_storage(gcp_bucket, release.blob_name(file_path), file_path)

    @staticmethod
    def bq_load_shards(**kwargs):
        """
        Create a table shard containing only records of this release. The date in the table name is based on the end
        date of this release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None
        """
        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        bq_load_shards(release)

    @staticmethod
    def cleanup(**kwargs):
        """
        Delete subdirectories for downloaded and transformed events files.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        cleanup_dirs(release)