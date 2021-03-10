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

import datetime
import json
import logging
import os
import re
from datetime import datetime
from shutil import copyfile
from typing import List
from zipfile import BadZipFile, ZipFile

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.hooks.base_hook import BaseHook
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowVars, AirflowConns
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.telescope_utils import list_to_jsonl_gz
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.url_utils import retry_session

import os.path
from googleapiclient.discovery import build, Resource
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import base64
from bs4 import BeautifulSoup, SoupStrainer
import csv


class JstorRelease(SnapshotRelease):

    def __init__(self, dag_id: str, release_date: Pendulum, reports_info: List[dict]):
        """ Construct a JstorRelease.

        :param release_date: the release date.
        :param reports_info: list with report_type (country or institution) and url of reports
        """

        self.reports_info = reports_info
        download_files_regex = f"^{dag_id}_(country|institution)\.tsv$"
        transform_files_regex = f"^{dag_id}_(country|institution)\.jsonl.gz"

        super().__init__(dag_id, release_date, download_files_regex, transform_files_regex)

    def download_path(self, report_type: str) -> str:
        return os.path.join(self.download_folder, f'{self.dag_id}_{report_type}.tsv')

    def transform_path(self, report_type: str) -> str:
        return os.path.join(self.transform_folder, f'{self.dag_id}_{report_type}.jsonl.gz')

    def download(self):
        """ Downloads available reports for a single Jstor release.

        :return: None.
        """

        for report_info in self.reports_info:
            report_type = report_info['type']
            url = report_info['url']

            download_path = self.download_path(report_type)
            # Download
            logging.info(f"Downloading file: {download_path}, url: {url}")
            file_path, updated = get_file(download_path, url, cache_subdir='',
                                          cache_dir=self.download_folder)

    def transform(self):
        """ Transform a Jstor release into json lines format and gzip the result.

        :return: None.
        """

        for file in self.download_files:
            results = []
            with open(file) as tsv_file:
                csv_reader = csv.DictReader(tsv_file, delimiter='\t')

            report_type = 'country' if 'country' in file else 'institution'
            list_to_jsonl_gz(self.transform_path(report_type), results)


def create_gmail_service():
    gmail_api_conn = BaseHook.get_connection(AirflowConns.GMAIL_API)
    json_credentials = json.loads(gmail_api_conn.extra_dejson['extra__google_cloud_platform__keyfile_dict'])

    scopes = ['https://www.googleapis.com/auth/gmail.readonly']
    creds = Credentials.from_authorized_user_info(json_credentials, scopes=scopes)

    service = build('gmail', 'v1', credentials=creds, cache_discovery=False)

    return service


def list_available_releases(service, current_publisher: str) -> dict:
    available_releases = {}
    # Call the Gmail API
    results = service.users().messages().list(userId='me', q='subject:"JSTOR Publisher Report Available"').execute()
    for message_info in results['messages']:
        id = message_info['id']

        message = service.users().messages().get(userId='me', id=id).execute()

        publisher = message['snippet'].split('&quot;')[1].split(' ')[0]
        if publisher != current_publisher:
            continue

        report_type = message['snippet'].split('&quot;')[1].split(' ')[1].lower()

        timestamp = int(message['internalDate'][:-3])
        release_date = pendulum.from_timestamp(timestamp)

        download_url = None
        message_data = base64.urlsafe_b64decode(message['payload']['body']['data'])
        for link in BeautifulSoup(message_data, 'html.parser', parse_only=SoupStrainer('a')):
            if link.text == 'Download Completed Report':
                download_url = link['href']
                continue
        if download_url is None:
            raise AirflowException(f"Can't find download link for report of {publisher} at {release_date} in e-mail.")

        try:
            available_releases[release_date].append({'type': report_type, 'url': download_url})
        except KeyError:
            available_releases[release_date] = [{'type': report_type, 'url': download_url}]

    return available_releases


class JstorTelescope(SnapshotTelescope):
    """
    The JSTOR telescope.

    Saved to the BigQuery tables: <project_id>.jstor.countryYYYYMMDD and <project_id>.jstor.institutionYYYYMMDD
    """

    DAG_ID = 'jstor'

    def __init__(self, dag_id: str = DAG_ID, start_date: datetime = datetime(2015, 9, 1),
                 schedule_interval: str = '@monthly', dataset_id: str = 'jstor',
                 source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
                 dataset_description: str = '',
                 catchup: bool = True, airflow_vars: List = None, airflow_conns: List = None):
        """ Construct a JstorTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param source_format: the format of the data to load into BigQuery.
        :param dataset_description: description for the BigQuery dataset.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        """

        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        if airflow_conns is None:
            airflow_conns = [AirflowConns.GMAIL_API]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id,
                         source_format=source_format,
                         dataset_description=dataset_description,
                         catchup=catchup,
                         airflow_vars=airflow_vars, airflow_conns=airflow_conns)
        self.publisher = 'ANU'
        self.add_setup_task_chain([self.check_dependencies,
                                   self.list_releases])
        self.add_task_chain([self.download,
                             self.upload_downloaded,
                             self.transform,
                             self.upload_transformed,
                             self.bq_load,
                             self.cleanup])

    def make_release(self, **kwargs) -> List[JstorRelease]:
        """ Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of grid release instances
        """

        ti: TaskInstance = kwargs['ti']
        available_releases = ti.xcom_pull(key=JstorTelescope.RELEASE_INFO, task_ids=self.list_releases.__name__,
                                          include_prior_dates=False)
        releases = []
        for release_date in available_releases:
            reports_info = available_releases[release_date]
            releases.append(JstorRelease(self.dag_id, release_date, reports_info))
        return releases

    def list_releases(self, **kwargs):
        """ Lists all Jstor releases for a given month and publishes their report_type, download_url and
        release_date's as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        service = create_gmail_service()
        available_releases = list_available_releases(service, self.publisher)

        continue_dag = len(available_releases)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(JstorTelescope.RELEASE_INFO, available_releases)

        return continue_dag

    def download(self, releases: List[JstorRelease], **kwargs):
        """ Task to download the GRID releases for a given month.

        :param releases: a list of GRID releases.
        :return: None.
        """

        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[JstorRelease], **kwargs):
        """ Task to upload the downloaded GRID releases for a given month.

        :param releases: a list of GRID releases.
        :return: None.
        """

        # Upload each downloaded release
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[JstorRelease], **kwargs):
        """ Task to transform the GRID releases for a given month.

        :param releases: a list of GRID releases.
        :return: None.
        """

        # Transform each release
        for release in releases:
            release.transform()
