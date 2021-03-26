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
import datetime
import logging
import os
import os.path
import os.path
import time
from collections import OrderedDict
from datetime import datetime
from typing import Dict
from typing import List

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models.taskinstance import TaskInstance
from bs4 import BeautifulSoup, SoupStrainer
from google.cloud.bigquery import SourceFormat
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.telescope_utils import convert
from observatory.platform.utils.telescope_utils import list_to_jsonl_gz
from observatory.platform.utils.template_utils import upload_files_from_list
from pendulum import Pendulum


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
        """ Creates full download path

        :param report_type: The report type (country or institution)
        :return: Download path
        """
        return os.path.join(self.download_folder, f'{self.dag_id}_{report_type}.tsv')

    def transform_path(self, report_type: str) -> str:
        """ Creates full transform path

        :param report_type: The report type (country or institution)
        :return: Transform path
        """
        return os.path.join(self.transform_folder, f"{self.dag_id}_{report_type}.jsonl.gz")

    def download(self):
        """ Downloads Google Books reports.

        :return: the paths on the system of the downloaded files.
        """
        for report in self.reports_info:
            headers = create_headers(report['url'])
            time.sleep(20)
            response = requests.get(report['url'], headers=headers)
            if response.status_code != 200:
                raise AirflowException(f'Could not get content from download url, reason: {response.reason}, '
                                       f'status_code: {response.status_code}')

            content = response.content.decode('utf-8')
            with open(self.download_path(report['type']), 'w') as f:
                f.write(content)

    def transform(self):
        """ Transform a Jstor release into json lines format and gzip the result.

        :return: None.
        """

        for file in self.download_files:
            results = []
            with open(file) as tsv_file:
                csv_reader = csv.DictReader(tsv_file, delimiter='\t')
                for row in csv_reader:
                    transformed_row = OrderedDict((convert(k), v) for k, v in row.items())
                    results.append(transformed_row)

            report_type = 'country' if 'country' in file else 'institution'
            list_to_jsonl_gz(self.transform_path(report_type), results)

    def cleanup(self) -> None:
        """ Delete files of downloaded, extracted and transformed release. Add to parent method cleanup and assign a
        label to the gmail messages that have been processed.

        :return: None.
        """
        super().cleanup()

        service = create_gmail_service()
        label_id = get_label_id(service, JstorTelescope.PROCESSED_LABEL_NAME)
        for report in self.reports_info:
            message_id = report['id']
            body = {
                'addLabelIds': [label_id]
            }
            service.users().messages().modify(userId='me', id=message_id, body=body).execute()


class JstorTelescope(SnapshotTelescope):
    """
    The JSTOR telescope.

    Saved to the BigQuery tables: <project_id>.jstor.jstor_countryYYYYMMDD and
    <project_id>.jstor.jstor_institutionYYYYMMDD
    """

    DAG_ID = 'jstor'
    PROCESSED_LABEL_NAME = 'processed_report'

    def __init__(self, dag_id: str = DAG_ID, start_date: datetime = datetime(2015, 9, 1),
                 schedule_interval: str = '@monthly', dataset_id: str = 'jstor',
                 source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON, dataset_description: str = '',
                 catchup: bool = False, airflow_vars: List = None, airflow_conns: List = None):
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
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, source_format=source_format,
                         dataset_description=dataset_description, catchup=catchup, airflow_vars=airflow_vars,
                         airflow_conns=airflow_conns)
        self.publisher = 'anupress'
        self.add_setup_task_chain([self.check_dependencies, self.list_releases])
        self.add_task_chain(
            [self.download, self.upload_downloaded, self.transform, self.upload_transformed, self.bq_load,
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
        label_id = get_label_id(service, self.PROCESSED_LABEL_NAME)
        available_releases = list_available_releases(service, self.publisher, label_id)

        continue_dag = len(available_releases)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(JstorTelescope.RELEASE_INFO, available_releases)

        return continue_dag

    def download(self, releases: List[JstorRelease], **kwargs):
        """ Task to download the Jstor releases for a given month.

        :param releases: a list of Jstor releases.
        :return: None.
        """

        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[JstorRelease], **kwargs):
        """ Task to upload the downloaded Jstor releases for a given month.

        :param releases: a list of Jstor releases.
        :return: None.
        """

        # Upload each downloaded release
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[JstorRelease], **kwargs):
        """ Task to transform the Jstor releases for a given month.

        :param releases: a list of Jstor releases.
        :return: None.
        """

        # Transform each release
        for release in releases:
            release.transform()


def create_headers(url: str) -> dict:
    """ Create a headers dict that can be used to make a request

    :param url: the download url
    :return: headers dictionary
    """
    referer = f"https://www.google.com/url?q={url}" \
              f"&amp;source=gmail&amp;ust={int(time.time())}000&amp;usg=AFQjCNFtACM-4Zqs3yA1AXl4GyEbfvCqwQ"
    headers = {
        'Referer': referer,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/89.0.4389.90 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9'

    }
    return headers


def create_gmail_service() -> Resource:
    """ Build the gmail service.

    :return: Gmail service instance
    """
    gmail_api_conn = BaseHook.get_connection(AirflowConns.GMAIL_API)
    scopes = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']
    creds = Credentials.from_authorized_user_info(gmail_api_conn.extra_dejson, scopes=scopes)

    service = build('gmail', 'v1', credentials=creds, cache_discovery=False)

    return service


def get_label_id(service: Resource, label_name: str) -> str:
    """ Get the id of a label based on the label name.

    :param service: Gmail service
    :param label_name: The name of the label
    :return: The label id
    """
    existing_labels = service.users().labels().list(userId='me').execute()['labels']
    label_id = [label['id'] for label in existing_labels if label['name'] == label_name]
    if label_id:
        label_id = label_id[0]
    else:
        # create label
        label_body = {
            'name': label_name,
            'messageListVisibility': 'show',
            'labelListVisibility': 'labelShow',
            'type': 'user'
        }
        result = service.users().labels().create(userId='me', body=label_body).execute()
        label_id = result['id']
    return label_id


def message_has_label(message: dict, label_id: str) -> bool:
    """ Checks if a message has the given label

    :param message: A Gmail message
    :param label_id: The label id
    :return: True if the message has the label
    """
    label_ids = message['labelIds']
    for label in label_ids:
        if label == label_id:
            return True


def list_available_releases(service: Resource, publisher: str, processed_label_id: str) -> Dict[Pendulum, List[dict]]:
    """ List the available releases by going through the messages of a gmail account and looking for a specific pattern.

    If a message has been processed previously it has a specific label, messages with this label will be skipped.
    The message should include a download url. The head of this download url contains the filename, from which the
    release date and publisher can be derived.

    :param service: Gmail service
    :param publisher: Name of the publisher
    :param processed_label_id: Id of the 'processed_reports' label
    :return: Dictionary with release dates as key and reports info as value, where reports info is a list of country
    and/or institution reports.
    """

    available_releases = {}
    # Call the Gmail API
    results = service.users().messages().list(userId='me', q='subject:"JSTOR Publisher Report Available"').execute()
    for message_info in results['messages']:
        message_id = message_info['id']
        message = service.users().messages().get(userId='me', id=message_id).execute()

        # check if message has label 'processed'
        if message_has_label(message, processed_label_id):
            continue

        # get download url
        download_url = None
        message_data = base64.urlsafe_b64decode(message['payload']['body']['data'])
        for link in BeautifulSoup(message_data, 'html.parser', parse_only=SoupStrainer('a')):
            if link.text == 'Download Completed Report':
                download_url = link['href']
                break
        if download_url is None:
            raise AirflowException(f"Can't find download link for report in e-mail, message snippet: {message.snippet}")

        # get filename from head
        headers = create_headers(download_url)
        time.sleep(20)
        response = requests.head(download_url, headers=headers, allow_redirects=True)
        if response.status_code != 200:
            raise AirflowException(f'Could not get HEAD of report download url, reason: {response.reason}, '
                                   f'status_code: {response.status_code}')
        filename, extension = response.headers['Content-Disposition'].split('=')[1].split('.')

        # get publisher
        report_publisher = filename.split('_')[1]
        if report_publisher != publisher:
            continue

        # get report_type
        report_mapping = {
            'PUBBCU': 'country',
            'PUBBIU': 'institution'
        }
        report_type = report_mapping[filename.split('_')[2]]

        # get release date
        release_date = pendulum.parse(filename.split('_')[-1])

        # check format
        if extension != 'tsv':
            raise AirflowException(f'File "{filename}.{extension}" does not have ".tsv" extension')

        # add report info
        try:
            available_releases[release_date].append({
                'type': report_type,
                'url': download_url,
                'id': message_id
            })
        except KeyError:
            available_releases[release_date] = [{
                'type': report_type,
                'url': download_url,
                'id': message_id
            }]

        logging.info(f'Processing report. Report type: {report_type}, release date: '
                     f'{release_date}, url: {download_url}, message id: {message_id}.')
    return available_releases
