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

import csv
import datetime
import os
import os.path
import re
from collections import OrderedDict, defaultdict
from datetime import datetime
from typing import List

import pendulum
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.telescope_utils import convert, initialize_sftp_connection, list_to_jsonl_gz
from observatory.platform.utils.template_utils import upload_files_from_list
from pendulum import Pendulum


class JstorRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: Pendulum, sftp_files: List[str]):
        """ Construct a JstorRelease.

        :param release_date: the release date.
        :param reports_info: list with report_type (country or institution) and url of reports
        """

        self.sftp_files = sftp_files
        download_files_regex = f"^{dag_id}_(country|institution)\.tsv$"
        transform_files_regex = f"^{dag_id}_(country|institution)\.jsonl.gz"

        super().__init__(dag_id, release_date, download_files_regex, transform_files_regex)

    def download_path(self, remote_path: str) -> str:
        """ Creates full download path
        :param remote_path: filepath of remote sftp tile
        :return: Download path
        """
        report_type = 'country' if 'BCU' in remote_path else 'institution'
        return os.path.join(self.download_folder, f'{self.dag_id}_{report_type}.tsv')

    def transform_path(self, path: str) -> str:
        """ Creates full transform path
        :param path: filepath of download file
        :return: Transform path
        """
        report_type = 'country' if 'country' in path else 'institution'
        return os.path.join(self.transform_folder, f"{self.dag_id}_{report_type}.jsonl.gz")

    def download(self):
        """ Downloads Google Books reports.
        :return: the paths on the system of the downloaded files.
        """
        sftp = initialize_sftp_connection()
        for file in self.sftp_files:
            sftp.get(file, localpath=self.download_path(file))
        sftp.close()

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

            list_to_jsonl_gz(self.transform_path(file), results)

    def cleanup(self) -> None:
        super().cleanup()

        sftp = initialize_sftp_connection()
        for file in self.sftp_files:
            sftp.remove(file)
        sftp.close()


class JstorTelescope(SnapshotTelescope):
    """
    The JSTOR telescope.

    Saved to the BigQuery tables: <project_id>.jstor.countryYYYYMMDD and <project_id>.jstor.institutionYYYYMMDD
    """

    DAG_ID = 'jstor'
    PROCESSED_LABEL_ID = 'processed_report'

    def __init__(self, dag_id: str = DAG_ID, start_date: datetime = datetime(2015, 9, 1),
                 schedule_interval: str = '@monthly', dataset_id: str = 'jstor',
                 source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON, dataset_description: str = '',
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
            airflow_conns = [AirflowConns.SFTP_SERVICE]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, source_format=source_format,
                         dataset_description=dataset_description, catchup=catchup, airflow_vars=airflow_vars,
                         airflow_conns=airflow_conns)
        publisher = 'anu-press'
        self.sftp_folder = f'/upload/{publisher}/jstor'
        self.sftp_regex = r'^PUB_(anu|ucl)press_PUB(BCU|BIU)_\d{8}\.tsv$'

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
        :return: A list of google books release instances
        """
        ti: TaskInstance = kwargs['ti']
        reports_info = ti.xcom_pull(key=JstorTelescope.RELEASE_INFO, task_ids=self.list_releases.__name__,
                                    include_prior_dates=False)
        releases = []
        for release_date, sftp_files in reports_info.items():
            releases.append(JstorRelease(self.dag_id, release_date, sftp_files))
        return releases

    def list_releases(self, **kwargs):
        """ Lists all Jstor releases available on the SFTP server and publishes sftp file paths and
        release_date's as an XCom.
        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        reports_info = defaultdict(list)

        sftp = initialize_sftp_connection()
        report_files = sftp.listdir(self.sftp_folder)
        for file in report_files:
            if re.match(self.sftp_regex, file):
                release_date = file[-12:].strip('.tsv')
                release_date = pendulum.from_format(release_date, 'YYYYMMDD', formatter='alternative')
                sftp_file = os.path.join(self.sftp_folder, file)
                reports_info[release_date].append(sftp_file)
        sftp.close()

        continue_dag = len(reports_info)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(JstorTelescope.RELEASE_INFO, reports_info)

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
