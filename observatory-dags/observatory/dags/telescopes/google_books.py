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
from collections import OrderedDict
from datetime import datetime
from typing import List

from airflow.models.taskinstance import TaskInstance
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.telescope_utils import list_to_jsonl_gz, initialize_sftp_connection, convert
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.airflow_utils import AirflowVars, AirflowConns, AirflowVariable as Variable
import pendulum
import re
from collections import defaultdict


class GoogleBooksRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: pendulum.Pendulum, sftp_files: List[str]):
        """ Construct a GoogleBooksRelease.
        :param dag_id:
        :param release_date: the release date.
        :param reports: which reports are part of the release (sales and/or traffic)
        """
        self.dag_id = dag_id
        self.release_date = release_date
        self.project_id = Variable.get(AirflowVars.PROJECT_ID)
        self.data_location = Variable.get(AirflowVars.DATA_LOCATION)
        self.sftp_files = sftp_files

        download_files_regex = f"^{self.dag_id}_(sales|traffic)\.csv$"
        transform_files_regex = f"^{self.dag_id}_(sales|traffic)\.jsonl\.gz$"
        super().__init__(self.dag_id, release_date, download_files_regex=download_files_regex,
                         transform_files_regex=transform_files_regex)

    def download_path(self, remote_path: str) -> str:
        """ Creates full download path
        :param remote_path: filepath of remote sftp tile
        :return: Download path
        """
        report_type = 'sales' if 'Sales' in remote_path else 'traffic'
        return os.path.join(self.download_folder, f'{self.dag_id}_{report_type}.csv')

    def transform_path(self, path: str) -> str:
        """ Creates full transform path
        :param path: filepath of download file
        :return: Transform path
        """
        report_type = 'sales' if 'sales' in path else 'traffic'
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
        """ Transforms sales and traffic report. For both reports it transforms the csv into a jsonl file and
        replaces spaces in the keys with underscores.
        :return:
        """
        for file in self.download_files:
            results = []
            with open(file, encoding='utf-16') as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter='\t')
                for row in csv_reader:
                    transformed_row = OrderedDict((convert(k.replace('%', 'Perc')), v) for k, v in row.items())
                    if 'sales' in file:
                        # transform to valid date format
                        transformed_row['Transaction_Date'] = pendulum.parse(transformed_row['Transaction_Date']).to_date_string()
                        # remove percentage sign
                        transformed_row['Publisher_Revenue_Perc'] = transformed_row['Publisher_Revenue_Perc'].strip('%')
                        # this field is not present for some publishers (UCL Press), for ANU Press the field value is
                        # “E-Book”
                        try:
                            transformed_row['Line_of_Business']
                        except KeyError:
                            transformed_row['Line_of_Business'] = None
                    else:
                        # remove percentage sign
                        transformed_row['Buy_Link_CTR'] = transformed_row['Buy_Link_CTR'].strip('%')

                    results.append(transformed_row)
            list_to_jsonl_gz(self.transform_path(file), results)

    def cleanup(self):
        super().cleanup()

        sftp = initialize_sftp_connection()
        for file in self.sftp_files:
            sftp.remove(file)
        sftp.close()


class GoogleBooksTelescope(SnapshotTelescope):
    """ The Google Books telescope."""

    def __init__(self, dag_id: str = 'google_books', start_date: datetime = datetime(2015, 9, 1),
                 schedule_interval: str = '@weekly', dataset_id: str = 'google', catchup: bool = False,
                 airflow_vars=None, airflow_conns=None):
        """ Construct a GoogleBooksTelescope instance.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        """
        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        if airflow_conns is None:
            airflow_conns = [AirflowConns.SFTP_SERVICE]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, catchup=catchup,
                         airflow_vars=airflow_vars, airflow_conns=airflow_conns)
        publisher = 'anu-press'
        self.sftp_folder = f'/upload/{publisher}/google_books'
        self.sftp_regex = r'^Google(SalesTransaction|BooksTraffic)Report_\d{4}_\d{2}.csv$'

        self.add_setup_task_chain([self.check_dependencies, self.list_releases])
        self.add_task_chain(
            [self.download, self.upload_downloaded, self.transform, self.upload_transformed, self.bq_load,
             self.cleanup])

    def make_release(self, **kwargs) -> List[GoogleBooksRelease]:
        """ Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of google books release instances
        """
        ti: TaskInstance = kwargs['ti']
        reports_info = ti.xcom_pull(key=GoogleBooksTelescope.RELEASE_INFO, task_ids=self.list_releases.__name__,
                                    include_prior_dates=False)
        releases = []
        for release_date, sftp_files in reports_info.items():
            releases.append(GoogleBooksRelease(self.dag_id, release_date, sftp_files))
        return releases

    def list_releases(self, **kwargs):
        """ Lists all Google Books releases available on the SFTP server and publishes sftp file paths and
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
                release_date = file[-11:].strip('.csv')
                release_date = pendulum.from_format(release_date, 'YYYY_MM', formatter='alternative')
                sftp_file = os.path.join(self.sftp_folder, file)
                # report_type = 'sales' if m.group(1) == 'SalesTransaction' else 'traffic'
                reports_info[release_date].append(sftp_file)
        sftp.close()

        continue_dag = len(reports_info)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(GoogleBooksTelescope.RELEASE_INFO, reports_info)

        return continue_dag

    def download(self, releases: List[GoogleBooksRelease], **kwargs):
        """ Task to download the Google Books releases for a given month.
        :param releases: a list of Google Books releases.
        :return: None.
        """
        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[GoogleBooksRelease], **kwargs):
        """ Task to upload the downloaded Google Books releases for a given month.
        :param releases: a list of Google Books releases.
        :return: None.
        """
        # Upload each downloaded release
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[GoogleBooksRelease], **kwargs):
        """ Task to transform the Google Books releases for a given month.
        :param releases: a list of Google Books releases.
        :return: None.
        """
        # Transform each release
        for release in releases:
            release.transform()
