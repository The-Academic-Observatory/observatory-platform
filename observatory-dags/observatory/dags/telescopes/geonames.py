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

# Author: Aniek Roelofs, James Diprose

from __future__ import annotations

import datetime
import gzip
import logging
import os
import shutil
from datetime import datetime
from typing import List
from zipfile import ZipFile

import pendulum
import requests
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.template_utils import upload_files_from_list


def fetch_release_date() -> Pendulum:
    """ Fetch the Geonames release date.

    :return: the release date.
    """

    response = requests.head(GeonamesRelease.DOWNLOAD_URL)
    date_str = response.headers['Last-Modified']
    date: Pendulum = pendulum.parse(date_str, tz='GMT')
    return date


def first_sunday_of_month(datetime: Pendulum) -> Pendulum:
    """ Get the first Sunday of the month based on a given datetime.

    :param datetime: the datetime.
    :return: the first Sunday of the month.
    """

    return datetime.start_of('month').first_of('month', day_of_week=7)


class GeonamesRelease(SnapshotRelease):
    DOWNLOAD_URL = 'https://download.geonames.org/export/dump/allCountries.zip'

    def __init__(self, dag_id: str, release_date: Pendulum):
        """ Create a GeonamesRelease instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        """

        download_file_name = f'{dag_id}.zip'
        extract_file_name = f'allCountries.txt'
        transform_file_name = f'{dag_id}.csv.gz'
        super().__init__(dag_id, release_date, download_file_name, extract_file_name, transform_file_name)

    @property
    def download_path(self) -> str:
        """ Get the path to the downloaded file.

        :return: the file path.
        """

        return os.path.join(self.download_folder, self.download_files_regex)

    @property
    def extract_path(self) -> str:
        """ Get the path to the extracted file.

        :return: the file path.
        """

        return os.path.join(self.extract_folder, self.extract_files_regex)

    @property
    def transform_path(self) -> str:
        """ Get the path to the transformed file.

        :return: the file path.
        """

        return os.path.join(self.transform_folder, self.transform_files_regex)

    def download(self):
        """ Downloads geonames dump file containing country data. The file is in zip format and will be extracted
        after downloading, saving the unzipped content.

        :return: None
        """

        file_path, updated = get_file(fname=self.download_path,
                                      origin=GeonamesRelease.DOWNLOAD_URL,
                                      cache_subdir='',
                                      extract=False)

        logging.info(f'Downloaded file: {file_path}')

    def extract(self):
        """ Extract a downloaded Geonames release.

        :return: None
        """

        with ZipFile(self.download_path) as zip_file:
            zip_file.extractall(self.extract_folder)

    def transform(self):
        """ Transforms release by storing file content in gzipped csv format.

        :return: None
        """

        with open(self.extract_path, 'rb') as file_in:
            with gzip.open(self.transform_path, 'wb') as file_out:
                shutil.copyfileobj(file_in, file_out)


class GeonamesTelescope(SnapshotTelescope):
    """
    A Telescope that harvests the GeoNames geographical database: https://www.geonames.org/

    Saved to the BigQuery table: <project_id>.geonames.geonamesYYYYMMDD
    """

    DAG_ID = 'geonames'
    DATASET_DESCRIPTION = 'The GeoNames geographical database: https://www.geonames.org/'

    def __init__(self, dag_id: str = DAG_ID, start_date: datetime = datetime(2020, 9, 1),
                 schedule_interval: str = '@weekly', dataset_id: str = 'geonames',
                 catchup: bool = False):

        airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                        AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id,
                         source_format=SourceFormat.CSV,
                         dataset_description=self.DATASET_DESCRIPTION,
                         load_bigquery_table_kwargs={'csv_field_delimiter': '\t', 'csv_quote_character': ''},
                         catchup=catchup, airflow_vars=airflow_vars)
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.fetch_release_date)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[GeonamesRelease]:
        """ Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: a list of GeonamesRelease instances.
        """

        ti: TaskInstance = kwargs['ti']
        release_date = ti.xcom_pull(key=GeonamesTelescope.RELEASE_INFO,
                                    task_ids=self.fetch_release_date.__name__,
                                    include_prior_dates=False)

        return [GeonamesRelease(self.dag_id, release_date)]

    def fetch_release_date(self, **kwargs):
        """ Get the Geonames release for a given month and publishes the release_date as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: whether to keep executing the DAG.
        """

        # Check if first Sunday of month
        execution_date = kwargs['execution_date']
        run_date = first_sunday_of_month(execution_date)
        logging.info(f'execution_date={execution_date}, run_date={run_date}')

        # If first Sunday of month get current release date and push for processing
        continue_dag = execution_date == run_date
        if continue_dag:
            # Fetch release date
            release_date = fetch_release_date()

            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(GeonamesTelescope.RELEASE_INFO, release_date, execution_date)

        return continue_dag

    def download(self, releases: List[GeonamesRelease], **kwargs):
        """ Task to download the GeonamesRelease release for a given month.

        :param releases: the list of GeonamesRelease instances.
        :return: None.
        """

        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[GeonamesRelease], **kwargs):
        """ Task to upload the downloaded GeonamesRelease release for a given month.

        :param releases: the list of GeonamesRelease instances.
        :return: None.
        """

        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def extract(self, releases: List[GeonamesRelease], **kwargs):
        """ Task to extract the GeonamesRelease release for a given month.

        :param release: GeonamesRelease.
        :return: None.
        """

        for release in releases:
            release.extract()

    def transform(self, releases: List[GeonamesRelease], **kwargs):
        """ Task to transform the GeonamesRelease release for a given month.

        :param releases: the list of GeonamesRelease instances.
        :return: None.
        """

        for release in releases:
            release.transform()
