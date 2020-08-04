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

import gzip
import logging
import os
import pathlib
import shutil
from zipfile import ZipFile

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from observatory_platform.utils.config_utils import AirflowVar, SubFolder, find_schema, schema_path, telescope_path
from observatory_platform.utils.config_utils import check_variables
from observatory_platform.utils.data_utils import get_file
from observatory_platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 upload_file_to_cloud_storage)
from tests.observatory_platform.config import test_fixtures_path


def pull_release(ti: TaskInstance):
    return ti.xcom_pull(key=GeonamesTelescope.RELEASES_TOPIC_NAME, task_ids=GeonamesTelescope.TASK_ID_DOWNLOAD,
                        include_prior_dates=False)


def fetch_release_date() -> Pendulum:
    """

    :return:
    """

    response = requests.head(GeonamesTelescope.DOWNLOAD_URL)
    date_str = response.headers['Last-Modified']
    date: Pendulum = pendulum.parse(date_str, tz='GMT')
    return date


def download_release(release: 'GeonamesRelease') -> str:
    """ Downloads geonames dump file containing country data. The file is in zip format and will be extracted
    after downloading, saving the unzipped content.

    :param release: instance of GeonamesRelease class
    :return: None.
    """

    file_path, updated = get_file(fname=release.filepath_download, origin=GeonamesTelescope.DOWNLOAD_URL,
                                  cache_subdir='', extract=False)

    return file_path


def extract_release(release: 'GeonamesRelease'):
    """ Extract a downloaded Geonames release

    :param release: instance of GeonamesRelease class
    :return: file path of transformed file
    """

    extract_folder = os.path.dirname(release.filepath_extract)

    with ZipFile(release.filepath_download) as zip_file:
        zip_file.extractall(extract_folder)

    os.rename(os.path.join(extract_folder, GeonamesTelescope.UNZIPPED_FILE_NAME), release.filepath_extract)


def transform_release(release: 'GeonamesRelease') -> str:
    """ Transforms release by storing file content in gzipped csv format.

    :param release: instance of GeonamesRelease class
    :return: file path of transformed file
    """

    with open(release.filepath_extract, 'rb') as file_in:
        with gzip.open(release.filepath_transform, 'wb') as file_out:
            shutil.copyfileobj(file_in, file_out)

    return release.filepath_transform


class GeonamesRelease:
    """ Used to store info on a given geonames release """

    def __init__(self, date: Pendulum):
        self.url = GeonamesTelescope.DOWNLOAD_URL
        self.date = date
        self.filepath_download = self.get_filepath(SubFolder.downloaded)
        self.filepath_extract = self.get_filepath(SubFolder.extracted)
        self.filepath_transform = self.get_filepath(SubFolder.transformed)

    def get_filepath(self, sub_folder: SubFolder) -> str:
        """ Gets complete path of file for download/extract/transform directory.

        :param sub_folder: name of subfolder
        :return: path of file.
        """

        date_str = self.date.strftime("%Y_%m_%d")

        if sub_folder == SubFolder.downloaded:
            file_name = f"{GeonamesTelescope.DAG_ID}_{date_str}.zip"
        elif sub_folder == SubFolder.extracted:
            file_name = f"{GeonamesTelescope.DAG_ID}_{date_str}.txt"
        else:
            file_name = f"{GeonamesTelescope.DAG_ID}_{date_str}.csv.gz"

        file_dir = telescope_path(sub_folder, GeonamesTelescope.DAG_ID)
        path = os.path.join(file_dir, file_name)

        return path

    def get_blob_name(self, sub_folder: SubFolder) -> str:
        """ Gives blob name that is used to determine path inside storage bucket

        :param sub_folder: name of subfolder
        :return: blob name
        """

        file_name = os.path.basename(self.get_filepath(sub_folder))
        blob_name = f'telescopes/{GeonamesTelescope.DAG_ID}/{file_name}'

        return blob_name


class GeonamesTelescope:
    """ A container for holding the constants and static functions for the Geonames telescope. """

    DAG_ID = 'geonames'
    DESCRIPTION = 'The GeoNames geographical database: https://www.geonames.org/'
    DATASET_ID = DAG_ID
    QUEUE = 'remote_queue'
    RETRIES = 3

    UNZIPPED_FILE_NAME = 'allCountries.txt'
    DOWNLOAD_URL = 'https://download.geonames.org/export/dump/allCountries.zip'
    DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'geonames.txt')
    RELEASES_TOPIC_NAME = 'releases'

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_DOWNLOAD = f"download"
    TASK_ID_UPLOAD_DOWNLOADED = f"upload_downloaded"
    TASK_ID_EXTRACT = f"extract"
    TASK_ID_TRANSFORM = f"transform"
    TASK_ID_UPLOAD_TRANSFORMED = f"upload_transformed"
    TASK_ID_BQ_LOAD = f"bq_load"
    TASK_ID_CLEANUP = f"cleanup"

    @staticmethod
    def check_dependencies(**kwargs):
        """ Check that all variables exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        """

        vars_valid = check_variables(AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                     AirflowVar.data_location.get(), AirflowVar.download_bucket_name.get(),
                                     AirflowVar.transform_bucket_name.get())
        if not vars_valid:
            raise AirflowException('Required variables are missing')

    @staticmethod
    def download(**kwargs):
        """ Download release to file.
        If dev environment, copy test file from this repository to the right location. Else download from url.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        environment = Variable.get(AirflowVar.environment.get())

        # Fetch release date and create GeonamesRelease object
        release_date = fetch_release_date()
        release = GeonamesRelease(release_date)

        # Download release
        if environment == 'dev':
            shutil.copy(GeonamesTelescope.DEBUG_FILE_PATH, release.filepath_download)
        else:
            download_release(release)

        # Push release date for other tasks
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(GeonamesTelescope.RELEASES_TOPIC_NAME, release, kwargs['execution_date'])

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Task to upload the downloaded Geonames release.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release: GeonamesRelease = pull_release(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVar.download_bucket_name.get())

        # Upload file
        upload_file_to_cloud_storage(bucket_name, release.get_blob_name(SubFolder.downloaded),
                                     file_path=release.filepath_download)

    @staticmethod
    def extract(**kwargs):
        """ Task to extract the downloaded Geonames release.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release: GeonamesRelease = pull_release(ti)

        # Extract release
        extract_release(release)

    @staticmethod
    def transform(**kwargs):
        """ Transform release into a jsonl file.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release: GeonamesRelease = pull_release(ti)

        # Transform
        transform_release(release)

    @staticmethod
    def upload_transformed(**kwargs):
        """ Upload transformed release to a google cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release: GeonamesRelease = pull_release(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Upload file
        upload_file_to_cloud_storage(bucket_name, release.get_blob_name(SubFolder.transformed),
                                     file_path=release.filepath_transform)

    @staticmethod
    def load_to_bq(**kwargs):
        """ Upload transformed release to a bigquery table.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release date and create GeonamesRelease object
        ti: TaskInstance = kwargs['ti']
        release: GeonamesRelease = pull_release(ti)

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Create dataset and make table_id
        dataset_id = GeonamesTelescope.DATASET_ID
        create_bigquery_dataset(project_id, dataset_id, data_location, GeonamesTelescope.DESCRIPTION)
        table_id = bigquery_partitioned_table_id(GeonamesTelescope.DAG_ID, release.date)

        # Select schema file based on release date
        analysis_schema_path = schema_path('telescopes')
        schema_file_path = find_schema(analysis_schema_path, GeonamesTelescope.DAG_ID, release.date)
        if schema_file_path is None:
            logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                          f'table_name={GeonamesTelescope.DAG_ID}, release_date={release.date}')
            exit(os.EX_CONFIG)

        # Load BigQuery table
        uri = f"gs://{bucket_name}/{release.get_blob_name(SubFolder.transformed)}"
        logging.info(f"URI: {uri}")
        load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path, SourceFormat.CSV,
                            csv_field_delimiter='\t', csv_quote_character="")

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed release.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release: GeonamesRelease = pull_release(ti)

        try:
            pathlib.Path(release.filepath_download).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.filepath_download}: {e}")

        try:
            pathlib.Path(release.filepath_transform).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.filepath_transform}: {e}")
