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
import json
import logging
import os
import pathlib
import shutil
from typing import Tuple

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud import storage
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from academic_observatory.utils.config_utils import (
    find_schema,
    ObservatoryConfig,
    SubFolder,
    schema_path,
    telescope_path,
)
from academic_observatory.utils.data_utils import get_file
from academic_observatory.utils.gc_utils import (
    bigquery_partitioned_table_id,
    create_bigquery_dataset,
    load_bigquery_table,
    upload_file_to_cloud_storage
)
from tests.academic_observatory.config import test_fixtures_path


def xcom_pull_info(ti: TaskInstance) -> Tuple[str, str, str, str]:
    """
    Pulls xcom messages, release_urls and config_dict.
    Parses retrieved config_dict and returns those values next to release_urls.

    :param ti:
    :return: release_date, environment, bucket, project_id
    """
    config_dict = ti.xcom_pull(key=GeonamesTelescope.XCOM_CONFIG_NAME, task_ids=GeonamesTelescope.TASK_ID_SETUP,
                               include_prior_dates=False)
    environment = config_dict['environment']
    bucket = config_dict['bucket']
    project_id = config_dict['project_id']
    release_date = config_dict['release_date']
    return release_date, environment, bucket, project_id


def download_release(geonames_release: 'GeonamesRelease') -> str:
    """
    Downloads geonames dump file containing country data. The file is in zip format and will be extracted
    after downloading, saving the unzipped content.
    :param geonames_release:
    :return: None.
    """
    filename = geonames_release.filepath_download
    filedir = os.path.dirname(geonames_release.filepath_extract)
    logging.info(f"Downloading file: {filename}, url: {GeonamesTelescope.DOWNLOAD_URL}")
    # extract zip file named 'filename' which contains 'allCountries.txt'
    file_path, updated = get_file(fname=filename, origin=GeonamesTelescope.DOWNLOAD_URL, cache_subdir=filedir,
                                  extract=True, archive_format='zip')
    # rename 'allCountries.txt' to release specific name
    os.rename(os.path.join(filedir, GeonamesTelescope.UNZIPPED_FILE_NAME), geonames_release.filepath_extract)
    if file_path:
        logging.info(f'Success downloading release: {file_path}')
    else:
        logging.error(f"Error downloading release: {file_path}")
        exit(os.EX_DATAERR)

    return file_path


def transform_release(geonames_release: 'GeonamesRelease') -> str:
    """
    Transforms release by storing file content in dict, and transforming dict to jsonl file.

    :param geonames_release: Instance of GeonamesRelease class
    """
    with open(geonames_release.filepath_extract, 'r') as f_in, open(geonames_release.filepath_transform, 'w') as f_out:
        fieldnames = ['geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude', 'featureclass',
                      'featurecode', 'countrycode', 'cc2', 'admin1code', 'admin2code', 'admin3code', 'admin4code',
                      'population', 'elevation', 'dem', 'timezone', 'modificationdate']
        reader = csv.DictReader(f_in, fieldnames=fieldnames, delimiter='\t')
        for row in reader:
            json.dump(row, f_out)
            f_out.write('\n')

    return geonames_release.filepath_transform


class GeonamesRelease:
    """ Used to store info on a given geonames release """

    def __init__(self, date: str):
        self.url = GeonamesTelescope.DOWNLOAD_URL
        self.date = date
        self.filepath_download = self.get_filepath(SubFolder.downloaded)
        self.filepath_extract = self.get_filepath(SubFolder.extracted)
        self.filepath_transform = self.get_filepath(SubFolder.transformed)

    def get_filepath(self, sub_folder: SubFolder) -> str:
        """
        Gets complete path of file for download/extract/transform directory
        :param sub_folder: name of subfolder
        :return:
        """
        if sub_folder == SubFolder.downloaded:
            file_name = f"{GeonamesTelescope.DAG_ID}_{self.date}".replace('-', '_')
        elif sub_folder == SubFolder.extracted:
            file_name = f"{GeonamesTelescope.DAG_ID}_{self.date}.txt".replace('-', '_')
        else:
            file_name = f"{GeonamesTelescope.DAG_ID}_{self.date}.jsonl".replace('-', '_')

        file_dir = telescope_path(GeonamesTelescope.DAG_ID, sub_folder)
        path = os.path.join(file_dir, file_name)

        return path

    def get_blob_name(self, sub_folder: SubFolder) -> str:
        """
        Gives blob name that is used to determine path inside storage bucket
        :param sub_folder: name of subfolder
        :return: blob name
        """
        file_name = os.path.basename(self.get_filepath(sub_folder))
        blob_name = os.path.join(f'telescopes/{GeonamesTelescope.DAG_ID}/{sub_folder.value}', file_name)

        return blob_name


class GeonamesTelescope:
    """ A container for holding the constants and static functions for the Geonames telescope. """
    DAG_ID = 'geonames'
    DESCRIPTION = 'Geonames'
    DATASET_ID = DAG_ID
    XCOM_CONFIG_NAME = "config"

    UNZIPPED_FILE_NAME = 'allCountries.txt'
    DOWNLOAD_URL = 'https://download.geonames.org/export/dump/allCountries.zip'
    DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'geonames.txt')

    TASK_ID_SETUP = "check_setup_requirements"
    TASK_ID_STOP = f"stop_workflow"
    TASK_ID_DOWNLOAD = f"download_release"
    TASK_ID_TRANSFORM = f"transform_release"
    TASK_ID_UPLOAD = f"upload_release"
    TASK_ID_BQ_LOAD = f"bq_load_release"
    TASK_ID_CLEANUP = f"cleanup_release"

    @staticmethod
    def check_setup_requirements(**kwargs):
        """
        Checks if 'CONFIG_PATH' airflow variable is available and points to a valid config file.
        The corresponding values will be stored in a dict and pushed with xcom, so they can be accessed in consequent
        tasks.
        kwargs is required to access task instance
        :param kwargs: NA
        """
        invalid_list = []
        config_dict = {}
        environment = None
        bucket = None
        project_id = None

        default_config = ObservatoryConfig.CONTAINER_DEFAULT_PATH
        config_path = Variable.get('CONFIG_PATH', default_var=default_config)

        config = ObservatoryConfig.load(config_path)
        if config is not None:
            config_is_valid = config.is_valid
            if config_is_valid:
                environment = config.environment.value
                bucket = config.bucket_name
                project_id = config.project_id
            else:
                invalid_list.append(f'Config file not valid: {config_is_valid}')
        if not config:
            invalid_list.append(f'Config file does not exist')

        if invalid_list:
            for invalid_reason in invalid_list:
                logging.warning("-" + invalid_reason + "\n\n")
            raise AirflowException
        else:
            config_dict['environment'] = environment
            config_dict['bucket'] = bucket
            config_dict['project_id'] = project_id
            config_dict['release_date'] = kwargs['ds']
            logging.info(f'environment: {environment}, bucket: {bucket}, project_id: {project_id}, '
                         f"release_date: {kwargs['ds']}")
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(GeonamesTelescope.XCOM_CONFIG_NAME, config_dict)

    @staticmethod
    def download(**kwargs):
        """
        Download release to file.
        If dev environment, copy test file from this repository to the right location. Else download from url.

        kwargs is required to access task instance
        """
        # Pull messages
        release_date, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        geonames_release = GeonamesRelease(release_date)
        if environment == 'dev':
            shutil.copy(GeonamesTelescope.DEBUG_FILE_PATH, geonames_release.filepath_extract)
        else:
            download_release(geonames_release)

    @staticmethod
    def transform(**kwargs):
        """
        Transform release into a jsonl file.

        kwargs is required to access task instance
        """
        # Pull messages
        release_date, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        geonames_release = GeonamesRelease(release_date)
        transform_release(geonames_release)

    @staticmethod
    def upload_to_gcs(**kwargs):
        """
        Upload transformed release to a google cloud storage bucket.
        kwargs is required to access task instance
        """
        # Pull messages
        release_date, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        geonames_release = GeonamesRelease(release_date)

        upload_file_to_cloud_storage(bucket, geonames_release.get_blob_name(SubFolder.transformed),
                                     file_path=geonames_release.filepath_transform)

    @staticmethod
    def load_to_bq(**kwargs):
        """
        Upload transformed release to a bigquery table.
        kwargs is required to access task instance
        """
        release_date, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        # Get bucket location
        storage_client = storage.Client()
        bucket_object = storage_client.get_bucket(bucket)
        location = bucket_object.location

        # Create dataset
        dataset_id = GeonamesTelescope.DAG_ID
        create_bigquery_dataset(project_id, dataset_id, location, GeonamesTelescope.DESCRIPTION)

        geonames_release = GeonamesRelease(release_date)
        # get release_date
        released_date: Pendulum = pendulum.parse(geonames_release.date)
        table_id = bigquery_partitioned_table_id(GeonamesTelescope.DAG_ID, released_date)

        # Select schema file based on release date
        analysis_schema_path = schema_path('telescopes')
        schema_file_path = find_schema(analysis_schema_path, GeonamesTelescope.DAG_ID, released_date)
        if schema_file_path is None:
            logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                          f'table_name={GeonamesTelescope.DAG_ID}, release_date={released_date}')
            exit(os.EX_CONFIG)

        # Load BigQuery table
        uri = f"gs://{bucket}/{geonames_release.get_blob_name(SubFolder.transformed)}"
        logging.info(f"URI: {uri}")
        load_bigquery_table(uri, dataset_id, location, table_id, schema_file_path,
                            SourceFormat.NEWLINE_DELIMITED_JSON)

    @staticmethod
    def cleanup_releases(**kwargs):
        """
        Delete files of downloaded, extracted and transformed release.
        kwargs is required to access task instance
        """
        # Pull messages
        release_date, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        geonames_release = GeonamesRelease(release_date)
        try:
            pathlib.Path(geonames_release.filepath_download).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {geonames_release.filepath_download}: {e}")

        try:
            pathlib.Path(geonames_release.filepath_transform).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {geonames_release.filepath_transform}: {e}")
