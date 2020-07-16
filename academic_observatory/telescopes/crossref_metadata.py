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

import functools
import logging
import os
import pathlib
import re
import shutil
import subprocess
from typing import List, Tuple

import pendulum
import requests
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
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
from academic_observatory.utils.gc_utils import (
    bigquery_partitioned_table_id,
    create_bigquery_dataset,
    load_bigquery_table,
    upload_file_to_cloud_storage
)
from academic_observatory.utils.proc_utils import wait_for_process
from academic_observatory.utils.url_utils import retry_session
from tests.academic_observatory.config import test_fixtures_path


def xcom_pull_info(ti: TaskInstance) -> Tuple[list, str, str, str]:
    """
    Pulls xcom messages, release_urls and config_dict.
    Parses retrieved config_dict and returns those values next to release_urls.

    :param ti:
    :return: release_urls, environment, bucket, project_id
    """
    release_urls = ti.xcom_pull(key=CrossrefMetadataTelescope.XCOM_MESSAGES_NAME,
                                task_ids=CrossrefMetadataTelescope.TASK_ID_LIST,
                                include_prior_dates=False)
    config_dict = ti.xcom_pull(key=CrossrefMetadataTelescope.XCOM_CONFIG_NAME,
                               task_ids=CrossrefMetadataTelescope.TASK_ID_SETUP,
                               include_prior_dates=False)
    environment = config_dict['environment']
    bucket = config_dict['bucket']
    project_id = config_dict['project_id']
    return release_urls, environment, bucket, project_id


def list_releases(telescope_url: str, start_date: Pendulum, end_date: Pendulum) -> List:
    """
    List releases between start and end date
    :param telescope_url: url of crossref api
    :param start_date: start date
    :param end_date: end date
    :return: list of url releases
    """
    snapshot_list = []
    for month in pendulum.period(start_date, end_date.subtract(days=1)).range('months'):
        snapshot_url = f"{os.path.join(telescope_url, month.format('%Y'), month.format('%m'), 'all.json.tar.gz')}"
        response = retry_session().head(snapshot_url)
        if response:
            snapshot_list.append(snapshot_url)

    return snapshot_list


def download_release(crossref_release: 'CrossrefMetaRelease', api_token: str):
    """
    Downloads release
    :param crossref_release: Instance of CrossrefRelease class
    :param api_token: token used to access crossref data
    :return:  None.
    """
    header = {'Crossref-Plus-API-Token': api_token}
    filename = crossref_release.filepath_download
    logging.info(f"Downloading from url: {crossref_release.url}")
    with requests.get(crossref_release.url, headers=header, stream=True) as response:
        with open(filename, 'wb') as file:
            response.raw.read = functools.partial(response.raw.read, decode_content=True)
            shutil.copyfileobj(response.raw, file)
    logging.info(f"Successfully download url to {filename}")


def decompress_release(crossref_release: 'CrossrefMetaRelease') -> str:
    """
    Decompresses release.

    :param crossref_release: Instance of CrossrefRelease class
    """
    logging.info(f"Extracting file: {crossref_release.filepath_download}")

    cmd = f"tar -xOzf {crossref_release.filepath_download} > {crossref_release.filepath_extract}"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         executable='/bin/bash')
    stdout, stderr = wait_for_process(p)
    if stdout:
        logging.info(stdout)
    if stderr:
        raise AirflowException(f"bash command failed for {crossref_release.url}: {stderr}")
    logging.info(f"File extracted to: {crossref_release.filepath_extract}")

    return crossref_release.filepath_extract


def transform_release(crossref_release: 'CrossrefMetaRelease') -> str:
    """
    Transforms release with sed command.

    :param crossref_release: Instance of CrossrefRelease class
    # """
    cmd = 'mawk \'BEGIN {FS="\\":";RS=",\\"";OFS=FS;ORS=RS} {for (i=1; i<=NF;i++) if(i != NF) gsub("-", "_", $i)}1\'' \
          f' {crossref_release.filepath_extract} | ' \
          'mawk \'!/^\}$|^\]$|,\\"$/{gsub("\[\[", "[");gsub("]]", "]");gsub(/,[ \\t]*$/,"");' \
          'gsub("\\"timestamp\\":_", "\\"timestamp\\":");gsub("\\"date_parts\\":\[null]", "\\"date_parts\\":[]");' \
          'gsub(/^\{\\"items\\":\[/,"");print}\' > ' \
          f'{crossref_release.filepath_transform}'
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         executable='/bin/bash')

    stdout, stderr = wait_for_process(p)
    if stdout:
        logging.info(stdout)
    if stderr:
        raise AirflowException(f"bash command failed for {crossref_release.url}: {stderr}")
    logging.info(f'Success transforming release: {crossref_release.url}')

    return crossref_release.filepath_transform


class CrossrefMetaRelease:
    """
    Used to store info on a given crossref release
    """

    def __init__(self, url):
        self.url = url
        self.date = self.release_date()
        self.filepath_download = self.get_filepath(SubFolder.downloaded)
        self.filepath_extract = self.get_filepath(SubFolder.extracted)
        self.filepath_transform = self.get_filepath(SubFolder.transformed)

    def release_date(self) -> str:
        """
        Finds date in a given url.

        :return: date in 'YYYY-MM' format
        """
        date = re.search(r'\d{4}/\d{2}', self.url).group()

        # create date string that can be parsed by pendulum
        date = date.replace('/', '-')

        return date

    def get_filepath(self, sub_folder: SubFolder) -> str:
        """
        Gets complete path of file for download/extract/transform directory
        :param sub_folder: name of subfolder
        :return:
        """
        if sub_folder == SubFolder.downloaded:
            file_name = f"{CrossrefMetadataTelescope.DAG_ID}_{self.date}.json.tar.gz".replace('-', '_')
        else:
            file_name = f"{CrossrefMetadataTelescope.DAG_ID}_{self.date}.json".replace('-', '_')

        file_dir = telescope_path(CrossrefMetadataTelescope.DAG_ID, sub_folder)
        path = os.path.join(file_dir, file_name)

        return path

    def get_blob_name(self, sub_folder: SubFolder) -> str:
        """
        Gives blob name that is used to determine path inside storage bucket
        :param sub_folder: name of subfolder
        :return: blob name
        """
        file_name = os.path.basename(self.get_filepath(sub_folder))
        blob_name = os.path.join(f'telescopes/{CrossrefMetadataTelescope.DAG_ID}/{sub_folder.value}', file_name)

        return blob_name


class CrossrefMetadataTelescope:
    """ A container for holding the constants and static functions for the crossref metadata telescope. """

    DAG_ID = 'crossref_metadata'
    DATASET_ID = 'crossref'
    DESCRIPTION = 'Crossref metadata'
    XCOM_MESSAGES_NAME = "messages"
    XCOM_CONFIG_NAME = "config"
    TELESCOPE_URL = 'https://api.crossref.org/snapshots/monthly/'
    TELESCOPE_DEBUG_URL = 'https://api.crossref.org/snapshots/monthly/{year}/{month:02d}/all.json.tar.gz'
    DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'crossref_metadata.json.tar.gz')

    TASK_ID_SETUP = "check_setup_requirements"
    TASK_ID_LIST = "list_releases"
    TASK_ID_STOP = "stop_workflow"
    TASK_ID_DOWNLOAD = "download_releases"
    TASK_ID_DECOMPRESS = "decompress_releases"
    TASK_ID_TRANSFORM = "transform_releases"
    TASK_ID_UPLOAD = "upload_releases"
    TASK_ID_BQ_LOAD = "bq_load_releases"
    TASK_ID_CLEANUP = "cleanup_releases"

    @staticmethod
    def check_setup_requirements(**kwargs):
        """
        Depending on whether run from inside or outside a composer environment:
        If run from outside, checks if 'CONFIG_PATH' airflow variable is available and points to a valid config file.
        If run from inside, check if airflow variables for 'env', 'bucket' and 'project_id' are set.

        Also checks if crossref api token is given as the password of a 'crossref' connection.

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

                if environment != 'dev':
                    # check if crossref connection and password exists
                    try:
                        connection = BaseHook.get_connection("crossref")
                        password = connection.password
                        if not password:
                            invalid_list.append(
                                "No crossref connection password set, please set api_token as password.")
                    except AirflowException:
                        invalid_list.append(
                            'No crossref connection set, please set "crossref" connection with api_token as password.')
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
            logging.info(f'environment: {environment}, bucket: {bucket}, project_id: {project_id}')
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(CrossrefMetadataTelescope.XCOM_CONFIG_NAME, config_dict)

    @staticmethod
    def list_releases_last_month(**kwargs):
        """
        Based on a list of all releases, checks which ones were released between this and the next execution date of the
        DAG.
        If the release falls within the time period mentioned above, checks if a bigquery table doesn't exist yet for
        the release.
        A list of releases that passed both checks is passed to the next tasks. If the list is empty the workflow will
        stop.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])
        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']

        if environment == 'dev':
            release_urls_out = [CrossrefMetadataTelescope.TELESCOPE_DEBUG_URL.format(year=execution_date.year,
                                                                                     month=execution_date.month)]
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(CrossrefMetadataTelescope.XCOM_MESSAGES_NAME, release_urls_out)
            return CrossrefMetadataTelescope.TASK_ID_DOWNLOAD if release_urls_out else CrossrefMetadataTelescope.TASK_ID_STOP

        releases_list = list_releases(CrossrefMetadataTelescope.TELESCOPE_URL, execution_date, next_execution_date)
        logging.info(f'Listed releases:\n{releases_list}\n')

        bq_hook = BigQueryHook()
        # Select the releases that were published on or after the execution_date and before the next_execution_date
        release_urls_out = []
        for release_url in releases_list:
            crossref_release = CrossrefMetaRelease(release_url)
            released_date: Pendulum = pendulum.parse(crossref_release.date)
            table_id = bigquery_partitioned_table_id(CrossrefMetadataTelescope.DAG_ID, released_date)

            table_exists = bq_hook.table_exists(
                project_id=project_id,
                dataset_id=CrossrefMetadataTelescope.DATASET_ID,
                table_id=table_id
            )
            logging.info(f'Checking if bigquery table exists for release: {release_url}')
            if table_exists:
                logging.info(
                    f'Found table: {project_id}.{CrossrefMetadataTelescope.DATASET_ID}.{table_id}')
            else:
                logging.info(f"Didn't find table. Processing {release_url} in this workflow")
                release_urls_out.append(release_url)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(CrossrefMetadataTelescope.XCOM_MESSAGES_NAME, release_urls_out)
        return CrossrefMetadataTelescope.TASK_ID_DOWNLOAD if release_urls_out else CrossrefMetadataTelescope.TASK_ID_STOP

    @staticmethod
    def download(**kwargs):
        """
        Download release to file.
        If dev environment, copy debug file from this repository to the right location. Else download from url.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            crossref_release = CrossrefMetaRelease(url)
            if environment == 'dev':
                shutil.copy(CrossrefMetadataTelescope.DEBUG_FILE_PATH, crossref_release.filepath_download)
            else:
                connection = BaseHook.get_connection("crossref")
                api_token = connection.password
                download_release(crossref_release, api_token)

    @staticmethod
    def decompress(**kwargs):
        """
        Decompresses release

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            crossref_release = CrossrefMetaRelease(url)
            decompress_release(crossref_release)

    @staticmethod
    def transform(**kwargs):
        """
        Transform release with sed command and save to new file.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            crossref_release = CrossrefMetaRelease(url)
            transform_release(crossref_release)

    @staticmethod
    def upload_to_gcs(**kwargs):
        """
        Upload transformed release to a google cloud storage bucket.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            crossref_release = CrossrefMetaRelease(url)
            upload_file_to_cloud_storage(bucket, crossref_release.get_blob_name(SubFolder.transformed),
                                         file_path=crossref_release.filepath_transform)

    @staticmethod
    def load_to_bq(**kwargs):
        """
        Upload transformed release to a bigquery table.

        kwargs is required to access task instance
        """
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        # Get bucket location
        storage_client = storage.Client()
        bucket_object = storage_client.get_bucket(bucket)
        location = bucket_object.location

        # Create dataset
        dataset_id = CrossrefMetadataTelescope.DATASET_ID
        create_bigquery_dataset(project_id, dataset_id, location, CrossrefMetadataTelescope.DESCRIPTION)

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        release_urls = ti.xcom_pull(key=CrossrefMetadataTelescope.XCOM_MESSAGES_NAME,
                                    task_ids=CrossrefMetadataTelescope.TASK_ID_LIST, include_prior_dates=False)
        for url in release_urls:
            crossref_release = CrossrefMetaRelease(url)
            # get release_date
            released_date: Pendulum = pendulum.parse(crossref_release.date)
            table_id = bigquery_partitioned_table_id(CrossrefMetadataTelescope.DAG_ID, released_date)

            # Select schema file based on release date
            analysis_schema_path = schema_path('telescopes')
            schema_file_path = find_schema(analysis_schema_path, CrossrefMetadataTelescope.DAG_ID, released_date)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={CrossrefMetadataTelescope.DAG_ID}, release_date={released_date}')
                exit(os.EX_CONFIG)

            # Load BigQuery table
            uri = f"gs://{bucket}/{crossref_release.get_blob_name(SubFolder.transformed)}"
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
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            crossref_release = CrossrefMetaRelease(url)
            try:
                pathlib.Path(crossref_release.filepath_download).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {crossref_release.filepath_download}: {e}")

            try:
                pathlib.Path(crossref_release.filepath_extract).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {crossref_release.filepath_extract}: {e}")

            try:
                pathlib.Path(crossref_release.filepath_transform).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {crossref_release.filepath_transform}: {e}")
