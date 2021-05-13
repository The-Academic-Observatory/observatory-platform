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

import logging
import os
import pathlib
import re
import shutil
import subprocess
from typing import List

import pendulum
import xmltodict
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from observatory.dags.config import schema_path
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable, AirflowVars, check_variables
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.gc_utils import (bigquery_sharded_table_id,
                                                 bigquery_table_exists,
                                                 create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 upload_file_to_cloud_storage)
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.utils.template_utils import SubFolder, telescope_path, test_data_path
from observatory.platform.utils.url_utils import retry_session


def pull_releases(ti: TaskInstance) -> List:
    """ Pull a list of Unpaywall release instances with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the list of Unpaywall release instances.
    """

    return ti.xcom_pull(key=UnpaywallTelescope.RELEASES_TOPIC_NAME, task_ids=UnpaywallTelescope.TASK_ID_LIST,
                        include_prior_dates=False)


def list_releases(start_date: Pendulum, end_date: Pendulum) -> List['UnpaywallRelease']:
    """ Parses xml string retrieved from GET request to create list of urls for
    different releases.

    :param start_date:
    :param end_date:
    :return: a list of UnpaywallRelease instances.
    """
    releases_list = []

    # Request releases page
    response = retry_session().get(UnpaywallTelescope.TELESCOPE_URL)

    if response is not None and response.status_code == 200:
        # Parse releases
        items = xmltodict.parse(response.text)['ListBucketResult']['Contents']
        for item in items:
            # Get filename and parse dates
            file_name = item['Key']
            last_modified = pendulum.parse(item['LastModified'])
            release_date = UnpaywallRelease.parse_release_date(file_name)

            # Only include release if last modified date is within start and end date.
            # Last modified date is used rather than release date because if release date is used then releases will
            # be missed.
            if start_date <= last_modified < end_date:
                release = UnpaywallRelease(file_name, last_modified, release_date)
                releases_list.append(release)
    else:
        raise ConnectionError(f"Error requesting url: {UnpaywallTelescope.TELESCOPE_URL}")

    return releases_list


def download_release(release: 'UnpaywallRelease') -> str:
    """ Downloads release from url.

    :param release: Instance of UnpaywallRelease class
    """
    filename = release.get_filepath_download()
    download_dir = os.path.dirname(filename)

    # Download
    logging.info(f"Downloading file: {filename}, url: {release.url}")
    file_path, updated = get_file(fname=filename, origin=release.url, cache_dir=download_dir)

    if file_path:
        logging.info(f'Success downloading release: {filename}')
    else:
        logging.error(f"Error downloading release: {filename}")
        exit(os.EX_DATAERR)

    return file_path


def extract_release(release: 'UnpaywallRelease') -> str:
    """ Decompresses release.

    :param release: Instance of UnpaywallRelease class
    """
    logging.info(f"Extracting file: {release.filepath_download}")

    cmd = f"gunzip -c {release.filepath_download} > {release.filepath_extract}"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    stdout, stderr = wait_for_process(p)

    if stdout:
        logging.info(stdout)

    if stderr:
        raise AirflowException(f"bash command failed for {release.url}: {stderr}")

    logging.info(f"File extracted to: {release.filepath_extract}")

    return release.filepath_extract


def transform_release(release: 'UnpaywallRelease') -> str:
    """ Transforms release by replacing a specific '-' with '_'.

    :param release: Instance of UnpaywallRelease class
    """
    cmd = f"sed 's/authenticated-orcid/authenticated_orcid/g' {release.filepath_extract} > " \
          f"{release.filepath_transform}"

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    stdout, stderr = wait_for_process(p)

    if stdout:
        logging.info(stdout)

    if stderr:
        raise AirflowException(f"bash command failed for {release.url}: {stderr}")

    logging.info(f'Success transforming release: {release.url}')

    return release.filepath_transform


class UnpaywallRelease:

    def __init__(self, file_name: str, last_modified: Pendulum, release_date: Pendulum):
        self.file_name = file_name
        self.last_modified = last_modified
        self.release_date = release_date
        self.filepath_download = self.get_filepath_download()
        self.filepath_extract = self.get_filepath_extract()
        self.filepath_transform = self.get_filepath_transform()

    @property
    def url(self):
        return f'{UnpaywallTelescope.TELESCOPE_URL}{self.file_name}'

    @staticmethod
    def parse_release_date(file_name: str) -> Pendulum:
        """ Parses a release date from a file name.

        :return: date.
        """

        date = re.search(r'\d{4}-\d{2}-\d{2}', file_name).group()

        return pendulum.parse(date)

    def get_filepath_download(self) -> str:
        """ Gives path to downloaded release.

        :return: absolute file path
        """

        date_str = self.release_date.strftime("%Y_%m_%d")
        compressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{date_str}.jsonl.gz"
        download_dir = telescope_path(SubFolder.downloaded, UnpaywallTelescope.DAG_ID)
        path = os.path.join(download_dir, compressed_file_name)

        return path

    def get_filepath_extract(self) -> str:
        """ Gives path to extracted and decompressed release.

        :return: absolute file path
        """

        date_str = self.release_date.strftime("%Y_%m_%d")
        decompressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{date_str}.jsonl"
        extract_dir = telescope_path(SubFolder.extracted, UnpaywallTelescope.DAG_ID)
        path = os.path.join(extract_dir, decompressed_file_name)

        return path

    def get_filepath_transform(self) -> str:
        """ Gives path to transformed release.

        :return: absolute file path
        """

        date_str = self.release_date.strftime("%Y_%m_%d")
        decompressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{date_str}.jsonl"
        transform_dir = telescope_path(SubFolder.transformed, UnpaywallTelescope.DAG_ID)
        path = os.path.join(transform_dir, decompressed_file_name)

        return path


class UnpaywallTelescope:
    """ A container for holding the constants and static functions for the Unpaywall telescope. """

    DAG_ID = 'unpaywall'
    DATASET_ID = 'our_research'
    QUEUE = 'remote_queue'
    DESCRIPTION = 'The Unpaywall database: https://unpaywall.org/'
    RELEASES_TOPIC_NAME = "releases"
    TELESCOPE_URL = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/'
    RETRIES = 3

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_LIST = "list_releases"
    TASK_ID_STOP = "stop_dag"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_UPLOAD_DOWNLOADED = "upload_downloaded"
    TASK_ID_EXTRACT = "extract"
    TASK_ID_TRANSFORM = "transform"
    TASK_ID_UPLOAD_TRANSFORMED = "upload_transformed"
    TASK_ID_BQ_LOAD = "bq_load"
    TASK_ID_CLEANUP = "cleanup"

    @staticmethod
    def check_dependencies(**kwargs):
        """ Check that all variables exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        vars_valid = check_variables(AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                                     AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET)
        if not vars_valid:
            raise AirflowException('Required variables are missing')

    @staticmethod
    def list_releases(**kwargs):
        """ Based on a list of all releases, checks which ones were released between this and the next execution date
        of the DAG. If the release falls within the time period mentioned above, checks if a bigquery table doesn't
        exist yet for the release. A list of releases that passed both checks is passed to the next tasks. If the list
        is empty the workflow will stop.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)

        # List releases between a start and end date
        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']
        releases_list = list_releases(execution_date, next_execution_date)
        logging.info(f'Releases between {execution_date} and {next_execution_date}:\n{releases_list}\n')

        # Check if the BigQuery table exists for each release to see if the workflow needs to process
        releases_list_out = []
        for release in releases_list:
            table_id = bigquery_sharded_table_id(UnpaywallTelescope.DAG_ID, release.release_date)

            if bigquery_table_exists(project_id, UnpaywallTelescope.DATASET_ID, table_id):
                logging.info(f'Skipping as table exists for {release.url}: '
                             f'{project_id}.{UnpaywallTelescope.DATASET_ID}.{table_id}')
            else:
                logging.info(f"Table doesn't exist yet, processing {release.url} in this workflow")
                releases_list_out.append(release)

        # If releases_list_out contains items then the DAG will continue (return True) otherwise it will
        # stop (return False)
        continue_dag = len(releases_list_out)
        if continue_dag:
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(UnpaywallTelescope.RELEASES_TOPIC_NAME, releases_list_out)
        return continue_dag

    @staticmethod
    def download(**kwargs):
        """ Download release to file.
        If develop environment, copy debug file from this repository to the right location. Else download from url.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Get variables
        environment = Variable.get(AirflowVars.ENVIRONMENT)

        # Download each release
        for release in releases_list:
            if environment == 'test':
                debug_file_path = os.path.join(test_data_path(), 'telescopes', 'unpaywall.jsonl.gz')
                shutil.copy(debug_file_path, release.filepath_download)
            else:
                download_release(release)

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Upload downloaded release to a google cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVars.DOWNLOAD_BUCKET)

        # Upload each release
        for release in releases_list:
            blob_name = f'telescopes/unpaywall/{os.path.basename(release.filepath_download)}'
            upload_file_to_cloud_storage(bucket_name, blob_name, file_path=release.filepath_download)

    @staticmethod
    def extract(**kwargs):
        """ Unzip release to new file.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Extract each release
        for release in releases_list:
            extract_release(release)

    @staticmethod
    def transform(**kwargs):
        """ Transform release with sed command and save to new file.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Transform each release
        for release in releases_list:
            transform_release(release)

    @staticmethod
    def upload_transformed(**kwargs):
        """ Upload transformed release to a google cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET)

        # Upload each release
        for release in releases_list:
            blob_name = f'telescopes/unpaywall/{os.path.basename(release.filepath_transform)}'
            upload_file_to_cloud_storage(bucket_name, blob_name, file_path=release.filepath_transform)

    @staticmethod
    def load_to_bq(**kwargs):
        """ Upload transformed release to a bigquery table.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases_list: List[UnpaywallRelease] = pull_releases(ti)

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)

        # Create dataset
        create_bigquery_dataset(project_id, UnpaywallTelescope.DATASET_ID, data_location,
                                UnpaywallTelescope.DESCRIPTION)

        for release in releases_list:
            # Get blob name
            blob_name = f'telescopes/unpaywall/{os.path.basename(release.filepath_transform)}'

            # Get release_date
            table_id = bigquery_sharded_table_id(UnpaywallTelescope.DAG_ID, release.release_date)

            # Select schema file based on release date
            analysis_schema_path = schema_path()
            schema_file_path = find_schema(analysis_schema_path, UnpaywallTelescope.DAG_ID, release.release_date)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={UnpaywallTelescope.DAG_ID}, release_date={release.release_date}')
                exit(os.EX_CONFIG)

            # Load BigQuery table
            uri = f"gs://{bucket_name}/{blob_name}"
            logging.info(f"URI: {uri}")
            success = load_bigquery_table(uri, UnpaywallTelescope.DATASET_ID, data_location, table_id, schema_file_path,
                                          SourceFormat.NEWLINE_DELIMITED_JSON)
            if not success:
                raise AirflowException("bq_load task: data failed to load data into BigQuery")

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed release.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        for release in releases_list:
            try:
                pathlib.Path(release.filepath_download).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {release.filepath_download}: {e}")

            try:
                pathlib.Path(release.filepath_extract).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {release.filepath_extract}: {e}")

            try:
                pathlib.Path(release.filepath_transform).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {release.filepath_transform}: {e}")
