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
import xml.etree.ElementTree as ET
from typing import List

import pendulum
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from academic_observatory.utils.config_utils import check_variables
from academic_observatory.utils.config_utils import (
    find_schema,
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
from academic_observatory.utils.proc_utils import wait_for_process
from academic_observatory.utils.url_utils import retry_session
from tests.academic_observatory.config import test_fixtures_path


def pull_releases(ti: TaskInstance) -> List:
    """ Pull a list of Unpaywall release instances with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the list of Unpaywall release instances.
    """

    return ti.xcom_pull(key=UnpaywallTelescope.RELEASES_TOPIC_NAME, task_ids=UnpaywallTelescope.TASK_ID_LIST,
                        include_prior_dates=False)


def list_releases(telescope_url: str) -> list:
    """ Parses xml string retrieved from GET request to create list of urls for
    different releases.

    :param telescope_url: url on which to execute GET request
    :return: snapshot_list, list of urls
    """
    snapshot_list = []

    xml_string = retry_session().get(telescope_url).text
    if xml_string:
        root = ET.fromstring(xml_string)
        for release in root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}Key'):
            snapshot_url = os.path.join(telescope_url, release.text)
            snapshot_list.append(snapshot_url)

    return snapshot_list


def download_release(unpaywall_release: 'UnpaywallRelease') -> str:
    """ Downloads release from url.

    :param unpaywall_release: Instance of UnpaywallRelease class
    """
    filename = unpaywall_release.get_filepath_download()
    download_dir = os.path.dirname(filename)

    # Download
    logging.info(f"Downloading file: {filename}, url: {unpaywall_release.url}")
    file_path, updated = get_file(fname=filename, origin=unpaywall_release.url, cache_dir=download_dir)

    if file_path:
        logging.info(f'Success downloading release: {filename}')
    else:
        logging.error(f"Error downloading release: {filename}")
        exit(os.EX_DATAERR)

    return file_path


def extract_release(unpaywall_release: 'UnpaywallRelease') -> str:
    """ Decompresses release.

    :param unpaywall_release: Instance of UnpaywallRelease class
    """
    logging.info(f"Extracting file: {unpaywall_release.filepath_download}")

    cmd = f"gunzip -c {unpaywall_release.filepath_download} > {unpaywall_release.filepath_extract}"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         executable='/bin/bash')
    stdout, stderr = wait_for_process(p)
    if stdout:
        logging.info(stdout)
    if stderr:
        raise AirflowException(f"bash command failed for {unpaywall_release.url}: {stderr}")
    logging.info(f"File extracted to: {unpaywall_release.filepath_extract}")

    return unpaywall_release.filepath_extract


def transform_release(unpaywall_release: 'UnpaywallRelease') -> str:
    """ Transforms release by replacing a specific '-' with '_'.

    :param unpaywall_release: Instance of UnpaywallRelease class
    """
    cmd = f"sed 's/authenticated-orcid/authenticated_orcid/g' {unpaywall_release.filepath_extract} > " \
          f"{unpaywall_release.filepath_transform}"

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')

    stdout, stderr = wait_for_process(p)
    if stdout:
        logging.info(stdout)
    if stderr:
        raise AirflowException(f"bash command failed for {unpaywall_release.url}: {stderr}")
    logging.info(f'Success transforming release: {unpaywall_release.url}')

    return unpaywall_release.filepath_transform


class UnpaywallRelease:
    def __init__(self, url):
        self.url = url
        self.date = self.release_date()
        self.filepath_download = self.get_filepath_download()
        self.filepath_extract = self.get_filepath_extract()
        self.filepath_transform = self.get_filepath_transform()

    def release_date(self) -> str:
        """ Finds date in a given url.

        :return: date in 'YYYY-MM-DD' format
        """
        date = re.search(r'\d{4}-\d{2}-\d{2}', self.url).group()

        return date

    def get_filepath_download(self) -> str:
        """ Gives path to downloaded release.

        :return: absolute file path
        """
        compressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{self.date}.jsonl.gz".replace('-', '_')
        download_dir = telescope_path(SubFolder.downloaded, UnpaywallTelescope.DAG_ID)
        path = os.path.join(download_dir, compressed_file_name)

        return path

    def get_filepath_extract(self) -> str:
        """ Gives path to extracted and decompressed release.

        :return: absolute file path
        """
        decompressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{self.date}.jsonl".replace('-', '_')
        extract_dir = telescope_path(SubFolder.extracted, UnpaywallTelescope.DAG_ID)
        path = os.path.join(extract_dir, decompressed_file_name)

        return path

    def get_filepath_transform(self) -> str:
        """ Gives path to transformed release.

        :return: absolute file path
        """
        decompressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{self.date}.jsonl".replace('-', '_')
        transform_dir = telescope_path(SubFolder.transformed, UnpaywallTelescope.DAG_ID)
        path = os.path.join(transform_dir, decompressed_file_name)

        return path


class UnpaywallTelescope:
    """ A container for holding the constants and static functions for the Unpaywall telescope. """

    DAG_ID = 'unpaywall'
    QUEUE = 'remote_queue'
    DESCRIPTION = 'The Unpaywall database: https://unpaywall.org/'
    DATASET_ID = DAG_ID
    RELEASES_TOPIC_NAME = "releases"
    TELESCOPE_URL = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/'
    TELESCOPE_DEBUG_URL = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_3000-01-27T153236.jsonl.gz'
    DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'unpaywall.jsonl.gz')

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

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        vars_valid = check_variables("data_path", "project_id", "data_location", "environment",
                                     "download_bucket_name", "transform_bucket_name")
        if not vars_valid:
            raise AirflowException('Required variables are missing')

    @staticmethod
    def list_releases(**kwargs):
        """ Based on a list of all releases, checks which ones were released between this and the next execution date
        of the DAG. If the release falls within the time period mentioned above, checks if a bigquery table doesn't
        exist yet for the release. A list of releases that passed both checks is passed to the next tasks. If the list
        is empty the workflow will stop.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get("project_id")
        environment = Variable.get("environment")

        if environment == 'dev':
            release_urls_out = [UnpaywallTelescope.TELESCOPE_DEBUG_URL]

            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(UnpaywallTelescope.RELEASES_TOPIC_NAME, release_urls_out)
            return UnpaywallTelescope.TASK_ID_DOWNLOAD if release_urls_out else UnpaywallTelescope.TASK_ID_STOP

        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']
        releases_list = list_releases(UnpaywallTelescope.TELESCOPE_URL)
        logging.info(f'All releases:\n{releases_list}\n')

        # Select the releases that were published on or after the execution_date and before the next_execution_date
        bq_hook = BigQueryHook()
        release_urls_out = []
        logging.info('Releases between current and next execution date:')
        for release_url in releases_list:
            unpaywall_release = UnpaywallRelease(release_url)
            released_date: Pendulum = pendulum.parse(unpaywall_release.date)
            table_id = bigquery_partitioned_table_id(UnpaywallTelescope.DAG_ID, released_date)

            if execution_date <= released_date < next_execution_date:
                logging.info(release_url)
                table_exists = bq_hook.table_exists(
                    project_id=project_id,
                    dataset_id=UnpaywallTelescope.DATASET_ID,
                    table_id=table_id
                )
                logging.info('Checking if bigquery table already exists:')
                if table_exists:
                    logging.info(
                        f'- Table exists for {release_url}: {project_id}.{UnpaywallTelescope.DATASET_ID}.{table_id}')
                else:
                    logging.info(f"- Table doesn't exist yet, processing {release_url} in this workflow")
                    release_urls_out.append(release_url)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(UnpaywallTelescope.RELEASES_TOPIC_NAME, release_urls_out)
        return UnpaywallTelescope.TASK_ID_DOWNLOAD if release_urls_out else UnpaywallTelescope.TASK_ID_STOP

    @staticmethod
    def download(**kwargs):
        """ Download release to file.
        If dev environment, copy debug file from this repository to the right location. Else download from url.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        release_urls = pull_releases(ti)

        # Get variables
        environment = Variable.get("environment")

        # Download each release
        for url in release_urls:
            unpaywall_release = UnpaywallRelease(url)
            if environment == 'dev':
                shutil.copy(UnpaywallTelescope.DEBUG_FILE_PATH, unpaywall_release.filepath_download)
            else:
                download_release(unpaywall_release)

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Upload downloaded release to a google cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        release_urls = pull_releases(ti)

        # Get variables
        bucket_name = Variable.get("download_bucket_name")

        # Upload each release
        for url in release_urls:
            unpaywall_release = UnpaywallRelease(url)
            blob_name = f'telescopes/unpaywall/{os.path.basename(unpaywall_release.filepath_transform)}'
            upload_file_to_cloud_storage(bucket_name, blob_name, file_path=unpaywall_release.filepath_download)

    @staticmethod
    def extract(**kwargs):
        """ Unzip release to new file.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        release_urls = pull_releases(ti)

        # Extract each release
        for url in release_urls:
            unpaywall_release = UnpaywallRelease(url)
            extract_release(unpaywall_release)

    @staticmethod
    def transform(**kwargs):
        """ Transform release with sed command and save to new file.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        release_urls = pull_releases(ti)

        # Transform each release
        for url in release_urls:
            unpaywall_release = UnpaywallRelease(url)
            transform_release(unpaywall_release)

    @staticmethod
    def upload_transformed(**kwargs):
        """ Upload transformed release to a google cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        release_urls = pull_releases(ti)

        # Get variables
        bucket_name = Variable.get("transform_bucket_name")

        # Upload each release
        for url in release_urls:
            unpaywall_release = UnpaywallRelease(url)
            blob_name = f'telescopes/unpaywall/{os.path.basename(unpaywall_release.filepath_transform)}'
            upload_file_to_cloud_storage(bucket_name, blob_name, file_path=unpaywall_release.filepath_transform)

    @staticmethod
    def load_to_bq(**kwargs):
        """ Upload transformed release to a bigquery table.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release_urls = pull_releases(ti)

        # Get variables
        project_id = Variable.get("project_id")
        bucket_name = Variable.get("transform_bucket_name")
        data_location = Variable.get("data_location")

        # Create dataset
        dataset_id = UnpaywallTelescope.DAG_ID
        create_bigquery_dataset(project_id, dataset_id, data_location, UnpaywallTelescope.DESCRIPTION)

        for url in release_urls:
            unpaywall_release = UnpaywallRelease(url)

            # Get blob name
            blob_name = f'telescopes/unpaywall/{os.path.basename(unpaywall_release.filepath_transform)}'

            # Get release_date
            released_date: Pendulum = pendulum.parse(unpaywall_release.date)
            table_id = bigquery_partitioned_table_id(UnpaywallTelescope.DAG_ID, released_date)

            # Select schema file based on release date
            analysis_schema_path = schema_path('telescopes')
            schema_file_path = find_schema(analysis_schema_path, UnpaywallTelescope.DAG_ID, released_date)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={UnpaywallTelescope.DAG_ID}, release_date={released_date}')
                exit(os.EX_CONFIG)

            # Load BigQuery table
            uri = f"gs://{bucket_name}/{blob_name}"
            logging.info(f"URI: {uri}")
            load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path,
                                SourceFormat.NEWLINE_DELIMITED_JSON)

    @staticmethod
    def cleanup_releases(**kwargs):
        """ Delete files of downloaded, extracted and transformed release.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release_urls = pull_releases(ti)

        for url in release_urls:
            unpaywall_release = UnpaywallRelease(url)
            try:
                pathlib.Path(unpaywall_release.filepath_download).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {unpaywall_release.filepath_download}: {e}")

            try:
                pathlib.Path(unpaywall_release.filepath_extract).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {unpaywall_release.filepath_extract}: {e}")

            try:
                pathlib.Path(unpaywall_release.filepath_transform).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {unpaywall_release.filepath_transform}: {e}")
