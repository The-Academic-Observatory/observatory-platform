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
import shutil
import subprocess

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat

from academic_observatory.utils.config_utils import (
    find_schema,
    SubFolder,
    schema_path,
    telescope_path,
    check_variables,
    check_connections
)
from academic_observatory.utils.gc_utils import (
    bigquery_partitioned_table_id,
    create_bigquery_dataset,
    load_bigquery_table,
    upload_file_to_cloud_storage
)
from academic_observatory.utils.gc_utils import bigquery_table_exists
from academic_observatory.utils.proc_utils import wait_for_process
from academic_observatory.utils.url_utils import retry_session
from tests.academic_observatory.config import test_fixtures_path


def download_release(release: 'CrossrefMetadataRelease', api_token: str):
    """ Downloads release

    :param release: Instance of CrossrefRelease class
    :param api_token: token used to access crossref data
    :return:  None.
    """

    logging.info(f"Downloading from url: {release.url}")

    # Set API token header
    header = {'Crossref-Plus-API-Token': f'Bearer {api_token}'}

    # Download release
    with requests.get(release.url, headers=header, stream=True) as response:
        # Check if authorisation with the api token was successful or not, raise error if not successful
        if response.status_code != 200:
            raise ConnectionError(f"Error downloading file {release.url}, status_code={response.status_code}")

        # Open file for saving
        with open(release.filepath_download, 'wb') as file:
            response.raw.read = functools.partial(response.raw.read, decode_content=True)
            shutil.copyfileobj(response.raw, file)

    logging.info(f"Successfully download url to {release.filepath_download}")


def extract_release(release: 'CrossrefMetadataRelease') -> str:
    """ Extract release.

    :param release: Instance of CrossrefRelease class
    """
    logging.info(f"Extracting file: {release.filepath_download}")

    cmd = f"tar -xOzf {release.filepath_download} > {release.filepath_extract}"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    stdout, stderr = wait_for_process(p)

    if stdout:
        logging.info(stdout)

    if stderr:
        raise AirflowException(f"bash command failed for {release.url}: {stderr}")

    logging.info(f"File extracted to: {release.filepath_extract}")

    return release.filepath_extract


def transform_release(release: 'CrossrefMetadataRelease') -> str:
    """ Transforms release with mawk command.

    :param release: Instance of CrossrefRelease class
    """

    cmd = 'mawk \'BEGIN {FS="\\":";RS=",\\"";OFS=FS;ORS=RS} {for (i=1; i<=NF;i++) if(i != NF) gsub("-", "_", $i)}1\'' \
          f' {release.filepath_extract} | ' \
          'mawk \'!/^\}$|^\]$|,\\"$/{gsub("\[\[", "[");gsub("]]", "]");gsub(/,[ \\t]*$/,"");' \
          'gsub("\\"timestamp\\":_", "\\"timestamp\\":");gsub("\\"date_parts\\":\[null]", "\\"date_parts\\":[]");' \
          'gsub(/^\{\\"items\\":\[/,"");print}\' > ' \
          f'{release.filepath_transform}'
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')

    stdout, stderr = wait_for_process(p)

    if stdout:
        logging.info(stdout)

    if stderr:
        raise AirflowException(f"bash command failed for {release.url}: {stderr}")

    logging.info(f'Success transforming release: {release.url}')

    return release.filepath_transform


class CrossrefMetadataRelease:
    """ Used to store info on a given crossref release """

    def __init__(self, year: int, month: int):
        self.year = year
        self.month = month
        self.url = CrossrefMetadataTelescope.TELESCOPE_URL.format(year=year, month=month)
        self.date = pendulum.datetime(year=year, month=month, day=1)
        self.filepath_download = self.get_filepath(SubFolder.downloaded)
        self.filepath_extract = self.get_filepath(SubFolder.extracted)
        self.filepath_transform = self.get_filepath(SubFolder.transformed)

    def exists(self):
        snapshot_url = CrossrefMetadataTelescope.TELESCOPE_URL.format(year=self.year, month=self.month)
        response = retry_session().head(snapshot_url)
        return response is not None and response.status_code == 302

    def get_filepath(self, sub_folder: SubFolder) -> str:
        """ Gets complete path of file for download/extract/transform directory.

        :param sub_folder: name of subfolder
        :return:
        """

        date_str = self.date.strftime("%Y_%m_%d")

        if sub_folder == SubFolder.downloaded:
            file_name = f"{CrossrefMetadataTelescope.DAG_ID}_{date_str}.json.tar.gz"
        else:
            file_name = f"{CrossrefMetadataTelescope.DAG_ID}_{date_str}.json"

        file_dir = telescope_path(sub_folder, CrossrefMetadataTelescope.DAG_ID)
        path = os.path.join(file_dir, file_name)

        return path

    def get_blob_name(self, sub_folder: SubFolder) -> str:
        """ Gives blob name that is used to determine path inside storage bucket

        :param sub_folder: name of subfolder
        :return: blob name
        """
        file_name = os.path.basename(self.get_filepath(sub_folder))
        blob_name = f'telescopes/{CrossrefMetadataTelescope.DAG_ID}/{file_name}'

        return blob_name


def pull_release(ti: TaskInstance) -> CrossrefMetadataRelease:
    """ Pull a list of CrossrefMetadataRelease instances with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the list of CrossrefMetadataRelease instances.
    """

    return ti.xcom_pull(key=CrossrefMetadataTelescope.RELEASES_TOPIC_NAME,
                        task_ids=CrossrefMetadataTelescope.TASK_ID_CHECK_RELEASE,
                        include_prior_dates=False)


class CrossrefMetadataTelescope:
    """ A container for holding the constants and static functions for the crossref metadata telescope. """

    DAG_ID = 'crossref_metadata'
    DATASET_ID = 'crossref'
    DESCRIPTION = 'The Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/'
    RELEASES_TOPIC_NAME = "releases"
    QUEUE = 'remote_queue'

    TELESCOPE_URL = 'https://api.crossref.org/snapshots/monthly/{year}/{month:02d}/all.json.tar.gz'
    DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'crossref_metadata.json.tar.gz')

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_CHECK_RELEASE = f"check_release"
    TASK_ID_DOWNLOAD = f"download"
    TASK_ID_UPLOAD_DOWNLOADED = 'upload_downloaded'
    TASK_ID_EXTRACT = f"extract"
    TASK_ID_TRANSFORM = f"transform_releases"
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD = f"bq_load"
    TASK_ID_CLEANUP = f"cleanup"

    @staticmethod
    def check_dependencies(**kwargs):
        """ Check that all variables exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        vars_valid = check_variables("data_path", "project_id", "data_location",
                                     "download_bucket_name", "transform_bucket_name")
        conns_valid = check_connections("crossref")

        if not vars_valid or not conns_valid:
            raise AirflowException('Required variables or connections are missing')

    @staticmethod
    def check_release_exists(**kwargs):
        """ Check that the release for this month exists.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get("project_id")

        # Construct the release for the execution date and check if it exists.
        # The release release for a given execution_date is added on the 5th day of the following month.
        # E.g. the 2020-05 release is added to the website on 2020-06-05.
        execution_date = kwargs['execution_date']
        print(f'Execution date: {execution_date}, {execution_date.year}, {execution_date.month}')
        continue_dag = False
        release = CrossrefMetadataRelease(execution_date.year, execution_date.month)
        if release.exists():
            table_id = bigquery_partitioned_table_id(CrossrefMetadataTelescope.DAG_ID, release.date)
            logging.info('Checking if bigquery table already exists:')
            if bigquery_table_exists(project_id, CrossrefMetadataTelescope.DATASET_ID, table_id):
                logging.info(f'Skipping as table exists for {release.url}: '
                             f'{project_id}.{CrossrefMetadataTelescope.DATASET_ID}.{table_id}')
            else:
                logging.info(f"Table doesn't exist yet, processing {release.url} in this workflow")
                continue_dag = True

        if continue_dag:
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(CrossrefMetadataTelescope.RELEASES_TOPIC_NAME, release, execution_date)
        return continue_dag

    @staticmethod
    def download(**kwargs):
        """ Download release to file. If dev environment, copy debug file from this repository to the right location.
        Else download from url.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        environment = Variable.get("environment")

        # Download release
        if environment == 'dev':
            shutil.copy(CrossrefMetadataTelescope.DEBUG_FILE_PATH, release.filepath_download)
        else:
            connection = BaseHook.get_connection("crossref")
            api_token = connection.password
            download_release(release, api_token)

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Upload the downloaded files to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        bucket_name = Variable.get("download_bucket_name")

        # Upload each release
        upload_file_to_cloud_storage(bucket_name, release.get_blob_name(SubFolder.downloaded),
                                     file_path=release.filepath_download)

    @staticmethod
    def extract(**kwargs):
        """ Extract release

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Extract the release
        extract_release(release)

    @staticmethod
    def transform(**kwargs):
        """ Transform release with sed command and save to new file.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Transform release
        transform_release(release)

    @staticmethod
    def upload_transformed(**kwargs):
        """ Upload transformed release to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        bucket_name = Variable.get("transform_bucket_name")

        # Upload release
        upload_file_to_cloud_storage(bucket_name, release.get_blob_name(SubFolder.transformed),
                                     file_path=release.filepath_transform)

    @staticmethod
    def bq_load(**kwargs):
        """ Upload transformed release to a bigquery table.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        project_id = Variable.get("project_id")
        data_location = Variable.get("data_location")
        bucket_name = Variable.get("transform_bucket_name")

        # Create dataset
        dataset_id = CrossrefMetadataTelescope.DATASET_ID
        create_bigquery_dataset(project_id, dataset_id, data_location, CrossrefMetadataTelescope.DESCRIPTION)

        # Load each release
        table_id = bigquery_partitioned_table_id(CrossrefMetadataTelescope.DAG_ID, release.date)

        # Select schema file based on release date
        analysis_schema_path = schema_path('telescopes')
        schema_file_path = find_schema(analysis_schema_path, CrossrefMetadataTelescope.DAG_ID, release.date)
        if schema_file_path is None:
            logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                          f'table_name={CrossrefMetadataTelescope.DAG_ID}, release_date={release.date}')
            exit(os.EX_CONFIG)

        # Load BigQuery table
        uri = f"gs://{bucket_name}/{release.get_blob_name(SubFolder.transformed)}"
        logging.info(f"URI: {uri}")
        load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path,
                            SourceFormat.NEWLINE_DELIMITED_JSON)

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed release.

        :param kwargs: the context passed from the PythonOperator. See https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Delete files for the release
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
