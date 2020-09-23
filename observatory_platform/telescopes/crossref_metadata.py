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

import functools
import glob
import logging
import os
import pathlib
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from subprocess import Popen

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from natsort import natsorted

from observatory_platform.utils.airflow_utils import AirflowVariable as Variable
from observatory_platform.utils.config_utils import (AirflowConn,
                                                     AirflowVar,
                                                     SubFolder,
                                                     check_connections,
                                                     check_variables,
                                                     find_schema,
                                                     schema_path,
                                                     telescope_path)
from observatory_platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 bigquery_table_exists,
                                                 create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 upload_file_to_cloud_storage,
                                                 upload_files_to_cloud_storage)
from observatory_platform.utils.proc_utils import wait_for_process
from observatory_platform.utils.url_utils import retry_session
from tests.observatory_platform.config import test_fixtures_path


def download_release(release: 'CrossrefMetadataRelease', api_token: str):
    """ Downloads release

    :param release: Instance of CrossrefRelease class
    :param api_token: token used to access crossref data
    :return:  None.
    """

    logging.info(f"Downloading from url: {release.url}")

    # Set API token header
    header = {
        'Crossref-Plus-API-Token': f'Bearer {api_token}'
    }

    # Download release
    with requests.get(release.url, headers=header, stream=True) as response:
        # Check if authorisation with the api token was successful or not, raise error if not successful
        if response.status_code != 200:
            raise ConnectionError(f"Error downloading file {release.url}, status_code={response.status_code}")

        # Open file for saving
        with open(release.download_path, 'wb') as file:
            response.raw.read = functools.partial(response.raw.read, decode_content=True)
            shutil.copyfileobj(response.raw, file)

    logging.info(f"Successfully download url to {release.download_path}")


def extract_release(release: 'CrossrefMetadataRelease') -> bool:
    """ Extract release.

    :param release: Instance of CrossrefRelease class
    """
    logging.info(f"extract_release: {release.download_path}")

    # Make directories
    os.makedirs(release.extract_path, exist_ok=True)

    # Was having issues with the next process not finding the folder that was just created, so sleep for 30 secs
    time.sleep(30)

    # Run command
    cmd = f'tar -xv -I "pigz -d" -f {release.download_path} -C {release.extract_path}'
    p: Popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    stdout, stderr = wait_for_process(p)
    logging.debug(stdout)
    success = p.returncode == 0 and 'error' not in stderr.lower()

    if success:
        logging.info(f"extract_release success: {release.download_path}")
    else:
        logging.error(f"extract_release error: {release.download_path}")
        logging.error(stdout)
        logging.error(stderr)

    return success


def transform_file(input_file_path: str, output_file_path: str) -> bool:
    r""" Transform Crossref Metadata file.

    :param input_file_path: the path of the file to transform.
    :param output_file_path: where to save the transformed file.
    :return: whether the transformation was successful or not.
    """

    cmd = 'mawk \'BEGIN {FS="\\":";RS=",\\"";OFS=FS;ORS=RS} {for (i=1; i<=NF;i++) if(i != NF) gsub("-", "_", $i)}1\'' \
          f' {input_file_path} | ' \
          'mawk \'!/^\}$|^\]$|,\\"$/{gsub("\[\[", "[");gsub("]]", "]");gsub(/,[ \\t]*$/,"");' \
          'gsub("\\"timestamp\\":_", "\\"timestamp\\":");gsub("\\"date_parts\\":\[null]", "\\"date_parts\\":[]");' \
          'gsub(/^\{\\"items\\":\[/,"");print}\' > ' \
          f'{output_file_path}'
    p: Popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    stdout, stderr = wait_for_process(p)
    logging.debug(stdout)
    success = p.returncode == 0

    if success:
        logging.info(f"transform_file success: {input_file_path}")
    else:
        logging.error(f"transform_file error: {input_file_path}")
        logging.error(stderr)

    return success


def transform_release(release: 'CrossrefMetadataRelease', max_workers: int = cpu_count()) -> bool:
    """ Transform a Crossref Metadata release into a form that can be loaded into BigQuery.

    :param release: the CrossrefMetadataRelease release
    :param max_workers: the number of processes to use when transforming files (one process per file).
    :return: whether the transformation was successful or not.
    """

    # input_release_path: the path to the folder containing the extracted files for the Crossref Metadata release.
    # output_release_path: the path where the transformed files will be saved.
    input_release_path = release.extract_path
    output_release_path = release.transform_path

    # Transform each file in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        futures_msgs = {}

        # List files and sort so that they are processed in ascending order
        input_file_paths = natsorted(glob.glob(f"{input_release_path}/*.json"))

        # Make output release path in case it hasn't been created
        os.makedirs(output_release_path, exist_ok=True)

        # Create tasks for each file
        for input_file_path in input_file_paths:
            # The output file will be a json lines file, hence adding the 'l' to the file extension
            output_file_path = os.path.join(output_release_path, os.path.basename(input_file_path) + 'l')
            msg = f'input_file_path={input_file_path}, output_file_path={output_file_path}'
            logging.info(f'transform_release: {msg}')
            future = executor.submit(transform_file, input_file_path, output_file_path)
            futures.append(future)
            futures_msgs[future] = msg

        # Wait for completed tasks
        results = []
        for future in as_completed(futures):
            success = future.result()
            msg = futures_msgs[future]
            results.append(success)
            if success:
                logging.info(f'transform_release success: {msg}')
            else:
                logging.error(f'transform_release failed: {msg}')

    return all(results)


class CrossrefMetadataRelease:
    """ Used to store info on a given crossref release """

    def __init__(self, year: int, month: int):
        self.year = year
        self.month = month
        self.url = CrossrefMetadataTelescope.TELESCOPE_URL.format(year=year, month=month)
        self.date = pendulum.datetime(year=year, month=month, day=1)

    def exists(self):
        snapshot_url = CrossrefMetadataTelescope.TELESCOPE_URL.format(year=self.year, month=self.month)
        response = retry_session().head(snapshot_url)
        return response is not None and response.status_code == 302

    @property
    def download_path(self):
        return self.get_path(SubFolder.downloaded)

    @property
    def extract_path(self):
        return self.get_path(SubFolder.extracted)

    @property
    def transform_path(self):
        return self.get_path(SubFolder.transformed)

    def get_path(self, sub_folder: SubFolder) -> str:
        """ Gets complete path of file for download/extract/transform directory or file.

        :param sub_folder: name of subfolder
        :return: the path.
        """

        date_str = self.date.strftime("%Y_%m_%d")
        file_name = f"{CrossrefMetadataTelescope.DAG_ID}_{date_str}"
        if sub_folder == SubFolder.downloaded:
            file_name = f"{file_name}.json.tar.gz"

        path = os.path.join(telescope_path(sub_folder, CrossrefMetadataTelescope.DAG_ID), file_name)
        return path

    def get_blob_name(self, sub_folder: SubFolder) -> str:
        """ Gives blob name that is used to determine path inside storage bucket

        :param sub_folder: name of subfolder
        :return: blob name
        """

        file_name = os.path.basename(self.get_path(sub_folder))
        blob_name = f'telescopes/{CrossrefMetadataTelescope.DAG_ID}/{file_name}'

        return blob_name


def pull_release(ti: TaskInstance) -> CrossrefMetadataRelease:
    """ Pull a list of CrossrefMetadataRelease instances with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the list of CrossrefMetadataRelease instances.
    """

    return ti.xcom_pull(key=CrossrefMetadataTelescope.RELEASES_TOPIC_NAME,
                        task_ids=CrossrefMetadataTelescope.TASK_ID_CHECK_RELEASE, include_prior_dates=False)


class CrossrefMetadataTelescope:
    """ A container for holding the constants and static functions for the crossref metadata telescope. """

    DAG_ID = 'crossref_metadata'
    DATASET_ID = 'crossref'
    DESCRIPTION = 'The Crossref Metadata Plus dataset: ' \
                  'https://www.crossref.org/services/metadata-retrieval/metadata-plus/'
    RELEASES_TOPIC_NAME = "releases"
    QUEUE = 'remote_queue'
    MAX_PROCESSES = cpu_count()
    MAX_CONNECTIONS = cpu_count()
    MAX_RETRIES = 3

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

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        vars_valid = check_variables(AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                     AirflowVar.data_location.get(), AirflowVar.download_bucket_name.get(),
                                     AirflowVar.transform_bucket_name.get())
        conns_valid = check_connections(AirflowConn.crossref.get())

        if not vars_valid or not conns_valid:
            raise AirflowException('Required variables or connections are missing')

    @staticmethod
    def check_release_exists(**kwargs):
        """ Check that the release for this month exists.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())

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

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        environment = Variable.get(AirflowVar.environment.get())

        # Download release
        if environment == 'dev':
            shutil.copy(CrossrefMetadataTelescope.DEBUG_FILE_PATH, release.download_path)
        else:
            connection = BaseHook.get_connection(AirflowConn.crossref.get())
            api_token = connection.password
            download_release(release, api_token)

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Upload the downloaded files to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVar.download_bucket_name.get())

        # Upload each release
        upload_file_to_cloud_storage(bucket_name, release.get_blob_name(SubFolder.downloaded),
                                     file_path=release.download_path)

    @staticmethod
    def extract(**kwargs):
        """ Extract release

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Extract the release
        result = extract_release(release)

        # Check result
        if result:
            logging.info(f'extract success: {release}')
        else:
            logging.error(f"extract error: {release}")
            exit(os.EX_DATAERR)

    @staticmethod
    def transform(**kwargs):
        """ Transform release with sed command and save to new file.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Transform release
        result = transform_release(release, max_workers=CrossrefMetadataTelescope.MAX_PROCESSES)

        # Check result
        if result:
            logging.info(f'transform success: {release}')
        else:
            logging.error(f"transform error: {release}")
            exit(os.EX_DATAERR)

    @staticmethod
    def upload_transformed(**kwargs):
        """ Upload transformed release to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # List files and sort so that they are processed in ascending order
        logging.info(f'upload_transformed listing files')
        file_paths = natsorted(glob.glob(f"{release.transform_path}/*.jsonl"))

        # List blobs
        logging.info(f'upload_transformed creating blob names')
        blob_names = [f'{release.get_blob_name(SubFolder.transformed)}/{os.path.basename(path)}' for path in file_paths]

        # Upload files
        logging.info(f'upload_transformed begin uploading files')
        success = upload_files_to_cloud_storage(bucket_name, blob_names, file_paths,
                                                max_processes=CrossrefMetadataTelescope.MAX_PROCESSES,
                                                max_connections=CrossrefMetadataTelescope.MAX_CONNECTIONS,
                                                retries=CrossrefMetadataTelescope.MAX_RETRIES)
        if success:
            logging.info(f'upload_transformed success: {release}')
        else:
            logging.error(f"upload_transformed error: {release}")
            exit(os.EX_DATAERR)

    @staticmethod
    def bq_load(**kwargs):
        """ Upload transformed release to a bigquery table.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

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
        uri = f"gs://{bucket_name}/{release.get_blob_name(SubFolder.transformed)}/*"
        logging.info(f"URI: {uri}")
        load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path,
                            SourceFormat.NEWLINE_DELIMITED_JSON)

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
        release = pull_release(ti)

        # Delete files for the release
        try:
            pathlib.Path(release.download_path).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.download_path}: {e}")

        try:
            shutil.rmtree(release.extract_path)
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.extract_path}: {e}")

        try:
            shutil.rmtree(release.transform_path)
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.transform_path}: {e}")
