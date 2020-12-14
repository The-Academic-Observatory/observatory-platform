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

# Author: James Diprose

import glob
import logging
import os
import re
import shutil
import subprocess
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from pathlib import Path, PosixPath
from subprocess import Popen
from typing import List

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models.taskinstance import TaskInstance
from google.cloud import storage
from google.cloud.bigquery import SourceFormat
from google.cloud.storage import Blob
from mag_archiver.mag import MagArchiverClient, MagDateType, MagRelease, MagState
from natsort import natsorted
from pendulum import Pendulum

from observatory.dags.config import schema_path
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable
from observatory.platform.utils.config_utils import (AirflowConns,
                                                     AirflowVars,
                                                     SubFolder,
                                                     check_connections,
                                                     check_variables,
                                                     telescope_path,
                                                     test_data_path)
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.gc_utils import (azure_to_google_cloud_storage_transfer,
                                                 bigquery_partitioned_table_id,
                                                 create_bigquery_dataset,
                                                 download_blobs_from_cloud_storage,
                                                 load_bigquery_table,
                                                 table_name_from_blob,
                                                 upload_files_to_cloud_storage,
                                                 bigquery_table_exists)
from observatory.platform.utils.proc_utils import wait_for_process


def pull_releases(ti: TaskInstance) -> List[MagRelease]:
    """ Pull a list of MagRelease instances with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the list of MagRelease instances.
    """

    return ti.xcom_pull(key=MagTelescope.RELEASES_TOPIC_NAME, task_ids=MagTelescope.TASK_ID_LIST,
                        include_prior_dates=False)


def list_mag_release_files(release_path: str) -> List[PosixPath]:
    """ List the MAG release file paths in a particular folder. Excludes the samples directory.
    :param release_name: the name of the MAG release.
    :param release_path: the path to the MAG release.
    :return: a list of PosixPath files.
    """

    release_folder = os.path.basename(os.path.abspath(release_path))
    include_regex = fr'^.*/{release_folder}(/advanced|/mag|/nlp)?/\w+.txt(.[0-9]+)?$'

    types = ['*.txt', '*.txt.[0-9]']
    files = []
    for file_type in types:
        paths = list(Path(release_path).rglob(file_type))
        for path in paths:
            path_string = str(path.resolve())
            if re.match(include_regex, path_string) is not None:
                files.append(path)
    files = natsorted(files, key=lambda x: str(x))
    return files


def transform_mag_file(input_file_path: str, output_file_path: str) -> bool:
    r""" Transform MAG file, removing the \x0 and \r characters. \r is the ^M windows character.

    :param input_file_path: the path of the file to transform.
    :param output_file_path: where to save the transformed file.
    :return: whether the transformation was successful or not.
    """

    # TODO: see if we can get rid of shell=True
    bash_command = fr"sed 's/\r//g; s/\x0//g' {str(input_file_path)} > {output_file_path}"
    logging.info(f"transform_mag_file bash command: {bash_command}")
    proc: Popen = subprocess.Popen(bash_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = wait_for_process(proc)
    logging.debug(output)
    success = proc.returncode == 0

    if success:
        logging.info(f"transform_mag_file success: {input_file_path}")
    else:
        logging.error(f"transform_mag_file error: {input_file_path}")
        logging.error(error)

    return success


def transform_mag_release(input_release_path: str, output_release_path: str, max_workers: int = cpu_count()) -> bool:
    """ Transform a MAG release into a form that can be loaded into BigQuery.

    :param input_release_path: the path to the folder containing the files for the MAG release.
    :param output_release_path: the path where the transformed files will be saved.
    :param max_workers: the number of processes to use when transforming files (one process per file).
    :return: whether the transformation was successful or not.
    """

    # Transform each file in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create tasks
        futures = []
        futures_msgs = {}

        paths = list_mag_release_files(input_release_path)
        for path in paths:
            # Make path to save file
            os.makedirs(output_release_path, exist_ok=True)
            output_path = os.path.join(output_release_path, path.name)
            msg = f'input_file_path={path}, output_file_path={output_path}'
            logging.info(f'transform_mag_release: {msg}')
            future = executor.submit(transform_mag_file, path, output_path)
            futures.append(future)
            futures_msgs[future] = msg

        # Wait for completed tasks
        results = []
        for future in as_completed(futures):
            success = future.result()
            msg = futures_msgs[future]
            results.append(success)
            if success:
                logging.info(f'transform_mag_release success: {msg}')
            else:
                logging.error(f'transform_mag_release failed: {msg}')

    return all(results)


class MagTelescope:
    """ A container for holding the constants and static functions for the Microsoft Academic Graph (MAG) telescope.

        Requires the following connections to be added to Airflow:
            mag_releases_table: the Azure account name (login) and sas token (password) for the MagReleases table in
            Azure.
            mag_snapshots_container: the Azure Storage Account name (login) and the sas token (password) for the
            Azure storage blob container that contains the MAG releases.
    """

    DAG_ID = 'mag'
    DATASET_ID = 'mag'
    QUEUE = 'remote_queue'
    DESCRIPTION = 'The Microsoft Academic Graph (MAG) dataset: https://www.microsoft.com/en-us/research/project/' \
                  'microsoft-academic-graph/'
    RELEASES_TOPIC_NAME = 'releases'
    MAX_PROCESSES = cpu_count()
    MAX_CONNECTIONS = cpu_count()
    RETRIES = 3

    TASK_ID_CHECK_DEPENDENCIES = 'check_dependencies'
    TASK_ID_LIST = 'list_releases'
    TASK_ID_TRANSFER = 'transfer'
    TASK_ID_DOWNLOAD = 'download'
    TASK_ID_TRANSFORM = 'transform'
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD = 'bq_load'
    TASK_ID_CLEANUP = 'cleanup'

    @staticmethod
    def check_dependencies(**kwargs):
        """ Check that all variables and connections exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        vars_valid = check_variables(AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID,
                                     AirflowVars.DATA_LOCATION, AirflowVars.DOWNLOAD_BUCKET,
                                     AirflowVars.TRANSFORM_BUCKET)
        conns_valid = check_connections(AirflowConns.MAG_RELEASES_TABLE, AirflowConns.MAG_SNAPSHOTS_CONTAINER)

        if not vars_valid or not conns_valid:
            raise AirflowException('Required variables or connections are missing')

    @staticmethod
    def list_releases(**kwargs):
        """ Task to list all MAG releases for a given month.

        Requires the following connection to be added to Airflow:
            mag_releases_table: the Azure account name (login) and the sas token (password) for the MagReleases table in
            Azure.

        Pushes the following xcom:
            a list of MagRelease instances.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        connection = BaseHook.get_connection("mag_releases_table")
        account_name = connection.login
        sas_token = connection.password
        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']
        project_id = Variable.get(AirflowVars.PROJECT_ID)

        client = MagArchiverClient(account_name=account_name, sas_token=sas_token)
        releases: List[MagRelease] = client.list_releases(start_date=execution_date, end_date=next_execution_date,
                                                          state=MagState.done, date_type=MagDateType.done)

        # Check if we can skip any releases
        releases_out = []
        logging.info('Check if releases already exist:')
        for release in releases:
            table_id = bigquery_partitioned_table_id(MagTelescope.DAG_ID, release.release_date)

            if bigquery_table_exists(project_id, MagTelescope.DATASET_ID, table_id):
                logging.info(f'Skipping as table exists for MAG {release.release_date} release: '
                             f'{project_id}.{MagTelescope.DATASET_ID}.{table_id}')
            else:
                logging.info(f"Table doesn't exist yet, processing MAG {release.release_date} release in this workflow")
                releases_out.append(release)

        continue_dag = len(releases_out)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(MagTelescope.RELEASES_TOPIC_NAME, releases_out, execution_date)
        return continue_dag

    @staticmethod
    def transfer(**kwargs):
        """ Task to transfer a MAG release from Azure to Google Cloud Storage.

        Requires the following connection to be added to Airflow:
            mag_snapshots_container: the Azure Storage Account name (login) and the sas token (password) for the
            Azure storage blob container that contains the MAG releases.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        # Get variables
        environment = Variable.get(AirflowVars.ENVIRONMENT)
        gcp_project_id = Variable.get(AirflowVars.PROJECT_ID)
        gcp_bucket_name = Variable.get(AirflowVars.DOWNLOAD_BUCKET)

        if environment == 'test':
            # TODO: this is a bit messy. In the future, for the test environment we should just have smaller files at
            # the MAG endpoint so that the transferring can be tested too
            mag_zip = os.path.join(test_data_path(), 'telescopes', 'mag-2020-05-21.zip')

            for release in releases:
                extracted_path = os.path.join('/tmp', release.source_container)
                os.makedirs(extracted_path, exist_ok=True)
                with zipfile.ZipFile(mag_zip, 'r') as zip_ref:
                    zip_ref.extractall(extracted_path)

                paths = glob.glob(f'{extracted_path}/**/*.*', recursive=True)
                blob_names = []
                for path in paths:
                    file_path = path.replace(f'{extracted_path}/mag-2020-05-21/', "")
                    blob_name = f'telescopes/{MagTelescope.DAG_ID}/{release.source_container}/{file_path}'
                    blob_names.append(blob_name)

                success = upload_files_to_cloud_storage(gcp_bucket_name, blob_names, paths,
                                                        max_processes=MagTelescope.MAX_PROCESSES,
                                                        max_connections=MagTelescope.MAX_CONNECTIONS,
                                                        retries=MagTelescope.RETRIES)
                if success:
                    logging.info(f'Success uploading develop MAG release to cloud storage: {release}')
                else:
                    logging.error(f"Error uploading develop MAG release to cloud storage: {release}")
                    exit(os.EX_DATAERR)
        else:
            # Get Azure connection information
            connection = BaseHook.get_connection("mag_snapshots_container")
            azure_account_name = connection.login
            azure_sas_token = connection.password

            # Download and extract each release posted this month
            azure_container = None
            row_keys = []
            include_prefixes = []

            for release in releases:
                azure_container = release.release_container
                row_keys.append(release.row_key)
                include_prefixes.append(release.release_path)

            if len(row_keys) > 0:
                description = 'Transfer MAG Releases: ' + ', '.join(row_keys)
                logging.info(description)
                success = azure_to_google_cloud_storage_transfer(azure_account_name, azure_sas_token, azure_container,
                                                                 include_prefixes, gcp_project_id, gcp_bucket_name,
                                                                 description)
                releases_str = ', '.join(row_keys)
                if success:
                    logging.info(f'Success transferring MAG releases: {releases_str}')
                else:
                    logging.error(f"Error transferring MAG release: {releases_str}")
                    exit(os.EX_DATAERR)
            else:
                logging.warning('No release to transfer')

    @staticmethod
    def download(**kwargs):
        """ Downloads the MAG release from Google Cloud Storage.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        bucket_name = Variable.get(AirflowVars.DOWNLOAD_BUCKET)

        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        # Download each release to the extracted folder path (since they are already extracted)
        extracted_path = telescope_path(SubFolder.extracted, MagTelescope.DAG_ID)
        for release in releases:
            logging.info(f"Downloading release: {release}")
            destination_path = os.path.join(extracted_path, release.source_container)
            success = download_blobs_from_cloud_storage(bucket_name, release.release_path, destination_path,
                                                        max_processes=MagTelescope.MAX_PROCESSES,
                                                        max_connections=MagTelescope.MAX_CONNECTIONS,
                                                        retries=MagTelescope.RETRIES)

            if success:
                logging.info(f'Success downloading MAG release: {release}')
            else:
                logging.error(f"Error downloading MAG release: {release}")
                exit(os.EX_DATAERR)

    @staticmethod
    def transform(**kwargs):
        """ Transforms the MAG release into a form that can be uploaded to BigQuery.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        # For each release and folder to include, transform the files with sed and save into the transformed directory
        for release in releases:
            logging.info(f'Transforming MAG release: {release}')
            release_extracted_path = os.path.join(telescope_path(SubFolder.extracted, MagTelescope.DAG_ID),
                                                  release.source_container)
            release_transformed_path = os.path.join(telescope_path(SubFolder.transformed, MagTelescope.DAG_ID),
                                                    release.source_container)
            success = transform_mag_release(release_extracted_path, release_transformed_path,
                                            max_workers=MagTelescope.MAX_PROCESSES)

            if success:
                logging.info(f'Success transforming MAG release: {release}')
            else:
                logging.error(f"Error transforming MAG release: {release}")
                exit(os.EX_DATAERR)

    @staticmethod
    def upload_transformed(**kwargs):
        """ Uploads the transformed MAG release files to Google Cloud Storage for loading into BigQuery.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET)

        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        # Upload files to cloud storage
        for release in releases:
            logging.info(f'Uploading MAG release to cloud storage: {release}')
            release_transformed_path = os.path.join(telescope_path(SubFolder.transformed, MagTelescope.DAG_ID),
                                                    release.source_container)
            posix_paths = list_mag_release_files(release_transformed_path)
            paths = [str(path) for path in posix_paths]
            blob_names = [f'telescopes/{MagTelescope.DAG_ID}/{release.source_container}/{path.name}' for path in
                          posix_paths]
            success = upload_files_to_cloud_storage(bucket_name, blob_names, paths,
                                                    max_processes=MagTelescope.MAX_PROCESSES,
                                                    max_connections=MagTelescope.MAX_CONNECTIONS,
                                                    retries=MagTelescope.RETRIES)
            if success:
                logging.info(f'Success uploading MAG release to cloud storage: {release}')
            else:
                logging.error(f"Error uploading MAG release to cloud storage: {release}")
                exit(os.EX_DATAERR)

    @staticmethod
    def bq_load(**kwargs):
        """ Loads a MAG release into BigQuery.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        # Get config variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET)

        # For each release, load into BigQuery
        for release in releases:
            release_path = f'telescopes/{MagTelescope.DAG_ID}/{release.source_container}'
            success = db_load_mag_release(project_id, bucket_name, data_location, release_path, release.release_date)

            if success:
                logging.info(f'Success loading MAG release: {release}')
            else:
                logging.error(f"Error loading MAG release: {release}")
                exit(os.EX_DATAERR)

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed releases.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        for release in releases:
            # Remove all extracted files
            release_extracted_path = os.path.join(telescope_path(SubFolder.extracted, MagTelescope.DAG_ID),
                                                  release.source_container)
            try:
                shutil.rmtree(release_extracted_path)
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {release_extracted_path}: {e}")

            # Remove all transformed files
            release_transformed_path = os.path.join(telescope_path(SubFolder.transformed, MagTelescope.DAG_ID),
                                                    release.source_container)
            try:
                shutil.rmtree(release_transformed_path)
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {release_transformed_path}: {e}")


def db_load_mag_release(project_id: str, bucket_name: str, data_location: str, release_path: str,
                        release_date: Pendulum, dataset_id: str = MagTelescope.DAG_ID) -> bool:
    """ Load a MAG release into BigQuery.

    :param project_id: the Google Cloud project id.
    :param bucket_name: the Google Cloud bucket name where the transformed files are stored.
    :param data_location: the location where the BigQuery dataset will be created.
    :param release_path: the path on the Google Cloud storage bucket where the particular MAG release is located.
    :param release_date: the release date of the MAG release.
    :param dataset_id: the identifier of the dataset.
    :return: whether the MAG release was loaded into BigQuery successfully.
    """

    settings = {
        'Authors': {
            'quote': '',
            'allow_quoted_newlines': True
        },
        'FieldsOfStudy': {
            'quote': '',
            'allow_quoted_newlines': False
        },
        'PaperAuthorAffiliations': {
            'quote': '',
            'allow_quoted_newlines': False
        },
        'PaperCitationContexts': {
            'quote': '',
            'allow_quoted_newlines': True
        },
        'PaperExtendedAttributes': {
            'quote': '',
            'allow_quoted_newlines': False
        },
        'Papers': {
            'quote': '',
            'allow_quoted_newlines': True
        }
    }

    # Create dataset
    create_bigquery_dataset(project_id, dataset_id, data_location, MagTelescope.DESCRIPTION)

    # Get bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # List release blobs
    blobs: List[Blob] = list(bucket.list_blobs(prefix=release_path))
    max_workers = len(blobs)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create tasks
        futures = []
        futures_msgs = {}
        analysis_schema_path = schema_path()
        prefix = 'Mag'
        file_extension = '.txt'

        # De-duplicate blobs, i.e. for tables where there are more than one file:
        # e.g. PaperAbstractsInvertedIndex.txt.1 and PaperAbstractsInvertedIndex.txt.2 become
        # PaperAbstractsInvertedIndex.txt.* so that both are loaded into the same table.
        blob_names = set()
        for blob in blobs:
            blob_name = blob.name
            if not blob_name.endswith(file_extension):
                blob_name_sans_index = re.match(r'^.+?(?=([0-9]+)?$)', blob_name).group(0)
                blob_name_with_wildcard = f'{blob_name_sans_index}*'
                blob_names.add(blob_name_with_wildcard)
            else:
                blob_names.add(blob_name)

        for blob_name in blob_names:
            # Make table name and id
            table_name = table_name_from_blob(blob_name, file_extension)
            table_id = bigquery_partitioned_table_id(table_name, release_date)

            # Get schema for table
            schema_file_path = find_schema(analysis_schema_path, table_name, release_date, prefix=prefix)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={table_name}, release_date={release_date}, prefix={prefix}')
                exit(os.EX_CONFIG)

            uri = f'gs://{bucket_name}/{blob_name}'
            msg = f'uri={uri}, table_id={table_id}, schema_file_path={schema_file_path}'
            logging.info(f'db_load_mag_release: {msg}')

            if table_name in settings:
                csv_quote_character = settings[table_name]['quote']
                csv_allow_quoted_newlines = settings[table_name]['allow_quoted_newlines']
            else:
                csv_quote_character = '"'
                csv_allow_quoted_newlines = False

            future = executor.submit(load_bigquery_table, uri, dataset_id, data_location, table_id, schema_file_path,
                                     SourceFormat.CSV, csv_field_delimiter='\t',
                                     csv_quote_character=csv_quote_character,
                                     csv_allow_quoted_newlines=csv_allow_quoted_newlines)
            futures_msgs[future] = msg
            futures.append(future)

        # Wait for completed tasks
        results = []
        for future in as_completed(futures):
            success = future.result()
            msg = futures_msgs[future]
            results.append(success)
            if success:
                logging.info(f'db_load_mag_release success: {msg}')
            else:
                logging.error(f'db_load_mag_release failed: {msg}')

    return all(results)
