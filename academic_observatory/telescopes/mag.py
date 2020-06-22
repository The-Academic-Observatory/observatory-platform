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

import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from pathlib import Path, PosixPath
from subprocess import Popen
from typing import List

from airflow.hooks.base_hook import BaseHook
from airflow.models.taskinstance import TaskInstance
from google.cloud import storage
from google.cloud.bigquery import SourceFormat
from google.cloud.storage import Blob

from academic_observatory.utils.config_utils import ObservatoryConfig
from academic_observatory.utils.config_utils import telescope_path, SubFolder, schema_path, find_schema
from academic_observatory.utils.gc_utils import azure_to_google_cloud_storage_transfer, TransferStatus, \
    download_blobs_from_cloud_storage, upload_files_to_cloud_storage, load_bigquery_table, create_bigquery_dataset, \
    bigquery_partitioned_table_id
from academic_observatory.utils.proc_utils import wait_for_process
from mag_archiver.mag import MagArchiverClient, MagDateType, MagRelease, MagState


def pull_mag_releases(ti: TaskInstance) -> List[MagRelease]:
    return ti.xcom_pull(key=MagTelescope.TOPIC_NAME, task_ids=MagTelescope.TASK_ID_LIST, include_prior_dates=False)


def list_mag_release_files(release_path) -> List[PosixPath]:
    exclude_path = os.path.join(release_path, 'samples')
    paths = list(Path(release_path).rglob("*.txt"))
    paths = [path for path in paths if not str(path).startswith(exclude_path)]
    return paths


def transform_mag_file(input_file_path: str, output_file_path: str) -> bool:
    # Transform files
    # Remove \x0 and \r characters \r is the ^M windows character
    # TODO: see if we can get rid of shell=True

    bash_command = fr"sed 's/\r//g; s/\x0//g' {str(input_file_path)} > {output_file_path}"
    logging.info(f"transform_mag_file bash command: {bash_command}")
    proc: Popen = subprocess.Popen(bash_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = wait_for_process(proc)
    logging.debug(output)
    success = proc.returncode == 0

    if not success:
        logging.error(f"transform_mag_file error transforming file: {input_file_path}")
        logging.error(error)

    return success


def transform_mag_release(release: MagRelease, max_workers: int = cpu_count()) -> bool:
    # Transform each file in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create tasks
        futures = []
        futures_msgs = {}

        release_extracted_path = os.path.join(telescope_path(MagTelescope.DAG_ID, SubFolder.extracted),
                                              release.source_container)
        release_transformed_path = os.path.join(telescope_path(MagTelescope.DAG_ID, SubFolder.transformed),
                                                release.source_container)

        paths = list_mag_release_files(release_extracted_path)
        for path in paths:
            # Make path to save file
            os.makedirs(release_transformed_path, exist_ok=True)
            output_path = os.path.join(release_transformed_path, path.name)
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


def db_load_mag_release(release: MagRelease) -> bool:
    settings = {
        'Authors': {'quote': '', 'allow_quoted_newlines': True},
        'FieldsOfStudy': {'quote': '', 'allow_quoted_newlines': False},
        'PaperAuthorAffiliations': {'quote': '', 'allow_quoted_newlines': False},
        'PaperCitationContexts': {'quote': '', 'allow_quoted_newlines': True},
        'Papers': {'quote': '', 'allow_quoted_newlines': True}
    }

    # Get Observatory Config
    config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
    project_id = config.project_id
    location = config.data_location
    bucket_name = config.bucket_name

    # Create dataset
    dataset_id = MagTelescope.DAG_ID
    create_bigquery_dataset(project_id, dataset_id, location, MagTelescope.DESCRIPTION)

    # Get bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # List release blobs
    blobs_path = f'telescopes/{MagTelescope.DAG_ID}/transformed/{release.source_container}'
    blobs: List[Blob] = list(bucket.list_blobs(prefix=blobs_path))
    max_workers = len(blobs)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create tasks
        futures = []
        futures_msgs = {}
        analysis_schema_path = schema_path('telescopes')
        prefix = 'Mag'

        for blob in blobs:
            # Make table name and id
            table_name = blob.name.split('/')[-1].replace('.txt', '')
            table_id = bigquery_partitioned_table_id(table_name, release.release_date)

            # Get schema for table
            schema_file_path = find_schema(analysis_schema_path, table_name, release.release_date, prefix=prefix)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={table_name}, release_date={release.release_date}, prefix={prefix}')
                exit(os.EX_CONFIG)

            uri = f'gs://{blob.bucket.name}/{blob.name}'
            msg = f'uri={uri}, table_id={table_id}, schema_file_path={schema_file_path}'
            logging.info(f'db_load_mag_release: {msg}')

            if table_name in settings:
                csv_quote_character = settings[table_name]['quote']
                csv_allow_quoted_newlines = settings[table_name]['allow_quoted_newlines']
            else:
                csv_quote_character = '"'
                csv_allow_quoted_newlines = False

            future = executor.submit(load_bigquery_table, uri, dataset_id, location, table_id, schema_file_path,
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


class MagTelescope:
    """ A container for holding the constants and static functions for the Microsoft Academic Graph (MAG) telescope. """

    DAG_ID = 'mag'
    DESCRIPTION = 'The Microsoft Academic Graph (MAG) dataset: https://www.microsoft.com/en-us/research/project/' \
                  'microsoft-academic-graph/'
    TASK_ID_LIST = f'{DAG_ID}_list_releases'
    TASK_ID_TRANSFER = f'{DAG_ID}_transfer'
    TASK_ID_DOWNLOAD = f'{DAG_ID}_download'
    TASK_ID_TRANSFORM = f'{DAG_ID}_transform'
    TASK_ID_UPLOAD = f'{DAG_ID}_upload'
    TASK_ID_DB_LOAD = f'{DAG_ID}_db_load'
    TASK_ID_STOP = f'{DAG_ID}_stop'
    TOPIC_NAME = 'message'
    MAX_PROCESSES = cpu_count()
    MAX_CONNECTIONS = cpu_count()
    MAX_RETRIES = 3

    @staticmethod
    def list_releases(**kwargs):
        """ Task to list all MAG releases for a given month """

        connection = BaseHook.get_connection("mag_releases_table")
        account_name = connection.login
        sas_token = connection.password
        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']

        client = MagArchiverClient(account_name=account_name, sas_token=sas_token)
        releases: List[MagRelease] = client.list_releases(start_date=execution_date, end_date=next_execution_date,
                                                          state=MagState.done, date_type=MagDateType.done)
        logging.info('list_releases:')
        for release in releases:
            logging.info(f'  - {release}')

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(MagTelescope.TOPIC_NAME, releases, execution_date)

        return MagTelescope.TASK_ID_TRANSFER if releases else MagTelescope.TASK_ID_STOP

    @staticmethod
    def transfer(**kwargs):
        # Get Azure connection information
        connection = BaseHook.get_connection("mag_snapshots_container")
        azure_account_name = connection.login
        azure_sas_token = connection.password

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        gcp_project_id = config.project_id
        gcp_bucket_name = config.bucket_name

        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_mag_releases(ti)

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
            transfer_job_name, status = azure_to_google_cloud_storage_transfer(azure_account_name, azure_sas_token,
                                                                               azure_container, include_prefixes,
                                                                               gcp_project_id, gcp_bucket_name,
                                                                               description)
            success = status == TransferStatus.success
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
        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        bucket_name = config.bucket_name

        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_mag_releases(ti)

        # Download each release to the extracted folder path (since they are already extracted)
        extracted_path = telescope_path(MagTelescope.DAG_ID, SubFolder.extracted)
        for release in releases:
            logging.info(f"Downloading release: {release}")
            destination_path = os.path.join(extracted_path, release.source_container)
            success = download_blobs_from_cloud_storage(bucket_name, release.release_path, destination_path,
                                                        max_processes=MagTelescope.MAX_PROCESSES,
                                                        max_connections=MagTelescope.MAX_CONNECTIONS,
                                                        retries=MagTelescope.MAX_RETRIES)

            if success:
                logging.info(f'Success downloading MAG release: {release}')
            else:
                logging.error(f"Error downloading MAG release: {release}")
                exit(os.EX_DATAERR)

    @staticmethod
    def transform(**kwargs):
        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_mag_releases(ti)

        # For each release and folder to include, transform the files with sed and save into the transformed directory
        for release in releases:
            logging.info(f'Transforming MAG release: {release}')
            success = transform_mag_release(release)

            if success:
                logging.info(f'Success transforming MAG release: {release}')
            else:
                logging.error(f"Error transforming MAG release: {release}")
                exit(os.EX_DATAERR)

    @staticmethod
    def upload(**kwargs):
        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        bucket_name = config.bucket_name

        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_mag_releases(ti)

        # Upload files to cloud storage
        for release in releases:
            logging.info(f'Uploading MAG release to cloud storage: {release}')
            release_transformed_path = os.path.join(telescope_path(MagTelescope.DAG_ID, SubFolder.transformed),
                                                    release.source_container)
            posix_paths = list_mag_release_files(release_transformed_path)
            paths = [str(path) for path in posix_paths]
            blob_names = [f'telescopes/{MagTelescope.DAG_ID}/transformed/{release.source_container}/{path.name}' for
                          path in posix_paths]
            success = upload_files_to_cloud_storage(bucket_name, blob_names, paths,
                                                    max_processes=MagTelescope.MAX_PROCESSES,
                                                    max_connections=MagTelescope.MAX_CONNECTIONS,
                                                    retries=MagTelescope.MAX_RETRIES)
            if success:
                logging.info(f'Success uploading MAG release to cloud storage: {release}')
            else:
                logging.error(f"Error uploading MAG release to cloud storage: {release}")
                exit(os.EX_DATAERR)

    @staticmethod
    def db_load(**kwargs):
        # Get MAG releases
        ti: TaskInstance = kwargs['ti']
        releases = pull_mag_releases(ti)

        # For each release, load into
        for release in releases:
            success = db_load_mag_release(release)

            if success:
                logging.info(f'Success loading MAG release: {release}')
            else:
                logging.error(f"Error loading MAG release: {release}")
                exit(os.EX_DATAERR)
