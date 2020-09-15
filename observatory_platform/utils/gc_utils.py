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

import codecs
import json
import logging
import multiprocessing
import os
import re
import time
from concurrent.futures import as_completed, ProcessPoolExecutor
from enum import Enum
from multiprocessing import BoundedSemaphore, cpu_count
from typing import List, Union

import pendulum
from crc32c import Checksum as Crc32cChecksum
from google.api_core.exceptions import Conflict
from google.cloud import storage, bigquery
from google.cloud.bigquery import SourceFormat, LoadJobConfig, LoadJob, QueryJob
from google.cloud.exceptions import NotFound
from google.cloud.storage import Blob
from googleapiclient import discovery as gcp_api
from pendulum import Pendulum
from requests.exceptions import ChunkedEncodingError

# The chunk size to use when uploading / downloading a blob in multiple parts, must be a multiple of 256 KB.
DEFAULT_CHUNK_SIZE = 256 * 1024 * 4


def hex_to_base64_str(hex_str: bytes) -> str:
    """ Covert a hexadecimal string into a base64 encoded string. Removes trailing newline character.

    :param hex_str: the hexadecimal encoded string.
    :return: the base64 encoded string.
    """

    string = codecs.decode(hex_str, 'hex')
    base64 = codecs.encode(string, 'base64')
    return base64.decode('utf8').rstrip('\n')


def crc32c_base64_hash(file_path: str, chunk_size: int = 8 * 1024) -> str:
    """ Create a base64 crc32c checksum of a file.

    :param file_path: the path to the file.
    :param chunk_size: the size of each chunk to check.
    :return: the checksum.
    """

    hash_alg = Crc32cChecksum()

    with open(file_path, 'rb') as f:
        chunk = f.read(chunk_size)
        while chunk:
            hash_alg.update(chunk)
            chunk = f.read(chunk_size)
    return hex_to_base64_str(hash_alg.hexdigest())


def table_name_from_blob(blob_name: str, file_extension: str):
    """ Make a BigQuery table name from a blob name.

    :param blob_name: the blob name.
    :param file_extension: the file extension of the blob.
    :return: the table name.
    """

    assert '.' in file_extension, 'file_extension must contain a .'
    file_name = os.path.basename(blob_name)
    match = re.match(fr'.+?(?={file_extension})', file_name)
    if match is None:
        raise ValueError(f'Could not find table name from blob_name={blob_name}')
    return match.group(0)


class TransferStatus(Enum):
    """ The status of the Google Cloud Data Transfer operation """

    in_progress = 'IN_PROGRESS'
    success = 'SUCCESS'
    aborted = 'ABORTED'
    failed = 'FAILED'


def bigquery_table_exists(project_id: str, dataset_id: str, table_name: str) -> bool:
    """ Checks whether a BigQuery table exists or not.

    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :param table_name: the name of the table.
    :return: whether the table exists or not.
    """

    client = bigquery.Client(project_id)
    dataset = bigquery.Dataset(f'{project_id}.{dataset_id}')
    table = dataset.table(table_name)
    table_exists = True

    try:
        client.get_table(table)
    except NotFound:
        table_exists = False

    return table_exists


def bigquery_partitioned_table_id(table_name, datetime: Pendulum) -> str:
    """ Create a partitioned table identifier for a BigQuery table.

    :param table_name: the name of the table.
    :param datetime: the date to append as a partition suffix.
    :return: the table id.
    """
    return f"{table_name}{datetime.strftime('%Y%m%d')}"


def create_bigquery_dataset(project_id: str, dataset_id: str, location: str, description: str = '') -> None:
    """ Create a BigQuery dataset.

    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :param location: the location where the dataset will be stored:
    https://cloud.google.com/compute/docs/regions-zones/#locations
    :param description: a description for the dataset
    :return: None
    """

    func_name = create_bigquery_dataset.__name__

    # Make the dataset reference
    dataset_ref = f'{project_id}.{dataset_id}'

    # Make dataset handle
    client = bigquery.Client()
    dataset = bigquery.Dataset(dataset_ref)

    # Set properties
    dataset.location = location
    dataset.description = description

    # Create dataset, if already exists then catch exception
    try:
        logging.info(f"{func_name}: creating dataset dataset_ref={dataset_ref}")
        dataset = client.create_dataset(dataset)
    except Conflict as e:
        logging.warning(f"{func_name}: dataset already exists dataset_ref={dataset_ref}, exception={e}")


def load_bigquery_table(uri: str, dataset_id: str, location: str, table: str, schema_file_path: str,
                        source_format: str, csv_field_delimiter: str = ',', csv_quote_character: str = '"',
                        csv_allow_quoted_newlines: bool = False, csv_skip_leading_rows: int = 0,
                        partition: bool = False, partition_field: Union[None, str] = None,
                        partition_type: str = bigquery.TimePartitioningType.DAY, require_partition_filter=True,
                        write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE) -> bool:
    """ Load a BigQuery table from an object on Google Cloud Storage.

    :param uri: the uri of the object to load from Google Cloud Storage into BigQuery.
    :param dataset_id: BigQuery dataset id.
    :param location: location of the BigQuery dataset.
    :param table: BigQuery table name.
    :param schema_file_path: path on local file system to BigQuery table schema.
    :param source_format: the format of the data to load into BigQuery.
    :param csv_field_delimiter: the field delimiter character for data in CSV format.
    :param csv_quote_character: the quote character for data in CSV format.
    :param csv_allow_quoted_newlines: whether to allow quoted newlines for data in CSV format.
    :param csv_skip_leading_rows: the number of leading rows to skip for data in CSV format.
    :param partition: whether to partition the table.
    :param partition_field: the name of the partition field.
    :param partition_type: the type of partitioning.
    :param require_partition_filter: whether the partition filter is required or not when querying the table.
    :param write_disposition: whether to append, overwrite or throw an error when data already exists in the table.
    Default is to overwrite.
    :return:
    """

    assert uri.startswith("gs://"), "load_big_query_table: 'uri' must begin with 'gs://'"

    func_name = load_bigquery_table.__name__
    msg = f'uri={uri}, dataset_id={dataset_id}, location={location}, table={table}, ' \
          f'schema_file_path={schema_file_path}, source_format={source_format}'
    logging.info(f"{func_name}: load bigquery table {msg}")

    client = bigquery.Client()
    dataset = client.dataset(dataset_id)

    # Create load job
    job_config = LoadJobConfig()

    # Set global options
    job_config.source_format = source_format
    job_config.schema = client.schema_from_json(schema_file_path)
    job_config.write_disposition = write_disposition

    # Set CSV options
    if source_format == SourceFormat.CSV:
        job_config.field_delimiter = csv_field_delimiter
        job_config.quote_character = csv_quote_character
        job_config.allow_quoted_newlines = csv_allow_quoted_newlines
        job_config.skip_leading_rows = csv_skip_leading_rows

    # Set partitioning settings
    if partition:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type,
            field=partition_field,
            require_partition_filter=require_partition_filter
        )

    load_job: LoadJob = client.load_table_from_uri(
        uri,
        dataset.table(table),
        location=location,
        job_config=job_config
    )
    result = load_job.result()
    logging.info(f"{func_name}: load bigquery table result.state={result.state}, {msg}")
    return result.state == 'DONE'


def run_bigquery_query(query: str) -> List:
    """ Run a BigQuery query.

    :param query: the query to run.
    :return: the results.
    """

    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()
    return list(rows)


def copy_bigquery_table(source_table_id: str, destination_table_id: str, data_location: str) -> bool:
    """ Copy a BigQuery table.

    :param source_table_id: the id of the source table, including the project name and dataset id.
    :param destination_table_id: the id of the destination table, including the project name and dataset id.
    :param data_location: the location of the datasets.
    :return: whether the table was copied successfully or not.
    """

    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job = client.copy_table(source_table_id, destination_table_id, location=data_location, job_config=job_config)
    result = job.result()
    return result.done()


def create_bigquery_view(project_id: str, dataset_id: str, view_name: str, query: str) -> None:
    """ Create a BigQuery view.

    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :param view_name: the name to call the view.
    :param query: the query for the view.
    :return: None
    """

    client = bigquery.Client()
    dataset = bigquery.DatasetReference(project_id, dataset_id)
    view_ref = dataset.table(view_name)
    view = bigquery.Table(view_ref)
    view.view_query = query
    view = client.create_table(view, exists_ok=True)


def create_bigquery_table_from_query(sql: str, project_id: str, dataset_id: str, table_id: str, location: str,
                                     description: str = '', labels=None,
                                     query_parameters=None,
                                     partition: bool = False, partition_field: Union[None, str] = None,
                                     partition_type: str = bigquery.TimePartitioningType.DAY,
                                     require_partition_filter=True, cluster: bool = False,
                                     clustering_fields=None) -> bool:
    """ Create a BigQuery dataset from a provided query.

    :param sql: the sql query to be executed
    :param labels: labels to place on the new table
    :param project_id: the Google Cloud project id
    :param dataset_id: the BigQuery dataset id
    :param table_id: the BigQuery table id
    :param location: the location where the dataset will be stored:
    https://cloud.google.com/compute/docs/regions-zones/#locations
    :param query_parameters: parameters for a parametrised query.
    :param description: a description for the dataset
    :param partition: whether to partition the table.
    :param partition_field: the name of the partition field.
    :param partition_type: the type of partitioning.
    :param require_partition_filter: whether the partition filter is required or not when querying the table.
    :param cluster: whether to cluster the table or not.
    :param clustering_fields: what fields to cluster on.
    :return:
    """

    # Handle mutable default arguments
    if labels is None:
        labels = {}
    if query_parameters is None:
        query_parameters = []
    if clustering_fields is None:
        clustering_fields = []

    func_name = create_bigquery_dataset.__name__
    msg = f'project_id={project_id}, dataset_id={dataset_id}, location={location}, table={table_id}'
    logging.info(f"{func_name}: create bigquery table from query, {msg}")

    # Make the dataset reference
    dataset_ref = f'{project_id}.{dataset_id}'

    # Make dataset handle
    client = bigquery.Client()
    dataset = bigquery.Dataset(dataset_ref)

    # Set properties
    dataset.location = location
    dataset.description = description

    job_config = bigquery.QueryJobConfig(
        allow_large_results=True,
        destination=dataset.table(table_id),
        description=description,
        labels=labels,
        use_legacy_sql=False,
        query_parameters=query_parameters
    )

    # Set partitioning settings
    if partition:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type,
            field=partition_field,
            require_partition_filter=require_partition_filter
        )

    if cluster:
        job_config.clustering_fields = clustering_fields

    query_job: QueryJob = client.query(sql, job_config=job_config)
    query_job.result()
    success = query_job.done()
    logging.info(f"{func_name}: create bigquery table from query {msg}: {success}")
    return success


def download_blob_from_cloud_storage(bucket_name: str, blob_name: str, file_path: str, retries: int = 3,
                                     connection_sem: BoundedSemaphore = None,
                                     chunk_size: int = DEFAULT_CHUNK_SIZE) -> bool:
    """ Download a blob to a file.

    :param bucket_name: the name of the Google Cloud storage bucket.
    :param blob_name: the path to the blob.
    :param file_path: the file path where the blob should be saved.
    :param retries: the number of times to retry downloading the blob.
    :param connection_sem: a BoundedSemaphore to limit the number of download connections that can run at once.
    :param chunk_size: the chunk size to use when downloading a blob in multiple parts, must be a multiple of 256 KB.
    :return: whether the download was successful or not.
    """

    func_name = download_blob_from_cloud_storage.__name__
    logging.info(f"{func_name}: {file_path}")

    # Get blob
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob: Blob = bucket.blob(blob_name)

    # State
    download = True
    success = False

    # Get hash for blob, have to call reload
    blob.reload()
    expected_hash = blob.crc32c

    # Check if file exists and check hash
    if os.path.exists(file_path):
        # Check file's hash
        logging.info(f'{func_name}: file exists, checking hash {file_path}')
        actual_hash = crc32c_base64_hash(file_path)

        # Compare hashes
        files_match = expected_hash == actual_hash
        logging.info(f'{func_name}: files_match={files_match}, expected_hash={expected_hash}, '
                     f'actual_hash={actual_hash}')
        if files_match:
            logging.info(f'{func_name}: skipping download as files match bucket_name={bucket_name}, '
                         f'blob_name={blob_name}, file_path={file_path}')
            download = False
            success = True

    if download:
        # Get connection semaphore
        if connection_sem is not None:
            connection_sem.acquire()

        for i in range(0, retries):
            try:
                blob.chunk_size = chunk_size
                blob.download_to_filename(file_path)
                success = True
                break
            except ChunkedEncodingError as e:
                logging.error(f'{func_name}: exception downloading file: try={i}, file_path={file_path}, exception={e}')

        # Release connection semaphore
        if connection_sem is not None:
            connection_sem.release()

    return success


def download_blobs_from_cloud_storage(bucket_name: str, prefix: str, destination_path: str,
                                      max_processes: int = cpu_count(), max_connections: int = cpu_count(),
                                      retries: int = 3, chunk_size: int = DEFAULT_CHUNK_SIZE) -> bool:
    """ Download all blobs on a Google Cloud Storage bucket that are within a prefixed path, to a destination on the
    local file system.

    :param bucket_name: the name of the Google Cloud storage bucket.
    :param prefix: the prefixed path on the bucket, where blobs will be searched for.
    :param destination_path: the destination on the local file system to download files too.
    :param max_processes: the maximum number of processes.
    :param max_connections: the maximum number of download connections at once.
    :param retries: the number of times to retry downloading the blob.
    :param chunk_size: the chunk size to use when downloading a blob in multiple parts, must be a multiple of 256 KB.
    :return: whether the files were downloaded successfully or not.
    """

    func_name = download_blobs_from_cloud_storage.__name__

    # Get bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # List blobs
    blobs: List[Blob] = list(bucket.list_blobs(prefix=prefix))
    logging.info(f"{func_name}: {blobs}")

    # Download each blob in parallel
    manager = multiprocessing.Manager()
    connection_sem = manager.BoundedSemaphore(value=max_connections)
    with ProcessPoolExecutor(max_workers=max_processes) as executor:
        # Create tasks
        futures = []
        futures_msgs = {}
        for blob in blobs:
            # Save files to destination path, remove blobs_path from blob name
            filename = f'{os.path.normpath(destination_path)}{blob.name.replace(prefix, "")}'
            msg = f'bucket_name={bucket_name}, blob_name={blob.name}, filename={filename}'
            logging.info(f'{func_name}: {msg}')

            # Create directory
            dirname = os.path.dirname(filename)
            os.makedirs(dirname, exist_ok=True)

            future = executor.submit(download_blob_from_cloud_storage, bucket_name, blob.name, filename,
                                     retries=retries, connection_sem=connection_sem, chunk_size=chunk_size)
            futures.append(future)
            futures_msgs[future] = msg

        # Wait for completed tasks
        results = []
        for future in as_completed(futures):
            success = future.result()
            results.append(success)
            msg = futures_msgs[future]

            if success:
                logging.info(f'{func_name}: success, {msg}')
            else:
                logging.info(f'{func_name}: failed, {msg}')

    return all(results)


def upload_files_to_cloud_storage(bucket_name: str, blob_names: List[str], file_paths: List[str],
                                  max_processes: int = cpu_count(), max_connections: int = cpu_count(),
                                  retries: int = 3, chunk_size: int = DEFAULT_CHUNK_SIZE) -> bool:
    """ Upload a list of files to Google Cloud storage.

    :param bucket_name: the name of the Google Cloud storage bucket.
    :param blob_names: the destination paths of blobs where the files will be uploaded.
    :param file_paths: the paths of the files to upload as blobs.
    :param max_processes: the maximum number of processes.
    :param max_connections: the maximum number of upload connections at once.
    :param retries: the number of times to retry uploading a file if an error occurs.
    :param chunk_size: the chunk size to use when uploading a blob in multiple parts, must be a multiple of 256 KB.
    :return: whether the files were uploaded successfully or not.
    """

    func_name = upload_files_to_cloud_storage.__name__
    logging.info(f'{func_name}: uploading files')

    # Upload each file in parallel
    manager = multiprocessing.Manager()
    connection_sem = manager.BoundedSemaphore(value=max_connections)
    with ProcessPoolExecutor(max_workers=max_processes) as executor:
        # Create tasks
        futures = []
        futures_msgs = {}
        for blob_name, file_path in zip(blob_names, file_paths):
            msg = f'{func_name}: bucket_name={bucket_name}, blob_name={blob_name}, file_path={str(file_path)}'
            logging.info(f"{func_name}: {msg}")
            future = executor.submit(upload_file_to_cloud_storage, bucket_name, blob_name, file_path=str(file_path),
                                     retries=retries, connection_sem=connection_sem, chunk_size=chunk_size)
            futures.append(future)
            futures_msgs[future] = msg

        # Wait for completed tasks
        results = []
        for future in as_completed(futures):
            success = future.result()
            results.append(success)
            msg = futures_msgs[future]
            if success:
                logging.info(f'{func_name}: success, {msg}')
            else:
                logging.info(f'{func_name}: failed, {msg}')

    return all(results)


def upload_file_to_cloud_storage(bucket_name: str, blob_name: str, file_path: str, retries: int = 3,
                                 connection_sem: BoundedSemaphore = None, chunk_size: int = DEFAULT_CHUNK_SIZE) -> bool:
    """ Upload a file to Google Cloud Storage.

    :param bucket_name: the name of the Google Cloud Storage bucket.
    :param blob_name: the name of the blob to save.
    :param file_path: the path of the file to upload.
    :param retries: the number of times to retry uploading a file if an error occurs.
    :param connection_sem: a BoundedSemaphore to limit the number of upload connections that can run at once.
    :param chunk_size: the chunk size to use when uploading a blob in multiple parts, must be a multiple of 256 KB.
    :return: whether the upload was successful or not.
    """
    func_name = upload_file_to_cloud_storage.__name__
    logging.info(f"{func_name}: bucket_name={bucket_name}, blob_name={blob_name}")

    # State
    upload = True
    success = False

    # Get blob
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Check if blob exists already and matches the file we are uploading
    if blob.exists():
        # Get blob hash
        blob.reload()
        expected_hash = blob.crc32c

        # Check file hash
        actual_hash = crc32c_base64_hash(file_path)

        # Compare hashes
        files_match = expected_hash == actual_hash
        logging.info(f'{func_name}: files_match={files_match}, expected_hash={expected_hash}, '
                     f'actual_hash={actual_hash}')
        if files_match:
            logging.info(
                f'{func_name}: skipping upload as files match bucket_name={bucket_name}, blob_name={blob_name}, '
                f'file_path={file_path}')
            upload = False
            success = True

    # Upload if file doesn't exist or exists and doesn't match
    if upload:
        # Get connection semaphore
        if connection_sem is not None:
            connection_sem.acquire()

        for i in range(0, retries):
            try:
                blob.chunk_size = chunk_size
                blob.upload_from_filename(file_path)
                success = True
                break
            except ChunkedEncodingError as e:
                logging.error(f'{func_name}: exception uploading file: try={i}, exception={e}')

        # Release connection semaphore
        if connection_sem is not None:
            connection_sem.release()

    return success


def azure_to_google_cloud_storage_transfer(azure_storage_account_name: str, azure_sas_token: str, azure_container: str,
                                           include_prefixes: List[str], gc_project_id: str, gc_bucket: str,
                                           description: str, start_date: Pendulum = pendulum.utcnow()) \
        -> bool:
    """ Transfer files from an Azure blob container to a Google Cloud Storage bucket.

    :param azure_storage_account_name: the name of the Azure Storage account that holds the Azure blob container.
    :param azure_sas_token: the shared access signature (SAS) for the Azure blob container.
    :param azure_container: the name of the Azure Blob container where files will be copied from.
    :param include_prefixes: the prefixes of blobs to download from the Azure blob container.
    :param gc_project_id: the Google Cloud project id that holds the Google Cloud Storage bucket.
    :param gc_bucket: the Google Cloud bucket name.
    :param description: a description for the transfer job.
    :param start_date: the date that the transfer job will start.
    :return: whether the transfer was a success or not.
    """

    # Leave out scheduleStartTime to make sure that the job starts immediately
    # Make sure to specify scheduleEndDate as today otherwise the job will repeat
    func_name = azure_to_google_cloud_storage_transfer.__name__

    job = {
        'description': description,
        'status': 'ENABLED',
        'projectId': gc_project_id,
        'schedule': {
            'scheduleStartDate': {
                'day': start_date.day,
                'month': start_date.month,
                'year': start_date.year
            },
            'scheduleEndDate': {
                'day': start_date.day,
                'month': start_date.month,
                'year': start_date.year
            },
        },
        'transferSpec': {
            'azureBlobStorageDataSource': {
                'storageAccount': azure_storage_account_name,
                'azureCredentials': {
                    'sasToken': azure_sas_token,
                },
                'container': azure_container,

            },
            'objectConditions': {
                'includePrefixes': include_prefixes
            },
            'gcsDataSink': {
                'bucketName': gc_bucket
            }
        }
    }

    client = gcp_api.build('storagetransfer', 'v1')
    create_result = client.transferJobs().create(body=job).execute()
    transfer_job_name = create_result['name']

    transfer_job_filter = json.dumps({
        'project_id': gc_project_id,
        'job_names': [transfer_job_name]
    })
    wait_time = 60
    while True:
        response = client.transferOperations().list(name='transferOperations', filter=transfer_job_filter).execute()
        if 'operations' in response:
            operations = response['operations']

            in_progress_count, success_count, failed_count, aborted_count = 0, 0, 0, 0
            for op in operations:
                status = op['metadata']['status']
                if status == TransferStatus.success.value:
                    success_count += 1
                elif status == TransferStatus.failed.value:
                    failed_count += 1
                elif status == TransferStatus.aborted.value:
                    aborted_count += 1
                elif status == TransferStatus.in_progress.value:
                    in_progress_count += 1

            num_operations = len(operations)
            logging.info(f"{func_name}: transfer job {transfer_job_name} operations success={success_count}, "
                         f"failed={failed_count}, aborted={aborted_count}, in_progress_count={in_progress_count}")

            if success_count >= num_operations:
                status = TransferStatus.success
                break
            elif failed_count >= 1:
                status = TransferStatus.failed
                break
            elif aborted_count >= 1:
                status = TransferStatus.aborted
                break

        time.sleep(wait_time)

    return status == TransferStatus.success


def upload_telescope_file_list(bucket_name: str, dag_id: str, execution_date: str, file_list: List[str]) -> List[str]:
    """ Upload list of files to cloud storage.

    :param bucket_name: Name of storage bucket.
    :param dag_id: DAG ID.
    :param execution_date: Execution date.
    :param file_list: List of files to upload.
    :return: List of location paths in the cloud.
    """

    blob_list = list()
    for file in file_list:
        file_name = os.path.basename(file)
        blob_name = f'telescopes/{dag_id}/{execution_date}/{file_name}'
        blob_list.append(blob_name)
        upload_file_to_cloud_storage(bucket_name, blob_name, file_path=file)
    return blob_list
