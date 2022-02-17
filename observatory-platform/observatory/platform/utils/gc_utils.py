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

# Author: James Diprose, Aniek Roelofs

""" General google cloud utility functions (independent of telescope usage) """

import json
import logging
import multiprocessing
import os
import re
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from enum import Enum
from multiprocessing import BoundedSemaphore, cpu_count
from typing import List, Optional, Tuple, Union, Any

import pendulum
from airflow.models import Variable
from google.api_core.exceptions import BadRequest, Conflict
from google.cloud import bigquery, storage
from google.cloud.bigquery import LoadJob, LoadJobConfig, QueryJob, SourceFormat
from google.cloud.bigquery.job import QueryJobConfig
from google.cloud.exceptions import Conflict, NotFound
from google.cloud.storage import Blob
from googleapiclient import discovery as gcp_api
from observatory.api.client.model.big_query_bytes_processed import (
    BigQueryBytesProcessed,
)
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.api import make_observatory_api
from observatory.platform.utils.config_utils import utils_templates_path
from observatory.platform.utils.file_utils import crc32c_base64_hash
from observatory.platform.utils.jinja2_utils import (
    make_sql_jinja2_filename,
    render_template,
)
from requests.exceptions import ChunkedEncodingError

# The chunk size to use when uploading / downloading a blob in multiple parts, must be a multiple of 256 KB.
DEFAULT_CHUNK_SIZE = 256 * 1024 * 4

# BigQuery daily query byte limit.
BIGQUERY_QUERY_DAILY_BYTE_LIMIT = 1024 * 1024 * 1024 * 1024 * 5  # 5 TiB


# This function makes it easier to mock out in tests. This mechanism is here to allow you to hard code disabling the
# daily quota check across the platform.  When you are convinced the feature is working correctly in production and
# that you wan't it always on, then the mechanism can be removed.
def bq_query_daily_limit_enabled():
    """Whether to turn on BigQuery daily limits.

    :return: Whether to use daily limits.
    """

    return True


def bq_query_bytes_estimate(*args, **kwargs) -> int:
    """Do a dry run of a BigQuery query to estimate the bytes processed.

    :param args: Positional arguments to pass onto the callable.
    :param kwargs: Named arguments to pass onto the callable.
    :return: Query bytes estimate.
    """

    if "job_config" not in kwargs:
        kwargs["job_config"] = QueryJobConfig()

    config = deepcopy(kwargs["job_config"])
    config.dry_run = True
    kwargs["job_config"] = config

    bytes_estimate = bigquery.Client().query(*args, **kwargs).total_bytes_processed
    return bytes_estimate


def bq_query_bytes_budget_check(*, bytes_budget: Optional[int], bytes_estimate: int):
    """Check that the estimated number of processed bytes required does not exceed the budgeted number of bytes for the
    query. If the estimate exceeds the budget, this function throws an exception.

    :param bytes_budget: The processed bytes budget for this query.
    :param bytes_estimate: Estimated number of bytes processed in query.
    """

    if bytes_budget is None:
        return

    if bytes_estimate > bytes_budget:
        raise Exception(f"Bytes estimate {bytes_estimate} exceeds the budget {bytes_budget}.")


def get_bytes_processed(*, api, project: str) -> int:
    """Get the bytes processed over the last 24 hours.
    :param api: Observatory platform API client object.
    :param project: GCP project.
    :return: Number of bytes processed.
    """

    return api.get_bigquery_bytes_processed(project=project)


def save_bytes_processed(*, api: Any, project: str, bytes_estimate: int):
    """Update the number of bytes processed.
    :param api: Observatory platform API client object.
    :param project: GCP project.
    :param bytes_estimate: Bytes estimate of the current query.
    """

    record = BigQueryBytesProcessed(
        project=project,
        total=bytes_estimate,
    )
    api.post_bigquery_bytes_processed(record)


def bq_query_bytes_daily_limit_check(bytes_estimate: int) -> BigQueryBytesProcessed:
    """Check the daily byte limit. Raise exception if the current query will put us over the limit.

    :param bytes_estimate: Estimated number of bytes processed in query.
    """

    # Disable if other repos not ready yet
    if not bq_query_daily_limit_enabled():
        return

    try:
        project = Variable.get(AirflowVars.PROJECT_ID)
        api = make_observatory_api()
    except Exception as e:
        logging.warning(f"Skipping daily byte limit check: {e}")
        return

    # Get bytes processed over last 24 hours
    bytes_processed = get_bytes_processed(api=api, project=project)

    # Check that the bytes processed over the last 24 hours + the estimated query amount is below the daily limit
    projected_estimate = bytes_processed + bytes_estimate
    if projected_estimate > BIGQUERY_QUERY_DAILY_BYTE_LIMIT:
        raise Exception(
            f"The projected bytes estimate of {projected_estimate} exceeds the daily limit of {BIGQUERY_QUERY_DAILY_BYTE_LIMIT}"
        )

    # Save the estimate, assumes the actual query will run after
    save_bytes_processed(api=api, project=project, bytes_estimate=bytes_estimate)


def table_name_from_blob(blob_name: str, file_extension: str):
    """Make a BigQuery table name from a blob name.

    :param blob_name: the blob name.
    :param file_extension: the file extension of the blob.
    :return: the table name.
    """

    assert "." in file_extension, "file_extension must contain a ."
    file_name = os.path.basename(blob_name)
    match = re.match(fr".+?(?={file_extension})", file_name)
    if match is None:
        raise ValueError(f"Could not find table name from blob_name={blob_name}")
    return match.group(0)


class TransferStatus(Enum):
    """The status of the Google Cloud Data Transfer operation"""

    in_progress = "IN_PROGRESS"
    success = "SUCCESS"
    aborted = "ABORTED"
    failed = "FAILED"


def bigquery_table_exists(project_id: str, dataset_id: str, table_name: str) -> bool:
    """Checks whether a BigQuery table exists or not.

    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :param table_name: the name of the table.
    :return: whether the table exists or not.
    """

    client = bigquery.Client(project_id)
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    table = dataset.table(table_name)
    table_exists = True

    try:
        client.get_table(table)
    except NotFound:
        table_exists = False

    return table_exists


def bigquery_sharded_table_id(table_name, datetime: pendulum.Date) -> str:
    """Create a sharded table identifier for a BigQuery table.

    :param table_name: the name of the table.
    :param datetime: the date to append as a shard suffix.
    :return: the table id.
    """
    return f"{table_name}{datetime.strftime('%Y%m%d')}"


def create_bigquery_dataset(project_id: str, dataset_id: str, location: str, description: str = "") -> None:
    """Create a BigQuery dataset.

    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :param location: the location where the dataset will be stored:
    https://cloud.google.com/compute/docs/regions-zones/#locations
    :param description: a description for the dataset
    :return: None
    """

    func_name = create_bigquery_dataset.__name__

    # Make the dataset reference
    dataset_ref = f"{project_id}.{dataset_id}"

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


def delete_bigquery_dataset(project_id: str, dataset_id: str):
    """Delete a bigquery dataset and all its tables.
    :param project_id: GCP Project ID.
    :param dataset_id: GCP Dataset ID.
    """

    client = bigquery.Client()
    dataset_ref = f"{project_id}.{dataset_id}"
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def load_bigquery_table(
    uri: str,
    dataset_id: str,
    location: str,
    table: str,
    schema_file_path: str,
    source_format: str,
    csv_field_delimiter: str = ",",
    csv_quote_character: str = '"',
    csv_allow_quoted_newlines: bool = False,
    csv_skip_leading_rows: int = 0,
    partition: bool = False,
    partition_field: Union[None, str] = None,
    partition_type: bigquery.TimePartitioningType = bigquery.TimePartitioningType.DAY,
    require_partition_filter=False,
    write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    table_description: str = "",
    project_id: str = None,
    cluster: bool = False,
    clustering_fields=None,
    ignore_unknown_values: bool = False,
) -> bool:
    """Load a BigQuery table from an object on Google Cloud Storage.

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
    :param table_description: the description of the table.
    :param project_id: Google Cloud project id.
    :param cluster: whether to cluster the table or not.
    :param clustering_fields: what fields to cluster on.
    Default is to overwrite.
    :param ignore_unknown_values: whether to ignore unknown values or not.
    :return:
    """

    assert uri.startswith("gs://"), "load_big_query_table: 'uri' must begin with 'gs://'"

    func_name = load_bigquery_table.__name__
    msg = (
        f"uri={uri}, dataset_id={dataset_id}, location={location}, table={table}, "
        f"schema_file_path={schema_file_path}, source_format={source_format}"
    )
    logging.info(f"{func_name}: load bigquery table {msg}")

    client = bigquery.Client()
    if project_id is None:
        project_id = client.project
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")

    # Handle mutable default arguments
    if clustering_fields is None:
        clustering_fields = []

    # Create load job
    job_config = LoadJobConfig()

    # Set global options
    job_config.source_format = source_format
    job_config.schema = client.schema_from_json(schema_file_path)
    job_config.write_disposition = write_disposition
    job_config.destination_table_description = table_description
    job_config.ignore_unknown_values = ignore_unknown_values

    # Set CSV options
    if source_format == SourceFormat.CSV:
        job_config.field_delimiter = csv_field_delimiter
        job_config.quote_character = csv_quote_character
        job_config.allow_quoted_newlines = csv_allow_quoted_newlines
        job_config.skip_leading_rows = csv_skip_leading_rows

    # Set partitioning settings
    if partition:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type, field=partition_field, require_partition_filter=require_partition_filter
        )
    # Set clustering settings
    if cluster:
        job_config.clustering_fields = clustering_fields

    load_job = None
    try:
        load_job: [LoadJob, None] = client.load_table_from_uri(
            uri, dataset.table(table), location=location, job_config=job_config
        )

        result = load_job.result()
        state = result.state == "DONE"

        logging.info(f"{func_name}: load bigquery table result.state={result.state}, {msg}")
    except BadRequest as e:
        logging.error(f"{func_name}: load bigquery table failed: {e}.")
        if load_job:
            logging.error(f"Error collection:\n{load_job.errors}")
        state = False

    return state


def run_bigquery_query(query: str, bytes_budget: Optional[int] = 549755813888) -> list:
    """Run a BigQuery query.  Defaults to 0.5 TiB query budget.

    :param query: the query to run.
    :param bytes_budget: Maximum bytes allowed to be processed by the query.
    :return: the results.
    """

    bytes_estimate = bq_query_bytes_estimate(query)

    bq_query_bytes_budget_check(bytes_budget=bytes_budget, bytes_estimate=bytes_estimate)

    bq_query_bytes_daily_limit_check(bytes_estimate)

    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()
    success = query_job.errors is None

    return list(rows)


def copy_bigquery_table(
    source_table_id: Union[str, list],
    destination_table_id: str,
    data_location: str,
    write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
) -> bool:
    """Copy a BigQuery table.

    :param source_table_id: the id of the source table, including the project name and dataset id.
    :param destination_table_id: the id of the destination table, including the project name and dataset id.
    :param data_location: the location of the datasets.
    :param write_disposition: whether to append, overwrite or throw an error when data already exists in the table.
    :return: whether the table was copied successfully or not.
    """
    func_name = copy_bigquery_table.__name__
    msg = f"source_table_ids={source_table_id}, destination_table_id={destination_table_id}, location={data_location}"
    logging.info(f"{func_name}: copying bigquery table {msg}")

    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()

    job_config.write_disposition = write_disposition

    job = client.copy_table(source_table_id, destination_table_id, location=data_location, job_config=job_config)
    result = job.result()
    return result.done()


def create_bigquery_view(project_id: str, dataset_id: str, view_name: str, query: str) -> None:
    """Create a BigQuery view.

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


def create_bigquery_table_from_query(
    sql: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    location: str,
    description: str = "",
    labels=None,
    query_parameters=None,
    partition: bool = False,
    partition_field: Union[None, str] = None,
    partition_type: str = bigquery.TimePartitioningType.DAY,
    require_partition_filter=True,
    cluster: bool = False,
    clustering_fields=None,
    bytes_budget: Optional[int] = 549755813888,
) -> bool:
    """Create a BigQuery dataset from a provided query. Defaults to 0.5 TiB query budget.

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
    :param bytes_budget: Maximum bytes allowed to be processed by query.
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
    msg = f"project_id={project_id}, dataset_id={dataset_id}, location={location}, table={table_id}"
    logging.info(f"{func_name}: create bigquery table from query, {msg}")

    # Make the dataset reference
    dataset_ref = f"{project_id}.{dataset_id}"

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
        query_parameters=query_parameters,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Set partitioning settings
    if partition:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type, field=partition_field, require_partition_filter=require_partition_filter
        )

    if cluster:
        job_config.clustering_fields = clustering_fields

    bytes_estimate = bq_query_bytes_estimate(sql, job_config=job_config)

    bq_query_bytes_budget_check(bytes_budget=bytes_budget, bytes_estimate=bytes_estimate)

    bq_query_bytes_daily_limit_check(bytes_estimate)

    query_job: QueryJob = client.query(sql, job_config=job_config)
    query_job.result()
    success = query_job.done()
    logging.info(f"{func_name}: create bigquery table from query {msg}: {success}")
    return success


def storage_bucket_exists(bucket_name: str):
    """Check whether the Google Cloud Storage bucket exists

    :param bucket_name: Bucket name (without gs:// prefix)
    :return: Whether the bucket exists or not
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    exists = bucket.exists()

    return exists


def create_cloud_storage_bucket(
    bucket_name: str, location: str = None, project_id: str = None, lifecycle_delete_age: int = None
) -> bool:
    """Create a cloud storage bucket

    :param bucket_name: The name of the bucket. Bucket names must start and end with a number or letter.
    :param location: (Optional) The location of the bucket. If not passed, the default location, US, will be used.
    :param project_id: The project which the client acts on behalf of. Will be passed when creating a topic. If not
    passed, falls back to the default inferred from the environment.
    :param lifecycle_delete_age: Days until files in bucket are deleted
    :return: Whether creating bucket was successful or not.
    """
    func_name = create_cloud_storage_bucket.__name__
    logging.info(f"{func_name}: {bucket_name}")

    success = False

    client = storage.Client(project=project_id)
    bucket = storage.Bucket(client, name=bucket_name)
    if lifecycle_delete_age:
        bucket.add_lifecycle_delete_rule(age=lifecycle_delete_age)
    try:
        client.create_bucket(bucket, location=location)
        success = True
    except Conflict:
        logging.info(
            f"{func_name}: bucket already exists, name: {bucket.name}, project: {bucket.project_number}, "
            f"location: {bucket.location}"
        )
    return success


def copy_blob_from_cloud_storage(
    blob_name: str, bucket_name: str, destination_bucket_name: str, new_name: str = None
) -> bool:
    """Copy a blob from one bucket to another

    :param blob_name: The name of the blob. This corresponds to the unique path of the object in the bucket.
    :param bucket_name: The bucket to which the blob belongs.
    :param destination_bucket_name: The bucket into which the blob should be copied.
    :param new_name:  (Optional) The new name for the copied file.
    :return: Whether copy was successful.
    """
    func_name = copy_blob_from_cloud_storage.__name__
    logging.info(f"{func_name}: {os.path.join(bucket_name, blob_name)}")

    client = storage.Client()

    # source blob and bucket
    bucket = storage.Bucket(client, name=bucket_name)
    blob = bucket.blob(blob_name)

    # destination bucket
    destination_bucket = storage.Bucket(client, name=destination_bucket_name)

    # copy
    response = bucket.copy_blob(blob, destination_bucket=destination_bucket, new_name=new_name)
    success = True if response else False

    return success


def download_blob_from_cloud_storage(
    bucket_name: str,
    blob_name: str,
    file_path: str,
    retries: int = 3,
    connection_sem: BoundedSemaphore = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> bool:
    """Download a blob to a file.

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
        logging.info(f"{func_name}: file exists, checking hash {file_path}")
        actual_hash = crc32c_base64_hash(file_path)

        # Compare hashes
        files_match = expected_hash == actual_hash
        logging.info(
            f"{func_name}: files_match={files_match}, expected_hash={expected_hash}, " f"actual_hash={actual_hash}"
        )
        if files_match:
            logging.info(
                f"{func_name}: skipping download as files match bucket_name={bucket_name}, "
                f"blob_name={blob_name}, file_path={file_path}"
            )
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
                logging.error(f"{func_name}: exception downloading file: try={i}, file_path={file_path}, exception={e}")

        # Release connection semaphore
        if connection_sem is not None:
            connection_sem.release()

    return success


def download_blobs_from_cloud_storage(
    bucket_name: str,
    prefix: str,
    destination_path: str,
    max_processes: int = cpu_count(),
    max_connections: int = cpu_count(),
    retries: int = 3,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> bool:
    """Download all blobs on a Google Cloud Storage bucket that are within a prefixed path, to a destination on the
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
            msg = f"bucket_name={bucket_name}, blob_name={blob.name}, filename={filename}"
            logging.info(f"{func_name}: {msg}")

            # Create directory
            dirname = os.path.dirname(filename)
            os.makedirs(dirname, exist_ok=True)

            future = executor.submit(
                download_blob_from_cloud_storage,
                bucket_name,
                blob.name,
                filename,
                retries=retries,
                connection_sem=connection_sem,
                chunk_size=chunk_size,
            )
            futures.append(future)
            futures_msgs[future] = msg

        # Wait for completed tasks
        results = []
        for future in as_completed(futures):
            success = future.result()
            results.append(success)
            msg = futures_msgs[future]

            if success:
                logging.info(f"{func_name}: success, {msg}")
            else:
                logging.info(f"{func_name}: failed, {msg}")

    return all(results)


def upload_files_to_cloud_storage(
    bucket_name: str,
    blob_names: List[str],
    file_paths: List[str],
    max_processes: int = cpu_count(),
    max_connections: int = cpu_count(),
    retries: int = 3,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> bool:
    """Upload a list of files to Google Cloud storage.

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
    logging.info(f"{func_name}: uploading files")

    # Upload each file in parallel
    manager = multiprocessing.Manager()
    connection_sem = manager.BoundedSemaphore(value=max_connections)
    with ProcessPoolExecutor(max_workers=max_processes) as executor:
        # Create tasks
        futures = []
        futures_msgs = {}
        for blob_name, file_path in zip(blob_names, file_paths):
            msg = f"{func_name}: bucket_name={bucket_name}, blob_name={blob_name}, file_path={str(file_path)}"
            logging.info(f"{func_name}: {msg}")
            future = executor.submit(
                upload_file_to_cloud_storage,
                bucket_name,
                blob_name,
                file_path=str(file_path),
                retries=retries,
                connection_sem=connection_sem,
                chunk_size=chunk_size,
            )
            futures.append(future)
            futures_msgs[future] = msg

        # Wait for completed tasks
        results = []
        for future in as_completed(futures):
            print(f"future type is {type(future)}")
            success, upload = future.result()
            results.append(success)
            msg = futures_msgs[future]
            if success:
                logging.info(f"{func_name}: success, {msg}")
            else:
                logging.info(f"{func_name}: failed, {msg}")

    return all(results)


def upload_file_to_cloud_storage(
    bucket_name: str,
    blob_name: str,
    file_path: str,
    retries: int = 3,
    connection_sem: BoundedSemaphore = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    project_id: str = None,
    check_blob_hash: bool = True
) -> Tuple[bool, bool]:
    """Upload a file to Google Cloud Storage.

    :param bucket_name: the name of the Google Cloud Storage bucket.
    :param blob_name: the name of the blob to save.
    :param file_path: the path of the file to upload.
    :param retries: the number of times to retry uploading a file if an error occurs.
    :param connection_sem: a BoundedSemaphore to limit the number of upload connections that can run at once.
    :param chunk_size: the chunk size to use when uploading a blob in multiple parts, must be a multiple of 256 KB.
    :param project_id: the project in which the bucket is located, defaults to inferred from the environment.
    :param check_blob_hash: check whether the blob exists and if the crc32c hashes match, in which case skip uploading.
    :return: whether the task was successful or not and whether the file was uploaded.
    """
    func_name = upload_file_to_cloud_storage.__name__
    logging.info(f"{func_name}: bucket_name={bucket_name}, blob_name={blob_name}")

    # State
    upload = True
    success = False

    # Get blob
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Check if blob exists already and matches the file we are uploading
    if check_blob_hash and blob.exists():
        # Get blob hash
        blob.reload()
        expected_hash = blob.crc32c

        # Check file hash
        actual_hash = crc32c_base64_hash(file_path)

        # Compare hashes
        files_match = expected_hash == actual_hash
        logging.info(
            f"{func_name}: files_match={files_match}, expected_hash={expected_hash}, " f"actual_hash={actual_hash}"
        )
        if files_match:
            logging.info(
                f"{func_name}: skipping upload as files match. bucket_name={bucket_name}, blob_name={blob_name}, "
                f"file_path={file_path}"
            )
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
                logging.error(f"{func_name}: exception uploading file: try={i}, exception={e}")

        # Release connection semaphore
        if connection_sem is not None:
            connection_sem.release()

    return success, upload


def google_cloud_storage_transfer_job(job: dict, func_name: str, gc_project_id: str) -> Tuple[bool, int]:
    """Start a google cloud storage transfer job

    :param job: contains the details of the transfer job
    :param func_name: function name used for detailed logging info
    :param gc_project_id: the Google Cloud project id that holds the Google Cloud Storage bucket.
    :return: whether the transfer was a success or not and the number of objects transferred.
    """
    client = gcp_api.build("storagetransfer", "v1")
    create_result = client.transferJobs().create(body=job).execute()
    transfer_job_name = create_result["name"]

    transfer_job_filter = json.dumps({"project_id": gc_project_id, "job_names": [transfer_job_name]})
    wait_time = 60
    while True:
        response = client.transferOperations().list(name="transferOperations", filter=transfer_job_filter).execute()
        if "operations" in response:
            operations = response["operations"]

            in_progress_count, success_count, failed_count, aborted_count, objects_count = (0, 0, 0, 0, 0)
            for op in operations:
                status = op["metadata"]["status"]
                if status == TransferStatus.success.value:
                    success_count += 1
                elif status == TransferStatus.failed.value:
                    failed_count += 1
                elif status == TransferStatus.aborted.value:
                    aborted_count += 1
                elif status == TransferStatus.in_progress.value:
                    in_progress_count += 1

                try:
                    objects_found = int(op["metadata"]["counters"]["objectsFoundFromSource"])
                except KeyError:
                    objects_found = 0
                objects_count += objects_found

            num_operations = len(operations)
            logging.info(
                f"{func_name}: transfer job {transfer_job_name} operations success={success_count}, "
                f"failed={failed_count}, aborted={aborted_count}, in_progress_count={in_progress_count}, "
                f"objects_count={objects_count}"
            )

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

    return status == TransferStatus.success, objects_count


def azure_to_google_cloud_storage_transfer(
    *,
    azure_storage_account_name: str,
    azure_sas_token: str,
    azure_container: str,
    include_prefixes: List[str],
    gc_project_id: str,
    gc_bucket: str,
    description: str,
    gc_bucket_path: str = None,
    start_date: pendulum.DateTime = pendulum.now("UTC"),
) -> bool:
    """Transfer files from an Azure blob container to a Google Cloud Storage bucket.

    :param azure_storage_account_name: the name of the Azure Storage account that holds the Azure blob container.
    :param azure_sas_token: the shared access signature (SAS) for the Azure blob container.
    :param azure_container: the name of the Azure Blob container where files will be copied from.
    :param include_prefixes: the prefixes of blobs to download from the Azure blob container.
    :param gc_project_id: the Google Cloud project id that holds the Google Cloud Storage bucket.
    :param gc_bucket: the Google Cloud bucket name.
    :param description: a description for the transfer job.
    :param gc_bucket_path: the path in the Google Cloud bucket to save the objects.
    :param start_date: the date that the transfer job will start.
    :return: whether the transfer was a success or not.
    """

    # Leave out scheduleStartTime to make sure that the job starts immediately
    # Make sure to specify scheduleEndDate as today otherwise the job will repeat
    func_name = azure_to_google_cloud_storage_transfer.__name__

    job = {
        "description": description,
        "status": "ENABLED",
        "projectId": gc_project_id,
        "schedule": {
            "scheduleStartDate": {"day": start_date.day, "month": start_date.month, "year": start_date.year},
            "scheduleEndDate": {"day": start_date.day, "month": start_date.month, "year": start_date.year},
        },
        "transferSpec": {
            "azureBlobStorageDataSource": {
                "storageAccount": azure_storage_account_name,
                "azureCredentials": {"sasToken": azure_sas_token},
                "container": azure_container,
            },
            "objectConditions": {"includePrefixes": include_prefixes},
            "gcsDataSink": {"bucketName": gc_bucket},
        },
    }

    if gc_bucket_path is not None:
        # Must end in a / see https://cloud.google.com/storage-transfer/docs/create-manage-transfer-program
        if not gc_bucket_path.endswith("/"):
            gc_bucket_path = f"{gc_bucket_path}/"

        job["transferSpec"]["gcsDataSink"]["path"] = gc_bucket_path

    success, objects_count = google_cloud_storage_transfer_job(job, func_name, gc_project_id)
    return success


def aws_to_google_cloud_storage_transfer(
    aws_access_key_id: str,
    aws_secret_key: str,
    aws_bucket: str,
    include_prefixes: List[str],
    gc_project_id: str,
    gc_bucket: str,
    description: str,
    gc_bucket_path: str = None,
    last_modified_since: pendulum.DateTime = None,
    last_modified_before: pendulum.DateTime = None,
    transfer_manifest: str = None,
    start_date: pendulum.DateTime = pendulum.now("UTC"),
) -> Tuple[bool, int]:
    """Transfer files from an AWS bucket to a Google Cloud Storage bucket.

    :param aws_access_key_id: the id of the key for the aws S3 bucket.
    :param aws_secret_key: the secret key for the aws S3 bucket.
    :param aws_bucket: the name of the aws S3 bucket where files will be copied from.
    :param include_prefixes: the prefixes of blobs to download from the Azure blob container.
    :param gc_project_id: the Google Cloud project id that holds the Google Cloud Storage bucket.
    :param gc_bucket: the Google Cloud bucket name.
    :param description: a description for the transfer job.
    :param gc_bucket_path: the destination folder inside the Google Cloud bucket.
    :param last_modified_since:
    :param last_modified_before:
    :param transfer_manifest: Path to manifest file in Google Cloud bucket (incl gs://)
    :param start_date: the date that the transfer job will start.
    :return: whether the transfer was a success or not.
    """

    # Leave out scheduleStartTime to make sure that the job starts immediately
    # Make sure to specify scheduleEndDate as today otherwise the job will repeat
    func_name = aws_to_google_cloud_storage_transfer.__name__

    job = {
        "description": description,
        "status": "ENABLED",
        "projectId": gc_project_id,
        "schedule": {
            "scheduleStartDate": {"day": start_date.day, "month": start_date.month, "year": start_date.year},
            "scheduleEndDate": {"day": start_date.day, "month": start_date.month, "year": start_date.year},
        },
        "transferSpec": {
            "awsS3DataSource": {
                "bucketName": aws_bucket,
                "awsAccessKey": {"accessKeyId": aws_access_key_id, "secretAccessKey": aws_secret_key},
            },
            "objectConditions": {"includePrefixes": include_prefixes},
            "gcsDataSink": {"bucketName": gc_bucket},
        },
    }
    if gc_bucket_path:
        job["transferSpec"]["gcsDataSink"]["path"] = gc_bucket_path

    if last_modified_since:
        job["transferSpec"]["objectConditions"]["lastModifiedSince"] = last_modified_since.isoformat().replace(
            "+00:00", "Z"
        )
    if last_modified_before:
        job["transferSpec"]["objectConditions"]["lastModifiedBefore"] = last_modified_before.isoformat().replace(
            "+00:00", "Z"
        )

    if transfer_manifest:
        job["transferSpec"]["transferManifest"] = {"location": transfer_manifest}

    success, objects_count = google_cloud_storage_transfer_job(job, func_name, gc_project_id)
    return success, objects_count


def select_table_shard_dates(
    project_id: str, dataset_id: str, table_id: str, end_date: Union[pendulum.DateTime, pendulum.Date], limit: int = 1
) -> List[pendulum.Date]:
    """Returns a list of table shard dates, sorted from the most recent to the oldest date. By default it returns
    the first result.
    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :param table_id: the table id (without the date suffix on the end).
    :param end_date: the end date of the table suffixes to search for (most recent date).
    :param limit: the number of results to return.
    :return:
    """

    template_path = os.path.join(utils_templates_path(), make_sql_jinja2_filename("select_table_shard_dates"))
    query = render_template(
        template_path,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        end_date=end_date.strftime("%Y-%m-%d"),
        limit=limit,
    )
    rows = run_bigquery_query(query)
    dates = []
    for row in rows:
        py_date = row["suffix"]
        date = pendulum.Date(py_date.year, py_date.month, py_date.day)
        dates.append(date)
    return dates


def delete_bucket_dir(*, bucket_name: str, prefix: str):
    """Recursively delete blobs from a GCS bucket with a folder prefix.

    :param bucket_name: Bucket name.
    :param prefix: Directory prefix.
    """

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        blob.delete()
