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

import contextlib
import csv
import datetime
import json
import logging
import os
import pathlib
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from multiprocessing import cpu_count
from typing import List, Tuple, Optional

import pendulum
from airflow import AirflowException
from google.api_core.exceptions import Conflict
from google.auth.credentials import Credentials
from google.cloud import storage
from google.cloud.exceptions import Conflict
from google.cloud.storage import Blob, bucket
from googleapiclient import discovery as gcp_api
from requests.exceptions import ChunkedEncodingError

from observatory_platform.airflow import get_data_path
from observatory_platform.files import crc32c_base64_hash

# The chunk size to use when uploading / downloading a blob in multiple parts, must be a multiple of 256 KB.
DEFAULT_CHUNK_SIZE = 256 * 1024 * 4


class TransferStatus(Enum):
    """The status of the Google Cloud Data Transfer operation"""

    in_progress = "IN_PROGRESS"
    success = "SUCCESS"
    aborted = "ABORTED"
    failed = "FAILED"


def gcs_blob_uri(bucket_name: str, blob_name: str) -> str:
    """Generates a GCS blob URI

    :param bucket_name: The name of the storage bucket
    :param blob_name: The name of the blob in the bucket
    :return: The GCS URI
    """
    return f"gs://{bucket_name}/{blob_name}"


def gcs_uri_parts(blob_uri: str) -> Tuple[str, str]:
    """Extracts the GCS bucket name and blob path from the given GCS URI.

    :param blob_uri: the blob URI.
    :return:
    """

    if not blob_uri.startswith("gs://"):
        raise ValueError("Invalid GCS uri, it should start with 'gs://'")

    parts = blob_uri[5:].split("/", 1)  # Remove 'gs://' and split the remaining string
    bucket_name = parts[0]
    blob_path = parts[1] if len(parts) > 1 else None

    return bucket_name, blob_path


def gcs_blob_name_from_path(local_filepath: str) -> str:
    """Creates a blob name from a local file path.

    :param local_filepath: The local filepath
    :return: The name of the blob on cloud storage
    """

    # Get the workflow folder for this file and find where the data path starts
    data_path = get_data_path()
    if not local_filepath.startswith(data_path):
        raise Exception("Provided local path does not begin with the DATA PATH variable")

    # Remove data path
    sans_data_path = local_filepath[len(data_path) :]

    # Make sure that path is using forward slashes for Google Cloud Storage
    blob_path = pathlib.Path(sans_data_path).as_posix().strip("/")

    return blob_path


def gcs_bucket_exists(bucket_name: str, client: Optional[storage.Client] = None):
    """Check whether the Google Cloud Storage bucket exists

    :param bucket_name: Bucket name (without gs:// prefix)
    :param client: Storage client. If None default Client is created.
    :return: Whether the bucket exists or not
    """
    if client is None:
        client = storage.Client()
    bucket = client.bucket(bucket_name)

    exists = bucket.exists()

    return exists


def gcs_create_bucket(
    *,
    bucket_name: str,
    location: str = None,
    project_id: str = None,
    lifecycle_delete_age: int = None,
    client: storage.Client = None,
) -> bool:
    """Create a cloud storage bucket

    :param bucket_name: The name of the bucket. Bucket names must start and end with a number or letter.
    :param location: (Optional) The location of the bucket. If not passed, the default location, US, will be used.
    :param project_id: The project which the client acts on behalf of. Will be passed when creating a topic. If not
    passed, falls back to the default inferred from the environment.
    :param lifecycle_delete_age: Days until files in bucket are deleted
    :param client: Storage client. If None default Client is created.
    :return: Whether creating bucket was successful or not.
    """
    func_name = gcs_create_bucket.__name__
    logging.info(f"{func_name}: {bucket_name}")

    success = False

    if client is None:
        client = storage.Client()
    client.project = project_id
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


def gcs_copy_blob(
    *, blob_name: str, src_bucket: str, dst_bucket: str, new_name: str = None, client: storage.Client = None
) -> bool:
    """Copy a blob from one bucket to another

    :param blob_name: The name of the blob. This corresponds to the unique path of the object in the bucket.
    :param src_bucket: The bucket to which the blob belongs.
    :param dst_bucket: The bucket into which the blob should be copied.
    :param new_name:  (Optional) The new name for the copied file.
    :param client: Storage client. If None default Client is created.
    :return: Whether copy was successful.
    """
    func_name = gcs_copy_blob.__name__
    logging.info(f"{func_name}: {os.path.join(src_bucket, blob_name)}")

    if client is None:
        client = storage.Client()

    # source blob and bucket
    bucket = storage.Bucket(client, name=src_bucket)
    blob = bucket.blob(blob_name)

    # destination bucket
    destination_bucket = storage.Bucket(client, name=dst_bucket)

    # copy
    response = bucket.copy_blob(blob, destination_bucket=destination_bucket, new_name=new_name)
    success = True if response else False

    return success


def gcs_download_blob(
    *,
    bucket_name: str,
    blob_name: str,
    file_path: str,
    retries: int = 3,
    connection_sem: threading.BoundedSemaphore = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    client: storage.Client = None,
) -> bool:
    """Download a blob to a file.

    :param bucket_name: the name of the Google Cloud storage bucket.
    :param blob_name: the path to the blob.
    :param file_path: the file path where the blob should be saved.
    :param retries: the number of times to retry downloading the blob.
    :param connection_sem: a BoundedSemaphore to limit the number of download connections that can run at once.
    :param chunk_size: the chunk size to use when downloading a blob in multiple parts, must be a multiple of 256 KB.
    :param client: Storage client. If None default Client is created.
    :return: whether the download was successful or not.
    """

    func_name = gcs_download_blob.__name__
    logging.info(f"{func_name}: {file_path}")

    # Get blob
    if client is None:
        client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob: Blob = bucket.blob(blob_name)
    uri = gcs_blob_uri(bucket_name, blob_name)

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
                logging.error(
                    f"{func_name}: exception downloading file: try={i}, file_path={file_path}, uri={uri}, exception={e}"
                )

        # Release connection semaphore
        if connection_sem is not None:
            connection_sem.release()

    return success


def gcs_download_blobs(
    *,
    bucket_name: str,
    prefix: str,
    destination_path: str,
    max_processes: int = cpu_count(),
    max_connections: int = cpu_count(),
    retries: int = 3,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    client: storage.Client = None,
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
    :param client: Storage client. If None default Client is created.
    :return: whether the files were downloaded successfully or not.
    """

    func_name = gcs_download_blobs.__name__

    # Get bucket
    if client is None:
        client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # List blobs
    blobs: List[Blob] = list(bucket.list_blobs(prefix=prefix))
    logging.info(f"{func_name}: {blobs}")

    # Download each blob in parallel
    connection_sem = threading.BoundedSemaphore(value=max_connections)
    with ThreadPoolExecutor(max_workers=max_processes) as executor:
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
                gcs_download_blob,
                bucket_name=bucket_name,
                blob_name=blob.name,
                file_path=filename,
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


def gcs_upload_files(
    *,
    bucket_name: str,
    file_paths: List[str],
    blob_names: List[str] = None,
    max_processes: int = cpu_count(),
    max_connections: int = cpu_count(),
    retries: int = 3,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    credentials: Optional[Credentials] = None,
) -> bool:
    """Upload a list of files to Google Cloud storage.

    :param bucket_name: the name of the Google Cloud storage bucket.
    :param file_paths: the paths of the files to upload as blobs.
    :param blob_names: the destination paths of blobs where the files will be uploaded. If not specified then these
    will be automatically generated based on the file_paths.
    :param max_processes: the maximum number of processes.
    :param max_connections: the maximum number of upload connections at once.
    :param retries: the number of times to retry uploading a file if an error occurs.
    :param chunk_size: the chunk size to use when uploading a blob in multiple parts, must be a multiple of 256 KB.
    :param credentials: the credentials to use with the Google Cloud Storage client.
    :return: whether the files were uploaded successfully or not.
    """

    func_name = gcs_upload_files.__name__
    logging.info(f"{func_name}: uploading files")

    # Assert that files exist
    is_files = [os.path.isfile(file_path) for file_path in file_paths]
    if not all(is_files):
        not_found = []
        for file_path, is_file in zip(file_paths, is_files):
            if not is_file:
                not_found.append(file_path)
        raise AirflowException(f"{func_name}: the following files could not be found {not_found}")

    # Create blob names
    if blob_names is None:
        blob_names = [gcs_blob_name_from_path(file_path) for file_path in file_paths]

    # Assert that file_paths and blob_names have the same length
    assert len(file_paths) == len(blob_names), f"{func_name}: file_paths and blob_names have different lengths"

    # Upload each file in parallel
    connection_sem = threading.BoundedSemaphore(value=max_connections)
    with ThreadPoolExecutor(max_workers=max_processes) as executor:
        # Create tasks
        futures = []
        futures_msgs = {}
        for blob_name, file_path in zip(blob_names, file_paths):
            msg = f"{func_name}: bucket_name={bucket_name}, blob_name={blob_name}, file_path={str(file_path)}"
            logging.info(f"{func_name}: {msg}")
            future = executor.submit(
                gcs_upload_file,
                bucket_name=bucket_name,
                blob_name=str(blob_name),
                file_path=str(file_path),
                retries=retries,
                connection_sem=connection_sem,
                chunk_size=chunk_size,
                credentials=credentials,
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


def gcs_upload_file(
    *,
    bucket_name: str,
    blob_name: str,
    file_path: str,
    retries: int = 3,
    connection_sem: Optional[threading.BoundedSemaphore] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    check_blob_hash: bool = True,
    client: storage.Client = None,
    credentials: Optional[Credentials] = None,
) -> Tuple[bool, bool]:
    """Upload a file to Google Cloud Storage.

    :param bucket_name: the name of the Google Cloud Storage bucket.
    :param blob_name: the name of the blob to save.
    :param file_path: the path of the file to upload.
    :param retries: the number of times to retry uploading a file if an error occurs.
    :param connection_sem: a BoundedSemaphore to limit the number of upload connections that can run at once.
    :param chunk_size: the chunk size to use when uploading a blob in multiple parts, must be a multiple of 256 KB.
    :param check_blob_hash: check whether the blob exists and if the crc32c hashes match, in which case skip uploading.
    :param client: Storage client. If None default Client is created.
    :param credentials: the credentials to use with the Google Cloud Storage client.
    :return: whether the task was successful or not and whether the file was uploaded.
    """
    func_name = gcs_upload_file.__name__
    logging.info(f"{func_name}: bucket_name={bucket_name}, blob_name={blob_name}")

    # State
    upload = True
    success = False

    # Get blob
    if client is None:
        client = storage.Client(credentials=credentials)

    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    uri = gcs_blob_uri(bucket_name, blob_name)

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
            logging.info(f"{func_name}: skipping upload as files match. uri={uri}, file_path={file_path}")
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
                logging.error(
                    f"{func_name}: exception uploading file: try={i}, file_path={file_path}, uri={uri}, exception={e}"
                )

        # Release connection semaphore
        if connection_sem is not None:
            connection_sem.release()

    return success, upload


def gcs_create_transfer_job(
    *,
    job: dict,
    func_name: str,
    gc_project_id: str,
    credentials: Optional[Credentials] = None,
) -> Tuple[bool, int]:
    """Start a google cloud storage transfer job

    :param job: contains the details of the transfer job
    :param func_name: function name used for detailed logging info
    :param gc_project_id: the Google Cloud project id that holds the Google Cloud Storage bucket.
    :param credentials: the credentials to use with the Google Cloud Storage client.
    :return: whether the transfer was a success or not and the number of objects transferred.
    """
    client = gcp_api.build("storagetransfer", "v1", credentials=credentials)
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


def gcs_create_azure_transfer(
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
    credentials: Optional[Credentials] = None,
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
    :param credentials: the credentials to use with the Google Cloud Storage client.
    :return: whether the transfer was a success or not.
    """

    # Leave out scheduleStartTime to make sure that the job starts immediately
    # Make sure to specify scheduleEndDate as today otherwise the job will repeat
    func_name = gcs_create_azure_transfer.__name__

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

    success, objects_count = gcs_create_transfer_job(
        job=job, func_name=func_name, gc_project_id=gc_project_id, credentials=credentials
    )
    return success


def gcs_create_aws_transfer(
    *,
    aws_key: Tuple[str, str],
    aws_bucket: str,
    include_prefixes: List[str],
    gc_project_id: str,
    gc_bucket_dst_uri: str,
    description: str,
    last_modified_since: pendulum.DateTime = None,
    last_modified_before: pendulum.DateTime = None,
    transfer_manifest: str = None,
    start_date: pendulum.DateTime = pendulum.now("UTC"),
    credentials: Optional[Credentials] = None,
) -> Tuple[bool, int]:
    """Transfer files from an AWS bucket to a Google Cloud Storage bucket.

    :param aws_key: a tuple containing the AWS Access Key ID and the Secret Key for the AWS S3 bucket.
    :param aws_bucket: the name of the aws S3 bucket where files will be copied from.
    :param include_prefixes: the prefixes of blobs to download from the Azure blob container.
    :param gc_project_id: the Google Cloud project id that holds the Google Cloud Storage bucket.
    :param gc_bucket_dst_uri: the full path to destination folder inside the Google Cloud bucket (incl gs://). If no path is specified after the bucket name, then the data will be transferred to the root of the bucket.
    :param description: a description for the transfer job.
    :param last_modified_since:
    :param last_modified_before:
    :param transfer_manifest: Path to manifest file in Google Cloud bucket (incl gs://).
    :param start_date: the date that the transfer job will start.
    :param credentials: the credentials to use with the Google Cloud Storage client.
    :return: whether the transfer was a success or not.
    """

    # Leave out scheduleStartTime to make sure that the job starts immediately
    # Make sure to specify scheduleEndDate as today otherwise the job will repeat
    func_name = gcs_create_aws_transfer.__name__
    aws_access_key_id, aws_secret_key = aws_key
    bucket_name, blob_path = gcs_uri_parts(gc_bucket_dst_uri)

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
            "gcsDataSink": {"bucketName": bucket_name},
        },
    }
    if blob_path:
        job["transferSpec"]["gcsDataSink"]["path"] = blob_path

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

    success, objects_count = gcs_create_transfer_job(
        job=job, func_name=func_name, gc_project_id=gc_project_id, credentials=credentials
    )
    return success, objects_count


def _is_utf8_str(s: str) -> bool:
    try:
        s.encode("utf-8").decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def gcs_upload_transfer_manifest(object_paths: List[str], blob_uri: str, client: storage.Client = None):
    """Save a GCS transfer manifest CSV file.

    :param object_paths: the object paths excluding bucket name.
    :param blob_uri: the full URI on GCS where the manifest should be uploaded to.
    :param client: Storage client. If None default Client is created.
    :return: None.
    """

    if client is None:
        client = storage.Client()

    # Write temp file
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as file:
        writer = csv.writer(file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for object_path in object_paths:
            # Check that object_path is UTF-8
            assert _is_utf8_str(object_path), f"gcs_save_transfer_manifest: {object_path} is not a UTF-8 string"
            writer.writerow([object_path])

        # Flush the buffer
        file.flush()

        # Check under 1 GiB: https://cloud.google.com/storage-transfer/docs/manifest#create_a_manifest
        file_path = file.name
        gib_size = os.path.getsize(file_path) / (1024**3)
        assert gib_size <= 1, f"gcs_save_transfer_manifest: {file_path} is {gib_size}GiB, but must be <= 1GiB."

        # Upload to cloud storage
        bucket_name, blob_path = gcs_uri_parts(blob_uri)
        success = gcs_upload_file(bucket_name=bucket_name, blob_name=blob_path, file_path=file_path, client=client)
        assert success, f"gcs_upload_transfer_manifest: error uploading manifest to {blob_uri}"


def gcs_delete_bucket_dir(*, bucket_name: str, prefix: str, client: storage.Client = None):
    """Recursively delete blobs from a GCS bucket with a folder prefix.

    :param bucket_name: Bucket name.
    :param prefix: Directory prefix.
    :param client: Storage client. If None default Client is created.
    """

    if client is None:
        client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        blob.delete()


def gcs_list_buckets_with_prefix(*, prefix: str = "", client: storage.Client = None) -> List[bucket.Bucket]:
    """List all Google Cloud buckets with prefix.

    :param prefix: Prefix of the buckets to list
    :param client: Storage client. If None default Client is created.
    :return: A list of bucket objects that are under the project.
    """

    if client is None:
        client = storage.Client()
    buckets = list(client.list_buckets())
    bucket_list = []
    for bucket in buckets:
        if bucket.name.startswith(prefix):
            bucket_list.append(bucket)

    return bucket_list


def gcs_list_blobs(
    bucket_name: str, prefix: str = None, match_glob: str = None, client: storage.Client = None
) -> List[storage.Blob]:
    """List blobs in a bucket using a gcs_uri.

    :param bucket_name: The name of the bucket
    :param prefix: The prefix to filter by
    :param match_glob: The glob pattern to filter by
    :param client: Storage client. If None default Client is created.
    :return: A list of blob objects in the bucket
    """
    if client is None:
        client = storage.Client()
    return list(client.list_blobs(bucket_name, prefix=prefix, match_glob=match_glob))


def gcs_delete_old_buckets_with_prefix(*, prefix: str, age_to_delete: int, client: storage.Client = None):
    """Deletes buckets that share the same prefix and if it is older than "age_to_delete" hours.

    Due to multiple unit tests being run at once, need to include a try and except as
    test buckets could have been deleted by other unit tests inbetween the time that they were
    grabbed and deleted, resulting in a "not found" error.

    :param prefix: The identifying prefix of the buckets to delete.
    :param age_to_delete: Delete if the age of the bucket is older than this amount.
    :param client: Storage client. If None default Client is created.
    """

    if client is None:
        client = storage.Client()

    # List all buckets in the project.
    bucket_list = gcs_list_buckets_with_prefix(prefix=prefix, client=client)

    buckets_deleted = []
    for bucket in bucket_list:
        # Check bucket age
        bucket_age = (datetime.datetime.now(datetime.timezone.utc) - bucket.time_created).total_seconds() / 3600.0

        # Delete bucket if older than specified age
        if bucket_age >= age_to_delete:
            # Attempt to delete bucket
            try:
                bucket.delete(force=True)
                buckets_deleted.append(bucket.name)
            except:
                logging.info(f"Bucket {bucket.name} was not found and removed. It may have already been deleted.")
                pass

    if len(buckets_deleted) < 1:
        logging.info(f"No buckets with prefix '{prefix}' older than {age_to_delete} hours to delete.")
    else:
        logging.info(
            f"Deleted the following buckets with prefix '{prefix}' older than {age_to_delete} hours: {buckets_deleted}"
        )


@contextlib.contextmanager
def gcs_hmac_key(project_id, service_account_email, client: storage.Client = None):
    """Generates a new HMAC key using the given project and service account.
    Deletes it when context closes.

    :param project_id: The Google Cloud project ID
    :param service_account_email: The service account used to generate the HMAC key
    :param client: Storage client. If None default Client is created.
    """

    if client is None:
        client = storage.Client(project=project_id)

    key, secret = client.create_hmac_key(service_account_email=service_account_email, project_id=project_id)
    try:
        yield key, secret
    finally:
        key.state = "INACTIVE"
        key.update()
        key.delete()
