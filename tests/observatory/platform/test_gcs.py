# Copyright 2020 Curtin University. All Rights Reserved.
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

import os
import unittest
from datetime import timedelta
from typing import Optional
from unittest.mock import patch

import boto3
import pendulum
from azure.storage.blob import BlobClient, BlobServiceClient
from click.testing import CliRunner
from google.cloud import storage

from observatory.platform.bigquery import bq_delete_old_datasets_with_prefix
from observatory.platform.files import crc32c_base64_hash, hex_to_base64_str
from observatory.platform.gcs import (
    gcs_blob_uri,
    gcs_create_aws_transfer,
    gcs_create_azure_transfer,
    gcs_copy_blob,
    gcs_create_bucket,
    gcs_delete_bucket_dir,
    gcs_download_blob,
    gcs_download_blobs,
    gcs_upload_file,
    gcs_upload_files,
    gcs_list_buckets_with_prefix,
    gcs_delete_old_buckets_with_prefix,
    gcs_blob_name_from_path,
)
from observatory.platform.observatory_environment import random_id, aws_bucket_test_env


def make_account_url(account_name: str) -> str:
    """Make an Azure Storage account URL from an account name.
    TODO: delete once it can be imported from mag-archiver.

    :param account_name: Azure Storage account name.
    :return: the account URL.
    """

    return f"https://{account_name}.blob.core.windows.net"


class TestGoogleCloudStorageNoAuth(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestGoogleCloudStorageNoAuth, self).__init__(*args, **kwargs)
        self.data = "hello world"
        self.expected_crc32c = "yZRlqg=="

    def test_gcs_blob_uri(self):
        blob_uri = gcs_blob_uri("my_bucket", "test.txt")
        self.assertEqual("gs://my_bucket/test.txt", blob_uri)

    def test_hex_to_base64_str(self):
        data = b"c99465aa"
        actual = hex_to_base64_str(data)
        self.assertEqual(self.expected_crc32c, actual)

    def test_crc32c_base64_hash(self):
        runner = CliRunner()
        file_name = "test.txt"

        with runner.isolated_filesystem():
            with open(file_name, "w") as f:
                f.write(self.data)

            actual_crc32c = crc32c_base64_hash(file_name)
            self.assertEqual(self.expected_crc32c, actual_crc32c)


class TestGoogleCloudStorage(unittest.TestCase):
    __init__already = False

    def __init__(self, *args, **kwargs):
        super(TestGoogleCloudStorage, self).__init__(*args, **kwargs)
        self.az_storage_account_name: str = os.getenv("TEST_AZURE_STORAGE_ACCOUNT_NAME")
        self.az_container_sas_token: str = os.getenv("TEST_AZURE_CONTAINER_SAS_TOKEN")
        self.az_container_name: str = os.getenv("TEST_AZURE_CONTAINER_NAME")
        self.aws_key = (os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY"))
        self.aws_region_name = os.getenv("AWS_DEFAULT_REGION")
        self.gc_project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
        self.gc_bucket_name: str = os.getenv("TEST_GCP_BUCKET_NAME")
        self.gc_location: str = os.getenv("TEST_GCP_DATA_LOCATION")
        self.data = "hello world"
        self.expected_crc32c = "yZRlqg=="

        self.prefix = "gcs_tests"

        # Save time and only have this run once.
        if not __class__.__init__already:
            bq_delete_old_datasets_with_prefix(prefix=self.prefix, age_to_delete=12)
            gcs_delete_old_buckets_with_prefix(prefix=self.prefix, age_to_delete=12)
            __class__.__init__already = True

    @patch("observatory.platform.airflow.Variable.get")
    def test_gcs_blob_name_from_path(self, mock_get_variable):
        """Tests the blob_name from_path function"""

        data_path = "/home/data/"
        mock_get_variable.return_value = data_path
        invalid_path = "/some/fake/invalid/path/file.txt"
        valid_path_1 = "/home/data/some/fake/valid/path/file.txt"
        valid_path_2 = f"{valid_path_1}/"  # Trailing slash
        self.assertRaises(Exception, gcs_blob_name_from_path, invalid_path)
        self.assertEqual("some/fake/valid/path/file.txt", gcs_blob_name_from_path(valid_path_1))
        self.assertEqual("some/fake/valid/path/file.txt", gcs_blob_name_from_path(valid_path_2))

    def test_gcs_create_bucket(self):
        """Test that storage bucket is created"""
        client = storage.Client()
        bucket_name = self.prefix + "_a" + random_id() + "a"
        bucket = client.bucket(bucket_name)

        lifecycle_delete_age = 1
        try:
            success = gcs_create_bucket(
                bucket_name=bucket_name,
                location=self.gc_location,
                project_id=self.gc_project_id,
                lifecycle_delete_age=lifecycle_delete_age,
            )
            self.assertTrue(success)

            # check success is false, because bucket already exists
            success = gcs_create_bucket(
                bucket_name=bucket_name,
                location=self.gc_location,
                project_id=self.gc_project_id,
                lifecycle_delete_age=lifecycle_delete_age,
            )
            self.assertFalse(success)
        finally:
            bucket.delete()

    def test_gcs_copy_blob(self):
        """Test that blob is copied from one bucket to another"""
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create file
            upload_file_name = f"{random_id()}.txt"
            copy_file_name = f"{random_id()}.txt"
            with open(upload_file_name, "w") as f:
                f.write(self.data)

            # Create client for blob
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)

            try:
                # Upload file
                result, upload = gcs_upload_file(
                    bucket_name=self.gc_bucket_name, blob_name=upload_file_name, file_path=upload_file_name
                )

                blob_original = bucket.blob(upload_file_name)
                blob_copy = bucket.blob(copy_file_name)

                gcs_copy_blob(
                    blob_name=upload_file_name,
                    src_bucket=self.gc_bucket_name,
                    dst_bucket=self.gc_bucket_name,
                    new_name=copy_file_name,
                )
                self.assertTrue(blob_original.exists())
                self.assertTrue(blob_copy.exists())

            finally:
                for blob in [blob_original, blob_copy]:
                    if blob.exists():
                        blob.delete()

    @patch("observatory.platform.airflow.Variable.get")
    def test_upload_download_blobs_from_cloud_storage(self, mock_get_variable):
        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            data_path = os.path.join(t, "data")
            os.makedirs(data_path, exist_ok=True)
            mock_get_variable.return_value = data_path

            # Create files
            upload_folder_name = random_id()
            upload_folder = os.path.join(data_path, upload_folder_name)
            file_paths = [os.path.join(upload_folder, f"{random_id()}.txt") for _ in range(3)]
            os.makedirs(upload_folder, exist_ok=True)
            for file_path in file_paths:
                with open(file_path, "w") as f:
                    f.write(self.data)

            # Get blob names
            blob_names = [gcs_blob_name_from_path(file_path) for file_path in file_paths]

            # Create client for blobs
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)

            try:
                # Upload blobs
                result = gcs_upload_files(bucket_name=self.gc_bucket_name, file_paths=file_paths)
                self.assertTrue(result)

                # Check that blobs exists and have correct hash
                for blob_name in blob_names:
                    blob = bucket.blob(blob_name)
                    self.assertTrue(blob.exists())
                    blob.reload()
                    self.assertEqual(self.expected_crc32c, blob.crc32c)

                # Download blobs
                download_folder_name = random_id()
                download_folder = os.path.join(data_path, download_folder_name)
                os.makedirs(download_folder)
                result = gcs_download_blobs(
                    bucket_name=self.gc_bucket_name, prefix=upload_folder_name, destination_path=download_folder_name
                )
                self.assertTrue(result)

                # Check that all files exist and have correct hashes
                for blob_name in blob_names:
                    download_file_path = os.path.join(
                        download_folder_name, blob_name.replace(f"{upload_folder_name}/", "")
                    )
                    self.assertTrue(os.path.isfile(download_file_path))
                    actual_crc32c = crc32c_base64_hash(download_file_path)
                    self.assertEqual(self.expected_crc32c, actual_crc32c)
            finally:
                # Delete blobs
                for blob_name in blob_names:
                    blob = bucket.blob(blob_name)
                    if blob.exists():
                        blob.delete()

    def test_upload_download_blob_from_cloud_storage(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create file
            upload_file_name = f"{random_id()}.txt"
            download_file_name = f"{random_id()}.txt"
            with open(upload_file_name, "w") as f:
                f.write(self.data)

            # Create client for blob
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)
            blob = bucket.blob(upload_file_name)

            try:
                # Upload file
                result, upload = gcs_upload_file(
                    bucket_name=self.gc_bucket_name, blob_name=upload_file_name, file_path=upload_file_name
                )
                self.assertTrue(result)

                # Check that blob exists and has correct hash
                self.assertTrue(blob.exists())
                blob.reload()
                self.assertEqual(self.expected_crc32c, blob.crc32c)

                # Download file
                result = gcs_download_blob(
                    bucket_name=self.gc_bucket_name, blob_name=upload_file_name, file_path=download_file_name
                )
                self.assertTrue(result)
                self.assertTrue(os.path.isfile(download_file_name))
                actual_crc32c = crc32c_base64_hash(download_file_name)
                self.assertEqual(self.expected_crc32c, actual_crc32c)
            finally:
                if blob.exists():
                    blob.delete()

    @unittest.skip("We are not using Azure to GCS transfer")
    def test_gcs_create_azure_transfer(self):
        blob_name = f"mag/2021-09-27/{random_id()}.txt"
        az_blob: Optional[BlobClient] = None

        # Create client for working with Google Cloud storage bucket
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.gc_bucket_name)
        gc_bucket_path = "telescopes"
        gc_blob_path = f"{gc_bucket_path}/{blob_name}"
        gc_blob = bucket.blob(gc_blob_path)

        try:
            # Create client for working with Azure storage bucket
            account_url = make_account_url(self.az_storage_account_name)
            client: BlobServiceClient = BlobServiceClient(account_url, self.az_container_sas_token)
            az_blob: BlobClient = client.get_blob_client(container=self.az_container_name, blob=blob_name)
            az_blob.upload_blob(self.data)

            # Transfer data
            transfer = gcs_create_azure_transfer(
                azure_storage_account_name=self.az_storage_account_name,
                azure_sas_token=self.az_container_sas_token,
                azure_container=self.az_container_name,
                include_prefixes=[blob_name],
                gc_project_id=self.gc_project_id,
                gc_bucket=self.gc_bucket_name,
                gc_bucket_path=gc_bucket_path,
                description=f"Test Azure to Google Cloud Storage Transfer "
                f"{pendulum.now('UTC').to_datetime_string()}",
            )

            # Check that transfer was successful
            self.assertTrue(transfer)

            # Check that blob exists
            self.assertTrue(gc_blob.exists())

            # Check that blob has expected crc32c token
            gc_blob.update()
            self.assertEqual(gc_blob.crc32c, self.expected_crc32c)
        finally:
            # Delete file on Google Cloud
            if gc_blob.exists():
                gc_blob.delete()

            # Delete file on Azure
            if az_blob is not None:
                az_blob.delete_blob()

    def test_gcs_create_aws_transfer(self):
        blob_name = f"{random_id()}.txt"
        aws_blob = None

        # Create client for working with Google Cloud storage bucket
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.gc_bucket_name)
        gc_blob = bucket.blob(blob_name)

        # Create bucket and dataset for use in first and second run
        with aws_bucket_test_env(prefix="gcs", region_name=self.aws_region_name) as aws_bucket_name:
            try:
                # Create client for working with AWS storage bucket
                s3 = boto3.resource("s3")
                aws_blob = s3.Object(aws_bucket_name, blob_name)
                aws_blob.put(Body=self.data)

                # Test transfer where no data is found, because modified date is not between dates
                success, objects_count = gcs_create_aws_transfer(
                    aws_key=self.aws_key,
                    aws_bucket=aws_bucket_name,
                    include_prefixes=[blob_name],
                    gc_project_id=self.gc_project_id,
                    gc_bucket_dst_uri=f"gs://{self.gc_bucket_name}",
                    description=f"Test AWS to Google Cloud Storage Transfer "
                    f"{pendulum.now('UTC').to_datetime_string()}",
                    last_modified_before=pendulum.datetime(2021, 1, 1),
                )
                # Check that transfer was successful, but no objects were transferred
                self.assertTrue(success)
                self.assertEqual(0, objects_count)

                # Transfer data
                success, objects_count = gcs_create_aws_transfer(
                    aws_key=self.aws_key,
                    aws_bucket=aws_bucket_name,
                    include_prefixes=[blob_name],
                    gc_project_id=self.gc_project_id,
                    gc_bucket_dst_uri=f"gs://{self.gc_bucket_name}",
                    description=f"Test AWS to Google Cloud Storage Transfer "
                    f"{pendulum.now('UTC').to_datetime_string()}",
                    last_modified_since=pendulum.datetime(2021, 1, 1),
                    last_modified_before=pendulum.now("UTC") + timedelta(days=1),
                )
                # Check that transfer was successful and 1 object was transferred
                self.assertTrue(success)
                self.assertEqual(1, objects_count)

                # Check that blob exists
                self.assertTrue(gc_blob.exists())

                # Check that blob has expected crc32c token
                gc_blob.update()
                self.assertEqual(gc_blob.crc32c, self.expected_crc32c)
            finally:
                # Delete file on Google Cloud
                if gc_blob.exists():
                    gc_blob.delete()

                # Delete file on AWS
                if aws_blob is not None:
                    aws_blob.delete()

    def test_gcs_delete_bucket_dir(self):
        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Create file
            test_dir = random_id()
            os.makedirs(os.path.join(t, test_dir), exist_ok=True)
            blob_name = os.path.join(test_dir, f"{random_id()}.txt")
            upload_file_name = os.path.join(t, blob_name)
            with open(upload_file_name, "w") as f:
                f.write(self.data)

            # Create client for blob
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)

            try:
                # Upload file
                result, upload = gcs_upload_file(
                    bucket_name=self.gc_bucket_name, blob_name=blob_name, file_path=upload_file_name
                )
                self.assertTrue(result)

                # Check that blob exists and has correct hash
                blob = bucket.blob(blob_name)
                self.assertTrue(blob.exists())
                blob.reload()
                self.assertEqual(self.expected_crc32c, blob.crc32c)

                gcs_delete_bucket_dir(bucket_name=self.gc_bucket_name, prefix=test_dir)
                self.assertFalse(blob.exists())
            finally:
                pass

    def test_gcs_list_buckets_with_prefix(self):
        client = storage.Client()
        bucket_id = self.prefix + "_" + random_id()

        try:
            # Create a test bucket
            success = gcs_create_bucket(bucket_name=bucket_id, location=self.gc_location, project_id=self.gc_project_id)
            self.assertTrue(success)

            # Get list of bucket objects under project
            bucket_list = gcs_list_buckets_with_prefix()
            bucket_names = [bucket.name for bucket in bucket_list]

            # Check that it is in the list of all other buckets
            self.assertTrue(set(bucket_names).issuperset({bucket_id}))

        finally:
            # Delete testing bucket
            bucket = client.get_bucket(bucket_id)
            bucket.delete(force=True)

    def test_gcs_delete_old_buckets_with_prefix(self):
        client = storage.Client()

        # Create unique prefix just for this test
        prefix = self.prefix + "_tdobwp_" + random_id()[:8] + "_"
        test_buckets = [prefix + random_id() for i in range(2)]

        success = False

        # Create test buckets
        for test_bucket in test_buckets:
            success_create = gcs_create_bucket(
                bucket_name=test_bucket, location=self.gc_location, project_id=self.gc_project_id
            )
            self.assertTrue(success_create)

        try:
            # Esnure buckets have been created.
            bucket_list = gcs_list_buckets_with_prefix(prefix=prefix)
            bucket_names = [bucket.name for bucket in bucket_list]
            self.assertTrue(set(bucket_names).issuperset(set(test_buckets)))

            # Remove datasets that have shared prefix and age of 0 days.
            gcs_delete_old_buckets_with_prefix(prefix=prefix, age_to_delete=0)

            # Check that buckets with unique prefix are not present.
            bucket_list_post = gcs_list_buckets_with_prefix(prefix=prefix)
            bucket_names_post = [bucket.name for bucket in bucket_list_post]
            self.assertFalse(set(bucket_names_post).issuperset(set(test_buckets)))

            success = True

        finally:
            # Delete testing buckets
            # Silly if statement since bucket.delete does not have a "if not found ok" option like for datasets.
            if not success:
                for test_bucket in test_buckets:
                    bucket = client.get_bucket(test_bucket)
                    bucket.delete(force=True)
