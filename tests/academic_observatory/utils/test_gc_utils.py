# Copyright 2019 Curtin University. All Rights Reserved.
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

import os
import unittest
import uuid
from typing import Optional

import pendulum
from azure.storage.blob import BlobServiceClient, BlobClient
# from academic_observatory.utils.gc_utils import hex_to_base64_str, crc32c_base64_hash, TransferStatus, \
# bigquery_partitioned_table_id, create_bigquery_dataset, download_blob_from_cloud_storage, \
# download_blobs_from_cloud_storage, azure_to_google_cloud_storage_transfer, load_bigquery_table, \
#     upload_files_to_cloud_storage
from click.testing import CliRunner
from google.cloud import storage, bigquery
from google.cloud.bigquery import SourceFormat, Table

from academic_observatory.utils import test_data_dir
from academic_observatory.utils.gc_utils import hex_to_base64_str, crc32c_base64_hash, bigquery_partitioned_table_id, \
    azure_to_google_cloud_storage_transfer, create_bigquery_dataset, load_bigquery_table, upload_file_to_cloud_storage, \
    download_blob_from_cloud_storage, upload_files_to_cloud_storage, download_blobs_from_cloud_storage


def make_account_url(account_name: str) -> str:
    """ Make an Azure Storage account URL from an account name.

    :param account_name: Azure Storage account name.
    :return: the account URL.
    """

    return f'https://{account_name}.blob.core.windows.net'


def random_id():
    return str(uuid.uuid4()).replace("-", "")


class TestGoogleCloudUtils(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestGoogleCloudUtils, self).__init__(*args, **kwargs)
        self.az_storage_account_name: str = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        self.az_container_sas_token: str = os.getenv('AZURE_CONTAINER_SAS_TOKEN')
        self.az_container_name: str = os.getenv('AZURE_CONTAINER_NAME')
        self.gc_project_id: str = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
        self.gc_bucket_name: str = os.getenv('GOOGLE_CLOUD_BUCKET_NAME')
        self.location = "us-west4"
        self.data = 'hello world'
        self.expected_crc32c = 'yZRlqg=='

    def test_hex_to_base64_str(self):
        data = b'c99465aa'
        actual = hex_to_base64_str(data)
        self.assertEqual(self.expected_crc32c, actual)

    def test_crc32c_base64_hash(self):
        runner = CliRunner()
        file_name = 'test.txt'

        with runner.isolated_filesystem():
            with open(file_name, 'w') as f:
                f.write(self.data)

            actual_crc32c = crc32c_base64_hash(file_name)
            self.assertEqual(self.expected_crc32c, actual_crc32c)

    def test_bigquery_partitioned_table_id(self):
        expected = 'my_table20200315'
        actual = bigquery_partitioned_table_id('my_table', pendulum.datetime(year=2020, month=3, day=15))
        self.assertEqual(expected, actual)

    def test_create_bigquery_dataset(self):
        dataset_name = random_id()
        client = bigquery.Client()
        try:
            create_bigquery_dataset(self.gc_project_id, dataset_name, self.location)
            # google.api_core.exceptions.NotFound will be raised if the dataset doesn't exist
            dataset: bigquery.Dataset = client.get_dataset(dataset_name)
            self.assertEqual(dataset.dataset_id, dataset_name)
        finally:
            client.delete_dataset(dataset_name, not_found_ok=True)

    def test_load_bigquery_table(self):
        schema_file_name = 'people_schema.json'
        dataset_name = random_id()
        client = bigquery.Client()
        test_data_path = os.path.join(test_data_dir(__file__), 'gc_utils')
        schema_path = os.path.join(test_data_path, schema_file_name)

        # CSV file
        csv_file_name = 'people.csv'
        csv_file_path = os.path.join(test_data_path, csv_file_name)

        # JSON file
        json_file_name = 'people.jsonl'
        json_file_path = os.path.join(test_data_path, json_file_name)

        try:
            # Create dataset
            create_bigquery_dataset(self.gc_project_id, dataset_name, self.location)
            dataset: bigquery.Dataset = client.get_dataset(dataset_name)
            self.assertEqual(dataset.dataset_id, dataset_name)

            # Upload CSV to storage bucket
            result = upload_file_to_cloud_storage(self.gc_bucket_name, csv_file_name, csv_file_path)
            self.assertTrue(result)

            # Test loading CSV table
            table_name = random_id()
            uri = f"gs://{self.gc_bucket_name}/{csv_file_name}"
            result = load_bigquery_table(uri, dataset_name, self.location, table_name, schema_file_path=schema_path,
                                         source_format=SourceFormat.CSV)
            self.assertTrue(result)
            table_id = f'{dataset_name}.{table_name}'
            table: Table = client.get_table(table_id)
            self.assertEqual(table.table_id, table_name)

            # Upload jsonl to storage bucket
            result = upload_file_to_cloud_storage(self.gc_bucket_name, json_file_name, json_file_path)
            self.assertTrue(result)

            # Test loading JSON newline table
            table_name = random_id()
            uri = f"gs://{self.gc_bucket_name}/{json_file_name}"
            result = load_bigquery_table(uri, dataset_name, self.location, table_name, schema_file_path=schema_path,
                                         source_format=SourceFormat.NEWLINE_DELIMITED_JSON)
            self.assertTrue(result)
            table_id = f'{dataset_name}.{table_name}'
            table: Table = client.get_table(table_id)
            self.assertEqual(table.table_id, table_name)
        finally:
            # Delete dataset
            client.delete_dataset(dataset_name, delete_contents=True, not_found_ok=True)

            # Delete blobs
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)
            files = [csv_file_name, json_file_name]
            for path in files:
                blob = bucket.blob(path)
                if blob.exists():
                    blob.delete()

    def test_upload_download_blobs_from_cloud_storage(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create files
            num_files = 3
            upload_folder_name = random_id()
            os.makedirs(upload_folder_name)
            upload_paths = [f'{upload_folder_name}/{random_id()}/{random_id()}.txt' for i in range(num_files)]
            upload_file_paths = []
            for path in upload_paths:
                os.makedirs(path)
                upload_file_path = os.path.join(path, f'{random_id()}.txt')
                upload_file_paths.append(upload_file_path)
                with open(upload_file_path, 'w') as f:
                    f.write(self.data)

            # Create client for blobs
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)

            try:
                # Upload blobs
                result = upload_files_to_cloud_storage(self.gc_bucket_name, upload_file_paths, upload_file_paths)
                self.assertTrue(result)

                # Check that blobs exists and have correct hash
                for blob_path in upload_file_paths:
                    blob = bucket.blob(blob_path)
                    self.assertTrue(blob.exists())
                    blob.reload()
                    self.assertEqual(self.expected_crc32c, blob.crc32c)

                # Download blobs
                download_folder_name = random_id()
                os.makedirs(download_folder_name)
                result = download_blobs_from_cloud_storage(self.gc_bucket_name, upload_folder_name,
                                                           download_folder_name)
                self.assertTrue(result)

                # Check that all files exist and have correct hashes
                for file_path in upload_file_paths:
                    download_file_path = os.path.join(download_folder_name,
                                                      file_path.replace(f'{upload_folder_name}/', ''))
                    self.assertTrue(os.path.isfile(download_file_path))
                    actual_crc32c = crc32c_base64_hash(download_file_path)
                    self.assertEqual(self.expected_crc32c, actual_crc32c)
            finally:
                # Delete blobs
                for blob_path in upload_file_paths:
                    blob = bucket.blob(blob_path)
                    if blob.exists():
                        blob.delete()

    def test_upload_download_blob_from_cloud_storage(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create file
            upload_file_name = f'{random_id()}.txt'
            download_file_name = f'{random_id()}.txt'
            with open(upload_file_name, 'w') as f:
                f.write(self.data)

            # Create client for blob
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)
            blob = bucket.blob(upload_file_name)

            try:
                # Upload file
                result = upload_file_to_cloud_storage(self.gc_bucket_name, upload_file_name, upload_file_name)
                self.assertTrue(result)

                # Check that blob exists and has correct hash
                self.assertTrue(blob.exists())
                blob.reload()
                self.assertEqual(self.expected_crc32c, blob.crc32c)

                # Download file
                result = download_blob_from_cloud_storage(self.gc_bucket_name, upload_file_name, download_file_name)
                self.assertTrue(result)
                self.assertTrue(os.path.isfile(download_file_name))
                actual_crc32c = crc32c_base64_hash(download_file_name)
                self.assertEqual(self.expected_crc32c, actual_crc32c)
            finally:
                if blob.exists():
                    blob.delete()

    @unittest.skip
    def test_azure_to_google_cloud_storage_transfer(self):
        blob_name = f'{random_id()}.txt'
        az_blob: Optional[BlobClient] = None

        # Create client for working with Google Cloud storage bucket
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.gc_bucket_name)
        gc_blob = bucket.blob(blob_name)

        try:
            # Create client for working with Azure storage bucket
            account_url = make_account_url(self.az_storage_account_name)
            client: BlobServiceClient = BlobServiceClient(account_url, self.az_container_sas_token)
            az_blob: BlobClient = client.get_blob_client(container=self.az_container_name, blob=blob_name)
            az_blob.upload_blob(self.data)

            # Transfer data
            transfer = azure_to_google_cloud_storage_transfer(
                self.az_storage_account_name,
                self.az_container_sas_token,
                self.az_container_name,
                include_prefixes=[blob_name],
                gc_project_id=self.gc_project_id,
                gc_bucket=self.gc_bucket_name,
                description='Test transfer'
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
