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
from google.cloud import bigquery
from google.cloud import storage

from academic_observatory.utils.gc_utils import hex_to_base64_str, crc32c_base64_hash, bigquery_partitioned_table_id, \
    azure_to_google_cloud_storage_transfer, create_bigquery_dataset, load_bigquery_table


def make_account_url(account_name: str) -> str:
    """ Make an Azure Storage account URL from an account name.

    :param account_name: Azure Storage account name.
    :return: the account URL.
    """

    return f'https://{account_name}.blob.core.windows.net'


class TestGoogleCloudUtils(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestGoogleCloudUtils, self).__init__(*args, **kwargs)
        self.az_storage_account_name: str = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        self.az_container_sas_token: str = os.getenv('AZURE_CONTAINER_SAS_TOKEN')
        self.az_container_name: str = os.getenv('AZURE_CONTAINER_NAME')
        self.gc_project_id: str = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
        self.gc_bucket_name: str = os.getenv('GOOGLE_CLOUD_BUCKET_NAME')

    def test_hex_to_base64_str(self):
        data = b'c99465aa'
        expected = 'yZRlqg=='
        actual = hex_to_base64_str(data)
        self.assertEqual(expected, actual)

    def test_crc32c_base64_hash(self):
        runner = CliRunner()
        file_name = 'test.txt'
        data = 'hello world'
        expected = 'yZRlqg=='

        with runner.isolated_filesystem():
            with open(file_name, 'w') as f:
                f.write(data)

            actual = crc32c_base64_hash(file_name)
            self.assertEqual(expected, actual)

    def test_bigquery_partitioned_table_id(self):
        expected = 'my_table20200315'
        actual = bigquery_partitioned_table_id('my_table', pendulum.datetime(year=2020, month=3, day=15))
        self.assertEqual(expected, actual)

    def test_create_bigquery_dataset(self):
        location = "us-west4"
        dataset_name = f'{str(uuid.uuid4()).replace("-", "")}'
        client = bigquery.Client()
        try:
            create_bigquery_dataset(self.gc_project_id, dataset_name, location)
            # google.api_core.exceptions.NotFound will be raised if the dataset doesn't exist
            dataset: bigquery.Dataset = client.get_dataset(dataset_name)
            self.assertEqual(dataset.dataset_id, dataset_name)
        finally:
            client.delete_dataset(dataset_name, not_found_ok=True)

    def test_load_bigquery_table(self):
        # Test loading CSV table

        load_bigquery_table()

        # Test loading JSON newline table

    def test_download_blob_from_cloud_storage(self):
        pass

    def test_download_blobs_from_cloud_storage(self):
        pass

    def test_upload_files_to_cloud_storage(self):
        pass

    def test_upload_file_to_cloud_storage(self):
        pass

    @unittest.skip
    def test_azure_to_google_cloud_storage_transfer(self):
        blob_name = f'{str(uuid.uuid4())}.txt'
        blob_data = 'hello world'
        gc_blob = None
        az_blob: Optional[BlobClient] = None

        try:
            # Create client for working with Azure storage bucket
            account_url = make_account_url(self.az_storage_account_name)
            client: BlobServiceClient = BlobServiceClient(account_url, self.az_container_sas_token)
            az_blob: BlobClient = client.get_blob_client(container=self.az_container_name, blob=blob_name)
            az_blob.upload_blob(blob_data)

            # Create client for working with Google Cloud storage bucket
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)
            gc_blob = bucket.blob(blob_name)

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
            expected_crc32c = 'yZRlqg=='
            gc_blob.update()
            self.assertEqual(gc_blob.crc32c, expected_crc32c)
        finally:
            # Delete file on Google Cloud
            if gc_blob is not None:
                gc_blob.delete()

            # Delete file on Azure
            if az_blob is not None:
                az_blob.delete_blob()
