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
from google.cloud import bigquery, storage
from google.cloud.bigquery import SourceFormat
from google.cloud.bigquery.job import QueryJobConfig
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.big_query_bytes_processed import (
    BigQueryBytesProcessed,
)
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.platform.utils.file_utils import crc32c_base64_hash, hex_to_base64_str
from observatory.platform.utils.gc_utils import (
    BIGQUERY_QUERY_DAILY_BYTE_LIMIT,
    aws_to_google_cloud_storage_transfer,
    azure_to_google_cloud_storage_transfer,
    bigquery_sharded_table_id,
    bigquery_table_exists,
    bq_query_bytes_budget_check,
    bq_query_bytes_daily_limit_check,
    bq_query_bytes_estimate,
    bq_query_daily_limit_enabled,
    copy_bigquery_table,
    copy_blob_from_cloud_storage,
    create_bigquery_dataset,
    create_bigquery_table_from_query,
    create_bigquery_view,
    create_cloud_storage_bucket,
    delete_bigquery_dataset,
    delete_bucket_dir,
    download_blob_from_cloud_storage,
    download_blobs_from_cloud_storage,
    get_bytes_processed,
    load_bigquery_table,
    run_bigquery_query,
    select_table_shard_dates,
    table_name_from_blob,
    save_bytes_processed,
    upload_file_to_cloud_storage,
    upload_files_to_cloud_storage,
)
from observatory.platform.utils.test_utils import (
    ObservatoryTestCase,
    random_id,
    test_fixtures_path,
)


def make_account_url(account_name: str) -> str:
    """Make an Azure Storage account URL from an account name.
    TODO: delete once it can be imported from mag-archiver.

    :param account_name: Azure Storage account name.
    :return: the account URL.
    """

    return f"https://{account_name}.blob.core.windows.net"


class TestGoogleCloudUtilsNoAuth(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestGoogleCloudUtilsNoAuth, self).__init__(*args, **kwargs)
        self.data = "hello world"
        self.expected_crc32c = "yZRlqg=="

    def test_table_name_from_blob(self):
        # .txt extension
        file_extension = ".txt"
        blob_path = "408fb41ff5b34388b2f7ccb0d6e3be32/mag-2020-05-21/ConferenceInstances.txt"
        expected_table_name = "ConferenceInstances"
        actual_table_name = table_name_from_blob(blob_path, file_extension)
        self.assertEqual(expected_table_name, actual_table_name)

        # txt.1 extension
        blob_path = "408fb41ff5b34388b2f7ccb0d6e3be32/mag-2020-05-21/PaperAbstractsInvertedIndex.txt.1"
        expected_table_name = "PaperAbstractsInvertedIndex"
        actual_table_name = table_name_from_blob(blob_path, file_extension)
        self.assertEqual(expected_table_name, actual_table_name)

        # txt.2 extension
        blob_path = "408fb41ff5b34388b2f7ccb0d6e3be32/mag-2020-05-21/PaperAbstractsInvertedIndex.txt.2"
        expected_table_name = "PaperAbstractsInvertedIndex"
        actual_table_name = table_name_from_blob(blob_path, file_extension)
        self.assertEqual(expected_table_name, actual_table_name)

        # txt.* extension
        blob_path = "408fb41ff5b34388b2f7ccb0d6e3be32/mag-2020-05-21/PaperAbstractsInvertedIndex.txt.*"
        expected_table_name = "PaperAbstractsInvertedIndex"
        actual_table_name = table_name_from_blob(blob_path, file_extension)
        self.assertEqual(expected_table_name, actual_table_name)

        # More than 1 digit
        blob_path = "408fb41ff5b34388b2f7ccb0d6e3be32/mag-2020-05-21/PaperAbstractsInvertedIndex.txt.100"
        expected_table_name = "PaperAbstractsInvertedIndex"
        actual_table_name = table_name_from_blob(blob_path, file_extension)
        self.assertEqual(expected_table_name, actual_table_name)

        # No match
        blob_path = "408fb41ff5b34388b2f7ccb0d6e3be32/mag-2020-05-21/ConferenceInstances.jsonl"
        with self.assertRaises(ValueError):
            table_name_from_blob(blob_path, file_extension)

        # Another extension
        file_extension = ".jsonl"
        expected_table_name = "ConferenceInstances"
        actual_table_name = table_name_from_blob(blob_path, file_extension)
        self.assertEqual(expected_table_name, actual_table_name)

        # Check that assertion raised when no . in file extension
        with self.assertRaises(AssertionError):
            table_name_from_blob(blob_path, "txt")

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

    def test_bigquery_sharded_table_id(self):
        expected = "my_table20200315"
        actual = bigquery_sharded_table_id("my_table", pendulum.datetime(year=2020, month=3, day=15))
        self.assertEqual(expected, actual)


class TestGoogleCloudUtils(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestGoogleCloudUtils, self).__init__(*args, **kwargs)
        self.az_storage_account_name: str = os.getenv("TEST_AZURE_STORAGE_ACCOUNT_NAME")
        self.az_container_sas_token: str = os.getenv("TEST_AZURE_CONTAINER_SAS_TOKEN")
        self.az_container_name: str = os.getenv("TEST_AZURE_CONTAINER_NAME")
        self.aws_key_id: str = os.getenv("TEST_AWS_KEY_ID")
        self.aws_secret_key: str = os.getenv("TEST_AWS_SECRET_KEY")
        self.aws_bucket_name: str = os.getenv("TEST_AWS_BUCKET_NAME")
        self.gc_project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
        self.gc_bucket_name: str = os.getenv("TEST_GCP_BUCKET_NAME")
        self.gc_bucket_location: str = os.getenv("TEST_GCP_DATA_LOCATION")
        self.data = "hello world"
        self.expected_crc32c = "yZRlqg=="

    def test_create_bigquery_dataset(self):
        dataset_name = random_id()
        client = bigquery.Client()
        try:
            create_bigquery_dataset(self.gc_project_id, dataset_name, self.gc_bucket_location)
            # google.api_core.exceptions.NotFound will be raised if the dataset doesn't exist
            dataset: bigquery.Dataset = client.get_dataset(dataset_name)
            self.assertEqual(dataset.dataset_id, dataset_name)
        finally:
            client.delete_dataset(dataset_name, not_found_ok=True)

    def test_load_bigquery_table(self):
        schema_file_name = "people_schema.json"
        dataset_id = random_id()
        client = bigquery.Client()
        test_data_path = test_fixtures_path("utils")
        schema_folder = os.path.join(test_data_path, schema_file_name)

        # CSV file
        csv_file_path = os.path.join(test_data_path, "people.csv")
        csv_blob_name = f"people_{random_id()}.csv"

        # JSON file
        json_file_path = os.path.join(test_data_path, "people.jsonl")
        json_blob_name = f"people_{random_id()}.jsonl"

        try:
            # Create dataset
            create_bigquery_dataset(self.gc_project_id, dataset_id, self.gc_bucket_location)
            dataset: bigquery.Dataset = client.get_dataset(dataset_id)
            self.assertEqual(dataset.dataset_id, dataset_id)

            # Upload CSV to storage bucket
            result, upload = upload_file_to_cloud_storage(self.gc_bucket_name, csv_blob_name, csv_file_path)
            self.assertTrue(result)

            # Test loading CSV table
            table_name = random_id()
            uri = f"gs://{self.gc_bucket_name}/{csv_blob_name}"
            result = load_bigquery_table(
                uri,
                dataset_id,
                self.gc_bucket_location,
                table_name,
                schema_file_path=schema_folder,
                source_format=SourceFormat.CSV,
            )
            self.assertTrue(result)
            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, table_name))

            # Upload JSONL to storage bucket
            result, upload = upload_file_to_cloud_storage(self.gc_bucket_name, json_blob_name, json_file_path)
            self.assertTrue(result)

            # Test loading JSON newline table
            table_name = random_id()
            uri = f"gs://{self.gc_bucket_name}/{json_blob_name}"
            result = load_bigquery_table(
                uri,
                dataset_id,
                self.gc_bucket_location,
                table_name,
                schema_file_path=schema_folder,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            self.assertTrue(result)
            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, table_name))

            # Test loading time partitioned table
            table_name = random_id()
            result = load_bigquery_table(
                uri,
                dataset_id,
                self.gc_bucket_location,
                table_name,
                schema_file_path=schema_folder,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                partition=True,
                partition_field="dob",
            )
            self.assertTrue(result)
            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, table_name))

            # Test loading time partitioned and clustered table
            table_name = random_id()
            result = load_bigquery_table(
                uri,
                dataset_id,
                self.gc_bucket_location,
                table_name,
                schema_file_path=schema_folder,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                partition=True,
                partition_field="dob",
                cluster=True,
                clustering_fields=["first_name"],
            )
            self.assertTrue(result)
            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, table_name))
        finally:
            # Delete dataset
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

            # Delete blobs
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)
            files = [csv_blob_name, json_blob_name]
            for path in files:
                blob = bucket.blob(path)
                if blob.exists():
                    blob.delete()

    def test_run_bigquery_query(self):
        query = "SELECT * FROM `bigquery-public-data.labeled_patents.figures` LIMIT 3"
        key = {"gcs_path": 0, "x_relative_min": 1, "y_relative_min": 2, "x_relative_max": 3, "y_relative_max": 4}
        expected_results = [
            bigquery.Row(
                (
                    "gs://gcs-public-data--labeled-patents/espacenet_en66.pdf",
                    0.356321839,
                    0.745274914,
                    0.66969147,
                    0.93685567,
                ),
                key,
            ),
            bigquery.Row(
                (
                    "gs://gcs-public-data--labeled-patents/espacenet_en43.pdf",
                    0.395039322,
                    0.682130584,
                    0.640048397,
                    0.93556701,
                ),
                key,
            ),
            bigquery.Row(
                (
                    "gs://gcs-public-data--labeled-patents/espacenet_en98.pdf",
                    0.358136721,
                    0.637457045,
                    0.664246824,
                    0.93556701,
                ),
                key,
            ),
        ]
        with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
            results = run_bigquery_query(query)
        self.assertEqual(len(results), 3)
        for expected_row, actual_row in zip(expected_results, results):
            self.assertEqual(expected_row, actual_row)

        # Check bytes estimate
        bytes_estimate = bq_query_bytes_estimate(query)
        self.assertEqual(bytes_estimate, 8182)

        # Query within quota
        with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
            results = run_bigquery_query(query, bytes_budget=8182)
        self.assertEqual(len(results), 3)

        # Query exceeds quota
        with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
            self.assertRaises(Exception, run_bigquery_query, query, bytes_budget=1000)

    def test_copy_table(self):
        dataset_id = random_id()
        client = bigquery.Client()

        table_name = "figures"
        source_table_id = "bigquery-public-data.labeled_patents.figures"
        destination_table_id = f"{self.gc_project_id}.{dataset_id}.{table_name}"
        data_location = self.gc_bucket_location

        try:
            create_bigquery_dataset(self.gc_project_id, dataset_id, data_location)

            success = copy_bigquery_table(source_table_id, destination_table_id, data_location)
            self.assertTrue(success)

            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, table_name))
        finally:
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    def test_create_view(self):
        dataset_id = random_id()
        client = bigquery.Client()

        data_location = self.gc_bucket_location
        view_name = "test_view"
        try:
            create_bigquery_dataset(self.gc_project_id, dataset_id, data_location)

            query = "SELECT * FROM `bigquery-public-data.labeled_patents.figures` LIMIT 3"
            create_bigquery_view(self.gc_project_id, dataset_id, view_name, query)

            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, view_name))
        finally:
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    def test_create_bigquery_table_from_query(self):
        dataset_id = random_id()
        client = bigquery.Client()

        table_name = "presidents"
        data_location = self.gc_bucket_location
        query = """
        WITH presidents AS
        (SELECT 'Washington' as name, DATE('1789-04-30') as date UNION ALL
        SELECT 'Adams', DATE('1797-03-04') UNION ALL
        SELECT 'Jefferson', DATE('1801-03-04') UNION ALL
        SELECT 'Madison', DATE('1809-03-04') UNION ALL
        SELECT 'Monroe', DATE('1817-03-04'))
        SELECT * FROM presidents
        """

        try:
            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                create_bigquery_dataset(self.gc_project_id, dataset_id, data_location)
                success = create_bigquery_table_from_query(
                    query,
                    self.gc_project_id,
                    dataset_id,
                    table_name,
                    data_location,
                    partition=True,
                    partition_field="date",
                    cluster=True,
                    clustering_fields=["date"],
                )
            self.assertTrue(success)
            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, table_name))
        finally:
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    def test_create_bigquery_table_from_query_bytes_within_budget(self):
        dataset_id = random_id()
        client = bigquery.Client()

        table_name = "presidents"
        data_location = self.gc_bucket_location
        query = """
        WITH presidents AS
        (SELECT 'Washington' as name, DATE('1789-04-30') as date UNION ALL
        SELECT 'Adams', DATE('1797-03-04') UNION ALL
        SELECT 'Jefferson', DATE('1801-03-04') UNION ALL
        SELECT 'Madison', DATE('1809-03-04') UNION ALL
        SELECT 'Monroe', DATE('1817-03-04'))
        SELECT * FROM presidents
        """

        create_bigquery_dataset(self.gc_project_id, dataset_id, data_location)
        with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
            success = create_bigquery_table_from_query(
                query,
                self.gc_project_id,
                dataset_id,
                table_name,
                data_location,
                partition=True,
                partition_field="date",
                cluster=True,
                clustering_fields=["date"],
                bytes_budget=0,
            )
        self.assertTrue(success)
        self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, table_name))

        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    def test_create_bigquery_table_from_query_bytes_over_budget(self):
        dataset_id = random_id()
        client = bigquery.Client()

        table_name = "presidents"
        data_location = self.gc_bucket_location
        query = """
        WITH presidents AS
        (SELECT 'Washington' as name, DATE('1789-04-30') as date UNION ALL
        SELECT 'Adams', DATE('1797-03-04') UNION ALL
        SELECT 'Jefferson', DATE('1801-03-04') UNION ALL
        SELECT 'Madison', DATE('1809-03-04') UNION ALL
        SELECT 'Monroe', DATE('1817-03-04'))
        SELECT * FROM presidents
        """

        create_bigquery_dataset(self.gc_project_id, dataset_id, data_location)
        self.assertRaises(
            Exception,
            create_bigquery_table_from_query,
            query,
            self.gc_project_id,
            dataset_id,
            table_name,
            data_location,
            partition=True,
            partition_field="date",
            cluster=True,
            clustering_fields=["date"],
            bytes_budget=-1,
        )

        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    def test_create_cloud_storage_bucket(self):
        """Test that storage bucket is created"""
        client = storage.Client()
        bucket_name = "a" + random_id() + "a"
        bucket = client.bucket(bucket_name)

        lifecycle_delete_age = 1
        try:
            success = create_cloud_storage_bucket(
                bucket_name, self.gc_bucket_location, self.gc_project_id, lifecycle_delete_age
            )

            self.assertTrue(success)

            # check success is false, because bucket already exists
            success = create_cloud_storage_bucket(
                bucket_name, self.gc_bucket_location, self.gc_project_id, lifecycle_delete_age
            )
            self.assertFalse(success)
        finally:
            bucket.delete()

    def test_copy_blob_from_cloud_storage(self):
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
                result, upload = upload_file_to_cloud_storage(self.gc_bucket_name, upload_file_name, upload_file_name)

                blob_original = bucket.blob(upload_file_name)
                blob_copy = bucket.blob(copy_file_name)

                copy_blob_from_cloud_storage(
                    upload_file_name, self.gc_bucket_name, self.gc_bucket_name, new_name=copy_file_name
                )
                self.assertTrue(blob_original.exists())
                self.assertTrue(blob_copy.exists())

            finally:
                for blob in [blob_original, blob_copy]:
                    if blob.exists():
                        blob.delete()

    def test_upload_download_blobs_from_cloud_storage(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create files
            num_files = 3
            upload_folder_name = random_id()
            os.makedirs(upload_folder_name)
            upload_paths = [f"{upload_folder_name}/{random_id()}/{random_id()}.txt" for i in range(num_files)]
            upload_file_paths = []
            for path in upload_paths:
                os.makedirs(path)
                upload_file_path = os.path.join(path, f"{random_id()}.txt")
                upload_file_paths.append(upload_file_path)
                with open(upload_file_path, "w") as f:
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
                result = download_blobs_from_cloud_storage(
                    self.gc_bucket_name, upload_folder_name, download_folder_name
                )
                self.assertTrue(result)

                # Check that all files exist and have correct hashes
                for file_path in upload_file_paths:
                    download_file_path = os.path.join(
                        download_folder_name, file_path.replace(f"{upload_folder_name}/", "")
                    )
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
                result, upload = upload_file_to_cloud_storage(self.gc_bucket_name, upload_file_name, upload_file_name)
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

    def test_azure_to_google_cloud_storage_transfer(self):
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
            transfer = azure_to_google_cloud_storage_transfer(
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

    def test_aws_to_google_cloud_storage_transfer(self):
        blob_name = f"{random_id()}.txt"
        aws_blob = None

        # Create client for working with Google Cloud storage bucket
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.gc_bucket_name)
        gc_blob = bucket.blob(blob_name)

        try:
            # Create client for working with AWS storage bucket
            s3 = boto3.resource("s3", aws_access_key_id=self.aws_key_id, aws_secret_access_key=self.aws_secret_key)
            aws_blob = s3.Object(self.aws_bucket_name, blob_name)
            aws_blob.put(Body=self.data)

            # Test transfer where no data is found, because modified date is not between dates
            success, objects_count = aws_to_google_cloud_storage_transfer(
                self.aws_key_id,
                self.aws_secret_key,
                self.aws_bucket_name,
                include_prefixes=[blob_name],
                gc_project_id=self.gc_project_id,
                gc_bucket=self.gc_bucket_name,
                description=f"Test AWS to Google Cloud Storage Transfer " f"{pendulum.now('UTC').to_datetime_string()}",
                last_modified_before=pendulum.datetime(2021, 1, 1),
            )
            # Check that transfer was successful, but no objects were transferred
            self.assertTrue(success)
            self.assertEqual(0, objects_count)

            # Transfer data
            success, objects_count = aws_to_google_cloud_storage_transfer(
                self.aws_key_id,
                self.aws_secret_key,
                self.aws_bucket_name,
                include_prefixes=[blob_name],
                gc_project_id=self.gc_project_id,
                gc_bucket=self.gc_bucket_name,
                description=f"Test AWS to Google Cloud Storage Transfer " f"{pendulum.now('UTC').to_datetime_string()}",
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

    def test_delete_bigquery_dataset(self):
        def dataset_exists(project_id, dataset_id):
            client = bigquery.Client(project=project_id)
            try:
                client.get_dataset(dataset_id)
                return True
            except:
                return False

        project_id = self.gc_project_id
        dataset_id = random_id()
        create_bigquery_dataset(project_id=project_id, dataset_id=dataset_id, location=self.gc_bucket_location)

        self.assertTrue(dataset_exists(project_id, dataset_id))
        delete_bigquery_dataset(project_id, dataset_id)
        self.assertFalse(dataset_exists(project_id, dataset_id))

    def test_delete_bucket_dir(self):
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
                result, upload = upload_file_to_cloud_storage(self.gc_bucket_name, blob_name, upload_file_name)
                self.assertTrue(result)

                # Check that blob exists and has correct hash
                blob = bucket.blob(blob_name)
                self.assertTrue(blob.exists())
                blob.reload()
                self.assertEqual(self.expected_crc32c, blob.crc32c)

                delete_bucket_dir(bucket_name=self.gc_bucket_name, prefix=test_dir)
                self.assertFalse(blob.exists())
            finally:
                pass

    def test_select_table_shard_dates(self):
        client = bigquery.Client()
        dataset_id = random_id()
        table_id = "fundref"
        release_1 = pendulum.date(year=2019, month=5, day=1)
        release_2 = pendulum.date(year=2019, month=6, day=1)
        release_3 = pendulum.date(year=2019, month=7, day=1)
        query = "SELECT * FROM `bigquery-public-data.labeled_patents.figures` LIMIT 1"

        try:
            create_bigquery_dataset(self.gc_project_id, dataset_id, self.gc_bucket_location)
            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                create_bigquery_table_from_query(
                    query,
                    self.gc_project_id,
                    dataset_id,
                    bigquery_sharded_table_id(table_id, release_1),
                    self.gc_bucket_location,
                )
                create_bigquery_table_from_query(
                    query,
                    self.gc_project_id,
                    dataset_id,
                    bigquery_sharded_table_id(table_id, release_2),
                    self.gc_bucket_location,
                )
                create_bigquery_table_from_query(
                    query,
                    self.gc_project_id,
                    dataset_id,
                    bigquery_sharded_table_id(table_id, release_3),
                    self.gc_bucket_location,
                )

            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                dates = select_table_shard_dates(self.gc_project_id, dataset_id, table_id, release_1)
            for date in dates:
                self.assertIsInstance(date, pendulum.Date)
            self.assertEqual(release_1, dates[0])

            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                dates = select_table_shard_dates(self.gc_project_id, dataset_id, table_id, release_2)
            for date in dates:
                self.assertIsInstance(date, pendulum.Date)
            self.assertEqual(release_2, dates[0])

            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                dates = select_table_shard_dates(self.gc_project_id, dataset_id, table_id, release_3)
            for date in dates:
                self.assertIsInstance(date, pendulum.Date)
            self.assertEqual(release_3, dates[0])

        finally:
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


class TestBigQueryByteLimits(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.timezone = "Pacific/Auckland"
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)

    @patch("observatory.platform.utils.gc_utils.bigquery.Client.query")
    def test_bq_query_bytes_estimate(self, m_query):
        class MockJob:
            def __init__(self):
                self.total_bytes_processed = 12

        m_query.return_value = MockJob()
        bytes = bq_query_bytes_estimate()
        self.assertEqual(bytes, 12)
        _, kwargs = m_query.call_args
        self.assertEqual(kwargs["job_config"].dry_run, True)

        bytes = bq_query_bytes_estimate(**{"job_config": QueryJobConfig(dry_run=False)})
        self.assertEqual(bytes, 12)
        _, kwargs = m_query.call_args
        self.assertEqual(kwargs["job_config"].dry_run, True)

    def test_bq_query_bytes_budget_check(self):
        # no bytes_budget
        bq_query_bytes_budget_check(bytes_budget=None, bytes_estimate=0)

        # PASS
        bq_query_bytes_budget_check(bytes_budget=10, bytes_estimate=10)

        # FAIL
        self.assertRaises(Exception, bq_query_bytes_budget_check, bytes_budget=9, bytes_estimate=10)

    def test_get_bytes_processed_existing(self):
        project = "project"
        with self.env.create():
            obj = BigQueryBytesProcessed(
                project=project,
                total=10
            )
            self.api.post_bigquery_bytes_processed(obj)

            total = get_bytes_processed(api=self.api, project=project)
            self.assertEqual(total, 10)

    def test_get_bytes_processed_not_exist(self):
        project = "project"
        with self.env.create():
            total = get_bytes_processed(api=self.api, project=project)
            self.assertEqual(total, 0)
            total = self.api.get_bigquery_bytes_processed(project=project)
            self.assertEqual(total, 0)

    def test_save_bytes_processed(self):
        project = "project"
        with self.env.create():
            total = get_bytes_processed(api=self.api, project=project)
            self.assertEqual(total, 0)
            save_bytes_processed(api=self.api, project=project, bytes_estimate=10)
            total = get_bytes_processed(api=self.api, project=project)
            self.assertEqual(total, 10)

    @patch("observatory.platform.utils.gc_utils.Variable.get")
    @patch("observatory.platform.utils.gc_utils.bq_query_daily_limit_enabled")
    def test_bq_query_bytes_daily_limit_check_disabled(self, m_enable, m_get):
        m_enable.return_value = False
        bq_query_bytes_daily_limit_check(0)
        self.assertEqual(m_get.call_count, 0)

    @patch("observatory.platform.utils.gc_utils.logging.warning")
    @patch("observatory.platform.utils.gc_utils.Variable.get")
    @patch("observatory.platform.utils.gc_utils.bq_query_daily_limit_enabled")
    def test_bq_query_bytes_daily_limit_check_skipped(self, m_enable, m_get, m_log):
        self.assertTrue(bq_query_daily_limit_enabled())

        m_enable.return_value = True
        m_get.side_effect = Exception("Skip")

        bq_query_bytes_daily_limit_check(0)
        self.assertEqual(m_get.call_count, 1)
        self.assertEqual(m_log.call_count, 1)

    @patch("observatory.platform.utils.gc_utils.save_bytes_processed")
    @patch("observatory.platform.utils.gc_utils.get_bytes_processed")
    @patch("observatory.platform.utils.gc_utils.make_observatory_api")
    @patch("observatory.platform.utils.gc_utils.Variable.get")
    @patch("observatory.platform.utils.gc_utils.bq_query_daily_limit_enabled")
    def test_bq_query_bytes_daily_limit_check_within_limit(self, m_enable, m_get, m_api, m_gbp, m_ubp):
        m_enable.return_value = True
        m_get.return_value = "project"
        m_gbp.return_value = 11

        bq_query_bytes_daily_limit_check(10)
        self.assertEqual(m_get.call_count, 1)
        self.assertEqual(m_gbp.call_count, 1)
        self.assertEqual(m_ubp.call_count, 1)
        _, kwargs = m_ubp.call_args
        self.assertEqual(kwargs["bytes_estimate"], 10)

    @patch("observatory.platform.utils.gc_utils.save_bytes_processed")
    @patch("observatory.platform.utils.gc_utils.get_bytes_processed")
    @patch("observatory.platform.utils.gc_utils.make_observatory_api")
    @patch("observatory.platform.utils.gc_utils.Variable.get")
    @patch("observatory.platform.utils.gc_utils.bq_query_daily_limit_enabled")
    def test_bq_query_bytes_daily_limit_check_breach_limit(self, m_enable, m_get, m_api, m_gbp, m_ubp):
        m_enable.return_value = True
        m_get.return_value = "project"
        m_gbp.return_value = BIGQUERY_QUERY_DAILY_BYTE_LIMIT

        self.assertRaises(Exception, bq_query_bytes_daily_limit_check, 1)
        self.assertEqual(m_get.call_count, 1)
        self.assertEqual(m_gbp.call_count, 1)
        self.assertEqual(m_ubp.call_count, 0)
