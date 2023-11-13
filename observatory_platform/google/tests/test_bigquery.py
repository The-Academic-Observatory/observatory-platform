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

import datetime
import json
import os
import unittest
from unittest.mock import patch

import pendulum
import time
from click.testing import CliRunner
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage
from google.cloud.bigquery import SourceFormat

from observatory_platform.config import module_file_path
from observatory_platform.google.bigquery import (
    bq_table_id,
    bq_select_latest_table,
    bq_sharded_table_id,
    bq_table_exists,
    bq_create_dataset,
    bq_query_bytes_estimate,
    bq_copy_table,
    bq_snapshot,
    bq_upsert_records,
    bq_delete_records,
    bq_select_columns,
    bq_create_table_from_query,
    bq_create_view,
    bq_create_empty_table,
    bq_load_table,
    bq_load_from_memory,
    bq_run_query,
    bq_select_table_shard_dates,
    bq_delete_old_datasets_with_prefix,
    bq_list_datasets_with_prefix,
    bq_list_tables,
    bq_export_table,
    bq_query_bytes_budget_check,
)
from observatory_platform.files import load_jsonl
from observatory_platform.google.gcs import gcs_delete_old_buckets_with_prefix, gcs_upload_file
from observatory_platform.sandbox.test_utils import random_id, bq_dataset_test_env


class TestGoogleCloudUtilsNoAuth(unittest.TestCase):
    def test_bigquery_sharded_table_id(self):
        expected = "project_id.dataset_id.my_table20200315"
        actual = bq_sharded_table_id(
            "project_id", "dataset_id", "my_table", pendulum.datetime(year=2020, month=3, day=15)
        )
        self.assertEqual(expected, actual)


class TestBigQuery(unittest.TestCase):
    __init__already = False

    def __init__(self, *args, **kwargs):
        super(TestBigQuery, self).__init__(*args, **kwargs)
        self.gc_project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
        self.gc_bucket_name: str = os.getenv("TEST_GCP_BUCKET_NAME")
        self.gc_location: str = os.getenv("TEST_GCP_DATA_LOCATION")
        self.data = "hello world"
        self.expected_crc32c = "yZRlqg=="
        self.prefix = "bq_tests"
        self.patents_table_id = f"bigquery-public-data.labeled_patents.figures"
        self.test_data_path = module_file_path("observatory_platform.google.tests.fixtures")

        # Save time and only have this run once.
        if not __class__.__init__already:
            bq_delete_old_datasets_with_prefix(prefix=self.prefix, age_to_delete=12)
            gcs_delete_old_buckets_with_prefix(prefix=self.prefix, age_to_delete=12)
            __class__.__init__already = True

    @patch("observatory_platform.google.bigquery.bq_select_table_shard_dates")
    def test_bq_select_latest_table(self, mock_sel_table_suffixes):
        """Test make_table_name"""
        dt = pendulum.datetime(2021, 1, 1)
        mock_sel_table_suffixes.return_value = [dt]

        # Sharded
        table_id = bq_table_id("project_id", "dataset_id", "hello")
        sharded_table_id = bq_table_id("project_id", "dataset_id", "hello20210101")
        actual_table_id = bq_select_latest_table(table_id=table_id, end_date=dt, sharded=True)
        self.assertEqual(sharded_table_id, actual_table_id)

        # Not sharded
        actual_table_id = bq_select_latest_table(table_id=table_id, end_date=dt, sharded=False)
        self.assertEqual(table_id, actual_table_id)

    def test_bq_create_dataset(self):
        # This test doesn't use bq_dataset_test_env as it is testing bq_create_dataset
        dataset_id = self.prefix + "_" + random_id()
        client = bigquery.Client()
        try:
            bq_create_dataset(project_id=self.gc_project_id, dataset_id=dataset_id, location=self.gc_location)
            # google.api_core.exceptions.NotFound will be raised if the dataset doesn't exist
            dataset: bigquery.Dataset = client.get_dataset(dataset_id)
            self.assertEqual(dataset.dataset_id, dataset_id)
        finally:
            client.delete_dataset(dataset_id, not_found_ok=True)

    def test_bq_create_empty_table(self):
        schema_file_path = os.path.join(self.test_data_path, "people_schema.json")

        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            # Create table with all parameters set
            table_id = bq_table_id(self.gc_project_id, dataset_id, "with_schema")
            table = bq_create_empty_table(
                table_id=table_id,
                schema_file_path=schema_file_path,
                clustering_fields=["dob"],
            )
            self.assertIsInstance(table, bigquery.Table)
            self.assertEqual(["dob"], table.clustering_fields)
            self.assertTrue(bq_table_exists(table_id=table_id))

            # Create table with minimal parameters set
            table_id = bq_table_id(self.gc_project_id, dataset_id, "without_schema")
            table = bq_create_empty_table(table_id=table_id)
            self.assertIsInstance(table, bigquery.Table)
            self.assertTrue(bq_table_exists(table_id=table_id))

    def test_bq_query_bytes_estimate(self):
        # Check that correctly estimates bytes budget
        expected_bytes = 8182
        bytes = bq_query_bytes_estimate("SELECT * FROM bigquery-public-data.labeled_patents.figures")
        self.assertEqual(expected_bytes, bytes)

    def test_bq_query_bytes_budget_check(self):
        # Under budget
        bq_query_bytes_budget_check(bytes_budget=10, bytes_estimate=10)

        # Over budget
        with self.assertRaises(Exception):
            bq_query_bytes_budget_check(bytes_budget=9, bytes_estimate=10)

        # Test assertion errors
        with self.assertRaises(AssertionError):
            bq_query_bytes_budget_check(bytes_budget=None, bytes_estimate=10)

        with self.assertRaises(AssertionError):
            bq_query_bytes_budget_check(bytes_budget="1", bytes_estimate=10)

        with self.assertRaises(AssertionError):
            bq_query_bytes_budget_check(bytes_budget=1, bytes_estimate=None)

        with self.assertRaises(AssertionError):
            bq_query_bytes_budget_check(bytes_budget=1, bytes_estimate="1")

    def test_bq_run_query(self):
        query = f"SELECT * FROM `{self.patents_table_id}` LIMIT 3"
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
        results = bq_run_query(query)
        self.assertEqual(len(results), 3)
        for expected_row, actual_row in zip(expected_results, results):
            self.assertEqual(expected_row, actual_row)

        # Check bytes estimate
        bytes_estimate = bq_query_bytes_estimate(query)
        self.assertEqual(bytes_estimate, 8182)

        # Query within quota
        results = bq_run_query(query, bytes_budget=8182)
        self.assertEqual(len(results), 3)

        # Query exceeds quota
        self.assertRaises(Exception, bq_run_query, query, bytes_budget=1000)

    def test_bq_copy_table(self):
        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            src_table_id = self.patents_table_id
            dst_table_id = bq_table_id(self.gc_project_id, dataset_id, "figures")
            success = bq_copy_table(src_table_id=src_table_id, dst_table_id=dst_table_id)
            self.assertTrue(success)
            self.assertTrue(bq_table_exists(table_id=dst_table_id))

    def test_bq_create_view(self):
        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            # Create a view
            query = f"SELECT * FROM `{self.patents_table_id}` LIMIT 3"
            view_id = bq_table_id(self.gc_project_id, dataset_id, "test_view")
            bq_create_view(view_id=view_id, query=query)
            self.assertTrue(bq_table_exists(table_id=view_id))

            # Attempt to update the view created above
            query = f"SELECT * FROM `{self.patents_table_id}` LIMIT 2"
            view = bq_create_view(view_id=view_id, query=query, update_if_exists=True)
            self.assertEqual(view.view_query, query)
            with self.assertRaises(Conflict):
                bq_create_view(view_id=view_id, query=query, update_if_exists=False)

    def test_bq_create_table_from_query_without_schema(self):
        query = """
        WITH presidents AS
        (SELECT 'Washington' as name, DATE('1789-04-30') as date UNION ALL
        SELECT 'Adams', DATE('1797-03-04') UNION ALL
        SELECT 'Jefferson', DATE('1801-03-04') UNION ALL
        SELECT 'Madison', DATE('1809-03-04') UNION ALL
        SELECT 'Monroe', DATE('1817-03-04'))
        SELECT * FROM presidents
        """

        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            # Test with clustering fields
            table_id = bq_table_id(self.gc_project_id, dataset_id, "clustered")
            success = bq_create_table_from_query(
                sql=query,
                table_id=table_id,
                clustering_fields=["date"],
            )
            self.assertTrue(success)
            self.assertTrue(bq_table_exists(table_id=table_id))

            # Test without clustering fields
            table_id = bq_table_id(self.gc_project_id, dataset_id, "not_clustered")
            success = bq_create_table_from_query(
                sql=query,
                table_id=table_id,
            )
            self.assertTrue(success)
            self.assertTrue(bq_table_exists(table_id=table_id))

    def test_bq_create_table_from_query_with_schema(self):
        query = """
        WITH presidents AS
        (SELECT 'Washington' as name, DATE('1789-04-30') as date UNION ALL
        SELECT 'Adams', DATE('1797-03-04') UNION ALL
        SELECT 'Jefferson', DATE('1801-03-04') UNION ALL
        SELECT 'Madison', DATE('1809-03-04') UNION ALL
        SELECT 'Monroe', DATE('1817-03-04'))
        SELECT * FROM presidents
        """

        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            with CliRunner().isolated_filesystem():
                schema = [
                    {"mode": "NULLABLE", "name": "name", "type": "STRING", "description": "Foo Bar"},
                    {"mode": "NULLABLE", "name": "date", "type": "DATE", "description": "Foo Bar"},
                ]
                schema_file_path = "schema.json"
                with open(schema_file_path, "w") as f:
                    json.dump(schema, f)

                # Test with clustering fields
                table_id = bq_table_id(self.gc_project_id, dataset_id, "clustered")
                success = bq_create_table_from_query(
                    sql=query,
                    table_id=table_id,
                    clustering_fields=["date"],
                    schema_file_path=schema_file_path,
                )
                self.assertTrue(success)
                self.assertTrue(bq_table_exists(table_id=table_id))

                # Test without clustering fields
                table_id = bq_table_id(self.gc_project_id, dataset_id, "not_clustered")
                success = bq_create_table_from_query(
                    sql=query,
                    table_id=table_id,
                    schema_file_path=schema_file_path,
                )
                self.assertTrue(success)
                self.assertTrue(bq_table_exists(table_id=table_id))

    def test_bq_create_table_from_query_bytes_within_budget(self):
        query = """
        WITH presidents AS
        (SELECT 'Washington' as name, DATE('1789-04-30') as date UNION ALL
        SELECT 'Adams', DATE('1797-03-04') UNION ALL
        SELECT 'Jefferson', DATE('1801-03-04') UNION ALL
        SELECT 'Madison', DATE('1809-03-04') UNION ALL
        SELECT 'Monroe', DATE('1817-03-04'))
        SELECT * FROM presidents
        """

        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents")
            success = bq_create_table_from_query(
                sql=query,
                table_id=table_id,
                clustering_fields=["date"],
                bytes_budget=0,
            )
            self.assertTrue(success)
            self.assertTrue(bq_table_exists(table_id=table_id))

    def test_bq_create_table_from_query_bytes_over_budget(self):
        query = """
        WITH presidents AS
        (SELECT 'Washington' as name, DATE('1789-04-30') as date UNION ALL
        SELECT 'Adams', DATE('1797-03-04') UNION ALL
        SELECT 'Jefferson', DATE('1801-03-04') UNION ALL
        SELECT 'Madison', DATE('1809-03-04') UNION ALL
        SELECT 'Monroe', DATE('1817-03-04'))
        SELECT * FROM presidents
        """

        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents")
            self.assertRaises(
                Exception,
                bq_create_table_from_query,
                sql=query,
                table_id=table_id,
                clustering_fields=["date"],
                bytes_budget=-1,
            )

    def test_bq_list_datasets_with_prefix(self):
        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            # Get list of datasets under project
            dataset_list = bq_list_datasets_with_prefix()
            dataset_names = [dataset.dataset_id for dataset in dataset_list]
            self.assertTrue(set(dataset_names).issuperset({dataset_id}))

    def test_bq_delete_old_datasets_with_prefix(self):
        client = bigquery.Client()

        # Create unique prefix just for this test
        prefix = self.prefix + "_tdodwp_" + random_id()[:8] + "_"
        test_datasets = [prefix + random_id() for i in range(2)]

        try:
            # Create test datasets
            for test_dataset in test_datasets:
                bq_create_dataset(project_id=self.gc_project_id, dataset_id=test_dataset, location=self.gc_location)

            # Ensure that datasets have been created.
            dataset_list = bq_list_datasets_with_prefix(prefix=prefix)
            dataset_names = [dataset.dataset_id for dataset in dataset_list]
            self.assertTrue(set(dataset_names).issuperset(set(test_datasets)))

            # Remove datasets that have shared prefix and age of 0 days.
            bq_delete_old_datasets_with_prefix(prefix=prefix, age_to_delete=0)

            # Check that datasets have been deleted.
            dataset_list_post = bq_list_datasets_with_prefix(prefix=prefix)
            dataset_names_post = [dataset.dataset_id for dataset in dataset_list_post]
            self.assertFalse(set(dataset_names_post).issuperset(set(test_datasets)))

        finally:
            # Delete testing datasets
            for test_dataset in test_datasets:
                client.delete_dataset(test_dataset, delete_contents=True, not_found_ok=True)

    def test_bq_select_table_shard_dates(self):
        release_1 = pendulum.date(year=2019, month=5, day=1)
        release_2 = pendulum.date(year=2019, month=6, day=1)
        release_3 = pendulum.date(year=2019, month=7, day=1)
        query = f"SELECT * FROM `{self.patents_table_id}` LIMIT 1"

        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            bq_create_table_from_query(
                sql=query,
                table_id=bq_sharded_table_id(self.gc_project_id, dataset_id, "fundref", release_1),
            )
            bq_create_table_from_query(
                sql=query,
                table_id=bq_sharded_table_id(self.gc_project_id, dataset_id, "fundref", release_2),
            )
            bq_create_table_from_query(
                sql=query,
                table_id=bq_sharded_table_id(self.gc_project_id, dataset_id, "fundref", release_3),
            )

            table_id = bq_table_id(self.gc_project_id, dataset_id, "fundref")
            dates = bq_select_table_shard_dates(table_id=table_id, end_date=release_1)
            for date in dates:
                self.assertIsInstance(date, pendulum.Date)
            self.assertEqual(release_1, dates[0])

            dates = bq_select_table_shard_dates(table_id=table_id, end_date=release_2)
            for date in dates:
                self.assertIsInstance(date, pendulum.Date)
            self.assertEqual(release_2, dates[0])

            dates = bq_select_table_shard_dates(table_id=table_id, end_date=release_3)
            for date in dates:
                self.assertIsInstance(date, pendulum.Date)
            self.assertEqual(release_3, dates[0])

    def test_bq_list_tables(self):
        table_ids = bq_list_tables("bigquery-public-data", "labeled_patents")
        self.assertSetEqual(
            {
                "bigquery-public-data.labeled_patents.extracted_data",
                "bigquery-public-data.labeled_patents.figures",
                "bigquery-public-data.labeled_patents.invention_types",
            },
            set(table_ids),
        )

    def test_bq_export_table(self):
        client = storage.Client()
        csv_blob_name = f"figures_{random_id()}.csv"

        try:
            # Export table
            table_id = "bigquery-public-data.labeled_patents.figures"
            uri = f"gs://{self.gc_bucket_name}/{csv_blob_name}"
            success = bq_export_table(table_id=table_id, file_type="csv", destination_uri=uri)
            self.assertTrue(success)

            # Check that file is in GCS
            bucket = client.get_bucket(self.gc_bucket_name)
            blob = bucket.blob(csv_blob_name)
            self.assertTrue(blob.exists())

        finally:
            # Delete blobs
            # TODO: use the observatory environment
            bucket = client.get_bucket(self.gc_bucket_name)
            files = [csv_blob_name]
            for path in files:
                blob = bucket.blob(path)
                if blob.exists():
                    blob.delete()

    def test_bq_load_table(self):
        schema_file_path = os.path.join(self.test_data_path, "people_schema.json")

        # CSV file
        csv_file_path = os.path.join(self.test_data_path, "people.csv")
        csv_blob_name = f"people_{random_id()}.csv"

        # JSON files
        json_file_path = os.path.join(self.test_data_path, "people.jsonl")
        json_blob_name = f"people_{random_id()}.jsonl"
        json_extra_file_path = os.path.join(self.test_data_path, "people_extra.jsonl")
        json_extra_blob_name = f"people_{random_id()}.jsonl"

        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            try:
                # Upload CSV to storage bucket
                result, upload = gcs_upload_file(
                    bucket_name=self.gc_bucket_name, blob_name=csv_blob_name, file_path=csv_file_path
                )
                self.assertTrue(result)

                # Test loading CSV table
                uri = f"gs://{self.gc_bucket_name}/{csv_blob_name}"
                table_id = bq_table_id(self.gc_project_id, dataset_id, random_id())
                result = bq_load_table(
                    uri=uri,
                    table_id=table_id,
                    schema_file_path=schema_file_path,
                    source_format=SourceFormat.CSV,
                )
                self.assertTrue(result)
                self.assertTrue(bq_table_exists(table_id=table_id))

                # Upload JSONL to storage bucket
                result, upload = gcs_upload_file(
                    bucket_name=self.gc_bucket_name, blob_name=json_blob_name, file_path=json_file_path
                )
                self.assertTrue(result)

                # Test loading JSON newline table
                uri = f"gs://{self.gc_bucket_name}/{json_blob_name}"
                table_id = bq_table_id(self.gc_project_id, dataset_id, random_id())
                result = bq_load_table(
                    uri=uri,
                    table_id=table_id,
                    schema_file_path=schema_file_path,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                )
                self.assertTrue(result)
                self.assertTrue(bq_table_exists(table_id=table_id))

                # Upload additional JSONL to storage bucket
                result, upload = gcs_upload_file(
                    bucket_name=self.gc_bucket_name, blob_name=json_extra_blob_name, file_path=json_extra_file_path
                )
                self.assertTrue(result)

                # Test loading two files into the same table
                uri = [
                    f"gs://{self.gc_bucket_name}/{json_blob_name}",
                    f"gs://{self.gc_bucket_name}/{json_extra_blob_name}",
                ]
                table_id = bq_table_id(self.gc_project_id, dataset_id, random_id())
                result = bq_load_table(
                    uri=uri,
                    table_id=table_id,
                    schema_file_path=schema_file_path,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                )
                self.assertTrue(result)
                self.assertTrue(bq_table_exists(table_id=table_id))

                # Test loading time partitioned table
                table_id = bq_table_id(self.gc_project_id, dataset_id, random_id())
                result = bq_load_table(
                    uri=uri,
                    table_id=table_id,
                    schema_file_path=schema_file_path,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                    partition=True,
                    partition_field="dob",
                )
                self.assertTrue(result)
                self.assertTrue(bq_table_exists(table_id=table_id))

                # Test loading time partitioned and clustered table
                table_id = bq_table_id(self.gc_project_id, dataset_id, random_id())
                result = bq_load_table(
                    uri=uri,
                    table_id=table_id,
                    schema_file_path=schema_file_path,
                    source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                    partition=True,
                    partition_field="dob",
                    cluster=True,
                    clustering_fields=["first_name"],
                )
                self.assertTrue(result)
                self.assertTrue(bq_table_exists(table_id=table_id))
            finally:
                # Delete blobs
                storage_client = storage.Client()
                bucket = storage_client.get_bucket(self.gc_bucket_name)
                files = [csv_blob_name, json_blob_name]
                for path in files:
                    blob = bucket.blob(path)
                    if blob.exists():
                        blob.delete()

    def test_bq_load_from_memory(self):
        json_file_path = os.path.join(self.test_data_path, "people.jsonl")
        test_data = load_jsonl(json_file_path)

        schema_file_path = os.path.join(self.test_data_path, "people_schema.json")

        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            # Test loading from memory - without a schema
            table_id = bq_table_id(self.gc_project_id, dataset_id, random_id())
            result = bq_load_from_memory(
                records=test_data,
                table_id=table_id,
                schema_file_path=None,
            )
            self.assertTrue(result)
            self.assertTrue(bq_table_exists(table_id=table_id))

            # Test loading from memory - with a schema
            table_id = bq_table_id(self.gc_project_id, dataset_id, random_id())
            result = bq_load_from_memory(
                records=test_data,
                table_id=table_id,
                schema_file_path=schema_file_path,
            )
            self.assertTrue(result)
            self.assertTrue(bq_table_exists(table_id=table_id))

            # Test loading time partitioned table
            table_id = bq_table_id(self.gc_project_id, dataset_id, random_id())
            result = bq_load_from_memory(
                records=test_data,
                table_id=table_id,
                schema_file_path=schema_file_path,
                partition=True,
                partition_field="dob",
            )
            self.assertTrue(result)
            self.assertTrue(bq_table_exists(table_id=table_id))

            # Test loading time partitioned and clustered table
            table_id = bq_table_id(self.gc_project_id, dataset_id, random_id())
            result = bq_load_from_memory(
                records=test_data,
                table_id=table_id,
                schema_file_path=schema_file_path,
                partition=True,
                partition_field="dob",
                cluster=True,
                clustering_fields=["first_name"],
            )
            self.assertTrue(result)
            self.assertTrue(bq_table_exists(table_id=table_id))

    def test_bq_select_columns(self):
        columns = bq_select_columns(table_id=self.patents_table_id)
        self.assertEqual(
            [
                dict(column_name="gcs_path", data_type="STRING"),
                dict(column_name="x_relative_min", data_type="FLOAT64"),
                dict(column_name="y_relative_min", data_type="FLOAT64"),
                dict(column_name="x_relative_max", data_type="FLOAT64"),
                dict(column_name="y_relative_max", data_type="FLOAT64"),
            ],
            columns,
        )

    def test_bq_upsert_records(self):
        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            # Create main table
            main_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents")
            sql = """
            WITH presidents AS
            (SELECT 'Washington' as name, '67' as death_age, DATE('1789-04-29') as date UNION ALL
            SELECT 'Adams', '90', DATE('1797-03-04') UNION ALL
            SELECT 'Jefferson', '83', DATE('1801-03-04') UNION ALL
            SELECT 'Madison', '85', DATE('1809-03-04') UNION ALL
            SELECT 'Monroe', '73', DATE('1817-03-04')) 
            SELECT * FROM presidents
            """
            success = bq_create_table_from_query(
                sql=sql,
                table_id=main_table_id,
            )
            self.assertTrue(success)

            # Create upsert table
            # Washington updated with correct inaugural address date
            # Kennedy added
            # Adams, president that shares shame name but won't update but add record because the death_age is different.
            upsert_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents_upserts")
            sql = """
            WITH presidents AS
            (SELECT 'Washington' as name, '67' as death_age, DATE('1789-04-30') as date UNION ALL
            SELECT 'Adams', '80', DATE('1829-03-04') UNION ALL
            SELECT 'Kennedy', '46', DATE('1961-01-20'))
            SELECT * FROM presidents
            """
            success = bq_create_table_from_query(
                sql=sql,
                table_id=upsert_table_id,
            )
            self.assertTrue(success)

            # Upsert records
            bq_upsert_records(
                main_table_id=main_table_id, upsert_table_id=upsert_table_id, primary_key=["name", "death_age"]
            )

            # Check that main_table is in correct state
            expected = [
                dict(name="Adams", death_age="80", date=datetime.date(1829, 3, 4)),
                dict(name="Adams", death_age="90", date=datetime.date(1797, 3, 4)),
                dict(name="Jefferson", death_age="83", date=datetime.date(1801, 3, 4)),
                dict(name="Kennedy", death_age="46", date=datetime.date(1961, 1, 20)),
                dict(name="Madison", death_age="85", date=datetime.date(1809, 3, 4)),
                dict(name="Monroe", death_age="73", date=datetime.date(1817, 3, 4)),
                dict(name="Washington", death_age="67", date=datetime.date(1789, 4, 30)),
            ]
            results = bq_run_query(f"SELECT * FROM {main_table_id} ORDER BY name ASC")
            results = [dict(row) for row in results]
            self.assertEqual(expected, results)

    def test_bq_delete_records(self):
        def insert_data(
            main_table_id: str,
            delete_table_id: str,
            main_key: str = "name",
            delete_key: str = "name",
            main_prefix: str = "",
            delete_prefix: str = "",
        ):
            sql = f"""
            WITH presidents AS
            (SELECT '{main_prefix}Washington' as {main_key}, DATE('1789-04-30') as date UNION ALL
            SELECT '{main_prefix}Adams', DATE('1797-03-04') UNION ALL
            SELECT '{main_prefix}Jefferson', DATE('1801-03-04') UNION ALL
            SELECT '{main_prefix}Madison', DATE('1809-03-04') UNION ALL
            SELECT '{main_prefix}Monroe', DATE('1817-03-04'))
            SELECT * FROM presidents
            """
            success = bq_create_table_from_query(
                sql=sql,
                table_id=main_table_id,
            )
            self.assertTrue(success)

            # Create upsert table
            # Delete Madison and Monroe
            sql = f"""
            WITH presidents AS
            (SELECT '{delete_prefix}Madison' as {delete_key}, DATE('1809-03-04') as date UNION ALL
            SELECT '{delete_prefix}Monroe', DATE('1817-03-03') )
            SELECT * FROM presidents
            """
            success = bq_create_table_from_query(
                sql=sql,
                table_id=delete_table_id,
            )
            self.assertTrue(success)

        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            main_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents")
            delete_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents_deletes")
            insert_data(main_table_id, delete_table_id)

            # Delete records: same primary key
            bq_delete_records(
                main_table_id=main_table_id,
                delete_table_id=delete_table_id,
                main_table_primary_key="name",
                delete_table_primary_key="name",
            )

            # Check that main_table is in correct state
            expected = [
                dict(name="Adams", date=datetime.date(1797, 3, 4)),
                dict(name="Jefferson", date=datetime.date(1801, 3, 4)),
                dict(name="Washington", date=datetime.date(1789, 4, 30)),
            ]
            results = bq_run_query(f"SELECT * FROM {main_table_id} ORDER BY name ASC")
            results = [dict(row) for row in results]
            self.assertEqual(expected, results)

        # Delete records: different primary key
        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            main_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents")
            delete_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents_deletes")
            insert_data(main_table_id, delete_table_id, delete_key="hello")

            # Delete records: same primary key
            bq_delete_records(
                main_table_id=main_table_id,
                delete_table_id=delete_table_id,
                main_table_primary_key="name",
                delete_table_primary_key="hello",
            )

            # Check that main_table is in correct state
            expected = [
                dict(name="Adams", date=datetime.date(1797, 3, 4)),
                dict(name="Jefferson", date=datetime.date(1801, 3, 4)),
                dict(name="Washington", date=datetime.date(1789, 4, 30)),
            ]
            results = bq_run_query(f"SELECT * FROM {main_table_id} ORDER BY name ASC")
            results = [dict(row) for row in results]
            self.assertEqual(expected, results)

        # Delete records: add a prefix
        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            main_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents")
            delete_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents_deletes")
            insert_data(main_table_id, delete_table_id, main_prefix="President", delete_prefix="")

            # Delete records: same primary key
            bq_delete_records(
                main_table_id=main_table_id,
                delete_table_id=delete_table_id,
                main_table_primary_key="name",
                delete_table_primary_key="name",
                main_table_primary_key_prefix="",
                delete_table_primary_key_prefix="President",
            )

            # Check that main_table is in correct state
            expected = [
                dict(name="PresidentAdams", date=datetime.date(1797, 3, 4)),
                dict(name="PresidentJefferson", date=datetime.date(1801, 3, 4)),
                dict(name="PresidentWashington", date=datetime.date(1789, 4, 30)),
            ]
            results = bq_run_query(f"SELECT * FROM {main_table_id} ORDER BY name ASC")
            results = [dict(row) for row in results]
            self.assertEqual(expected, results)

        # Delete records: By matching on multiple fields.
        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            main_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents")
            delete_table_id = bq_table_id(self.gc_project_id, dataset_id, "presidents_deletes")
            insert_data(main_table_id, delete_table_id)

            # Delete records: By matching on multiple keys.
            bq_delete_records(
                main_table_id=main_table_id,
                delete_table_id=delete_table_id,
                main_table_primary_key=["name", "date"],
                delete_table_primary_key=["name", "date"],
                main_table_primary_key_prefix="",
                delete_table_primary_key_prefix="",
            )

            # Check that main_table is in correct state
            expected = [
                dict(name="Adams", date=datetime.date(1797, 3, 4)),
                dict(name="Jefferson", date=datetime.date(1801, 3, 4)),
                dict(name="Monroe", date=datetime.date(1817, 3, 4)),
                dict(name="Washington", date=datetime.date(1789, 4, 30)),
            ]
            results = bq_run_query(f"SELECT * FROM {main_table_id} ORDER BY name ASC")
            results = [dict(row) for row in results]
            self.assertEqual(expected, results)

    def test_bq_snapshot(self):
        with bq_dataset_test_env(
            project_id=self.gc_project_id, location=self.gc_location, prefix=self.prefix
        ) as dataset_id:
            # First copy data to src_table_id
            src_table_id = bq_table_id(self.gc_project_id, dataset_id, "figures")
            success = bq_copy_table(
                src_table_id=self.patents_table_id,
                dst_table_id=src_table_id,
            )
            self.assertTrue(success)

            # Create snapshot
            date = pendulum.now()
            expiry_date = date.add(seconds=10)
            dst_table_id = bq_sharded_table_id(self.gc_project_id, dataset_id, "figures_snapshot", date)
            success = bq_snapshot(
                src_table_id=src_table_id,
                dst_table_id=dst_table_id,
                expiry_date=expiry_date,
            )
            self.assertTrue(success)

            # Test if snapshot exists
            self.assertTrue(bq_table_exists(table_id=dst_table_id))

            # Check that expires by waiting 15 seconds and checking that the table doesn't exist
            time.sleep(15)
            self.assertFalse(bq_table_exists(table_id=dst_table_id))
