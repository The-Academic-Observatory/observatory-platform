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

# Author: Richard Hosking

import os
import unittest
from typing import Optional

import pendulum
from click.testing import CliRunner
from google.cloud import bigquery, storage
from google.cloud.bigquery import SourceFormat
from observatory.dags.config import workflow_sql_templates_path
from observatory.platform.utils.jinja2_utils import make_sql_jinja2_filename, render_template
from observatory.platform.utils.file_utils import crc32c_base64_hash, hex_to_base64_str
from observatory.platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 bigquery_table_exists,
                                                 copy_bigquery_table,
                                                 copy_blob_from_cloud_storage,
                                                 create_bigquery_dataset,
                                                 create_bigquery_table_from_query,
                                                 download_blob_from_cloud_storage,
                                                 download_blobs_from_cloud_storage,
                                                 load_bigquery_table,
                                                 run_bigquery_query,
                                                 table_name_from_blob,
                                                 upload_file_to_cloud_storage,
                                                 upload_files_to_cloud_storage)

from tests.observatory.test_utils import random_id
from tests.observatory.test_utils import test_fixtures_path

class TestElasticSearchExportQueries(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestElasticSearchExportQueries, self).__init__(*args, **kwargs)
        self.gc_project_id: str = os.getenv('TEST_GCP_PROJECT_ID')
        self.gc_bucket_name: str = os.getenv('TEST_GCP_BUCKET_NAME')
        self.gc_bucket_location: str = os.getenv('TEST_GCP_DATA_LOCATION')
        self.data = 'hello world'
        self.expected_crc32c = 'yZRlqg=='

    def test_export_dois(self):
        schema_file_name = 'doi_schema.json'
        dataset_id = random_id()
        client = bigquery.Client()
        test_data_path = os.path.join(test_fixtures_path(), 'workflows', 'es_exports')
        schema_path = os.path.join(test_data_path, schema_file_name)

        # Elasticsearch test index id
        es_index_id="dois"

        # DOIs Base
        release_date_previous=pendulum.datetime(year=2020, month=8, day=10)
        dois_base_json_file_path = os.path.join(test_data_path, 'dois_base.jsonl')
        dois_base_json_blob_name = f'dois_base_{random_id()}.jsonl'

        # DOIs New
        release_date_current=pendulum.datetime(year=2020, month=8, day=17)
        dois_new_json_file_path = os.path.join(test_data_path, 'dois_new.jsonl')
        dois_new_json_blob_name = f'dois_new_{random_id()}.jsonl'


        try:
            # Create dataset
            create_bigquery_dataset(self.gc_project_id, dataset_id, self.gc_bucket_location)
            dataset: bigquery.Dataset = client.get_dataset(dataset_id)
            self.assertEqual(dataset.dataset_id, dataset_id)

            # Upload DOIs Base JSONL to storage bucket and into BigQuery
            result, upload = upload_file_to_cloud_storage(self.gc_bucket_name, dois_base_json_blob_name, dois_base_json_file_path)
            self.assertTrue(result)

            dois_base_table_name = bigquery_partitioned_table_id('doi', release_date_previous)
            uri = f"gs://{self.gc_bucket_name}/{dois_base_json_blob_name}"
            result = load_bigquery_table(uri, dataset_id, self.gc_bucket_location, dois_base_table_name,
                                         schema_file_path=schema_path,
                                         source_format=SourceFormat.NEWLINE_DELIMITED_JSON)
            self.assertTrue(result)
            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, dois_base_table_name))

            # Upload DOIs New JSONL to storage bucket and into BigQuery
            result, upload = upload_file_to_cloud_storage(self.gc_bucket_name, dois_new_json_blob_name, dois_new_json_file_path)
            self.assertTrue(result)

            dois_new_table_name = bigquery_partitioned_table_id('doi', release_date_current)
            uri = f"gs://{self.gc_bucket_name}/{dois_new_json_blob_name}"
            result = load_bigquery_table(uri, dataset_id, self.gc_bucket_location, dois_new_table_name,
                                         schema_file_path=schema_path,
                                         source_format=SourceFormat.NEWLINE_DELIMITED_JSON)
            self.assertTrue(result)
            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, dois_new_table_name))

            # Test DOI diff Query
            template_path = os.path.join(workflow_sql_templates_path(), make_sql_jinja2_filename('export_doi_diff'))
            sql = render_template(template_path, project_id=self.gc_project_id, dataset_id=dataset_id, 
                                  release_date_previous=release_date_previous, release_date_current=release_date_current, 
                                  es_index_id=es_index_id)

            result_table_name = random_id()

            success = create_bigquery_table_from_query(sql, self.gc_project_id, dataset_id, result_table_name, self.gc_bucket_location)
            self.assertTrue(success)
            self.assertTrue(bigquery_table_exists(self.gc_project_id, dataset_id, result_table_name))


            # Check for expected number of Updates, Creates and Deletions
            template_path = os.path.join(test_data_path, make_sql_jinja2_filename('check_diff'))
            sql = render_template(template_path, project_id=self.gc_project_id, dataset_id=dataset_id, 
                                  table_id=result_table_name)
            
            key = {'updated': 0, 'created': 1, 'deleted': 2}
            expected_results = [bigquery.Row((13, 4, 3), key)]

            results = run_bigquery_query(sql)
            self.assertEqual(len(results), 1)
            for expected_row, actual_row in zip(expected_results, results):
                self.assertEqual(expected_row, actual_row)



        finally:
            # Delete dataset
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

            # Delete blobs
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.gc_bucket_name)
            files = [dois_base_json_blob_name, dois_new_json_blob_name]
            for path in files:
                blob = bucket.blob(path)
                if blob.exists():
                    blob.delete()
