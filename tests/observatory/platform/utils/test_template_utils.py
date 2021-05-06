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

# Author: James Diprose, Aniek Roelofs

import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import pendulum

from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from airflow.exceptions import AirflowException
from click.testing import CliRunner
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
from observatory.dags.config import schema_path
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.telescopes.stream_telescope import StreamRelease, StreamTelescope
from observatory.platform.utils.template_utils import (SubFolder, add_partition_date, blob_name, bq_append_from_file,
                                                       bq_append_from_partition, bq_delete_old,
                                                       bq_load_ingestion_partition, bq_load_partition, bq_load_shard,
                                                       bq_load_shard_v2, create_date_table_id, on_failure_callback,
                                                       partition_field, prepare_bq_load, prepare_bq_load_v2,
                                                       reset_variables, table_ids_from_path, telescope_path,
                                                       test_data_path, upload_files_from_list)

from tests.observatory.test_utils import random_id


class MockSnapshotTelescope(SnapshotTelescope):
    def __init__(self, dag_id: str = 'dag_id', start_date: datetime = datetime(2021, 1, 1), schedule_interval: str =
                 '@weekly', dataset_id: str = random_id(), source_format: SourceFormat =
                 SourceFormat.NEWLINE_DELIMITED_JSON, schema_prefix: str = 'prefix', schema_version: str = '1',
                 dataset_description: str = 'dataset_description'):
        table_descriptions = {'file': 'table description'}
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, source_format=source_format,
                         schema_prefix=schema_prefix, schema_version=schema_version,
                         dataset_description=dataset_description, table_descriptions=table_descriptions)

    def make_release(self, **kwargs):
        return SnapshotRelease(self.dag_id, datetime(2021, 3, 1), transform_files_regex='file.txt')


class MockStreamTelescope(StreamTelescope):
    def __init__(self, dag_id: str = 'dag_id', start_date: datetime = datetime(2021, 1, 1), schedule_interval: str =
                 '@weekly', dataset_id: str = random_id(), merge_partition_field: str = 'id', updated_date_field: str
                 = 'timestamp', bq_merge_days: int = 7, source_format: SourceFormat =
                 SourceFormat.NEWLINE_DELIMITED_JSON, schema_prefix: str = 'prefix', schema_version: str = '1',
                 dataset_description: str = 'dataset_description'):
        table_descriptions = {'file': 'table description'}
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, merge_partition_field, updated_date_field,
                         bq_merge_days, source_format=source_format,
                         schema_prefix=schema_prefix, schema_version=schema_version,
                         dataset_description=dataset_description, table_descriptions=table_descriptions)

    def make_release(self, **kwargs):
        return StreamRelease(self.dag_id, datetime(2021, 2, 1), datetime(2021, 3, 1), False,
                             transform_files_regex='file.txt')


class TestTemplateUtils(unittest.TestCase):
    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_telescope_path(self, mock_variable_get):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Mock getting home path
            reset_variables()
            data_path = "data"
            mock_variable_get.return_value = data_path

            # The name of the telescope to create and expected root folder
            telescope_name = "grid"
            root_path = os.path.join(data_path, "telescopes")

            # Create subdir
            path_downloaded = telescope_path(SubFolder.downloaded, telescope_name)
            expected = os.path.join(root_path, SubFolder.downloaded.value, telescope_name)
            self.assertEqual(expected, path_downloaded)
            self.assertTrue(os.path.exists(path_downloaded))

            # Create subdir
            path_extracted = telescope_path(SubFolder.extracted, telescope_name)
            expected = os.path.join(root_path, SubFolder.extracted.value, telescope_name)
            self.assertEqual(expected, path_extracted)
            self.assertTrue(os.path.exists(path_extracted))

            # Create subdir
            path_transformed = telescope_path(SubFolder.transformed, telescope_name)
            expected = os.path.join(root_path, SubFolder.transformed.value, telescope_name)
            self.assertEqual(expected, path_transformed)
            self.assertTrue(os.path.exists(path_transformed))

    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_test_data_path(self, mock_variable_get):
        # Mock test data path variable
        expected_path = "/tmp/test_data"
        mock_variable_get.return_value = expected_path

        actual_path = test_data_path()
        self.assertEqual(expected_path, actual_path)

    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_blob_name(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            cwd = os.getcwd()
            mock_variable_get.side_effect = side_effect

            file_path = os.path.join(cwd, 'data/telescopes/transform/dag_id/dag_id_2021_03_01/file.txt')
            blob = blob_name(file_path)

            self.assertEqual('telescopes/dag_id/dag_id_2021_03_01/file.txt', blob)

    @patch("observatory.platform.utils.template_utils.upload_files_to_cloud_storage")
    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_upload_files_from_list(self, mock_variable_get, mock_upload_files):
        mock_variable_get.side_effect = side_effect
        files_list = ['/data/file1.txt',
                      '/data/file2.txt']
        blob_names = []
        for file_path in files_list:
            blob_names.append(blob_name(file_path))

        mock_upload_files.return_value = True
        success = upload_files_from_list(files_list, 'bucket_name')
        self.assertTrue(success)
        mock_upload_files.assert_called_once_with('bucket_name', blob_names, files_list)

        mock_upload_files.return_value = False
        with self.assertRaises(AirflowException):
            upload_files_from_list(files_list, 'bucket_name')

    def test_add_partition_date(self):
        list_of_dicts = [{'k1a': 'v2a'}, {'k1b': 'v2b'}, {'k1c': 'v2c'}]
        partition_date = datetime(2020, 1, 1)

        result = add_partition_date(list_of_dicts, partition_date)
        expected_result = [{'k1a': 'v2a', partition_field: partition_date.strftime('%Y-%m-%d')},
                           {'k1b': 'v2b', partition_field: partition_date.strftime('%Y-%m-%d')},
                           {'k1c': 'v2c', partition_field: partition_date.strftime('%Y-%m-%d')}]
        self.assertListEqual(expected_result, result)

        result = add_partition_date(list_of_dicts, partition_date, bigquery.TimePartitioningType.HOUR)
        expected_result = [{'k1a': 'v2a', partition_field: partition_date.isoformat()},
                           {'k1b': 'v2b', partition_field: partition_date.isoformat()},
                           {'k1c': 'v2c', partition_field: partition_date.isoformat()}]
        self.assertListEqual(expected_result, result)

        result = add_partition_date(list_of_dicts, partition_date, bigquery.TimePartitioningType.MONTH)
        expected_result = [{'k1a': 'v2a', partition_field: partition_date.strftime('%Y-%m-%d')},
                           {'k1b': 'v2b', partition_field: partition_date.strftime('%Y-%m-%d')},
                           {'k1c': 'v2c', partition_field: partition_date.strftime('%Y-%m-%d')}]
        self.assertListEqual(expected_result, result)

    def test_table_ids_from_path(self):
        transform_path = '/opt/observatory/data/telescopes/transform/telescope/2020_01_01-2020_02_01/telescope.jsonl.gz'
        main_table_id, partition_table_id = table_ids_from_path(transform_path)
        self.assertEqual('telescope', main_table_id)
        self.assertEqual('telescope_partitions', partition_table_id)

    def test_create_date_table_id(self):
        table_id = 'table'
        date = datetime(2020, 1, 1)
        time_type = bigquery.TimePartitioningType

        type_map = {time_type.HOUR: "%Y%m%d%H",
                    time_type.DAY: "%Y%m%d",
                    time_type.MONTH: "%Y%m",
                    time_type.YEAR: "%Y"}
        for partition_type, date_format in type_map.items():
            date_table_id = create_date_table_id(table_id, date, partition_type)
            orginal_table_id = date_table_id.split('$')[0]
            date_str = date_table_id.split('$')[1]

            self.assertEqual(table_id, orginal_table_id)
            self.assertIsInstance(datetime.strptime(date_str, date_format), datetime)

        with self.assertRaises(TypeError):
            create_date_table_id(table_id, date, 'error')

    @patch('observatory.platform.utils.template_utils.find_schema')
    @patch('observatory.platform.utils.template_utils.create_bigquery_dataset')
    @patch("airflow.models.variable.Variable.get")
    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_prepare_bq_load(self, mock_airflowvariable_get, mock_variable_get, mock_create_bigquery_dataset,
                             mock_find_schema):
        with CliRunner().isolated_filesystem():
            mock_airflowvariable_get.side_effect = side_effect
            mock_variable_get.side_effect = side_effect
            mock_find_schema.return_value = 'schema.json'

            telescope, release = setup(MockSnapshotTelescope)

            table_id, _ = table_ids_from_path(release.transform_files[0])
            project_id, bucket_name, data_location, schema_file_path = prepare_bq_load(telescope.dataset_id, table_id,
                                                                                       release.release_date,
                                                                                       telescope.schema_prefix,
                                                                                       telescope.schema_version,
                                                                                       telescope.dataset_description)

            self.assertEqual('project', project_id)
            self.assertEqual('transform-bucket', bucket_name)
            self.assertEqual('US', data_location)
            self.assertEqual('schema.json', schema_file_path)
            mock_find_schema.assert_called_once_with(schema_path(), table_id, release.release_date,
                                                     telescope.schema_prefix, telescope.schema_version)
            mock_create_bigquery_dataset.assert_called_once_with(project_id, telescope.dataset_id,
                                                                 data_location,
                                                                 telescope.dataset_description)

            mock_find_schema.return_value = None
            with self.assertRaises(SystemExit):
                prepare_bq_load(telescope.dataset_id, table_id, release.release_date, telescope.schema_prefix,
                                telescope.schema_version, telescope.dataset_description)

    @patch('observatory.platform.utils.template_utils.find_schema')
    @patch('observatory.platform.utils.template_utils.create_bigquery_dataset')
    @patch("airflow.models.variable.Variable.get")
    def test_prepare_bq_load_v2(self, mock_variable_get, mock_create_bigquery_dataset, mock_find_schema):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_find_schema.return_value = 'schema.json'

            telescope, release = setup(MockSnapshotTelescope)
            telescope.project_id = 'project_id'
            telescope.dataset_location = 'us'

            table_id, _ = table_ids_from_path(release.transform_files[0])

            schema_file_path = prepare_bq_load_v2(telescope.project_id, telescope.dataset_id,
                                                  telescope.dataset_location, table_id, release.release_date,
                                                  telescope.schema_prefix, telescope.schema_version,
                                                  telescope.dataset_description)

            self.assertEqual('schema.json', schema_file_path)
            mock_find_schema.assert_called_once_with(schema_path(), table_id, release.release_date,
                                                     telescope.schema_prefix, telescope.schema_version)
            mock_create_bigquery_dataset.assert_called_once_with(telescope.project_id, telescope.dataset_id,
                                                                 telescope.dataset_location,
                                                                 telescope.dataset_description)

            mock_find_schema.return_value = None
            with self.assertRaises(SystemExit):
                prepare_bq_load_v2(telescope.project_id, telescope.dataset_id, telescope.dataset_location, table_id,
                                   release.release_date, telescope.schema_prefix, telescope.schema_version,
                                   telescope.dataset_description)

    @patch('observatory.platform.utils.template_utils.load_bigquery_table')
    @patch('observatory.platform.utils.template_utils.prepare_bq_load')
    @patch("airflow.models.variable.Variable.get")
    def test_bq_load_shard(self, mock_variable_get, mock_prepare_bq_load, mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = (None, 'bucket_name', 'data_location', 'schema.json')
            mock_load_bigquery_table.return_value = True

            telescope, release = setup(MockSnapshotTelescope)

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)
                table_description = telescope.table_descriptions.get(table_id, '')

                bq_load_shard(release.release_date, transform_blob, telescope.dataset_id, table_id,
                              telescope.source_format,
                              prefix=telescope.schema_prefix, schema_version=telescope.schema_version,
                              dataset_description=telescope.dataset_description, table_description=table_description,
                              **telescope.load_bigquery_table_kwargs)

                mock_prepare_bq_load.assert_called_once_with(telescope.dataset_id, table_id, release.release_date,
                                                             telescope.schema_prefix, telescope.schema_version,
                                                             telescope.dataset_description)
                mock_load_bigquery_table.assert_called_once_with(
                    'gs://bucket_name/telescopes/dag_id/dag_id_2021_03_01/file.txt',
                    telescope.dataset_id,
                    'data_location', 'file20210301', 'schema.json',
                    telescope.source_format,
                    table_description=table_description)

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_load_shard(release.release_date, transform_blob, telescope.dataset_id, table_id,
                                  telescope.source_format,
                                  prefix=telescope.schema_prefix, schema_version=telescope.schema_version,
                                  dataset_description=telescope.dataset_description, table_description=table_description,
                                  **telescope.load_bigquery_table_kwargs)

    @patch('observatory.platform.utils.template_utils.load_bigquery_table')
    @patch('observatory.platform.utils.template_utils.prepare_bq_load_v2')
    @patch("airflow.models.variable.Variable.get")
    def test_bq_load_shard_v2(self, mock_variable_get, mock_prepare_bq_load, mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = 'schema.json'
            mock_load_bigquery_table.return_value = True

            telescope, release = setup(MockSnapshotTelescope)
            telescope.project_id = 'project_id'
            telescope.dataset_location = 'us'

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)

                bq_load_shard_v2(telescope.project_id, release.transform_bucket, transform_blob, telescope.dataset_id,
                                 telescope.dataset_location, table_id, release.release_date, telescope.source_format,
                                 prefix=telescope.schema_prefix, schema_version=telescope.schema_version,
                                 dataset_description=telescope.dataset_description,
                                 **telescope.load_bigquery_table_kwargs)

                mock_prepare_bq_load.assert_called_once_with(telescope.project_id, telescope.dataset_id,
                                                             telescope.dataset_location, table_id, release.release_date,
                                                             telescope.schema_prefix,
                                                             telescope.schema_version, telescope.dataset_description)
                mock_load_bigquery_table.assert_called_once_with(
                    'gs://transform-bucket/telescopes/dag_id/dag_id_2021_03_01/file.txt',
                    telescope.dataset_id,
                    telescope.dataset_location, 'file20210301', 'schema.json',
                    telescope.source_format,
                    project_id=telescope.project_id)

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_load_shard_v2(telescope.project_id, release.transform_bucket, transform_blob, telescope.dataset_id,
                                     telescope.dataset_location, table_id, release.release_date, telescope.source_format,
                                     prefix=telescope.schema_prefix, schema_version=telescope.schema_version,
                                     dataset_description=telescope.dataset_description,
                                     **telescope.load_bigquery_table_kwargs)

    @patch('observatory.platform.utils.template_utils.load_bigquery_table')
    @patch('observatory.platform.utils.template_utils.prepare_bq_load')
    @patch("airflow.models.variable.Variable.get")
    def test_bq_load_ingestion_partition(self, mock_variable_get, mock_prepare_bq_load, mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = (None, 'bucket_name', 'data_location', 'schema.json')
            mock_load_bigquery_table.return_value = True

            telescope, release = setup(MockStreamTelescope)

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_description = telescope.table_descriptions.get(main_table_id, '')
                bq_load_ingestion_partition(release.end_date, transform_blob, telescope.dataset_id, main_table_id,
                                            partition_table_id, telescope.source_format, telescope.schema_prefix,
                                            telescope.schema_version, telescope.dataset_description,
                                            table_description=table_description,
                                            **telescope.load_bigquery_table_kwargs)

                mock_prepare_bq_load.assert_called_once_with(telescope.dataset_id, main_table_id, release.end_date,
                                                             telescope.schema_prefix, telescope.schema_version,
                                                             telescope.dataset_description)
                date_table_id = create_date_table_id(partition_table_id, pendulum.today(),
                                                     bigquery.TimePartitioningType.DAY)
                mock_load_bigquery_table.assert_called_once_with(
                    'gs://bucket_name/telescopes/dag_id/2021_02_01-2021_03_01/file.txt',
                    telescope.dataset_id,
                    'data_location', date_table_id, 'schema.json',
                    telescope.source_format,
                    partition=True, partition_type=bigquery.table.TimePartitioningType.DAY,
                    table_description=table_description)

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_load_ingestion_partition(release.end_date, transform_blob, telescope.dataset_id, main_table_id,
                                                partition_table_id, telescope.source_format, telescope.schema_prefix,
                                                telescope.schema_version,
                                                telescope.dataset_description, table_description=table_description,
                                                **telescope.load_bigquery_table_kwargs)

    @patch('observatory.platform.utils.template_utils.load_bigquery_table')
    @patch('observatory.platform.utils.template_utils.prepare_bq_load_v2')
    @patch("airflow.models.variable.Variable.get")
    def test_bq_load_partition(self, mock_variable_get, mock_prepare_bq_load, mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = 'schema.json'
            mock_load_bigquery_table.return_value = True

            telescope, release = setup(MockSnapshotTelescope)
            telescope.project_id = 'project_id'
            telescope.dataset_location = 'us'

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)
                table_description = telescope.table_descriptions.get(table_id, '')

                bq_load_partition(telescope.project_id, release.transform_bucket, transform_blob, telescope.dataset_id,
                                  telescope.dataset_location, table_id, release.release_date,
                                  telescope.source_format,
                                  bigquery.table.TimePartitioningType.MONTH, prefix=telescope.schema_prefix,
                                  schema_version=telescope.schema_version,
                                  dataset_description=telescope.dataset_description,
                                  table_description=table_description,
                                  **telescope.load_bigquery_table_kwargs)

                mock_prepare_bq_load.assert_called_once_with(telescope.project_id, telescope.dataset_id,
                                                             telescope.dataset_location, table_id, release.release_date,
                                                             telescope.schema_prefix,
                                                             telescope.schema_version, telescope.dataset_description)
                mock_load_bigquery_table.assert_called_once_with(
                    'gs://transform-bucket/telescopes/dag_id/dag_id_2021_03_01/file.txt',
                    telescope.dataset_id,
                    telescope.dataset_location, 'file$202103', 'schema.json',
                    telescope.source_format,
                    partition=True, partition_field=partition_field,
                    partition_type=bigquery.table.TimePartitioningType.MONTH,
                    table_description=table_description)

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_load_partition(telescope.project_id, release.transform_bucket, transform_blob, telescope.dataset_id,
                                      telescope.dataset_location, table_id, release.release_date,
                                      telescope.source_format,
                                      bigquery.table.TimePartitioningType.MONTH, prefix=telescope.schema_prefix,
                                      schema_version=telescope.schema_version,
                                      dataset_description=telescope.dataset_description,
                                      table_description=table_description,
                                      **telescope.load_bigquery_table_kwargs)

    @patch('observatory.platform.utils.template_utils.run_bigquery_query')
    @patch("airflow.models.variable.Variable.get")
    def test_bq_delete_old(self, mock_variable_get, mock_run_bigquery_query):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect

            telescope, release = setup(MockStreamTelescope)
            start_date_str = release.start_date.strftime("%Y-%m-%d")
            end_date_str = (release.end_date + timedelta(days=1)).strftime("%Y-%m-%d")

            for transform_path in release.transform_files:
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                bq_delete_old(release.start_date, release.end_date, telescope.dataset_id, main_table_id,
                              partition_table_id,
                              telescope.merge_partition_field, telescope.updated_date_field)

                expected_query = "\n\nMERGE\n" \
                                 "  `{dataset_id}.{main_table}` M\n" \
                                 "USING\n" \
                                 "  (SELECT {merge_partition_field} AS id, {updated_date_field} AS date FROM `{dataset_id}.{partition_table}` WHERE _PARTITIONDATE >= '{start_date}' AND _PARTITIONDATE < '{end_date}') P\n" \
                                 "ON\n" \
                                 "  M.{merge_partition_field} = P.id\n" \
                                 "WHEN MATCHED AND M.{updated_date_field} <= P.date OR M.{updated_date_field} is null THEN\n" \
                                 "  DELETE".format(dataset_id=telescope.dataset_id, main_table=main_table_id,
                                                   partition_table=partition_table_id,
                                                   merge_partition_field=telescope.merge_partition_field,
                                                   updated_date_field=telescope.updated_date_field,
                                                   start_date=start_date_str,
                                                   end_date=end_date_str)
                mock_run_bigquery_query.assert_called_once_with(expected_query)

    @patch('observatory.platform.utils.template_utils.copy_bigquery_table')
    @patch('observatory.platform.utils.template_utils.prepare_bq_load')
    @patch("airflow.models.variable.Variable.get")
    def test_bq_append_from_partition(self, mock_variable_get, mock_prepare_bq_load, mock_copy_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = ('project_id', 'bucket_name', 'data_location', 'schema.json')
            mock_copy_bigquery_table.return_value = True

            telescope, release = setup(MockStreamTelescope)
            start_date = datetime(2020, 2, 1)
            end_date = datetime(2020, 2, 3)

            for transform_path in release.transform_files:
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                bq_append_from_partition(start_date, end_date, telescope.dataset_id, main_table_id,
                                         partition_table_id,
                                         telescope.schema_prefix, telescope.schema_version)

                mock_prepare_bq_load.assert_called_once_with(telescope.dataset_id, main_table_id, end_date,
                                                             telescope.schema_prefix, telescope.schema_version)
                source_table_ids = [f'project_id.{telescope.dataset_id}.{partition_table_id}$20200201',
                                    f'project_id.{telescope.dataset_id}.{partition_table_id}$20200202',
                                    f'project_id.{telescope.dataset_id}.{partition_table_id}$20200203',
                                    ]
                mock_copy_bigquery_table.assert_called_once_with(source_table_ids, f'project_id.{telescope.dataset_id}.'
                                                                                   f'{main_table_id}',
                                                                 'data_location',
                                                                 bigquery.WriteDisposition.WRITE_APPEND)

                mock_copy_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_append_from_partition(start_date, end_date, telescope.dataset_id, main_table_id,
                                             partition_table_id,
                                             telescope.schema_prefix, telescope.schema_version)

    @patch('observatory.platform.utils.template_utils.load_bigquery_table')
    @patch('observatory.platform.utils.template_utils.prepare_bq_load')
    @patch("airflow.models.variable.Variable.get")
    def test_bq_append_from_file(self, mock_variable_get, mock_prepare_bq_load,
                                 mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = ('project_id', 'bucket_name', 'data_location', 'schema.json')
            mock_load_bigquery_table.return_value = True

            telescope, release = setup(MockStreamTelescope)

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_description = telescope.table_descriptions.get(main_table_id, '')

                bq_append_from_file(release.end_date, transform_blob, telescope.dataset_id, main_table_id,
                                    telescope.source_format, telescope.schema_prefix, telescope.schema_version,
                                    telescope.dataset_description, table_description=table_description,
                                    **telescope.load_bigquery_table_kwargs)

                mock_prepare_bq_load.assert_called_once_with(telescope.dataset_id, main_table_id, release.end_date,
                                                             telescope.schema_prefix, telescope.schema_version,
                                                             telescope.dataset_description)
                mock_load_bigquery_table.assert_called_once_with(
                    'gs://bucket_name/telescopes/dag_id/2021_02_01-2021_03_01/file.txt',
                    telescope.dataset_id,
                    'data_location', 'file', 'schema.json',
                    telescope.source_format,
                    write_disposition='WRITE_APPEND', table_description=table_description)

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_append_from_file(release.end_date, transform_blob, telescope.dataset_id, main_table_id,
                                        telescope.source_format, telescope.schema_prefix, telescope.schema_version,
                                        telescope.dataset_description, table_description=table_description,
                                        **telescope.load_bigquery_table_kwargs)

    @patch("observatory.platform.utils.template_utils.create_slack_webhook")
    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_on_failure_callback(self, mock_variable_get, mock_create_slack_webhook):
        mock_variable_get.side_effect = ['develop', 'project_id', 'staging', 'project_id']
        mock_create_slack_webhook.return_value = Mock(spec=SlackWebhookHook)

        kwargs = {'exception': AirflowException("Exception message")}
        on_failure_callback(**kwargs)
        mock_create_slack_webhook.assert_not_called()

        on_failure_callback(**kwargs)
        mock_create_slack_webhook.assert_called_once_with('Task failed, exception:\n'
                                                          'airflow.exceptions.AirflowException: Exception message',
                                                          'project_id',
                                                          **kwargs)


def side_effect(arg):
    values = {
        'project_id': 'project',
        'download_bucket': 'download-bucket',
        'transform_bucket': 'transform-bucket',
        'data_path': os.path.join(os.getcwd(), 'data'),
        'data_location': 'US'
    }
    return values[arg]


@patch("observatory.platform.utils.template_utils.AirflowVariable.get")
@patch("airflow.models.variable.Variable.get")
def setup(telescope_class, mock_variable_get, mock_airflowvariable_get):
    mock_airflowvariable_get.side_effect = side_effect
    mock_variable_get.side_effect = side_effect

    telescope = telescope_class()
    release = telescope.make_release()
    folders = [release.download_folder, release.extract_folder, release.transform_folder]
    for folder in folders:
        file = os.path.join(folder, 'file.txt')
        with open(file, 'w') as f:
            f.write('data')

    return telescope, release
