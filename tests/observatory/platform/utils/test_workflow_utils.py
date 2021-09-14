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

import copy
import datetime
import json
import os
import shutil
import unittest
from functools import partial
from unittest.mock import Mock, patch
from urllib.parse import quote

import paramiko
import pendulum
import pysftp
from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from click.testing import CliRunner
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.file_utils import (
    gzip_file_crc,
    list_to_jsonl_gz,
    load_jsonl,
)
from observatory.platform.utils.test_utils import ObservatoryEnvironment
from observatory.platform.utils.test_utils import random_id
from observatory.platform.utils.test_utils import test_fixtures_path
from observatory.platform.utils.workflow_utils import (
    PeriodCount,
    ScheduleOptimiser,
    SubFolder,
    add_partition_date,
    blob_name,
    bq_append_from_file,
    bq_append_from_partition,
    bq_delete_old,
    bq_load_ingestion_partition,
    bq_load_partition,
    bq_load_shard,
    bq_load_shard_v2,
    create_date_table_id,
    make_dag_id,
    make_observatory_api,
    make_sftp_connection,
    make_workflow_sensor,
    normalized_schedule_interval,
    on_failure_callback,
    prepare_bq_load,
    prepare_bq_load_v2,
    table_ids_from_path,
    upload_files_from_list,
    workflow_path,
    make_release_date,
    make_table_name,
    fetch_dags_modules,
    fetch_dag_bag,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)
from observatory.platform.workflows.stream_telescope import (
    StreamRelease,
    StreamTelescope,
)

DEFAULT_SCHEMA_PATH = "/path/to/schemas"


class MockSnapshotTelescope(SnapshotTelescope):
    def __init__(
        self,
        dag_id: str = "dag_id",
        start_date: pendulum.DateTime = pendulum.datetime(2021, 1, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = random_id(),
        schema_folder: str = DEFAULT_SCHEMA_PATH,
        source_format: SourceFormat = SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_prefix: str = "prefix",
        schema_version: str = "1",
        dataset_description: str = "dataset_description",
    ):
        table_descriptions = {"file": "table description"}
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            source_format=source_format,
            schema_prefix=schema_prefix,
            schema_version=schema_version,
            dataset_description=dataset_description,
            table_descriptions=table_descriptions,
        )

    def make_release(self, **kwargs):
        return SnapshotRelease(self.dag_id, pendulum.datetime(2021, 3, 1), transform_files_regex="file.txt")


class MockStreamTelescope(StreamTelescope):
    def __init__(
        self,
        dag_id: str = "dag_id",
        start_date: pendulum.DateTime = pendulum.datetime(2021, 1, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = random_id(),
        merge_partition_field: str = "id",
        bq_merge_days: int = 7,
        schema_folder: str = DEFAULT_SCHEMA_PATH,
        source_format: SourceFormat = SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_prefix: str = "prefix",
        schema_version: str = "1",
        dataset_description: str = "dataset_description",
    ):
        table_descriptions = {"file": "table description"}
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            merge_partition_field,
            bq_merge_days,
            schema_folder,
            source_format=source_format,
            schema_prefix=schema_prefix,
            schema_version=schema_version,
            dataset_description=dataset_description,
            table_descriptions=table_descriptions,
        )

    def make_release(self, **kwargs):
        return StreamRelease(
            self.dag_id,
            pendulum.datetime(2021, 2, 1),
            pendulum.datetime(2021, 3, 1),
            False,
            transform_files_regex="file.txt",
        )


class TestTemplateUtils(unittest.TestCase):
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_workflow_path(self, mock_variable_get):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # The name of the telescope to create, data path and expected root folder
            data_path = "tests/observatory/platform/utils/data"
            telescope_name = "grid"
            root_path = os.path.join(data_path, "telescopes")

            # Test getting variable from env
            with patch.dict("os.environ", {f"AIRFLOW_VAR_{AirflowVars.DATA_PATH.upper()}": data_path}, clear=True):
                path = workflow_path(SubFolder.downloaded, telescope_name)
            expected = os.path.join(root_path, SubFolder.downloaded.value, telescope_name)
            self.assertEqual(expected, path)
            self.assertTrue(os.path.exists(path))
            self.assertEqual(0, mock_variable_get.call_count)

            # Mock getting home path
            mock_variable_get.return_value = data_path

            # Create subdir
            path_downloaded = workflow_path(SubFolder.downloaded, telescope_name)
            expected = os.path.join(root_path, SubFolder.downloaded.value, telescope_name)
            self.assertEqual(expected, path_downloaded)
            self.assertTrue(os.path.exists(path_downloaded))

            # Create subdir
            path_extracted = workflow_path(SubFolder.extracted, telescope_name)
            expected = os.path.join(root_path, SubFolder.extracted.value, telescope_name)
            self.assertEqual(expected, path_extracted)
            self.assertTrue(os.path.exists(path_extracted))

            # Create subdir
            path_transformed = workflow_path(SubFolder.transformed, telescope_name)
            expected = os.path.join(root_path, SubFolder.transformed.value, telescope_name)
            self.assertEqual(expected, path_transformed)
            self.assertTrue(os.path.exists(path_transformed))

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_blob_name(self, mock_variable_get):
        with CliRunner().isolated_filesystem() as t:
            data_path = os.path.join(t, "data")
            mock_variable_get.side_effect = partial(side_effect, data_path=data_path)

            file_path = os.path.join(data_path, "telescopes", "transform", "dag_id", "dag_id_2021_03_01", "file.txt")
            blob = blob_name(file_path)

            expected = os.path.join("telescopes", "dag_id", "dag_id_2021_03_01", "file.txt")
            self.assertEqual(expected, blob)

    @patch("observatory.platform.utils.workflow_utils.upload_files_to_cloud_storage")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_upload_files_from_list(self, mock_variable_get, mock_upload_files):

        with CliRunner().isolated_filesystem() as t:
            data_path = os.path.join(t, "data")
            mock_variable_get.side_effect = partial(side_effect, data_path=data_path)

            files_list = ["file1.txt", "file2.txt"]
            files_list = [os.path.join(data_path, f) for f in files_list]
            blob_names = []
            for file_path in files_list:
                blob_names.append(blob_name(file_path))

            mock_upload_files.return_value = True
            success = upload_files_from_list(files_list, "bucket_name")
            self.assertTrue(success)
            mock_upload_files.assert_called_once_with("bucket_name", blob_names, files_list)

            mock_upload_files.return_value = False
            with self.assertRaises(AirflowException):
                upload_files_from_list(files_list, "bucket_name")

    def test_add_partition_date(self):
        list_of_dicts = [{"k1a": "v2a"}, {"k1b": "v2b"}, {"k1c": "v2c"}]
        partition_date = datetime.datetime(2020, 1, 1)

        # Add partition date with default partition_type and partition_field
        result = add_partition_date(copy.deepcopy(list_of_dicts), partition_date)
        expected_result = [
            {"k1a": "v2a", "release_date": partition_date.strftime("%Y-%m-%d")},
            {"k1b": "v2b", "release_date": partition_date.strftime("%Y-%m-%d")},
            {"k1c": "v2c", "release_date": partition_date.strftime("%Y-%m-%d")},
        ]
        self.assertListEqual(expected_result, result)

        result = add_partition_date(
            copy.deepcopy(list_of_dicts), partition_date, bigquery.TimePartitioningType.HOUR, "partition_field"
        )
        expected_result = [
            {"k1a": "v2a", "partition_field": partition_date.isoformat()},
            {"k1b": "v2b", "partition_field": partition_date.isoformat()},
            {"k1c": "v2c", "partition_field": partition_date.isoformat()},
        ]
        self.assertListEqual(expected_result, result)

        result = add_partition_date(
            copy.deepcopy(list_of_dicts), partition_date, bigquery.TimePartitioningType.MONTH, "partition_field"
        )
        expected_result = [
            {"k1a": "v2a", "partition_field": partition_date.strftime("%Y-%m-%d")},
            {"k1b": "v2b", "partition_field": partition_date.strftime("%Y-%m-%d")},
            {"k1c": "v2c", "partition_field": partition_date.strftime("%Y-%m-%d")},
        ]
        self.assertListEqual(expected_result, result)

    def test_table_ids_from_path(self):
        transform_path = "/opt/observatory/data/telescopes/transform/telescope/2020_01_01-2020_02_01/telescope.jsonl.gz"
        main_table_id, partition_table_id = table_ids_from_path(transform_path)
        self.assertEqual("telescope", main_table_id)
        self.assertEqual("telescope_partitions", partition_table_id)

    def test_create_date_table_id(self):
        table_id = "table"
        date = datetime.datetime(2020, 1, 1)
        time_type = bigquery.TimePartitioningType

        type_map = {time_type.HOUR: "%Y%m%d%H", time_type.DAY: "%Y%m%d", time_type.MONTH: "%Y%m", time_type.YEAR: "%Y"}
        for partition_type, date_format in type_map.items():
            date_table_id = create_date_table_id(table_id, date, partition_type)
            orginal_table_id = date_table_id.split("$")[0]
            date_str = date_table_id.split("$")[1]

            self.assertEqual(table_id, orginal_table_id)
            self.assertIsInstance(datetime.datetime.strptime(date_str, date_format), datetime.datetime)

        with self.assertRaises(TypeError):
            create_date_table_id(table_id, date, "error")

    @patch("observatory.platform.utils.workflow_utils.find_schema")
    @patch("observatory.platform.utils.workflow_utils.create_bigquery_dataset")
    @patch("airflow.models.variable.Variable.get")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_prepare_bq_load(
        self, mock_airflowvariable_get, mock_variable_get, mock_create_bigquery_dataset, mock_find_schema
    ):
        with CliRunner().isolated_filesystem():
            mock_airflowvariable_get.side_effect = side_effect
            mock_variable_get.side_effect = side_effect
            mock_find_schema.return_value = "schema.json"
            schema_folder = DEFAULT_SCHEMA_PATH

            telescope, release = setup(MockSnapshotTelescope)

            table_id, _ = table_ids_from_path(release.transform_files[0])
            project_id, bucket_name, data_location, schema_file_path = prepare_bq_load(
                schema_folder,
                telescope.dataset_id,
                table_id,
                release.release_date,
                telescope.schema_prefix,
                telescope.schema_version,
                telescope.dataset_description,
            )

            self.assertEqual("project", project_id)
            self.assertEqual("transform-bucket", bucket_name)
            self.assertEqual("US", data_location)
            self.assertEqual("schema.json", schema_file_path)
            mock_find_schema.assert_called_once_with(
                schema_folder, table_id, release.release_date, telescope.schema_prefix, telescope.schema_version
            )
            mock_create_bigquery_dataset.assert_called_once_with(
                project_id, telescope.dataset_id, data_location, telescope.dataset_description
            )

            mock_find_schema.return_value = None
            with self.assertRaises(SystemExit):
                prepare_bq_load(
                    schema_folder,
                    telescope.dataset_id,
                    table_id,
                    release.release_date,
                    telescope.schema_prefix,
                    telescope.schema_version,
                    telescope.dataset_description,
                )

    @patch("observatory.platform.utils.workflow_utils.find_schema")
    @patch("observatory.platform.utils.workflow_utils.create_bigquery_dataset")
    @patch("airflow.models.variable.Variable.get")
    def test_prepare_bq_load_v2(self, mock_variable_get, mock_create_bigquery_dataset, mock_find_schema):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_find_schema.return_value = "schema.json"

            telescope, release = setup(MockSnapshotTelescope)
            telescope.project_id = "project_id"
            telescope.dataset_location = "us"
            schema_folder = DEFAULT_SCHEMA_PATH

            table_id, _ = table_ids_from_path(release.transform_files[0])

            schema_file_path = prepare_bq_load_v2(
                schema_folder,
                telescope.project_id,
                telescope.dataset_id,
                telescope.dataset_location,
                table_id,
                release.release_date,
                telescope.schema_prefix,
                telescope.schema_version,
                telescope.dataset_description,
            )

            self.assertEqual("schema.json", schema_file_path)
            mock_find_schema.assert_called_once_with(
                schema_folder, table_id, release.release_date, telescope.schema_prefix, telescope.schema_version
            )
            mock_create_bigquery_dataset.assert_called_once_with(
                telescope.project_id, telescope.dataset_id, telescope.dataset_location, telescope.dataset_description
            )

            mock_find_schema.return_value = None
            with self.assertRaises(SystemExit):
                prepare_bq_load_v2(
                    schema_folder,
                    telescope.project_id,
                    telescope.dataset_id,
                    telescope.dataset_location,
                    table_id,
                    release.release_date,
                    telescope.schema_prefix,
                    telescope.schema_version,
                    telescope.dataset_description,
                )

    @patch("observatory.platform.utils.workflow_utils.load_bigquery_table")
    @patch("observatory.platform.utils.workflow_utils.prepare_bq_load")
    @patch("airflow.models.variable.Variable.get")
    def test_bq_load_shard(self, mock_variable_get, mock_prepare_bq_load, mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = (None, "bucket_name", "data_location", "schema.json")
            mock_load_bigquery_table.return_value = True
            schema_folder = DEFAULT_SCHEMA_PATH

            telescope, release = setup(MockSnapshotTelescope)

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)
                table_description = telescope.table_descriptions.get(table_id, "")

                bq_load_shard(
                    schema_folder,
                    release.release_date,
                    transform_blob,
                    telescope.dataset_id,
                    table_id,
                    telescope.source_format,
                    prefix=telescope.schema_prefix,
                    schema_version=telescope.schema_version,
                    dataset_description=telescope.dataset_description,
                    table_description=table_description,
                    **telescope.load_bigquery_table_kwargs,
                )

                mock_prepare_bq_load.assert_called_once_with(
                    schema_folder,
                    telescope.dataset_id,
                    table_id,
                    release.release_date,
                    telescope.schema_prefix,
                    telescope.schema_version,
                    telescope.dataset_description,
                )
                mock_load_bigquery_table.assert_called_once_with(
                    "gs://bucket_name/telescopes/dag_id/dag_id_2021_03_01/file.txt",
                    telescope.dataset_id,
                    "data_location",
                    "file20210301",
                    "schema.json",
                    telescope.source_format,
                    table_description=table_description,
                )

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_load_shard(
                        schema_folder,
                        release.release_date,
                        transform_blob,
                        telescope.dataset_id,
                        table_id,
                        telescope.source_format,
                        prefix=telescope.schema_prefix,
                        schema_version=telescope.schema_version,
                        dataset_description=telescope.dataset_description,
                        table_description=table_description,
                        **telescope.load_bigquery_table_kwargs,
                    )

    @patch("observatory.platform.utils.workflow_utils.load_bigquery_table")
    @patch("observatory.platform.utils.workflow_utils.prepare_bq_load_v2")
    @patch("observatory.platform.utils.airflow_utils.Variable.get")
    def test_bq_load_shard_v2(self, mock_variable_get, mock_prepare_bq_load, mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = "schema.json"
            mock_load_bigquery_table.return_value = True
            schema_folder = DEFAULT_SCHEMA_PATH

            telescope, release = setup(MockSnapshotTelescope)
            telescope.project_id = "project_id"
            telescope.dataset_location = "us"

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)

                bq_load_shard_v2(
                    schema_folder,
                    telescope.project_id,
                    release.transform_bucket,
                    transform_blob,
                    telescope.dataset_id,
                    telescope.dataset_location,
                    table_id,
                    release.release_date,
                    telescope.source_format,
                    prefix=telescope.schema_prefix,
                    schema_version=telescope.schema_version,
                    dataset_description=telescope.dataset_description,
                    **telescope.load_bigquery_table_kwargs,
                )

                mock_prepare_bq_load.assert_called_once_with(
                    schema_folder,
                    telescope.project_id,
                    telescope.dataset_id,
                    telescope.dataset_location,
                    table_id,
                    release.release_date,
                    telescope.schema_prefix,
                    telescope.schema_version,
                    telescope.dataset_description,
                )
                mock_load_bigquery_table.assert_called_once_with(
                    "gs://transform-bucket/telescopes/dag_id/dag_id_2021_03_01/file.txt",
                    telescope.dataset_id,
                    telescope.dataset_location,
                    "file20210301",
                    "schema.json",
                    telescope.source_format,
                    project_id=telescope.project_id,
                )

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_load_shard_v2(
                        schema_folder,
                        telescope.project_id,
                        release.transform_bucket,
                        transform_blob,
                        telescope.dataset_id,
                        telescope.dataset_location,
                        table_id,
                        release.release_date,
                        telescope.source_format,
                        prefix=telescope.schema_prefix,
                        schema_version=telescope.schema_version,
                        dataset_description=telescope.dataset_description,
                        **telescope.load_bigquery_table_kwargs,
                    )

    @patch("observatory.platform.utils.workflow_utils.load_bigquery_table")
    @patch("observatory.platform.utils.workflow_utils.prepare_bq_load")
    @patch("airflow.models.variable.Variable.get")
    def test_bq_load_ingestion_partition(self, mock_variable_get, mock_prepare_bq_load, mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = (None, "bucket_name", "data_location", "schema.json")
            mock_load_bigquery_table.return_value = True
            schema_path = DEFAULT_SCHEMA_PATH

            telescope, release = setup(MockStreamTelescope)

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_description = telescope.table_descriptions.get(main_table_id, "")
                bq_load_ingestion_partition(
                    schema_path,
                    release.end_date,
                    transform_blob,
                    telescope.dataset_id,
                    main_table_id,
                    partition_table_id,
                    telescope.source_format,
                    telescope.schema_prefix,
                    telescope.schema_version,
                    telescope.dataset_description,
                    table_description=table_description,
                    **telescope.load_bigquery_table_kwargs,
                )

                mock_prepare_bq_load.assert_called_once_with(
                    schema_path,
                    telescope.dataset_id,
                    main_table_id,
                    release.end_date,
                    telescope.schema_prefix,
                    telescope.schema_version,
                    telescope.dataset_description,
                )
                date_table_id = create_date_table_id(
                    partition_table_id, pendulum.today(), bigquery.TimePartitioningType.DAY
                )
                mock_load_bigquery_table.assert_called_once_with(
                    "gs://bucket_name/telescopes/dag_id/2021_02_01-2021_03_01/file.txt",
                    telescope.dataset_id,
                    "data_location",
                    date_table_id,
                    "schema.json",
                    telescope.source_format,
                    partition=True,
                    partition_type=bigquery.table.TimePartitioningType.DAY,
                    table_description=table_description,
                )

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_load_ingestion_partition(
                        schema_path,
                        release.end_date,
                        transform_blob,
                        telescope.dataset_id,
                        main_table_id,
                        partition_table_id,
                        telescope.source_format,
                        telescope.schema_prefix,
                        telescope.schema_version,
                        telescope.dataset_description,
                        table_description=table_description,
                        **telescope.load_bigquery_table_kwargs,
                    )

    @patch("observatory.platform.utils.workflow_utils.load_bigquery_table")
    @patch("observatory.platform.utils.workflow_utils.prepare_bq_load_v2")
    @patch("observatory.platform.utils.airflow_utils.Variable.get")
    def test_bq_load_partition(self, mock_variable_get, mock_prepare_bq_load, mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = "schema.json"
            mock_load_bigquery_table.return_value = True
            schema_path = DEFAULT_SCHEMA_PATH

            telescope, release = setup(MockSnapshotTelescope)
            telescope.project_id = "project_id"
            telescope.dataset_location = "us"

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)
                table_description = telescope.table_descriptions.get(table_id, "")

                bq_load_partition(
                    schema_path,
                    telescope.project_id,
                    release.transform_bucket,
                    transform_blob,
                    telescope.dataset_id,
                    telescope.dataset_location,
                    table_id,
                    release.release_date,
                    telescope.source_format,
                    bigquery.table.TimePartitioningType.MONTH,
                    prefix=telescope.schema_prefix,
                    schema_version=telescope.schema_version,
                    dataset_description=telescope.dataset_description,
                    table_description=table_description,
                    **telescope.load_bigquery_table_kwargs,
                )

                mock_prepare_bq_load.assert_called_once_with(
                    schema_path,
                    telescope.project_id,
                    telescope.dataset_id,
                    telescope.dataset_location,
                    table_id,
                    release.release_date,
                    telescope.schema_prefix,
                    telescope.schema_version,
                    telescope.dataset_description,
                )
                mock_load_bigquery_table.assert_called_once_with(
                    "gs://transform-bucket/telescopes/dag_id/dag_id_2021_03_01/file.txt",
                    telescope.dataset_id,
                    telescope.dataset_location,
                    "file$202103",
                    "schema.json",
                    telescope.source_format,
                    partition=True,
                    partition_field="release_date",
                    project_id=telescope.project_id,
                    partition_type=bigquery.table.TimePartitioningType.MONTH,
                    table_description=table_description,
                )

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_load_partition(
                        schema_path,
                        telescope.project_id,
                        release.transform_bucket,
                        transform_blob,
                        telescope.dataset_id,
                        telescope.dataset_location,
                        table_id,
                        release.release_date,
                        telescope.source_format,
                        bigquery.table.TimePartitioningType.MONTH,
                        prefix=telescope.schema_prefix,
                        schema_version=telescope.schema_version,
                        dataset_description=telescope.dataset_description,
                        table_description=table_description,
                        **telescope.load_bigquery_table_kwargs,
                    )

    @patch("observatory.platform.utils.workflow_utils.run_bigquery_query")
    @patch("airflow.models.variable.Variable.get")
    def test_bq_delete_old(self, mock_variable_get, mock_run_bigquery_query):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect

            telescope, release = setup(MockStreamTelescope)
            start_date_str = release.start_date.strftime("%Y-%m-%d")
            end_date_str = release.end_date.strftime("%Y-%m-%d")

            for transform_path in release.transform_files:
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                bq_delete_old(
                    release.start_date,
                    release.end_date,
                    telescope.dataset_id,
                    main_table_id,
                    partition_table_id,
                    telescope.merge_partition_field,
                )

                expected_query = (
                    "\n\nMERGE\n"
                    "  `{dataset_id}.{main_table}` M\n"
                    "USING\n"
                    "  (SELECT {merge_partition_field} AS id FROM `{dataset_id}.{partition_table}` WHERE _PARTITIONDATE > '{start_date}' AND _PARTITIONDATE <= '{end_date}') P\n"
                    "ON\n"
                    "  M.{merge_partition_field} = P.id\n"
                    "WHEN MATCHED THEN\n"
                    "  DELETE".format(
                        dataset_id=telescope.dataset_id,
                        main_table=main_table_id,
                        partition_table=partition_table_id,
                        merge_partition_field=telescope.merge_partition_field,
                        start_date=start_date_str,
                        end_date=end_date_str,
                    )
                )
                mock_run_bigquery_query.assert_called_once_with(expected_query)

    @patch("observatory.platform.utils.workflow_utils.copy_bigquery_table")
    @patch("observatory.platform.utils.workflow_utils.prepare_bq_load")
    @patch("airflow.models.variable.Variable.get")
    def test_bq_append_from_partition(self, mock_variable_get, mock_prepare_bq_load, mock_copy_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = ("project_id", "bucket_name", "data_location", "schema.json")
            mock_copy_bigquery_table.return_value = True
            schema_path = DEFAULT_SCHEMA_PATH

            telescope, release = setup(MockStreamTelescope)
            start_date = pendulum.datetime(2020, 2, 1)
            end_date = pendulum.datetime(2020, 2, 3)

            for transform_path in release.transform_files:
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                bq_append_from_partition(
                    schema_path,
                    start_date,
                    end_date,
                    telescope.dataset_id,
                    main_table_id,
                    partition_table_id,
                    telescope.schema_prefix,
                    telescope.schema_version,
                )

                mock_prepare_bq_load.assert_called_once_with(
                    schema_path,
                    telescope.dataset_id,
                    main_table_id,
                    end_date,
                    telescope.schema_prefix,
                    telescope.schema_version,
                )
                source_table_ids = [
                    f"project_id.{telescope.dataset_id}.{partition_table_id}$20200202",
                    f"project_id.{telescope.dataset_id}.{partition_table_id}$20200203",
                ]
                mock_copy_bigquery_table.assert_called_once_with(
                    source_table_ids,
                    f"project_id.{telescope.dataset_id}." f"{main_table_id}",
                    "data_location",
                    bigquery.WriteDisposition.WRITE_APPEND,
                )

                mock_copy_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_append_from_partition(
                        schema_path,
                        start_date,
                        end_date,
                        telescope.dataset_id,
                        main_table_id,
                        partition_table_id,
                        telescope.schema_prefix,
                        telescope.schema_version,
                    )

    @patch("observatory.platform.utils.workflow_utils.load_bigquery_table")
    @patch("observatory.platform.utils.workflow_utils.prepare_bq_load")
    @patch("airflow.models.variable.Variable.get")
    def test_bq_append_from_file(self, mock_variable_get, mock_prepare_bq_load, mock_load_bigquery_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_prepare_bq_load.return_value = ("project_id", "bucket_name", "data_location", "schema.json")
            mock_load_bigquery_table.return_value = True
            schema_path = DEFAULT_SCHEMA_PATH

            telescope, release = setup(MockStreamTelescope)

            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_description = telescope.table_descriptions.get(main_table_id, "")

                bq_append_from_file(
                    schema_path,
                    release.end_date,
                    transform_blob,
                    telescope.dataset_id,
                    main_table_id,
                    telescope.source_format,
                    telescope.schema_prefix,
                    telescope.schema_version,
                    telescope.dataset_description,
                    table_description=table_description,
                    **telescope.load_bigquery_table_kwargs,
                )

                mock_prepare_bq_load.assert_called_once_with(
                    schema_path,
                    telescope.dataset_id,
                    main_table_id,
                    release.end_date,
                    telescope.schema_prefix,
                    telescope.schema_version,
                    telescope.dataset_description,
                )
                mock_load_bigquery_table.assert_called_once_with(
                    "gs://bucket_name/telescopes/dag_id/2021_02_01-2021_03_01/file.txt",
                    telescope.dataset_id,
                    "data_location",
                    "file",
                    "schema.json",
                    telescope.source_format,
                    write_disposition="WRITE_APPEND",
                    table_description=table_description,
                )

                mock_load_bigquery_table.return_value = False
                with self.assertRaises(AirflowException):
                    bq_append_from_file(
                        schema_path,
                        release.end_date,
                        transform_blob,
                        telescope.dataset_id,
                        main_table_id,
                        telescope.source_format,
                        telescope.schema_prefix,
                        telescope.schema_version,
                        telescope.dataset_description,
                        table_description=table_description,
                        **telescope.load_bigquery_table_kwargs,
                    )

    @patch("observatory.platform.utils.workflow_utils.create_slack_webhook")
    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_on_failure_callback(self, mock_variable_get, mock_create_slack_webhook):
        mock_variable_get.side_effect = ["develop", "project_id", "staging", "project_id"]
        mock_create_slack_webhook.return_value = Mock(spec=SlackWebhookHook)

        kwargs = {"exception": AirflowException("Exception message")}
        on_failure_callback(**kwargs)
        mock_create_slack_webhook.assert_not_called()

        on_failure_callback(**kwargs)
        mock_create_slack_webhook.assert_called_once_with(
            "Task failed, exception:\n" "airflow.exceptions.AirflowException: Exception message", "project_id", **kwargs
        )


def side_effect(arg, data_path: str = ""):
    values = {
        "project_id": "project",
        "download_bucket": "download-bucket",
        "transform_bucket": "transform-bucket",
        "data_path": data_path,
        "data_location": "US",
    }
    return values[arg]


@patch("observatory.platform.utils.workflow_utils.Variable.get")
@patch("airflow.models.variable.Variable.get")
def setup(telescope_class, mock_variable_get, mock_airflowvariable_get):
    mock_airflowvariable_get.side_effect = side_effect
    mock_variable_get.side_effect = side_effect

    telescope = telescope_class()
    release = telescope.make_release()
    folders = [release.download_folder, release.extract_folder, release.transform_folder]
    for folder in folders:
        file = os.path.join(folder, "file.txt")
        with open(file, "w") as f:
            f.write("data")

    return telescope, release


class TestWorkflowUtils(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestWorkflowUtils, self).__init__(*args, **kwargs)

    def test_normalized_schedule_interval(self):
        """Test normalized_schedule_interval"""
        schedule_intervals = [
            (None, None),
            ("@daily", "0 0 * * *"),
            ("@weekly", "0 0 * * 0"),
            ("@monthly", "0 0 1 * *"),
            ("@quarterly", "0 0 1 */3 *"),
            ("@yearly", "0 0 1 1 *"),
            ("@once", None),
            (datetime.timedelta(days=1), datetime.timedelta(days=1)),
        ]
        for test in schedule_intervals:
            schedule_interval = test[0]
            expected_n_schedule_interval = test[1]
            actual_n_schedule_interval = normalized_schedule_interval(schedule_interval)

            self.assertEqual(expected_n_schedule_interval, actual_n_schedule_interval)

    @patch.object(pysftp, "Connection")
    @patch("airflow.hooks.base_hook.BaseHook.get_connection")
    def test_make_sftp_connection(self, mock_airflow_conn, mock_pysftp_connection):
        """Test that sftp connection is initialized correctly"""

        # set up variables
        username = "username"
        password = "password"
        host = "host"
        host_key = quote(paramiko.RSAKey.generate(512).get_base64(), safe="")

        # mock airflow sftp service conn
        mock_airflow_conn.return_value = Connection(uri=f"ssh://{username}:{password}@{host}?host_key={host_key}")

        # run function
        sftp = make_sftp_connection()

        # confirm sftp server was initialised with correct username, password and cnopts
        call_args = mock_pysftp_connection.call_args

        self.assertEqual(1, len(call_args[0]))
        self.assertEqual(host, call_args[0][0])

        self.assertEqual(4, len(call_args[1]))
        self.assertEqual(username, call_args[1]["username"])
        self.assertEqual(password, call_args[1]["password"])
        self.assertIsInstance(call_args[1]["cnopts"], pysftp.CnOpts)
        self.assertIsNone(call_args[1]["port"])

    def test_make_dag_id(self):
        """Test make_dag_id"""

        expected_dag_id = "onix_curtin_press"
        dag_id = make_dag_id("onix", "Curtin Press")
        self.assertEqual(expected_dag_id, dag_id)

    def test_fetch_dags_modules(self):
        """Test fetch_dags_modules"""

        dags_module_names_val = '["academic_observatory_workflows.dags", "oaebu_workflows.dags"]'
        expected = ["academic_observatory_workflows.dags", "oaebu_workflows.dags"]
        env = ObservatoryEnvironment(enable_api=False, enable_elastic=False)
        with env.create():
            # Test when no variable set
            with self.assertRaises(KeyError):
                fetch_dags_modules()

            # Test when using an Airflow Variable exists
            env.add_variable(Variable(key="dags_module_names", val=dags_module_names_val))
            actual = fetch_dags_modules()
            self.assertEqual(expected, actual)

        with ObservatoryEnvironment(enable_api=False, enable_elastic=False).create():
            # Set environment variable
            new_env = env.new_env
            new_env["AIRFLOW_VAR_DAGS_MODULE_NAMES"] = dags_module_names_val
            os.environ.update(new_env)

            # Test when using an Airflow Variable set with an environment variable
            actual = fetch_dags_modules()
            self.assertEqual(expected, actual)

    def test_fetch_dag_bag(self):
        """Test fetch_dag_bag"""

        env = ObservatoryEnvironment(enable_api=False, enable_elastic=False)
        with env.create() as t:
            # No DAGs found
            dag_bag = fetch_dag_bag(t)
            self.assertEqual(0, len(dag_bag.dag_ids))

            # Bad DAG
            src = test_fixtures_path("utils", "bad_dag.py")
            shutil.copy(src, os.path.join(t, "dags.py"))
            with self.assertRaises(Exception):
                fetch_dag_bag(t)

            # Copy Good DAGs to folder
            src = test_fixtures_path("utils", "good_dag.py")
            shutil.copy(src, os.path.join(t, "dags.py"))

            # DAGs found
            expected_dag_ids = {"hello", "world"}
            dag_bag = fetch_dag_bag(t)
            actual_dag_ids = set(dag_bag.dag_ids)
            self.assertSetEqual(expected_dag_ids, actual_dag_ids)

    def test_make_release_date(self):
        """Test make_table_name"""

        next_execution_date = pendulum.datetime(2021, 11, 11)
        expected_release_date = pendulum.datetime(2021, 11, 10)
        actual_release_date = make_release_date(**{"next_execution_date": next_execution_date})
        self.assertEqual(expected_release_date, actual_release_date)

    @patch("observatory.platform.utils.workflow_utils.select_table_shard_dates")
    def test_make_table_name(self, mock_sel_table_suffixes):
        """Test make_table_name"""
        dt = pendulum.datetime(2021, 1, 1)
        mock_sel_table_suffixes.return_value = [dt]

        # Sharded
        expected_table_name = "hello20210101"
        actual_table_name = make_table_name(
            project_id="project_id", dataset_id="dataset_id", table_id="hello", end_date=dt, sharded=True
        )
        self.assertEqual(expected_table_name, actual_table_name)

        # Not sharded
        expected_table_name = "hello"
        actual_table_name = make_table_name(
            project_id="project_id", dataset_id="dataset_id", table_id="hello", end_date=dt, sharded=False
        )
        self.assertEqual(expected_table_name, actual_table_name)

    @patch("airflow.hooks.base_hook.BaseHook.get_connection")
    def test_make_observatory_api(self, mock_get_connection):
        """Test make_observatory_api"""

        conn_type = "http"
        host = "api.observatory.academy"
        api_key = "my_api_key"

        # No port
        mock_get_connection.return_value = Connection(uri=f"{conn_type}://:{api_key}@{host}")
        api = make_observatory_api()
        self.assertEqual(f"http://{host}", api.api_client.configuration.host)
        self.assertEqual(api_key, api.api_client.configuration.api_key["api_key"])

        # Port
        port = 8080
        mock_get_connection.return_value = Connection(uri=f"{conn_type}://:{api_key}@{host}:{port}")
        api = make_observatory_api()
        self.assertEqual(f"http://{host}:{port}", api.api_client.configuration.host)
        self.assertEqual(api_key, api.api_client.configuration.api_key["api_key"])

        # Assertion error: missing conn_type empty string
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(uri=f"://:{api_key}@{host}")
            make_observatory_api()

        # Assertion error: missing host empty string
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(uri=f"{conn_type}://:{api_key}@")
            make_observatory_api()

        # Assertion error: missing password empty string
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(uri=f"://:{api_key}@{host}")
            make_observatory_api()

        # Assertion error: missing conn_type None
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(password=api_key, host=host)
            make_observatory_api()

        # Assertion error: missing host None
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(conn_type=conn_type, password=api_key)
            make_observatory_api()

        # Assertion error: missing password None
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(host=host, password=api_key)
            make_observatory_api()

    def test_list_to_jsonl_gz(self):
        """Test writing list of dicts to jsonl.gz file"""
        list_of_dicts = [{"k1a": "v1a", "k2a": "v2a"}, {"k1b": "v1b", "k2b": "v2b"}]
        file_path = "list.jsonl.gz"
        expected_file_hash = "e608cfeb"
        with CliRunner().isolated_filesystem():
            list_to_jsonl_gz(file_path, list_of_dicts)
            self.assertTrue(os.path.isfile(file_path))
            actual_file_hash = gzip_file_crc(file_path)
            self.assertEqual(expected_file_hash, actual_file_hash)

    def test_load_jsonl(self):
        """Test loading json lines files"""

        with CliRunner().isolated_filesystem() as t:
            expected_records = [
                {"name": "Elon Musk"},
                {"name": "Jeff Bezos"},
                {"name": "Peter Beck"},
                {"name": "Richard Branson"},
            ]
            file_path = os.path.join(t, "test.json")
            with open(file_path, mode="w") as f:
                for record in expected_records:
                    f.write(f"{json.dumps(record)}\n")

            actual_records = load_jsonl(file_path)
            self.assertListEqual(expected_records, actual_records)


class TestMakeTelescopeSensor(unittest.TestCase):
    """Test the external task sensor creation."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    class Organisation:
        def __init__(self):
            self.name = "test"

    class Response:
        def __init__(self):
            self.organisation = TestMakeTelescopeSensor.Organisation()

    def test_make_telescope_sensor(self):
        telescope = TestMakeTelescopeSensor.Response()
        sensor = make_workflow_sensor(telescope.organisation.name, "dag_prefix")
        self.assertEqual(sensor.task_id, "dag_prefix_test_sensor")
        self.assertEqual(sensor.mode, "reschedule")
        self.assertEqual(sensor.external_dag_id, "dag_prefix_test")


class TestScheduleOptimiser(unittest.TestCase):
    """Test schedule optimiser that minimises API calls."""

    def __init__(self, *args, **kwargs):
        super(TestScheduleOptimiser, self).__init__(*args, **kwargs)

        self.max_per_call = 2
        self.max_per_query = 10

        self.historic_counts_trivial = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 2),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 3),
        ]

    def test_get_num_calls(self):
        """Test the num_call calculation."""

        num_results = 10
        max_per_call = 2
        calls = ScheduleOptimiser.get_num_calls(num_results, max_per_call)
        self.assertEqual(calls, 5)

        max_per_call = 3
        calls = ScheduleOptimiser.get_num_calls(num_results, max_per_call)
        self.assertEqual(calls, 4)

    def test_extract_schedule(self):
        """Test schedule extraction from solution."""

        moves = [0, 1, 2, 3]
        schedule = ScheduleOptimiser.extract_schedule(self.historic_counts_trivial, moves)

        for i in range(len(schedule), 0, -1):
            self.assertEqual(schedule[i - 1].start.month, i)

        moves = [0, 1, 2, 1]
        schedule = ScheduleOptimiser.extract_schedule(self.historic_counts_trivial, moves)
        self.assertEqual(len(schedule), 2)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 1)
        self.assertEqual(schedule[1].start.month, 2)
        self.assertEqual(schedule[1].end.month, 4)

    def test_optimise_leading_zeros(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 21)), 1),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 1)
        self.assertEqual(schedule[0].start, pendulum.date(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.date(1000, 4, 21))
        self.assertEqual(min_calls, 1)
        self.assertEqual(len(schedule), 1)

    def test_optimise_leading_zeros2(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 1)
        self.assertEqual(min_calls, 0)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 1)

    def test_optimise_leading_trivial(self):
        schedule, min_calls = ScheduleOptimiser.optimise(
            self.max_per_call, self.max_per_query, self.historic_counts_trivial
        )
        self.assertEqual(len(schedule), 1)
        self.assertEqual(min_calls, 3)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 4)

    def test_optimise_historic_counts_case1(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 10),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 2),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 3),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 2)
        self.assertEqual(min_calls, 8)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 1)
        self.assertEqual(schedule[1].start.month, 2)
        self.assertEqual(schedule[1].end.month, 4)

    def test_optimise_historic_counts_case2(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 5),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 6),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 10),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 5, 1), end=pendulum.date(1000, 5, 1)), 2),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 4)  # Naive is 5
        self.assertEqual(min_calls, 12)  # Naive is 12
        self.assertEqual(schedule[0].start, pendulum.date(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.date(1000, 1, 1))
        self.assertEqual(schedule[1].start, pendulum.date(1000, 2, 1))
        self.assertEqual(schedule[1].end, pendulum.date(1000, 2, 1))
        self.assertEqual(schedule[2].start, pendulum.date(1000, 3, 1))
        self.assertEqual(schedule[2].end, pendulum.date(1000, 4, 1))
        self.assertEqual(schedule[3].start, pendulum.date(1000, 5, 1))
        self.assertEqual(schedule[3].end, pendulum.date(1000, 5, 1))

    def test_optimise_historic_counts_case3(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 5, 1), end=pendulum.date(1000, 5, 1)), 2),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 1)  # Naive is 5
        self.assertEqual(min_calls, 3)  # Naive is 5
        self.assertEqual(schedule[0].start, pendulum.date(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.date(1000, 5, 1))

    def test_optimise_historic_counts_case4(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 3),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 3),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 3),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 5, 1), end=pendulum.date(1000, 5, 1)), 3),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 2)  # Naive is 5
        self.assertEqual(min_calls, 7)  # Naive is 13
        self.assertEqual(schedule[0].start, pendulum.date(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.date(1000, 1, 1))
        self.assertEqual(schedule[1].start, pendulum.date(1000, 2, 1))
        self.assertEqual(schedule[1].end, pendulum.date(1000, 5, 1))
