# Copyright 2021 Curtin University
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
# limitations under the License.Ï

# Author: Aniek Roelofs, Tuan Chien

import json
import os
import shutil
from glob import glob
from typing import List, Tuple
from unittest.mock import MagicMock, call, patch

import jsonlines
import pendulum
from airflow.utils.state import State
from click.testing import CliRunner
from google.cloud.bigquery import SourceFormat, TimePartitioningType

from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.table_type import TableType
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.platform.utils.release_utils import get_dataset_releases, get_latest_dataset_release
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    test_fixtures_path,
)
from observatory.platform.utils.workflow_utils import (
    batch_blob_name,
    bigquery_sharded_table_id,
    blob_name,
    create_date_table_id,
    table_ids_from_path,
)
from observatory.platform.workflows.stream_telescope import (
    StreamRelease,
    StreamTelescope,
)

DEFAULT_SCHEMA_PATH = "/path/to/schemas"


class MyTestStreamTelescope(StreamTelescope):
    """
    Generic Workflow telescope for running tasks.
    """

    DAG_ID = "unit_test_stream"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 1, 1),
        schedule_interval: str = "@weekly",
        dataset_id: str = "dataset_id",
        merge_partition_field: str = "id",
        schema_folder: str = DEFAULT_SCHEMA_PATH,
        source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_prefix: str = "prefix",
        schema_version: str = "1",
        dataset_description: str = "dataset_description",
        batch_load: bool = False,
        workflow_id: int = 1,
        dataset_type_id=DAG_ID,
    ):
        table_descriptions = {
            "1": "table description 1",
            "2": "table description 2",
            self.DAG_ID: "table description batch",
        }
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            merge_partition_field,
            schema_folder,
            source_format=source_format,
            schema_prefix=schema_prefix,
            schema_version=schema_version,
            dataset_description=dataset_description,
            table_descriptions=table_descriptions,
            batch_load=batch_load,
            workflow_id=workflow_id,
            dataset_type_id=dataset_type_id,
        )

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.set_release_info)
        self.add_task_chain(
            [
                self.transform,
                self.upload_transformed,
                self.bq_load_partition,
                self.bq_delete_old,
                self.bq_append_new,
                self.cleanup,
                self.add_new_dataset_releases,
            ]
        )

        self.file_content = {
            "run1": {
                "file1": [
                    {"id": "id1", "source": "file1", "description": "This row should stay unchanged"},
                    {"id": "id2", "source": "file1", "description": "This row should be deleted"},
                ],
                "file2": [
                    {"id": "id4", "source": "file1", "description": "This row should stay unchanged"},
                    {"id": "id5", "source": "file1", "description": "This row should be deleted"},
                ],
            },
            "run2": {
                "file1": [
                    {"id": "id2", "source": "file2", "description": "This row was updated from file 2"},
                    {"id": "id3", "source": "file2", "description": "This row is new from file 2"},
                ],
                "file2": [
                    {"id": "id5", "source": "file2", "description": "This row was updated from file 2"},
                    {"id": "id6", "source": "file2", "description": "This row is new from file 2"},
                ],
            },
        }

    def make_release(self, **kwargs) -> StreamRelease:
        """Make a Release instance

        :param kwargs: The context passed from the PythonOperator.
        :return: StreamRelease instance.
        """
        start_date, end_date, first_release = self.get_release_info(**kwargs)
        release = StreamRelease(self.dag_id, start_date, end_date, first_release)
        return release

    def set_release_info(self, **kwargs):
        """Set release info on the telescope"""
        self._start_date, self._end_date, self._first_release = self.get_release_info(**kwargs)
        return True

    def transform(self, release: StreamRelease, **kwargs):
        """Create 2 transform files by writing file_content to a jsonl file.

        :param release: a StreamRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        run = "run1" if release.first_release else "run2"
        for file in ["file1", "file2"]:
            dst_path = os.path.join(release.transform_folder, f"stream_telescope_{file}.jsonl")
            with jsonlines.open(dst_path, "w") as f:
                content = self.file_content[run][file]
                f.write_all(content)


class TestStreamTelescope(ObservatoryTestCase):
    """Tests the StreamTelescope."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestStreamTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        # First release, append from file and don't update main table
        self.run1 = {
            "date": pendulum.datetime(year=2020, month=7, day=27),
            "first_release": True,
            "expected_end_date": pendulum.datetime(year=2020, month=7, day=26),
        }

        # Second release, load partition and update main table
        self.run2 = {
            "date": pendulum.datetime(year=2020, month=10, day=5),
            "first_release": False,
            "expected_end_date": pendulum.datetime(year=2020, month=10, day=4),
        }

        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"
        self.workflow = 1

    def setup_api(self):
        org = Organisation(name=self.org_name)
        result = self.api.put_organisation(org)
        self.assertIsInstance(result, Organisation)

        tele_type = WorkflowType(type_id="tele_type", name="My Telescope")
        result = self.api.put_workflow_type(tele_type)
        self.assertIsInstance(result, WorkflowType)

        telescope = Workflow(organisation=Organisation(id=1), workflow_type=WorkflowType(id=1))
        result = self.api.put_workflow(telescope)
        self.assertIsInstance(result, Workflow)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            name="My dataset type",
            type_id=MyTestStreamTelescope.DAG_ID,
            table_type=TableType(id=1),
        )

        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="My dataset",
            service="bigquery",
            address="project.dataset.table",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        result = self.api.put_dataset(dataset)
        self.assertIsInstance(result, Dataset)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    @patch("observatory.platform.utils.workflow_utils.find_schema")
    def test_telescope(self, mock_find_schema, m_makeapi):
        """Test the telescope end to end and that all bq load tasks run as expected.

        :return: None.
        """
        # Mock find schema, because schema is in fixtures directory
        mock_find_schema.return_value = os.path.join(test_fixtures_path("workflows"), "stream_telescope_schema.json")
        m_makeapi.return_value = self.api

        # Setup Telescopes, first one without batch_load and second one with batch_load
        batch_load_settings = [True, False]
        for batch_load in batch_load_settings:
            with self.subTest(batch_load=batch_load):
                # Setup Observatory environment
                env = ObservatoryEnvironment(
                    self.project_id, self.data_location, api_host=self.host, api_port=self.port
                )
                dataset_id = env.add_dataset()

                telescope = MyTestStreamTelescope(dataset_id=dataset_id, batch_load=batch_load, workflow_id=1)
                dag = telescope.make_dag()
                release = None

                # Create the Observatory environment and run tests
                with env.create(task_logging=True):
                    self.setup_api()
                    for run in [self.run1, self.run2]:
                        with env.create_dag_run(dag, run["date"]):
                            # Test that all dependencies are specified: no error should be thrown
                            ti = env.run_task(telescope.check_dependencies.__name__)
                            self.assertEqual("success", ti.state)

                            # Set release info data for testing
                            ti = env.run_task(telescope.set_release_info.__name__)
                            self.assertEqual("success", ti.state)

                            self.assertEqual(run["first_release"], telescope._first_release)
                            if run == self.run1:
                                self.assertEqual(dag.default_args["start_date"], telescope._start_date)
                                self.assertEqual(run["expected_end_date"], telescope._end_date)
                            else:
                                self.assertEqual(pendulum.instance(release.end_date), telescope._start_date)
                                self.assertEqual(run["expected_end_date"], telescope._end_date)

                            # Use release info for other tasks
                            release = StreamRelease(
                                telescope.dag_id,
                                telescope._start_date,
                                telescope._end_date,
                                telescope._first_release,
                            )

                            ti = env.run_task(telescope.transform.__name__)
                            self.assertEqual("success", ti.state)
                            self.assertEqual(2, len(release.transform_files))

                            # Test that files upload is called
                            env.run_task(telescope.upload_transformed.__name__)
                            for file in release.transform_files:
                                self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                            # Get bq_load_info
                            bq_load_info = telescope.get_bq_load_info(release)
                            if batch_load:
                                transform_blob = batch_blob_name(release.transform_folder)
                                main_table_id, partition_table_id = (telescope.dag_id, f"{telescope.dag_id}_partitions")
                                self.assertListEqual(
                                    [(transform_blob, main_table_id, partition_table_id)], bq_load_info
                                )
                            else:
                                expected_bq_info = []
                                for transform_path in sorted(release.transform_files):
                                    transform_blob = blob_name(transform_path)
                                    main_table_id, partition_table_id = table_ids_from_path(transform_path)
                                    expected_bq_info.append((transform_blob, main_table_id, partition_table_id))
                                self.assertEqual(expected_bq_info, bq_load_info)

                            # Test that data is loaded into partition table
                            ti = env.run_task(telescope.bq_load_partition.__name__)
                            self.assertEqual(State.SUCCESS, ti.state)
                            if run != self.run1:
                                for _, _, partition_table_id in bq_load_info:
                                    partition_table_id = create_date_table_id(
                                        partition_table_id, release.end_date, TimePartitioningType.DAY
                                    )
                                    table_id = f"{self.project_id}.{dataset_id}.{partition_table_id}"
                                    if batch_load:
                                        expected_content = (
                                            telescope.file_content["run2"]["file1"]
                                            + telescope.file_content["run2"]["file2"]
                                        )
                                    else:
                                        if "file1" in table_id:
                                            expected_content = telescope.file_content["run2"]["file1"]
                                        else:
                                            expected_content = telescope.file_content["run2"]["file2"]
                                    self.assert_table_content(table_id, expected_content)

                            # Test that row(s) is deleted from the main table in the second run
                            with patch("observatory.platform.utils.gc_utils.bq_query_bytes_daily_limit_check"):
                                ti = env.run_task(telescope.bq_delete_old.__name__)
                                self.assertEqual(State.SUCCESS, ti.state)

                            if run != self.run1:
                                for _, main_table_id, _ in bq_load_info:
                                    table_id = f"{self.project_id}.{dataset_id}.{main_table_id}"
                                    if batch_load:
                                        expected_content = [
                                            telescope.file_content["run1"]["file1"][0],
                                            telescope.file_content["run1"]["file2"][0],
                                        ]
                                    else:
                                        if "file1" in table_id:
                                            expected_content = [telescope.file_content["run1"]["file1"][0]]
                                        else:
                                            expected_content = [telescope.file_content["run1"]["file2"][0]]
                                    self.assert_table_content(table_id, expected_content)

                            # Test that data is loaded from a file in the first run and two rows are appended to the
                            # main table in the second run
                            env.run_task(telescope.bq_append_new.__name__)
                            for _, main_table_id, _ in bq_load_info:
                                table_id = f"{self.project_id}.{dataset_id}.{main_table_id}"
                                if run == self.run1:
                                    if batch_load:
                                        expected_content = (
                                            telescope.file_content["run1"]["file1"]
                                            + telescope.file_content["run1"]["file2"]
                                        )
                                    else:
                                        if "file1" in table_id:
                                            expected_content = telescope.file_content["run1"]["file1"]
                                        else:
                                            expected_content = telescope.file_content["run1"]["file2"]
                                else:
                                    if batch_load:
                                        expected_content = [
                                            telescope.file_content["run1"]["file1"][0],
                                            telescope.file_content["run2"]["file1"][0],
                                            telescope.file_content["run2"]["file1"][1],
                                            telescope.file_content["run1"]["file2"][0],
                                            telescope.file_content["run2"]["file2"][0],
                                            telescope.file_content["run2"]["file2"][1],
                                        ]
                                    else:
                                        if "file1" in table_id:
                                            expected_content = [
                                                telescope.file_content["run1"]["file1"][0],
                                                telescope.file_content["run2"]["file1"][0],
                                                telescope.file_content["run2"]["file1"][1],
                                            ]
                                        else:
                                            expected_content = [
                                                telescope.file_content["run1"]["file2"][0],
                                                telescope.file_content["run2"]["file2"][0],
                                                telescope.file_content["run2"]["file2"][1],
                                            ]
                                self.assert_table_content(table_id, expected_content)

                            # Test that all telescope data deleted
                            download_folder, extract_folder, transform_folder = (
                                release.download_folder,
                                release.extract_folder,
                                release.transform_folder,
                            )
                            env.run_task(telescope.cleanup.__name__)
                            self.assert_cleanup(download_folder, extract_folder, transform_folder)

                            # add_dataset_release_task
                            dataset_releases = get_dataset_releases(dataset_id=1)
                            if release.first_release:
                                self.assertEqual(len(dataset_releases), 0)
                            else:
                                self.assertEqual(len(dataset_releases), 1)

                            ti = env.run_task("add_new_dataset_releases")
                            self.assertEqual(ti.state, State.SUCCESS)
                            dataset_releases = get_dataset_releases(dataset_id=1)

                            if release.first_release:
                                self.assertEqual(len(dataset_releases), 1)
                            else:
                                self.assertEqual(len(dataset_releases), 2)

                            latest_release = get_latest_dataset_release(dataset_releases)
                            self.assertEqual(pendulum.instance(latest_release.start_date), release.start_date)
                            self.assertEqual(pendulum.instance(latest_release.end_date), release.end_date)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    @patch("observatory.platform.workflows.stream_telescope.bq_append_from_file")
    @patch("observatory.platform.workflows.stream_telescope.bq_append_from_partition")
    @patch("observatory.platform.workflows.stream_telescope.bq_delete_old")
    @patch("observatory.platform.workflows.stream_telescope.bq_load_ingestion_partition")
    @patch("observatory.platform.workflows.stream_telescope.upload_files_from_list")
    def test_telescope_function_calls(
        self,
        upload_files_from_list,
        bq_load_ingestion_partition,
        bq_delete_old,
        bq_append_from_partition,
        bq_append_from_file,
        m_makeapi,
    ):
        """Test the telescope end to end and that all functions are called with the correct params.

        :return: None.
        """

        m_makeapi.return_value = self.api

        # Setup Telescopes, first one without batch_load and second one with batch_load
        batch_load_settings = [False, True]
        for batch_load in batch_load_settings:
            with self.subTest(batch_load=batch_load):
                # Setup Observatory environment
                env = ObservatoryEnvironment(
                    self.project_id, self.data_location, api_host=self.host, api_port=self.port
                )
                dataset_id = env.add_dataset()

                telescope = MyTestStreamTelescope(dataset_id=dataset_id, batch_load=batch_load)
                dag = telescope.make_dag()
                release = None

                # Create the Observatory environment and run tests
                with env.create(task_logging=True):
                    self.setup_api()
                    for run in [self.run1, self.run2]:
                        with env.create_dag_run(dag, run["date"]):
                            # Test that all dependencies are specified: no error should be thrown
                            ti = env.run_task(telescope.check_dependencies.__name__)
                            self.assertEqual(ti.state, "success")

                            # Set release info data for testing
                            ti = env.run_task(telescope.set_release_info.__name__)
                            self.assertEqual(ti.state, "success")

                            self.assertEqual(run["first_release"], telescope._first_release)
                            if run == self.run1:
                                self.assertEqual(dag.default_args["start_date"], telescope._start_date)
                                self.assertEqual(run["expected_end_date"], telescope._end_date)
                            else:
                                self.assertEqual(release.end_date, telescope._start_date)
                                self.assertEqual(run["expected_end_date"], telescope._end_date)

                            # Use release info for other tasks
                            release = StreamRelease(
                                telescope.dag_id,
                                telescope._start_date,
                                telescope._end_date,
                                telescope._first_release,
                            )

                            env.run_task(telescope.transform.__name__)
                            self.assertEqual(2, len(release.transform_files))

                            # Test that files upload is called
                            env.run_task(telescope.upload_transformed.__name__)
                            upload_files_from_list.assert_called_with(release.transform_files, release.transform_bucket)

                            # Test that bq load info for bq load functions is as expected
                            bq_load_info = telescope.get_bq_load_info(release)
                            if batch_load:
                                transform_blob = batch_blob_name(release.transform_folder)
                                main_table_id, partition_table_id = (telescope.dag_id, f"{telescope.dag_id}_partitions")
                                self.assertListEqual(
                                    [(transform_blob, main_table_id, partition_table_id)], bq_load_info
                                )
                            else:
                                expected_bq_info = []
                                for transform_path in sorted(release.transform_files):
                                    transform_blob = blob_name(transform_path)
                                    main_table_id, partition_table_id = table_ids_from_path(transform_path)
                                    expected_bq_info.append((transform_blob, main_table_id, partition_table_id))
                                self.assertEqual(expected_bq_info, bq_load_info)

                            # Test whether the correct bq load functions are called for different runs
                            ti = env.run_task(telescope.bq_load_partition.__name__)
                            self.assertEqual(State.SUCCESS, ti.state)
                            if run == self.run1:
                                self.assertEqual(0, bq_load_ingestion_partition.call_count)
                            else:
                                self.assertEqual(len(bq_load_info), bq_load_ingestion_partition.call_count)
                                expected_calls = []
                                for transform_blob, main_table_id, partition_table_id in bq_load_info:
                                    table_description = telescope.table_descriptions.get(main_table_id, "")
                                    expected_calls.append(
                                        call(
                                            telescope.schema_folder,
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
                                    )
                                    bq_load_ingestion_partition.assert_has_calls(expected_calls, any_order=True)
                                self.assertEqual("success", ti.state)

                            # Test whether the correct bq delete functions are called for different runs
                            ti = env.run_task(telescope.bq_delete_old.__name__)
                            self.assertEqual(State.SUCCESS, ti.state)
                            if run == self.run1:
                                # First release the task is run but does not execute the bq_delete_old function
                                self.assertEqual(0, bq_delete_old.call_count)
                            else:
                                self.assertEqual(len(bq_load_info), bq_delete_old.call_count)
                                expected_calls = []
                                for _, main_table_id, partition_table_id in bq_load_info:
                                    expected_calls.append(
                                        call(
                                            release.end_date,
                                            telescope.dataset_id,
                                            main_table_id,
                                            partition_table_id,
                                            telescope.merge_partition_field,
                                            bytes_budget=None,
                                            project_id=self.project_id,
                                        )
                                    )
                                bq_delete_old.assert_has_calls(expected_calls, any_order=True)
                                self.assertEqual("success", ti.state)

                            # Test whether the correct bq append functions are called for different runs
                            ti = env.run_task(telescope.bq_append_new.__name__)
                            if run == self.run1:
                                self.assertEqual(len(bq_load_info), bq_append_from_file.call_count)
                                expected_calls = []
                                for transform_blob, main_table_id, partition_table_id in bq_load_info:
                                    table_description = telescope.table_descriptions.get(main_table_id, "")
                                    expected_calls.append(
                                        call(
                                            telescope.schema_folder,
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
                                    )
                                bq_append_from_file.assert_has_calls(expected_calls, any_order=True)
                                self.assertEqual("success", ti.state)
                            else:
                                self.assertEqual(len(bq_load_info), bq_append_from_partition.call_count)

                                expected_calls = []
                                for _, main_table_id, partition_table_id in bq_load_info:
                                    expected_calls.append(
                                        call(
                                            release.end_date,
                                            telescope.dataset_id,
                                            main_table_id,
                                            partition_table_id,
                                        )
                                    )
                                bq_append_from_partition.assert_has_calls(expected_calls, any_order=True)
                                self.assertEqual("success", ti.state)
                            # Test that all telescope data deleted
                            download_folder, extract_folder, transform_folder = (
                                release.download_folder,
                                release.extract_folder,
                                release.transform_folder,
                            )
                            env.run_task(telescope.cleanup.__name__)
                            self.assert_cleanup(download_folder, extract_folder, transform_folder)

                            # Reset mocked functions
                            for mocked_function in [
                                upload_files_from_list,
                                bq_load_ingestion_partition,
                                bq_delete_old,
                                bq_append_from_partition,
                                bq_append_from_file,
                            ]:
                                mocked_function.reset_mock()

                            # add_dataset_release_task
                            dataset_releases = get_dataset_releases(dataset_id=1)
                            if release.first_release:
                                self.assertEqual(len(dataset_releases), 0)
                            else:
                                self.assertEqual(len(dataset_releases), 1)

                            ti = env.run_task("add_new_dataset_releases")
                            self.assertEqual(ti.state, State.SUCCESS)
                            dataset_releases = get_dataset_releases(dataset_id=1)

                            if release.first_release:
                                self.assertEqual(len(dataset_releases), 1)
                            else:
                                self.assertEqual(len(dataset_releases), 2)

                            latest_release = get_latest_dataset_release(dataset_releases)
                            self.assertEqual(pendulum.instance(latest_release.start_date), release.start_date)
                            self.assertEqual(pendulum.instance(latest_release.end_date), release.end_date)


class MockTelescope(StreamTelescope):
    DAG_ID = "telescope"

    def __init__(
        self, dag_id: str = DAG_ID, start_date: pendulum.DateTime = pendulum.now(), schedule_interval: str = "@monthly"
    ):
        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            dataset_id="data",
            merge_partition_field="field",
            schema_folder="folder",
            workflow_id=1,
            dataset_type_id=dag_id,
        )
        self.add_setup_task(self.task)
        self.add_task(self.add_new_dataset_releases)

    def task(self, **kwargs):
        self._start, self._end, self._first_release = self.get_release_info(**kwargs)
        return True

    def make_release(self, **kwargs) -> StreamRelease:
        return StreamRelease(dag_id="dag", start_date=pendulum.now(), end_date=pendulum.now(), first_release=True)

    def get_bq_load_info(self, release: StreamRelease) -> List[Tuple[str, str, str]]:
        return [
            ("transform_blob1", "main_table1", "partition_table1"),
            ("transform_blob2", "main_table2", "partition_table2"),
        ]


class TestStreamTelescopeTasks(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"
        self.workflow = 1

    def setup_api(self):
        org = Organisation(name=self.org_name)
        result = self.api.put_organisation(org)
        self.assertIsInstance(result, Organisation)

        tele_type = WorkflowType(type_id="tele_type", name="My Telescope")
        result = self.api.put_workflow_type(tele_type)
        self.assertIsInstance(result, WorkflowType)

        telescope = Workflow(organisation=Organisation(id=1), workflow_type=WorkflowType(id=1))
        result = self.api.put_workflow(telescope)
        self.assertIsInstance(result, Workflow)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            name="My dataset type",
            type_id=MockTelescope.DAG_ID,
            table_type=TableType(id=1),
        )

        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="My dataset",
            service="bigquery",
            address="project.dataset.table",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        result = self.api.put_dataset(dataset)
        self.assertIsInstance(result, Dataset)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_get_release_info(self, m_makeapi):
        m_makeapi.return_value = self.api

        start_date = pendulum.datetime(2020, 8, 1)
        env = ObservatoryEnvironment(api_host=self.host, api_port=self.port)
        with env.create():
            self.setup_api()
            first_execution_date = pendulum.datetime(2021, 9, 2)
            telescope = MockTelescope(start_date=start_date, schedule_interval="@monthly")
            dag = telescope.make_dag()

            # First DAG Run
            with env.create_dag_run(dag=dag, execution_date=first_execution_date):
                ti = env.run_task("task")
                self.assertEqual(ti.state, State.SUCCESS)
                expected_start = start_date
                expected_end = pendulum.datetime(2021, 9, 1)
                self.assertEqual(expected_start, telescope._start)
                self.assertEqual(expected_end, telescope._end)
                self.assertTrue(telescope._first_release)

                ti = env.run_task("add_new_dataset_releases")
                self.assertEqual(ti.state, State.SUCCESS)

            # Second DAG Run
            second_execution_date = pendulum.datetime(2021, 12, 2)
            with env.create_dag_run(dag=dag, execution_date=second_execution_date):
                ti = env.run_task("task")
                self.assertFalse(telescope._first_release)
                self.assertEqual(ti.state, State.SUCCESS)
                expected_start = pendulum.datetime(2021, 10, 1)
                expected_end = pendulum.datetime(2021, 12, 1)
                self.assertEqual(expected_start, telescope._start)
                self.assertEqual(expected_end, telescope._end)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_download(self, m_get):
        m_get.return_value = "data"
        telescope = MockTelescope()
        release = telescope.make_release()
        release.download = MagicMock()
        telescope.download(release)
        self.assertEqual(release.download.call_count, 1)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_upload_downloaded(self, m_get):
        m_get.return_value = "data"
        with patch("observatory.platform.workflows.stream_telescope.upload_files_from_list") as m_upload:
            telescope = MockTelescope()

            release = telescope.make_release()
            telescope.upload_downloaded(release)

            self.assertEqual(m_upload.call_count, 1)
            call_args, _ = m_upload.call_args
            self.assertEqual(call_args[0], [])
            self.assertEqual(call_args[1], "data")

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_extract(self, m_get):
        m_get.return_value = "data"
        telescope = MockTelescope()
        release = telescope.make_release()
        release.extract = MagicMock()
        telescope.extract(release)
        self.assertEqual(release.extract.call_count, 1)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_transform(self, m_get):
        m_get.return_value = "data"
        telescope = MockTelescope()
        release = telescope.make_release()
        release.transform = MagicMock()
        telescope.transform(release)
        self.assertEqual(release.transform.call_count, 1)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_upload_transformed(self, m_get):
        m_get.return_value = "data"
        with patch("observatory.platform.workflows.stream_telescope.upload_files_from_list") as m_upload:
            telescope = MockTelescope()

            release = telescope.make_release()
            telescope.upload_transformed(release)

            self.assertEqual(m_upload.call_count, 1)
            call_args, _ = m_upload.call_args
            self.assertEqual(call_args[0], [])
            self.assertEqual(call_args[1], "data")

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_bq_create_snapshot(self, m_get):
        m_get.side_effect = lambda x: {"data_path": "data", "project_id": "project_id"}[x]
        with patch(
            "observatory.platform.workflows.stream_telescope.create_bigquery_snapshot"
        ) as m_create_bigquery_snapshot:
            telescope = MockTelescope()
            release = telescope.make_release()
            telescope.bq_create_snapshot(release)

            self.assertEqual(2, m_create_bigquery_snapshot.call_count)
            table_id = bigquery_sharded_table_id("main_table1_snapshots", release.end_date)
            self.assertEqual(
                ("project_id", telescope.dataset_id, "main_table1", telescope.dataset_id, table_id),
                (m_create_bigquery_snapshot.call_args_list[0].args),
            )
            table_id = bigquery_sharded_table_id("main_table2_snapshots", release.end_date)
            self.assertEqual(
                ("project_id", telescope.dataset_id, "main_table2", telescope.dataset_id, table_id),
                (m_create_bigquery_snapshot.call_args_list[1].args),
            )

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_merge_update_files(self, m_get):
        m_get.return_value = "data"
        telescope = MockTelescope()

        class MockRelease:
            def __init__(self, dir):
                self.transform_folder = dir
                self.transform_files = glob(os.path.join(dir, "db_merge*.json"))
                self.first_release = True
                self.transform_bucket = "bucket"

        fixtures = glob(os.path.join(test_fixtures_path("workflows"), "db_merge*.json"))
        with CliRunner().isolated_filesystem() as tmpdir:
            for file in fixtures:
                filename = os.path.basename(file)
                dst = os.path.join(tmpdir, filename)
                shutil.copyfile(file, dst)

            release = MockRelease(tmpdir)

            # Test first release. Should do nothing.
            telescope.merge_update_files(release)
            self.assertEqual(len(os.listdir(tmpdir)), 2)  # Did nothing

            # Subsequent releases
            release.first_release = False

            self.assertEqual(len(os.listdir(tmpdir)), 2)
            with patch("observatory.platform.workflows.stream_telescope.upload_files_from_list"):
                telescope.merge_update_files(release)
            self.assertEqual(len(os.listdir(tmpdir)), 1)

            merged_file = os.path.join(tmpdir, "telescope.jsonl")
            self.assertTrue(os.path.exists(merged_file))

            key = telescope.merge_partition_field
            with open(merged_file, "r") as f:
                merged_data = {}
                for line in f:
                    row = json.loads(line)
                    merged_data[row[key]] = row["aux"]

            self.assertEqual(len(merged_data), 2)
            self.assertEqual(merged_data["myfirstdoi"], "value3")
            self.assertEqual(merged_data["myseconddoi"], "value2")
