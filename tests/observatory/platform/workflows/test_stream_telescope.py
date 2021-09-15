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
# limitations under the License.

# Author: Aniek Roelofs, Tuan Chien

from unittest.mock import MagicMock
from airflow.utils.state import State

from observatory.platform.workflows.stream_telescope import StreamRelease, StreamTelescope, get_data_interval
import os
from datetime import timedelta
from unittest.mock import patch

import pendulum
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase
from observatory.platform.utils.workflow_utils import blob_name, table_ids_from_path
from observatory.platform.workflows.stream_telescope import (
    StreamRelease,
    StreamTelescope,
)

DEFAULT_SCHEMA_PATH = "/path/to/schemas"


class TestStreamTelescope(StreamTelescope):
    """
    Generic Workflow telescope for running tasks.
    """

    DAG_ID = "unit_test_stream"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 1, 1),
        schedule_interval: str = "@daily",
        dataset_id: str = "dataset_id",
        merge_partition_field: str = "id",
        bq_merge_days: int = 7,
        schema_folder: str = DEFAULT_SCHEMA_PATH,
        source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
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

        self.add_setup_task_chain([self.check_dependencies, self.get_release_info])
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load_partition)
        self.add_task_chain([self.bq_delete_old, self.bq_append_new, self.cleanup], trigger_rule="none_failed")

    def make_release(self, **kwargs) -> StreamRelease:
        """Make a Release instance

        :param kwargs: The context passed from the PythonOperator.
        :return: CrossrefEventsRelease
        """
        ti: TaskInstance = kwargs["ti"]
        start_date, end_date, first_release = ti.xcom_pull(
            key=TestStreamTelescope.RELEASE_INFO, include_prior_dates=True
        )

        start_date = pendulum.parse(start_date)
        end_date = pendulum.parse(end_date)

        release = StreamRelease(self.dag_id, start_date, end_date, first_release)
        return release

    def transform(self, release: StreamRelease, **kwargs):
        with open(os.path.join(release.transform_folder, "file.jsonl"), "w") as f:
            f.write(release.release_id)


class TestTestStreamTelescope(ObservatoryTestCase):
    """ Tests the StreamTelescope. """

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestTestStreamTelescope, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    @patch("observatory.platform.workflows.stream_telescope.bq_append_from_file")
    @patch("observatory.platform.workflows.stream_telescope.bq_append_from_partition")
    @patch("observatory.platform.workflows.stream_telescope.bq_delete_old")
    @patch("observatory.platform.workflows.stream_telescope.bq_load_ingestion_partition")
    @patch("observatory.platform.workflows.stream_telescope.upload_files_from_list")
    def test_telescope(
        self,
        upload_files_from_list,
        bq_load_ingestion_partition,
        bq_delete_old,
        bq_append_from_partition,
        bq_append_from_file,
    ):
        """Test the telescope end to end.
        :return: None.
        """
        run1 = {
            "date": pendulum.datetime(year=2020, month=1, day=1),
            "first_release": True,
            "bq_append_from_file": True,
        }
        run2 = {"date": pendulum.datetime(year=2020, month=1, day=2), "first_release": False, "bq_load_partition": True}
        run3 = {
            "date": pendulum.datetime(year=2020, month=1, day=8),
            "first_release": False,
            "bq_load_partition": True,
            "bq_delete_old": True,
            "bq_append_from_partition": True,
        }
        run4 = {"date": pendulum.datetime(year=2020, month=1, day=9), "first_release": False, "bq_load_partition": True}

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        telescope = TestStreamTelescope(dataset_id=dataset_id)
        dag = telescope.make_dag()
        release = None

        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            for run in [run1, run2, run3, run4]:
                with env.create_dag_run(dag, run["date"]):
                    # Test that all dependencies are specified: no error should be thrown
                    env.run_task(telescope.check_dependencies.__name__)

                    # Test start and end date for release info
                    ti = env.run_task(telescope.get_release_info.__name__)
                    start_date, end_date, first_release = ti.xcom_pull(
                        key=TestStreamTelescope.RELEASE_INFO,
                        task_ids=telescope.get_release_info.__name__,
                        include_prior_dates=False,
                    )

                    start_date = pendulum.parse(start_date)
                    end_date = pendulum.parse(end_date)

                    self.assertEqual(run["first_release"], first_release)
                    if first_release:
                        self.assertEqual(start_date, dag.default_args["start_date"])
                        self.assertEqual(end_date, pendulum.today("UTC") - timedelta(days=1))
                    else:
                        self.assertEqual(release.end_date + timedelta(days=1), start_date)
                        self.assertEqual(pendulum.today("UTC") - timedelta(days=1), end_date)

                    # Use release info for other tasks
                    release = StreamRelease(
                        telescope.dag_id,
                        start_date,
                        end_date,
                        first_release,
                    )
                    transform_path = os.path.join(release.transform_folder, "file.jsonl")

                    env.run_task(telescope.transform.__name__)
                    self.assertTrue(os.path.exists(transform_path))

                    # Test that files upload is called
                    env.run_task(telescope.upload_transformed.__name__)
                    upload_files_from_list.assert_called_once_with([transform_path], release.transform_bucket)

                    # Test whether the correct bq load functions are called for different runs
                    ti = env.run_task(telescope.bq_load_partition.__name__)
                    if run.get("bq_load_partition", False):
                        transform_blob = blob_name(transform_path)
                        main_table_id, partition_table_id = table_ids_from_path(transform_path)
                        table_description = telescope.table_descriptions.get(main_table_id, "")
                        bq_load_ingestion_partition.assert_called_once_with(
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
                        self.assertEqual("success", ti.state)
                    else:
                        self.assertEqual(0, bq_load_ingestion_partition.call_count)
                        self.assertEqual("skipped", ti.state)

                    # Test whether the correct bq delete functions are called for different runs
                    ti = env.run_task(telescope.bq_delete_old.__name__)
                    if run.get("bq_delete_old", False):
                        # Use previous ti, because after running task new xcoms have been pushed
                        previous_ti = ti.get_previous_ti()
                        start_date = pendulum.from_format(
                            previous_ti.xcom_pull(
                                key=telescope.XCOM_BQ_DATES, task_ids=ti.task_id, include_prior_dates=True
                            ),
                            "YYYYMMDD",
                        )
                        end_date = pendulum.instance(ti.start_date)
                        main_table_id, partition_table_id = table_ids_from_path(transform_path)
                        bq_delete_old.assert_called_once_with(
                            start_date,
                            end_date,
                            telescope.dataset_id,
                            main_table_id,
                            partition_table_id,
                            telescope.merge_partition_field,
                        )
                        self.assertEqual("success", ti.state)
                    else:
                        self.assertEqual(0, bq_delete_old.call_count)
                        self.assertEqual("skipped", ti.state)

                    # Test whether the correct bq append functions are called for different runs
                    ti = env.run_task(telescope.bq_append_new.__name__)
                    if run.get("bq_append_from_partition", False):
                        # Use previous ti, because after running task new xcoms have been pushed
                        previous_ti = ti.get_previous_ti()
                        start_date = pendulum.from_format(
                            previous_ti.xcom_pull(
                                key=telescope.XCOM_BQ_DATES, task_ids=ti.task_id, include_prior_dates=True
                            ),
                            "YYYYMMDD",
                        )
                        end_date = pendulum.instance(ti.start_date)
                        main_table_id, partition_table_id = table_ids_from_path(transform_path)
                        bq_append_from_partition.assert_called_once_with(
                            start_date,
                            end_date,
                            telescope.dataset_id,
                            main_table_id,
                            partition_table_id,
                            telescope.merge_partition_field,
                        )
                        self.assertEqual("success", ti.state)
                    elif run.get("bq_append_from_file", False):
                        transform_blob = blob_name(transform_path)
                        main_table_id, partition_table_id = table_ids_from_path(transform_path)
                        table_description = telescope.table_descriptions.get(main_table_id, "")
                        bq_append_from_file.assert_called_once_with(
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
                        self.assertEqual("success", ti.state)
                    else:
                        self.assertEqual(0, bq_delete_old.call_count)
                        self.assertEqual("skipped", ti.state)

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


class MockTelescope(StreamTelescope):
    def __init__(self, start_date: pendulum.DateTime = pendulum.now(), schedule_interval: str = "@monthly"):
        super().__init__(
            dag_id="dag",
            start_date=start_date,
            schedule_interval=schedule_interval,
            dataset_id="data",
            merge_partition_field="field",
            bq_merge_days=1,
            schema_folder="folder",
        )
        self.add_setup_task(self.task)

    def task(self, **kwargs):
        self.start, self.end, self.first_release = self.get_release_info(**kwargs)
        print("Hello")

    def make_release(self, **kwargs):
        return StreamRelease(dag_id="dag", start_date=pendulum.now(), end_date=pendulum.now(), first_release=True)


class TestStreamTelescopeTasks(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_get_data_interval(self):
        # Daily
        execution_date = pendulum.datetime(2021, 9, 5)
        expected_end = pendulum.datetime(2021, 9, 6)
        start, end = get_data_interval(execution_date, "@daily")
        self.assertEqual(execution_date, start)
        self.assertEqual(expected_end, end)

        # Weekly
        execution_date = pendulum.datetime(2021, 9, 5)
        expected_end = pendulum.datetime(2021, 9, 12)
        start, end = get_data_interval(execution_date, "@weekly")
        self.assertEqual(execution_date, start)
        self.assertEqual(expected_end, end)

        # Monthly
        execution_date = pendulum.datetime(2021, 9, 1)
        expected_end = pendulum.datetime(2021, 10, 1)
        start, end = get_data_interval(execution_date, "@monthly")
        self.assertEqual(execution_date, start)
        self.assertEqual(expected_end, end)

    def test_get_release_info(self):
        start_date = pendulum.datetime(2020, 9, 1)
        env = ObservatoryEnvironment(enable_api=False)
        with env.create():
            first_execution_date = pendulum.datetime(2021, 9, 1)
            telescope = MockTelescope(start_date=start_date, schedule_interval="@monthly")
            dag = telescope.make_dag()

            # First DAG Run
            with env.create_dag_run(dag=dag, execution_date=first_execution_date):
                ti = env.run_task("task", dag, execution_date=first_execution_date)
                self.assertEqual(ti.state, State.SUCCESS)
                expected_start = start_date
                expected_end = pendulum.datetime(2021, 9, 30)
                self.assertEqual(expected_start, telescope.start)
                self.assertEqual(expected_end, telescope.end)
                self.assertTrue(telescope.first_release)

            # Second DAG Run
            second_execution_date = pendulum.datetime(2021, 12, 1)
            with env.create_dag_run(dag=dag, execution_date=second_execution_date):
                ti = env.run_task("task", dag, execution_date=second_execution_date)
                self.assertEqual(ti.state, State.SUCCESS)
                expected_start = pendulum.datetime(2021, 10, 1)
                expected_end = pendulum.datetime(2021, 12, 31)
                self.assertEqual(expected_start, telescope.start)
                self.assertEqual(expected_end, telescope.end)
                self.assertFalse(telescope.first_release)

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

            releases = telescope.make_release()
            telescope.upload_downloaded(releases)

            self.assertEqual(m_upload.call_count, 1)
            call_args, _ = m_upload.call_args
            self.assertEqual(call_args[0], [])
            self.assertEqual(call_args[1], "data")

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_transform(self, m_get):
        m_get.return_value = "data"
        telescope = MockTelescope()
        releases = telescope.make_release()
        releases.transform = MagicMock()
        telescope.transform(releases)
        self.assertEqual(releases.transform.call_count, 1)

    @patch("observatory.platform.utils.workflow_utils.Variable.get")
    def test_extract(self, m_get):
        m_get.return_value = "data"
        telescope = MockTelescope()
        release = telescope.make_release()
        release.extract = MagicMock()
        telescope.extract(release)
        self.assertEqual(release.extract.call_count, 1)