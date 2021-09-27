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

# Author: Tuan Chien

from unittest.mock import MagicMock
from unittest.mock import patch

import pendulum
from airflow.utils.state import State

from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
)
from observatory.platform.utils.test_utils import ObservatoryTestCase
from observatory.platform.workflows.stream_telescope import StreamRelease, StreamTelescope, get_data_interval


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


class TestStreamTelescope(ObservatoryTestCase):
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
