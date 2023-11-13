# Copyright 2019-2024 Curtin University
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

import pendulum
from airflow.exceptions import AirflowException

from observatory_platform.airflow.release import set_task_state, make_snapshot_date
from observatory_platform.sandbox.test_utils import SandboxTestCase


class TestWorkflow(SandboxTestCase):
    def test_make_snapshot_date(self):
        """Test make_table_name"""

        data_interval_end = pendulum.datetime(2021, 11, 11)
        expected_date = pendulum.datetime(2021, 11, 11)
        actual_date = make_snapshot_date(**{"data_interval_end": data_interval_end})
        self.assertEqual(expected_date, actual_date)

    def test_set_task_state(self):
        """Test set_task_state"""

        task_id = "test_task"
        set_task_state(True, task_id)
        with self.assertRaises(AirflowException):
            set_task_state(False, task_id)
