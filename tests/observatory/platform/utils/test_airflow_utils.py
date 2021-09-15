# Copyright 2021 Curtin University. All Rights Reserved.
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


import unittest

from airflow.exceptions import AirflowException

from observatory.platform.utils.airflow_utils import set_task_state


class TestAirflowUtils(unittest.TestCase):
    def test_set_task_state(self):
        """Test set_task_state"""

        task_id = "test_task"
        set_task_state(True, task_id)
        with self.assertRaises(AirflowException):
            set_task_state(False, task_id)
