# Copyright 2019 Curtin University
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


import unittest
from unittest.mock import patch

from airflow.exceptions import AirflowException
from observatory.platform.utils.proc_utils import run_bash_cmd


class TestProcUtils(unittest.TestCase):
    def test_run_bash_cmd(self):
        # Run echo with stdout
        with patch("observatory.platform.utils.proc_utils.logging.info") as m_log:
            cmd = "echo hello"
            run_bash_cmd(cmd)
            self.assertEqual(m_log.call_count, 1)

        # Have an error
        cmd = "$(exit 1)"
        self.assertRaises(AirflowException, run_bash_cmd, cmd)
