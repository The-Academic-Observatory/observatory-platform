# Copyright 2022 Curtin University
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

# Author: Aniek Roelofs

import unittest
from unittest.mock import patch

from observatory_platform.proc_utils import wait_for_process


class TestProcUtils(unittest.TestCase):
    @patch("subprocess.Popen")
    def test_wait_for_process(self, mock_popen):
        proc = mock_popen()
        proc.communicate.return_value = "out".encode(), "err".encode()

        out, err = wait_for_process(proc)
        self.assertEqual("out", out)
        self.assertEqual("err", err)
