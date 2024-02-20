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
import subprocess
from unittest.mock import MagicMock
from unittest.mock import patch

from observatory_platform.proc_utils import stream_process, wait_for_process


class TestProcUtils(unittest.TestCase):
    @patch("subprocess.Popen")
    def test_wait_for_process(self, mock_popen):
        proc = mock_popen()
        proc.communicate.return_value = "out".encode(), "err".encode()

        out, err = wait_for_process(proc)
        self.assertEqual("out", out)
        self.assertEqual("err", err)

    @patch("observatory_platform.utils.proc_utils.logging")
    def test_stream_process_success(self, mock_logging):
        command = """
for i in {1..10}; do
  echo "Hello World"
  sleep 1
done
"""
        proc = subprocess.Popen(
            ["bash", "-c", command],
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        result = stream_process(proc)

        mock_logging.debug.assert_called_with()


        a = 1

        # a = 1
        #
        # proc = MagicMock()
        # proc.poll.side_effect = [None, None, 0]  # Simulate process running and then ending
        # proc.returncode = 0  # Simulate successful exit code
        # proc.stdout.readline.return_value = "output line\n".encode()  # Simulate stdout output
        # proc.stderr.readline.return_value = "".encode()  # Simulate empty stderr
        #
        # result = stream_process(proc)
        #
        # self.assertTrue(result)
        # proc.stdout.readline.assert_called()
        # proc.stderr.readline.assert_called()
        # mock_logging.debug.assert_called_with("output line\n".encode())
        # mock_logging.error.assert_not_called()

    @patch("observatory_platform.utils.proc_utils.logging")
    def test_stream_process_failure(self, mock_logging):
        proc = MagicMock()
        proc.poll.side_effect = [None, None, 0]  # Simulate process running and then ending
        proc.returncode = 1  # Simulate failure exit code
        proc.stdout.readline.return_value = "output line\n".encode()  # Simulate stdout output
        proc.stderr.readline.return_value = "error line\n".encode()  # Simulate stderr output

        result = stream_process(proc)

        self.assertFalse(result)
        proc.stdout.readline.assert_called()
        proc.stderr.readline.assert_called()
        mock_logging.debug.assert_called_with("output line\n".encode())
        mock_logging.error.assert_called_with("error line\n".encode())
        mock_logging.error.assert_called_with("Command failed with return code 1")
