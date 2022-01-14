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

import subprocess
from subprocess import Popen

from observatory.platform.utils.proc_utils import wait_for_process, stream_process
import unittest
from unittest.mock import patch


class TestProcUtils(unittest.TestCase):
    def test_wait_for_process(self):
        # Test stdout
        proc: Popen = subprocess.Popen(
            ["echo", "foo"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, _ = wait_for_process(proc)
        self.assertEqual("foo\n", out)

        # Test stderr
        proc: Popen = subprocess.Popen(
            ["ls", "bar"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, err = wait_for_process(proc)
        self.assertEqual("ls: bar: No such file or directory\n", err)

    @patch("builtins.print")
    def test_stream_process(self, mock_print):
        # Test stdout with debug=False
        proc: Popen = subprocess.Popen(
            ["echo", "foo"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, _ = stream_process(proc, False)
        self.assertEqual("foo\n", out)
        mock_print.assert_not_called()
        mock_print.reset_mock()

        # Test stderr with debug=False
        proc: Popen = subprocess.Popen(
            ["ls", "bar"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, err = stream_process(proc, False)
        self.assertEqual("ls: bar: No such file or directory\n", err)
        mock_print.assert_called_once_with("ls: bar: No such file or directory\n", end="")
        mock_print.reset_mock()

        # Test stdout with debug=True
        proc: Popen = subprocess.Popen(
            ["echo", "foo"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, _ = stream_process(proc, True)
        self.assertEqual("foo\n", out)
        mock_print.assert_called_once_with("foo\n", end="")
        mock_print.reset_mock()

        # Test stderr with debug=True
        proc: Popen = subprocess.Popen(
            ["ls", "bar"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, err = stream_process(proc, True)
        self.assertEqual("ls: bar: No such file or directory\n", err)
        mock_print.assert_called_once_with("ls: bar: No such file or directory\n", end="")
