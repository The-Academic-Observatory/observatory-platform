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

# Author: Aniek Roelofs

import unittest
from unittest.mock import Mock, call, patch
from observatory.platform.utils.proc_utils import wait_for_process, stream_process


class TestProcUtils(unittest.TestCase):
    def test_wait_for_process(self):
        proc = Mock()
        proc.communicate.return_value = "out".encode("utf-8"), "err".encode("utf-8")
        output, error = wait_for_process(proc)
        self.assertEqual("out", output)
        self.assertEqual("err", error)

    @patch("builtins.print")
    def test_stream_process(self, mock_print):
        proc = Mock()
        proc.stdout = ["out1".encode("utf-8"), "out2".encode("utf-8")]
        proc.stderr = ["err1".encode("utf-8"), "err2".encode("utf-8")]
        proc.poll.side_effect = [None, 1, None, 1]

        output_concat, error_concat = stream_process(proc, debug=True)
        self.assertEqual(8, mock_print.call_count)
        self.assertIn(call("out1", end=""), mock_print.call_args_list)
        self.assertEqual("out1out2out1out2", output_concat)
        self.assertEqual("err1err2err1err2", error_concat)

        mock_print.reset_mock()
        output_concat, error_concat = stream_process(proc, debug=False)
        self.assertEqual(4, mock_print.call_count)
        self.assertNotIn(call("out1", end=""), mock_print.call_args_list)
        self.assertEqual("out1out2out1out2", output_concat)
        self.assertEqual("err1err2err1err2", error_concat)
