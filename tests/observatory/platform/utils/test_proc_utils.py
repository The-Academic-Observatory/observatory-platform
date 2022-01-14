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
from unittest.mock import call, patch

from observatory.platform.utils.proc_utils import stream_process, wait_for_process


class TestProcUtils(unittest.TestCase):
    @patch("subprocess.Popen")
    def test_wait_for_process(self, mock_popen):
        proc = mock_popen()
        proc.communicate.return_value = "out".encode(), "err".encode()

        out, err = wait_for_process(proc)
        self.assertEqual("out", out)
        self.assertEqual("err", err)

    @patch("subprocess.Popen")
    @patch("builtins.print")
    def test_stream_process(self, mock_print, mock_popen):
        proc = mock_popen()
        proc.stdout = ["out1".encode(), "out2".encode()]
        proc.stderr = ["err1".encode(), "err2".encode()]
        proc.poll.side_effect = [None, 0, None, 0]

        # Test with debug=False
        out, err = stream_process(proc, False)
        self.assertEqual("out1out2out1out2", out)
        self.assertEqual("err1err2err1err2", err)
        self.assertListEqual([call(err, end="") for err in ["err1", "err2", "err1", "err2"]], mock_print.call_args_list)
        mock_print.reset_mock()

        # Test with debug=True
        out, err = stream_process(proc, True)
        self.assertEqual("out1out2out1out2", out)
        self.assertEqual("err1err2err1err2", err)
        self.assertListEqual(
            [call(err, end="") for err in ["out1", "out2", "err1", "err2", "out1", "out2", "err1", "err2"]],
            mock_print.call_args_list,
        )
