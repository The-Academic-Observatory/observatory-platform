# Copyright 2024 Curtin University
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

# Author: Keegan Smith

from datetime import datetime
import unittest
from zoneinfo import ZoneInfo

from observatory_platform.date_utils import datetime_normalise


class test_normalise_datetime(unittest.TestCase):
    """Tests for normalise_datetime"""

    def test_str_inputs(self):
        inputs = [
            "2024-01-01 12:00:00+0000",
            "2024-01-01 00:00:00+0800",
            "2024-01-01 12:00:00-0800",
            "2024-01-01 12:00:00Z",
            "2024-01-01 12:00:00UTC+1",
        ]
        expected_outputs = [
            "2024-01-01T12:00:00+00:00",
            "2023-12-31T16:00:00+00:00",
            "2024-01-01T20:00:00+00:00",
            "2024-01-01T12:00:00+00:00",
            "2024-01-01T13:00:00+00:00",
        ]
        for input, expected_output in zip(inputs, expected_outputs):
            actual_output = datetime_normalise(input)
            self.assertEqual(expected_output, actual_output)

    def test_dt_inputs(self):
        inputs = [
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("UTC")),
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("Etc/GMT+1")),
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("Etc/GMT-1")),
            datetime(2023, 12, 31, 23, 0, 0, tzinfo=ZoneInfo("Etc/GMT+1")),
        ]
        expected_outputs = [
            "2024-01-01T12:00:00+00:00",
            "2024-01-01T13:00:00+00:00",
            "2023-12-31T23:00:00+00:00",
            "2024-01-01T00:00:00+00:00",
        ]
        for input, expected_output in zip(inputs, expected_outputs):
            actual_output = datetime_normalise(input)
            self.assertEqual(expected_output, actual_output)

    def test_missing_tz(self):
        inputs = ["2024-01-01 00:00:00", "2024-01-01T12:00:00", datetime(2024, 1, 1, 12, 0, 0)]
        expected_outputs = ["2024-01-01T00:00:00+00:00", "2024-01-01T12:00:00+00:00", "2024-01-01T12:00:00+00:00"]
        for input, expected_output in zip(inputs, expected_outputs):
            actual_output = datetime_normalise(input)
            self.assertEqual(expected_output, actual_output)
