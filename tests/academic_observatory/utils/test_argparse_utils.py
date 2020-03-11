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

# Author: James Diprose

import datetime
import os
import unittest
from argparse import ArgumentTypeError

from academic_observatory.utils import validate_path, validate_datetime


class TestArgparseUtils(unittest.TestCase):

    def test_validate_date(self):
        # Check that ArgumentTypeError is raised when incorrect date format used
        with self.assertRaises(ArgumentTypeError):
            validate_datetime('2020-01-01', "%Y-%m")

        # Check that correct datetime.datetime type is returned
        datetime_expected = datetime.datetime(year=2020, month=1, day=1)
        datetime_actual = validate_datetime('2020-01-01', "%Y-%m-%d")
        self.assertEqual(datetime_actual, datetime_expected)

    def test_validate_path(self):
        with self.assertRaises(ArgumentTypeError):
            validate_path(os.path.join(os.path.dirname(__file__), 'ishouldnotexist'))
