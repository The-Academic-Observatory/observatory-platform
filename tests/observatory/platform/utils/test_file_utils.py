# Copyright 2020, 2021 Curtin University. All Rights Reserved.
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


import os
import itertools
import unittest

from observatory.platform.utils.file_utils import load_csv_gz, load_jsonl
from observatory.platform.utils.test_utils import test_fixtures_path


class TestFileUtils(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestFileUtils, self).__init__(*args, **kwargs)

        fixtures_path = test_fixtures_path("elastic")
        self.csv_file_path = os.path.join(fixtures_path, "load_csv_gz.csv.gz")
        self.jsonl_file_path = os.path.join(fixtures_path, "load_json_gz.jsonl.gz")
        self.expected_records = [
            {"first_name": "Jim", "last_name": "Holden"},
            {"first_name": "Alex", "last_name": "Kamal"},
            {"first_name": "Naomi", "last_name": "Nagata"},
            {"first_name": "Amos", "last_name": "Burton"},
        ]

    def test_load_csv_gz(self):
        """ Test that CSV loader functions """

        # Test with list
        actual_records = load_csv_gz(self.csv_file_path)
        self.assertListEqual(self.expected_records, actual_records)

    def test_load_jsonl_gz(self):
        """ Test that Json lines loader functions """

        # Test with list
        actual_records = list(load_jsonl(self.jsonl_file_path, yield_items=True))
        self.assertListEqual(self.expected_records, actual_records)

        # Test with yield
