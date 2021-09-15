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
import unittest
from typing import Generator

from observatory.platform.utils.file_utils import load_csv, load_jsonl, yield_jsonl, yield_csv, is_gzip
from observatory.platform.utils.test_utils import test_fixtures_path


class TestFileUtils(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestFileUtils, self).__init__(*args, **kwargs)

        fixtures_path = test_fixtures_path("elastic")
        self.csv_gz_file_path = os.path.join(fixtures_path, "load_csv_gz.csv.gz")
        self.jsonl_gz_file_path = os.path.join(fixtures_path, "load_jsonl_gz.jsonl.gz")
        self.csv_file_path = os.path.join(fixtures_path, "load_csv.csv")
        self.jsonl_file_path = os.path.join(fixtures_path, "load_jsonl.jsonl")
        self.expected_records = [
            {"first_name": "Jim", "last_name": "Holden"},
            {"first_name": "Alex", "last_name": "Kamal"},
            {"first_name": "Naomi", "last_name": "Nagata"},
            {"first_name": "Amos", "last_name": "Burton"},
        ]

    def test_is_gzip(self):
        """Test is_gzip"""

        self.assertTrue(is_gzip(self.csv_gz_file_path))
        self.assertFalse(is_gzip(self.csv_file_path))

    def test_load_csv(self):
        """Test that CSV loader functions"""

        # Read gzipped CSV
        actual_records = load_csv(self.csv_gz_file_path)
        self.assertListEqual(self.expected_records, actual_records)

        # Read plain CSV
        actual_records = load_csv(self.csv_file_path)
        self.assertListEqual(self.expected_records, actual_records)

    def test_load_jsonl(self):
        """Test that Json lines loader functions"""

        # Read gzipped json lines
        actual_records = load_jsonl(self.jsonl_gz_file_path)
        self.assertListEqual(self.expected_records, actual_records)

        # Read plain json lines
        actual_records = load_jsonl(self.jsonl_file_path)
        self.assertListEqual(self.expected_records, actual_records)

    def test_yield_csv(self):
        """Test that yield CSV loader functions"""

        # Read gzipped CSV
        generator = yield_csv(self.csv_file_path)
        self.assertIsInstance(generator, Generator)
        self.assertListEqual(self.expected_records, list(generator))

    def test_yield_jsonl(self):
        """Test that yield Json lines loader functions"""

        generator = yield_jsonl(self.jsonl_file_path)
        self.assertIsInstance(generator, Generator)
        self.assertListEqual(self.expected_records, list(generator))
