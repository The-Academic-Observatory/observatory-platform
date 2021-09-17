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

# Author: James Diprose, Tuan Chien

import csv
import json
import os
import unittest

from click.testing import CliRunner
from observatory.platform.utils.json_util import csv_to_jsonlines, to_json_lines
from observatory.platform.utils.test_utils import (
    ObservatoryTestCase,
    test_fixtures_path,
)


class TestJsonUtil(ObservatoryTestCase):
    def test_to_json_lines(self):
        hello = dict()
        hello["hello"] = "world"
        hello_list = [hello] * 3

        # Multiple items
        expected = '{"hello": "world"}\n{"hello": "world"}\n{"hello": "world"}\n'
        actual = to_json_lines(hello_list)
        self.assertEqual(expected, actual)

        # One item
        expected = '{"hello": "world"}\n'
        actual = to_json_lines([hello])
        self.assertEqual(expected, actual)

        # No items
        expected = ""
        actual = to_json_lines([])
        self.assertEqual(expected, actual)

    def test_csv_to_jsonlines(self):
        fixtures_dir = test_fixtures_path("utils")
        csv_file = os.path.join(fixtures_dir, "test.csv")

        with CliRunner().isolated_filesystem():
            output_file = "test.jsonl"
            expected_hash = "d7233f74c7a9bd526c868a5fec24fe52"
            algorithm = "md5"
            csv_to_jsonlines(csv_file=csv_file, jsonl_file=output_file)
            self.assert_file_integrity(output_file, expected_hash, algorithm)
