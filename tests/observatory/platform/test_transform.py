# Copyright 2019 Curtin University. All Rights Reserved.
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

import copy
import datetime
import glob
import os
import pathlib
import unittest
import uuid

import jsonlines
from click.testing import CliRunner
from google.cloud import bigquery

from observatory.platform.files import validate_file_hash, load_jsonl
from observatory.platform.observatory_environment import test_fixtures_path
from observatory.platform.transform import add_partition_date, find_replace_file, get_chunks, split_and_compress


class TestTransform(unittest.TestCase):
    def test_find_replace_file(self):
        fixture_dir = test_fixtures_path("utils")
        src = os.path.join(fixture_dir, "find_replace.txt")
        expected_hash = "ffa623201cb9538bd3c030cd0b9f6b66"

        with CliRunner().isolated_filesystem():
            find_replace_file(src=src, dst="output", pattern="authenticated-orcid", replacement="authenticated_orcid")
            validate_file_hash(file_path="output", expected_hash=expected_hash)

    def test_add_partition_date(self):
        list_of_dicts = [{"k1a": "v2a"}, {"k1b": "v2b"}, {"k1c": "v2c"}]
        partition_date = datetime.datetime(2020, 1, 1)

        # Add partition date with default partition_type and partition_field
        result = add_partition_date(copy.deepcopy(list_of_dicts), partition_date)
        expected_result = [
            {"k1a": "v2a", "snapshot_date": partition_date.strftime("%Y-%m-%d")},
            {"k1b": "v2b", "snapshot_date": partition_date.strftime("%Y-%m-%d")},
            {"k1c": "v2c", "snapshot_date": partition_date.strftime("%Y-%m-%d")},
        ]
        self.assertListEqual(expected_result, result)

        result = add_partition_date(
            copy.deepcopy(list_of_dicts), partition_date, bigquery.TimePartitioningType.HOUR, "partition_field"
        )
        expected_result = [
            {"k1a": "v2a", "partition_field": partition_date.isoformat()},
            {"k1b": "v2b", "partition_field": partition_date.isoformat()},
            {"k1c": "v2c", "partition_field": partition_date.isoformat()},
        ]
        self.assertListEqual(expected_result, result)

        result = add_partition_date(
            copy.deepcopy(list_of_dicts), partition_date, bigquery.TimePartitioningType.MONTH, "partition_field"
        )
        expected_result = [
            {"k1a": "v2a", "partition_field": partition_date.strftime("%Y-%m-%d")},
            {"k1b": "v2b", "partition_field": partition_date.strftime("%Y-%m-%d")},
            {"k1c": "v2c", "partition_field": partition_date.strftime("%Y-%m-%d")},
        ]
        self.assertListEqual(expected_result, result)

    def test_get_chunks(self):
        """Test chunk generation."""

        items = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        chunks = list(get_chunks(input_list=items, chunk_size=2))
        self.assertEqual(len(chunks), 5)
        self.assertEqual(len(chunks[0]), 2)
        self.assertEqual(len(chunks[4]), 1)

    def test_split_and_compress(self):
        with CliRunner().isolated_filesystem() as tmp:
            # Make a random file
            file_path = os.path.join(tmp, "output.jsonl")
            n_lines = 1000
            expected_data = [{"name": str(uuid.uuid4()), "country": str(uuid.uuid4())} for _ in range(n_lines)]
            with open(file_path, mode="w") as f:
                with jsonlines.Writer(f) as writer:
                    writer.write_all(expected_data)

            # Split compress file
            max_output_size = 1024 * 2
            input_buffer_size = 512
            split_and_compress(
                pathlib.Path(file_path),
                pathlib.Path(tmp),
                max_output_size=max_output_size,
                input_buffer_size=input_buffer_size,
            )

            # Read files and check that it matches expected_data
            file_paths = sorted(glob.glob(os.path.join(tmp, "*.jsonl.gz")))
            self.assertEqual(24, len(file_paths))
            data = []
            for file_path in file_paths:
                data += load_jsonl(file_path)
            self.assertEqual(expected_data, data)
