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

# Author: James Diprose, Tuan Chien

import copy
import datetime
import glob
import gzip
import json
import os
import pathlib
import shutil
import tempfile
import unittest
import uuid
from typing import Generator

import jsonlines
from google.cloud import bigquery

from observatory_platform.config import module_file_path
from observatory_platform.files import (
    add_partition_date,
    find_replace_file,
    get_chunks,
    get_file_hash,
    get_hasher_,
    gunzip_files,
    gzip_file_crc,
    is_gzip,
    list_files,
    load_csv,
    load_jsonl,
    save_jsonl_gz,
    split_file,
    split_file_and_compress,
    validate_file_hash,
    yield_csv,
    yield_jsonl,
)


class TestFileUtils(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestFileUtils, self).__init__(*args, **kwargs)

        self.fixtures_path = module_file_path("observatory_platform.tests.fixtures")
        self.csv_gz_file_path = os.path.join(self.fixtures_path, "load_csv_gz.csv.gz")
        self.jsonl_gz_file_path = os.path.join(self.fixtures_path, "load_jsonl_gz.jsonl.gz")
        self.csv_file_path = os.path.join(self.fixtures_path, "load_csv.csv")
        self.jsonl_file_path = os.path.join(self.fixtures_path, "load_jsonl.jsonl")
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

    def test_save_jsonl_gz(self):
        """Test writing list of dicts to jsonl.gz file"""
        list_of_dicts = [{"k1a": "v1a", "k2a": "v2a"}, {"k1b": "v1b", "k2b": "v2b"}]
        expected_file_hash = "e608cfeb"
        with tempfile.TemporaryDirectory() as t:
            file_path = os.path.join(t, "list.jsonl.gz")
            save_jsonl_gz(file_path, list_of_dicts)
            self.assertTrue(os.path.isfile(file_path))
            actual_file_hash = gzip_file_crc(file_path)
            self.assertEqual(expected_file_hash, actual_file_hash)

    def test_load_jsonl(self):
        """Test that Json lines loader functions"""

        # Read gzipped json lines
        actual_records = load_jsonl(self.jsonl_gz_file_path)
        self.assertListEqual(self.expected_records, actual_records)

        # Read plain json lines
        actual_records = load_jsonl(self.jsonl_file_path)
        self.assertListEqual(self.expected_records, actual_records)

        with tempfile.TemporaryDirectory() as t:
            expected_records = [
                {"name": "Elon Musk"},
                {"name": "Jeff Bezos"},
                {"name": "Peter Beck"},
                {"name": "Richard Branson"},
            ]
            file_path = os.path.join(t, "test.json")
            with open(file_path, mode="w") as f:
                for record in expected_records:
                    f.write(f"{json.dumps(record)}\n")

            actual_records = load_jsonl(file_path)
            self.assertListEqual(expected_records, actual_records)

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

    def test_get_hasher_(self):
        # MD5
        hasher = get_hasher_("md5")
        self.assertEqual(hasher.name, "md5")

        # SHA-256
        hasher = get_hasher_("sha256")
        self.assertEqual(hasher.name, "sha256")

        # SHA-512
        hasher = get_hasher_("sha512")
        self.assertEqual(hasher.name, "sha512")

        # Invalid
        self.assertRaises(Exception, get_hasher_, "invalid")

    def test_get_file_hash(self):
        expected_hash = "f299060e0383392ebeac64b714eca7e3"
        file_path = os.path.join(self.fixtures_path, "test_hasher.txt")
        computed_hash = get_file_hash(file_path=file_path)
        self.assertEqual(expected_hash, computed_hash)

    def test_validate_file_hash(self):
        expected_hash = "f299060e0383392ebeac64b714eca7e3"
        file_path = os.path.join(self.fixtures_path, "test_hasher.txt")

        self.assertTrue(validate_file_hash(file_path=file_path, expected_hash=expected_hash))

    def test_gunzip_files(self):
        filename = "testzip.txt.gz"
        expected_hash = "62d83685cff9cd962ac5abb563c61f38"
        output_file = "testzip.txt"
        src = os.path.join(self.fixtures_path, filename)

        # Save in same dir
        with tempfile.TemporaryDirectory() as tmpdir:
            dst = os.path.join(tmpdir, filename)
            shutil.copyfile(src, dst)

            gunzip_files(file_list=[dst])
            self.assertTrue(
                validate_file_hash(file_path=os.path.join(tmpdir, output_file), expected_hash=expected_hash)
            )

        # Specify save dir
        with tempfile.TemporaryDirectory() as tmpdir:
            dst = os.path.join(tmpdir, filename)
            gunzip_files(file_list=[src], output_dir=tmpdir)
            self.assertTrue(
                validate_file_hash(file_path=os.path.join(tmpdir, output_file), expected_hash=expected_hash)
            )

        # Skip non gz files
        with tempfile.TemporaryDirectory() as tmpdir:
            dst = os.path.join(tmpdir, filename)
            src_path = os.path.join(self.fixtures_path, output_file)
            gunzip_files(file_list=[src_path], output_dir=tmpdir)
            self.assertFalse(os.path.exists(dst))

    def test_split_file_and_compress(self):
        # Check that files are split and compressed properly
        with tempfile.TemporaryDirectory() as tmp:
            # Make a random file
            n_lines = 1000
            expected_data = [{"name": str(uuid.uuid4()), "country": str(uuid.uuid4())} for _ in range(n_lines)]
            file_path = os.path.join(tmp, "output.jsonl")
            with open(file_path, mode="w") as f:
                with jsonlines.Writer(f) as writer:
                    writer.write_all(expected_data)

            # Split compress file
            max_output_size = 1024 * 2
            input_buffer_size = 512
            split_file_and_compress(
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

        # Test that one file produced when under limit
        with tempfile.TemporaryDirectory() as tmp:
            expected_data = ["hello", "world"]
            file_path = os.path.join(tmp, "test.txt")
            with open(file_path, mode="w") as f:
                f.writelines("\n".join(expected_data))
            split_file_and_compress(
                pathlib.Path(file_path),
                pathlib.Path(tmp),
                max_output_size=max_output_size,
                input_buffer_size=input_buffer_size,
            )
            file_paths = sorted(glob.glob(os.path.join(tmp, "*.txt.gz")))
            self.assertEqual(1, len(file_paths))
            data = []
            for file_path in file_paths:
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    data += [line.strip() for line in f.readlines()]
            self.assertEqual(expected_data, data)

        # Test that only one file is produced when we just exceed the limit but there are no bytes to be written into
        # second file
        with tempfile.TemporaryDirectory() as tmp:
            expected_data = [str(uuid.uuid4()) for _ in range(98)]
            file_path = os.path.join(tmp, "test.txt")
            with open(file_path, mode="w") as f:
                f.writelines("\n".join(expected_data))
            split_file_and_compress(
                pathlib.Path(file_path),
                pathlib.Path(tmp),
                max_output_size=max_output_size,
                input_buffer_size=input_buffer_size,
            )
            file_paths = sorted(glob.glob(os.path.join(tmp, "*.txt.gz")))
            self.assertEqual(1, len(file_paths))
            data = []
            for file_path in file_paths:
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    data += [line.strip() for line in f.readlines()]
            self.assertEqual(expected_data, data)

    def test_split_file(self):
        # Check that files are split and compressed properly
        with tempfile.TemporaryDirectory() as tmp:
            # Make a random file
            n_lines = 1000
            expected_data = [{"name": str(uuid.uuid4()), "country": str(uuid.uuid4())} for _ in range(n_lines)]
            file_path = os.path.join(tmp, "output.jsonl")
            with open(file_path, mode="w") as f:
                with jsonlines.Writer(f) as writer:
                    writer.write_all(expected_data)

            # Split compress file
            max_output_size = 1024 * 2
            split_file(
                pathlib.Path(file_path),
                pathlib.Path(tmp),
                max_output_size=max_output_size,
            )

            # Read files and check that it matches expected_data
            file_paths = sorted(list_files(tmp, r"^output\d{12}\.jsonl$"))
            self.assertEqual(48, len(file_paths))
            data = []
            for file_path in file_paths:
                data += load_jsonl(file_path)
            self.assertEqual(expected_data, data)

        # Test that one file produced when under limit
        with tempfile.TemporaryDirectory() as tmp:
            expected_data = ["hello", "world"]
            file_path = os.path.join(tmp, "test.txt")
            with open(file_path, mode="w") as f:
                f.writelines("\n".join(expected_data))
            split_file(
                pathlib.Path(file_path),
                pathlib.Path(tmp),
                max_output_size=max_output_size,
            )
            file_paths = sorted(list_files(tmp, r"^test\d{12}\.txt$"))
            self.assertEqual(1, len(file_paths))
            data = []
            for file_path in file_paths:
                with open(file_path, "r") as f:
                    data += [line.strip() for line in f.readlines()]
            self.assertEqual(expected_data, data)

        # Test that only one file is produced when we just exceed the limit but there are no bytes to be written into
        # second file
        with tempfile.TemporaryDirectory() as tmp:
            # 2071 bytes vs 2048 max
            expected_data = [str(uuid.uuid4()) for _ in range(56)]
            file_path = os.path.join(tmp, "test.txt")
            with open(file_path, mode="w") as f:
                f.writelines("\n".join(expected_data))
            split_file(
                pathlib.Path(file_path),
                pathlib.Path(tmp),
                max_output_size=max_output_size,
            )
            file_paths = sorted(list_files(tmp, r"^test\d{12}\.txt$"))
            self.assertEqual(1, len(file_paths))
            data = []
            for file_path in file_paths:
                with open(file_path, "r") as f:
                    data += [line.strip() for line in f.readlines()]
            self.assertEqual(expected_data, data)

    def test_find_replace_file(self):
        src = os.path.join(self.fixtures_path, "find_replace.txt")
        expected_hash = "ffa623201cb9538bd3c030cd0b9f6b66"

        with tempfile.TemporaryDirectory() as t:
            output_path = os.path.join(t, "output")
            find_replace_file(
                src=src, dst=output_path, pattern="authenticated-orcid", replacement="authenticated_orcid"
            )
            validate_file_hash(file_path=output_path, expected_hash=expected_hash)

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
