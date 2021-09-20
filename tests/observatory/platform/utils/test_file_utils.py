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


import os
import shutil
import unittest
from typing import Generator

from _hashlib import HASH
from click.testing import CliRunner
from observatory.platform.utils.file_utils import (
    find_replace_file,
    get_file_hash,
    get_hasher_,
    gunzip_files,
    is_gzip,
    load_csv,
    load_jsonl,
    validate_file_hash,
    yield_csv,
    yield_jsonl,
)
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
        fixtures_dir = test_fixtures_path("utils")
        file_path = os.path.join(fixtures_dir, "test_hasher.txt")
        computed_hash = get_file_hash(file_path=file_path)
        self.assertEqual(expected_hash, computed_hash)

    def test_validate_file_hash(self):
        expected_hash = "f299060e0383392ebeac64b714eca7e3"
        fixtures_dir = test_fixtures_path("utils")
        file_path = os.path.join(fixtures_dir, "test_hasher.txt")

        self.assertTrue(validate_file_hash(file_path=file_path, expected_hash=expected_hash))

    def test_gunzip_files(self):
        fixture_dir = test_fixtures_path("utils")
        filename = "testzip.txt.gz"
        expected_hash = "62d83685cff9cd962ac5abb563c61f38"
        output_file = "testzip.txt"
        src = os.path.join(fixture_dir, filename)

        # Save in same dir
        with CliRunner().isolated_filesystem() as tmpdir:
            dst = os.path.join(tmpdir, filename)
            shutil.copyfile(src, dst)

            gunzip_files(file_list=[dst])
            self.assertTrue(validate_file_hash(file_path=output_file, expected_hash=expected_hash))

        # Specify save dir
        with CliRunner().isolated_filesystem() as tmpdir:
            dst = os.path.join(tmpdir, filename)
            gunzip_files(file_list=[src], output_dir=tmpdir)
            self.assertTrue(validate_file_hash(file_path=output_file, expected_hash=expected_hash))

        # Skip non gz files
        with CliRunner().isolated_filesystem() as tmpdir:
            dst = os.path.join(tmpdir, filename)
            src_path = os.path.join(fixture_dir, output_file)
            gunzip_files(file_list=[src_path], output_dir=tmpdir)
            self.assertFalse(os.path.exists(dst))

    def test_find_replace_file(self):
        fixture_dir = test_fixtures_path("utils")
        src = os.path.join(fixture_dir, "find_replace.txt")
        expected_hash = "ffa623201cb9538bd3c030cd0b9f6b66"

        with CliRunner().isolated_filesystem():
            find_replace_file(src=src, dst="output", pattern="authenticated-orcid", replacement="authenticated_orcid")
            validate_file_hash(file_path="output", expected_hash=expected_hash)
