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

# Author: James Diprose, Aniek Roelofs

import os
import pathlib
import unittest
from unittest.mock import patch

import pendulum
from click.testing import CliRunner

import tests.observatory.platform.utils as platform_utils_tests
from observatory.platform.bigquery import bq_find_schema
from observatory.platform.config import (
    module_file_path,
    observatory_home,
    terraform_credentials_path,
)
from observatory.platform.observatory_environment import test_fixtures_path


class TestConfig(unittest.TestCase):
    def test_module_file_path(self):
        # Go back one step (the default)
        expected_path = str(pathlib.Path(*pathlib.Path(platform_utils_tests.__file__).resolve().parts[:-1]).resolve())
        actual_path = module_file_path("tests.observatory.platform.utils", nav_back_steps=-1)
        self.assertEqual(expected_path, actual_path)

        # Go back two steps
        expected_path = str(pathlib.Path(*pathlib.Path(platform_utils_tests.__file__).resolve().parts[:-2]).resolve())
        actual_path = module_file_path("tests.observatory.platform.utils", nav_back_steps=-2)
        self.assertEqual(expected_path, actual_path)

    @patch("observatory.platform.config.pathlib.Path.home")
    def test_observatory_home(self, home_mock):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create home path and mock getting home path
            home_path = "user-home"
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with runner.isolated_filesystem():
                # Test that observatory home works
                path = observatory_home()
                self.assertTrue(os.path.exists(path))
                self.assertEqual(f"{home_path}/.observatory", path)

                # Test that subdirectories are created
                path = observatory_home("subfolder")
                self.assertTrue(os.path.exists(path))
                self.assertEqual(f"{home_path}/.observatory/subfolder", path)

    def test_terraform_credentials_path(self):
        expected_path = os.path.expanduser("~/.terraform.d/credentials.tfrc.json")
        actual_path = terraform_credentials_path()
        self.assertEqual(expected_path, actual_path)

    def test_find_schema(self):
        schemas_path = test_fixtures_path("schemas")
        test_release_date = pendulum.datetime(2022, 11, 11)
        previous_release_date = pendulum.datetime(1950, 11, 11)

        # Nonexistent tables test case
        result = bq_find_schema(path=schemas_path, table_name="this_table_does_not_exist")
        self.assertIsNone(result)

        result = bq_find_schema(path=schemas_path, table_name="does_not_exist", prefix="this_table")
        self.assertIsNone(result)

        result = bq_find_schema(
            path=schemas_path, table_name="this_table_does_not_exist", release_date=test_release_date
        )
        self.assertIsNone(result)

        result = bq_find_schema(
            path=schemas_path, table_name="does_not_exist", release_date=test_release_date, prefix="this_table"
        )
        self.assertIsNone(result)

        # Release date on table name that doesn't end in date
        result = bq_find_schema(path=schemas_path, table_name="table_a", release_date=test_release_date)
        self.assertIsNone(result)

        result = bq_find_schema(path=schemas_path, table_name="a", release_date=test_release_date, prefix="table_")
        self.assertIsNone(result)

        # Release date before table date
        snapshot_date = pendulum.datetime(year=1000, month=1, day=1)
        result = bq_find_schema(path=schemas_path, table_name="table_b", release_date=snapshot_date)
        self.assertIsNone(result)

        # Basic test case - no date
        expected_schema = "table_a.json"
        result = bq_find_schema(path=schemas_path, table_name="table_a")
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # Prefix with no date
        expected_schema = "table_a.json"
        result = bq_find_schema(path=schemas_path, table_name="a", prefix="table_")
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # Table with date
        expected_schema = "table_b_2000-01-01.json"
        result = bq_find_schema(path=schemas_path, table_name="table_b", release_date=test_release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # Table with date and prefix
        expected_schema = "table_b_2000-01-01.json"
        result = bq_find_schema(path=schemas_path, table_name="b", release_date=test_release_date, prefix="table_")
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # Table with old date
        expected_schema = "table_b_1900-01-01.json"
        result = bq_find_schema(path=schemas_path, table_name="table_b", release_date=previous_release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # Table with old date and prefix
        expected_schema = "table_b_1900-01-01.json"
        result = bq_find_schema(path=schemas_path, table_name="b", release_date=previous_release_date, prefix="table_")
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))
