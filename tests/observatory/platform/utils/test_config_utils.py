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
import tests.observatory.platform.utils as platform_utils_tests
from click.testing import CliRunner
from observatory.platform.utils.config_utils import (
    find_schema,
    module_file_path,
    observatory_home,
    terraform_credentials_path,
)
from observatory.platform.utils.template_utils import (
    SubFolder,
    reset_variables,
    telescope_path,
    test_data_path,
)
from tests.observatory.test_utils import test_fixtures_path


class TestConfigUtils(unittest.TestCase):
    def test_module_file_path(self):
        # Go back one step (the default)
        expected_path = str(pathlib.Path(*pathlib.Path(platform_utils_tests.__file__).resolve().parts[:-1]).resolve())
        actual_path = module_file_path("tests.observatory.platform.utils", nav_back_steps=-1)
        self.assertEqual(expected_path, actual_path)

        # Go back two steps
        expected_path = str(pathlib.Path(*pathlib.Path(platform_utils_tests.__file__).resolve().parts[:-2]).resolve())
        actual_path = module_file_path("tests.observatory.platform.utils", nav_back_steps=-2)
        self.assertEqual(expected_path, actual_path)

    @patch("observatory.platform.utils.config_utils.pathlib.Path.home")
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
        schemas_path = os.path.join(test_fixtures_path(), "telescopes")

        # Tests that don't use a prefix
        table_name = "grid"

        # 2020-09-21
        release_date = pendulum.datetime(year=2015, month=9, day=21)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNone(result)

        # 2020-09-22
        expected_schema = "grid_2015-09-22.json"
        release_date = pendulum.datetime(year=2015, month=9, day=22)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2015-09-23
        release_date = pendulum.datetime(year=2015, month=9, day=23)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2020-04-28
        expected_schema = "grid_2016-04-28.json"
        release_date = pendulum.datetime(year=2016, month=4, day=28)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2016-04-29
        release_date = pendulum.datetime(year=2016, month=4, day=29)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # Tests that use a prefix
        table_name = "Papers"
        prefix = "Mag"

        # 2020-05-20
        release_date = pendulum.datetime(year=2020, month=5, day=20)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNone(result)

        # 2020-05-21
        expected_schema = "MagPapers_2020-05-21.json"
        release_date = pendulum.datetime(year=2020, month=5, day=21)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2020-05-22
        release_date = pendulum.datetime(year=2020, month=5, day=22)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2020-06-05
        expected_schema = "MagPapers_2020-06-05.json"
        release_date = pendulum.datetime(year=2020, month=6, day=5)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2020-06-06
        release_date = pendulum.datetime(year=2020, month=6, day=6)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # Versioned example
        expected_schema = "testschema_v1_2021-04-20.json"
        release_date = pendulum.datetime(year=2021, month=4, day=20)
        result = find_schema(schemas_path, "testschema", release_date, prefix="", ver="v1")
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # No schema paths
        release_date = pendulum.datetime(year=2021, month=4, day=20)
        result = find_schema(schemas_path, "testschema", release_date, prefix="", ver="v2")
        self.assertIsNone(result)

    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_telescope_path(self, mock_variable_get):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Mock getting home path
            reset_variables()
            data_path = "data"
            mock_variable_get.return_value = data_path

            # The name of the telescope to create and expected root folder
            telescope_name = "grid"
            root_path = os.path.join(data_path, "telescopes")

            # Create subdir
            path_downloaded = telescope_path(SubFolder.downloaded, telescope_name)
            expected = os.path.join(root_path, SubFolder.downloaded.value, telescope_name)
            self.assertEqual(expected, path_downloaded)
            self.assertTrue(os.path.exists(path_downloaded))

            # Create subdir
            path_extracted = telescope_path(SubFolder.extracted, telescope_name)
            expected = os.path.join(root_path, SubFolder.extracted.value, telescope_name)
            self.assertEqual(expected, path_extracted)
            self.assertTrue(os.path.exists(path_extracted))

            # Create subdir
            path_transformed = telescope_path(SubFolder.transformed, telescope_name)
            expected = os.path.join(root_path, SubFolder.transformed.value, telescope_name)
            self.assertEqual(expected, path_transformed)
            self.assertTrue(os.path.exists(path_transformed))

    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_test_data_path(self, mock_variable_get):
        # Mock test data path variable
        expected_path = "/tmp/test_data"
        mock_variable_get.return_value = expected_path

        actual_path = test_data_path()
        self.assertEqual(expected_path, actual_path)
