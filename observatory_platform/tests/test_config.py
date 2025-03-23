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
import tempfile
import unittest
from unittest.mock import patch

import observatory_platform.tests as platform_utils_tests
from observatory_platform.config import module_file_path, observatory_home


class TestConfig(unittest.TestCase):
    def test_module_file_path(self):
        # Go back one step (the default)
        expected_path = str(pathlib.Path(*pathlib.Path(platform_utils_tests.__file__).resolve().parts[:-1]).resolve())
        actual_path = module_file_path("observatory_platform.tests", nav_back_steps=-1)
        self.assertEqual(expected_path, actual_path)

        # Go back two steps
        expected_path = str(pathlib.Path(*pathlib.Path(platform_utils_tests.__file__).resolve().parts[:-1]).resolve())
        actual_path = module_file_path("observatory_platform.tests.fixtures", nav_back_steps=-2)
        self.assertEqual(expected_path, actual_path)

    @patch("observatory_platform.config.pathlib.Path.home")
    def test_observatory_home(self, home_mock):
        with tempfile.TemporaryDirectory() as t:
            # Create home path and mock getting home path
            home_path = os.path.join(t, "user-home")
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            # Test that observatory home works
            path = observatory_home()
            self.assertTrue(os.path.exists(path))
            self.assertEqual(os.path.join(home_path, ".observatory"), path)

            # Test that subdirectories are created
            path = observatory_home("subfolder")
            self.assertTrue(os.path.exists(path))
            self.assertEqual(os.path.join(home_path, ".observatory", "subfolder"), path)
