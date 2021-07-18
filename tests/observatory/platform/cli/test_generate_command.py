# Copyright 2020 Curtin University
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
import unittest
from unittest.mock import Mock, patch

import click
from click.testing import CliRunner
from observatory.platform.cli.cli import generate
from observatory.platform.cli.generate_command import GenerateCommand


class TestGenerateCommand(unittest.TestCase):
    def test_generate_fernet_key(self):
        cmd = GenerateCommand()

        # Test that keys are random
        num_keys = 100
        random_keys = [cmd.generate_fernet_key() for _ in range(num_keys)]
        self.assertEqual(len(set(random_keys)), num_keys)

        # Test that keys have length of 44
        expected_key_len = 44
        [self.assertEqual(expected_key_len, len(key)) for key in random_keys]

    def test_generate_local_config(self):
        cmd = GenerateCommand()
        config_path = "config.yaml"

        with CliRunner().isolated_filesystem():
            cmd.generate_local_config(config_path)
            self.assertTrue(os.path.exists(config_path))

    def test_generate_terraform_config(self):
        cmd = GenerateCommand()
        config_path = "config-terraform.yaml"

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_config(config_path)
            self.assertTrue(os.path.exists(config_path))

    @patch("observatory.platform.cli.generate_command.open")
    def test_generate_telescope_telescope(self, mock_open):
        # Cannot do filesystem isolation since we are writing explicit paths.
        runner = CliRunner()
        result = runner.invoke(generate, ["telescope", "Telescope", "MyTestTelescope"])
        self.assertEqual(result.exit_code, 0)
        call_args = mock_open.call_args_list
        dagfile = os.path.basename(call_args[0][0][0])
        telescopefile = os.path.basename(call_args[1][0][0])
        self.assertEqual(dagfile, "mytesttelescope.py")
        self.assertEqual(telescopefile, "mytesttelescope.py")

        result = runner.invoke(generate, ["telescope", "StreamTelescope", "MyTestTelescope"])
        self.assertEqual(result.exit_code, 0)
        call_args = mock_open.call_args_list
        dagfile = os.path.basename(call_args[2][0][0])
        telescopefile = os.path.basename(call_args[3][0][0])
        self.assertEqual(dagfile, "mytesttelescope.py")
        self.assertEqual(telescopefile, "mytesttelescope.py")

        result = runner.invoke(generate, ["telescope", "SnapshotTelescope", "MyTestTelescope"])
        self.assertEqual(result.exit_code, 0)
        call_args = mock_open.call_args_list
        dagfile = os.path.basename(call_args[4][0][0])
        telescopefile = os.path.basename(call_args[5][0][0])
        self.assertEqual(dagfile, "mytesttelescope.py")
        self.assertEqual(telescopefile, "mytesttelescope.py")

        result = runner.invoke(generate, ["telescope", "unknown", "MyTestTelscope"])
        self.assertEqual(result.exit_code, 1)
