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

# Author: James Diprose

import os
import unittest

from click.testing import CliRunner

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
        config_path = 'config.yaml'

        with CliRunner().isolated_filesystem():
            cmd.generate_local_config(config_path)
            self.assertTrue(os.path.exists(config_path))

    def test_generate_terraform_config(self):
        cmd = GenerateCommand()
        config_path = 'config-terraform.yaml'

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_config(config_path)
            self.assertTrue(os.path.exists(config_path))
