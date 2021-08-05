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

# Author: James Diprose, Tuan Chien, Aniek Roelofs

import os
import unittest
from click.testing import CliRunner
from datetime import datetime
from unittest.mock import patch, call

from observatory.platform.cli.cli import generate
from observatory.platform.cli.generate_command import GenerateCommand
from observatory.platform.utils.config_utils import module_file_path


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
        """ Test generate telescope command. Cannot do filesystem isolation since we are writing explicit paths.

        :param mock_open: mock the 'open' function
        :return: None.
        """
        # Create expected file paths
        observatory_dir = module_file_path('observatory.platform', nav_back_steps=-4)
        dag_dst_dir = module_file_path('observatory.dags.dags')
        dag_dst_file = os.path.join(dag_dst_dir, "my_test_telescope.py")

        telescope_dst_dir = module_file_path('observatory.dags.telescopes')
        telescope_dst_file = os.path.join(telescope_dst_dir, "my_test_telescope.py")

        test_dst_dir = os.path.join(observatory_dir, 'tests', 'observatory', 'dags', 'telescopes')
        test_dst_file = os.path.join(test_dst_dir, "test_my_test_telescope.py")

        doc_dst_dir = os.path.join(observatory_dir, 'docs', 'telescopes')
        doc_dst_file = os.path.join(doc_dst_dir, "my_test_telescope.md")

        schema_dst_dir = module_file_path('observatory.dags.database.schema')
        schema_dst_file = os.path.join(schema_dst_dir, f"my_test_telescope_{datetime.now().strftime('%Y-%m-%d')}.json")

        doc_index_file = os.path.join(doc_dst_dir, "index.rst")

        for telescope_type in ['Telescope', 'StreamTelescope', 'SnapshotTelescope']:
            mock_open.reset_mock()
            result = CliRunner().invoke(generate, ["telescope", telescope_type, "MyTestTelescope",
                                                   "Firstname Lastname"])
            self.assertEqual(result.exit_code, 0)
            expected_call_list = [call(file, 'w') for file in [dag_dst_file, telescope_dst_file, test_dst_file,
                                                               doc_dst_file, schema_dst_file]]
            expected_call_list.append(call(doc_index_file, 'a'))
            self.assertListEqual(expected_call_list, mock_open.call_args_list)