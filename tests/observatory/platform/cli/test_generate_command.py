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

import logging
import os
import unittest

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

    def test_generate_workflow(self):
        """ Test generate workflow """

        # Test valid workflow types
        package_name = "my_workflows_package"
        workflow_module = "my_test_workflow"
        workflow_types = ["Workflow", "SnapshotTelescope", "StreamTelescope"]
        for workflow_type in workflow_types:
            runner = CliRunner()
            with runner.isolated_filesystem() as t:
                result = runner.invoke(generate, ["workflow", t, package_name, workflow_type, "MyTestWorkflow"])
                self.assertEqual(result.exit_code, 0)
                expected_files = [
                    os.path.join(t, package_name, "__init__.py"),
                    os.path.join(t, package_name, "dags", f"{workflow_module}.py"),
                    os.path.join(t, package_name, "dags", "__init__.py"),
                    os.path.join(t, package_name, "workflows", f"{workflow_module}.py"),
                    os.path.join(t, package_name, "workflows", "__init__.py"),
                    os.path.join(t, "docs", "index.rst"),
                    os.path.join(t, "docs", f"{workflow_module}.md"),
                    os.path.join(t, "tests", package_name, "workflows", "__init__.py"),
                    os.path.join(t, "tests", package_name, "workflows", f"test_{workflow_module}.py"),
                    os.path.join(t, "tests", package_name, "__init__.py"),
                ]
                for file_path in expected_files:
                    logging.info(file_path)
                    self.assertTrue(os.path.isfile(file_path))

        # Test invalid workflow types
        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            result = CliRunner().invoke(generate, ["workflow", t, "my_workflows_package", "Unknown", "MyTestTelescope"])
            self.assertEqual(result.exit_code, 2)
            self.assertTrue("invalid choice: Unknown" in result.output)
