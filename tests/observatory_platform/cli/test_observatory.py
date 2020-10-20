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

# Author: James Diprose, Aniek Roelofs

import fileinput
import json
import os
import pathlib
import sys
import unittest
from datetime import datetime
from unittest.mock import patch

from click.testing import CliRunner

from observatory_platform.cli.observatory import ObservatoryConfig, cli, create_terraform_variables, gen_fernet_key
from observatory_platform.utils.config_utils import terraform_variables_tf_path
from observatory_platform.utils.terraform_utils import TerraformApi


def not_linux():
    return not sys.platform.startswith('linux')


def replace_value_in_file(config_path: str, key: str, value: str):
    for line in fileinput.input(config_path, inplace=True):
        if key in line:
            complete_key = line.split(':')[0]
            line = ': '.join([complete_key, value]) + '\n'
        print(line, end='')


class TestObservatoryPlatform(unittest.TestCase):

    @unittest.skipIf(not_linux(), "Only runs on Linux")
    def test_platform_start_stop(self):
        """ Test that the observatory platform start and stop return the correct exit codes """
        runner = CliRunner()
        with runner.isolated_filesystem():
            # File paths
            working_dir = pathlib.Path().absolute()
            config_file_path = os.path.join(working_dir, 'config.yaml')
            credentials_file_path = os.path.join(working_dir, 'google_application_credentials.json')

            # Make config file
            ObservatoryConfig.save_default('local', config_file_path, test=True)
            replace_value_in_file(config_file_path, 'google_application_credentials', credentials_file_path)

            # Make a fake google application credentials as it is required by the secret
            with open(credentials_file_path, 'w') as f:
                f.write('')

            # Test that start command works
            result = runner.invoke(cli, ['platform', 'start', '--config-path', config_file_path])
            self.assertEqual(result.exit_code, os.EX_OK)

            # Test that stop command works
            result = runner.invoke(cli, ['platform', 'stop', '--config-path', config_file_path])
            self.assertEqual(result.exit_code, os.EX_OK)

    @unittest.skipIf(not_linux(), "Only runs on Linux")
    @patch('observatory_platform.cli.observatory.get_docker_path')
    @patch('observatory_platform.cli.observatory.get_docker_compose_path')
    @patch('observatory_platform.cli.observatory.is_docker_running')
    def test_platform_check_dependencies(self, mock_is_docker_running, mock_get_docker_compose_path,
                                         mock_get_docker_path):
        """ Test that the correct output is printed for various missing dependencies """
        runner = CliRunner()
        with runner.isolated_filesystem():
            # No config file should exist because we are in a new isolated filesystem
            config_file_path = os.path.join(pathlib.Path().absolute(), 'config.yaml')

            # Mock to return None which should make the command line interface print out information
            # about how to install Docker and Docker Compose and exit
            mock_get_docker_path.return_value = None
            mock_get_docker_compose_path.return_value = None
            mock_is_docker_running.return_value = False

            # Check that correct exit code returned and that output has links to install Docker and Docker Compose
            result = runner.invoke(cli, ['platform', 'start', '--config-path', config_file_path])

            # Docker not installed
            self.assertIn('https://docs.docker.com/get-docker/', result.output)

            # Docker Compose not installed
            self.assertIn('https://docs.docker.com/compose/install/', result.output)

            # config.yaml
            self.assertIn('- file not found, generating a default file', result.output)

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)

        with runner.isolated_filesystem():
            # Test that invalid config errors show up
            # Test that error message is printed when Docker is installed but not running
            mock_get_docker_path.return_value = '/docker'
            mock_get_docker_compose_path.return_value = '/docker-compose'
            mock_is_docker_running.return_value = False

            # File paths
            working_dir = pathlib.Path().absolute()
            config_file_path = os.path.join(working_dir, 'config.yaml')

            # Create default config file
            ObservatoryConfig.save_default('local', config_file_path, test=False)
            config = ObservatoryConfig(config_file_path)
            result = runner.invoke(cli, ['platform', 'start', '--config-path', config_file_path])

            # Invalid config and which properties
            self.assertIn(f'config.yaml:\n   - path: {config_file_path}\n   - file invalid\n', result.output)
            for key, value in config.dict.items():
                # string is printed from config_utils.customise_pointer
                if isinstance(value, str) and value.endswith(' <--'):
                    self.assertIn(f"- {key}: Value should be customised, can't end with ' <--'", result.output)
                elif isinstance(value, dict):
                    for key2, value2 in value.items():
                        if isinstance(value2, str) and value2.endswith(' <--'):
                            self.assertIn(f"- {key2}: Value should be customised, can't end with ' <--'", result.output)

            # Check that google credentials file does not exist is printed
            self.assertIn(f'google_application_credentials: the file {config.local.google_application_credentials} '
                          f'does not exist', result.output)

            # Check that Docker is not running message printed
            self.assertIn('Docker:\n   - path: /docker\n   - not running, please start', result.output)

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)


class TestObservatoryTerraform(unittest.TestCase):
    # TODO change to 'observatory-unit-testing' once latest version is available
    organisation = 'COKI-project'
    workspaces_prefix = datetime.now().strftime("%Y-%M-%d_%H-%M-%S-%f")
    token = os.getenv('TESTS_TERRAFORM_TOKEN')
    terraform_api = TerraformApi(token)

    def test_create_terraform_variables(self):
        """ Test that all variables in config file are correctly formatted to variable 'attributes' dict"""
        with CliRunner().isolated_filesystem():
            # File paths
            working_dir = pathlib.Path().absolute()
            config_file_path = os.path.join(working_dir, 'config.yaml')
            # Create default config file
            ObservatoryConfig.save_default('terraform', config_file_path, terraform_variables_tf_path(), test=True)
            # Initiate config
            config = ObservatoryConfig(config_file_path)
            # Create variables
            terraform_variables = create_terraform_variables(config)
            for var in terraform_variables:
                self.assertIsInstance(var, dict)
                self.assertEqual(var.keys(), {'key', 'value', 'description', 'hcl', 'sensitive', 'category'})

                if var['key'] == 'backend':
                    for key, value in var.items():
                        if key == 'description':
                            self.assertEqual(value, None)
                        else:
                            self.assertIsInstance(value, str)
                else:
                    for value in var.values():
                        self.assertIsInstance(value, str)

    @patch("click.confirm")
    def test_terraform_create_update(self, mock_click_confirm):
        """ Test creating and updating a terraform cloud workspace"""
        # Create token json
        token_json = {
            "credentials": {
                "app.terraform.io": {
                    "token": self.token
                }
            }
        }
        runner = CliRunner()
        with runner.isolated_filesystem():
            # File paths
            working_dir = pathlib.Path().absolute()
            terraform_credentials_path = os.path.join(working_dir, 'token.json')
            config_file_path = os.path.join(working_dir, 'config_terraform.yaml')
            credentials_file_path = os.path.join(working_dir, 'google_application_credentials.json')

            # Create config file
            ObservatoryConfig.save_default('terraform', config_file_path, terraform_variables_tf_path(), test=True)
            # Update 'organisation', 'workspace' and 'google_application_credentials'
            replace_value_in_file(config_file_path, 'organization', self.organisation)
            replace_value_in_file(config_file_path, 'workspaces_prefix', self.workspaces_prefix)
            replace_value_in_file(config_file_path, 'google_application_credentials', credentials_file_path)

            # Create token file
            with open(terraform_credentials_path, 'w') as f:
                json.dump(token_json, f)

            # Make a fake google application credentials as it is required schema validation
            with open(credentials_file_path, 'w') as f:
                f.write('')

            # Create config instance
            config = ObservatoryConfig(config_file_path)
            # Create terraform api instance
            terraform_api = TerraformApi(self.token)
            workspace = self.workspaces_prefix + config.terraform.environment

            # As a safety measure, delete workspace even though it shouldn't exist yet
            terraform_api.delete_workspace(self.organisation, workspace)

            # Create workspace, confirm yes
            mock_click_confirm.return_value = 'y'
            result = runner.invoke(cli,
                                   ['terraform', 'create-workspace', config_file_path, '--terraform-credentials-file',
                                    terraform_credentials_path])
            self.assertIn("Successfully created workspace", result.output)

            # Create workspace, confirm no
            mock_click_confirm.return_value = False
            result = runner.invoke(cli,
                                   ['terraform', 'create-workspace', config_file_path, '--terraform-credentials-file',
                                    terraform_credentials_path])
            self.assertNotIn("Creating workspace...", result.output)

            # Update workspace, same config file but sensitive values will be replaced
            mock_click_confirm.return_value = 'y'
            result = runner.invoke(cli,
                                   ['terraform', 'update-workspace', config_file_path, '--terraform-credentials-file',
                                    terraform_credentials_path])
            self.assertIn("Successfully updated workspace", result.output)

            # Update workspace, confirm no
            mock_click_confirm.return_value = False
            result = runner.invoke(cli,
                                   ['terraform', 'update-workspace', config_file_path, '--terraform-credentials-file',
                                    terraform_credentials_path])
            self.assertNotIn("Updating workspace...", result.output)

            # Delete workspace
            terraform_api.delete_workspace(self.organisation, workspace)

    def test_terraform_check_dependencies(self):
        """ Test that checking for dependencies prints the correct output when files are missing"""
        runner = CliRunner()
        with runner.isolated_filesystem():
            working_dir = pathlib.Path().absolute()
            # No config file should exist because we are in a new isolated filesystem
            config_file_path = os.path.join(working_dir, 'config_terraform.yaml')

            # Check that correct exit code and output are returned
            result = runner.invoke(cli, ['terraform', 'create-workspace', config_file_path])
            # No config file
            self.assertIn(f"Error: Invalid value for 'CONFIG_PATH': File '{config_file_path}' does not exist.",
                          result.output)
            # Check return code, exit from click invalid option
            self.assertEqual(result.exit_code, 2)

            terraform_credentials_path = os.path.join(working_dir, 'credentials.tfrc.json')
            terraform_variables_path = os.path.join(working_dir, 'variables.tf')

            # Run again with existing config, specifying terraform files that don't exist. Check that correct exit
            # code and output are returned
            ObservatoryConfig.save_default('terraform', config_file_path, terraform_variables_tf_path(), test=True)
            result = runner.invoke(cli,
                                   ['terraform', 'create-workspace', config_file_path, '--terraform-credentials-file',
                                    terraform_credentials_path, '--terraform-variables-path', terraform_variables_path])
            # No terraform credentials file
            self.assertIn("Terraform credentials file:\n   - file not found, create one by running 'terraform login'",
                          result.output)
            # No terraform variables file
            self.assertIn("Terraform variables.tf file:\n   - file not found, check path to your variables file",
                          result.output)
            self.assertIn("- need terraform variables file to determine whether file is valid", result.output)
            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)


class TestObservatoryGenerate(unittest.TestCase):
    def test_gen_fernet_key(self):
        """ Test that generate fernet key returns a string """
        fernet_key = gen_fernet_key()
        self.assertIsInstance(fernet_key, str)

    @patch("click.confirm")
    def test_generate(self, mock_click_confirm):
        """ Test that the fernet key and default config files are generated """
        runner = CliRunner()
        with runner.isolated_filesystem():
            # test generate fernet key
            result = runner.invoke(cli, ['generate', 'fernet-key'])
            self.assertEqual(result.exit_code, os.EX_OK)

            # test generate local config
            mock_click_confirm.return_value = 'y'
            result = runner.invoke(cli, ['generate', 'config.yaml'])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertTrue(os.path.isfile(ObservatoryConfig.LOCAL_DEFAULT_PATH))

            # test generate terraform config
            mock_click_confirm.return_value = 'y'
            result = runner.invoke(cli, ['generate', 'config_terraform.yaml'])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertTrue(os.path.isfile(ObservatoryConfig.TERRAFORM_DEFAULT_PATH))
