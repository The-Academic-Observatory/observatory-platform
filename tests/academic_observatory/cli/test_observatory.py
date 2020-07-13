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
import pathlib
import sys
import unittest
from unittest.mock import patch

from click.testing import CliRunner

from academic_observatory.cli.observatory import cli, ObservatoryConfig


def not_linux():
    return not sys.platform.startswith('linux')


class TestObservatory(unittest.TestCase):

    @unittest.skipIf(not_linux(), "Only runs on Linux")
    def test_platform_start_stop(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # File paths
            working_dir = pathlib.Path().absolute()
            config_file_path = os.path.join(working_dir, 'config.yaml')
            credentials_file_path = os.path.join(working_dir, 'google_application_credentials.json')

            # Make config file
            config = ObservatoryConfig.make_default()
            config.project_id = 'my-project-id'
            config.data_location = 'us'
            config.download_bucket_name = 'my-project-download-bucket'
            config.transform_bucket_name = 'my-project-transform-bucket'
            config.google_application_credentials = credentials_file_path
            config.save(config_file_path)

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
    @patch('academic_observatory.cli.observatory.get_docker_path')
    @patch('academic_observatory.cli.observatory.get_docker_compose_path')
    @patch('academic_observatory.cli.observatory.is_docker_running')
    def test_platform_check_dependencies(self, mock_is_docker_running, mock_get_docker_compose_path,
                                         mock_get_docker_path):
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
            config_file_path = os.path.join(pathlib.Path().absolute(), 'config.yaml')

            # Create default config file
            config = ObservatoryConfig.make_default()
            config.save(config_file_path)
            result = runner.invoke(cli, ['platform', 'start', '--config-path', config_file_path])

            # Invalid config and which properties
            self.assertIn(f'config.yaml:\n   - path: {config_file_path}\n   - file invalid\n', result.output)
            self.assertIn('project_id: null value not allowed', result.output)
            self.assertIn('data_location: null value not allowed', result.output)
            self.assertIn('download_bucket_name: null value not allowed', result.output)
            self.assertIn('transform_bucket_name: null value not allowed', result.output)
            self.assertIn('google_application_credentials: null value not allowed', result.output)

            # Check that Docker is not running message printed
            self.assertIn('Docker:\n   - path: /docker\n   - not running, please start', result.output)

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)

        with runner.isolated_filesystem():
            # File paths
            working_dir = pathlib.Path().absolute()
            config_file_path = os.path.join(working_dir, 'config.yaml')
            credentials_file_path = os.path.join(working_dir, 'google_application_credentials.json')

            # Test that error message is printed when Google Credentials env variable is set but file doesn't exist
            config = ObservatoryConfig.make_default()
            config.google_application_credentials = credentials_file_path
            config.save(config_file_path)
            result = runner.invoke(cli, ['platform', 'start', '--config-path', config_file_path])

            # Check that google credentials file does not exist is printed
            self.assertIn(f'google_application_credentials: the file {credentials_file_path} does not exist',
                          result.output)

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)
