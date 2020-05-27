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

from academic_observatory.cli.observatory import cli, ObservatoryConfig, gen_fernet_key


def not_linux():
    return not sys.platform.startswith('linux')


class TestObservatory(unittest.TestCase):

    @unittest.skipIf(not_linux(), "Only runs on Linux")
    def test_platform_start_stop(self):
        runner = CliRunner()
        config_file_path = '/tmp/config.yaml'
        credentials_file_path = '/tmp/cred.json'
        env = {
            'FERNET_KEY': gen_fernet_key(),
            'GOOGLE_APPLICATION_CREDENTIALS': config_file_path
        }

        # Make config file
        config = ObservatoryConfig.make_default()
        config.project_id = 'my-project-id'
        config.bucket_name = 'my-bucket-name'
        config.save(config_file_path)

        # Make a fake google application credentials
        with open(credentials_file_path, 'w') as f:
            f.write('')

        result = runner.invoke(cli, ['platform', 'start', '--config-path', config_file_path], env=env)
        self.assertEqual(result.exit_code, os.EX_OK)

        result = runner.invoke(cli, ['platform', 'stop', '--config-path', config_file_path], env=env)
        self.assertEqual(result.exit_code, os.EX_OK)

        try:
            pathlib.Path(config_file_path).unlink()
        except FileNotFoundError:
            pass

        try:
            pathlib.Path(credentials_file_path).unlink()
        except FileNotFoundError:
            pass

    @unittest.skipIf(not_linux(), "Only runs on Linux")
    @patch('academic_observatory.cli.observatory.is_docker_installed')
    @patch('academic_observatory.cli.observatory.is_docker_compose_installed')
    @patch('academic_observatory.cli.observatory.is_docker_running')
    def test_platform_check_dependencies(self, mock_is_docker_installed, mock_is_docker_compose_installed,
                                         mock_is_docker_running):
        # Mock to return None which should make the command line interface print out information
        # about how to install Docker and Docker Compose and exit
        mock_is_docker_installed.return_value = False
        mock_is_docker_compose_installed.return_value = False
        mock_is_docker_running.return_value = False

        # Make sure no config file
        config_file_path = '/tmp/config.yaml'
        try:
            pathlib.Path(config_file_path).unlink()
        except FileNotFoundError:
            pass

        # Check that correct exit code returned and that output has links to install Docker and Docker Compose
        runner = CliRunner()

        env = {
            'GOOGLE_APPLICATION_CREDENTIALS': None,
            'FERNET_KEY': None
        }
        result = runner.invoke(cli, ['platform', 'start', '--config-path', config_file_path], env=env)

        # Docker not installed
        self.assertIn('https://docs.docker.com/get-docker/', result.output)

        # Docker Compose not installed
        self.assertIn('https://docs.docker.com/compose/install/', result.output)

        # Docker not running
        self.assertIn('Docker: not running, please start', result.output)

        # GOOGLE_APPLICATION_CREDENTIALS
        self.assertIn('https://cloud.google.com/docs/authentication/getting-started', result.output)

        # Fernet key
        self.assertIn('FERNET_KEY: environment variable not set', result.output)

        # config.yaml
        self.assertIn('config.yaml: not found so generating a default file', result.output)

        # Check return code
        self.assertEqual(result.exit_code, os.EX_CONFIG)

        # Test that invalid config errors show up
        config = ObservatoryConfig.make_default()
        config.save(config_file_path)
        result = runner.invoke(cli, ['platform', 'start', '--config-path', config_file_path], env=env)

        # Invalid config and which properties
        self.assertIn('config.yaml: invalid', result.output)
        self.assertIn('bucket_name: null value not allowed', result.output)
        self.assertIn('project_id: null value not allowed', result.output)

        # Check return code
        self.assertEqual(result.exit_code, os.EX_CONFIG)

        try:
            pathlib.Path(config_file_path).unlink()
        except FileNotFoundError:
            pass
