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

import pathlib
import os
import unittest
from typing import Any
from unittest.mock import Mock
from unittest.mock import patch
from click.testing import CliRunner
import requests

import observatory.platform.docker as docker_module
from observatory.platform.cli.cli import (REDIS_PORT, FLOWER_UI_PORT, ELASTIC_PORT, KIBANA_PORT,
                                          DOCKER_NETWORK_NAME, DEBUG, AIRFLOW_UI_PORT, HOST_UID, HOST_GID)
from observatory.platform.platform_builder import PlatformBuilder
from observatory.platform.observatory_config import ObservatoryConfig

class MockFromEnv(Mock):
    def __init__(self, is_running: bool, **kwargs: Any):
        super().__init__(**kwargs)
        self.is_running = is_running

    def ping(self):
        if self.is_running:
            return True
        raise requests.exceptions.ConnectionError()


class TestPlatformBuilder(unittest.TestCase):

    def setUp(self) -> None:
        self.is_env_local = True
        self.config_path = 'config.yaml'
        self.build_path = 'build'
        self.dags_path = 'dags'
        self.data_path = 'data'
        self.logs_path = 'logs'
        self.postgres_path = 'postgres'

    def make_platform_command(self):
        return PlatformBuilder(self.config_path, build_path=self.build_path, dags_path=self.dags_path,
                               data_path=self.data_path, logs_path=self.logs_path, postgres_path=self.postgres_path)

    def test_is_environment_valid(self):
        cmd = self.make_platform_command()

    def test_docker_module_path(self):
        """ Test that the path to the Docker module  is found """

        cmd = self.make_platform_command()
        expected_path = str(pathlib.Path(*pathlib.Path(docker_module.__file__).resolve().parts[:-1]).resolve())
        self.assertEqual(expected_path, cmd.docker_module_path)

    def test_docker_exe_path(self):
        """ Test that the path to the Docker executable is found """

        cmd = self.make_platform_command()
        result = cmd.docker_exe_path
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith('docker'))

    def test_docker_compose_path(self):
        """ Test that the path to the Docker Compose executable is found """

        cmd = self.make_platform_command()
        result = cmd.docker_compose_path
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith('docker-compose'))

    @patch('observatory.platform.platform_builder.docker.from_env')
    def test_is_docker_running_true(self, mock_from_env):
        """ Test the property is_docker_running returns True when Docker is running  """

        mock_from_env.return_value = MockFromEnv(True)
        cmd = self.make_platform_command()
        self.assertTrue(cmd.is_docker_running)

    @patch('observatory.platform.platform_builder.docker.from_env')
    def test_is_docker_running_false(self, mock_from_env):
        """ Test the property is_docker_running returns False when Docker is not running  """

        mock_from_env.return_value = MockFromEnv(False)
        cmd = self.make_platform_command()
        self.assertFalse(cmd.is_docker_running)

    def test_make_observatory_files(self):
        """ Test   """

        with CliRunner().isolated_filesystem():
            # Save default config file
            ObservatoryConfig.save_default(self.config_path)

            # Make directories
            os.makedirs(self.dags_path, exist_ok=True)
            os.makedirs(self.data_path, exist_ok=True)
            os.makedirs(self.logs_path, exist_ok=True)
            os.makedirs(self.postgres_path, exist_ok=True)

            # Make observatory files
            cmd = self.make_platform_command()
            cmd.make_observatory_files()
            a = 1


    def test_make_environment(self):
        pass

    def test_build(self):
        pass

    def test_start_stop(self):
        pass
