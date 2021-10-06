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
from datetime import datetime
from typing import Any
from unittest.mock import patch, Mock

from click.testing import CliRunner

from observatory.platform.cli.platform_command import PlatformCommand
from observatory.platform.observatory_config import Observatory, ObservatoryConfig, Backend, Environment, BackendType
from observatory.platform.utils.test_utils import find_free_port, save_empty_file


class MockUrlOpen(Mock):
    def __init__(self, status: int, **kwargs: Any):
        super().__init__(**kwargs)
        self.status = status

    def getcode(self):
        return self.status


class TestPlatformCommand(unittest.TestCase):
    def setUp(self) -> None:
        self.config_file_name = "config.yaml"
        self.observatory_platform_package_name = "observatory-platform"
        self.airflow_fernet_key = "fernet-key"
        self.airflow_secret_key = "secret-key"
        self.airflow_ui_port = find_free_port()

    def make_observatory_config(self, t):
        """Make an ObservatoryConfig instance.

        :param t: the temporary path.
        :return: the ObservatoryConfig.
        """

        return ObservatoryConfig(
            backend=Backend(type=BackendType.local, environment=Environment.develop),
            observatory=Observatory(
                package=os.path.join(t, self.observatory_platform_package_name),
                package_type="editable",
                airflow_fernet_key=self.airflow_fernet_key,
                airflow_secret_key=self.airflow_secret_key,
                observatory_home=t,
                airflow_ui_port=find_free_port(),
            ),
        )

    @patch("observatory.platform.docker.platform_runner.ObservatoryConfig.load")
    def test_ui_url(self, mock_config_load):
        with CliRunner().isolated_filesystem() as t:
            # Save empty config
            config_path = save_empty_file(t, self.config_file_name)

            # Make config
            config = self.make_observatory_config(t)
            mock_config_load.return_value = config

            # Test that ui URL is correct
            cmd = PlatformCommand(config_path)
            cmd.config.observatory.airflow_ui_port = self.airflow_ui_port

            self.assertEqual(f"http://localhost:{self.airflow_ui_port}", cmd.ui_url)

    @patch("observatory.platform.docker.platform_runner.ObservatoryConfig.load")
    @patch("urllib.request.urlopen")
    def test_wait_for_airflow_ui_success(self, mock_url_open, mock_config_load):
        # Mock the status code return value: 200 should succeed
        mock_url_open.return_value = MockUrlOpen(200)

        with CliRunner().isolated_filesystem() as t:
            # Save empty config
            config_path = save_empty_file(t, self.config_file_name)

            # Make config
            config = self.make_observatory_config(t)
            mock_config_load.return_value = config

            # Test that ui connects
            cmd = PlatformCommand(config_path)
            start = datetime.now()
            state = cmd.wait_for_airflow_ui()
            end = datetime.now()
            duration = (end - start).total_seconds()

            self.assertTrue(state)
            self.assertAlmostEquals(0, duration, delta=0.5)

    @patch("observatory.platform.docker.platform_runner.ObservatoryConfig.load")
    @patch("urllib.request.urlopen")
    def test_wait_for_airflow_ui_failed(self, mock_url_open, mock_config_load):
        # Mock the status code return value: 500 should fail
        mock_url_open.return_value = MockUrlOpen(500)

        with CliRunner().isolated_filesystem() as t:
            # Save empty config
            config_path = save_empty_file(t, self.config_file_name)

            # Make config
            config = self.make_observatory_config(t)
            mock_config_load.return_value = config

            # Test that ui error
            cmd = PlatformCommand(config_path)
            expected_timeout = 10
            start = datetime.now()
            state = cmd.wait_for_airflow_ui(expected_timeout)
            end = datetime.now()
            duration = (end - start).total_seconds()

            self.assertFalse(state)
            self.assertAlmostEquals(expected_timeout, duration, delta=1)
