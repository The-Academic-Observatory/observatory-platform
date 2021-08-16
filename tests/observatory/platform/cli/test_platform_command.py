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
import random
import unittest
from datetime import datetime
from typing import Any
from unittest.mock import patch, Mock

from click.testing import CliRunner

from observatory.platform.cli.platform_command import PlatformCommand
from observatory.platform.observatory_config import save_yaml


class MockUrlOpen(Mock):
    def __init__(self, status: int, **kwargs: Any):
        super().__init__(**kwargs)
        self.status = status

    def getcode(self):
        return self.status


class TestPlatformCommand(unittest.TestCase):

    def setUp(self) -> None:
        self.airflow_ui_port = random.randint(1, 65535)

    def save_config(self, file_path: str, observatory_home: str):
        dict_ = {
            "backend": {"type": "local", "environment": "develop"},
            "observatory": {
                "observatory_home": observatory_home,
                "airflow_fernet_key": "random-fernet-key",
                "airflow_secret_key": "random-secret-key",
            }
        }

        save_yaml(file_path, dict_)

    def test_ui_url(self):
        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformCommand(config_path)
            cmd.config.observatory.airflow_ui_port = self.airflow_ui_port

            self.assertEqual(f'http://localhost:{self.airflow_ui_port}', cmd.ui_url)

    @patch('urllib.request.urlopen')
    def test_wait_for_airflow_ui_success(self, mock_url_open):
        # Mock the status code return value: 200 should succeed
        mock_url_open.return_value = MockUrlOpen(200)

        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)

            cmd = PlatformCommand(config_path)
            cmd.config.observatory.airflow_ui_port = self.airflow_ui_port

            start = datetime.now()
            state = cmd.wait_for_airflow_ui()
            end = datetime.now()
            duration = (end - start).total_seconds()

            self.assertTrue(state)
            self.assertAlmostEquals(0, duration, delta=0.5)

    @patch('urllib.request.urlopen')
    def test_wait_for_airflow_ui_failed(self, mock_url_open):
        # Mock the status code return value: 500 should fail
        mock_url_open.return_value = MockUrlOpen(500)

        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformCommand(config_path)

            expected_timeout = 10
            start = datetime.now()
            state = cmd.wait_for_airflow_ui(expected_timeout)
            end = datetime.now()
            duration = (end - start).total_seconds()

            self.assertFalse(state)
            self.assertAlmostEquals(expected_timeout, duration, delta=1)
