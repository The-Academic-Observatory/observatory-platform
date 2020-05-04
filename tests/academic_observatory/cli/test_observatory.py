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
from unittest.mock import patch

from click.testing import CliRunner

from academic_observatory.cli.observatory import cli


class TestObservatory(unittest.TestCase):

    def test_platform_start_stop(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ['platform', 'start'])
            self.assertEqual(result.exit_code, os.EX_OK)

            result = runner.invoke(cli, ['platform', 'stop'])
            self.assertEqual(result.exit_code, os.EX_OK)

    @patch('academic_observatory.cli.observatory.shutil')
    def test_platform_check_dependencies(self, mock_shutil):
        # Mock shutil.which to return None which should make the command line interface print out information
        # about how to install Docker and Docker Compose and exit
        mock_shutil.which.return_value = None

        # Check that correct exit code returned and that output has links to install Docker and Docker Compose
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ['platform', 'stop'])
            self.assertEqual(result.exit_code, os.EX_CONFIG)
            self.assertIn('https://docs.docker.com/get-docker/', result.output)
            self.assertIn('https://docs.docker.com/compose/install/', result.output)
