# Copyright 2022 Curtin University
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

# Author: Aniek Roelofs

import os
import unittest
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from observatory.api.cli.cli import cli


class TestGenerateCommand(unittest.TestCase):
    @patch("observatory.api.cli.cli.OpenApiRenderer", new_callable=MagicMock)
    def test_generate_openapi_spec(self, mock_openapi_renderer):
        """Test that the openapi file is generated"""
        mock_openapi_renderer().render.return_value = "rendered content"
        mock_openapi_renderer.reset_mock()
        runner = CliRunner()

        # Test generate openapi
        with runner.isolated_filesystem():
            template_file = os.path.abspath("template.jinja2")
            open(template_file, "a").close()
            output_file = os.path.abspath("openapi.yaml")

            result = runner.invoke(
                cli, ["generate-openapi-spec", template_file, output_file, "--usage-type", "cloud_endpoints"]
            )
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertTrue(os.path.isfile(output_file))
            mock_openapi_renderer.assert_called_once_with(template_file, usage_type="cloud_endpoints")

            with open(output_file, "r") as f:
                content = f.read()
            self.assertEqual("rendered content", content)
