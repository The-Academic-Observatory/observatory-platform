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

import unittest
from unittest.mock import patch

from click.testing import CliRunner
from observatory.api.cli.openapi_renderer import OpenApiRenderer, render_template


class TestOpenApiRenderer(unittest.TestCase):
    def test_render_template(self):
        template_path = "openapi.yaml.jinja2"
        template = "{# Test comment #}Some jinja2 {{ variable1 }} and another {{ variable2 }}"
        expected_render = "Some jinja2 variable and another different variable"

        with CliRunner().isolated_filesystem():
            # Write test template
            with open(template_path, "w") as f:
                f.write(template)

            # Render template and test that we get expected render
            render = render_template(
                template_path,
                variable1="variable",
                variable2="different variable",
            )
            self.assertEqual(render, expected_render)

    def test_openapi_renderer(self):
        template_path = "openapi.yaml.jinja2"

        # Test cloud endpoints
        renderer = OpenApiRenderer(template_path, "cloud_endpoints")
        self.assertEqual(template_path, renderer.openapi_template_path)
        self.assertEqual("cloud_endpoints", renderer.type)

        # Test backend
        renderer = OpenApiRenderer(template_path, "backend")
        self.assertEqual(template_path, renderer.openapi_template_path)
        self.assertEqual("backend", renderer.type)

        # Test openapi_generator
        renderer = OpenApiRenderer(template_path, "openapi_generator")
        self.assertEqual(template_path, renderer.openapi_template_path)
        self.assertEqual("openapi_generator", renderer.type)

        # Test invalid usage type
        with self.assertRaises(TypeError):
            OpenApiRenderer(template_path, "invalid_type")

    @patch("observatory.api.cli.openapi_renderer.render_template")
    def test_render(self, mock_render_template):
        mock_render_template.return_value = "rendered content"

        template_path = "openapi.yaml.jinja2"
        renderer = OpenApiRenderer(template_path, "cloud_endpoints")
        result = renderer.render()

        mock_render_template.assert_called_once_with(renderer.openapi_template_path, type=renderer.type)
        self.assertEqual("rendered content", result)

    def test_to_dict(self):
        template_path = "openapi.yaml.jinja2"
        template = "paths:\n  /my_endpoint:\n    foo: bar"

        with CliRunner().isolated_filesystem():
            with open(template_path, "w") as f:
                f.write(template)

            renderer = OpenApiRenderer(template_path, "backend")
            result = renderer.to_dict()
            self.assertDictEqual({"paths": {"/my_endpoint": {"foo": "bar"}}}, result)
