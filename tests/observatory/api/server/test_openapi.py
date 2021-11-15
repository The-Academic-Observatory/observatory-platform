# Copyright 2021 Curtin University
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

from click.testing import CliRunner
from openapi_spec_validator import validate_v2_spec
from openapi_spec_validator.readers import read_from_filename

from observatory.api.cli.openapi_renderer import OpenApiRenderer
from observatory.platform.utils.config_utils import module_file_path


class TestOpenApiSchema(unittest.TestCase):
    def setUp(self) -> None:
        self.template_file = os.path.join(module_file_path("observatory.api.server"), "openapi.yaml.jinja2")

    def test_validate_backend(self):
        """Test that the backend OpenAPI spec is valid"""

        renderer = OpenApiRenderer(self.template_file, usage_type="backend")
        render = renderer.render()
        self.validate_spec(render)

    def test_validate_cloud_endpoints(self):
        """Test that the cloud endpoints OpenAPI spec is valid"""

        renderer = OpenApiRenderer(self.template_file, usage_type="cloud_endpoints")
        render = renderer.render()
        render = render.replace("${host}", "api.observatory.academy")
        render = render.replace("${backend_address}", "192.168.1.1")
        self.validate_spec(render)

    def test_validate_api_client(self):
        """Test that the API Client OpenAPI spec is valid"""

        renderer = OpenApiRenderer(self.template_file, usage_type="openapi_generator")
        render = renderer.render()
        self.validate_spec(render)

    def validate_spec(self, render: str):
        with CliRunner().isolated_filesystem():
            file_name = "openapi.yaml"
            with open(file_name, mode="w") as f:
                f.write(render)

            spec_dict, spec_url = read_from_filename(file_name)
            validate_v2_spec(spec_dict)
