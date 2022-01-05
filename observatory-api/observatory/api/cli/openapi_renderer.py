# Copyright 2020 Artificial Dimensions Ltd
# Copyright 2020-2021 Curtin University
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

from typing import Dict

import yaml
from jinja2 import Template


def render_template(template_path: str, **kwargs) -> str:
    """Render a Jinja2 template.

    :param template_path: the path to the template.
    :param kwargs: the keyword variables to populate the template with.
    :return: the rendered template as a string.
    """

    # Read file contents
    with open(template_path, "r") as file:
        contents = file.read()

    # Fill template with text
    template = Template(contents)

    # Render template
    rendered = template.render(**kwargs)

    return rendered


class OpenApiRenderer:
    def __init__(self, openapi_template_path: str, usage_type: str):
        """Construct an object that renders an OpenAPI 2 Jinja2 file.

        :param openapi_template_path: the path to the OpenAPI 2 Jinja2 template.
        :param usage_type: The usage type for which the OpenAPI file is generated, one of: cloud_endpoints, backend,
            openapi_generator
        """

        self.openapi_template_path = openapi_template_path
        allowed_types = ["cloud_endpoints", "backend", "openapi_generator"]
        if usage_type not in allowed_types:
            raise TypeError(f"Given type is not one of the allowed types: {allowed_types}")
        self.type = usage_type

    def render(self) -> str:
        """Render the OpenAPI file.

        :return: the rendered output.
        """

        return render_template(self.openapi_template_path, type=self.type)

    def to_dict(self) -> Dict:
        """Render and output the OpenAPI file as a dictionary.

        :return: the dictionary.
        """

        assert self.type == "backend", "Only supported when the openapi config file is used for the backend"
        return yaml.safe_load(self.render())


# if __name__ == '__main__':
#     template_path = sys.argv[1]
#     build_path = sys.argv[2]
#     builder = OpenApiRenderer(template_path, cloud_endpoints=True, api_client=False)
#     with open(build_path, "w") as f:
#         f.write(builder.render())
