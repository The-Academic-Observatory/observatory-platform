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
    def __init__(self, openapi_template_path: str, api_client: bool = False):
        """Construct an object that renders an OpenAPI 2 Jinja2 file.

        :param openapi_template_path: the path to the OpenAPI 2 Jinja2 template.
        :param api_client: whether to render the file for the Server (default) or the Client.
        """

        self.openapi_template_path = openapi_template_path
        self.api_client = api_client

    def render(self) -> str:
        """Render the OpenAPI file.

        :return: the rendered output.
        """

        return render_template(
            self.openapi_template_path,
            api_client=self.api_client,
        )

    def to_dict(self) -> Dict:
        """Render and output the OpenAPI file as a dictionary.

        :return: the dictionary.
        """

        return yaml.safe_load(self.render())
