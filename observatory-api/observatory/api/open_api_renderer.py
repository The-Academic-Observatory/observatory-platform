from typing import Dict

import yaml
from jinja2 import Template

from observatory.api.elastic import QUERY_FILTER_PARAMETERS


def render_template(template_path: str, **kwargs) -> str:
    """ Render a Jinja2 template.

    :param template_path: the path to the template.
    :param kwargs: the keyword variables to populate the template with.
    :return: the rendered template as a string.
    """

    # Read file contents
    with open(template_path, 'r') as file:
        contents = file.read()

    # Fill template with text
    template = Template(contents)

    # Render template
    rendered = template.render(**kwargs)

    return rendered


class OpenApiRenderer:

    def __init__(self, openapi_template_path: str, cloud_endpoints: bool = False):
        self.openapi_template_path = openapi_template_path
        self.cloud_endpoints = cloud_endpoints

    def render(self):
        return render_template(self.openapi_template_path, cloud_endpoints=self.cloud_endpoints,
                               query_filter_parameters=QUERY_FILTER_PARAMETERS)

    def to_dict(self) -> Dict:
        assert not self.cloud_endpoints, "Only supported where self.cloud_endpoints is False"
        return yaml.safe_load(self.render())
