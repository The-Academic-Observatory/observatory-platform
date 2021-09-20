# Copyright 2020 Artificial Dimensions Ltd
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
    template = Template(contents, keep_trailing_newline=True)

    # Render template
    rendered = template.render(**kwargs)

    return rendered


def make_jinja2_filename(file_name: str) -> str:
    """Add .jinja2 to a filename.

    :param file_name: the filename without an extension.
    :return: the filename.
    """
    return f"{file_name}.jinja2"


def make_sql_jinja2_filename(file_name: str) -> str:
    """Add .sql.jinja2 to a filename.

    :param file_name: the filename without an extension.
    :return: the filename.
    """
    return f"{file_name}.sql.jinja2"
