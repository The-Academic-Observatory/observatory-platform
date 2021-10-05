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

import dataclasses
import os
import shutil
from abc import ABC, abstractmethod
from typing import Dict, List

from observatory.platform.utils.jinja2_utils import render_template


class BuilderInterface(ABC):
    """An interface for building Docker and DockerCompose systems."""

    @abstractmethod
    def add_template(self, *, path: str, **kwargs):
        """Add a Jinja template that will be rendered to the build directory before running Docker Compose commands.

        :param path: the path to the Jinja2 template.
        :param kwargs: the kwargs to use when rendering the template.
        :return: None
        """

        pass

    @abstractmethod
    def add_file(self, *, path: str, output_file_name: str):
        """Add a file that will be copied to the build directory before running the Docker Compose commands.

        :param path: a path to the file.
        :param output_file_name: the output file name.
        :return: None
        """

        pass

    @abstractmethod
    def make_files(self) -> None:
        """Render all Jinja templates and copy all files into the build directory.

        :return: None.
        """


def rendered_file_name(template_file_path: str):
    """Make the rendered file name from the Jinja template name.

    :param template_file_path: the file path to the Jinja2 template.
    :return: the output file name.
    """

    return os.path.basename(template_file_path).replace(".jinja2", "")


@dataclasses.dataclass
class File:
    """A file to copy.

    :param path: the path to the file.
    :param output_file_name: the output file.
    """

    path: str
    output_file_name: str


@dataclasses.dataclass
class Template:
    """A Jinja2 Template to render.

    :param path: the path to the Jinja2 template.
    :param kwargs: the kwargs to use when rendering the template.
    """

    path: str
    kwargs: Dict

    @property
    def output_file_name(self) -> str:
        """Make the output file name.

        :return: the output file name.
        """

        return rendered_file_name(self.path)


class Builder(BuilderInterface):
    def __init__(self, *, build_path: str):
        """BuilderInterface implementation.

        :param build_path: the path where the system will be built.
        """

        self.build_path = build_path
        self.templates: List[Template] = []
        self.files: List[File] = []

    def add_template(self, *, path: str, **kwargs):
        """Add a Jinja template that will be rendered to the build directory before running Docker Compose commands.

        :param path: the path to the Jinja2 template.
        :param kwargs: the kwargs to use when rendering the template.
        :return: None
        """

        self.templates.append(Template(path=path, kwargs=kwargs))

    def add_file(self, *, path: str, output_file_name: str):
        """Add a file that will be copied to the build directory before running the Docker Compose commands.

        :param path: a path to the file.
        :param output_file_name: the output file name.
        :return: None
        """

        self.files.append(File(path=path, output_file_name=output_file_name))

    def render_template(self, template: Template, output_file_path: str):
        """Render a file using a Jinja template and save.

        :param template: the template.
        :param output_file_path: the output path.
        :return: None.
        """

        render = render_template(template.path, **template.kwargs)
        with open(output_file_path, "w") as f:
            f.write(render)

    def make_files(self):
        """Render all Jinja templates and copy all files into the build directory.

        :return: None.
        """

        # Clear Docker directory and make build path
        if os.path.exists(self.build_path):
            shutil.rmtree(self.build_path)
        os.makedirs(self.build_path)

        # Render templates
        for template in self.templates:
            output_path = os.path.join(self.build_path, template.output_file_name)
            self.render_template(template, output_path)

        # Copy files
        for file in self.files:
            output_path = os.path.join(self.build_path, file.output_file_name)
            shutil.copy(file.path, output_path)


