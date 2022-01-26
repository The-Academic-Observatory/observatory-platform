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
import subprocess
from abc import ABC, abstractmethod
from subprocess import Popen
from typing import Dict, List

from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.proc_utils import wait_for_process


@dataclasses.dataclass
class ProcessOutput:
    """Output from a process

    :param output: the process std out.
    :param error: the process std error.
    :param return_code: the process return code.
    """

    output: str
    error: str
    return_code: int


class ComposeRunnerInterface(ABC):
    """An interface for running Docker Compose commands"""

    @abstractmethod
    def make_environment(self) -> Dict:
        """Make the environment variables.

        :return: environment dictionary.
        """
        pass

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

    @abstractmethod
    def build(self) -> ProcessOutput:
        """Build Docker Compose.

        :return: ProcessOutput.
        """
        pass

    @abstractmethod
    def start(self) -> ProcessOutput:
        """Start Docker Compose.

        :return: ProcessOutput.
        """
        pass

    @abstractmethod
    def stop(self) -> ProcessOutput:
        """Stop Docker Compose.

        :return: ProcessOutput.
        """
        pass


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


class ComposeRunner(ComposeRunnerInterface):
    COMPOSE_ARGS_PREFIX = ["docker-compose", "-f"]
    COMPOSE_BUILD_ARGS = ["build"]
    COMPOSE_START_ARGS = ["up", "-d"]
    COMPOSE_STOP_ARGS = ["down"]

    def __init__(
        self, *, compose_template_path: str, build_path: str, compose_template_kwargs: Dict = None, debug: bool = False
    ):
        """Docker Compose runner constructor.

        :param compose_template_path: the path to the Docker Compose Jinja2 template file.
        :param build_path: the path where Docker will build.
        :param compose_template_kwargs: the kwargs to use when rendering the Docker Compose Jinja2 template file.
        :param debug: whether to run in debug mode or not.
        """

        self.compose_template_path = compose_template_path
        self.build_path = build_path
        self.templates: List[Template] = []
        self.files: List[File] = []
        self.add_template(path=compose_template_path, **compose_template_kwargs)
        self.debug = debug

    @property
    def compose_file_name(self):
        """Return the file name for the rendered Docker Compose template.

        :return: Docker Compose file name.
        """

        return rendered_file_name(self.compose_template_path)

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

    def build(self) -> ProcessOutput:
        """Build the Docker containers.

        :return: output and error stream results and proc return code.
        """

        return self.__run_docker_compose_cmd(self.COMPOSE_BUILD_ARGS)

    def start(self) -> ProcessOutput:
        """Start the Docker containers.

        :return: output and error stream results and proc return code.
        """

        return self.__run_docker_compose_cmd(self.COMPOSE_START_ARGS)

    def stop(self) -> ProcessOutput:
        """Stop the Docker containers.

        :return: output and error stream results and proc return code.
        """

        return self.__run_docker_compose_cmd(self.COMPOSE_STOP_ARGS)

    def render_template(self, template: Template, output_file_path: str):
        """Render a file using a Jinja template and save.

        :param template: the template.
        :param output_file_path: the output path.
        :return: None.
        """

        render = render_template(template.path, **template.kwargs)
        with open(output_file_path, "w") as f:
            f.write(render)

    def __run_docker_compose_cmd(self, args: List) -> ProcessOutput:
        """Run a set of Docker Compose arguments.

        :param args: the list of arguments.
        :return: output and error stream results and proc return code.
        """

        # Make environment
        env = self.make_environment()

        # Make files
        self.make_files()

        # Build the containers first
        proc: Popen = subprocess.Popen(
            self.COMPOSE_ARGS_PREFIX + [self.compose_file_name] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=self.build_path,
        )

        # Wait for results
        output, error = wait_for_process(proc)

        return ProcessOutput(output, error, proc.returncode)
