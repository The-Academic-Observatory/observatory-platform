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
import subprocess
from subprocess import Popen
from typing import Dict, List

from observatory.platform.docker.builder import Builder, rendered_file_name
from observatory.platform.utils.proc_utils import stream_process


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


class ComposeRunner(Builder):
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

        super().__init__(build_path=build_path)
        if compose_template_kwargs is None:
            compose_template_kwargs = dict()

        self.debug = debug
        self.compose_template_path = compose_template_path
        self.add_template(path=compose_template_path, **compose_template_kwargs)

    def make_environment(self) -> Dict:
        """Make the environment variables.

        :return: environment dictionary.
        """

        return os.environ.copy()

    @property
    def compose_file_name(self):
        """Return the file name for the rendered Docker Compose template.

        :return: Docker Compose file name.
        """

        return rendered_file_name(self.compose_template_path)

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
        output, error = stream_process(proc, self.debug)

        return ProcessOutput(output, error, proc.returncode)
