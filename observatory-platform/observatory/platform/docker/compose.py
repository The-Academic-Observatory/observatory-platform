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
from typing import Dict
from typing import List

from observatory.platform.utils.proc_utils import stream_process


@dataclasses.dataclass
class ProcessOutput:
    output: str
    error: str
    return_code: int


class ComposeRunnerInterface(ABC):
    @abstractmethod
    def make_environment(self) -> Dict:
        pass

    @abstractmethod
    def start(self) -> ProcessOutput:
        pass

    @abstractmethod
    def stop(self) -> ProcessOutput:
        pass


class ComposeRunner(ComposeRunnerInterface):
    COMPOSE_ARGS_PREFIX = ["docker-compose", "-f"]
    COMPOSE_BUILD_ARGS = ["build"]
    COMPOSE_START_ARGS = ["up", "-d"]
    COMPOSE_STOP_ARGS = ["down"]

    def __init__(self, *, compose_file_path: str, build_path: str, debug: bool = False):
        self.compose_file_path = compose_file_path
        self.build_path = build_path
        self.debug = debug

    def start(self) -> ProcessOutput:
        """ Start the Observatory Platform.

        :return: output and error stream results and proc return code.
        """

        return self.__run_docker_compose_cmd(self.COMPOSE_START_ARGS)

    def stop(self) -> ProcessOutput:
        """ Stop the Observatory Platform.

        :return: output and error stream results and proc return code.
        """

        return self.__run_docker_compose_cmd(self.COMPOSE_STOP_ARGS)

    def __run_docker_compose_cmd(self, args: List) -> ProcessOutput:
        """ Run a set of Docker Compose arguments.

        :param args: the list of arguments.
        :return: output and error stream results and proc return code.
        """

        # Make environment
        env = self.make_environment()

        # Copy compose file to build directory
        build_file_path = os.path.join(self.build_path, os.path.basename(self.compose_file_path))
        shutil.copy(self.compose_file_path, build_file_path)

        compose_file_name = os.path.basename(self.compose_file_path)
        # Build the containers first
        proc: Popen = subprocess.Popen(
            self.COMPOSE_ARGS_PREFIX + [compose_file_name] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=self.build_path,
        )

        # Wait for results
        output, error = stream_process(proc, self.debug)

        return ProcessOutput(output, error, proc.returncode)
