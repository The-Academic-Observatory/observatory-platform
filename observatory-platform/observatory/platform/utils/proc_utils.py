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

import logging
from subprocess import Popen
from typing import Tuple


def wait_for_process(proc: Popen) -> Tuple[str, str]:
    """Wait for a process to finish, returning the std output and std error streams as strings.

    :param proc: the process object.
    :return: std output and std error streams as strings.
    """
    output, error = proc.communicate()
    output = output.decode("utf-8")
    error = error.decode("utf-8")
    return output, error


def stream_process(proc: Popen) -> bool:
    """Print output while a process is running, returning the std output and std error streams as strings.

    :param proc: the process object.
    :return: whether the command was successful or not.
    """

    while True:
        if proc.poll() is not None:
            break

        if proc.stdout:
            output = proc.stdout.readline()
            logging.debug(output)

        if proc.stderr:
            error = proc.stderr.readline()
            logging.error(error)

    # Check if the process had a non-zero exit code (indicating an error)
    if proc.returncode != 0:
        logging.error(f"Command failed with return code {proc.returncode}")
        return False

    return True
