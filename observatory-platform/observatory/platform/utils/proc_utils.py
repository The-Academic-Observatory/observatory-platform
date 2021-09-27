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
import subprocess
from subprocess import Popen
from typing import List, Tuple, Union

from airflow.exceptions import AirflowException


def wait_for_process(proc: Popen) -> Tuple[str, str]:
    """Wait for a process to finish, returning the std output and std error streams as strings.

    :param proc: the process object.
    :return: std output and std error streams as strings.
    """
    output, error = proc.communicate()
    output = output.decode("utf-8")
    error = error.decode("utf-8")
    return output, error


def stream_process(proc: Popen, debug: bool) -> Tuple[str, str]:
    """Print output while a process is running, returning the std output and std error streams as strings.

    :param proc: the process object.
    :param debug: whether debug info should be displayed.
    :return: std output and std error streams as strings.
    """
    output_concat = ""
    error_concat = ""
    while True:
        for line in proc.stdout:
            output = line.decode("utf-8")
            if debug:
                print(output, end="")
            output_concat += output
        for line in proc.stderr:
            error = line.decode("utf-8")
            print(error, end="")
            error_concat += error
        if proc.poll() is not None:
            break
    return output_concat, error_concat
