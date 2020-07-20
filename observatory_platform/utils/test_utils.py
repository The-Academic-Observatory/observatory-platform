# Copyright 2019 Curtin University
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

import os
import subprocess
from pathlib import Path
from subprocess import Popen

from observatory_platform.utils.proc_utils import wait_for_process


def gzip_file_crc(file_path: str) -> str:
    """ Get the crc of a gzip file.

    :param file_path: the path to the file.
    :return: the crc.
    """

    proc: Popen = subprocess.Popen(['gzip', '-vl', file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = wait_for_process(proc)
    return output.splitlines()[1].split(' ')[1].strip()


def fixtures_data_dir(current_file):
    """ Get the path to the Observatory Platform test data directory.

    :param current_file: pass the `__file__` handle from the test you are running.
    :return: the Observatory Platform test data directory.
    """

    path = str(Path(current_file).resolve())
    subdir = os.path.join('observatory-platform', 'tests')

    assert subdir in path, f"`{subdir}` not found in path `{path}`"

    start_i = path.find(subdir)
    end_i = start_i + len(subdir)
    tests_path = path[:end_i]
    return os.path.join(tests_path, 'fixtures')
