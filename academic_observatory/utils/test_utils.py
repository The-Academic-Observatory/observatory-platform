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
from pathlib import Path


def test_data_dir(current_file):
    """ Get the path to the Academic Observatory test data directory.

    :param current_file: pass the `__file__` handle from the test you are running.
    :return: the Academic Observatory test data directory.
    """

    path = str(Path(current_file).resolve())
    subdir = os.path.join('academic-observatory', 'tests')

    assert subdir in path, f"`{subdir}` not found in path `{path}`"

    start_i = path.find(subdir)
    end_i = start_i + len(subdir)
    tests_path = path[:end_i]
    return os.path.join(tests_path, 'data')

def fixtures_data_dir(current_file):
    """ Get the path to the Academic Observatory test data directory.

    :param current_file: pass the `__file__` handle from the test you are running.
    :return: the Academic Observatory test data directory.
    """

    path = str(Path(current_file).resolve())
    subdir = os.path.join('academic-observatory', 'tests')

    assert subdir in path, f"`{subdir}` not found in path `{path}`"

    start_i = path.find(subdir)
    end_i = start_i + len(subdir)
    tests_path = path[:end_i]
    return os.path.join(tests_path, 'fixtures')