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
import pathlib

import academic_observatory
from academic_observatory import dags


def observatory_home(*subdirs) -> str:
    """Get the Academic Observatory home directory. If the home directory doesn't exist then create it.

    :return: the Academic Observatory home directory.
    """

    user_home = str(pathlib.Path.home())
    home_ = os.path.join(user_home, ".observatory", *subdirs)

    if not os.path.exists(home_):
        os.makedirs(home_, exist_ok=True)

    return home_


def observatory_package_path() -> str:
    """ Get the path to the Academic Observatory package root folder.

    :return: the path to the Academic Observatory package root folder.
    """

    file_path = pathlib.Path(academic_observatory.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-2])
    return str(path.resolve())


def dags_path() -> str:
    """ Get the path to the Academic Observatory DAGs.

    :return: the path to the Academic Observatory DAGs.
    """

    # Recommended way to add non code files: https://python-packaging.readthedocs.io/en/latest/non-code-files.html

    file_path = pathlib.Path(dags.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1])
    return str(path.resolve())
