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

from academic_observatory import docker, dags


def ao_home(*subdirs) -> str:
    """Get the Academic Observatory home directory. If the home directory doesn't exist then create it.

    :return: the Academic Observatory home directory.
    """

    user_home = str(pathlib.Path.home())
    ao_home_ = os.path.join(user_home, ".academic_observatory", *subdirs)

    if not os.path.exists(ao_home_):
        os.makedirs(ao_home_, exist_ok=True)

    return ao_home_


def docker_configs_path() -> str:
    """ Get the path to the Docker configuration files.

    :return: the path for the Docker config files.
    """

    # Recommended way to add non code files: https://python-packaging.readthedocs.io/en/latest/non-code-files.html

    file_path = pathlib.Path(docker.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1])
    return str(path.resolve())


def dags_path() -> str:
    """ Get the path to the Academic Observatory DAGs.

    :return: the path to the Academic Observatory DAGs.
    """

    # Recommended way to add non code files: https://python-packaging.readthedocs.io/en/latest/non-code-files.html

    file_path = pathlib.Path(dags.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1])
    return str(path.resolve())
