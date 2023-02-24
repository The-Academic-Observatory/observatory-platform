# Copyright 2019, 2020 Curtin University
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

# Author: James Diprose, Aniek Roelofs


import importlib
import os
import pathlib


class AirflowVars:
    DATA_PATH = "data_path"
    WORKFLOWS = "workflows"
    DAGS_MODULE_NAMES = "dags_module_names"


class AirflowConns:
    SLACK = "slack"
    TERRAFORM = "terraform"
    OBSERVATORY_API = "observatory_api"


class Tag:
    """DAG tag."""

    observatory_platform = "observatory-platform"


def module_file_path(module_path: str, nav_back_steps: int = -1) -> str:
    """Get the file path of a module, given the Python import path to the module.

    :param module_path: the Python import path to the module, e.g. observatory.platform.dags
    :param nav_back_steps: the number of steps on the path to step back.
    :return: the file path to the module.
    """

    module = importlib.import_module(module_path)
    file_path = pathlib.Path(module.__file__).resolve()
    return os.path.normpath(str(pathlib.Path(*file_path.parts[:nav_back_steps]).resolve()))


def observatory_home(*subdirs) -> str:
    """Get the .observatory Observatory Platform home directory or subdirectory on the host machine. The home
    directory and subdirectories will be created if they do not exist.

    The default path: ~/.observatory can be overridden with the environment variable OBSERVATORY_HOME.

    :param: subdirs: an optional list of subdirectories.
    :return: the path.
    """

    obs_home = os.environ.get("OBSERVATORY_HOME", None)
    if obs_home is not None:
        path = os.path.join(os.path.expanduser(obs_home), *subdirs)
    else:
        user_home = str(pathlib.Path.home())
        path = os.path.join(user_home, ".observatory", *subdirs)

    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    return path


def terraform_credentials_path() -> str:
    """Get the path to the terraform credentials file that is created with 'terraform login'.

    :return: the path to the terraform credentials file
    """

    return os.path.join(pathlib.Path.home(), ".terraform.d/credentials.tfrc.json")


def sql_templates_path() -> str:
    """Return the path to the SQL templates.

    :return: the path.
    """

    return module_file_path("observatory.platform.sql")
