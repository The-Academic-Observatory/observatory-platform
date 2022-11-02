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

"""
Utility functions for creating the config files that are used with the local development and/or terraform environment.
"""

import glob
import importlib
import logging
import os
import pathlib
import re
from typing import Union

import pendulum
from natsort import natsorted


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


def find_schema(
    path: str,
    table_name: str,
    release_date: pendulum.DateTime = None,
    prefix: str = "",
) -> Union[str, None]:

    """Finds a schema file on a given path, with a particular table name, optional release date and prefix.

    Depending on the input and available files in the directory, this function's return will change
    If no release date is specified, will attempt to find a schema without a date (schema.json)
    If a release date is specified, the schema with a date <= to the release date is used (schema_1970-01-01.json)
    When looking for a schema with a date, the schema with the most recent date prior to the release date is preferred

    Examples:
    Say there is a schema_folder containing the following files:
        - table_1900-01-01.json
        - table_2000-01-01.json
        - table.json
    find_schema(schema_folder, table) -> table.json
    find_schema(schema_folder, table, release_date=2020-01-01) -> table_2000-01-01.json
    find_schema(schema_folder, table, release_date=1980-01-01) -> table_1900-01-01.json
    find_schema(schema_folder, table_2) -> None

    Now if schema_folder's contents change to
        - table.json
    find_schema(schema_folder, table) -> table.json
    find_schema(schema_folder, table, release_date=2020-01-01) -> None
    find_schema(schema_folder, table, release_date=1980-01-01) -> None
    find_schema(schema_folder, table_2) -> None

    :param path: the path to search within.
    :param table_name: the name of the table.
    :param release_date: the release date of the table.
    :param prefix: an optional prefix.
    :return: the path to the schema or None if no schema was found.
    """

    logging.info(
        f"Looking for schema with search parameters: analysis_schema_path={path}, "
        f"prefix={prefix}, table_name={table_name}, release_date={release_date}, "
    )

    # Make search path for schemas
    # Schema format: "prefix_table_name_YYY-MM-DD.json"
    date_re = "_[0-9]{4}-[0-9]{2}-[0-9]{2}" if release_date else ""
    re_string = f"{prefix}{table_name}{date_re}.json"
    search_pattern = re.compile(re_string)

    # Find potential schemas with a glob search
    schema_paths = glob.glob(os.path.join(path, "*"))

    # Filter the paths with the regex pattern
    file_names = [os.path.basename(i) for i in schema_paths]
    filtered_names = list(filter(search_pattern.match, file_names))
    filtered_paths = [os.path.join(path, i) for i in filtered_names]

    # No schemas were found
    if len(filtered_paths) == 0:
        logging.error("No schemas were found")
        return None

    # No release date supplied
    if not release_date:
        return filtered_paths[0]

    # Sort schema paths naturally
    filtered_paths = natsorted(filtered_paths)

    # Get schemas with dates <= release date
    suffix_len = 5  # .json
    date_str_len = 10  # YYYY-MM-DD
    selected_paths = []
    for path in filtered_paths:
        file_name = os.path.basename(path)
        date_str_start = -(date_str_len + suffix_len)
        date_str_end = -suffix_len
        datestr = file_name[date_str_start:date_str_end]
        schema_date = pendulum.parse(datestr)

        if schema_date <= release_date:
            selected_paths.append(path)
        else:
            break

    # Return the schema with the most recent release date
    if len(selected_paths):
        return selected_paths[-1]

    # No schemas were found
    logging.error("No schema found.")
    return None


def utils_templates_path() -> str:
    """Return the path to the util templates.

    :return: the path.
    """

    return module_file_path("observatory.platform.utils.templates")
