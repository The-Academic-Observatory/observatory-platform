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
from typing import Union

import pendulum
from natsort import natsorted
from pendulum import Pendulum


def module_file_path(module_path: str, nav_back_steps: int = -1) -> str:
    """ Get the file path of a module, given the Python import path to the module.

    :param module_path: the Python import path to the module, e.g. observatory.platform.dags
    :param nav_back_steps: the number of steps on the path to step back.
    :return: the file path to the module.
    """

    module = importlib.import_module(module_path)
    file_path = pathlib.Path(module.__file__).resolve()
    return str(pathlib.Path(*file_path.parts[:nav_back_steps]).resolve())


def observatory_home(*subdirs) -> str:
    """ Get the .observatory Observatory Platform home directory or subdirectory on the host machine. The home
    directory and subdirectories will be created if they do not exist.

    :param: subdirs: an optional list of subdirectories.
    :return: the path.
    """

    user_home = str(pathlib.Path.home())
    observatory_home_ = os.path.join(user_home, ".observatory", *subdirs)

    if not os.path.exists(observatory_home_):
        os.makedirs(observatory_home_, exist_ok=True)

    return observatory_home_


def terraform_credentials_path() -> str:
    """ Get the path to the terraform credentials file that is created with 'terraform login'.

    :return: the path to the terraform credentials file
    """

    return os.path.join(pathlib.Path.home(), '.terraform.d/credentials.tfrc.json')


def find_schema(path: str, table_name: str, release_date: Pendulum, prefix: str = '', ver: str = None) -> Union[
    str, None]:
    """ Finds a schema file on a given path, with a particular table name, release date and optional prefix.

    If no version string is sepcified, the most recent schema with a date less than or equal to the release date of the
    dataset is returned. If a version string is specified, the most current (date) schema in that series is returned.
    Use the schema_path function to find the path to the folder containing the schemas.
    For example (grid schemas):
     - grid2015-09-22.json
     - grid2016-04-28.json
     - wos_wok5.4_2016-01-01.json  (versioned schema with version 'wok5.4')

    For GRID releases between 2015-09-22 and 2016-04-28 grid_2015-09-22.json is returned and for GRID releases or after
    2016-04-28 grid_2016-04-28.json is returned (until a new schema with a later date is added).
    Unversioned schemas are named with the following pattern: prefix + table_name + YYYY-MM-DD + .json
    Versioned schemas are named as: prefix + table_name + '_' + ver + '_' + YYYY-MM-DD + .json
    * prefix: an optional prefix for datasets with multiple tables, for instance the Microsoft Academic Graph (MAG)
    dataset schema file names are prefixed with Mag, e.g. MagAffiliations2020-05-21.json, MagAuthors2020-05-21.json.
    The GRID dataset only has one table, so there is no prefix, e.g. grid2015-09-22.json.
    * table_name: the name of the table.
    * ver: version string.
    * YYYY-MM-DD: schema file names end in the release date that the particular schema should be used from in YYYY-MM-DD
    format. For versioned schemas, this is the date of schema creation.
    * prefix and table_name follow the naming conventions of the dataset, e.g. MAG uses CamelCase for tables and fields
    so CamelCase is used. When there is no preference from the dataset then lower snake case is used.

    :param path: the path to search within.
    :param table_name: the name of the table.
    :param release_date: the release date of the table.
    :param prefix: an optional prefix.
    :param ver: Schema version.
    :return: the path to the schema or None if no schema was found.
    """

    logging.info(f'Looking for schema with search parameters: analysis_schema_path={path}, '
                  f'prefix={prefix}, table_name={table_name}, release_date={release_date}, '
                  f'version={ver}')

    # Make search path for schemas
    if ver:
        search_path = os.path.join(path, f'{prefix}{table_name}_{ver}_*.json')
    else:
        search_path = os.path.join(path, f'{prefix}{table_name}*.json')

    # Find potential schemas with a glob search and sort them naturally
    schema_paths = glob.glob(search_path)
    schema_paths = natsorted(schema_paths)

    # No schemas were found
    if len(schema_paths) == 0:
        logging.error('No schema found.')
        return None

    # Deal with versioned schema first since it's simpler. Return most recent for versioned schema.
    if ver:
        return schema_paths[-1]

    # Get schemas with dates <= release date
    prefix_len = len(prefix + table_name)
    suffix_len = 5
    selected_paths = []
    for path in schema_paths:
        file_name = os.path.basename(path)
        schema_date = pendulum.parse(file_name[prefix_len:-suffix_len].replace('_', ''))

        if schema_date <= release_date:
            selected_paths.append(path)
        else:
            break

    # Return the schema with the most recent release date
    if len(selected_paths):
        return selected_paths[-1]

    # No schemas were found
    logging.error('No schema found.')
    return None
