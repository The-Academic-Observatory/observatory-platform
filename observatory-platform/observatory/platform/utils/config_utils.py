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


import glob
import importlib
import logging
import os
import pathlib
from enum import Enum
from typing import Union

import airflow
import pendulum
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from airflow.utils.db import create_session
from natsort import natsorted
from pendulum import Pendulum

from observatory.platform.utils.airflow_utils import AirflowVariable

# The path where data is saved on the system
data_path = None
test_data_path_val_ = None


class AirflowVars:
    """ Common Airflow Variable names used with the Observatory Platform """

    TEST_DATA_PATH = "test_data_path"
    DATA_PATH = "data_path"
    ENVIRONMENT = "environment"
    PROJECT_ID = "project_id"
    DATA_LOCATION = "data_location"
    DOWNLOAD_BUCKET = "download_bucket"
    TRANSFORM_BUCKET = "transform_bucket"
    TERRAFORM_ORGANIZATION = "terraform_organization"
    DAGS_MODULE_NAMES = "dags_module_names"


class AirflowConns:
    """ Common Airflow Connection names used with the Observatory Platform """

    CROSSREF = "crossref"
    MAG_RELEASES_TABLE = "mag_releases_table"
    MAG_SNAPSHOTS_CONTAINER = "mag_snapshots_container"
    TERRAFORM = "terraform"
    SLACK = "slack"


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


def find_schema(path: str, table_name: str, release_date: Pendulum, prefix: str = '', ver: str = '') \
        -> Union[str, None]:
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

    # Make search path for schemas
    if ver != '':
        search_path = os.path.join(path, f'{prefix}{table_name}_{ver}_*.json')
    else:
        search_path = os.path.join(path, f'{prefix}{table_name}*.json')

    # Find potential schemas with a glob search and sort them naturally
    schema_paths = glob.glob(search_path)
    schema_paths = natsorted(schema_paths)

    # No schemas were found
    if len(schema_paths) == 0:
        return None

    # Deal with versioned schema first since it's simpler. Return most recent for versioned schema.
    if ver != '':
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
    return None


def check_variables(*variables):
    """ Checks whether all given airflow variables exist.

    :param variables: name of airflow variable
    :return: True if all variables are valid
    """
    is_valid = True
    for name in variables:
        try:
            AirflowVariable.get(name)
        except KeyError:
            logging.error(f"Airflow variable '{name}' not set.")
            is_valid = False
    return is_valid


def check_connections(*connections):
    """ Checks whether all given airflow connections exist.

    :param connections: name of airflow connection
    :return: True if all connections are valid
    """
    is_valid = True
    for name in connections:
        try:
            BaseHook.get_connection(name)
        except KeyError:
            logging.error(f"Airflow connection '{name}' not set.")
            is_valid = False
    return is_valid


def list_connections(source):
    """ Get a list of data source connections with name starting with <source>_, e.g., wos_curtin.

    :param source: Data source (conforming to name convention) as a string, e.g., 'wos'.
    :return: A list of connection id strings with the prefix <source>_, e.g., ['wos_curtin', 'wos_auckland'].
    """
    with create_session() as session:
        query = session.query(Connection)
        query = query.filter(Connection.conn_id.like(f'{source}_%'))
        return query.all()


class SubFolder(Enum):
    """ The type of subfolder to create for telescope data """

    downloaded = 'download'
    extracted = 'extract'
    transformed = 'transform'


def telescope_path(sub_folder: SubFolder, name: str) -> str:
    """ Return a path for saving telescope data. Create it if it doesn't exist.

    :param sub_folder: the name of the sub folder for the telescope
    :param name: the name of the telescope.
    :return: the path.
    """

    # To avoid hitting the airflow database and the secret backend unnecessarily, data path is stored as a global
    # variable and only requested once.
    global data_path
    if data_path is None:
        logging.info('telescope_path: requesting data_path variable')
        data_path = airflow.models.Variable.get(AirflowVars.DATA_PATH)

    # Create telescope path
    path = os.path.join(data_path, 'telescopes', sub_folder.value, name)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    return path


def test_data_path() -> str:
    """ Return the path for the test data.

    :return: the path.
    """

    # To avoid hitting the airflow database and the secret backend unnecessarily, data path is stored as a global
    # variable and only requested once.
    global test_data_path_val_
    if test_data_path_val_ is None:
        logging.info('test_data_path: requesting test_data_path variable')
        test_data_path_val_ = airflow.models.Variable.get(AirflowVars.TEST_DATA_PATH)

    return test_data_path_val_
