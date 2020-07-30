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
import logging
import os
import pathlib
from enum import Enum
from typing import Union, Dict

import airflow
import cerberus.validator
import pendulum
import yaml
from airflow.hooks.base_hook import BaseHook
from cerberus import Validator
from cryptography.fernet import Fernet
from natsort import natsorted
from pendulum import Pendulum

import observatory_platform.database
from observatory_platform import dags

# The path where data is saved on the system
data_path = None


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


def observatory_package_path() -> str:
    """ Get the path to the Observatory Platform package root folder.

    :return: the path to the Observatory Platform package root folder.
    """

    file_path = pathlib.Path(observatory_platform.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-2])
    return str(path.resolve())


def dags_path() -> str:
    """ Get the path to the Observatory Platform DAGs.

    :return: the path to the Observatory Platform DAGs.
    """

    # Recommended way to add non code files: https://python-packaging.readthedocs.io/en/latest/non-code-files.html

    file_path = pathlib.Path(dags.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1])
    return str(path.resolve())


def schema_path(database: str) -> str:
    """ Get the absolute path to a schema folder.

    :param database: the name of the database, e.g. telescopes, platform, analysis etc.
    :return: the schema folder.
    """
    file_path = pathlib.Path(observatory_platform.database.__file__).resolve()
    return str(pathlib.Path(*file_path.parts[:-1], database, 'schema').resolve())


def find_schema(path: str, table_name: str, release_date: Pendulum, prefix: str = '') -> Union[str, None]:
    """ Finds a schema file on a given path, with a particular table name, release date and optional prefix.
    The most recent schema with a date less than or equal to the release date of the dataset is returned.
    Use the schema_path function to find the path to the folder containing the schemas.

    For example (grid schemas):
     - grid2015-09-22.json
     - grid2016-04-28.json

    For GRID releases between 2015-09-22 and 2016-04-28 grid_2015-09-22.json is returned and for GRID releases or after
    2016-04-28 grid_2016-04-28.json is returned (until a new schema with a later date is added).

    Schemas are named with the following pattern: prefix + table_name + YYYY-MM-DD + .json
    * prefix: an optional prefix for datasets with multiple tables, for instance the Microsoft Academic Graph (MAG)
    dataset schema file names are prefixed with Mag, e.g. MagAffiliations2020-05-21.json, MagAuthors2020-05-21.json.
    The GRID dataset only has one table, so there is no prefix, e.g. grid2015-09-22.json.
    * table_name: the name of the table.
    * YYYY-MM-DD: schema file names end in the release date that the particular schema should be used from in YYYY-MM-DD
    format.
    * prefix and table_name follow the naming conventions of the dataset, e.g. MAG uses CamelCase for tables and fields
    so CamelCase is used. When there is no preference from the dataset then lower snake case is used.

    :param path: the path to search within.
    :param table_name: the name of the table.
    :param release_date: the release date of the table.
    :param prefix: an optional prefix.
    :return: the path to the schema or None if no schema was found.
    """

    # Make search path for schemas
    search_path = os.path.join(path, f'{prefix}{table_name}*.json')

    # Find potential schemas with a glob search and sort them naturally
    schema_paths = glob.glob(search_path)
    schema_paths = natsorted(schema_paths)

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


class SubFolder(Enum):
    """ The type of subfolder to create for telescope data """

    downloaded = 'download'
    extracted = 'extract'
    transformed = 'transform'


def check_variables(*variables):
    is_valid = True
    for name in variables:
        try:
            airflow.models.Variable.get(name)
        except KeyError:
            logging.error(f"Airflow variable '{name}' not set.")
            is_valid = False
    return is_valid


def check_connections(*connections):
    is_valid = True
    for name in connections:
        try:
            BaseHook.get_connection(name)
        except KeyError:
            logging.error(f"Airflow connection '{name}' not set.")
            is_valid = False
    return is_valid


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
        data_path = airflow.models.Variable.get("data_path")

    # Create telescope path
    path = os.path.join(data_path, 'telescopes', sub_folder.value, name)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    return path


class Environment(Enum):
    """ The environment being used """

    dev = 'dev'
    test = 'test'
    prod = 'prod'


class AirflowVar(Enum):
    data_path = {'name': 'data_path', 'default': None, 'schema': {}}
    environment = {'name': 'environment', 'default': Environment.dev.value,
                   'schema': {'type': 'string', 'allowed': ['dev', 'test', 'prod'], 'required': True}}
    project_id = {'name': 'project_id', 'default': None,
                  'schema': {'type': 'string', 'required': True}}
    data_location = {'name': 'data_location', 'default': None,
                     'schema': {'type': 'string', 'required': True}}
    download_bucket_name = {'name': 'download_bucket_name', 'default': None,
                            'schema': {'type': 'string', 'required': True}}
    transform_bucket_name = {'name': 'transform_bucket_name', 'default': None,
                             'schema': {'type': 'string', 'required': True}}

    def get(self):
        return self.value['name']


class AirflowConn(Enum):
    crossref = {'name': 'crossref', 'default': 'mysql://:crossref-token@',
                'schema': {'type': 'string', 'required': False}}
    mag_releases_table = {'name': 'mag_releases_table', 'default': 'mysql://azure-storage-account-name:url-encoded'
                                                                   '-sas-token@',
                          'schema': {'type': 'string', 'required': False}}
    mag_snapshots_container = {'name': 'mag_snapshots_container', 'default': 'mysql://azure-storage-account-name:url'
                                                                             '-encoded-sas-token@',
                               'schema': {'type': 'string', 'required': False}}

    def get(self):
        return self.value['name']


def dict_default(airflow_class):
    # TODO add typing for airflow_class
    dictionary = {}
    for obj in airflow_class:
        if obj.value['schema']:
            dictionary[obj.value['name']] = obj.value['default']
    return dictionary


class ObservatoryConfigValidator(Validator):

    def _validate_google_application_credentials(self, google_application_credentials, field, value):
        """ Validate that the Google Application Credentials file exists.

        The rule's arguments are validated against this schema: {'type': 'boolean'}
        """
        if google_application_credentials and value is not None and isinstance(value, str) and \
                not os.path.isfile(value):
            self._error(field, f"the file {value} does not exist. See "
                               f"https://cloud.google.com/docs/authentication/getting-started for instructions on "
                               f"how to create a service account and save the JSON key to your workstation.")


class ObservatoryConfig:
    HOST_DEFAULT_PATH = os.path.join(observatory_home(), 'config.yaml')
    schema = {
        'fernet_key': {
            'required': True,
            'type': 'string'
        },
        'google_application_credentials': {
            'type': 'string',
            'google_application_credentials': True,
            'required': True
        },
        'airflow_connections': {
            'required': False,
            'type': 'dict',
            'schema': {
            }
        },
        'airflow_variables': {
            'required': False,
            'type': 'dict',
            'schema': {
            }
        }
    }
    for airflow_conn in AirflowConn:
        if airflow_conn.value['schema']:
            schema['airflow_connections']['schema'][airflow_conn.get()] = airflow_conn.value['schema']
    for airflow_var in AirflowVar:
        if airflow_var.value['schema']:
            schema['airflow_variables']['schema'][airflow_var.get()] = airflow_var.value['schema']

    def __init__(self,
                 fernet_key: Union[None, str] = None,
                 google_application_credentials: Union[None, str] = None,
                 airflow_connections: Union[None, dict] = None,
                 airflow_variables: Union[None, dict] = None,
                 validator: ObservatoryConfigValidator = None):
        """ Holds the settings for the Observatory Platform, used by DAGs.

        :param environment: whether the system is running in dev, test or prod mode.
        :param google_application_credentials: the path to the Google Application Credentials: https://cloud.google.com/docs/authentication/getting-started
        :param fernet_key:
        """

        self.fernet_key = fernet_key
        self.google_application_credentials = google_application_credentials
        self.airflow_connections = airflow_connections
        self.airflow_variables = airflow_variables
        self.validator: ObservatoryConfigValidator = validator

    def __eq__(self, other):
        d1 = dict(self.__dict__)
        del d1['validator']
        d2 = dict(other.__dict__)
        del d2['validator']
        return isinstance(other, ObservatoryConfig) and d1 == d2

    def __ne__(self, other):
        return not self == other

    @property
    def is_valid(self):
        return self.validator is None or not len(self.validator._errors)

    def save(self, path: str) -> None:
        """ Save the ObservatoryConfig object to a file.

        :param path: the path to the configuration file.
        :return: None.
        """

        with open(path, 'w') as f:
            yaml.safe_dump(self.to_dict(), f)

    @staticmethod
    def load(path: str) -> 'ObservatoryConfig':
        """ Load an Observatory configuration file.

        :param path: the path to the Observatory configuration file.
        :return: the ObservatoryConfig instance.
        """

        config = None

        try:
            with open(path, 'r') as f:
                dict_ = yaml.safe_load(f)
                config = ObservatoryConfig.from_dict(dict_)
        except yaml.YAMLError:
            print(f'Error parsing {path}')
        except FileNotFoundError:
            print(f'No such file or directory: {path}')
        except cerberus.validator.DocumentError as e:
            print(f'cerberus.validator.DocumentError: {e}')

        return config

    def to_dict(self) -> Dict:
        """ Converts an ObservatoryConfig instance into a dictionary.

        :return: the dictionary.
        """

        return {
            'google_application_credentials': self.google_application_credentials,
            'fernet_key': self.fernet_key,
            'airflow_variables': self.airflow_variables,
            'airflow_connections': self.airflow_connections
        }

    @staticmethod
    def make_default() -> 'ObservatoryConfig':
        """ Make an ObservatoryConfig instance with default values.
        :return: the ObservatoryConfig instance.
        """

        google_application_credentials = None
        fernet_key = Fernet.generate_key().decode()
        airflow_connections = dict_default(AirflowConn)
        airflow_variables = dict_default(AirflowVar)
        return ObservatoryConfig(fernet_key, google_application_credentials,
                                 airflow_connections, airflow_variables)

    @staticmethod
    def from_dict(dict_: Dict) -> 'ObservatoryConfig':
        """ Make an ObservatoryConfig instance from a dictionary. If the dictionary is invalid,
        then an ObservatoryConfig instance will be returned with no properties set, except for the validator,
        which contains validation errors.

        :param dict_:  the input dictionary that has been read via yaml.safe_load.
        :return: the ObservatoryConfig instance.
        """
        validator = ObservatoryConfigValidator()
        is_valid = validator.validate(dict_, ObservatoryConfig.schema)

        if is_valid:
            google_application_credentials = dict_.get('google_application_credentials')
            fernet_key = dict_.get('fernet_key')
            airflow_variables = dict_.get('airflow_variables')
            airflow_connections = dict_.get('airflow_connections')

            return ObservatoryConfig(fernet_key, google_application_credentials,
                                     airflow_connections, airflow_variables, validator)
        else:
            return ObservatoryConfig(validator=validator)
