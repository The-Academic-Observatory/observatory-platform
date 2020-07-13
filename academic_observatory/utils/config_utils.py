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

import cerberus.validator
import pendulum
import yaml
from airflow.models import Variable
from cerberus import Validator
from cryptography.fernet import Fernet
from natsort import natsorted
from pendulum import Pendulum

import academic_observatory.database
from academic_observatory import dags


def observatory_home(*subdirs) -> str:
    """ Get the .observatory Academic Observatory home directory or subdirectory. The home directory and subdirectories
     will be created if they do not exist. The path given by the OBSERVATORY_PATH environment variable must exist
     otherwise a NotADirectoryError error will be thrown.
      - If the OBSERVATORY_PATH environment variable is set: OBSERVATORY_PATH + .observatory + optional subdirs.
      - If the OBSERVATORY_PATH environment variable is not set: user's home directory + .observatory + optional subdirs.
    :param: subdirs: an optional list of subdirectories.
    :return: the path.
    """

    # observatory_path = os.environ.get('OBSERVATORY_PATH')
    user_home = str(pathlib.Path.home())
    #
    # if observatory_path is None:
    #     observatory_path = user_home
    # elif not os.path.exists(observatory_path):
    #     msg = f'The path given by OBSERVATORY_PATH does not exist: {observatory_path}'
    #     logging.error(msg)
    #     raise FileNotFoundError(msg)

    observatory_home_ = os.path.join(user_home, ".observatory", *subdirs)

    if not os.path.exists(observatory_home_):
        os.makedirs(observatory_home_, exist_ok=True)

    return observatory_home_


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


def schema_path(database: str) -> str:
    """ Get the absolute path to a schema folder.

    :param database: the name of the database, e.g. telescopes, platform, analysis etc.
    :return: the schema folder.
    """
    file_path = pathlib.Path(academic_observatory.database.__file__).resolve()
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


def telescope_path(sub_folder: SubFolder, name: str) -> str:
    """ Return a path for saving telescope data. Create it if it doesn't exist.

    :param sub_folder: the name of the sub folder for the telescope
    :param name: the name of the telescope.
    :return: the path.
    """

    data_path = Variable.get("data_path")
    path = os.path.join(data_path, 'telescopes', sub_folder.value, name)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    return path


class Environment(Enum):
    """ The environment being used """

    dev = 'dev'
    test = 'test'
    prod = 'prod'


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
        'project_id': {
            'required': True,
            'type': 'string'
        },
        'data_location': {
            'required': True,
            'type': 'string'
        },
        'download_bucket_name': {
            'required': True,
            'type': 'string'
        },
        'transform_bucket_name': {
            'required': True,
            'type': 'string'
        },
        'environment': {
            'required': True,
            'type': 'string',
            'allowed': ['dev', 'test', 'prod']
        },
        'google_application_credentials': {
            'type': 'string',
            'google_application_credentials': True,
            'required': True
        },
        'fernet_key': {
            'required': True,
            'type': 'string'
        },
        'mag_releases_table_connection': {
            'required': False,
            'type': 'string'
        },
        'mag_snapshots_container_connection': {
            'required': False,
            'type': 'string'
        },
        'crossref_connection': {
            'required': False,
            'type': 'string'
        }
    }

    def __init__(self,
                 project_id: Union[None, str] = None,
                 data_location: Union[None, str] = None,
                 download_bucket_name: Union[None, str] = None,
                 transform_bucket_name: Union[None, str] = None,
                 environment: Environment = None,
                 google_application_credentials: Union[None, str] = None,
                 fernet_key: Union[None, str] = None,
                 mag_releases_table_connection: Union[None, str] = None,
                 mag_snapshots_container_connection: Union[None, str] = None,
                 crossref_connection: Union[None, str] = None,
                 validator: ObservatoryConfigValidator = None):
        """ Holds the settings for the Academic Observatory, used by DAGs.

        :param project_id: the Google Cloud project id.
        :param data_location: the location to store data.
        :param download_bucket_name:
        :param transform_bucket_name: the Google Cloud bucket where final results will be stored.
        :param environment: whether the system is running in dev, test or prod mode.
        :param google_application_credentials: the path to the Google Application Credentials: https://cloud.google.com/docs/authentication/getting-started
        :param fernet_key:
        :param mag_releases_table_connection:
        :param mag_snapshots_container_connection:
        :param crossref_connection:
        :param validator:
        """

        self.project_id = project_id
        self.data_location = data_location
        self.download_bucket_name = download_bucket_name
        self.transform_bucket_name = transform_bucket_name
        self.environment = environment
        self.google_application_credentials = google_application_credentials
        self.fernet_key = fernet_key
        self.mag_releases_table_connection = mag_releases_table_connection
        self.mag_snapshots_container_connection = mag_snapshots_container_connection
        self.crossref_connection = crossref_connection
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
            'project_id': self.project_id,
            'data_location': self.data_location,
            'download_bucket_name': self.download_bucket_name,
            'transform_bucket_name': self.transform_bucket_name,
            'environment': self.environment.value,
            'google_application_credentials': self.google_application_credentials,
            'fernet_key': self.fernet_key,
            'mag_releases_table_connection': self.mag_releases_table_connection,
            'mag_snapshots_container_connection': self.mag_snapshots_container_connection,
            'crossref_connection': self.crossref_connection
        }

    @staticmethod
    def make_default() -> 'ObservatoryConfig':
        """ Make an ObservatoryConfig instance with default values.
        :return: the ObservatoryConfig instance.
        """

        project_id = None
        data_location = None
        download_bucket_name = None
        transform_bucket_name = None
        environment = Environment.dev
        google_application_credentials = None
        fernet_key = Fernet.generate_key().decode()
        mag_releases_table_connection = 'mysql://azure-storage-account-name:url-encoded-sas-token@'
        mag_snapshots_container_connection = 'mysql://azure-storage-account-name:url-encoded-sas-token@'
        crossref_connection = 'mysql://:crossref-token@'
        return ObservatoryConfig(project_id, data_location, download_bucket_name, transform_bucket_name, environment,
                                 google_application_credentials, fernet_key, mag_releases_table_connection,
                                 mag_snapshots_container_connection, crossref_connection)

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
            project_id = dict_.get('project_id')
            data_location = dict_.get('data_location')
            download_bucket_name = dict_.get('download_bucket_name')
            transform_bucket_name = dict_.get('transform_bucket_name')
            environment = Environment(dict_.get('environment'))
            google_application_credentials = dict_.get('google_application_credentials')
            fernet_key = dict_.get('fernet_key')
            mag_releases_table_connection = dict_.get('mag_releases_table_connection')
            mag_snapshots_container_connection = dict_.get('mag_snapshots_container_connection')
            crossref_connection = dict_.get('crossref_connection')

            return ObservatoryConfig(project_id, data_location, download_bucket_name, transform_bucket_name,
                                     environment, google_application_credentials, fernet_key,
                                     mag_releases_table_connection, mag_snapshots_container_connection,
                                     crossref_connection, validator)
        else:
            return ObservatoryConfig(validator=validator)
