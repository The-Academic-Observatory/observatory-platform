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


import logging
import os
import pathlib
from enum import Enum
from typing import Union, Dict

import cerberus.validator
import yaml
from cerberus import Validator

import academic_observatory
import academic_observatory.database.analysis.bigquery.schema
import academic_observatory.debug_files
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

    observatory_path = os.environ.get('OBSERVATORY_PATH')
    user_home = str(pathlib.Path.home())

    if observatory_path is None:
        observatory_path = user_home
    elif not os.path.exists(observatory_path):
        msg = f'The path given by OBSERVATORY_PATH does not exist: {observatory_path}'
        logging.error(msg)
        raise FileNotFoundError(msg)

    observatory_home_ = os.path.join(observatory_path, ".observatory", *subdirs)

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


def bigquery_schema_path(name: str) -> str:
    """ Get the relative path to a bigquery schema.

    :return: the path to the bigquery schema.
    """
    file_path = pathlib.Path(academic_observatory.database.analysis.bigquery.schema.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1], name)
    if path.is_file():
        return str(path.resolve())
    else:
        print(f"schema file at {str(path.resolve())} does not exist")


def debug_file_path(name: str) -> str:
    """ Get the relative path to a file used for debugging.

    :return: the path to the debug file.
    """
    file_path = pathlib.Path(academic_observatory.debug_files.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1], name)
    if path.is_file():
        return str(path.resolve())
    else:
        print(f"debug file at {str(path.resolve())} does not exist")


class SubFolder(Enum):
    """ The type of subfolder to create for telescope data """

    downloaded = 'downloaded'
    extracted = 'extracted'
    transformed = 'transformed'


def telescope_path(name: str, sub_folder: SubFolder) -> str:
    """ Return a path for saving telescope data. Create it if it doesn't exist.

    :param name: the name of the telescope.
    :param sub_folder: the name of the sub folder for the telescope
    :return: the path.
    """

    return observatory_home('data', 'telescopes', name, sub_folder.value)


class Environment(Enum):
    """ The environment being used """

    dev = 'dev'
    test = 'test'
    prod = 'prod'


class ObservatoryConfig:
    HOST_DEFAULT_PATH = os.path.join(observatory_home(), 'config.yaml')
    CONTAINER_DEFAULT_PATH = '/run/secrets/config.yaml'
    schema = {
        'project_id': {
            'required': True,
            'type': 'string'
        },
        'bucket_name': {
            'required': True,
            'type': 'string'
        },
        'location': {
            'required': True,
            'type': 'string'
        },
        'dags_path': {
            'required': False,
            'type': 'string'
        },
        'google_application_credentials': {
            'required': False,
            'type': 'string'
        },
        'environment': {
            'required': True,
            'type': 'string',
            'allowed': ['dev', 'test', 'prod']
        }
    }

    def __init__(self, project_id: Union[None, str] = None, bucket_name: Union[None, str] = None,
                 location: Union[None, str] = None, dags_path: str = None, google_application_credentials: str = None,
                 environment: Environment = None, validator: Validator = None):
        """ Holds the settings for the Academic Observatory, used by DAGs.

        :param project_id: the Google Cloud project id.
        :param bucket_name: the Google Cloud bucket where final results will be stored.
        :param location: the Google Cloud location for the project,
        :param dags_path: the path to the DAGs folder.
        :param google_application_credentials: the path to the Google Application Credentials: https://cloud.google.com/docs/authentication/getting-started
        :param environment: whether the system is running in dev, test or prod mode.
        """

        self.project_id = project_id
        self.bucket_name = bucket_name
        self.location = location
        self.dags_path = dags_path
        self.google_application_credentials = google_application_credentials
        self.environment = environment
        self.validator: Validator = validator

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
            'bucket_name': self.bucket_name,
            'location': self.location,
            'dags_path': self.dags_path,
            'google_application_credentials': self.google_application_credentials,
            'environment': self.environment.value
        }

    @staticmethod
    def make_default() -> 'ObservatoryConfig':
        """ Make an ObservatoryConfig instance with default values.
        :return: the ObservatoryConfig instance.
        """

        project_id = None
        bucket_name = None
        location = None
        dags_path = '/usr/local/airflow/dags'
        google_application_credentials = '/run/secrets/google_application_credentials.json'
        environment = Environment.dev
        return ObservatoryConfig(project_id, bucket_name, location, dags_path, google_application_credentials,
                                 environment)

    @staticmethod
    def from_dict(dict_: Dict) -> 'ObservatoryConfig':
        """ Make an ObservatoryConfig instance from a dictionary. If the dictionary is invalid,
        then an ObservatoryConfig instance will be returned with no properties set, except for the validator,
        which contains validation errors.

        :param dict_:  the input dictionary that has been read via yaml.safe_load.
        :return: the ObservatoryConfig instance.
        """
        validator = Validator()
        is_valid = validator.validate(dict_, ObservatoryConfig.schema)

        if is_valid:
            project_id = dict_.get('project_id')
            bucket_name = dict_.get('bucket_name')
            location = dict_.get('location')
            dags_path = dict_.get('dags_path')
            google_application_credentials = dict_.get('google_application_credentials')
            environment = Environment(dict_.get('environment'))
            return ObservatoryConfig(project_id=project_id, bucket_name=bucket_name, location=location,
                                     dags_path=dags_path, google_application_credentials=google_application_credentials,
                                     environment=environment, validator=validator)
        else:
            return ObservatoryConfig(validator=validator)
