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


import os
import pathlib
import socket
from enum import Enum
from typing import Tuple, Union, Dict

import cerberus.validator
import yaml
from cerberus import Validator

import academic_observatory
from academic_observatory import dags
import academic_observatory.database.analysis.bigquery.schema
import academic_observatory.debug_files


def is_vendor_google() -> bool:
    """ Identify if the system is running in Google Cloud.
    :return: whether system running in Google Cloud.
    """
    result = True
    try:
        socket.getaddrinfo('metadata.google.internal', 80)
    except socket.gaierror:
        result = False
    return result


def observatory_home(*subdirs) -> str:
    """Get the Academic Observatory home directory.
      - If the home directory doesn't exist then create it.
      - If the system is running in Google Cloud then the home directory will be set to: /home/airflow/gcs/data

    :return: the Academic Observatory home directory.
    """

    if is_vendor_google():
        user_home = '/home/airflow/gcs/data'
    else:
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

    def __init__(self, project_id: Union[None, str], bucket_name: Union[None, str], dags_path: str,
                 google_application_credentials: str, environment: Environment):
        """ Holds the settings for the Academic Observatory, used by DAGs.

        :param project_id: the Google Cloud project id.
        :param bucket_name: the Google Cloud bucket where final results will be stored.
        :param dags_path: the path to the DAGs folder.
        :param google_application_credentials: the path to the Google Application Credentials: https://cloud.google.com/docs/authentication/getting-started
        :param environment: whether the system is running in dev, test or prod mode.
        """

        self.project_id = project_id
        self.bucket_name = bucket_name
        self.dags_path = dags_path
        self.google_application_credentials = google_application_credentials
        self.environment = environment

    @staticmethod
    def __validate(dict_: Dict) -> Tuple[bool, Validator]:
        """ Validate the input dictionary against the ObservatoryConfig's schema.

        :param dict_: the input dictionary that has been read via yaml.safe_load
        :return: whether the validation passed or not and the validation object which contains details such as
        why the validation failed.
        """

        v = Validator()
        result = v.validate(dict_, ObservatoryConfig.schema)
        return result, v

    def save(self, path: str) -> None:
        """ Save the ObservatoryConfig object to a file.

        :param path: the path to the configuration file.
        :return: None.
        """

        with open(path, 'w') as f:
            yaml.safe_dump(self.to_dict(), f)

    @staticmethod
    def load(path: str) -> Tuple[bool, Union[None, Validator], Union['ObservatoryConfig', None]]:
        """ Load an Observatory configuration file.

        :param path: the path to the Observatory configuration file.
        :return: whether the config file is valid, the validator which contains details such as why the validation
        failed and the ObservatoryConfig instance.
        """

        is_valid = False
        validator = None
        config = None

        try:
            with open(path, 'r') as f:
                dict_ = yaml.safe_load(f)
                is_valid, validator = ObservatoryConfig.__validate(dict_)
                if is_valid:
                    config = ObservatoryConfig.from_dict(dict_)
        except yaml.YAMLError:
            print(f'Error parsing {path}')
        except FileNotFoundError:
            print(f'No such file or directory: {path}')
        except cerberus.validator.DocumentError as e:
            print(f'cerberus.validator.DocumentError: {e}')

        return is_valid, validator, config

    def to_dict(self) -> Dict:
        """ Converts an ObservatoryConfig instance into a dictionary.

        :return: the dictionary.
        """

        return {
            'project_id': self.project_id,
            'bucket_name': self.bucket_name,
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
        dags_path = '/usr/local/airflow/dags'
        google_application_credentials = '/run/secrets/google_application_credentials.json'
        environment = Environment.dev
        return ObservatoryConfig(project_id, bucket_name, dags_path, google_application_credentials, environment)

    @staticmethod
    def from_dict(dict_: Dict) -> 'ObservatoryConfig':
        """ Make an ObservatoryConfig instance from a dictionary.

        :param dict_:  the input dictionary that has been read via yaml.safe_load.
        :return: the ObservatoryConfig instance.
        """

        project_id = dict_['project_id']
        bucket_name = dict_['bucket_name']
        environment = Environment(dict_['environment'])
        dags_path = dict_.get('dags_path')
        google_application_credentials = dict_.get('google_application_credentials')
        return ObservatoryConfig(project_id, bucket_name, dags_path, google_application_credentials, environment)
