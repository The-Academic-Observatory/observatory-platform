# Copyright 2020 Curtin University
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

from __future__ import annotations

import os
import shutil
import subprocess
import time
import urllib.request
from enum import Enum
from subprocess import Popen
from typing import Tuple, Union, Dict

import cerberus.validator
import click
import docker
import requests
import yaml
from cerberus import Validator
from cryptography.fernet import Fernet

from academic_observatory.utils.path_utils import observatory_home, observatory_package_path, dags_path


@click.group()
def cli():
    """ The Academic Observatory command line tool.

    COMMAND: the commands to run include:\n
      - platform: start and stop the local Academic Observatory platform.\n
      - generate: generate a variety of outputs\n.
    """

    pass


def gen_fernet_key() -> str:
    """ Generate a Fernet key.

    :return: the Fernet key.
    """

    return Fernet.generate_key().decode()


def gen_config_interface():
    """ Command line user interface for generating config.yaml

    :return: None.
    """

    print("Generating config.yaml...")
    config = ObservatoryConfig.make_default()
    config_path = ObservatoryConfig.HOST_DEFAULT_PATH

    if not os.path.exists(config_path) or click.confirm(f'The file "{config_path}" exists, do you want to '
                                                        f'overwrite it?'):
        config.save(config_path)
        print(f'config.yaml saved to: "{config_path}"')
        print(f'Please customise the following parameters in config.yaml:')
        params = config.to_dict()
        for key, val in params.items():
            if val is None:
                print(f'  - {key}')
    else:
        print("Not generating config.yaml")


@cli.command()
@click.argument('command',
                type=click.Choice(['fernet-key', 'config.yaml']))
def generate(command):
    """ Generate information for the Academic Observatory platform.\n

    COMMAND: the command to give the generator:\n
      - fernet-key: generate a fernet key.\n
      - config.yaml: generate a config.yaml file.\n
    """

    if command == 'fernet-key':
        print(gen_fernet_key())
    elif command == 'config.yaml':
        gen_config_interface()


def wait_for_process(proc: Popen) -> Tuple[str, str]:
    """ Wait for a process to finish, returning the std output and std error streams as strings.

    :param proc: the process object.
    :return: std output and std error streams as strings.
    """
    output, error = proc.communicate()
    output = output.decode('utf-8')
    error = error.decode('utf-8')
    return output, error


def is_docker_installed() -> bool:
    """ Checks whether Docker is installed or not.

    :return: whether Docker is installed or not.
    """

    return shutil.which("docker") is not None


def is_docker_compose_installed() -> bool:
    """ Checks whether Docker Compose is installed or not.

    :return: whether Docker Compose is installed or not.
    """

    return shutil.which("docker-compose") is not None


def is_docker_running() -> bool:
    """ Checks whether Docker is running or not.

    :return: whether Docker is running or not.
    """

    client = docker.from_env()
    try:
        is_running = client.ping()
    except requests.exceptions.ConnectionError:
        is_running = False
    return is_running


def get_google_application_credentials() -> Union[str, None]:
    """ Get the GOOGLE_APPLICATION_CREDENTIALS environment variable.

    :return: the GOOGLE_APPLICATION_CREDENTIALS environment variable or None if it doesn't exist.
    """
    return os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')


def get_fernet_key() -> Union[str, None]:
    """ Get the FERNET_KEY environment variable.

    :return: the FERNET_KEY environment variable or None if it doesn't exist.
    """
    return os.environ.get('FERNET_KEY')


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
            'required': True,
            'type': 'string'
        },
        'data_path': {
            'required': True,
            'type': 'string'
        },
        'google_application_credentials': {
            'required': True,
            'type': 'string'
        },
        'environment': {
            'required': True,
            'type': 'string',
            'allowed': ['dev', 'test', 'prod']
        }
    }

    def __init__(self, project_id: Union[None, str], bucket_name: Union[None, str], dags_path: str, data_path: str,
                 google_application_credentials: str, environment: Environment):
        """ Holds the settings for the Academic Observatory, used by DAGs.

        :param project_id: the Google Cloud project id.
        :param bucket_name: the Google Cloud bucket where final results will be stored.
        :param dags_path: the path to the DAGs folder.
        :param data_path: the path where intermediate data will be saved.
        :param google_application_credentials: the path to the Google Application Credentials: https://cloud.google.com/docs/authentication/getting-started
        :param environment: whether the system is running in dev, test or prod mode.
        """

        self.project_id = project_id
        self.bucket_name = bucket_name
        self.dags_path = dags_path
        self.data_path = data_path
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
    def load(path: str) -> Tuple[bool, Union[None, Validator], Union[ObservatoryConfig, None]]:
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
            'data_path': self.data_path,
            'google_application_credentials': self.google_application_credentials,
            'environment': self.environment.value
        }

    @staticmethod
    def make_default() -> ObservatoryConfig:
        """ Make an ObservatoryConfig instance with default values.
        :return: the ObservatoryConfig instance.
        """

        project_id = None
        bucket_name = None
        dags_path = '/usr/local/airflow/dags'
        data_path = '/usr/local/airflow/data'
        google_application_credentials = '/run/secrets/google_application_credentials.json'
        environment = Environment.dev
        return ObservatoryConfig(project_id, bucket_name, dags_path, data_path, google_application_credentials,
                                 environment)

    @staticmethod
    def from_dict(dict_: Dict) -> ObservatoryConfig:
        """ Make an ObservatoryConfig instance from a dictionary.

        :param dict_:  the input dictionary that has been read via yaml.safe_load.
        :return: the ObservatoryConfig instance.
        """

        project_id = dict_['project_id']
        bucket_name = dict_['bucket_name']
        dags_path = dict_['dags_path']
        data_path = dict_['data_path']
        google_application_credentials = dict_['google_application_credentials']
        environment = Environment(dict_['environment'])
        return ObservatoryConfig(project_id, bucket_name, dags_path, data_path, google_application_credentials,
                                 environment)


def get_env(config_path: str, dags_path: str, data_path: str, airflow_ui_port: int, airflow_postgres_path: str,
            package_path: str, fernet_key: str, google_application_credentials: str):
    """ Make an environment containing the environment variables that are required to build and start the
    Observatory docker environment.

    :param config_path: the path to config.yaml on the host system.
    :param dags_path: the path to the DAGs folder on the host system.
    :param data_path: the path to the data path on the host system.
    :param airflow_ui_port: the Apache Airflow UI port to expose on the host system.
    :param airflow_postgres_path: the path to the Apache Airflow Postgres data folder on the host system.
    :param package_path: the path to the Academic Observatory Python project on the host system.
    :param fernet_key: the Fernet key.
    :param google_application_credentials: the path to the Google Application Credentials on the host system.
    :return:
    """

    env = os.environ.copy()
    env['HOST_CONFIG_PATH'] = config_path
    env['HOST_AIRFLOW_UI_PORT'] = str(airflow_ui_port)
    env['HOST_DAGS_PATH'] = dags_path
    env['HOST_POSTGRES_PATH'] = airflow_postgres_path
    env['HOST_DATA_PATH'] = data_path
    env['HOST_PACKAGE_PATH'] = package_path
    env['HOST_FERNET_KEY'] = fernet_key
    env['HOST_GOOGLE_APPLICATION_CREDENTIALS'] = google_application_credentials
    return env


def wait_for_airflow_ui(ui_url: str, timeout=60) -> bool:
    """ Wait for the Apache Airflow UI to start.

    :param ui_url: the URL to the Apache Airflow UI.
    :param timeout: the number of seconds to wait before timing out.
    :return: whether connecting to the Apache Airflow UI was successful or not.
    """

    start = time.time()
    ui_started = False
    while True:
        duration = time.time() - start
        if duration >= timeout:
            break

        try:
            if urllib.request.urlopen(ui_url).getcode() == 200:
                ui_started = True
                break
            time.sleep(0.5)
        except ConnectionResetError:
            pass

    return ui_started


@cli.command()
@click.argument('command',
                type=click.Choice(['start', 'stop']))
@click.option('--config-path',
              type=click.Path(exists=False, file_okay=True, dir_okay=False),
              default=ObservatoryConfig.HOST_DEFAULT_PATH,
              help='The path to the config.yaml configuration file.',
              show_default=True)
@click.option('--dags-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              default=dags_path(),
              help='The path on the host machine to mount as the Apache Airflow DAGs folder.',
              show_default=True)
@click.option('--data-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              default=observatory_home('data'),
              help='The path on the host machine to mount as the data folder.',
              show_default=True)
@click.option('--airflow-ui-port',
              type=int,
              default=8080,
              help='Apache Airflow external UI port.',
              show_default=True)
@click.option('--airflow-postgres-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=False),
              default=observatory_home('airflow-postgres'),
              help='The path on the host machine to mount as the Apache Airflow PostgreSQL data folder.',
              show_default=True)
@click.option('--debug',
              is_flag=True,
              default=False,
              help='Print debugging information.')
def platform(command, config_path, dags_path, data_path, airflow_ui_port, airflow_postgres_path, debug):
    """ Run the local Academic Observatory platform.\n

    COMMAND: the command to give the platform:\n
      - start: start the platform.\n
      - stop: stop the platform.\n
    """

    # The minimum number of characters per line
    min_line_chars = 80

    ######################
    # Check dependencies #
    ######################

    print("Academic Observatory: checking dependencies...".ljust(min_line_chars), end='\r')
    docker_installed = is_docker_installed()
    docker_compose_installed = is_docker_compose_installed()
    docker_running = is_docker_running()
    google_application_credentials = get_google_application_credentials()
    google_application_credentials_exists = google_application_credentials is not None
    fernet_key = get_fernet_key()
    fernet_key_exists = fernet_key is not None
    config_exists = os.path.exists(config_path)
    config_valid, config_validator, config = ObservatoryConfig.load(config_path)
    config_loaded = config is not None
    all_deps = all([docker_installed, docker_compose_installed, docker_running, google_application_credentials_exists,
                    fernet_key_exists, config_exists, config_loaded, config_valid])

    if not all_deps:
        print("Academic Observatory: dependencies missing".ljust(min_line_chars))
    else:
        print("Academic Observatory: all dependencies found".ljust(min_line_chars))

    if docker_installed:
        print(" - Docker: installed")
    else:
        print(" - Docker: not installed, please install https://docs.docker.com/get-docker/")

    if docker_compose_installed:
        print(" - Docker Compose: installed")
    else:
        print(" - Docker Compose: not installed, please install https://docs.docker.com/compose/install/")

    if docker_running:
        print(" - Docker: running")
    else:
        print(" - Docker: not running, please start")

    if google_application_credentials:
        print(f" - GOOGLE_APPLICATION_CREDENTIALS: environment variable set {google_application_credentials}")
    else:
        print(" - GOOGLE_APPLICATION_CREDENTIALS: environment variable not set. See "
              "https://cloud.google.com/docs/authentication/getting-started for instructions on how to set it. "
              "It is recommended to add it to ~/.bashrc.")

    if fernet_key_exists:
        print(" - FERNET_KEY: environment variable set")
    else:
        print(" - FERNET_KEY: environment variable not set. See below for command to set it: \n"
              "               echo export FERNET_KEY=`observatory generate fernet-key` >> ~/.bashrc && "
              "source ~/.bashrc")

    if config_exists:
        print(f" - config.yaml: found '{config_path}'")

        if config_valid:
            print(f" - config.yaml: valid")
        else:
            print(f" - config.yaml: invalid")
            for key, value in config_validator.errors.items():
                print(f'    - {key}: {", ".join(value)}')
    else:
        print(f" - config.yaml: not found so generating a default file '{config_path}'")
        gen_config_interface()

    if not all_deps:
        exit(os.EX_CONFIG)

    ##################
    # Run commands   #
    ##################

    # Make environment variables for running commands
    package_path = observatory_package_path()
    env = get_env(config_path, dags_path, data_path, airflow_ui_port, airflow_postgres_path,
                  package_path, fernet_key, google_application_credentials)

    # Make docker-compose command
    args = ['docker-compose']
    compose_files = ['docker-compose.secrets.yml', 'docker-compose.airflow-postgres.yml',
                     'docker-compose.airflow-webserver.yml']
    for file_name in compose_files:
        path = os.path.join(package_path, file_name)
        args.append('-f')
        args.append(path)

    # Start the appropriate process
    if command == 'start':
        print('Academic Observatory: building...'.ljust(min_line_chars), end="\r")

        # Build the containers first
        proc: Popen = subprocess.Popen(args + ['build'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        output, error = wait_for_process(proc)

        if debug:
            print(output)

        if proc.returncode == 0:
            print('Academic Observatory: built'.ljust(min_line_chars))
        else:
            print('Academic Observatory: build error'.ljust(min_line_chars))
            print(error)
            exit(os.EX_CONFIG)

        # Start the built containers
        print('Academic Observatory: starting...                                               ', end='\r')
        proc: Popen = subprocess.Popen(args + ['up', '-d'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        output, error = wait_for_process(proc)

        if debug:
            print(output)

        if proc.returncode == 0:
            ui_url = f'http://localhost:{airflow_ui_port}'
            ui_started = wait_for_airflow_ui(ui_url)

            if ui_started:
                print('Academic Observatory: started'.ljust(min_line_chars))
                print(f'View the Apache Airflow UI at {ui_url}')
            else:
                print('Academic Observatory: error starting'.ljust(min_line_chars))
                print(f'Could not find the Airflow UI at {ui_url}')
        else:
            print("Error starting the Academic Observatory")
            print(error)
            exit(os.EX_CONFIG)

    elif command == 'stop':
        print('Academic Observatory: stopping...'.ljust(min_line_chars), end='\r')
        proc = subprocess.Popen(args + ['down'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        output, error = wait_for_process(proc)

        if debug:
            print(output)

        if proc.returncode == 0:
            print('Academic Observatory: stopped'.ljust(min_line_chars))
        else:
            print('Academic Observatory: error stopping'.ljust(min_line_chars))
            print(error)
            exit(os.EX_CONFIG)

    exit(os.EX_OK)


if __name__ == "__main__":
    cli()
