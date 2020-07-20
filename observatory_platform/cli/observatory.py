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

import os
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from subprocess import Popen

import click
import docker
import requests
from cryptography.fernet import Fernet

from observatory_platform.utils.config_utils import observatory_home, observatory_package_path, \
    dags_path as default_dags_path, ObservatoryConfig
from observatory_platform.utils.proc_utils import wait_for_process


@click.group()
def cli():
    """ The Observatory Platform command line tool.

    COMMAND: the commands to run include:\n
      - platform: start and stop the local Observatory Platform platform.\n
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
    """ Generate information for the Observatory Platform platform.\n

    COMMAND: the command to give the generator:\n
      - fernet-key: generate a fernet key.\n
      - config.yaml: generate a config.yaml file.\n
    """

    if command == 'fernet-key':
        print(gen_fernet_key())
    elif command == 'config.yaml':
        gen_config_interface()


def get_docker_path() -> str:
    """ Get the path to Docker.

    :return: the path or None.
    """

    return shutil.which("docker")


def get_docker_compose_path() -> str:
    """ Get the path to Docker Compose.

    :return: the path or None.
    """

    return shutil.which("docker-compose")


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


def get_env(dags_path: str, data_path: str, logs_path: str, postgres_path: str, host_uid: int, host_gid: int,
            package_path: str, config: ObservatoryConfig):
    """ Make an environment containing the environment variables that are required to build and start the
    Observatory docker environment.

    :param dags_path: the path to the DAGs folder on the host system.
    :param data_path: the path to the data path on the host system.
    :param logs_path: the path to the logs path on the host system.
    :param postgres_path: the path to the Apache Airflow Postgres data folder on the host system.
    :param package_path: the path to the Observatory Platform Python project on the host system.
    :param host_uid: the user id of the host system.
    :param host_gid: the group id of the host system.
    :param config: the config file.
    :return:
    """

    env = os.environ.copy()

    # Host settings
    env['HOST_USER_ID'] = str(host_uid)
    env['HOST_GROUP_ID'] = str(host_gid)
    env['HOST_LOGS_PATH'] = logs_path
    env['HOST_DAGS_PATH'] = dags_path
    env['HOST_DATA_PATH'] = data_path
    env['HOST_POSTGRES_PATH'] = postgres_path
    env['HOST_PACKAGE_PATH'] = package_path

    # Secrets
    env['HOST_GOOGLE_APPLICATION_CREDENTIALS'] = config.google_application_credentials
    env['FERNET_KEY'] = config.fernet_key

    # Set connections
    env['AIRFLOW_CONN_MAG_RELEASES_TABLE'] = config.mag_releases_table_connection
    env['AIRFLOW_CONN_MAG_SNAPSHOTS_CONTAINER'] = config.mag_snapshots_container_connection
    env['AIRFLOW_CONN_CROSSREF'] = config.crossref_connection

    # Set variables
    env['AIRFLOW_VAR_PROJECT_ID'] = config.project_id
    env['AIRFLOW_VAR_DATA_LOCATION'] = config.data_location
    env['AIRFLOW_VAR_DOWNLOAD_BUCKET_NAME'] = config.download_bucket_name
    env['AIRFLOW_VAR_TRANSFORM_BUCKET_NAME'] = config.transform_bucket_name
    env['AIRFLOW_VAR_ENVIRONMENT'] = config.environment.value

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
        except ConnectionRefusedError:
            pass
        except urllib.error.URLError:
            pass

    return ui_started


def indent(string: str, num_spaces: int) -> str:
    return string.rjust(len(string) + num_spaces)


def build_compose_files(working_dir: str):
    return subprocess.Popen(['cat', 'docker-compose.local.yml', 'docker-compose.observatory.yml'],
                            stdout=subprocess.PIPE, cwd=working_dir)


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
              default=default_dags_path(),
              help='The path on the host machine to mount as the Apache Airflow DAGs folder.',
              show_default=True)
@click.option('--data-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              default=observatory_home('data'),
              help='The path on the host machine to mount as the data folder.',
              show_default=True)
@click.option('--logs-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              default=observatory_home('logs'),
              help='The path on the host machine to mount as the logs folder.',
              show_default=True)
@click.option('--postgres-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=False),
              default=observatory_home('postgres'),
              help='The path on the host machine to mount as the PostgreSQL data folder.',
              show_default=True)
@click.option('--host-uid',
              type=click.INT,
              default=os.getuid(),
              help='The user id of the host system. Used to set the user id in the Docker containers.',
              show_default=True)
@click.option('--host-gid',
              type=click.INT,
              default=os.getgid(),
              help='The group id of the host system. Used to set the group id in the Docker containers.',
              show_default=True)
@click.option('--debug',
              is_flag=True,
              default=False,
              help='Print debugging information.')
def platform(command, config_path, dags_path, data_path, logs_path, postgres_path, host_uid, host_gid, debug):
    """ Run the local Observatory Platform platform.\n

    COMMAND: the command to give the platform:\n
      - start: start the platform.\n
      - stop: stop the platform.\n
    """

    # The minimum number of characters per line
    min_line_chars = 80

    ######################
    # Check dependencies #
    ######################

    print("Observatory Platform: checking dependencies...".ljust(min_line_chars), end='\r')

    # Get all dependencies
    docker_path = get_docker_path()
    docker_compose_path = get_docker_compose_path()
    docker_running = is_docker_running()
    config_exists = os.path.exists(config_path)
    config = ObservatoryConfig.load(config_path)
    config_is_valid = False
    if config is not None:
        config_is_valid = config.is_valid

    # Check that all dependencies are met
    all_deps = all([docker_path is not None,
                    docker_compose_path is not None,
                    docker_running,
                    config_exists,
                    config_is_valid,
                    config is not None])

    # Indentation variables
    indent1 = 2
    indent2 = 3
    indent3 = 4

    if not all_deps:
        print("Observatory Platform: dependencies missing".ljust(min_line_chars))
    else:
        print("Observatory Platform: all dependencies found".ljust(min_line_chars))

    print(indent("Docker:", indent1))
    if docker_path is not None:
        print(indent(f"- path: {docker_path}", indent2))

        if docker_running:
            print(indent(f"- running", indent2))
        else:
            print(indent("- not running, please start", indent2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/get-docker/", indent2))

    print(indent("Host machine settings:", indent1))
    print(indent(f"- observatory home: {observatory_home()}", indent2))
    print(indent(f"- data-path: {data_path}", indent2))
    print(indent(f"- dags-path: {dags_path}", indent2))
    print(indent(f"- logs-path: {logs_path}", indent2))
    print(indent(f"- postgres-path: {postgres_path}", indent2))
    print(indent(f"- host-uid: {host_uid}", indent2))

    print(indent("Docker Compose:", indent1))
    if docker_compose_path is not None:
        print(indent(f"- path: {docker_compose_path}", indent2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/compose/install/", indent2))

    print(indent("config.yaml:", indent1))
    if config_exists:
        print(indent(f"- path: {config_path}", indent2))

        if config.is_valid:
            print(indent("- file valid", indent2))
        else:
            print(indent("- file invalid", indent2))
            for key, value in config.validator.errors.items():
                print(indent(f'- {key}: {", ".join(value)}', indent3))
    else:
        print(indent("- file not found, generating a default file", indent2))
        gen_config_interface()

    if not all_deps:
        exit(os.EX_CONFIG)

    ##################
    # Run commands   #
    ##################

    # Working directory for running the build etc
    package_path = observatory_package_path()

    # Make environment variables for running commands
    env = get_env(dags_path, data_path, logs_path, postgres_path, host_uid, host_gid, package_path, config)

    # Docker compose commands
    compose_args = ['docker-compose', '-f', '-']

    # Start the appropriate process
    if command == 'start':
        print('Observatory Platform: building...'.ljust(min_line_chars), end="\r")

        # Build the containers first
        cat_proc = build_compose_files(package_path)
        proc: Popen = subprocess.Popen(compose_args + ['build'], stdin=cat_proc.stdout, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, env=env, cwd=package_path)
        output, error = wait_for_process(proc)

        if debug:
            print(output)

        if proc.returncode == 0:
            print('Observatory Platform: built'.ljust(min_line_chars))
        else:
            print('Observatory Platform: build error'.ljust(min_line_chars))
            print(error)
            exit(os.EX_CONFIG)

        # Start the built containers
        print('Observatory Platform: starting...'.ljust(min_line_chars), end='\r')
        cat_proc = build_compose_files(package_path)
        proc: Popen = subprocess.Popen(compose_args + ['up', '-d'], stdin=cat_proc.stdout, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, env=env, cwd=package_path)
        output, error = wait_for_process(proc)

        if debug:
            print(output)

        if proc.returncode == 0:
            ui_url = f'http://localhost:8080'
            ui_started = wait_for_airflow_ui(ui_url, timeout=120)

            if ui_started:
                print('Observatory Platform: started'.ljust(min_line_chars))
                print(f'View the Apache Airflow UI at {ui_url}')
            else:
                print('Observatory Platform: error starting'.ljust(min_line_chars))
                print(f'Could not find the Airflow UI at {ui_url}')
        else:
            print("Error starting the Observatory Platform")
            print(error)
            exit(os.EX_CONFIG)

    elif command == 'stop':
        print('Observatory Platform: stopping...'.ljust(min_line_chars), end='\r')
        cat_proc = build_compose_files(package_path)
        proc = subprocess.Popen(compose_args + ['down'], stdin=cat_proc.stdout, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, env=env, cwd=package_path)
        output, error = wait_for_process(proc)

        if debug:
            print(output)

        if proc.returncode == 0:
            print('Observatory Platform: stopped'.ljust(min_line_chars))
        else:
            print('Observatory Platform: error stopping'.ljust(min_line_chars))
            print(error)
            exit(os.EX_CONFIG)
    exit(os.EX_OK)


if __name__ == "__main__":
    cli()
