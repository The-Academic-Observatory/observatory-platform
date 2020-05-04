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

import logging
import os
import pathlib
import shutil
import subprocess

import click

from academic_observatory import docker, dags
from academic_observatory.utils import ao_home


@click.group()
def cli():
    """ The Academic Observatory command line tool.

    COMMAND: the commands to run include:\n
      - platform: start and stop the local Academic Observatory platform.\n
    """

    pass


def check_dependencies():
    # Check if docker is installed
    docker_ = shutil.which("docker")
    if docker_ is None:
        print("The Academic Observatory command line tool requires Docker: https://docs.docker.com/get-docker/")

    # Check if docker-compose is installed
    docker_compose_ = shutil.which("docker-compose")
    if docker_compose_ is None:
        print("The Academic Observatory command line tool requires Docker Compose: "
              "https://docs.docker.com/compose/install/")

    return docker_ is not None and docker_compose_ is not None


def docker_module_path():
    """ Get docker module path

    Recommended way to add non code files: https://python-packaging.readthedocs.io/en/latest/non-code-files.html
    """

    file_path = pathlib.Path(docker.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1])
    return str(path.resolve())


def dags_module_path():
    """ Get dags module path

    Recommended way to add non code files: https://python-packaging.readthedocs.io/en/latest/non-code-files.html
    """

    file_path = pathlib.Path(dags.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1])
    return str(path.resolve())


def log_output(output, error, returncode):
    logging.debug(f"returncode: {returncode}")
    logging.debug(f"stdout: {output}")
    logging.debug(f"stderr: {error}")


def make_env(airflow_ui_port, airflow_dags_path, airflow_postgres_path):
    env = os.environ.copy()
    env['AIRFLOW_UI_PORT'] = str(airflow_ui_port)
    env['AIRFLOW_DAGS_PATH'] = airflow_dags_path
    env['AIRFLOW_POSTGRES_PATH'] = airflow_postgres_path
    return env


@cli.command()
@click.argument('command',
                type=click.Choice(['start', 'stop']))
@click.option('--airflow-ui-port',
              type=int,
              default=8080,
              help='Apache Airflow external UI port.',
              show_default=True)
@click.option('--airflow-dags-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              default=dags_module_path(),
              help='The path to mount as the Apache Airflow DAGs folder.',
              show_default=True)
@click.option('--airflow-postgres-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=False),
              default=ao_home('mnt', 'airflow-postgres'),
              help='The path to mount as the Apache Airflow PostgreSQL data folder.',
              show_default=True)
@click.option('--debug',
              is_flag=True,
              default=False,
              help='Print debugging information.')
def platform(command, airflow_ui_port, airflow_dags_path, airflow_postgres_path, debug):
    """ Run the local Academic Observatory platform.\n

    COMMAND: the command to give the platform:\n
      - start: start the platform.\n
      - stop: stop the platform.\n
      - status: the state that the platform is in.\n
    """

    # Set logging level
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    # Check that docker and docker compose are installed
    if not check_dependencies():
        exit(os.EX_CONFIG)

    # Make absolute paths for docker compose files and base docker-compose command
    docker_module_path_ = docker_module_path()
    compose_postgres_path = os.path.join(docker_module_path_, 'docker-compose.airflow-postgres.yml')
    compose_webserver_path = os.path.join(docker_module_path_, 'docker-compose.airflow-webserver.yml')
    args = ['docker-compose', '-f', compose_postgres_path, '-f', compose_webserver_path]

    # Make environment variables for running commands
    env = make_env(airflow_ui_port, airflow_dags_path, airflow_postgres_path)

    # Start the appropriate process
    if command == 'start':
        proc = subprocess.Popen(args + ['up', '-d'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        output, error = proc.communicate()
        log_output(output, error, proc.returncode)
    elif command == 'stop':
        proc = subprocess.Popen(args + ['down'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        output, error = proc.communicate()
        log_output(output, error, proc.returncode)

    exit(os.EX_OK)


if __name__ == "__main__":
    cli()
