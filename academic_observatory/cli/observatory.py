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

import academic_observatory
from academic_observatory.utils import ao_home, dags_path, docker_configs_path


@click.group()
def cli():
    """ The Academic Observatory command line tool.

    COMMAND: the commands to run include:\n
      - platform: start and stop the local Academic Observatory platform.\n
    """

    pass


def __check_dependencies():
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


def __log_output(output, error, returncode):
    logging.debug(f"returncode: {returncode}")
    logging.debug(f"stdout: \n{output.decode('utf-8')}")

    if returncode != 0:
        logging.error(f"stderr: \n{error.decode('utf-8')}")


def __make_env(airflow_ui_port, airflow_dags_path, airflow_postgres_path, ao_package_path_):
    env = os.environ.copy()
    env['AIRFLOW_UI_PORT'] = str(airflow_ui_port)
    env['AIRFLOW_DAGS_PATH'] = airflow_dags_path
    env['AIRFLOW_POSTGRES_PATH'] = airflow_postgres_path
    env['ACADEMIC_OBSERVATORY_PACKAGE_PATH'] = ao_package_path_
    return env


def ao_package_path() -> str:
    """ Get the path to the Academic Observatory package root folder.

    :return: the path to the Academic Observatory package root folder.
    """

    file_path = pathlib.Path(academic_observatory.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-2])
    return str(path.resolve())


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
              default=dags_path(),
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
    if not __check_dependencies():
        exit(os.EX_CONFIG)

    # Make docker-compose command
    args = ['docker-compose']
    compose_files = ['docker-compose.secrets.yml', 'docker-compose.airflow-postgres.yml',
                     'docker-compose.airflow-webserver.yml']
    for file_name in compose_files:
        path = os.path.join(docker_configs_path(), file_name)
        args.append('-f')
        args.append(path)

    # Make environment variables for running commands
    ao_package_path_ = ao_package_path()
    env = __make_env(airflow_ui_port, airflow_dags_path, airflow_postgres_path, ao_package_path_)

    # Start the appropriate process
    if command == 'start':
        # Check that appropriate paths have been set otherwise log a warning
        if 'GOOGLE_APPLICATION_CREDENTIALS' not in env:
            logging.warning(
                "The environment variable `GOOGLE_APPLICATION_CREDENTIALS` is not set: unable to communicate "
                "with Google Cloud")

        if 'GOOGLE_BUCKET_NAME' not in env:
            logging.warning("The environment variable `GOOGLE_BUCKET_NAME` is not set: unable to save data to Google "
                            "Cloud")

        if 'GOOGLE_PROJECT_ID' not in env:
            logging.warning("The environment variable `GOOGLE_PROJECT_ID` is not set: unable to save data to Google "
                            "Cloud")

        # Build the containers first
        proc = subprocess.Popen(args + ['build'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        output, error = proc.communicate()
        __log_output(output, error, proc.returncode)

        # Start the built containers
        proc = subprocess.Popen(args + ['up', '-d'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        output, error = proc.communicate()
        __log_output(output, error, proc.returncode)
    elif command == 'stop':
        proc = subprocess.Popen(args + ['down'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        output, error = proc.communicate()
        __log_output(output, error, proc.returncode)

    exit(os.EX_OK)


if __name__ == "__main__":
    cli()
