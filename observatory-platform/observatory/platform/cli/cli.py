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

# Author: James Diprose, Aniek Roelofs

import os
from typing import Union

import click

from observatory.platform.cli.click_utils import indent, INDENT1, INDENT2, INDENT3
from observatory.platform.cli.generate_command import GenerateCommand
from observatory.platform.cli.platform_command import PlatformCommand
from observatory.platform.cli.terraform_command import TerraformCommand, TerraformConfig
from observatory.platform.platform_builder import (BUILD_PATH, DAGS_MODULE, DATA_PATH, LOGS_PATH,
                                                   POSTGRES_PATH, HOST_UID, HOST_GID, REDIS_PORT, FLOWER_UI_PORT,
                                                   AIRFLOW_UI_PORT, ELASTIC_PORT, KIBANA_PORT,
                                                   DOCKER_NETWORK_NAME, DOCKER_COMPOSE_PROJECT_NAME, DEBUG)
from observatory.platform.utils.config_utils import (observatory_home,
                                                     terraform_credentials_path as default_terraform_credentials_path)

PLATFORM_NAME = 'Observatory Platform'
TERRAFORM_NAME = 'Observatory Terraform'

LOCAL_CONFIG_PATH = os.path.join(observatory_home(), 'config.yaml')
TERRAFORM_CONFIG_PATH = os.path.join(observatory_home(), 'config-terraform.yaml')


@click.group()
def cli():
    """ The Observatory Platform command line tool.

    COMMAND: the commands to run include:\n
      - platform: start and stop the local Observatory Platform platform.\n
      - generate: generate a variety of outputs.\n
      - terraform: manage Terraform Cloud workspaces.\n
    """

    pass


@cli.command()
@click.argument('command',
                type=click.Choice(['start', 'stop']))
@click.option('--config-path',
              type=click.Path(exists=False, file_okay=True, dir_okay=False),
              default=LOCAL_CONFIG_PATH,
              help='The path to the config.yaml configuration file.',
              show_default=True)
@click.option('--build-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              default=BUILD_PATH,
              help='The path on the host machine to use for building the Observatory Platform.',
              show_default=True)
@click.option('--dags-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              default=DAGS_MODULE,
              help='The path on the host machine to mount as the Apache Airflow DAGs folder.',
              show_default=True)
@click.option('--data-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              default=DATA_PATH,
              help='The path on the host machine to mount as the data folder.',
              show_default=True)
@click.option('--logs-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              default=LOGS_PATH,
              help='The path on the host machine to mount as the logs folder.',
              show_default=True)
@click.option('--postgres-path',
              type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=False),
              default=POSTGRES_PATH,
              help='The path on the host machine to mount as the PostgreSQL data folder.',
              show_default=True)
@click.option('--host-uid',
              type=click.INT,
              default=HOST_UID,
              help='The user id of the host system. Used to set the user id in the Docker containers.',
              show_default=True)
@click.option('--host-gid',
              type=click.INT,
              default=HOST_GID,
              help='The group id of the host system. Used to set the group id in the Docker containers.',
              show_default=True)
@click.option('--redis-port',
              type=click.INT,
              default=REDIS_PORT,
              help='The host Redis port number.',
              show_default=True)
@click.option('--flower-ui-port',
              type=click.INT,
              default=FLOWER_UI_PORT,
              help='The host\'s Flower UI port number.',
              show_default=True)
@click.option('--airflow-ui-port',
              type=click.INT,
              default=AIRFLOW_UI_PORT,
              help='The host\'s Apache Airflow UI port number.',
              show_default=True)
@click.option('--elastic-port',
              type=click.INT,
              default=ELASTIC_PORT,
              help='The host\'s Elasticsearch port number.',
              show_default=True)
@click.option('--kibana-port',
              type=click.INT,
              default=KIBANA_PORT,
              help='The host\'s Kibana port number.',
              show_default=True)
@click.option('--docker-network-name',
              type=click.STRING,
              default=DOCKER_NETWORK_NAME,
              help='The Docker Network name, used to specify a custom Docker Network.',
              show_default=True)
@click.option('--docker-compose-project-name',
              type=click.STRING,
              default=DOCKER_COMPOSE_PROJECT_NAME,
              help='The namespace for Docker containers.',
              show_default=True)
@click.option('--debug',
              is_flag=True,
              default=DEBUG,
              help='Print debugging information.')
def platform(command: str, config_path: str, build_path: str, dags_path: str, data_path: str, logs_path: str,
             postgres_path: str, host_uid: int, host_gid: int, redis_port: int, flower_ui_port: int,
             airflow_ui_port: int, elastic_port: int, kibana_port: int, docker_network_name: Union[None, str],
             docker_compose_project_name: str, debug):
    """ Run the local Observatory Platform platform.\n

    COMMAND: the command to give the platform:\n
      - start: start the platform.\n
      - stop: stop the platform.\n
    """

    # Make the platform command, which encapsulates functionality for running the observatory
    cmd = PlatformCommand(config_path, build_path=build_path, dags_path=dags_path,
                          data_path=data_path, logs_path=logs_path, postgres_path=postgres_path,
                          host_uid=host_uid, host_gid=host_gid, redis_port=redis_port,
                          flower_ui_port=flower_ui_port, airflow_ui_port=airflow_ui_port,
                          elastic_port=elastic_port, kibana_port=kibana_port,
                          docker_network_name=docker_network_name,
                          docker_compose_project_name=docker_compose_project_name,
                          debug=debug)
    generate_cmd = GenerateCommand()

    # The minimum number of characters per line
    min_line_chars = 80

    ######################
    # Check dependencies #
    ######################

    print(f"{PLATFORM_NAME}: checking dependencies...".ljust(min_line_chars), end='\r')

    if not cmd.is_environment_valid:
        print(f"{PLATFORM_NAME}: dependencies missing".ljust(min_line_chars))
    else:
        print(f"{PLATFORM_NAME}: all dependencies found".ljust(min_line_chars))

    print(indent("Docker:", INDENT1))
    if cmd.docker_exe_path is not None:
        print(indent(f"- path: {cmd.docker_exe_path}", INDENT2))

        if cmd.is_docker_running:
            print(indent(f"- running", INDENT2))
        else:
            print(indent("- not running, please start", INDENT2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/get-docker/", INDENT2))

    print(indent("Host machine settings:", INDENT1))
    print(indent(f"- observatory home: {observatory_home()}", INDENT2))
    print(indent(f"- data-path: {data_path}", INDENT2))
    print(indent(f"- dags-path: {dags_path}", INDENT2))
    print(indent(f"- logs-path: {logs_path}", INDENT2))
    print(indent(f"- postgres-path: {postgres_path}", INDENT2))
    print(indent(f"- host-uid: {host_uid}", INDENT2))

    print(indent("Docker Compose:", INDENT1))
    if cmd.docker_compose_path is not None:
        print(indent(f"- path: {cmd.docker_compose_path}", INDENT2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/compose/install/", INDENT2))

    print(indent("config.yaml:", INDENT1))
    if cmd.config_exists:
        print(indent(f"- path: {config_path}", INDENT2))

        if cmd.config.is_valid:
            print(indent("- file valid", INDENT2))
        else:
            print(indent("- file invalid", INDENT2))
            for error in cmd.config.errors:
                print(indent('- {}: {}'.format(error.key, error.value), INDENT3))
    else:
        print(indent(f"- file not found, generating a default file on path: {config_path}", INDENT2))
        generate_cmd.generate_local_config(config_path)

    if not cmd.is_environment_valid:
        exit(os.EX_CONFIG)

    ##################
    # Run commands   #
    ##################

    # Start the appropriate process
    if command == 'start':
        print(f'{PLATFORM_NAME}: building...'.ljust(min_line_chars), end="\r")
        output, error, return_code = cmd.build()

        if return_code == 0:
            print(f'{PLATFORM_NAME}: built'.ljust(min_line_chars))
        else:
            print(f'{PLATFORM_NAME}: build error'.ljust(min_line_chars))
            exit(os.EX_CONFIG)

        # Start the built containers
        print(f'{PLATFORM_NAME}: starting...'.ljust(min_line_chars), end='\r')
        output, error, return_code = cmd.start()

        if return_code == 0:
            ui_started = cmd.wait_for_airflow_ui(timeout=120)

            if ui_started:
                print(f'{PLATFORM_NAME}: started'.ljust(min_line_chars))
                print(f'View the Apache Airflow UI at {cmd.ui_url}')
            else:
                print(f'{PLATFORM_NAME}: error starting'.ljust(min_line_chars))
                print(f'Could not find the Airflow UI at {cmd.ui_url}')
        else:
            print("Error starting the Observatory Platform")
            exit(os.EX_CONFIG)

    elif command == 'stop':
        print(f'{PLATFORM_NAME}: stopping...'.ljust(min_line_chars), end='\r')
        cmd.make_files()
        output, error, return_code = cmd.stop()

        if debug:
            print(output)

        if return_code == 0:
            print(f'{PLATFORM_NAME}: stopped'.ljust(min_line_chars))
        else:
            print(f'{PLATFORM_NAME}: error stopping'.ljust(min_line_chars))
            print(error)
            exit(os.EX_CONFIG)
    exit(os.EX_OK)


@cli.group()
def generate():
    """ The Observatory Platform generate command.

    COMMAND: the commands to run include:\n
      - secrets: generate secrets.\n
      - config: generate configuration files for the Observatory Platform.\n
    """

    pass


@generate.command()
@click.argument('command', type=click.Choice(['fernet-key']))
def secrets(command: str):
    """ Generate secrets for the Observatory Platform.\n

    COMMAND: the type of secret to generate:\n
      - fernet-key: generate a random Fernet Key.\n
    """

    if command == 'fernet-key':
        cmd = GenerateCommand()
        print(cmd.generate_fernet_key())


@generate.command()
@click.argument('command', type=click.Choice(['local', 'terraform']))
@click.option('--config-path',
              type=click.Path(exists=False, file_okay=True, dir_okay=False),
              default=None,
              help='The path to the config file to generate.',
              show_default=True)
def config(command: str, config_path: str):
    """ Generate config files for the Observatory Platform.\n

    COMMAND: the type of config file to generate:\n
      - local: generate a config file for running the Observatory Platform locally.\n
      - terraform: generate a config file for running the Observatory Platform with Terraform.\n
    """

    # Make the generate command, which encapsulates functionality for generating data
    cmd = GenerateCommand()

    cmd_func = None
    config_name = ''
    if command == 'local':
        if config_path is None:
            config_path = LOCAL_CONFIG_PATH
        cmd_func = cmd.generate_local_config
        config_name = 'Observatory Config'
    elif command == 'terraform':
        if config_path is None:
            config_path = TERRAFORM_CONFIG_PATH
        cmd_func = cmd.generate_terraform_config
        config_name = 'Terraform Config'

    if not os.path.exists(config_path) or \
            click.confirm(f'The file "{config_path}" exists, do you want to overwrite it?'):
        cmd_func(config_path)
    else:
        click.echo(f"Not generating {config_name}")


# increase content width for cleaner help output
@cli.command(context_settings=dict(max_content_width=120))
@click.argument('command',
                type=click.Choice(['build-terraform', 'build-image', 'create-workspace', 'update-workspace']))
# The path to the config-terraform.yaml configuration file.
@click.argument('config-path',
                type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option('--terraform-credentials-path',
              type=click.Path(exists=False, file_okay=True, dir_okay=False),
              default=default_terraform_credentials_path(),
              help='',
              show_default=True)
@click.option('-v', '--verbose',
              count=True,
              help='Set the verbosity level of terraform API (max -vv).')
def terraform(command, config_path, terraform_credentials_path, verbose):
    """ Commands to manage the deployment of the Observatory Platform with Terraform Cloud.\n

    COMMAND: the type of config file to generate:\n
      - create-workspace: create a Terraform Cloud workspace.\n
      - update-workspace: update a Terraform Cloud workspace.\n
      - build-image: build a Google Compute image for the Terraform deployment with Packer.\n
    """

    # The minimum number of characters per line
    min_line_chars = 80

    terraform_cmd = TerraformCommand(config_path, terraform_credentials_path, verbose=verbose)
    generate_cmd = GenerateCommand()

    ######################
    # Check dependencies #
    ######################

    print(f"{TERRAFORM_NAME}: checking dependencies...".ljust(min_line_chars), end='\r')

    if not terraform_cmd.is_environment_valid:
        print(f"{TERRAFORM_NAME}: dependencies missing".ljust(min_line_chars))
    else:
        print(f"{TERRAFORM_NAME}: all dependencies found".ljust(min_line_chars))

    print(indent("Config:", INDENT1))
    if terraform_cmd.config_exists:
        print(indent(f"- path: {config_path}", INDENT2))
        if terraform_cmd.config.is_valid:
            print(indent("- file valid", INDENT2))
        else:
            print(indent("- file invalid", INDENT2))
            for key, value in terraform_cmd.config.validator.errors.items():
                print(indent(f'- {key}: {value}', INDENT3))
    else:
        print(indent("- file not found, generating a default file", INDENT2))
        generate_cmd.generate_terraform_config(config_path)

    print(indent("Terraform credentials file:", INDENT1))
    if terraform_cmd.terraform_credentials_exists:
        print(indent(f"- path: {terraform_cmd.terraform_credentials_path}", INDENT2))
    else:
        print(indent("- file not found, create one by running 'terraform login'", INDENT2))

    if not terraform_cmd.is_environment_valid:
        exit(os.EX_CONFIG)

    ####################
    # Run commands     #
    ####################

    if command == 'build-terraform':
        # Build image with packer
        terraform_cmd.build_terraform()
    elif command == 'build-image':
        # Build image with packer
        terraform_cmd.build_image()
    else:
        # Get organization, environment and prefix
        organization = terraform_cmd.config.terraform.organization
        environment = terraform_cmd.config.backend.environment.value
        workspace = terraform_cmd.config.terraform_workspace_id

        # Display settings for workspace
        print('\nTerraform Cloud Workspace: ')
        print(indent(f'Organization: {organization}', INDENT1))
        print(indent(f"- Name: {workspace} (prefix: '{TerraformConfig.WORKSPACE_PREFIX}' + suffix: '{environment}')", INDENT1))
        print(indent(f'- Settings: ', INDENT1))
        print(indent(f'- Auto apply: True', INDENT2))
        print(indent(f'- Terraform Variables:', INDENT1))

        # Create a new workspace
        if command == 'create-workspace':
            terraform_cmd.create_workspace()

        # Update an existing workspace
        elif command == 'update-workspace':
            terraform_cmd.update_workspace()


if __name__ == "__main__":
    cli()
