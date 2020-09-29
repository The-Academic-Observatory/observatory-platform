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

import json
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

from observatory_platform.utils.config_utils import observatory_home, \
    observatory_package_path, \
    dags_path as default_dags_path, \
    ObservatoryConfig, \
    terraform_credentials_path as default_terraform_credentials_path, docker_path
from observatory_platform.utils.jinja2_utils import render_template
from observatory_platform.utils.proc_utils import wait_for_process, stream_process
from observatory_platform.utils.terraform_utils import TerraformApi


def build_file(template_file_name: str, output_file_name: str, working_dir: str, **kwargs):
    template_path = os.path.join(docker_path(), template_file_name)
    render = render_template(template_path, **kwargs)

    output_path = os.path.join(working_dir, output_file_name)
    with open(output_path, 'w') as f:
        f.write(render)


def build_docker_files(config: ObservatoryConfig, working_dir: str, is_env_local: bool):
    build_file('Dockerfile.observatory.jinja2', 'Dockerfile.observatory', working_dir,
               dags_projects=config.dags_projects,
               is_env_local=is_env_local)
    build_file('docker-compose.observatory.yml.jinja2', 'docker-compose.observatory.yml', working_dir,
               dags_projects=config.dags_projects,
               airflow_connections=config.airflow_connections,
               airflow_variables=config.airflow_variables,
               is_env_local=is_env_local)
    build_file('entrypoint-airflow.sh.jinja2', 'entrypoint-airflow.sh', working_dir,
               dags_projects=config.dags_projects,
               is_env_local=is_env_local)


def build_requirements_files(config: ObservatoryConfig, working_dir: str):
    # Copy observatory requirements.txt
    input_file = os.path.join(observatory_package_path(), 'requirements.txt')
    output_file = os.path.join(working_dir, f'requirements.txt')
    shutil.copy(input_file, output_file)

    # Copy all project requirements files for local projects
    for project in config.dags_projects:
        if project.type == 'local':
            input_file = os.path.join(project.value, 'requirements.txt')
            output_file = os.path.join(working_dir, f'requirements.{project.name}.txt')
            shutil.copy(input_file, output_file)


def build_observatory(config: ObservatoryConfig, working_dir: str, is_env_local: bool = True):
    # Build Docker and requirements files
    build_docker_files(config, working_dir, is_env_local)
    build_requirements_files(config, working_dir)

    # Copy other files
    file_names = ['entrypoint-root.sh', 'elasticsearch.yml']
    for file_name in file_names:
        input_file = os.path.join(docker_path(), file_name)
        output_file = os.path.join(working_dir, file_name)
        shutil.copy(input_file, output_file)



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


def gen_config_interface(backend: str, terraform_variables_path: str = ObservatoryConfig.TERRAFORM_VARIABLES_PATH):
    """
    Command line user interface for generating config.yaml

    :param backend: Which type of config file, either 'local' or 'terraform'.
    :param terraform_variables_path: Path to the terraform variables.tf file.
    :return: None
    """

    print("Generating config.yaml...")
    if backend == 'local':
        config_path = ObservatoryConfig.LOCAL_DEFAULT_PATH
        terraform_variables_path = None
    else:
        config_path = ObservatoryConfig.TERRAFORM_DEFAULT_PATH
        print(f"- Terraform variables.tf path: {terraform_variables_path}")

    if not os.path.exists(config_path) or click.confirm(f'The file "{config_path}" exists, do you want to '
                                                        f'overwrite it?'):
        ObservatoryConfig.save_default(backend, config_path, terraform_variables_path)
        print(f'config.yaml saved to: "{config_path}"')
        print(f"Please customise the parameters with '<--' in the config file. Parameters with '#' are optional.")
    else:
        print("Not generating config.yaml")


class OptionRequiredIfTerraform(click.Option):
    """ Make variables.tf file required when generating config file for terraform. """

    def full_process_value(self, ctx, value):
        value = super(OptionRequiredIfTerraform, self).full_process_value(ctx, value)

        if value is None and ctx.params['command'] == 'config_terraform.yaml':
            msg = 'Terraform variables.tf file required if command is "config_terraform.yaml"'
            raise click.MissingParameter(ctx=ctx, param=self, message=msg)
        return value


# increase content width for cleaner help output
@cli.command(context_settings=dict(max_content_width=120))
@click.argument('command',
                type=click.Choice(['fernet-key', 'config.yaml', 'config_terraform.yaml']))
@click.option('--terraform-variables-path',
              type=click.Path(exists=True, file_okay=True, dir_okay=False),
              default=ObservatoryConfig.TERRAFORM_VARIABLES_PATH,
              cls=OptionRequiredIfTerraform,
              help='Terraform variables.tf file.',
              show_default=True)
def generate(command, terraform_variables_path):
    """ Generate information for the Observatory Platform platform.\n

    COMMAND: the command to give the generator:\n
      - fernet-key: generate a fernet key.\n
      - config.yaml: generate a config.yaml file.\n
      - config_terraform.yaml: generate a config_terraform.yaml file.\n
    """

    if command == 'fernet-key':
        print(gen_fernet_key())
    elif command == 'config.yaml':
        gen_config_interface('local')
    elif command == 'config_terraform.yaml':
        gen_config_interface('terraform', terraform_variables_path)


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


# def make_airflow_var_dags_paths(dags_projects: List[DagsProject]):
#     return ':'.join([f'/opt/observatory/{project.name}/{project.dags_path}' for project in dags_projects])


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
    # TODO: make config file normal again!
    # config_ = config.local

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

    # Create Airflow variables
    env['AIRFLOW_VAR_LOAD_DEFAULT_DAGS'] = str(config.load_default_dags)
    env['AIRFLOW_VAR_DAGS_PACKAGE_NAMES'] = ':'.join([project.name.replace('-', '_') for project in
                                                      config.dags_projects])
    for variable in config.airflow_variables:
        env[variable.env_var_name] = variable.value

    # Airflow connections
    for conn in config.airflow_connections:
        env[conn.env_var_name] = conn.value

    # Set connections and variables
    # for obj in itertools.chain(AirflowConn, AirflowVar):
    #     if obj.value['schema']:
    #         if type(obj) == AirflowConn:
    #             env[f'AIRFLOW_CONN_{obj.get().upper()}'] = config_.airflow_connections[obj.get()]
    #         else:
    #             env[f'AIRFLOW_VAR_{obj.get().upper()}'] = config_.airflow_variables[obj.get()]

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


@cli.command()
@click.argument('command',
                type=click.Choice(['start', 'stop']))
@click.option('--config-path',
              type=click.Path(exists=False, file_okay=True, dir_okay=False),
              default=ObservatoryConfig.LOCAL_DEFAULT_PATH,
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
    docker_exe_path = get_docker_path()
    docker_compose_path = get_docker_compose_path()
    docker_running = is_docker_running()
    config_exists = os.path.exists(config_path)
    config_is_valid = False
    config = None
    if config_exists:
        config = ObservatoryConfig(config_path)
        if config is not None:
            config_is_valid = config.is_valid

    # Check that all dependencies are met
    all_deps = all([docker_exe_path is not None,
                    docker_compose_path is not None,
                    docker_running,
                    config_exists,
                    config_is_valid,
                    config is not None])

    # Indentation variables
    indent1 = 2
    indent2 = 3
    indent3 = 4
    indent4 = 5

    if not all_deps:
        print("Observatory Platform: dependencies missing".ljust(min_line_chars))
    else:
        print("Observatory Platform: all dependencies found".ljust(min_line_chars))

    print(indent("Docker:", indent1))
    if docker_exe_path is not None:
        print(indent(f"- path: {docker_exe_path}", indent2))

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
            for key, values in config.validator.errors.items():
                for value in values:
                    if type(value) is dict:
                        print(indent(f'- {key}: ', indent3))
                        for nested_key, nested_value in value.items():
                            print(indent('- {}: {}'.format(nested_key, *nested_value), indent4))
                    else:
                        print(indent('- {}: {}'.format(key, *values), indent3))
    else:
        print(indent("- file not found, generating a default file", indent2))
        gen_config_interface('local')

    if not all_deps:
        exit(os.EX_CONFIG)

    ##################
    # Run commands   #
    ##################

    # Working directory for running the build etc
    package_path = observatory_package_path()
    current_working_dir = observatory_home('build')

    # Make environment variables for running commands
    env = get_env(dags_path, data_path, logs_path, postgres_path, host_uid, host_gid, package_path, config)

    # Build observatory files
    build_observatory(config, current_working_dir, is_env_local=True)

    # Docker compose commands
    compose_args = ['docker-compose', '-f', 'docker-compose.observatory.yml']

    # Start the appropriate process
    if command == 'start':
        print('Observatory Platform: building...'.ljust(min_line_chars), end="\r")

        # Build the containers first
        proc: Popen = subprocess.Popen(compose_args + ['build'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       env=env, cwd=current_working_dir)
        output, error = stream_process(proc, debug)

        if proc.returncode == 0:
            print('Observatory Platform: built'.ljust(min_line_chars))
        else:
            print('Observatory Platform: build error'.ljust(min_line_chars))
            exit(os.EX_CONFIG)

        # Start the built containers
        print('Observatory Platform: starting...'.ljust(min_line_chars), end='\r')
        proc: Popen = subprocess.Popen(compose_args + ['up', '-d'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       env=env, cwd=current_working_dir)
        output, error = stream_process(proc, debug)

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
            exit(os.EX_CONFIG)

    elif command == 'stop':
        print('Observatory Platform: stopping...'.ljust(min_line_chars), end='\r')
        proc = subprocess.Popen(compose_args + ['down'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
                                cwd=current_working_dir)
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


def create_terraform_variables(config: ObservatoryConfig) -> list:
    """
    Create a list of variable attributes from variables defined in the config file
    :param config: the config object
    :return: terraform variables
    """
    terraform_variables = []
    for variable, value in config.dict.items():
        hcl = False
        # terraform uses lowercase bool values
        if isinstance(value, bool):
            value = str(value).lower()
        # turn python dict into hcl syntax dict
        elif isinstance(value, dict):
            value = json.dumps(value, separators=(',', '='))
            hcl = True
        # if value is a local path that exists, use the content of this file instead of the literal path string
        elif os.path.exists(str(value)):
            lines = ''
            with open(value, 'r') as file:
                for line in file:
                    lines += line
            value = lines
        # add description if available
        try:
            description = config.schema[variable]['meta']['description']
        except KeyError:
            description = None
        # whether variable should be marked as sensitive in terraform cloud
        sensitive = True if variable in config.sensitive_variables else False

        # create variable attributes dict
        terraform_variable = TerraformApi.create_var_attributes(variable, value, category='terraform',
                                                                description=description, hcl=hcl, sensitive=sensitive)
        terraform_variables.append(terraform_variable)
    return terraform_variables


# increase content width for cleaner help output
@cli.command(context_settings=dict(max_content_width=120))
@click.argument('command',
                type=click.Choice(['create-workspace', 'update-workspace']))
# The path to the config_terraform.yaml configuration file.
@click.argument('config-path',
                type=click.Path(exists=True, file_okay=True, dir_okay=False),
                )
@click.option('--terraform-variables-path',
              type=click.Path(exists=False, file_okay=True, dir_okay=False),
              default=ObservatoryConfig.TERRAFORM_VARIABLES_PATH,
              help='Terraform variables.tf file',
              show_default=True)
@click.option('--terraform-credentials-file',
              type=click.Path(exists=False, file_okay=True, dir_okay=False),
              default=default_terraform_credentials_path(),
              help='',
              show_default=True)
@click.option('-v', '--verbose',
              count=True,
              help='Set the verbosity level of terraform API (max -vv).')
def terraform(command, config_path, terraform_variables_path, terraform_credentials_file, verbose):
    """
    Uses file at CONFIG_PATH to create or update a workspace in Terraform Cloud.
    A default config file can be created with `observatory generate config_terraform.yaml`
    """
    # The minimum number of characters per line
    min_line_chars = 80

    ######################
    # Check dependencies #
    ######################

    print("Observatory Terraform: checking dependencies...".ljust(min_line_chars), end='\r')

    # Get all dependencies
    config_exists = os.path.exists(config_path)
    terraform_variables_exists = os.path.exists(terraform_variables_path)
    terraform_credentials_exists = os.path.exists(terraform_credentials_file)
    config_is_valid = False
    config = None
    if terraform_variables_exists:
        config = ObservatoryConfig(config_path, terraform_variables_path)
        if config is not None:
            config_is_valid = config.is_valid

    # Check that all dependencies are met
    all_deps = all(
        [config_exists, terraform_variables_exists,
         terraform_credentials_exists, config_is_valid, config is not None])

    # Indentation variables
    indent1 = 2
    indent2 = 3
    indent3 = 4

    if not all_deps:
        print("Observatory Terraform: dependencies missing".ljust(min_line_chars))
    else:
        print("Observatory Terraform: all dependencies found".ljust(min_line_chars))

    print(indent("Terraform variables.tf file:", indent1))
    if terraform_variables_exists:
        print(indent(f"- path: {terraform_variables_path}", indent2))
    else:
        print(indent("- file not found, check path to your variables file", indent2))

    print(indent("Config:", indent1))
    if config_exists:
        print(indent(f"- path: {config_path}", indent2))

        if not terraform_variables_exists:
            print(indent("- need terraform variables file to determine whether file is valid", indent2))
        elif config.is_valid:
            print(indent("- file valid", indent2))
        else:
            print(indent("- file invalid", indent2))
            for key, value in config.validator.errors.items():
                print(indent(f'- {key}: {value}', indent3))
    else:
        if not terraform_variables_exists:
            print(indent("- file not found, need terraform variables.tf file to generate default config", indent2))
        else:
            print(indent("- file not found, generating a default file", indent2))
            gen_config_interface('terraform', terraform_variables_path)

    print(indent("Terraform credentials file:", indent1))
    if terraform_credentials_exists:
        print(indent(f"- path: {terraform_credentials_file}", indent2))
    else:
        print(indent("- file not found, create one by running 'terraform login'", indent2))

    if not all_deps:
        exit(os.EX_CONFIG)

    ####################
    # Create Workspace #
    ####################

    # Get terraform token
    token = TerraformApi.token_from_file(terraform_credentials_file)
    terraform_api = TerraformApi(token, verbose)

    # Get variables
    terraform_variables = create_terraform_variables(config)

    # Get environment and prefix
    prefix = config.terraform.backend['terraform']['workspaces_prefix']
    environment = config.terraform.environment

    # Get organization, use with api
    organization = config.terraform.backend['terraform']['organization']

    workspace = prefix + environment

    # display settings for workspace
    print('\nTerraform Cloud Workspace: ')
    print(indent(f'Organization: {organization}', indent1))
    print(indent(f"- Name: {workspace} (prefix: '{prefix}' + suffix: '{environment}')", indent1))
    print(indent(f'- Settings: ', indent1))
    print(indent(f'- Auto apply: True', indent2))
    print(indent(f'- Terraform Cloud Version: 0.13.0-beta3', indent2))
    print(indent(f'- Terraform Variables:', indent1))
    # creating a new workspace
    if command == 'create-workspace':
        for variable in terraform_variables:
            if variable['sensitive'] == 'true':
                print(indent(f"* \x1B[3m{variable['key']}\x1B[23m: sensitive", indent2))
            else:
                print(indent(f"* \x1B[3m{variable['key']}\x1B[23m: {variable['value']}", indent2))
        # confirm creating workspace
        if click.confirm("Would you like to create a new workspace with these settings?"):
            print("Creating workspace...")
            # Create new workspace
            terraform_api.create_workspace(organization, workspace, auto_apply=True,
                                           description="")
            # Get workspace ID
            workspace_id = terraform_api.workspace_id(organization, workspace)

            # Add variables to workspace
            for var in terraform_variables:
                terraform_api.add_workspace_variable(var, workspace_id)
            print('Successfully created workspace')
    # updating an existing workspace
    elif command == 'update-workspace':
        # Get workspace ID
        workspace_id = terraform_api.workspace_id(organization, workspace)
        add, edit, unchanged, delete = terraform_api.plan_variable_changes(terraform_variables, workspace_id)
        if add:
            print(indent(f'NEW', indent1))
            for variable in add:
                print(indent(f"* \x1B[3m{variable['key']}\x1B[23m: {variable['value']}", indent2))
        if edit:
            print(indent(f'UPDATE', indent1))
            for variable in edit:
                if variable[0]['sensitive'] == 'true':
                    print(indent(f"* \x1B[3m{variable[0]['key']}\x1B[23m: {variable[2]} -> sensitive", indent2))
                else:
                    print(
                        indent(f"* \x1B[3m{variable[0]['key']}\x1B[23m: {variable[2]} -> {variable[0]['value']}",
                               indent2))
        if delete:
            print(indent(f'DELETE', indent1))
            for variable in delete:
                print(indent(f"* \x1B[3m{variable[0]}\x1B[23m: {variable[2]}", indent2))
        if unchanged:
            print(indent(f'UNCHANGED', indent1))
            for variable in unchanged:
                print(indent(f"* \x1B[3m{variable['key']}\x1B[23m: {variable['value']}", indent2))

        # confirm creating workspace
        if click.confirm("Would you like to update the workspace with these settings?"):
            print("Updating workspace...")
            # Update variables in workspace
            terraform_api.update_workspace_variables(add, edit, delete, workspace_id)
            print('Successfully updated workspace')


if __name__ == "__main__":
    cli()
