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
import pathlib
import shutil
import subprocess
import time
import urllib.request
from functools import partial
from subprocess import Popen
from typing import Tuple, Union

import click
import docker
import requests
from cryptography.fernet import Fernet

import academic_observatory.terraform
from academic_observatory.utils.config_utils import observatory_home, observatory_package_path, dockerfiles_path, \
    dags_path, \
    ObservatoryConfig


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


def stream_process(proc: Popen, info: str, min_line_chars: int, verbose: int) -> Tuple[str, str]:
    """ Print output while a process is running, returning the std output and std error streams as strings.

    :param proc: the process object.
    :return: std output and std error streams as strings.
    """
    print(f'{info}'.ljust(min_line_chars))
    output_concat = ''
    error_concat = ''
    if verbose >= 2:
        print(f"Command executed:\n{subprocess.list2cmdline(proc.args)}")
    while True:
        for line in proc.stdout:
            output = line.decode('utf-8')
            if verbose >= 1:
                print(output, end='')
            output_concat += output
        for line in proc.stderr:
            error = line.decode('utf-8')
            print(error, end='')
            error_concat += error
        if proc.poll() is not None:
            break
    return output_concat, error_concat


def wait_for_process(proc: Popen) -> Tuple[str, str]:
    """ Wait for a process to finish, returning the std output and std error streams as strings.

    :param proc: the process object.
    :return: std output and std error streams as strings.
    """
    output, error = proc.communicate()
    output = output.decode('utf-8')
    error = error.decode('utf-8')
    return output, error


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


def indent(string: str, num_spaces: int) -> str:
    return string.rjust(len(string) + num_spaces)


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

    # Get all dependencies
    docker_path = get_docker_path()
    docker_compose_path = get_docker_compose_path()
    docker_running = is_docker_running()
    gapp_cred_var = get_google_application_credentials()
    gapp_cred_file_exists = os.path.exists(gapp_cred_var) if gapp_cred_var is not None else False
    fernet_key = get_fernet_key()
    config_exists = os.path.exists(config_path)
    config_valid, config_validator, config = ObservatoryConfig.load(config_path)

    # Check that all dependencies are met
    all_deps = all([docker_path is not None,
                    docker_compose_path is not None,
                    docker_running,
                    gapp_cred_var is not None,
                    gapp_cred_file_exists,
                    fernet_key is not None,
                    config_exists,
                    config_valid,
                    config is not None])

    # Indentation variables
    indent1 = 2
    indent2 = 3
    indent3 = 4

    if not all_deps:
        print("Academic Observatory: dependencies missing".ljust(min_line_chars))
    else:
        print("Academic Observatory: all dependencies found".ljust(min_line_chars))

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
    print(indent(f"- airflow-ui-port: {airflow_ui_port}", indent2))
    print(indent(f"- airflow-postgres-path: {airflow_postgres_path}", indent2))

    print(indent("Docker Compose:", indent1))
    if docker_compose_path is not None:
        print(indent(f"- path: {docker_compose_path}", indent2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/compose/install/", indent2))

    print(indent("GOOGLE_APPLICATION_CREDENTIALS:", indent1))
    if gapp_cred_var is not None:
        print(indent(f"- environment variable: {gapp_cred_var}", indent2))

        if gapp_cred_file_exists:
            print(indent("- file exists", indent2))
        else:
            print(indent("- file does not exist", indent2))
    else:
        print(indent("- environment variable not set. "
                     "See https://cloud.google.com/docs/authentication/getting-started for instructions on how to set "
                     "it. Add it to ~/.bashrc.", indent2))

    print(indent("FERNET_KEY:", indent1))
    if fernet_key is not None:
        print(indent("- environment variable: hidden", indent2))
    else:
        print(indent("- environment variable: not set. See below for command to set it: \n"
                     "               echo export FERNET_KEY=`observatory generate fernet-key` >> ~/.bashrc && "
                     "source ~/.bashrc", indent2))

    print(indent("config.yaml:", indent1))
    if config_exists:
        print(indent(f"- path: {config_path}", indent2))

        if config_valid:
            print(indent("- file valid", indent2))
        else:
            print(indent("- file invalid", indent2))
            for key, value in config_validator.errors.items():
                print(indent(f'- {key}: {", ".join(value)}', indent3))
    else:
        print(indent("- file not found, generating a default file", indent2))
        gen_config_interface()

    if not all_deps:
        exit(os.EX_CONFIG)

    ##################
    # Run commands   #
    ##################

    # Make environment variables for running commands
    package_path = observatory_package_path()
    env = get_env(config_path, dags_path, data_path, airflow_ui_port, airflow_postgres_path,
                  package_path, fernet_key, gapp_cred_var)

    # Make docker-compose command
    args = ['docker-compose']
    compose_files = ['docker-compose.secrets.yml', 'docker-compose.airflow-postgres.yml',
                     'docker-compose.airflow-webserver.yml']
    docker_path = dockerfiles_path()
    for file_name in compose_files:
        path = os.path.join(docker_path, 'platform', file_name)
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


def get_terraform_env(terraform_app_token: str, terraform_dir: str):
    """
    """
    env = os.environ.copy()
    env['HOST_TERRAFORM_APP_TOKEN'] = terraform_app_token
    env['HOST_WORKSPACES_PATH'] = terraform_dir
    return env


def terraform_build(docker_args: list, env, min_line_chars: int, verbose: int):
    info = 'Building docker container...'
    proc: Popen = subprocess.Popen(docker_args + ['build'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    output, error = stream_process(proc, info, min_line_chars, verbose)
    return output, error


def terraform_run_cmd(terraform_args: list, info: str, docker_args: list, env, min_line_chars: int, verbose: int):
    proc: Popen = subprocess.Popen(docker_args + ['run', 'terraform', 'terraform'] + terraform_args,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    output, error = stream_process(proc, info, min_line_chars, verbose)
    if proc.returncode == 0:
        print('Finished.\n'.ljust(min_line_chars))
    else:
        print('Error.\n'.ljust(min_line_chars))
        exit(os.EX_CONFIG)
    return output, error


def bash_run_cmd(bash_cmd: str, info: str, docker_args: list, env, min_line_chars: int, verbose: int):
    proc: Popen = subprocess.Popen(docker_args + ['run', 'terraform', 'bash', '-c'] + [bash_cmd],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    output, error = stream_process(proc, info, min_line_chars, verbose)
    if proc.returncode == 0:
        print('Finished.\n'.ljust(min_line_chars))
    else:
        print('Error.\n'.ljust(min_line_chars))
        exit(os.EX_CONFIG)
    return output, error


class TerraformConfig:
    # hardcoded in terraform backend configuration. Can't be obtained from variables
    prefix = 'ao-'
    organisation = 'coki'

    # api url
    api_url = 'https://app.terraform.io/api/v2'

    # workspaces that will be created and the relative directory where their configuration files are hosted
    workspaces = {'dev': None,
                  'prod': None,
                  'shared': 'shared'}
    # workspaces = {'shared': 'shared'}
    # terraform variables
    project = 'workflows-dev'
    region = 'us-east1'
    zone = 'us-east1-b'
    composer_prefix = 'composer-instance-'

    # names of terraform outputs
    output_dags_bucket = 'dags_bucket'
    output_credentials = 'google_credentials'
    output_github_bucket_dev = 'github_bucket_dev'
    output_github_bucket_prod = 'github_bucket_prod'
    output_github_token = 'github_token'
    # TODO create github token and change to academic observatory
    github_repo = 'aroelo/academic-observatory'


class TerraformWorkspace:
    def __init__(self, name, token):
        self.name = name
        self.token = token
        self.workspace = TerraformConfig.prefix + name

        self.headers = {
            'Content-Type': 'application/vnd.api+json',
            'Authorization': f'Bearer {token}'
        }

    def is_workspace_locked(self):
        response = requests.get(
            f'{TerraformConfig.api_url}/organizations/{TerraformConfig.organisation}/workspaces/{self.workspace}',
            headers=self.headers)
        lock = json.loads(response.text)['data']['attributes']['locked']
        return lock

    def workspace_id(self):
        response = requests.get(
            f'{TerraformConfig.api_url}/organizations/{TerraformConfig.organisation}/workspaces/{self.workspace}',
            headers=self.headers)
        workspace_id = json.loads(response.text)['data']['id']
        return workspace_id

    def list_workspace_variables(self, workspace_id):
        response = requests.get(f'{TerraformConfig.api_url}/workspaces/{workspace_id}/vars', headers=self.headers)

        if not response:
            print(response.text)
            exit(os.EX_CONFIG)

        return json.loads(response.text)

    def update_workspace_variable(self, key, value, description=None, sensitive=None, category=None):
        workspace_id = self.workspace_id()
        vars = self.list_workspace_variables(workspace_id)['data']
        var_id = None
        for var in vars:
            attributes = var['attributes']
            var_key = attributes['key']
            if var_key == key:
                var_id = var["id"]
        if var_id is None:
            print(f"Variable '{key}' does not exist yet, create variable first.")
            return

        attributes = {"key": key, "value": value}
        if description:
            attributes["description"] = description
        if sensitive:
            if sensitive != "false" and sensitive != "true":
                print('Sensitive has to be either "true" or "false"')
                exit(os.EX_CONFIG)
            else:
                attributes["sensitive"] = sensitive
        if category:
            if category != 'env' and category != 'terraform':
                print('Category has to be either "env" or "terraform"')
                exit(os.EX_CONFIG)
            else:
                attributes["category"] = category

        data = {"data": {"type": "vars", "id": var_id, "attributes": attributes}}
        response = requests.patch(f'{TerraformConfig.api_url}/workspaces/{workspace_id}/vars/{var_id}',
                                  headers=self.headers, json=data)

        if not response:
            print(response.text)
            exit(os.EX_CONFIG)

        return json.loads(response.text)

    def add_workspace_variable(self, key, value, description, sensitive, category):
        workspace_id = self.workspace_id()
        vars = self.list_workspace_variables(workspace_id)['data']
        for var in vars:
            attributes = var['attributes']
            var_key = attributes['key']
            if var_key == key:
                print(f"Variable '{key}' already exists.")
                return

        attributes = {"key": key, "value": value, "description": description, "hcl": "false"}
        if sensitive != "false" and sensitive != "true":
            print('Sensitive has to be either "true" or "false"')
            exit(os.EX_CONFIG)
        else:
            attributes["sensitive"] = sensitive
        if category:
            if category != 'env' and category != 'terraform':
                print('Category has to be either "env" or "terraform"')
                exit(os.EX_CONFIG)
            else:
                attributes["category"] = category

        data = {"data": {"type": "vars", "attributes": attributes}}
        response = requests.post(f'{TerraformConfig.api_url}/workspaces/{workspace_id}/vars',
                                 headers=self.headers, json=data)

        if not response:
            print(response.text)
            exit(os.EX_CONFIG)

        return json.loads(response.text)


def terraform_setup_workspaces(docker_args, env, min_line_chars, verbose, github_token, google_application_credentials,
                               terraform_user_token):
    print(f'Terraform setting up workspaces'.ljust(min_line_chars))
    for workspace in TerraformConfig.workspaces:
        # # init workspace
        # info = f'Initialising workspace {workspace}..'
        # args = ['init', '-reconfigure', '-input=false']
        # terraform_run_cmd(args, info, docker_args, env, min_line_chars, verbose)

        # init/create new workspace
        # init fails if there are no workspaces created yet
        info = f'Init/create new workspace "{workspace}"...'
        cmd = f'terraform init -reconfigure -input=false || terraform workspace new {workspace}'
        bash_run_cmd(cmd, info, docker_args, env, min_line_chars, verbose)

        # select workspace
        # workspace new fails if the workspace doesn't exist yet
        info = f'Selecting workspace "{workspace}"...'
        cmd = f'terraform workspace new {workspace} || terraform workspace select {workspace}'
        bash_run_cmd(cmd, info, docker_args, env, min_line_chars, verbose)
        #
        # # select workspace
        # info = f'Selecting workspace "{workspace}"...'
        # args = ['workspace', 'select', workspace]
        # terraform_run_cmd(args, info, docker_args, env, min_line_chars, verbose)

        # add variables to workspace
        if workspace == 'dev':
            machine_type = 'n1-standard-1'
            composer_disk_size = '120'
        else:
            machine_type = 'e2-standard-16'
            composer_disk_size = '600'

        terraform_workspace = TerraformWorkspace(workspace, terraform_user_token)
        vars = workspace_variables(workspace, google_application_credentials, github_token, machine_type,
                                             composer_disk_size)
        for var in vars:
            var_attr = vars[var]
            terraform_workspace.add_workspace_variable(var, var_attr['value'], var_attr['description'],
                                                       var_attr['sensitive'], var_attr['category'])

        if workspace == 'shared':
            # apply workspace
            info = f'Applying workspace {workspace}..'
            args = ['apply', '-auto-approve', TerraformConfig.workspaces[workspace]]
            terraform_run_cmd(args, info, docker_args, env, min_line_chars, verbose)

            # clone github repo to bucket
            info = f'Cloning github repository to bucket'
            cmd = f'cd {TerraformConfig.workspaces[workspace]}; ' \
                  f'terraform output {TerraformConfig.output_credentials} > /temp.json; ' \
                  f'gcloud auth activate-service-account --key-file=/temp.json; ' \
                  f'github_token=$(terraform output {TerraformConfig.output_github_token}); ' \
                  f'mkdir tmpgithub; ' \
                  f'git -C tmpgithub clone --single-branch --branch develop https://$github_token@github.com/{TerraformConfig.github_repo}; ' \
                  f'github_bucket=$(terraform output {TerraformConfig.output_github_bucket_dev}); ' \
                  f'gsutil -m rsync -r -c -d tmpgithub $github_bucket; rm -rf tmpgithub;' \
                  f'mkdir tmpgithub; ' \
                  f'git -C tmpgithub clone --single-branch --branch master https://$github_token@github.com/{TerraformConfig.github_repo}; ' \
                  f'github_bucket=$(terraform output {TerraformConfig.output_github_bucket_prod}); ' \
                  f'gsutil -m rsync -r -c -d tmpgithub $github_bucket; rm -rf tmpgithub;'
            bash_run_cmd(cmd, info, docker_args, env, min_line_chars, verbose)

        print(f'Terraform set-up workspace {workspace}.\n'.ljust(min_line_chars))


def workspace_variables(workspace_name, google_credentials, github_token, machine_type, composer_disk_size):
    variables = {}
    variables['google-credentials_'] = {"value": google_credentials,
                                        "description": 'Determines whether composer environment should be '
                                                       'created/destroyed',
                                        "sensitive": 'true',
                                        "category": 'terraform'}
    variables['github-token_'] = {"value": github_token,
                                  "description": 'Personal access token used to run github action',
                                  "sensitive": 'true',
                                  "category": 'terraform'}
    variables['project_'] = {"value": TerraformConfig.project,
                             "description": 'Google project name',
                             "sensitive": 'false',
                             "category": 'terraform'}
    variables['region_'] = {"value": TerraformConfig.region,
                            "description": 'The location or Compute Engine region for the environment.',
                            "sensitive": 'false',
                            "category": 'terraform'}
    variables['zone_'] = {"value": TerraformConfig.zone,
                          "description": 'The Compute Engine zone in which to deploy the VMs running the '
                                         'Apache Airflow software, specified as the zone name or relative '
                                         'resource name',
                          "sensitive": 'false',
                          "category": 'terraform'}
    if workspace_name == 'dev' or workspace_name == 'prod':
        variables['machine-type'] = {"value": machine_type,
                                     "description": 'The Compute Engine machine type used for cluster instances, '
                                                    'specified as a name or relative resource name.',
                                     "sensitive": 'false',
                                     "category": 'terraform'}
        variables['composer-disk-size-gb'] = {"value": composer_disk_size,
                                              "description": 'The disk size in GB used for node VMs. Minimum size '
                                                             'is 20GB. If unspecified, defaults to 100GB.',
                                              "sensitive": 'false',
                                              "category": 'terraform'}
        variables['composer-prefix_'] = {"value": TerraformConfig.composer_prefix,
                                         "description": 'Prefix for composer environment name. Followed by '
                                                        'either dev or prod',
                                         "sensitive": 'false',
                                         "category": 'terraform'}
        variables['composer-status_'] = {"value": "off",
                                         "description": 'Determines whether composer environment should be '
                                                        'created/destroyed',
                                         "sensitive": "false", "category": "terraform"}
    return variables


def get_dags_bucket(docker_args, env, min_line_chars, verbose):
    info = 'Retrieving dags bucket name'
    # get bucket name
    args = ['output', TerraformConfig.output_dags_bucket]
    output, error = terraform_run_cmd(args, info, docker_args, env, min_line_chars, verbose)
    dags_bucket = output.strip('\r\n')

    if not dags_bucket.startswith('gs://'):
        print(f'Could not find bucket name in output'.ljust(min_line_chars))
    return dags_bucket


class OptionRequiredIf(click.Option):
    def full_process_value(self, ctx, value):
        value = super(OptionRequiredIf, self).full_process_value(ctx, value)
        if value is None and ctx.params['command'] == 'init_workspaces':
            msg = "Required if command is 'init_workspaces'"
            raise click.MissingParameter(ctx=ctx, param=self, message=msg)

        return value


@cli.command()
@click.argument('command',
                type=click.Choice(
                    ['build', 'setup_workspaces', TerraformConfig.prefix + 'dev', TerraformConfig.prefix + 'prod']))
@click.option('--plan/--apply', default=True)
@click.option('--on/--off')
@click.option('--google_credentials_file',
              type=click.Path(exists=False, file_okay=True, dir_okay=False),
              cls=OptionRequiredIf,
              help='')
@click.option('--github_token',
              type=click.STRING,
              cls=OptionRequiredIf,
              help='')
@click.option('--terraform_token_file',
              type=click.Path(exists=False, file_okay=True, dir_okay=False),
              help='',
              required=True)
@click.option('-v', '--verbose', count=True)
def terraform(command, google_credentials_file, github_token, terraform_token_file, verbose, on, plan):
    # The minimum number of characters per line
    min_line_chars = 80

    ######################
    # Check dependencies #
    ######################

    print("Academic Observatory: checking dependencies...".ljust(min_line_chars), end='\r')

    # Get all dependencies
    docker_path = get_docker_path()
    docker_compose_path = get_docker_compose_path()
    docker_running = is_docker_running()

    # Check that all dependencies are met
    all_deps = all([docker_path is not None,
                    docker_compose_path is not None,
                    docker_running])

    # Indentation variables
    indent1 = 2
    indent2 = 3

    if not all_deps:
        print("Academic Observatory: dependencies missing".ljust(min_line_chars))
    else:
        print("Academic Observatory: all dependencies found".ljust(min_line_chars))

    print(indent("Docker:", indent1))
    if docker_path is not None:
        print(indent(f"- path: {docker_path}", indent2))

        if docker_running:
            print(indent(f"- running", indent2))
        else:
            print(indent("- not running, please start", indent2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/get-docker/", indent2))

    print(indent("Docker Compose:", indent1))
    if docker_compose_path is not None:
        print(indent(f"- path: {docker_compose_path}", indent2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/compose/install/", indent2))

    if not all_deps:
        exit(os.EX_CONFIG)

    ##################
    # Run commands   #
    ##################

    file_path = pathlib.Path(academic_observatory.terraform.__file__).resolve()
    terraform_dir = pathlib.Path(*file_path.parts[:-1])
    # Make environment variables for running commands
    env = get_terraform_env(terraform_token_file, terraform_dir)
    print(docker_path)
    # Make docker-compose command
    docker_args = ['docker-compose']
    compose_files = ['docker-compose.terraform.secrets.yml', 'docker-compose.terraform.yml']
    docker_path = dockerfiles_path()
    for file_name in compose_files:
        path = os.path.join(docker_path, 'terraform', file_name)
        docker_args.append('-f')
        docker_args.append(path)

    with open(terraform_token_file, 'r') as f:
        terraform_user_token = f.read()

    # Start the appropriate process
    if command == 'build':
        terraform_build(docker_args, env, min_line_chars, verbose)

    elif command == 'setup_workspaces':
        with open(google_credentials_file, "r") as f:
            google_application_credentials = f.read().replace('\n', '')

        terraform_setup_workspaces(docker_args, env, min_line_chars, verbose, github_token,
                                   google_application_credentials, terraform_user_token)

    elif TerraformConfig.prefix in command:
        if on:
            composer_status = 'on'
        else:
            composer_status = 'off'
        if 'dev' in command:
            workspace = 'dev'
        else:
            workspace = 'prod'

        # select workspace
        info = f'Selecting workspace {workspace}..'
        args = ['workspace', 'select', workspace]
        terraform_run_cmd(args, info, docker_args, env, min_line_chars, verbose)

        # init workspace
        info = f'Initialising workspace {workspace}..'
        args = ['init', '-reconfigure', '-input=false']
        terraform_run_cmd(args, info, docker_args, env, min_line_chars, verbose)

        # update variable
        terraform_workspace = TerraformWorkspace(workspace, terraform_user_token)
        terraform_workspace.update_workspace_variable('composer-status_', composer_status)

        # plan
        if plan:
            info = f'Planning workspace {workspace}..'
            args = ['plan', '-input=false']
            terraform_run_cmd(args, info, docker_args, env, min_line_chars, verbose)
            if not on:
                print(f'WARNING: Bucket linked to composer environment will be deleted.'.ljust(min_line_chars))
        # apply
        else:
            state_lock = terraform_workspace.is_workspace_locked()
            if state_lock:
                print(f'Terraform state is locked, go to '
                      f'https://app.terraform.io/app/{TerraformConfig.organisation}/workspaces/{terraform_workspace.workspace}'
                      f' to see who locked it.'.ljust(min_line_chars))
                exit(os.EX_CONFIG)

            # copy dag 'sync_github.py'
            if on:
                info = f'Applying workspace {workspace}..'
                args = ['apply', '-auto-approve']
                terraform_run_cmd(args, info, docker_args, env, min_line_chars, verbose)

                # bash_cmd = f'terraform output {TerraformConfig.output_credentials} > /temp.json;' \
                #            f'gcloud auth activate-service-account --key-file=/temp.json;' \
                #            f'dags_bucket_path=$(gcloud composer environments describe {composer_environment_name} ' \
                #            f"--location {TerraformConfig.region} --format='get(config.dagGcsPrefix)')" \
                #            f'gsutil cp sync_github.py $dags_bucket_path'
                dags_bucket = get_dags_bucket(docker_args, env, min_line_chars, verbose)

                info = f'Copying dag to dags bucket of {workspace}...'
                bash_cmd = f'terraform output {TerraformConfig.output_credentials} > /temp.json; ' \
                           'gcloud auth activate-service-account --key-file=/temp.json; ' \
                           f'gsutil cp sync_github.py {dags_bucket}'
                bash_run_cmd(bash_cmd, info, docker_args, env, min_line_chars, verbose)

            # Delete storage bucket linked to composer environment
            else:
                dags_bucket = get_dags_bucket(docker_args, env, min_line_chars, verbose)
                composer_bucket = os.path.dirname(dags_bucket)

                info = f'Applying workspace {workspace}..'
                args = ['apply', '-auto-approve']
                terraform_run_cmd(args, info, docker_args, env, min_line_chars, verbose)

                info = f'Removing composer bucket of {workspace}...'
                bash_cmd = f'terraform output {TerraformConfig.output_credentials} > /temp.json; ' \
                           'gcloud auth activate-service-account --key-file=/temp.json; ' \
                           f'gsutil -m rm -r {composer_bucket}'
                bash_run_cmd(bash_cmd, info, docker_args, env, min_line_chars, verbose)


if __name__ == "__main__":
    cli()
