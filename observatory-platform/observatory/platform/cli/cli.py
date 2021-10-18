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

# Author: James Diprose, Aniek Roelofs, Tuan Chien

import os
import subprocess
import sys

import click
from observatory.platform.cli.click_utils import INDENT1, INDENT2, INDENT3, indent
from observatory.platform.cli.generate_command import GenerateCommand
from observatory.platform.cli.platform_command import PlatformCommand
from observatory.platform.cli.terraform_command import TerraformCommand
from observatory.platform.observatory_config import (
    generate_fernet_key,
    generate_secret_key,
)
from observatory.platform.platform_builder import DEBUG, HOST_UID
from observatory.platform.utils.config_utils import observatory_home
from observatory.platform.utils.config_utils import (
    terraform_credentials_path as default_terraform_credentials_path,
)
from observatory.platform.utils.proc_utils import stream_process

PLATFORM_NAME = "Observatory Platform"
TERRAFORM_NAME = "Observatory Terraform"

LOCAL_CONFIG_PATH = os.path.join(observatory_home(), "config.yaml")
TERRAFORM_CONFIG_PATH = os.path.join(observatory_home(), "config-terraform.yaml")


@click.group()
def cli():
    """The Observatory Platform command line tool.

    COMMAND: the commands to run include:\n
      - platform: start and stop the local Observatory Platform platform.\n
      - generate: generate a variety of outputs.\n
      - terraform: manage Terraform Cloud workspaces.\n
    """

    pass


@cli.command()
@click.argument("command", type=click.Choice(["start", "stop"]))
@click.option(
    "--config-path",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
    default=LOCAL_CONFIG_PATH,
    help="The path to the config.yaml configuration file.",
    show_default=True,
)
@click.option(
    "--host-uid",
    type=click.INT,
    default=HOST_UID,
    help="The user id of the host system. Used to set the user id in the Docker containers.",
    show_default=True,
)
@click.option("--debug", is_flag=True, default=DEBUG, help="Print debugging information.")
def platform(command: str, config_path: str, host_uid: int, debug):
    """Run the local Observatory Platform platform.\n

    COMMAND: the command to give the platform:\n
      - start: start the platform.\n
      - stop: stop the platform.\n
    """

    min_line_chars = 80
    print(f"{PLATFORM_NAME}: checking dependencies...".ljust(min_line_chars), end="\r")
    if os.path.isfile(config_path):
        # Make the platform command, which encapsulates functionality for running the observatory
        platform_cmd = PlatformCommand(config_path, host_uid=host_uid, debug=debug)

        # Check dependencies
        platform_check_dependencies(platform_cmd, min_line_chars=min_line_chars)

        # Start the appropriate process
        if command == "start":
            platform_start(platform_cmd)
        elif command == "stop":
            platform_stop(platform_cmd)

        exit(os.EX_OK)
    else:
        print(indent("config.yaml:", INDENT1))
        print(indent(f"- file not found, generating a default file on path: {config_path}", INDENT2))
        generate_cmd = GenerateCommand()
        generate_cmd.generate_local_config(config_path, editable=False, workflows=[], oapi=False)
        exit(os.EX_CONFIG)


def platform_check_dependencies(platform_cmd: PlatformCommand, min_line_chars: int = 80):
    """Check Platform dependencies.

    :param platform_cmd: the platform command instance.
    :param min_line_chars: the minimum number of lines when printing to the command line interface.
    :return: None.
    """

    if not platform_cmd.is_environment_valid:
        print(f"{PLATFORM_NAME}: dependencies missing".ljust(min_line_chars))
    else:
        print(f"{PLATFORM_NAME}: all dependencies found".ljust(min_line_chars))

    print(indent("Docker:", INDENT1))
    if platform_cmd.docker_exe_path is not None:
        print(indent(f"- path: {platform_cmd.docker_exe_path}", INDENT2))

        if platform_cmd.is_docker_running:
            print(indent(f"- running", INDENT2))
        else:
            print(indent("- not running, please start", INDENT2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/get-docker/", INDENT2))

    print(indent("Docker Compose:", INDENT1))
    if platform_cmd.docker_compose_path is not None:
        print(indent(f"- path: {platform_cmd.docker_compose_path}", INDENT2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/compose/install/", INDENT2))

    print(indent("config.yaml:", INDENT1))
    print(indent(f"- path: {platform_cmd.config_path}", INDENT2))
    if platform_cmd.config.is_valid:
        print(indent("- file valid", INDENT2))
    else:
        print(indent("- file invalid", INDENT2))
        for error in platform_cmd.config.errors:
            print(indent("- {}: {}".format(error.key, error.value), INDENT3))

    if not platform_cmd.is_environment_valid:
        exit(os.EX_CONFIG)

    print(indent("Host machine settings:", INDENT1))
    print(indent(f"- observatory home: {platform_cmd.config.observatory.observatory_home}", INDENT2))
    print(indent(f"- host-uid: {platform_cmd.host_uid}", INDENT2))


def platform_start(platform_cmd: PlatformCommand, min_line_chars: int = 80):
    """Check Platform dependencies.

    :param platform_cmd: the platform command instance.
    :param min_line_chars: the minimum number of lines when printing to the command line interface.
    :return: None.
    """

    print(f"{PLATFORM_NAME}: building...".ljust(min_line_chars), end="\r")
    response = platform_cmd.build()

    if response.return_code == 0:
        print(f"{PLATFORM_NAME}: built".ljust(min_line_chars))
    else:
        print(f"{PLATFORM_NAME}: build error".ljust(min_line_chars))
        exit(os.EX_CONFIG)

    # Start the built containers
    print(f"{PLATFORM_NAME}: starting...".ljust(min_line_chars), end="\r")
    response = platform_cmd.start()

    if response.return_code == 0:
        ui_started = platform_cmd.wait_for_airflow_ui(timeout=120)

        if ui_started:
            print(f"{PLATFORM_NAME}: started".ljust(min_line_chars))
            print(f"View the Apache Airflow UI at {platform_cmd.ui_url}")
        else:
            print(f"{PLATFORM_NAME}: error starting".ljust(min_line_chars))
            print(f"Could not find the Airflow UI at {platform_cmd.ui_url}")
    else:
        print("Error starting the Observatory Platform")
        exit(os.EX_CONFIG)


def platform_stop(platform_cmd: PlatformCommand, min_line_chars: int = 80):
    """Start the Observatory platform.

    :param platform_cmd: the platform command instance.
    :param min_line_chars: the minimum number of lines when printing to the command line interface.
    :return: None.
    """

    print(f"{PLATFORM_NAME}: stopping...".ljust(min_line_chars), end="\r")
    response = platform_cmd.stop()

    if platform_cmd.debug:
        print(response.output)

    if response.return_code == 0:
        print(f"{PLATFORM_NAME}: stopped".ljust(min_line_chars))
    else:
        print(f"{PLATFORM_NAME}: error stopping".ljust(min_line_chars))
        print(response.error)
        exit(os.EX_CONFIG)


@cli.group()
def generate():
    """The Observatory Platform generate command.

    COMMAND: the commands to run include:\n
      - secrets: generate secrets.\n
      - config: generate configuration files for the Observatory Platform.\n
      - project: generate a new project directory and required files.\n
      - workflow: generate all files for a new workflow.
    """

    pass


@generate.command()
@click.argument("project_path", type=click.Path(exists=False, file_okay=False, dir_okay=True))
@click.argument("package_name", type=str)
@click.argument("author_name", type=str)
def project(project_path: str, package_name: str, author_name: str):
    """Generate a new workflows project.

    \b
    PROJECT_PATH is the Python project path.
    PACKAGE_NAME is the Python package name.
    AUTHOR_NAME is the author name, used for readthedocs.

    \b
    For example, the command: observatory generate project /path/to/my-workflows-project my_workflows_project

    \b Will generate the following files and folders:

    \b
    └── my-workflows-project
        ├── docs
        │   ├── _build
        │   ├── _static
        │   ├── _templates
        │   ├── workflows
        │   ├── conf.py
        │   ├── generate_schema_csv.py
        │   ├── index.rst
        │   ├── make.bat
        │   ├── Makefile
        │   ├── requirements.txt
        │   └── test_generate_schema_csv.py
        ├── {package_name}
        │   ├── dags
        │   │   └── __init__.py
        │   ├── database
        │   │   ├── schema
        │   │   │   └── __init__.py
        │   │   └── __init__.py
        │   └── workflows
        │   │   ├── tests
        │   │   │   └── __init__.py
        │   │   └── __init__.py
        │   ├── __init__.py
        │   └── config.py
        ├── setup.cfg
        └── setup.py
    """
    cmd = GenerateCommand()
    cmd.generate_workflows_project(project_path, package_name, author_name)

    if click.confirm(
        f"Would you like to install the '{package_name}' package inside your new project? This is required for "
        f"the workflows to function."
    ):
        proc = subprocess.Popen(
            [sys.executable, "-m", "pip", "install", "-e", "."],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=project_path,
        )
        stream_process(proc, True)


@generate.command()
@click.argument(
    "workflow_type", type=click.Choice(["Workflow", "StreamTelescope", "SnapshotTelescope", "OrganisationTelescope"])
)
@click.argument("workflow_name", type=str)
@click.option(
    "-p",
    "--project-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    default=os.getcwd(),
    help="The path to the project directory.",
    show_default=True,
)
def workflow(workflow_type: str, workflow_name: str, project_path: str):
    """Generate all files for a new workflow.

    \b
    WORKFLOW_TYPE is the type of workflow.
    WORKFLOW_NAME is the the name of your new workflow.
    PROJECT_PATH is the Python project path, default is current directory.

    \b
    For example, the command: observatory generate workflow /path/to/my-workflows-project my_workflows_project Workflow MyWorkflow

    \b Will generate the files in bold inside your workflows project:

    \b
    └── my-workflows-project
        ├── docs
        │   ├── _build
        │   ├── _static
        │   ├── _templates
        │   ├── workflows
        │   │   └── \033[1mmy_workflow.md\033[0m
        │   ├── generate_schema_csv.py
        │   ├── index.rst
        │   ├── make.bat
        │   └── Makefile
        ├── my_workflows_project
        │   ├── dags
        │   │   ├── __init__.py
        │   │   └── \033[1mmy_workflow.py\033[0m
        │   ├── database
        │   │   ├── schema
        │   │   │   ├── __init__.py
        │   │   │   └── \033[1mmy_workflow_YYYY_MM_DD.json\033[0m
        │   │   └── __init__.py
        │   └── workflows
        │   │   ├── tests
        │   │   │   ├── __init__.py
        │   │   │   └── \033[1mtest_my_workflow.py\033[0m
        │   │   ├── __init__.py
        │   │   └── \033[1mmy_workflow.py\033[0m
        │   ├── __init__.py
        │   ├── config.py
        │   └── \033[1midentifiers.py\033[0m (OrganisationTelescope only)
        ├── setup.cfg
        └── setup.py
    """
    print(f"Given workflow type: {workflow_type}")
    print(f"Given workflow name: {workflow_name}")
    print(f"Given project path: {project_path}\n")

    # Check if egg-info dir is available
    egg_info_dir = [d for d in os.listdir(project_path) if d.endswith(".egg-info")]
    if not egg_info_dir:
        print(
            "Invalid workflows project, the given projects directory does not contain an installed python package.\n"
            "Either run this command from inside a valid workflows project or specify the path to a valid workflows "
            "project with the '--project-path' option.\n"
            "A new workflows project can be created using the 'observatory generate project' command."
        )
        exit(os.EX_CONFIG)
    if len(egg_info_dir) > 1:
        print(
            "Invalid workflows project, the given projects directory contains more than 1 installed python package.\n"
            "Either run this command from inside a valid workflows project or specify the path to a valid workflows "
            "project with the '--project-path' option.\n"
            "A new workflows project can be created using the 'observatory generate project' command."
        )
        exit(os.EX_CONFIG)

    # Get package name
    top_level_file = os.path.join(project_path, egg_info_dir[0], "top_level.txt")
    with open(top_level_file, "r") as f:
        package_name = f.readline().strip()
        print(f"Found package inside project: {package_name}")

    cmd = GenerateCommand()
    cmd.generate_workflow(
        project_path=project_path, package_name=package_name, workflow_type=workflow_type, workflow_class=workflow_name
    )


@generate.command()
@click.argument("command", type=click.Choice(["fernet-key", "secret-key"]))
def secrets(command: str):
    """Generate secrets for the Observatory Platform.\n

    COMMAND: the type of secret to generate:\n
      - fernet-key: generate a random Fernet Key.\n
      - secret-key: generate a random secret key.\n
    """

    if command == "fernet-key":
        print(generate_fernet_key())
    else:
        print(generate_secret_key())


@generate.command()
@click.argument("command", type=click.Choice(["local", "terraform"]))
@click.option(
    "--config-path",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
    default=None,
    help="The path to the config file to generate.",
    show_default=True,
)
@click.option("--interactive", flag_value=True, help="Configuration through an interactive Q&A mode")
@click.option(
    "--ao-wf",
    flag_value=True,
    help="Indicates that the academic-observatory-workflows was installed through the cli installer script",
)
@click.option(
    "--oaebu-wf",
    flag_value=True,
    help="Indicates that the oaebu-workflows was installed through the cli installer script",
)
@click.option(
    "--oapi", flag_value=True, help="Indicates that the observatory api was installed through the cli installer script"
)
@click.option("--editable", flag_value=True, help="Indicates the observatory platform is editable")
def config(command: str, config_path: str, interactive: bool, ao_wf: bool, oaebu_wf: bool, oapi: bool, editable: bool):
    """Generate config files for the Observatory Platform.\n

    COMMAND: the type of config file to generate:\n
      - local: generate a config file for running the Observatory Platform locally.\n
      - terraform: generate a config file for running the Observatory Platform with Terraform.\n

    :param interactive: whether to interactively ask for configuration options.
    :param ao_wf: Whether academic_observatory_workflows was installed using the installer script.
    :param oaebu_wf: Whether oaebu_workflows was installed using the installer script.
    :param oapi: Whether the Observatory API was installed using the installer script.
    :param editable: Whether the observatory platform is editable.
    """

    # Make the generate command, which encapsulates functionality for generating data
    cmd = GenerateCommand()

    if config_path is None:
        config_path = LOCAL_CONFIG_PATH if command == "local" else TERRAFORM_CONFIG_PATH

    config_name = "Observatory config" if command == "local" else "Terraform config"

    workflows = []
    if ao_wf:
        workflows.append("academic-observatory-workflows")
    if oaebu_wf:
        workflows.append("oaebu-workflows")

    cmd_func = None
    if command == "local" and not interactive:
        cmd_func = cmd.generate_local_config
    elif command == "terraform" and not interactive:
        cmd_func = cmd.generate_terraform_config
    elif command == "local" and interactive:
        cmd_func = cmd.generate_local_config_interactive
    else:
        cmd_func = cmd.generate_terraform_config_interactive

    if not os.path.exists(config_path) or click.confirm(
        f'The file "{config_path}" exists, do you want to overwrite it?'
    ):
        cmd_func(config_path, workflows=workflows, oapi=oapi, editable=editable)
    else:
        click.echo(f"Not generating {config_name}")


# increase content width for cleaner help output
@cli.command(context_settings=dict(max_content_width=120))
@click.argument(
    "command",
    type=click.Choice(["build-terraform", "build-image", "build-api-image", "create-workspace", "update-workspace"]),
)
# The path to the config-terraform.yaml configuration file.
@click.argument("config-path", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option(
    "--terraform-credentials-path",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
    default=default_terraform_credentials_path(),
    help="",
    show_default=True,
)
@click.option("--debug", is_flag=True, default=DEBUG, help="Print debugging information.")
def terraform(command, config_path, terraform_credentials_path, debug):
    """Commands to manage the deployment of the Observatory Platform with Terraform Cloud.\n

    COMMAND: the type of config file to generate:\n
      - create-workspace: create a Terraform Cloud workspace.\n
      - update-workspace: update a Terraform Cloud workspace.\n
      - build-image: build a Google Compute image for the Terraform deployment with Packer.\n
      - build-terraform: build the Terraform files.\n
    """

    # The minimum number of characters per line
    min_line_chars = 80

    terraform_cmd = TerraformCommand(config_path, terraform_credentials_path, debug=debug)
    generate_cmd = GenerateCommand()

    # Check dependencies
    terraform_check_dependencies(terraform_cmd, generate_cmd)

    # Run commands
    if command == "build-terraform":
        # Build image with packer
        terraform_cmd.build_terraform()
    elif command == "build-image":
        # Build image with packer
        terraform_cmd.build_image()
    elif command == "build-api-image":
        # Build docker image stored in google container registry
        terraform_cmd.build_google_container_image()
    else:
        # Create a new workspace
        if command == "create-workspace":
            terraform_cmd.create_workspace()

        # Update an existing workspace
        elif command == "update-workspace":
            terraform_cmd.update_workspace()


def terraform_check_dependencies(
    terraform_cmd: TerraformCommand, generate_cmd: GenerateCommand, min_line_chars: int = 80
):
    """Check Terraform dependencies.

    :param terraform_cmd: the Terraform command instance.
    :param generate_cmd: the generate command instance.
    :param min_line_chars: the minimum number of lines when printing to the command line interface.
    :return: None.
    """

    print(f"{TERRAFORM_NAME}: checking dependencies...".ljust(min_line_chars), end="\r")

    if not terraform_cmd.is_environment_valid:
        print(f"{TERRAFORM_NAME}: dependencies missing".ljust(min_line_chars))
    else:
        print(f"{TERRAFORM_NAME}: all dependencies found".ljust(min_line_chars))

    print(indent("Config:", INDENT1))
    if terraform_cmd.config_exists:
        print(indent(f"- path: {terraform_cmd.config_path}", INDENT2))
        if terraform_cmd.config.is_valid:
            print(indent("- file valid", INDENT2))
        else:
            print(indent("- file invalid", INDENT2))
            for key, value in terraform_cmd.config.validator.errors.items():
                print(indent(f"- {key}: {value}", INDENT3))
    else:
        print(indent("- file not found, generating a default file", INDENT2))
        generate_cmd.generate_terraform_config(terraform_cmd.config_path, editable=False, workflows=[], oapi=False)

    print(indent("Terraform credentials file:", INDENT1))
    if terraform_cmd.terraform_credentials_exists:
        print(indent(f"- path: {terraform_cmd.terraform_credentials_path}", INDENT2))
    else:
        print(indent("- file not found, create one by running 'terraform login'", INDENT2))

    print(indent("Packer", INDENT1))
    if terraform_cmd.terraform_builder.packer_exe_path is not None:
        print(indent(f"- path: {terraform_cmd.terraform_builder.packer_exe_path}", INDENT2))
    else:
        print(indent("- not installed, please install https://www.packer.io/docs/install", INDENT2))

    print(indent("Google Cloud SDK", INDENT1))
    if terraform_cmd.terraform_builder.gcloud_exe_path is not None:
        print(indent(f"- path: {terraform_cmd.terraform_builder.gcloud_exe_path}", INDENT2))
    else:
        print(indent("- not installed, please install https://cloud.google.com/sdk/docs/install", INDENT2))

    if not terraform_cmd.is_environment_valid:
        exit(os.EX_CONFIG)


if __name__ == "__main__":
    cli()
