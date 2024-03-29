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

import json
import os
from typing import ClassVar

import click

from observatory.platform.cli.cli_utils import INDENT1, INDENT2, INDENT3, indent
from observatory.platform.cli.generate_command import GenerateCommand
from observatory.platform.cli.platform_command import PlatformCommand
from observatory.platform.cli.terraform_command import TerraformCommand
from observatory.platform.config import observatory_home
from observatory.platform.config import (
    terraform_credentials_path as default_terraform_credentials_path,
)
from observatory.platform.docker.platform_runner import DEBUG, HOST_UID
from observatory.platform.observatory_config import Config, TerraformConfig, ObservatoryConfig
from observatory.platform.observatory_config import (
    generate_fernet_key,
    generate_secret_key,
)

PLATFORM_NAME = "Observatory Platform"
TERRAFORM_NAME = "Observatory Terraform"

LOCAL_CONFIG_PATH = os.path.join(observatory_home(), "config.yaml")
TERRAFORM_CONFIG_PATH = os.path.join(observatory_home(), "config-terraform.yaml")


def load_config(cls: ClassVar, config_path: str) -> Config:
    """Load a config file.
    :param cls: the config file class.
    :param config_path: the path to the config file.
    :return: the config file or exit with an OS.EX_CONFIG error or FileExistsError.
    """

    print(indent("config.yaml:", INDENT1))
    config_exists = os.path.exists(config_path)

    if not config_exists:
        msg = indent(f"- file not found, generating a default file on path: {config_path}", INDENT2)
        generate_cmd = GenerateCommand()
        if cls == ObservatoryConfig:
            print(msg)
            generate_cmd.generate_local_config(config_path, editable=False, workflows=[], oapi=False)
        elif cls == TerraformConfig:
            print(msg)
            generate_cmd.generate_terraform_config(config_path, editable=False, workflows=[], oapi=False)
        else:
            print(indent(f"- file not found, exiting: {config_path}", INDENT2))
        exit(os.EX_CONFIG)
    else:
        cfg = cls.load(config_path)
        if cfg.is_valid:
            print(indent("- file valid", INDENT2))
        else:
            print(indent("- file invalid", INDENT2))
            for error in cfg.errors:
                print(indent(f"- {error.key}: {error.value}", INDENT3))
            exit(os.EX_CONFIG)

        return cfg


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
    cfg = load_config(ObservatoryConfig, config_path)

    # Make the platform command, which encapsulates functionality for running the observatory
    platform_cmd = PlatformCommand(cfg, host_uid=host_uid, debug=debug)

    # Check dependencies
    platform_check_dependencies(platform_cmd, min_line_chars=min_line_chars)

    # Start the appropriate process
    if command == "start":
        platform_start(platform_cmd)
    elif command == "stop":
        platform_stop(platform_cmd)

    exit(os.EX_OK)


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

    print(indent("Docker Compose V2:", INDENT1))
    if platform_cmd.docker_compose:
        print(indent(f"- installed", INDENT2))
    else:
        print(indent("- not installed, please install https://docs.docker.com/compose/install/", INDENT2))

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
        print(response.output)
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
        print(response.output)
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
    type=click.Choice(["build-terraform", "build-image", "create-workspace", "update-workspace"]),
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

    cfg = load_config(TerraformConfig, config_path)
    terraform_cmd = TerraformCommand(cfg, terraform_credentials_path, debug=debug)
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
    else:
        # Create a new workspace
        if command == "create-workspace":
            terraform_cmd.create_workspace()

        # Update an existing workspace
        elif command == "update-workspace":
            terraform_cmd.update_workspace()


@cli.command("sort-schema")
@click.argument("input-file", type=click.Path(exists=True, file_okay=True, dir_okay=False))
def sort_schema_cmd(input_file):
    def sort_schema(schema):
        sorted_schema = sorted(schema, key=lambda x: x["name"])

        for field in sorted_schema:
            if field["type"] == "RECORD" and "fields" in field:
                field["fields"] = sort_schema(field["fields"])

        return sorted_schema

    # Load the JSON schema from a string
    with open(input_file, mode="r") as f:
        data = json.load(f)

    # Sort the schema
    sorted_json_schema = sort_schema(data)

    # Save the schema
    with open(input_file, mode="w") as f:
        json.dump(sorted_json_schema, f, indent=2)


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
