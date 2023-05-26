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

from pathlib import Path
from typing import Any, List, Optional

import click

from observatory.platform.config import module_file_path
from observatory.platform.observatory_config import (
    BackendType,
    CloudSqlDatabase,
    Environment,
    GoogleCloud,
    Observatory,
    ObservatoryConfig,
    Terraform,
    TerraformConfig,
    VirtualMachine,
    WorkflowsProject,
    is_fernet_key,
    is_secret_key,
    Config,
)

# Terminal formatting
BOLD = "\033[1m"
END = "\033[0m"


class DefaultWorkflowsProject:
    """Get Workflows Project configuration for when it was selected via the installer script (editable type)."""

    @staticmethod
    def academic_observatory_workflows():
        package_name = "academic-observatory-workflows"
        package = module_file_path("academic_observatory_workflows.dags", nav_back_steps=-3)
        package_type = "editable"
        dags_module = "academic_observatory_workflows.dags"

        return WorkflowsProject(
            package_name=package_name, package=package, package_type=package_type, dags_module=dags_module
        )

    @staticmethod
    def oaebu_workflows():
        package_name = "oaebu-workflows"
        package = module_file_path("oaebu_workflows.dags", nav_back_steps=-3)
        package_type = "editable"
        dags_module = "oaebu_workflows.dags"

        return WorkflowsProject(
            package_name=package_name, package=package, package_type=package_type, dags_module=dags_module
        )


class GenerateCommand:
    def generate_local_config(self, config_path: str, *, editable: bool, workflows: List[str], oapi: bool):
        """Command line user interface for generating an Observatory Config config.yaml.

        :param config_path: the path where the config file should be saved.
        :param editable: Whether the observatory platform is editable.
        :param workflows: List of installer script installed workflows.
        :return: None
        """

        file_type = "Observatory Config"
        click.echo(f"Generating {file_type}...")

        workflows = InteractiveConfigBuilder.get_installed_workflows(workflows)
        config = ObservatoryConfig(workflows_projects=workflows)

        if editable:
            InteractiveConfigBuilder.set_editable_observatory_platform(config.observatory)

            if oapi:
                InteractiveConfigBuilder.set_editable_observatory_api(config.observatory)

        config.save(path=config_path)

        click.echo(f'{file_type} saved to: "{config_path}"')

    def generate_terraform_config(self, config_path: str, *, editable: bool, workflows: List[str], oapi: bool):
        """Command line user interface for generating a Terraform Config config-terraform.yaml.

        :param config_path: the path where the config file should be saved.
        :param editable: Whether the observatory platform is editable.
        :param workflows: List of installer script installed workflows.
        :return: None
        """

        file_type = "Terraform Config"
        click.echo(f"Generating {file_type}...")
        workflows = InteractiveConfigBuilder.get_installed_workflows(workflows)
        config = TerraformConfig(workflows_projects=workflows)

        if editable:
            InteractiveConfigBuilder.set_editable_observatory_platform(config.observatory)

            if oapi:
                InteractiveConfigBuilder.set_editable_observatory_api(config.observatory)

        config.save(path=config_path)

        click.echo(f'{file_type} saved to: "{config_path}"')

    def generate_local_config_interactive(self, config_path: str, *, workflows: List[str], oapi: bool, editable: bool):
        """Construct an Observatory local config file through user assisted configuration.

        :param config_path: Configuration file path.
        :param workflows: List of installer script installed workflows projects.
        :param oapi: Whether installer script installed the Observatory API.
        :param editable: Whether the observatory platform is editable.
        """

        file_type = "Observatory Config"
        click.echo(f"Generating {file_type}...")

        config = InteractiveConfigBuilder.build(
            backend_type=BackendType.local, workflows=workflows, oapi=oapi, editable=editable
        )

        if editable:
            InteractiveConfigBuilder.set_editable_observatory_platform(config.observatory)

        config.save(config_path)
        click.echo(f'{file_type} saved to: "{config_path}"')

    def generate_terraform_config_interactive(
        self, config_path: str, *, workflows: List[str], oapi: bool, editable: bool
    ):
        """Construct an Observatory Terraform config file through user assisted configuration.

        :param config_path: Configuration file path.
        :param workflows: List of workflows projects installed by installer script.
        :param oapi: Whether installer script installed the Observatory API.
        :param editable: Whether the observatory platform is editable.
        """

        file_type = "Terraform Config"
        click.echo(f"Generating {file_type}...")

        config = InteractiveConfigBuilder.build(
            backend_type=BackendType.terraform, workflows=workflows, oapi=oapi, editable=editable
        )

        if editable:
            InteractiveConfigBuilder.set_editable_observatory_platform(config.observatory)

        config.save(config_path)
        click.echo(f'{file_type} saved to: "{config_path}"')


class FernetKeyType(click.ParamType):
    """Fernet key type for click prompt.  Will validate the input against the is_fernet_key method."""

    name = "FernetKeyType"

    def convert(
        self, value: Any, param: Optional[click.core.Parameter] = None, ctx: Optional[click.core.Context] = None
    ) -> Any:
        valid, msg = is_fernet_key(value)
        if not valid:
            self.fail(f"Input is not a valid Fernet key. Reason: {msg}", param=param, ctx=ctx)

        return value


class FlaskSecretKeyType(click.ParamType):
    """Secret key type for click prompt.  Will validate the input against the is_secret_key method."""

    name = "SecretKeyType"

    def convert(
        self, value: Any, param: Optional[click.core.Parameter] = None, ctx: Optional[click.core.Context] = None
    ) -> Any:
        valid, msg = is_secret_key(value)
        if not valid:
            self.fail(f"Input is not a valid secret key. Reason: {msg}", param=param, ctx=ctx)

        return value


class InteractiveConfigBuilder:
    """Helper class for configuring the ObservatoryConfig class parameters through interactive user input."""

    @staticmethod
    def set_editable_observatory_platform(observatory: Observatory):
        """Set observatory package settings to editable.

        :param observatory: Observatory object to change.
        """

        observatory.package = module_file_path("observatory.platform", nav_back_steps=-3)
        observatory.package_type = "editable"

    @staticmethod
    def set_editable_observatory_api(observatory: Observatory):
        """Set observatory api package settings to editable.

        :param observatory: Observatory object to change.
        """

        observatory.api_package = module_file_path("observatory.api", nav_back_steps=-3)
        observatory.api_package_type = "editable"

    @staticmethod
    def get_installed_workflows(workflows: List[str]) -> List[WorkflowsProject]:
        """Add the workflows projects installed by the installer script.

        :param workflows: List of installed workflows (via installer script).
        :return: List of WorkflowsProjects installed by the installer.
        """

        workflows_projects = []
        if "academic-observatory-workflows" in workflows:
            workflows_projects.append(DefaultWorkflowsProject.academic_observatory_workflows())

        if "oaebu-workflows" in workflows:
            workflows_projects.append(DefaultWorkflowsProject.oaebu_workflows())

        return workflows_projects

    @staticmethod
    def build(*, backend_type: BackendType, workflows: List[str], oapi: bool, editable: bool) -> Config:
        """Build the correct observatory configuration object through user assisted parameters.

        :param backend_type: The type of Observatory backend being configured.
        :param workflows: List of workflows installed by installer script.
        :param oapi: Whether installer script installed the Observatory API.
        :param editable: Whether the observatory platform is editable.
        :return: An observatory configuration object.
        """

        workflows_projects = InteractiveConfigBuilder.get_installed_workflows(workflows)

        if backend_type == BackendType.local:
            config = ObservatoryConfig(workflows_projects=workflows_projects)
        else:
            config = TerraformConfig(workflows_projects=workflows_projects)

        # Common sections for all backends
        InteractiveConfigBuilder.config_backend(config=config, backend_type=backend_type)
        InteractiveConfigBuilder.config_observatory(config=config, oapi=oapi, editable=editable)
        InteractiveConfigBuilder.config_terraform(config)
        InteractiveConfigBuilder.config_google_cloud(config)
        InteractiveConfigBuilder.config_workflows_projects(config)

        # Extra sections for Terraform
        if backend_type == BackendType.terraform:
            InteractiveConfigBuilder.config_cloud_sql_database(config)
            InteractiveConfigBuilder.config_airflow_main_vm(config)
            InteractiveConfigBuilder.config_airflow_worker_vm(config)

        return config

    @staticmethod
    def config_backend(*, config: Config, backend_type: BackendType):
        """Configure the backend section.

        :param config: Configuration object to edit.
        :param backend_type: The backend type being used.
        """

        click.echo("Configuring backend settings")
        config.backend.type = backend_type

        text = "What kind of environment is this?"
        default = Environment.develop.name
        choices = click.Choice(
            choices=[Environment.develop.name, Environment.staging.name, Environment.production.name],
            case_sensitive=False,
        )

        config.backend.environment = Environment[
            click.prompt(text=text, type=choices, default=default, show_default=True, show_choices=True)
        ]

    @staticmethod
    def config_observatory(*, config: Config, oapi: bool, editable: bool):
        """Configure the observatory section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring Observatory settings")

        if editable:
            InteractiveConfigBuilder.set_editable_observatory_platform(config.observatory)
        # else:
        #     # Fill in if used installer script
        #     text = "What type of observatory platform installation did you perform? A git clone is an editable type, and a pip install is a pypi type."
        #     choices = click.Choice(choices=["editable", "sdist", "pypi"], case_sensitive=False)
        #     default = "pypi"
        #     package_type = click.prompt(text=text, type=choices, default=default, show_default=True, show_choices=True)
        #     config.observatory.package_type = package_type

        text = "Enter an Airflow Fernet key (leave blank to autogenerate)"
        default = ""
        fernet_key = click.prompt(text=text, type=FernetKeyType(), default=default)

        if fernet_key != "":
            config.observatory.airflow_fernet_key = fernet_key

        text = "Enter an Airflow secret key (leave blank to autogenerate)"
        default = ""
        secret_key = click.prompt(text=text, type=FlaskSecretKeyType(), default=default)

        if secret_key != "":
            config.observatory.airflow_secret_key = secret_key

        text = "Enter an email address to use for logging into the Airflow web interface"
        default = config.observatory.airflow_ui_user_email
        user_email = click.prompt(text=text, type=str, default=default, show_default=True)
        config.observatory.airflow_ui_user_email = user_email

        text = f"Password for logging in with {user_email}"
        default = config.observatory.airflow_ui_user_password
        user_pass = click.prompt(text=text, type=str, default=default, show_default=True)
        config.observatory.airflow_ui_user_password = user_pass

        text = "Enter observatory config directory. If it does not exist, it will be created."
        default = config.observatory.observatory_home
        observatory_home = click.prompt(
            text=text, type=click.Path(exists=False, readable=True), default=default, show_default=True
        )
        config.observatory.observatory_home = observatory_home
        Path(observatory_home).mkdir(exist_ok=True, parents=True)

        text = "Enter postgres password"
        default = config.observatory.postgres_password
        postgres_password = click.prompt(text=text, type=str, default=default, show_default=True)
        config.observatory.postgres_password = postgres_password

        text = "Redis port"
        default = config.observatory.redis_port
        redis_port = click.prompt(text=text, type=int, default=default, show_default=True)
        config.observatory.redis_port = redis_port

        text = "Flower UI port"
        default = config.observatory.flower_ui_port
        flower_ui_port = click.prompt(text=text, type=int, default=default, show_default=True)
        config.observatory.flower_ui_port = flower_ui_port

        text = "Airflow UI port"
        default = config.observatory.airflow_ui_port
        airflow_ui_port = click.prompt(text=text, type=int, default=default, show_default=True)
        config.observatory.airflow_ui_port = airflow_ui_port

        text = "API port"
        default = config.observatory.api_port
        api_port = click.prompt(text=text, type=int, default=default, show_default=True)
        config.observatory.api_port = api_port

        text = "Docker network name"
        default = config.observatory.docker_network_name
        docker_network_name = click.prompt(text=text, type=str, default=default, show_default=True)
        config.observatory.docker_network_name = docker_network_name

        text = "Is the docker network external?"
        default = config.observatory.docker_network_is_external
        docker_network_is_external = click.prompt(text=text, type=bool, default=default, show_default=True)
        config.observatory.docker_network_is_external = docker_network_is_external

        text = "Docker compose project name"
        default = config.observatory.docker_compose_project_name
        docker_compose_project_name = click.prompt(text=text, type=str, default=default, show_default=True)
        config.observatory.docker_compose_project_name = docker_compose_project_name

        text = "Do you wish to enable ElasticSearch and Kibana?"
        choices = click.Choice(choices=["y", "n"], case_sensitive=False)
        default = "y"
        enable_elk = click.prompt(text=text, default=default, type=choices, show_default=True, show_choices=True)

        config.observatory.enable_elk = True if enable_elk == "y" else False

        # If installed by installer script, we can fill in details
        if oapi and editable:
            InteractiveConfigBuilder.set_editable_observatory_api(config.observatory)
        elif not oapi:
            text = "Observatory API package name"
            default = config.observatory.api_package
            api_package = click.prompt(text=text, type=str, default=default, show_default=True)
            config.observatory.api_package = api_package

            text = "Observatory API package type"
            default = config.observatory.api_package_type
            api_package_type = click.prompt(text=text, type=str, default=default, show_default=True)
            config.observatory.api_package_type = api_package_type

    @staticmethod
    def config_google_cloud(config: Config):
        """Configure the Google Cloud section.

        :param config: Configuration object to edit.
        """

        zone = None
        region = None
        buckets = list()

        if not config.schema["google_cloud"]["required"]:
            text = "Do you want to configure Google Cloud settings?"
            proceed = click.confirm(text=text, default=False, abort=False, show_default=True)
            if not proceed:
                return

        text = "Google Cloud Project ID"
        project_id = click.prompt(text=text, type=str)

        text = "Path to Google Service Account key file (json)"
        credentials = click.prompt(text=text, type=click.Path(exists=True, readable=True))

        text = "Data location"
        default = "us"
        data_location = click.prompt(text=text, type=str, default=default, show_default=True)

        if config.backend.type == BackendType.terraform:
            text = "Region"
            default = "us-west1"
            region = click.prompt(text=text, type=str, default=default, show_default=True)

            text = "Zone"
            default = "us-west1-a"
            zone = click.prompt(text=text, type=str, default=default, show_default=True)

        config.google_cloud = GoogleCloud(
            project_id=project_id,
            credentials=credentials,
            region=region,
            zone=zone,
            data_location=data_location,
        )

    @staticmethod
    def config_terraform(config: Config):
        """Configure the Terraform section.

        :param config: Configuration object to edit.
        """

        if not config.schema["terraform"]["required"]:
            text = "Do you want to configure Terraform settings?"
            proceed = click.confirm(text=text, default=False, abort=False, show_default=True)
            if not proceed:
                return

        if config.backend.type == BackendType.local:
            suffix = " (leave blank to disable)"
            default = ""
        else:
            suffix = ""
            default = None

        text = f"Terraform organization name{suffix}"
        organization = click.prompt(text=text, type=str, default=default)

        if organization == "":
            return

        config.terraform = Terraform(organization=organization)

    @staticmethod
    def config_workflows_projects(config: Config):
        """Configure the DAGs projects section.

        :param config: Configuration object to edit.
        """

        click.echo(
            "Configuring workflows projects. If you opted to install some workflows projects through the installer script then they will be automatically added to the config file for you. If not, e.g., if you installed via pip, you will need to add those projects manually now (or later)."
        )

        text = "Do you want to add workflows projects?"
        add_workflows_projects = click.confirm(text=text, default=False, abort=False, show_default=True)

        if not add_workflows_projects:
            return

        projects = list()
        while True:
            text = "Workflows package name"
            package_name = click.prompt(text=text, type=str)

            text = "Workflows package, either a local path to a Python source (editable), sdist, or PyPI package name and version"
            package = click.prompt(text=text, type=click.Path(exists=True, readable=True))

            text = "Package type"
            choices = click.Choice(choices=["editable", "sdist", "pypi"], case_sensitive=False)
            default = "editable"
            package_type = click.prompt(text=text, default=default, type=choices, show_default=True, show_choices=True)

            text = "Python import path to the module that contains the Apache Airflow DAGs to load"
            dags_module = click.prompt(text=text, type=str)

            projects.append(
                WorkflowsProject(
                    package_name=package_name,
                    package=package,
                    package_type=package_type,
                    dags_module=dags_module,
                )
            )

            text = "Do you wish to add another DAGs project?"
            add_another = click.confirm(text=text, default=False, abort=False, show_default=True)

            if not add_another:
                break

        config.workflows_projects.extend(projects)

    @staticmethod
    def config_cloud_sql_database(config: TerraformConfig):
        """Configure the cloud SQL database section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring the Google Cloud SQL Database")

        text = "Google CloudSQL db tier"
        default = "db-custom-2-7680"
        tier = click.prompt(text=text, type=str, default=default, show_default=True)

        text = "Google CloudSQL backup start time"
        default = "23:00"
        backup_start_time = click.prompt(text=text, type=str, default=default, show_default=True)

        config.cloud_sql_database = CloudSqlDatabase(
            tier=tier,
            backup_start_time=backup_start_time,
        )

    @staticmethod
    def config_airflow_main_vm(config: TerraformConfig):
        """Configure the Airflow main virtual machine section.

        :param config: Configuration object to edit.
        """

        click.echo(BOLD + "Configuring settings for the main VM that runs the Airflow scheduler and webserver" + END)

        text = "Machine type"
        default = "n2-standard-2"
        machine_type = click.prompt(text=text, type=str, default=default, show_default=True)

        text = "Disk size (GB)"
        default = 50
        disk_size = click.prompt(text=text, type=int, default=default, show_default=True)

        text = "Disk type"
        schema = config.schema["airflow_main_vm"]["schema"]
        default = "pd-ssd"
        choices = click.Choice(choices=schema["disk_type"]["allowed"], case_sensitive=False)
        disk_type = click.prompt(text=text, type=choices, show_choices=True, default=default, show_default=True)

        text = "Create VM? If yes, and you run Terraform apply, the vm will be created. Otherwise if false, and you run Terraform apply, the vm will be destroyed."
        create = click.confirm(text=text, default=True, abort=False, show_default=True)

        config.airflow_main_vm = VirtualMachine(
            machine_type=machine_type,
            disk_size=disk_size,
            disk_type=disk_type,
            create=create,
        )

    @staticmethod
    def config_airflow_worker_vm(config: TerraformConfig):
        """Configure the Airflow worker virtual machine section.

        :param config: Configuration object to edit.
        """

        click.echo(BOLD + "Configuring settings for the worker VM" + END)

        text = "Machine type"
        default = "n1-standard-8"
        machine_type = click.prompt(text=text, type=str, default=default, show_default=True)

        text = "Disk size (GB)"
        default = 3000
        disk_size = click.prompt(text=text, type=int, default=default, show_default=True)

        text = "Disk type"
        schema = config.schema["airflow_worker_vm"]["schema"]
        default = "pd-standard"
        choices = click.Choice(choices=schema["disk_type"]["allowed"], case_sensitive=False)
        disk_type = click.prompt(text=text, type=choices, show_choices=True, default=default, show_default=True)

        text = "Create VM? If yes, and you run Terraform apply, the vm will be created. Otherwise if false, and you run Terraform apply, the vm will be destroyed."
        create = click.confirm(text=text, default=False, abort=False, show_default=True)

        config.airflow_worker_vm = VirtualMachine(
            machine_type=machine_type,
            disk_size=disk_size,
            disk_type=disk_type,
            create=create,
        )
