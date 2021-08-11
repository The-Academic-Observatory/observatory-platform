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
from typing import List, Tuple, Union

import click
import observatory.dags.dags
import observatory.dags.telescopes
import observatory.templates
from cryptography.fernet import Fernet
from observatory.platform.observatory_config import (
    AirflowConnection,
    AirflowVariable,
    Api,
    BackendType,
    CloudSqlDatabase,
    CloudStorageBucket,
    DagsProject,
    ElasticSearch,
    Environment,
    GoogleCloud,
    ObservatoryConfig,
    Terraform,
    TerraformConfig,
    VirtualMachine,
    generate_secret_key,
)
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.jinja2_utils import render_template


class TelescopeTypes:
    """
    Telescope types that we can generate from a template.
    """

    telescope = "Telescope"
    stream_telescope = "StreamTelescope"
    snapshot_telescope = "SnapshotTelescope"


class GenerateCommand:
    def generate_fernet_key(self) -> str:
        """Generate a Fernet key.

        :return: the Fernet key.
        """

        return Fernet.generate_key()

    @staticmethod
    def observatory_dagsproject() -> List[DagsProject]:
        package_name = "observatory-dags"
        path = module_file_path("observatory.dags", nav_back_steps=-3)
        dags_module = "observatory.dags.dags"

        return [
            DagsProject(
                package_name=package_name,
                path=path,
                dags_module=dags_module,
            )
        ]

    def generate_local_config(self, *, config_path: str, install_odags: bool):
        """Command line user interface for generating an Observatory Config config.yaml.

        :param config_path: the path where the config file should be saved.
        :param install_odags: Whether the user selected to install observatory-dags.
        :return: None
        """

        file_type = "Observatory Config"
        click.echo(f"Generating {file_type}...")

        dags_projects = GenerateCommand.observatory_dagsproject() if install_odags else None
        config = ObservatoryConfig(dags_projects=dags_projects)
        config.save(path=config_path)

        click.echo(f'{file_type} saved to: "{config_path}"')

    def generate_terraform_config(self, *, config_path: str, install_odags: bool):
        """Command line user interface for generating a Terraform Config config-terraform.yaml.

        :param config_path: the path where the config file should be saved.
        :param install_odags: Whether the user selected to install observatory-dags.
        :return: None
        """

        file_type = "Terraform Config"
        click.echo(f"Generating {file_type}...")

        dags_projects = GenerateCommand.observatory_dagsproject() if install_odags else None
        config = TerraformConfig(dags_projects=dags_projects)
        config.save(path=config_path)

        click.echo(f'{file_type} saved to: "{config_path}"')
        click.echo(
            "Please customise the parameters with '<--' in the config file. "
            "Parameters commented out with '#' are optional."
        )

    def get_telescope_template_path_(self, telescope_type: str) -> Tuple[str, str]:
        """
        Get the correct template files to use.

        :param telescope_type: Name of the telescope type.
        :return: The telescope template path, and the dag template path.
        """

        templates_dir = observatory.templates.__path__._path[0]

        dag_file = "telescope_dag.py.jinja2"
        test_file = "test.py.jinja2"
        doc_file = "doc.md.jinja2"

        if telescope_type == TelescopeTypes.telescope:
            telescope_file = "telescope.py.jinja2"
        elif telescope_type == TelescopeTypes.stream_telescope:
            telescope_file = "streamtelescope.py.jinja2"
        elif telescope_type == TelescopeTypes.snapshot_telescope:
            telescope_file = "snapshottelescope.py.jinja2"
        else:
            raise Exception(f"Unsupported telescope type: {telescope_type}")

        telescope_path = os.path.join(templates_dir, telescope_file)
        dag_path = os.path.join(templates_dir, dag_file)
        test_path = os.path.join(templates_dir, test_file)
        doc_path = os.path.join(templates_dir, doc_file)
        return telescope_path, dag_path, test_path, doc_path

    def generate_new_telescope(self, telescope_type: str, telescope_name: str):
        """
        Make a new telescope template.

        :param telescope_type: Type of telescope to generate.
        :param telescope_name: Class name of the new telescope.
        """

        telescope_path, dag_path, test_path, doc_path = self.get_telescope_template_path_(telescope_type)
        telescope_module = telescope_name.lower()
        telescope_file = f"{telescope_module}.py"

        # Render dag
        dag = render_template(dag_path, telescope_module=telescope_module, telescope_name=telescope_name)

        # Render telescope
        telescope = render_template(telescope_path, telescope_name=telescope_name)

        # Render test
        test = render_template(test_path, telescope_name=telescope_name)

        # Render documentation
        doc = render_template(doc_path, telescope_name=telescope_name)

        # Save templates
        dag_dst_dir = observatory.dags.dags.__path__[0]
        dag_dst_file = os.path.join(dag_dst_dir, telescope_file)

        telescope_dst_dir = observatory.dags.telescopes.__path__[0]
        telescope_dst_file = os.path.join(telescope_dst_dir, telescope_file)

        test_dst_dir = "tests/observatory/dags/telescopes"
        test_dst_file = os.path.join(test_dst_dir, f"test_{telescope_module}.py")

        doc_dst_dir = "docs"
        doc_dst_file = os.path.join(doc_dst_dir, "telescopes", f"{telescope_module}.md")

        doc_index_file = os.path.join(doc_dst_dir, "telescopes", "index.rst")

        print(f"Created a new dag file: {dag_dst_file}")
        print(f"Created a new telescope file: {telescope_dst_file}")
        print(f"Created a new telescope documentation file: {doc_dst_file}")
        print(f"Created a new telescope test file: {test_dst_file}")

        # Write out dag template
        with open(dag_dst_file, "w") as f:
            f.write(dag)

        # Write out telescope template
        with open(telescope_dst_file, "w") as f:
            f.write(telescope)

        # Write out test template
        with open(test_dst_file, "w") as f:
            f.write(test)

        # Write out documentation template
        with open(doc_dst_file, "w") as f:
            f.write(doc)

        # Add the new telescope doc to the documentation index
        with open(doc_index_file, "a") as f:
            f.write(f"    {telescope_module}\n")

    def generate_local_config_interactive(self, *, config_path: str, install_odags: bool):
        """Construct an Observatory local config file through user assisted configuration.

        :param config_path: Configuration file path.
        :param install_odags: Whether the user opted to install observatory-dags.
        """

        file_type = "Observatory Config"
        click.echo(f"Generating {file_type}...")

        config = InteractiveConfigBuilder.build(
            backend_type=BackendType.local,
            install_odags=install_odags,
        )

        config.save(config_path)
        click.echo(f'{file_type} saved to: "{config_path}"')

    def generate_terraform_config_interactive(self, *, config_path: str, install_odags: bool):
        """Construct an Observatory Terraform config file through user assisted configuration.

        :param config_path: Configuration file path.
        :param install_odags: Whether the user opted to install observatory-dags.
        """

        file_type = "Terraform Config"
        click.echo(f"Generating {file_type}...")

        config = InteractiveConfigBuilder.build(
            backend_type=BackendType.terraform,
            install_odags=install_odags,
        )

        config.save(config_path)
        click.echo(f'{file_type} saved to: "{config_path}"')


class InteractiveConfigBuilder:
    """Helper class for configuring the ObservatoryConfig class parameters through interactive user input."""

    @staticmethod
    def build(*, backend_type: BackendType, install_odags: bool) -> Union[ObservatoryConfig, TerraformConfig]:
        """Build the correct observatory configuration object through user assisted parameters.

        :param backend_type: The type of Observatory backend being configured.
        :param install_odags: Whether the user opted into installing observatory-dags in the bash installation script.
        :return: An observatory configuration object.
        """

        dags_projects = GenerateCommand.observatory_dagsproject() if install_odags else None

        if backend_type == BackendType.local:
            config = ObservatoryConfig(dags_projects=dags_projects)
        elif backend_type == BackendType.terraform:
            config = TerraformConfig(dags_projects=dags_projects)

        # Common sections for all backends
        InteractiveConfigBuilder.config_backend(config=config, backend_type=backend_type)
        InteractiveConfigBuilder.config_observatory(config)
        InteractiveConfigBuilder.config_terraform(config=config, backend_type=backend_type)
        InteractiveConfigBuilder.config_google_cloud(config)
        InteractiveConfigBuilder.config_airflow_connections(config)
        InteractiveConfigBuilder.config_airflow_variables(config)
        InteractiveConfigBuilder.config_dags_projects(config)

        # Extra sections for Terraform
        if backend_type == BackendType.terraform:
            InteractiveConfigBuilder.config_cloud_sql_database(config)
            InteractiveConfigBuilder.config_airflow_main_vm(config)
            InteractiveConfigBuilder.config_airflow_worker_vm(config)
            InteractiveConfigBuilder.config_elasticsearch(config)
            InteractiveConfigBuilder.config_api(config)

        return config

    @staticmethod
    def config_backend(*, config: Union[ObservatoryConfig, TerraformConfig], backend_type: BackendType):
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
    def config_observatory(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the observatory section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring Observatory settings")
        text = "Enter an Airflow Fernet key (leave blank to autogenerate)"
        default = ""
        fernet_key = click.prompt(text=text, type=str, default=default)

        if fernet_key != "":
            config.airflow_fernet_key = fernet_key

        text = "Enter an Airflow secret key (leave blank to autogenerate)"
        default = ""
        secret_key = click.prompt(text=text, type=str, default=default)

        if secret_key != "":
            config.airflow_secret_key = secret_key

        text = "Enter an email address to use for logging into the Airflow web interface"
        default = config.observatory.airflow_ui_user_email
        user_email = click.prompt(text=text, type=str, default=default, show_default=True)
        config.observatory.airflow_ui_user_email = user_email

        text = f"Password for logging in with {user_email}"
        default = config.observatory.airflow_ui_user_password
        user_pass = click.prompt(text=text, type=str, default=default, show_default=True)
        config.observatory.airflow_ui_user_password = user_pass

        text = "Enter observatory config directory"
        default = config.observatory.observatory_home
        observatory_home = click.prompt(
            text=text, type=click.Path(exists=True, readable=True), default=default, show_default=True
        )
        config.observatory.observatory_home = observatory_home

        text = "Enter postgres password"
        postgres_password = click.prompt(text=text, type=str)
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

        text = "Elastic port"
        default = config.observatory.elastic_port
        elastic_port = click.prompt(text=text, type=int, default=default, show_default=True)
        config.observatory.elastic_port = elastic_port

        text = "Kibana port"
        default = config.observatory.kibana_port
        kibana_port = click.prompt(text=text, type=int, default=default, show_default=True)
        config.observatory.kibana_port = kibana_port

        text = "Docker network name"
        default = config.observatory.docker_network_name
        docker_network_name = click.prompt(text=text, type=str, default=default, show_default=True)
        config.observatory.docker_network_name = docker_network_name

        text = "Docker compose project name"
        default = config.observatory.docker_compose_project_name
        docker_compose_project_name = click.prompt(text=text, type=str, default=default, show_default=True)
        config.observatory.docker_compose_project_name = docker_compose_project_name

    @staticmethod
    def config_google_cloud(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the Google Cloud section.

        :param config: Configuration object to edit.
        """

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

        text = "Region"
        default = "us-west1"
        region = click.prompt(text=text, type=str, default=default, show_default=True)

        text = "Zone"
        default = "us-west1-a"
        zone = click.prompt(text=text, type=str, default=default, show_default=True)

        text = "Download bucket name"
        download_bucket = click.prompt(text=text, type=str)

        text = "Transform bucket name"
        transform_bucket = click.prompt(text=text, type=str)

        buckets = [
            CloudStorageBucket(id="download_bucket", name=download_bucket),
            CloudStorageBucket(id="transform_bucket", name=transform_bucket),
        ]

        config.google_cloud = GoogleCloud(
            project_id=project_id,
            credentials=credentials,
            region=region,
            zone=zone,
            data_location=data_location,
            buckets=buckets,
        )

    @staticmethod
    def config_terraform(*, config: Union[ObservatoryConfig, TerraformConfig], backend_type: BackendType):
        """Configure the Terraform section.

        :param config: Configuration object to edit.
        """

        if not config.schema["terraform"]["required"]:
            text = "Do you want to configure Terraform settings?"
            proceed = click.confirm(text=text, default=False, abort=False, show_default=True)
            if not proceed:
                return

        if backend_type == BackendType.local:
            suffix = " (leave blank to disable)"
            default = ""
        elif backend_type == BackendType.terraform:
            suffix = ""
            default = None

        text = f"Terraform organization name{suffix}"
        organization = click.prompt(text=text, type=str, default=default)

        if organization == "":
            return

        config.terraform = Terraform(organization=organization)

    @staticmethod
    def config_airflow_connections(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the Airflow connections section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring Airflow Connections")

        text = "Do you have any Airlfow connections you wish to add?"
        add_connections = click.confirm(text=text, default=False, abort=False, show_default=True)

        if not add_connections:
            return

        connections = list()
        while True:
            text = "Airflow connection name"
            conn_id = click.prompt(text=text, type=str)

            text = "Airflow connection value"
            conn_name = click.prompt(text=text, type=str)

            connections.append(AirflowConnection(name=conn_id, value=conn_name))

            text = "Do you wish to add another connection?"
            add_another = click.confirm(text=text, default=False, abort=False, show_default=True)

            if not add_another:
                break

        config.airflow_connections = connections

    @staticmethod
    def config_airflow_variables(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the Airflow variables section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring Airflow Variables")

        text = "Do you want to add Airflow variables?"
        add_variables = click.confirm(text=text, default=False, abort=False, show_default=True)

        if not add_variables:
            return

        variables = list()
        while True:
            text = "Name of Airflow variable"
            name = click.prompt(text=text, type=str)

            text = "Value of Airflow variable"
            value = click.prompt(text=text, type=str)

            variables.append(AirflowVariable(name=name, value=value))

            text = "Do you wish to add another variable?"
            add_another = click.confirm(text=text, default=False, abort=False, show_default=True)

            if not add_another:
                break

        config.airflow_variables = variables

    @staticmethod
    def config_dags_projects(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the DAGs projects section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring DAGs Projects")

        text = "Do you want to add custom DAGs projects (not including observatory-dags)?"
        add_dags_projects = click.confirm(text=text, default=False, abort=False, show_default=True)

        if not add_dags_projects:
            return

        projects = list()
        while True:
            text = "DAGs project package name"
            package_name = click.prompt(text=text, type=str)

            text = "DAGs project path"
            path = click.prompt(text=text, type=click.Path(exists=True, readable=True))

            text = "DAGs project module name"
            dags_module = click.prompt(text=text, type=str)

            projects.append(
                DagsProject(
                    package_name=package_name,
                    path=path,
                    dags_module=dags_module,
                )
            )

            text = "Do you wish to add another DAGs project?"
            add_another = click.confirm(text=text, default=False, abort=False, show_default=True)

            if not add_another:
                break

        config.dags_projects.extend(projects)

    @staticmethod
    def config_cloud_sql_database(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the cloud SQL database section.

        :param config: Configuration object to edit.
        """

        if not config.schema["cloud_sql_database"]["required"]:
            text = "Do you want to configure Terraform settings?"
            proceed = click.confirm(text=text, default=False, abort=False, show_default=True)
            if not proceed:
                return

        click.echo("Configuring the Google Cloud SQL Database")

        text = "Google CloudSQL db tier"
        tier = click.prompt(text=text, type=str)

        text = "Google CloudSQL backup start time, e.g., '13:00'"
        backup_start_time = click.prompt(text=text, type=str)

        config.cloud_sql_database = CloudSqlDatabase(
            tier=tier,
            backup_start_time=backup_start_time,
        )

    @staticmethod
    def config_airflow_main_vm(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the Airflow main virtual machine section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring settings for the main VM that runs the Airflow scheduler and webserver")

        text = "Machine type, e.g., n2-standard-2"
        machine_type = click.prompt(text=text, type=str)

        text = "Disk size (GB), e.g., 50"
        disk_size = click.prompt(text=text, type=int)

        text = "Disk type, e.g., pd-ssd"
        disk_type = click.prompt(text=text, type=str)

        # FIND OUT WHAT CREATE MEANS
        text = "Create VM?"
        create = click.confirm(text=text, default=True, abort=False, show_default=True)

        config.airflow_main_vm = VirtualMachine(
            machine_type=machine_type,
            disk_size=disk_size,
            disk_type=disk_type,
            create=create,
        )

    @staticmethod
    def config_airflow_worker_vm(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the Airflow worker virtual machine section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring settings for the weekly on-demand VM that runs large tasks")

        text = "Machine type, e.g., n2-standard-2"
        machine_type = click.prompt(text=text, type=str)

        text = "Disk size (GB), e.g., 3000"
        disk_size = click.prompt(text=text, type=int)

        text = "Disk type, e.g., pd-standard"
        disk_type = click.prompt(text=text, type=str)

        # FIND OUT WHAT CREATE MEANS
        text = "Create VM?"
        create = click.confirm(text=text, default=False, abort=False, show_default=True)

        config.airflow_worker_vm = VirtualMachine(
            machine_type=machine_type,
            disk_size=disk_size,
            disk_type=disk_type,
            create=create,
        )

    @staticmethod
    def config_elasticsearch(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the ElasticSearch section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring ElasticSearch")

        text = "Elasticsearch host url, e.g., https://host:port"
        host = click.prompt(text=text, type=str)

        text = "API key"
        api_key = click.prompt(text=text, type=str)

        config.elasticsearch = ElasticSearch(
            host=host,
            api_key=api_key,
        )

    @staticmethod
    def config_api(config: Union[ObservatoryConfig, TerraformConfig]):
        """Configure the API section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring the Observatory API")

        text = (
            "Custom domain name for the API, used for the google cloud endpoints service, e.g., api.observatory.academy"
        )
        domain_name = click.prompt(text=text, type=str)

        text = "Subdomain scheme"
        choices = click.Choice(choices=["project_id", "environment"], case_sensitive=False)
        subdomain_type = click.prompt(text=text, type=choices, show_default=True, show_choices=True)

        config.api = Api(
            domain_name=domain_name,
            subdomain=subdomain_type,
        )
