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
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

import click
from click.termui import edit
from observatory.platform.observatory_config import (
    AirflowConnection,
    AirflowVariable,
    Api,
    BackendType,
    CloudSqlDatabase,
    CloudStorageBucket,
    ElasticSearch,
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
)
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.proc_utils import stream_process

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
    def generate_local_config(self, config_path: str, *, editable: str, workflows: List[str], oapi: bool):
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

    def generate_terraform_config(self, config_path: str, *, editable: str, workflows: List[str], oapi: bool):
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

    def generate_workflows_project(self, project_path: str, package_name: str, author_name: str):
        """Create all directories, init files and a setup.cfg + setup.py file for a new workflows project.

        :param project_path: The path to the new project directory
        :param package_name: The name of the new project package
        :param author_name: The name of the author, used for readthedocs.
        :return: None.
        """
        # Get paths to folders
        dag_dst_dir = os.path.join(project_path, package_name, "dags")
        workflow_dst_dir = os.path.join(project_path, package_name, "workflows")
        workflow_test_dst_dir = os.path.join(workflow_dst_dir, "tests")
        schema_dst_dir = os.path.join(project_path, package_name, "database", "schema")
        doc_dst_dir = os.path.join(project_path, "docs", "workflows")

        # Make folders
        for path in [dag_dst_dir, workflow_dst_dir, workflow_test_dst_dir, doc_dst_dir, schema_dst_dir]:
            os.makedirs(path, exist_ok=True)

        # Make init files
        package_folder = os.path.join(project_path, package_name)
        database_folder = os.path.join(project_path, package_name, "database")
        init_paths = [
            package_folder,
            dag_dst_dir,
            workflow_dst_dir,
            workflow_test_dst_dir,
            database_folder,
            schema_dst_dir,
        ]
        for path in init_paths:
            if not os.path.isfile(path):
                open(os.path.join(path, "__init__.py"), "a").close()

        templates_dir = module_file_path("observatory.platform.cli.templates.generate_project")

        # Create setup.cfg file
        setup_cfg_template = os.path.join(templates_dir, "setup.cfg.jinja2")
        setup_cfg = render_template(setup_cfg_template, package_name=package_name, python_version="3.7")
        setup_cfg_path = os.path.join(project_path, "setup.cfg")
        write_rendered_template(setup_cfg_path, setup_cfg, "setup.cfg")

        # Create setup.py file
        setup_py_template = os.path.join(templates_dir, "setup.py.jinja2")
        setup_py = render_template(setup_py_template, python_version="3.7")
        setup_py_path = os.path.join(project_path, "setup.py")
        write_rendered_template(setup_py_path, setup_py, "setup.py")

        # Create config.py with schema_folder function
        config_template = os.path.join(templates_dir, "config.py.jinja2")
        config = render_template(config_template, package_name=package_name)
        config_path = os.path.join(project_path, package_name, "config.py")
        write_rendered_template(config_path, config, "config.py")

        # Create a working docs directory using sphinx-quickstart
        create_docs_directory(project_path, package_name, author_name, templates_dir)

        print(
            f"""
        Created the following files and directories:
        
        └── {project_path}
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
            │   └── requirements.txt
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
        )

    def generate_workflow(self, project_path: str, package_name: str, workflow_type: str, workflow_class: str):
        """
        Write files for a new telescope which is using one of the templates

        :param project_path: the path to the workflows project.
        :param package_name: the Python package name.
        :param workflow_type: Type of telescope to generate.
        :param workflow_class: Class name of the new telescope.
        """
        # Add underscores between capitalised letters, make them all lowercase and strip underscores at the start/end
        workflow_module = re.sub(r"([A-Z])", r"_\1", workflow_class).lower().strip("_")

        # Get paths to folders
        dag_dst_dir = os.path.join(project_path, package_name, "dags")
        workflow_dst_dir = os.path.join(project_path, package_name, "workflows")
        schema_dst_dir = os.path.join(project_path, package_name, "database", "schema")
        doc_dst_dir = os.path.join(project_path, "docs")

        # Get paths to files
        dag_dst_file = os.path.join(dag_dst_dir, f"{workflow_module}.py")
        identifiers_dst_file = os.path.join(project_path, package_name, "identifiers.py")
        workflow_dst_file = os.path.join(workflow_dst_dir, f"{workflow_module}.py")
        test_dst_file = os.path.join(workflow_dst_dir, "tests", f"test_{workflow_module}.py")
        index_dst_file = os.path.join(doc_dst_dir, "index.rst")
        doc_dst_file = os.path.join(doc_dst_dir, "workflows", f"{workflow_module}.md")
        schema_dst_file = os.path.join(schema_dst_dir, f"{workflow_module}_{datetime.now().strftime('%Y-%m-%d')}.json")

        # Render templates
        workflow_path, dag_path, test_path, doc_index_path, doc_path, schema_path = get_workflow_template_path(
            workflow_type
        )
        dag = render_template(
            dag_path, workflow_module=workflow_module, workflow_class=workflow_class, package_name=package_name
        )
        workflow = render_template(
            workflow_path, workflow_module=workflow_module, workflow_class=workflow_class, package_name=package_name
        )
        test = render_template(
            test_path, workflow_module=workflow_module, workflow_class=workflow_class, package_name=package_name
        )
        doc = render_template(doc_path, workflow_module=workflow_module, workflow_class=workflow_class)
        schema = render_template(schema_path)

        # Write out files
        write_rendered_template(dag_dst_file, dag, "dag")
        write_rendered_template(workflow_dst_file, workflow, "workflow")
        write_rendered_template(test_dst_file, test, "test")
        write_rendered_template(doc_dst_file, doc, "documentation")
        write_rendered_template(schema_dst_file, schema, "schema")

        # Update documentation index
        with open(index_dst_file, "a") as f:
            f.write(f"   workflows/{workflow_module}\n")
        print(f"- Updated the documentation index file: {index_dst_file}")

        # Update TelescopeTypes in identifiers.py when using organisation template
        if workflow_type == "OrganisationTelescope":
            if not os.path.isfile(identifiers_dst_file):
                with open(identifiers_dst_file, "w") as f:
                    f.write("class TelescopeTypes:\n")
            with open(identifiers_dst_file, "a") as f:
                f.write(f'    {workflow_module} = "{workflow_module}"\n')
            print(f"- Updated the identifiers file: {identifiers_dst_file}")

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


def get_workflow_template_path(workflow_type: str) -> Tuple[str, str, str, str, str, str]:
    """
    Get the correct template files to use.

    :param workflow_type: Name of the workflow type.
    :return: The template paths for the workflow, dag, test, doc and schema files
    """

    templates_dir = module_file_path("observatory.platform.workflows.templates")

    workflow_types = {
        "Workflow": {"dag": "dag.py.jinja2", "workflow": "workflow.py.jinja2", "test": "test.py.jinja2"},
        "SnapshotTelescope": {
            "dag": "dag.py.jinja2",
            "workflow": "workflow_snapshot.py.jinja2",
            "test": "test_snapshot.py.jinja2",
        },
        "StreamTelescope": {
            "dag": "dag.py.jinja2",
            "workflow": "workflow_stream.py.jinja2",
            "test": "test_stream.py.jinja2",
        },
        "OrganisationTelescope": {
            "dag": "dag_organisation.py.jinja2",
            "workflow": "workflow_organisation.py.jinja2",
            "test": "test_organisation.py.jinja2",
        },
    }

    workflow_files = workflow_types.get(workflow_type)
    if workflow_files is None:
        raise Exception(f"Unsupported workflow type: {workflow_type}")

    workflow_file = workflow_files["workflow"]
    dag_file = workflow_files["dag"]
    test_file = workflow_files["test"]
    doc_index_file = "index.rst.jinja2"
    doc_file = "doc.md.jinja2"
    schema_file = "schema.json.jinja2"

    workflow_path = os.path.join(templates_dir, workflow_file)
    dag_path = os.path.join(templates_dir, dag_file)
    test_path = os.path.join(templates_dir, test_file)
    doc_index_path = os.path.join(templates_dir, doc_index_file)
    doc_path = os.path.join(templates_dir, doc_file)
    schema_path = os.path.join(templates_dir, schema_file)
    return workflow_path, dag_path, test_path, doc_index_path, doc_path, schema_path


def write_rendered_template(file_path: str, template: str, file_type: str):
    """Write the rendered template for a workflow file to a local file.

    :param file_path: The path to the local file.
    :param template: The rendered template.
    :param file_type: The file type, used for printing information.
    :return: None.
    """
    if os.path.exists(file_path):
        if not click.confirm(
            f"\nA {file_type} file already exists at: '{file_path}'\n" f"Would you like to overwrite the file?"
        ):
            return
    with open(file_path, "w") as f:
        f.write(template)
    print(f"- Created a new {file_type} file: {file_path}")


def create_docs_directory(project_path: str, package_name: str, author_name: str, templates_dir: str):
    """Create a new docs directory, part of a new workflows project

    :param project_path: The full path to the project source
    :param package_name: The name of the package inside the project
    :param author_name: The author name (required for sphinx quickstart)
    :param templates_dir: The directory with template files
    :return: None.
    """
    # Copy generate_schema_csv.py to docs
    src = os.path.join(templates_dir, "generate_schema_csv.py")
    dst = os.path.join(project_path, "docs", "generate_schema_csv.py")
    shutil.copy(src, dst)

    # Copy test_generate_schema_csv.py to docs
    src = os.path.join(templates_dir, "test_generate_schema_csv.py")
    dst = os.path.join(project_path, "docs", "test_generate_schema_csv.py")
    shutil.copy(src, dst)

    # Copy requirements.txt to docs
    src = os.path.join(templates_dir, "docs_requirements.txt")
    dst = os.path.join(project_path, "docs", "requirements.txt")
    shutil.copy(src, dst)

    # Run sphinx quickstart to set up docs
    if os.path.isfile(os.path.join(project_path, "docs", "conf.py")):
        print(
            f"WARNING, the docs directory already has configuration files. The sphinx-quickstart command to set "
            f"up the docs directory will raise an error."
        )
    sphinx_template_dir = os.path.join(templates_dir, "sphinx-quickstart")
    proc = subprocess.Popen(
        [
            "sphinx-quickstart",
            "-q",
            "-t",
            sphinx_template_dir,
            "-p",
            package_name,
            "-a",
            author_name,
            "-d",
            f"package_name={package_name}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=os.path.join(project_path, "docs"),
    )
    stream_process(proc, True)


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
    def build(
        *, backend_type: BackendType, workflows: List[str], oapi: bool, editable: bool
    ) -> Union[ObservatoryConfig, TerraformConfig]:
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
        InteractiveConfigBuilder.config_airflow_connections(config)
        InteractiveConfigBuilder.config_airflow_variables(config)
        InteractiveConfigBuilder.config_workflows_projects(config)

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
    def config_observatory(*, config: Union[ObservatoryConfig, TerraformConfig], oapi: bool, editable: bool):
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
    def config_google_cloud(config: Union[ObservatoryConfig, TerraformConfig]):
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

        if config.backend.type == BackendType.local:
            text = "Download bucket name"
            download_bucket = click.prompt(text=text, type=str)

            text = "Transform bucket name"
            transform_bucket = click.prompt(text=text, type=str)

            buckets.append(CloudStorageBucket(id="download_bucket", name=download_bucket))
            buckets.append(CloudStorageBucket(id="transform_bucket", name=transform_bucket))

        config.google_cloud = GoogleCloud(
            project_id=project_id,
            credentials=credentials,
            region=region,
            zone=zone,
            data_location=data_location,
            buckets=buckets,
        )

    @staticmethod
    def config_terraform(config: Union[ObservatoryConfig, TerraformConfig]):
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
    def config_workflows_projects(config: Union[ObservatoryConfig, TerraformConfig]):
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

    @staticmethod
    def config_elasticsearch(config: TerraformConfig):
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
    def config_api(config: TerraformConfig):
        """Configure the API section.

        :param config: Configuration object to edit.
        """

        click.echo("Configuring the Observatory API")

        text = "Custom domain name for the API, used for the google cloud endpoints service"
        default = "api.observatory.academy"
        domain_name = click.prompt(text=text, type=str, default=default, show_default=True)

        text = "Subdomain scheme"
        default = "project_id"
        choices = click.Choice(choices=["project_id", "environment"], case_sensitive=False)
        subdomain = click.prompt(text=text, type=choices, default=default, show_default=True, show_choices=True)

        config.api = Api(
            domain_name=domain_name,
            subdomain=subdomain,
        )
