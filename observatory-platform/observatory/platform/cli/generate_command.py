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
from datetime import datetime
from typing import Tuple

import click
from cryptography.fernet import Fernet

from observatory.platform.observatory_config import ObservatoryConfig, TerraformConfig
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.config_utils import module_file_path


class GenerateCommand:
    def generate_fernet_key(self) -> bytes:
        """Generate a Fernet key.

        :return: the Fernet key.
        """

        return Fernet.generate_key()

    def generate_local_config(self, config_path: str):
        """Command line user interface for generating an Observatory Config config.yaml.

        :param config_path: the path where the config file should be saved.
        :return: None
        """

        file_type = "Observatory Config"
        click.echo(f"Generating {file_type}...")
        ObservatoryConfig.save_default(config_path)
        click.echo(f'{file_type} saved to: "{config_path}"')

    def generate_terraform_config(self, config_path: str):
        """Command line user interface for generating a Terraform Config config-terraform.yaml.

        :param config_path: the path where the config file should be saved.
        :return: None
        """

        file_type = "Terraform Config"
        click.echo(f"Generating {file_type}...")
        TerraformConfig.save_default(config_path)
        click.echo(f'{file_type} saved to: "{config_path}"')
        click.echo(
            "Please customise the parameters with '<--' in the config file. "
            "Parameters commented out with '#' are optional."
        )

    def generate_new_workflows_project(self, project_path: str, package_name: str):
        # Get paths to folders
        dag_dst_dir = os.path.join(project_path, package_name, "dags")
        workflow_dst_dir = os.path.join(project_path, package_name, "workflows")
        schema_dst_dir = os.path.join(project_path, package_name, "database", "schema")
        test_dst_dir = os.path.join(project_path, "tests", "workflows")
        doc_dst_dir = os.path.join(project_path, "docs")

        # Make folders
        for path in [dag_dst_dir, workflow_dst_dir, test_dst_dir, doc_dst_dir, schema_dst_dir]:
            os.makedirs(path, exist_ok=True)

        # Make init files
        package_folder = os.path.join(project_path, package_name)
        database_folder = os.path.join(project_path, package_name, "database")
        tests_folder = os.path.join(project_path, "tests")
        init_paths = [package_folder, dag_dst_dir, workflow_dst_dir, tests_folder, test_dst_dir, database_folder,
                      schema_dst_dir]
        for path in init_paths:
            if not os.path.isfile(path):
                open(os.path.join(path, "__init__.py"), "a").close()

        print(f"""
        Created the following files and directories:
        
        └── {project_path}
            ├── docs
            │   └── index.rst
            ├── {package_name}
            │   ├── __init__.py
            │   ├── dags
            │   │   └── __init__.py
            │   ├── database
            │   │   ├── __init__.py
            │   │   └── schema
            │   │       └── __init__.py
            │   └── workflows
            │       └── __init__.py
            └── tests
                ├── __init__.py
                └── workflows
                    └── __init__.py
        """)

    def generate_new_workflow(
            self,
            project_path: str,
            package_name: str,
            workflow_type: str,
            workflow_class: str,
    ):
        """
        Write files for a new telescope which is using one of the templates

        :param project_path: the path to the workflows project.
        :param package_name: the Python package name.
        :param workflow_type: Type of telescope to generate.
        :param workflow_class: Class name of the new telescope.
        """
        # Add underscores between capitalised letters, make them all lowercase and strip underscores at the start/end
        telescope_module = re.sub(r"([A-Z])", r"_\1", workflow_class).lower().strip("_")

        # Get paths to folders
        dag_dst_dir = os.path.join(project_path, package_name, "dags")
        workflow_dst_dir = os.path.join(project_path, package_name, "workflows")
        schema_dst_dir = os.path.join(project_path, package_name, "database", "schema")
        test_dst_dir = os.path.join(project_path, "tests", "workflows")
        doc_dst_dir = os.path.join(project_path, "docs")

        # Get paths to files
        dag_dst_file = os.path.join(dag_dst_dir, f"{telescope_module}.py")
        telescope_dst_file = os.path.join(workflow_dst_dir, f"{telescope_module}.py")
        test_dst_file = os.path.join(test_dst_dir, f"test_{telescope_module}.py")
        doc_dst_file = os.path.join(doc_dst_dir, f"{telescope_module}.md")
        schema_dst_file = os.path.join(schema_dst_dir, f"{telescope_module}_{datetime.now().strftime('%Y-%m-%d')}.json")

        # Render templates
        workflow_path, dag_path, test_path, doc_path, schema_path = get_workflow_template_path(workflow_type)
        dag = render_template(dag_path, telescope_module=telescope_module, telescope_class=workflow_class)
        workflow = render_template(workflow_path, telescope_module=telescope_module, telescope_class=workflow_class)
        test = render_template(test_path, telescope_module=telescope_module, telescope_class=workflow_class)
        doc = render_template(
            doc_path,
            telescope_module=telescope_module,
            telescope_class=workflow_class,
        )
        schema = render_template(schema_path)

        # Write out files
        write_workflow_file_template(dag_dst_file, dag, "dag")
        write_workflow_file_template(telescope_dst_file, workflow, "workflow")
        write_workflow_file_template(test_dst_file, test, "test")
        write_workflow_file_template(doc_dst_file, doc, "documentation")
        write_workflow_file_template(schema_dst_file, schema, "schema")

        # Update documentation index
        doc_index_file = os.path.join(doc_dst_dir, "index.rst")
        if not os.path.isfile(doc_index_file):
            with open(doc_index_file, "w") as f:
                f.write(
                    """
Telescopes
=======================
.. toctree::
    :maxdepth: 2

""")
        with open(doc_index_file, "a") as f:
            f.write(f"    {telescope_module}\n")
        print(f"- Updated the documentation index file: {doc_index_file}")

        # Update TelescopeTypes in identifiers.py when using organisation template
        if workflow_type == "OrganisationTelescope":
            identifiers_dst_file = os.path.join(
                module_file_path("observatory.api.client.identifiers"), "identifiers.py"
            )
            with open(identifiers_dst_file, "a") as f:
                f.write(f'    {telescope_module} = "{telescope_module}"\n')
            print(f"- Updated the identifiers file: {identifiers_dst_file}")


def get_workflow_template_path(workflow_type: str) -> Tuple[str, str, str, str, str]:
    """
    Get the correct template files to use.

    :param workflow_type: Name of the workflow type.
    :return: The template paths for the workflow, dag, test, doc and schema files
    """

    templates_dir = module_file_path("observatory.platform.workflows.templates")

    workflow_types = {
        "Workflow": {
            "dag": "dag.py.jinja2",
            "telescope": "workflow.py.jinja2",
            "test": "test.py.jinja2",
        },
        "SnapshotTelescope": {
            "dag": "dag.py.jinja2",
            "telescope": "telescope_snapshot.py.jinja2",
            "test": "test_snapshot.py.jinja2",
        },
        "StreamTelescope": {
            "dag": "dag.py.jinja2",
            "telescope": "telescope_stream.py.jinja2",
            "test": "test_stream.py.jinja2",
        },
        "OrganisationTelescope": {
            "dag": "dag_organisation.py.jinja2",
            "telescope": "telescope_organisation.py.jinja2",
            "test": "test_organisation.py.jinja2",
        },
    }

    workflow_files = workflow_types.get(workflow_type)
    if workflow_files is None:
        raise Exception(f"Unsupported telescope type: {workflow_type}")

    dag_file = workflow_files["dag"]
    workflow_file = workflow_files["telescope"]
    test_file = workflow_files["test"]
    doc_file = "doc.md.jinja2"
    schema_file = "schema.json.jinja2"

    workflow_path = os.path.join(templates_dir, workflow_file)
    dag_path = os.path.join(templates_dir, dag_file)
    test_path = os.path.join(templates_dir, test_file)
    doc_path = os.path.join(templates_dir, doc_file)
    schema_path = os.path.join(templates_dir, schema_file)
    return workflow_path, dag_path, test_path, doc_path, schema_path


def write_workflow_file_template(file_path: str, template: str, file_type: str):
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


def get_observatory_dir():
    """ Return path to dir where docs and tests are stored, for now this is in observatory platform, but will change
    once the repositories are split.

    :return: Path to observatory platform dir
    """
    return module_file_path("observatory.platform", nav_back_steps=-4)
