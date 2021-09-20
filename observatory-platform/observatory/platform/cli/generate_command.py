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
import shutil
import re
from datetime import datetime
from typing import Tuple

import click
import subprocess
from cryptography.fernet import Fernet

from observatory.platform.observatory_config import ObservatoryConfig, TerraformConfig
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.proc_utils import stream_process


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
