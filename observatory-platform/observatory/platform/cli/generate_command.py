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
from typing import Tuple

import click
import stringcase
from cryptography.fernet import Fernet

from observatory.platform.observatory_config import ObservatoryConfig, TerraformConfig
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.jinja2_utils import render_template


class WorkflowTypes:
    """
    Workflow types that we can generate from a template.
    """

    workflow = "Workflow"
    stream_telescope = "StreamTelescope"
    snapshot_telescope = "SnapshotTelescope"


class GenerateCommand:
    def generate_fernet_key(self) -> str:
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

    def get_workflow_template_path_(self, workflow_type: str) -> Tuple[str, str, str, str]:
        """
        Get the correct template files to use.

        :param workflow_type: Name of the workflow type.
        :return: The workflow template path, and the dag template path.
        """

        templates_dir = module_file_path("observatory.platform.workflows.templates")
        dag_file = "workflow_dag.py.jinja2"
        test_file = "test.py.jinja2"
        doc_file = "doc.md.jinja2"

        if workflow_type == WorkflowTypes.workflow:
            workflow_file = "workflow.py.jinja2"
        elif workflow_type == WorkflowTypes.stream_telescope:
            workflow_file = "stream_telescope.py.jinja2"
        elif workflow_type == WorkflowTypes.snapshot_telescope:
            workflow_file = "snapshot_telescope.py.jinja2"
        else:
            raise Exception(f"Unsupported workflow type: {workflow_type}")

        workflow_path = os.path.join(templates_dir, workflow_file)
        dag_path = os.path.join(templates_dir, dag_file)
        test_path = os.path.join(templates_dir, test_file)
        doc_path = os.path.join(templates_dir, doc_file)
        return workflow_path, dag_path, test_path, doc_path

    def generate_new_workflow(self, *, project_path: str, package_name: str, workflow_type: str, workflow_name: str):
        """ Make a new workflow template.

        :param project_path: the path to the workflows project.
        :param package_name: the Python package name.
        :param workflow_type: Type of workflow to generate.
        :param workflow_name: Class name of the new telescope.
        """

        # Standardise names
        package_name = stringcase.snakecase(package_name)
        workflow_class = stringcase.capitalcase(stringcase.camelcase(workflow_name))
        workflow_module = stringcase.lowercase(stringcase.snakecase(workflow_name))
        workflow_file = f"{workflow_module}.py"

        # Destination folders e.g.
        # academic-observatory-workflows/academic_observatory_workflows/workflows
        # academic-observatory-workflows/academic_observatory_workflows/dags
        # academic-observatory-workflows/tests/academic_observatory_workflows/workflows
        # academic-observatory-workflows/docs
        workflow_dst_dir = os.path.join(project_path, package_name, "workflows")
        dags_dst_dir = os.path.join(project_path, package_name, "dags")
        test_dst_dir = os.path.join(project_path, "tests", package_name, "workflows")
        doc_dst_dir = os.path.join(project_path, "docs")

        # Make folders
        for path in [workflow_dst_dir, dags_dst_dir, test_dst_dir, doc_dst_dir]:
            os.makedirs(path, exist_ok=True)

        # Make init files
        package_folder = os.path.join(project_path, package_name)
        tests_package_folder = os.path.join(project_path, "tests", package_name)
        init_paths = [package_folder, tests_package_folder, workflow_dst_dir, dags_dst_dir, test_dst_dir]
        for path in init_paths:
            if not os.path.isfile(path):
                open(os.path.join(path, "__init__.py"), "a").close()

        # Destination files
        workflow_dst_file = os.path.join(workflow_dst_dir, workflow_file)
        dag_dst_file = os.path.join(dags_dst_dir, workflow_file)
        test_dst_file = os.path.join(test_dst_dir, f"test_{workflow_file}")
        doc_dst_file = os.path.join(doc_dst_dir, f"{workflow_module}.md")
        doc_index_file = os.path.join(doc_dst_dir, f"index.rst")

        # Get template paths
        (
            workflow_template_path,
            dag_template_path,
            test_template_path,
            doc_template_path,
        ) = self.get_workflow_template_path_(workflow_type)

        # Render files
        workflow = render_template(
            workflow_template_path, workflow_class=workflow_class, workflow_module=workflow_module
        )
        dag = render_template(
            dag_template_path,
            package_name=package_name,
            workflow_class=workflow_class,
            workflow_module=workflow_module,
        )
        test = render_template(test_template_path, workflow_class=workflow_class, workflow_module=workflow_module)
        doc = render_template(doc_template_path, workflow_class=workflow_class, workflow_module=workflow_module)

        # Write out dag template
        with open(dag_dst_file, "w") as f:
            f.write(dag)
        print(f"Created a new workflow file: {workflow_dst_file}")

        # Write out workflow template
        with open(workflow_dst_file, "w") as f:
            f.write(workflow)
        print(f"Created a new dag file: {dag_dst_file}")

        # Write out test template
        with open(test_dst_file, "w") as f:
            f.write(test)
        print(f"Created a new workflow documentation file: {doc_dst_file}")

        # Write out documentation template
        with open(doc_dst_file, "w") as f:
            f.write(doc)
        print(f"Created a new workflow test file: {test_dst_file}")

        # Add the new workflow doc to the documentation index
        with open(doc_index_file, "a") as f:
            f.write(f"    {workflow_module}\n")
        print(f"Add the new workflow doc to the documentation index: {test_dst_file}")
