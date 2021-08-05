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
import observatory.dags.dags
import observatory.dags.telescopes
import observatory.templates
from cryptography.fernet import Fernet
from observatory.platform.observatory_config import ObservatoryConfig, TerraformConfig
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.config_utils import module_file_path


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

    def get_telescope_template_path_(self, telescope_type: str) -> Tuple[str, str, str, str, str]:
        """
        Get the correct template files to use.

        :param telescope_type: Name of the telescope type.
        :return: The template paths for the telescope, dag, test, doc and schema files
        """

        templates_dir = observatory.platform.telescopes.templates.__path__._path[0]

        dag_file = "dag.py.jinja2"
        test_file = "test.py.jinja2"
        doc_file = "doc.md.jinja2"
        schema_file = "schema.json.jinja2"

        if telescope_type == TelescopeTypes.telescope:
            telescope_file = "telescope.py.jinja2"
        elif telescope_type == TelescopeTypes.stream_telescope:
            telescope_file = "streamtelescope.py.jinja2"
            test_file = "test_stream.py.jinja2"
        elif telescope_type == TelescopeTypes.snapshot_telescope:
            telescope_file = "snapshottelescope.py.jinja2"
            test_file = "test_snapshot.py.jinja2"
        else:
            raise Exception(f"Unsupported workflow type: {workflow_type}")

        workflow_path = os.path.join(templates_dir, workflow_file)
        dag_path = os.path.join(templates_dir, dag_file)
        test_path = os.path.join(templates_dir, test_file)
        doc_path = os.path.join(templates_dir, doc_file)
        schema_path = os.path.join(templates_dir, schema_file)
        return telescope_path, dag_path, test_path, doc_path, schema_path

    def generate_new_telescope(self, telescope_type: str, telescope_class: str, author_name: str):
        """
        Write files for a new telescope which is using one of the templates

        :param telescope_type: Type of telescope to generate.
        :param telescope_class: Class name of the new telescope.
        :param author_name: Author name of the telescope files.
        """

        telescope_path, dag_path, test_path, doc_path, schema_path = self.get_telescope_template_path_(telescope_type)
        # Add underscores between capitalised letters, make them all lowercase and strip underscores at the start/end
        telescope_module = re.sub(r"([A-Z])", r"_\1", telescope_class).lower().strip('_')

        # Render templates
        dag = render_template(dag_path, telescope_module=telescope_module, telescope_class=telescope_class,
                              author_name=author_name)
        telescope = render_template(telescope_path, telescope_module=telescope_module, telescope_class=telescope_class,
                                    author_name=author_name)
        test = render_template(test_path, telescope_module=telescope_module, telescope_class=telescope_class,
                               author_name=author_name)
        doc = render_template(doc_path, telescope_name=telescope_class)
        schema = render_template(schema_path)

        # Get paths to files
        observatory_dir = module_file_path('observatory.platform', nav_back_steps=-4)

        dag_dst_dir = module_file_path('observatory.dags.dags')
        dag_dst_file = os.path.join(dag_dst_dir, f"{telescope_module}.py")

        telescope_dst_dir = module_file_path('observatory.dags.telescopes')
        telescope_dst_file = os.path.join(telescope_dst_dir, f"{telescope_module}.py")

        test_dst_dir = os.path.join(observatory_dir, 'tests', 'observatory', 'dags', 'telescopes')
        test_dst_file = os.path.join(test_dst_dir, f"test_{telescope_module}.py")

        doc_dst_dir = os.path.join(observatory_dir, 'docs', 'telescopes')
        doc_dst_file = os.path.join(doc_dst_dir, f"{telescope_module}.md")

        schema_dst_dir = module_file_path('observatory.dags.database.schema')
        schema_dst_file = os.path.join(schema_dst_dir, f"{telescope_module}_{datetime.now().strftime('%Y-%m-%d')}.json")

        # Write out files
        write_telescope_file(dag_dst_file, dag, "dag")
        write_telescope_file(telescope_dst_file, telescope, "telescope")
        write_telescope_file(test_dst_file, test, "test")
        write_telescope_file(doc_dst_file, doc, "documentation")
        write_telescope_file(schema_dst_file, schema, "schema")

        # Update documentation index
        doc_index_file = os.path.join(doc_dst_dir, "index.rst")
        with open(doc_index_file, "a") as f:
            f.write(f"\t{telescope_module}\n")
        print(f"- Updated the documentation index file: {doc_index_file}")


def write_telescope_file(file_path: str, template: str, file_type: str):
    """

    :param file_path:
    :param template:
    :param file_type:
    :return:
    """
    if os.path.exists(file_path):
        if not click.confirm(f"\nA {file_type} file already exists at: '{file_path}'\n"
                             f"Would you like to overwrite the file?"):
            print("\n")
            return
    with open(file_path, "w") as f:
        f.write(template)
    print(f"- Created a new {file_type} file: {file_path}")
