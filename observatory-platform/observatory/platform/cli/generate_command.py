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
import observatory.dags.dags
import observatory.dags.telescopes
import observatory.templates
from airflow.configuration import generate_fernet_key
from observatory.platform.observatory_config import ObservatoryConfig, TerraformConfig
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

        return generate_fernet_key()

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
