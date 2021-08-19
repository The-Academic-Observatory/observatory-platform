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

    def get_telescope_template_path_(self, telescope_type: str) -> Tuple[str, str, str, str, str]:
        """
        Get the correct template files to use.

        :param telescope_type: Name of the telescope type.
        :return: The template paths for the telescope, dag, test, doc and schema files
        """

        templates_dir = module_file_path("observatory.platform.telescopes.templates")

        telescope_types = {
            "Telescope": {
                "dag": "dag.py.jinja2",
                "telescope": "telescope.py.jinja2",
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

        telescope_files = telescope_types.get(telescope_type)
        if telescope_files is None:
            raise Exception(f"Unsupported telescope type: {telescope_type}")

        dag_file = telescope_files["dag"]
        telescope_file = telescope_files["telescope"]
        test_file = telescope_files["test"]
        doc_file = "doc.md.jinja2"
        schema_file = "schema.json.jinja2"

        telescope_path = os.path.join(templates_dir, telescope_file)
        dag_path = os.path.join(templates_dir, dag_file)
        test_path = os.path.join(templates_dir, test_file)
        doc_path = os.path.join(templates_dir, doc_file)
        schema_path = os.path.join(templates_dir, schema_file)
        return telescope_path, dag_path, test_path, doc_path, schema_path

    def generate_new_telescope(
        self,
        dags_folder: str,
        telescope_type: str,
        telescope_class: str,
        observatory_dir: str = module_file_path("observatory.platform", nav_back_steps=-4),
    ):
        """
        Write files for a new telescope which is using one of the templates

        :param dags_folder: The dags folder where the telescope & dag file will be written to.
        :param telescope_type: Type of telescope to generate.
        :param telescope_class: Class name of the new telescope.
        :param observatory_dir:
        """

        telescope_path, dag_path, test_path, doc_path, schema_path = self.get_telescope_template_path_(telescope_type)
        # Add underscores between capitalised letters, make them all lowercase and strip underscores at the start/end
        telescope_module = re.sub(r"([A-Z])", r"_\1", telescope_class).lower().strip("_")

        # Render templates
        dag = render_template(dag_path, telescope_module=telescope_module, telescope_class=telescope_class)
        telescope = render_template(telescope_path, telescope_module=telescope_module, telescope_class=telescope_class)
        test = render_template(test_path, telescope_module=telescope_module, telescope_class=telescope_class)
        doc = render_template(
            doc_path,
            telescope_module=telescope_module,
            telescope_class=telescope_class,
        )
        schema = render_template(schema_path)

        # Get paths to files
        dag_dst_dir = os.path.join(dags_folder, "dags")
        dag_dst_file = os.path.join(dag_dst_dir, f"{telescope_module}.py")

        telescope_dst_dir = os.path.join(dags_folder, "telescopes")
        telescope_dst_file = os.path.join(telescope_dst_dir, f"{telescope_module}.py")

        #TODO set to right folder once repositories are split
        observatory_dir = get_observatory_dir()

        test_dst_dir = os.path.join(observatory_dir, "tests", "observatory", "dags", "telescopes")
        test_dst_file = os.path.join(test_dst_dir, f"test_{telescope_module}.py")

        doc_dst_dir = os.path.join(observatory_dir, "docs", "telescopes")
        doc_dst_file = os.path.join(doc_dst_dir, f"{telescope_module}.md")

        schema_dst_dir = os.path.join(dags_folder, "database", "schema")
        schema_dst_file = os.path.join(schema_dst_dir, f"{telescope_module}_{datetime.now().strftime('%Y-%m-%d')}.json")

        # Write out files
        write_telescope_file_template(dag_dst_file, dag, "dag")
        write_telescope_file_template(telescope_dst_file, telescope, "telescope")
        write_telescope_file_template(test_dst_file, test, "test")
        write_telescope_file_template(doc_dst_file, doc, "documentation")
        write_telescope_file_template(schema_dst_file, schema, "schema")

        # Update documentation index
        doc_index_file = os.path.join(doc_dst_dir, "index.rst")
        with open(doc_index_file, "a") as f:
            f.write(f"    {telescope_module}\n")
        print(f"- Updated the documentation index file: {doc_index_file}")

        # Update TelescopeTypes in identifiers.py when using organisation template
        if telescope_type == "OrganisationTelescope":
            identifiers_dst_file = os.path.join(
                module_file_path("observatory.api.client.identifiers"), "identifiers.py"
            )
            with open(identifiers_dst_file, "a") as f:
                f.write(f'    {telescope_module} = "{telescope_module}"\n')
            print(f"- Updated the identifiers file: {identifiers_dst_file}")


def write_telescope_file_template(file_path: str, template: str, file_type: str):
    """Write the rendered template for a telescope file to a local file.

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
