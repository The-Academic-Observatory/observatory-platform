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

# Author: James Diprose, Aniek Roelofs

import click
from airflow.configuration import generate_fernet_key

from observatory.platform.observatory_config import ObservatoryConfig, TerraformConfig


class GenerateCommand:

    def generate_fernet_key(self) -> str:
        """ Generate a Fernet key.

        :return: the Fernet key.
        """

        return generate_fernet_key()

    def generate_local_config(self, config_path: str):
        """ Command line user interface for generating an Observatory Config config.yaml.

        :param config_path: the path where the config file should be saved.
        :return: None
        """

        file_type = 'Observatory Config'
        click.echo(f"Generating {file_type}...")
        ObservatoryConfig.save_default(config_path)
        click.echo(f'{file_type} saved to: "{config_path}"')
        click.echo("Please customise the parameters with '<--' in the config file. "
                   "Parameters commented out with '#' are optional.")

    def generate_terraform_config(self, config_path: str):
        """ Command line user interface for generating a Terraform Config config-terraform.yaml.

        :param config_path: the path where the config file should be saved.
        :return: None
        """

        file_type = 'Terraform Config'
        click.echo(f"Generating {file_type}...")
        TerraformConfig.save_default(config_path)
        click.echo(f'{file_type} saved to: "{config_path}"')
        click.echo("Please customise the parameters with '<--' in the config file. "
                   "Parameters commented out with '#' are optional.")
