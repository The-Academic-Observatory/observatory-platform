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

import os

import click
from airflow.configuration import generate_fernet_key

from observatory.platform.observatory_config import BackendType, ObservatoryConfig, TerraformConfig
from observatory.platform.utils.config_utils import observatory_home


class GenerateCommand:
    LOCAL_CONFIG_PATH = os.path.join(observatory_home(), 'config.yaml')
    TERRAFORM_CONFIG_PATH = os.path.join(observatory_home(), 'config-terraform.yaml')

    def generate_fernet_key(self) -> str:
        """ Generate a Fernet key.

        :return: the Fernet key.
        """

        return generate_fernet_key()

    def generate_config_file(self, backend_type: BackendType):
        """ Command line user interface for generating config.yaml or config-terraform.yaml.

        :param backend_type: Which type of config file, either BackendType.local or BackendType.terraform.
        :return: None
        """

        print("Generating config.yaml...")

        if backend_type == BackendType.local:
            config_path = GenerateCommand.LOCAL_CONFIG_PATH
            backend_cls = ObservatoryConfig
        elif backend_type == BackendType.terraform:
            config_path = GenerateCommand.TERRAFORM_CONFIG_PATH
            backend_cls = TerraformConfig
        else:
            raise ValueError(f'generate_config_file: backend type not known: {backend_type}')

        if not os.path.exists(config_path) or \
                click.confirm(f'The file "{config_path}" exists, do you want to overwrite it?'):
            backend_cls.save_default(config_path)
            print(f'config.yaml saved to: "{config_path}"')
            print(f"Please customise the parameters with '<--' in the config file. "
                  f"Parameters commented out with '#' are optional.")
        else:
            print("Not generating config.yaml")
