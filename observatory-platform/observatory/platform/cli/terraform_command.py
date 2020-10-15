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

from observatory.platform.cli.click import indent, INDENT1, INDENT2
from observatory.platform.observatory_config import TerraformConfig, TerraformVariable
from observatory.platform.terraform_api import TerraformApi


class TerraformCommand:

    def __init__(self, config_path: str, terraform_credentials_path: str, verbose: int = 0):
        """

        :param config_path: the path to the terraform config file.
        :param terraform_credentials_path: the path to the Terraform credentials file.
        :param verbose: whether to run the terraform API in verbose mode.
        """

        self.config_path = config_path
        self.terraform_credentials_path = terraform_credentials_path
        self.verbose = verbose

        # Load config and
        self.terraform_credentials_exists = os.path.exists(terraform_credentials_path)
        self.config_exists = os.path.exists(config_path)
        self.config_is_valid = False
        self.config = None
        if self.config_exists:
            self.config = TerraformConfig.load(config_path)
            self.config_is_valid = self.config.is_valid

    @property
    def is_environment_valid(self):
        """ Whether is the parameters passed to the TerraformCommand are valid.

        :return: whether the parameters passed to the TerraformCommand are valid.
        """

        return all([self.config_exists, self.terraform_credentials_exists, self.config_is_valid,
                    self.config is not None])

    def print_variable(self, var: TerraformVariable):
        """ Print the output for the CLI for a single TerraformVariable instance.

        :param var: the TerraformVariable instance.
        :return: None.
        """

        if var.sensitive:
            print(indent(f"* \x1B[3m{var.key}\x1B[23m: sensitive", INDENT2))
        else:
            print(indent(f"* \x1B[3m{var.key}\x1B[23m: {var.value}", INDENT2))

    def print_variable_update(self, old_var: TerraformVariable, new_var: TerraformVariable):
        """ Print the output for the CLI for a terraform variable that is being updated.

        :param old_var: the old TerraformVariable instance.
        :param new_var: the new TerraformVariable instance.
        :return: None.
        """

        if old_var.sensitive:
            print(indent(f"* \x1B[3m{old_var.key}\x1B[23m: sensitive -> sensitive", INDENT2))
        else:
            print(indent(f"* \x1B[3m{old_var.key}\x1B[23m: {old_var.value} -> {new_var.value}", INDENT2))

    def create_workspace(self):
        """ Create a Terraform workspace.

        :return: None.
        """

        # Get terraform token
        token = TerraformApi.token_from_file(self.terraform_credentials_path)
        terraform_api = TerraformApi(token, self.verbose)

        # Get variables
        terraform_variables = self.config.terraform_variables()

        # Get organization, environment and prefix
        organization = self.config.terraform.organization
        workspace = self.config.terraform_workspace_id

        for variable in terraform_variables:
            self.print_variable(variable)

        # confirm creating workspace
        if click.confirm("Would you like to create a new workspace with these settings?"):
            print("Creating workspace...")

            # Create new workspace
            terraform_api.create_workspace(organization, workspace, auto_apply=True, description="")

            # Get workspace ID
            workspace_id = terraform_api.workspace_id(organization, workspace)

            # Add variables to workspace
            for var in terraform_variables:
                terraform_api.add_workspace_variable(var, workspace_id)

            print('Successfully created workspace')

    def update_workspace(self):
        """ Update a Terraform workspace.

        :return: None.
        """

        # Get terraform token
        token = TerraformApi.token_from_file(self.terraform_credentials_path)
        terraform_api = TerraformApi(token, self.verbose)

        # Get variables
        terraform_variables = self.config.terraform_variables()

        # Get organization, environment and prefix
        organization = self.config.terraform.organization
        workspace = self.config.terraform_workspace_id

        # Get workspace ID
        workspace_id = terraform_api.workspace_id(organization, workspace)
        add, edit, unchanged, delete = terraform_api.plan_variable_changes(terraform_variables, workspace_id)

        if add:
            print(indent('NEW', INDENT1))
            for var in add:
                self.print_variable(var)
        if edit:
            print(indent('UPDATE', INDENT1))
            for old_var, new_var in edit:
                self.print_variable_update(old_var, new_var)
        if delete:
            print(indent('DELETE', INDENT1))
            for var in delete:
                self.print_variable(var)
        if unchanged:
            print(indent('UNCHANGED', INDENT1))
            for var in unchanged:
                self.print_variable(var)

        # confirm creating workspace
        if click.confirm("Would you like to update the workspace with these settings?"):
            print("Updating workspace...")

            # Update variables in workspace
            terraform_api.update_workspace_variables(add, edit, delete, workspace_id)

            print('Successfully updated workspace')
