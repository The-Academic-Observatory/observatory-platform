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

# Author: Aniek Roelofs, James Diprose

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import Tuple, List

import requests


class TerraformVariableCategory(Enum):
    """The type of Terraform variable."""

    terraform = "terraform"
    env = "env"


@dataclass
class TerraformVariable:
    """A TerraformVariable class.

    Attributes:
        key: variable key.
        value: variable value.
        var_id: variable id, uniquely identifies the variable in the cloud.
        description: variable description.
        category: variable category.
        hcl: whether the variable is HCL or not.
        sensitive: whether the variable is sensitive or not.
    """

    key: str
    value: str
    var_id: str = None
    description: str = ""
    category: TerraformVariableCategory = TerraformVariableCategory.terraform
    hcl: bool = False
    sensitive: bool = False

    def __str__(self):
        return self.key

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return self.key == other.key

    @staticmethod
    def from_dict(dict_) -> TerraformVariable:
        """Parse a dictionary into a TerraformVariable instance.

        :param dict_: the dictionary.
        :return: the TerraformVariable instance.
        """

        var_id = dict_.get("id")
        attributes = dict_["attributes"]
        key = attributes.get("key")
        value = attributes.get("value")
        sensitive = attributes.get("sensitive")
        category = attributes.get("category")
        hcl = attributes.get("hcl")
        description = attributes.get("description")

        return TerraformVariable(
            key,
            value,
            sensitive=sensitive,
            category=TerraformVariableCategory(category),
            hcl=hcl,
            description=description,
            var_id=var_id,
        )

    def to_dict(self):
        """Convert a TerraformVariable instance into a dictionary.

        :return: the dictionary.
        """

        var = {
            "type": "vars",
            "attributes": {
                "key": self.key,
                "value": self.value,
                "description": self.description,
                "category": self.category.value,
                "hcl": self.hcl,
                "sensitive": self.sensitive,
            },
        }

        if self.var_id is not None:
            var["id"] = self.var_id

        return var


class TerraformApi:
    TERRAFORM_WORKSPACE_VERSION = "0.13.5"
    VERBOSITY_WARNING = 0
    VERBOSITY_INFO = 1
    VERBOSITY_DEBUG = 2

    def __init__(self, token: str, verbosity: int = VERBOSITY_WARNING):
        """Create a TerraformApi instance.

        :param token: the Terraform API token.
        :param verbosity: the verbosity for the Terraform API.
        """

        self.token = token
        if verbosity == TerraformApi.VERBOSITY_WARNING:
            logging.getLogger().setLevel(logging.WARNING)
        elif verbosity == TerraformApi.VERBOSITY_INFO:
            logging.getLogger().setLevel(logging.INFO)
        elif verbosity >= TerraformApi.VERBOSITY_DEBUG:
            logging.getLogger().setLevel(logging.DEBUG)
        self.api_url = "https://app.terraform.io/api/v2"
        self.headers = {"Content-Type": "application/vnd.api+json", "Authorization": f"Bearer {token}"}

    @staticmethod
    def token_from_file(file_path: str) -> str:
        """Get the Terraform token from a credentials file.

        :param file_path: path to credentials file
        :return: token
        """

        with open(file_path, "r") as file:
            token = json.load(file)["credentials"]["app.terraform.io"]["token"]
        return token

    def create_workspace(
        self,
        organisation: str,
        workspace: str,
        auto_apply: bool,
        description: str,
        version: str = TERRAFORM_WORKSPACE_VERSION,
    ) -> int:
        """Create a new workspace in Terraform Cloud.

        :param organisation: Name of terraform organisation
        :param workspace: Name of terraform workspace
        :param auto_apply: Whether the new workspace should be set to auto_apply
        :param description: Description of the workspace
        :param version: The terraform version
        :return: The response status code
        """
        attributes = {
            "name": workspace,
            "auto-apply": str(auto_apply).lower(),
            "description": description,
            "terraform_version": version,
        }
        data = {"data": {"type": "workspaces", "attributes": attributes}}

        response = requests.post(
            f"{self.api_url}/organizations/{organisation}/workspaces", headers=self.headers, json=data
        )

        if response.status_code == HTTPStatus.CREATED:
            logging.info(f"Created workspace {workspace}")
            logging.debug(f"response: {response.text}")
        elif response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
            logging.warning(f"Workspace with name {workspace} already exists")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful creating workspace, response: {response.text}")
            exit(os.EX_CONFIG)
        return response.status_code

    def delete_workspace(self, organisation: str, workspace: str) -> int:
        """Delete a workspace in terraform cloud.

        :param organisation: Name of terraform organisation
        :param workspace: Name of terraform workspace
        :return: The response status code
        """

        response = requests.delete(
            f"{self.api_url}/organizations/{organisation}/workspaces/{workspace}", headers=self.headers
        )
        return response.status_code

    def workspace_id(self, organisation: str, workspace: str) -> str:
        """Returns the workspace id.

        :param organisation: Name of terraform organisation
        :param workspace: Name of terraform workspace
        :return: workspace id
        """
        response = requests.get(
            f"{self.api_url}/organizations/{organisation}/workspaces/{workspace}", headers=self.headers
        )
        if response.status_code == HTTPStatus.OK:
            logging.info(f"Retrieved workspace id for workspace '{workspace}'.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(
                f"Unsuccessful retrieving workspace id for workspace '{workspace}', response: {response.text}"
            )
            exit(os.EX_CONFIG)

        workspace_id = json.loads(response.text)["data"]["id"]
        return workspace_id

    def add_workspace_variable(self, variable: TerraformVariable, workspace_id: str) -> str:
        """Add a new variable to the workspace. Will return an error if the variable already exists.

        :param variable: the TerraformVariable instance.
        :param workspace_id: the workspace id
        :return: The var id
        """

        response = requests.post(
            f"{self.api_url}/workspaces/{workspace_id}/vars", headers=self.headers, json={"data": variable.to_dict()}
        )

        key = variable.key
        if response.status_code == HTTPStatus.CREATED:
            logging.info(f"Added variable {key}")
        else:
            msg = f"Unsuccessful adding variable {key}, response: {response.text}, status_code: {response.status_code}"
            logging.error(msg)
            raise ValueError(msg)

        var_id = json.loads(response.text)["data"]["id"]
        return var_id

    def update_workspace_variable(self, variable: TerraformVariable, workspace_id: str) -> int:
        """Update a workspace variable that is identified by its id.

        :param variable: attributes of the variable
        :param var_id: the variable id
        :param workspace_id: the workspace id
        :return: the response status code
        """
        response = requests.patch(
            f"{self.api_url}/workspaces/{workspace_id}/vars/{variable.var_id}",
            headers=self.headers,
            json={"data": variable.to_dict()},
        )
        try:
            key = json.loads(response.text)["data"]["attributes"]["key"]
        except KeyError:
            try:
                key = variable.key
            except KeyError:
                key = None
        if response.status_code == HTTPStatus.OK:
            logging.info(f"Updated variable {key}")
        else:
            msg = f"Unsuccessful updating variable with id {variable.var_id} and key {key}, response: {response.text}, status_code: {response.status_code}"
            logging.error(msg)
            raise ValueError(msg)

        return response.status_code

    def delete_workspace_variable(self, var: TerraformVariable, workspace_id: str) -> int:
        """Delete a workspace variable identified by its id. Should not return any content in response.

        :param var: the variable
        :param workspace_id: the workspace id
        :return: The response code
        """
        response = requests.delete(f"{self.api_url}/workspaces/{workspace_id}/vars/{var.var_id}", headers=self.headers)

        if response.status_code == HTTPStatus.NO_CONTENT:
            logging.info(f"Deleted variable with id {var.var_id}")
        else:
            msg = f"Unsuccessful deleting variable with id {var.var_id}, response: {response.text}, status_code: {response.status_code}"
            logging.error(msg)
            raise ValueError(msg)

        return response.status_code

    def list_workspace_variables(self, workspace_id: str) -> List[TerraformVariable]:
        """Returns a list of variables in the workspace. Each variable is a dict.

        :param workspace_id: The workspace id
        :return: Variables in the workspace
        """
        response = requests.get(f"{self.api_url}/workspaces/{workspace_id}/vars", headers=self.headers)

        if response.status_code == HTTPStatus.OK:
            logging.info(f"Retrieved workspace variables.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful retrieving workspace variables, response: {response.text}")
            exit(os.EX_CONFIG)

        workspace_vars = json.loads(response.text)["data"]
        return [TerraformVariable.from_dict(dict_) for dict_ in workspace_vars]

    def create_configuration_version(self, workspace_id: str) -> Tuple[str, str]:
        """Create a configuration version. A configuration version is a resource used to reference the uploaded
        configuration files. It is associated with the run to use the uploaded configuration files for performing the
        plan and apply.

        :param workspace_id: the workspace id
        :return: the upload url
        """
        data = {"data": {"type": "configuration-versions", "attributes": {"auto-queue-runs": "false"}}}
        response = requests.post(
            f"{self.api_url}/workspaces/{workspace_id}/configuration-versions", headers=self.headers, json=data
        )
        if response.status_code == 201:
            logging.info(f"Created configuration version.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful creating configuration version, response: {response.text}")
            exit(os.EX_CONFIG)

        upload_url = json.loads(response.text)["data"]["attributes"]["upload-url"]
        configuration_id = json.loads(response.text)["data"]["id"]
        return upload_url, configuration_id

    def get_configuration_version_status(self, configuration_id: str) -> str:
        """Show the configuration version and return it's status. The status will be pending when the
        configuration version is initially created and will remain pending until configuration files are supplied via
        upload, and while they are processed. The status will then be changed to 'uploaded'. Runs cannot be created
        using pending or errored configuration versions.

        :param configuration_id: the configuration version id
        :return: configuration version status
        """
        response = requests.get(f"{self.api_url}/configuration-versions/{configuration_id}", headers=self.headers)
        if response.status_code == HTTPStatus.OK:
            logging.info(f"Retrieved configuration version info.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful retrieving configuration version info, response: {response.text}")
            exit(os.EX_CONFIG)

        status = json.loads(response.text)["data"]["attributes"]["status"]
        return status

    @staticmethod
    def upload_configuration_files(upload_url: str, configuration_path: str) -> int:
        """Uploads the configuration files. Auto-queue-runs is set to false when creating configuration version,
        so conf will not be queued automatically.

        :param upload_url: upload url, returned when creating configuration version
        :param configuration_path: path to tar.gz file containing config files (main.tf)
        :return: the response code
        """

        headers = {"Content-Type": "application/octet-stream"}
        with open(configuration_path, "rb") as configuration:
            response = requests.put(upload_url, headers=headers, data=configuration.read())
        if response.status_code == HTTPStatus.OK:
            logging.info(f"Uploaded configuration.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful uploading configuration, response: {response.text}")
            exit(os.EX_CONFIG)

        return response.status_code

    def create_run(self, workspace_id: str, target_addrs: str = None, message: str = "") -> str:
        """Creates a run, optionally targeted at a target address. If auto-apply is set to true the run will be applied
        afterwards as well.

        :param workspace_id: the workspace id
        :param target_addrs: the target address (id of the module/resource)
        :param message: additional message that will be displayed at the terraform cloud run
        :return: the run id
        """

        data = {
            "data": {
                "attributes": {"message": message},
                "type": "runs",
                "relationships": {"workspace": {"data": {"type": "workspaces", "id": workspace_id}}},
            }
        }
        if target_addrs:
            data["data"]["attributes"]["target-addrs"] = [target_addrs]

        response = requests.post(f"{self.api_url}/runs", headers=self.headers, json=data)
        if response.status_code == HTTPStatus.CREATED:
            logging.info(f"Created run.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful creating run, response: {response.text}")
            exit(os.EX_CONFIG)

        run_id = json.loads(response.text)["data"]["id"]
        return run_id

    def get_run_details(self, run_id: str) -> dict:
        """Get details on a run identified by its id.
        :param run_id: the run id
        :return: the response text
        """

        response = requests.get(f"{self.api_url}/runs/{run_id}", headers=self.headers)
        if not response.status_code == HTTPStatus.OK:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful retrieving run details, response: {response.text}")
            exit(os.EX_CONFIG)

        return json.loads(response.text)

    def plan_variable_changes(
        self, new_vars: List[TerraformVariable], workspace_id: str
    ) -> Tuple[
        List[TerraformVariable],
        List[Tuple[TerraformVariable, TerraformVariable]],
        List[TerraformVariable],
        List[TerraformVariable],
    ]:
        """Compares the current variables in the workspace with a list of new variables. It sorts the new variables in
        one of 4 different categories and adds them to the corresponding list. Sensitive variables can never be
        'unchanged'.

        :param new_vars: list of potential new variables where each variable is a variable attributes dict
        :param workspace_id: the workspace id
        :return: lists of variables in different categories (add, edit, unchanged, delete).
        add: list of attributes dicts
        edit: list of tuples with (attributes dict, var id, old value)
        unchanged: list of attributes dicts
        delete: list of tuples with (var key, var id, old value)
        """

        add: List[TerraformVariable] = []
        edit: List[Tuple[TerraformVariable, TerraformVariable]] = []
        unchanged: List[TerraformVariable] = []

        # Get existing variables
        old_vars = self.list_workspace_variables(workspace_id)

        # Make dict with old variable keys as keys
        old_var_ids = {}
        for old_var in old_vars:
            old_var_ids[old_var.key] = old_var

        # Check which new variables need to be updated and which need to be added
        for new_var in new_vars:
            # check if variable already exists
            if new_var.key in old_var_ids.keys():
                old_var = old_var_ids[new_var.key]

                # check if values of old and new are the same
                if new_var.sensitive or new_var.value != old_var.value:
                    # Assign id of old variable to new variable
                    new_var.var_id = old_var.var_id
                    edit.append((old_var, new_var))
                else:
                    unchanged.append(new_var)
            else:
                add.append(new_var)

        # Compare old variables with new variables and check which are 'extra' in the old vars
        delete = list(set(old_vars) - set(new_vars))

        return add, edit, unchanged, delete

    def update_workspace_variables(self, add: list, edit: list, delete: list, workspace_id: str):
        """Update workspace accordingly to the planned changes.

        :param add: list of attributes dicts of new variables
        :param edit: list of tuples with (attributes dict, var id, old value) of existing variables that will be updated
        :param delete: list of tuples with (var key, var id, old value) of variables that will be deleted
        :param workspace_id: the workspace id
        :return: None
        """

        for var in add:
            self.add_workspace_variable(var, workspace_id)
        for old_var, new_var in edit:
            self.update_workspace_variable(new_var, workspace_id)
        for var in delete:
            self.delete_workspace_variable(var, workspace_id)
