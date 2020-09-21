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

# Author: Aniek Roelofs

import json
import logging
import os
import requests
from typing import Tuple


class TerraformApi:
    def __init__(self, token: str, verbosity: int = 0):
        self.token = token
        if verbosity == 0:
            logging.getLogger().setLevel(logging.WARNING)
        elif verbosity == 1:
            logging.getLogger().setLevel(logging.INFO)
        elif verbosity >= 2:
            logging.getLogger().setLevel(logging.DEBUG)
        self.api_url = 'https://app.terraform.io/api/v2'
        self.headers = {
            'Content-Type': 'application/vnd.api+json',
            'Authorization': f'Bearer {token}'
        }

    @staticmethod
    def token_from_file(file_path: str) -> str:
        """
        Get the terraform token from a credentials file
        :param file_path: path to credentials file
        :return: token
        """
        with open(file_path, 'r') as file:
            for line in file:
                if "token" in line:
                    token = line.split('"token":')[1].strip().strip('"}')
        return token

    def create_workspace(self, organisation: str, workspace: str, auto_apply: bool, description: str,
                         version: str = "0.13.0-beta3") -> int:
        """
        Create a new workspace in terraform cloud
        :param organisation: Name of terraform organisation
        :param workspace: Name of terraform workspace
        :param auto_apply: Whether the new workspace should be set to auto_apply
        :param description: Description of the workspace
        :param version: The terraform version
        :return: The response status code
        """
        attributes = {"name": workspace,
                      "auto-apply": str(auto_apply).lower(),
                      "description": description,
                      "terraform_version": version}
        data = {"data": {"type": "workspaces", "attributes": attributes}}

        response = requests.post(
            f'{self.api_url}/organizations/{organisation}/workspaces',
            headers=self.headers, json=data)

        if response.status_code == 201:
            logging.info(f"Created workspace {workspace}")
            logging.debug(f"response: {response.text}")
        elif response.status_code == 422:
            logging.warning(f"Workspace with name {workspace} already exists")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful creating workspace, response: {response.text}")
            exit(os.EX_CONFIG)
        return response.status_code

    def delete_workspace(self, organisation: str, workspace: str) -> int:
        """
        Delete a workspace in terraform cloud
        :param organisation: Name of terraform organisation
        :param workspace: Name of terraform workspace
        :return: The response status code
        """

        response = requests.delete(f'{self.api_url}/organizations/{organisation}/workspaces/{workspace}',
                                   headers=self.headers)
        return response.status_code

    def workspace_id(self, organisation: str, workspace: str) -> str:
        """
        Returns the workspace id
        :param organisation: Name of terraform organisation
        :param workspace: Name of terraform workspace
        :return: workspace id
        """
        response = requests.get(
            f'{self.api_url}/organizations/{organisation}/workspaces/{workspace}',
            headers=self.headers)
        if response.status_code == 200:
            logging.info(f"Retrieved workspace id for workspace '{workspace}'.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful retrieving workspace id for workspace '{workspace}', response: {response.text}")
            exit(os.EX_CONFIG)

        workspace_id = json.loads(response.text)['data']['id']
        return workspace_id

    def add_workspace_variable(self, attributes: dict, workspace_id: str) -> str:
        """
        Add a new variable to the workspace. Will return an error if the variable already exists.
        :param attributes: attributes of the variable
        :param workspace_id: the workspace id
        :return: The var id
        """
        data = {"data": {"type": "vars", "attributes": attributes}}
        response = requests.post(f'{self.api_url}/workspaces/{workspace_id}/vars',
                                 headers=self.headers, json=data)

        key = attributes['key']
        if response.status_code == 201:
            logging.info(f"Added variable {key}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful adding variable {key}, response: {response.text}")
            exit(os.EX_CONFIG)

        var_id = json.loads(response.text)['data']['id']
        return var_id

    def update_workspace_variable(self, attributes: dict, var_id: str, workspace_id: str) -> int:
        """
        Update a workspace variable that is identified by its id.
        :param attributes: attributes of the variable
        :param var_id: the variable id
        :param workspace_id: the workspace id
        :return: the response status code
        """
        data = {"data": {"type": "vars", "id": var_id, "attributes": attributes}}
        response = requests.patch(f'{self.api_url}/workspaces/{workspace_id}/vars/{var_id}', headers=self.headers,
                                  json=data)
        try:
            key = json.loads(response.text)['data']['attributes']['key']
        except KeyError:
            try:
                key = attributes['key']
            except KeyError:
                key = None
        if response.status_code == 200:
            logging.info(f"Updated variable {key}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful updating variable with id {var_id} and key {key}, response: {response.text}")
            exit(os.EX_CONFIG)

        return response.status_code

    def delete_workspace_variable(self, var_id: str, workspace_id: str) -> int:
        """
        Delete a workspace variable identified by its id. Should not return any content in response.
        :param var_id: the variable id
        :param workspace_id: the workspace id
        :return: The response code
        """
        response = requests.delete(f'{self.api_url}/workspaces/{workspace_id}/vars/{var_id}', headers=self.headers)

        if response.status_code == 204:
            logging.info(f"Deleted variable with id {var_id}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful deleting variable with id {var_id}, response: {response.text}")
            exit(os.EX_CONFIG)

        return response.status_code

    def list_workspace_variables(self, workspace_id: str) -> list:
        """
        Returns a list of variables in the workspace. Each variable is a dict.
        :param workspace_id: The workspace id
        :return: Variables in the workspace
        """
        response = requests.get(f'{self.api_url}/workspaces/{workspace_id}/vars', headers=self.headers)

        if response.status_code == 200:
            logging.info(f"Retrieved workspace variables.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful retrieving workspace variables, response: {response.text}")
            exit(os.EX_CONFIG)

        workspace_vars = json.loads(response.text)['data']
        return workspace_vars

    def create_configuration_version(self, workspace_id: str) -> str:
        """
        Create a configuration version. A configuration version is a resource used to reference the uploaded
        configuration files. It is associated with the run to use the uploaded configuration files for performing the
        plan and apply.
        :param workspace_id: the workspace id
        :return: the upload url
        """
        data = {"data": {"type": "configuration-versions", "attributes": {"auto-queue-runs": "false"}}}
        response = requests.post(f'{self.api_url}/workspaces/{workspace_id}/configuration-versions',
                                 headers=self.headers, json=data)
        if response.status_code == 201:
            logging.info(f"Created configuration version.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful creating configuration version, response: {response.text}")
            exit(os.EX_CONFIG)

        upload_url = json.loads(response.text)['data']['attributes']['upload-url']
        return upload_url

    @staticmethod
    def upload_configuration_files(upload_url: str, configuration_path: str) -> int:
        """
        Uploads the configuration files. Auto-queue-runs is set to false when creating configuration version,
        so conf will not be queued automatically.
        :param upload_url: upload url, returned when creating configuration version
        :param configuration_path: path to tar.gz file containing config files (main.tf)
        :return: the response code
        """
        headers = {'Content-Type': 'application/octet-stream'}
        with open(configuration_path, 'rb') as configuration:
            response = requests.put(upload_url, headers=headers, data=configuration.read())
        if response.status_code == 200:
            logging.info(f"Uploaded configuration.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful uploading configuration, response: {response.text}")
            exit(os.EX_CONFIG)

        return response.status_code

    def create_run(self, workspace_id: str, target_addrs: str = None, message: str = "") -> str:
        """
        Creates a run, optionally targeted at a target address. If auto-apply is set to true the run will be applied
        afterwards as well.
        :param workspace_id: the workspace id
        :param target_addrs: the target address (id of the module/resource)
        :param message: additional message that will be displayed at the terraform cloud run
        :return: the run id
        """
        data = {"data": {"attributes": {"message": message},
                         "type": "runs",
                         "relationships": {"workspace": {"data": {"type": "workspaces", "id": workspace_id}}},
                         }
                }
        if target_addrs:
            data['data']['attributes']['target-addrs'] = [target_addrs]

        response = requests.post(f'{self.api_url}/runs', headers=self.headers, json=data)
        if response.status_code == 201:
            logging.info(f"Created run.")
            logging.debug(f"response: {response.text}")
        else:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful creating run, response: {response.text}")
            exit(os.EX_CONFIG)

        run_id = json.loads(response.text)['data']['id']
        return run_id

    def get_run_details(self, run_id: str) -> dict:
        """
        Get details on a run identified by its id.
        :param run_id: the run id
        :return: the response text
        """
        response = requests.get(f'{self.api_url}/runs/{run_id}', headers=self.headers)
        if not response.status_code == 200:
            logging.error(f"Response status: {response.status_code}")
            logging.error(f"Unsuccessful retrieving run details, response: {response.text}")
            exit(os.EX_CONFIG)

        return json.loads(response.text)

    @staticmethod
    def create_var_attributes(key: str, value: str, category: str = 'terraform', description: str = '', hcl: bool = False,
                              sensitive: bool = False) -> dict:
        """
        Creates attributes dictionary for a variable.
        :param key: Key of variable
        :param value: Value of variable
        :param description: Description of variable
        :param hcl: Whether the value is hcl syntax
        :param sensitive: Whether the value is sensitive
        :param category: The category of the value ('terraform' or 'env')
        :return: attributes dict
        """

        attributes = {
            "key": key,
            "value": value,
            "description": description,
            "hcl": str(hcl).lower(),
            "sensitive": str(sensitive).lower()
        }

        if category != 'env' and category != 'terraform':
            print('Category has to be either "env" or "terraform"')
            exit(os.EX_CONFIG)
        else:
            attributes["category"] = category
        return attributes

    def plan_variable_changes(self, new_vars: list, workspace_id: str) -> Tuple[list, list, list, list]:
        """
        Compares the current variables in the workspace with a list of new variables. It sorts the new variables in
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
        add = []
        edit = []
        unchanged = []
        delete = []

        old_vars = self.list_workspace_variables(workspace_id)
        old_var_ids = {}
        # create dict with var name as key and tuple of (var id, value) as value
        for old_var in old_vars:
            attributes = old_var['attributes']
            existing_key = attributes['key']
            value = 'sensitive' if old_var['attributes']['sensitive'] else old_var['attributes']['value']
            old_var_ids[existing_key] = (old_var['id'], value)

        for var in new_vars:
            # check if variable already exists
            if var['key'] in old_var_ids.keys():
                # check if values of old and new are the same
                if var['value'] == old_var_ids[var['key']][1]:
                    unchanged.append(var)
                else:
                    edit.append((var, old_var_ids[var['key']][0], old_var_ids[var['key']][1]))
            else:
                add.append(var)

        # compare old variables with new variables and check which are 'extra' in the old vars
        deleted_vars = set([old_var['attributes']['key'] for old_var in old_vars]) - set(
            [new_var['key'] for new_var in new_vars])
        for var_key in deleted_vars:
            delete.append((var_key, old_var_ids[var_key][0], old_var_ids[var_key][1]))

        return add, edit, unchanged, delete

    def update_workspace_variables(self, add: list, edit: list, delete: list, workspace_id: str):
        """
        Update workspace accordingly to the planned changes
        :param add: list of attributes dicts of new variables
        :param edit: list of tuples with (attributes dict, var id, old value) of existing variables that will be updated
        :param delete: list of tuples with (var key, var id, old value) of variables that will be deleted
        :param workspace_id: the workspace id
        :return: None
        """
        for variable in add:
            self.add_workspace_variable(variable, workspace_id)
        for variable in edit:
            self.update_workspace_variable(variable[0], variable[1], workspace_id)
        for variable in delete:
            self.delete_workspace_variable(variable[1], workspace_id)
