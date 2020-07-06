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
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}


class TerraformApi:
    def __init__(self, token):
        self.token = token
        self.api_url = 'https://app.terraform.io/api/v2'
        self.headers = {
            'Content-Type': 'application/vnd.api+json',
            'Authorization': f'Bearer {token}'
        }

    def workspace_id(self, organisation, workspace):
        response = requests.get(
            f'{self.api_url}/organizations/{organisation}/workspaces/{workspace}',
            headers=self.headers)
        if response.status_code == 200:
            logging.info(f"Successfully retrieved workspace id, response: {response.text}")
        else:
            logging.info(f"Response status: {response.status_code}")
            logging.info(f"Unsuccessful retrieving workspace id, response: {response.text}")

        workspace_id = json.loads(response.text)['data']['id']
        return workspace_id

    def list_workspace_variables(self, workspace_id):
        response = requests.get(f'{self.api_url}/workspaces/{workspace_id}/vars', headers=self.headers)

        if response.status_code == 200:
            logging.info(f"Successfully retrieved workspace variables, response: {response.text}")
        else:
            logging.info(f"Response status: {response.status_code}")
            logging.info(f"Unsuccessful retrieving workspace variables, response: {response.text}")
            exit(os.EX_CONFIG)

        workspace_vars = json.loads(response.text)['data']
        return workspace_vars

    def update_workspace_variable(self, key, value, workspace_id):
        vars = self.list_workspace_variables(workspace_id)
        var_id = None
        for var in vars:
            attributes = var['attributes']
            var_key = attributes['key']
            if var_key == key:
                var_id = var["id"]
        if var_id is None:
            print(f"Variable '{key}' does not exist yet, create variable first.")
            return

        attributes = {"key": key, "value": value}
        data = {"data": {"type": "vars", "id": var_id, "attributes": attributes}}
        response = requests.patch(f'{self.api_url}/workspaces/{workspace_id}/vars/{var_id}',
                                  headers=self.headers, json=data)
        if response.status_code == 200:
            logging.info(f"Successfully updated variable {var_id}, response: {response.text}")
        else:
            logging.info(f"Response status: {response.status_code}")
            logging.info(f"Unsuccessful updating variable {var_id}, response: {response.text}")
            exit(os.EX_CONFIG)

        return json.loads(response.text)

    def create_run(self, workspace_id):
        data = {"data": {"attributes": {"message": f"Triggered from airflow DAG at {datetime.now()}"},
                         "type": "runs",
                         "relationships": {"workspace": {"data": {"type": "workspaces", "id": workspace_id}}}
                         }
                }
        response = requests.post(f'{self.api_url}/runs', headers=self.headers, json=data)
        if response.status_code == 201:
            logging.info(f"Successfully created run, response: {response.text}")
        else:
            logging.info(f"Response status: {response.status_code}")
            logging.info(f"Unsuccessful creating run, response: {response.text}")
            exit(os.EX_CONFIG)

        run_id = json.loads(response.text)['data']['id']
        return run_id


def update_status_variable():
    token = Variable.get('TERRAFORM_TOKEN')
    terraform_api = TerraformApi(token)
    workspace_id = terraform_api.workspace_id('coki', 'ao-vm-dev')
    terraform_api.update_workspace_variable('vm-status', 'on', workspace_id)


def turn_vm_on():
    token = Variable.get('TERRAFORM_TOKEN')
    terraform_api = TerraformApi(token)
    workspace_id = terraform_api.workspace_id('coki', 'ao-vm-dev')
    run_id = terraform_api.create_run(workspace_id)
    logging.info(run_id)


with DAG(dag_id="terraform", schedule_interval="@once", default_args=default_args, catchup=False) as dag:
    update_variable = PythonOperator(
        task_id="update_variable",
        python_callable=update_status_variable
    )
    vm_on = PythonOperator(
        task_id="turn_vm_on",
        python_callable=turn_vm_on
    )
    update_variable >> vm_on
