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
from airflow.hooks.base_hook import BaseHook
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun


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

    def create_run(self, workspace_id, target_addrs=None):
        data = {"data": {"attributes": {"message": f"Triggered from airflow DAG at {datetime.now()}"},
                         "type": "runs",
                         "relationships": {"workspace": {"data": {"type": "workspaces", "id": workspace_id}}},
                         }
                }
        if target_addrs:
            data['data']['attributes']['target-addrs'] = [target_addrs]

        response = requests.post(f'{self.api_url}/runs', headers=self.headers, json=data)
        if response.status_code == 201:
            logging.info(f"Successfully created run, response: {response.text}")
        else:
            logging.info(f"Response status: {response.status_code}")
            logging.info(f"Unsuccessful creating run, response: {response.text}")
            exit(os.EX_CONFIG)

        run_id = json.loads(response.text)['data']['id']
        return run_id


class TerraformWorkspace:
    # Terraform Cloud variables
    organisation = 'coki'
    prefix = 'observatory-'
    suffix = 'dev'  # either 'dev', 'staging' or 'prod'
    workspace = prefix + suffix

    # DAG variables
    DAG_ID_ON = "vm_on"
    DAG_ID_OFF = "vm_off"

    XCOM_WORKSPACE_ID = "workspace_id"
    XCOM_START_TIME_VM = "start_time_vm"
    CONN_TERRAFORM = "terraform"

    TASK_ID_WORKSPACE = "get_workspace_id"
    TASK_ID_VAR_ON = "update_vm_status_on"
    TASK_ID_VAR_OFF = "update_vm_status_off"
    TASK_ID_RUN = "terraform_run_configuration"
    TASK_ID_DAG_SUCCESS = "check_all_dags_success"
    TASK_ID_STOP = "stop_workflow"

    @staticmethod
    def get_workspace_id(**kwargs):
        token = BaseHook.get_connection(TerraformWorkspace.CONN_TERRAFORM).password
        terraform_api = TerraformApi(token)
        workspace_id = terraform_api.workspace_id(TerraformWorkspace.organisation, TerraformWorkspace.workspace)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(TerraformWorkspace.XCOM_WORKSPACE_ID, workspace_id)

    @staticmethod
    def update_status_variable_on(**kwargs):
        # Pull messages
        ti: TaskInstance = kwargs['ti']
        workspace_id = ti.xcom_pull(key=TerraformWorkspace.XCOM_WORKSPACE_ID,
                                    task_ids=TerraformWorkspace.TASK_ID_WORKSPACE)
        token = BaseHook.get_connection(TerraformWorkspace.CONN_TERRAFORM).password
        terraform_api = TerraformApi(token)
        terraform_api.update_workspace_variable('vm-status', 'on', workspace_id)

    @staticmethod
    def update_status_variable_off(**kwargs):
        # Pull messages
        ti: TaskInstance = kwargs['ti']
        workspace_id = ti.xcom_pull(key=TerraformWorkspace.XCOM_WORKSPACE_ID,
                                    task_ids=TerraformWorkspace.TASK_ID_WORKSPACE)
        token = BaseHook.get_connection(TerraformWorkspace.CONN_TERRAFORM).password
        terraform_api = TerraformApi(token)
        terraform_api.update_workspace_variable('vm-status', 'off', workspace_id)

    @staticmethod
    def terraform_run(**kwargs):
        # Pull messages
        ti: TaskInstance = kwargs['ti']
        workspace_id = ti.xcom_pull(key=TerraformWorkspace.XCOM_WORKSPACE_ID,
                                    task_ids=TerraformWorkspace.TASK_ID_WORKSPACE)

        if kwargs["dag_run"].dag_id == TerraformWorkspace.DAG_ID_ON:
            ti.xcom_push(TerraformWorkspace.XCOM_START_TIME_VM, kwargs["dag_run"].start_date)
        token = BaseHook.get_connection(TerraformWorkspace.CONN_TERRAFORM).password
        terraform_api = TerraformApi(token)
        # target doesn't work the first time, if the worker vm hasn't been created yet
        # target_addrs = f"google_compute_instance.airflow-worker-{TerraformWorkspace.suffix}"
        # run_id = terraform_api.create_run(workspace_id, target_addrs)
        run_id = terraform_api.create_run(workspace_id)
        logging.info(run_id)

    @staticmethod
    def check_success_dags(**kwargs):
        turn_vm_off = True

        ti: TaskInstance = kwargs['ti']
        start_time_vm = ti.xcom_pull(key=TerraformWorkspace.XCOM_START_TIME_VM, task_ids=TerraformWorkspace.TASK_ID_RUN,
                                     dag_id=TerraformWorkspace.DAG_ID_ON, include_prior_dates=True)
        logging.info(f'Datetime when vm was turned on: {start_time_vm}')
        dags = DagRun.get_latest_runs()
        for dag in dags:
            all_dag_runs = DagRun.find(dag_id=dag.dag_id)
            for dag_run in all_dag_runs:
                # ignore dags that were started earlier, before the last time that the VM was turned on
                if dag_run.start_date < start_time_vm:
                    continue
                # ignore current running dag
                elif dag_run.dag_id == kwargs["dag_run"].dag_id:
                    continue
                # if not all dags are successful, the VM should remain on (?)
                if dag_run.state != 'success':
                    turn_vm_off = False
                logging.info(f'id: {dag_run.dag_id}, start date: {dag_run.start_date}, state: {dag_run.state}')

        logging.info(f'Turning VM off: {turn_vm_off}')
        return TerraformWorkspace.TASK_ID_WORKSPACE if turn_vm_off else TerraformWorkspace.TASK_ID_STOP
