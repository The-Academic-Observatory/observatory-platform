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

import pendulum
from airflow import DAG
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from observatory.platform.workflows.terraform import TerraformTasks

default_args = {"owner": "airflow", "start_date": pendulum.datetime(2020, 1, 1)}

with DAG(
    dag_id=TerraformTasks.DAG_ID_DESTROY_VM,
    schedule_interval="*/10 * * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:
    # Check that dependencies exist before starting
    check = PythonOperator(
        task_id=TerraformTasks.TASK_ID_CHECK_DEPENDENCIES, python_callable=TerraformTasks.check_dependencies
    )

    # If vm is already off, stop dag
    vm_status = ShortCircuitOperator(
        task_id=TerraformTasks.TASK_ID_VM_STATUS,
        python_callable=TerraformTasks.get_variable_create,
    )

    # Destroying vm if all dags (since vm_create) are successful
    check_dags_status = BranchPythonOperator(
        task_id=TerraformTasks.TASK_ID_DAG_SUCCESS,
        python_callable=TerraformTasks.check_success_dags,
    )

    # Update terraform variable vm_create to False
    var_destroy = PythonOperator(
        task_id=TerraformTasks.TASK_ID_VAR_UPDATE,
        python_callable=TerraformTasks.update_terraform_variable,
    )

    # Run terraform configuration
    run_terraform = PythonOperator(task_id=TerraformTasks.TASK_ID_RUN, python_callable=TerraformTasks.terraform_run)

    # Check status of terraform run
    check_run_status = PythonOperator(
        task_id=TerraformTasks.TASK_ID_RUN_STATUS,
        python_callable=TerraformTasks.check_terraform_run_status,
    )

    # Check how long VM has been turned on for
    check_vm_runtime = PythonOperator(
        task_id=TerraformTasks.TASK_ID_VM_RUNTIME, python_callable=TerraformTasks.check_runtime_vm
    )

    check >> vm_status >> check_dags_status >> [var_destroy, check_vm_runtime]
    var_destroy >> run_terraform >> check_run_status
