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

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from academic_observatory.telescopes.terraform import TerraformWorkspace


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}


with DAG(dag_id=TerraformWorkspace.DAG_ID_ON, schedule_interval="@monthly", default_args=default_args, catchup=False) as dag:
    ws_id = PythonOperator(
        task_id=TerraformWorkspace.TASK_ID_WORKSPACE,
        python_callable=TerraformWorkspace.get_workspace_id,
        provide_context=True
    )

    var_on = PythonOperator(
        task_id=TerraformWorkspace.TASK_ID_VAR_ON,
        python_callable=TerraformWorkspace.update_status_variable_on,
        provide_context=True
    )

    run_terraform = PythonOperator(
        task_id=TerraformWorkspace.TASK_ID_RUN,
        python_callable=TerraformWorkspace.terraform_run,
        provide_context=True
    )

    ws_id >> var_on >> run_terraform
