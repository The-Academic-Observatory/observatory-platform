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
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from academic_observatory.telescopes.terraform import TerraformWorkspace


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}


with DAG(dag_id="dummy_telescope", schedule_interval="@monthly", default_args=default_args, catchup=False) as dag:
    trigger = TriggerDagRunOperator(
        task_id="trigger_dag",
        trigger_dag_id=TerraformWorkspace.DAG_ID_OFF,
        # conf={"message": dag.dag_id}  # will give NoneType AttributeError, related to sqlite backend?
    )
