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

# Author: James Diprose


import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

with DAG(dag_id="hello_world", default_args=default_args, schedule_interval="@once") as dag:
    t1 = BashOperator(
        task_id="t1",
        bash_command="echo 'hello'"
    )
    t2 = BashOperator(
        task_id="t2",
        bash_command="echo 'world'"
    )
    t1 >> t2
