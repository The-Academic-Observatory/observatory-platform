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
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 8, 10)
}


with DAG(dag_id="dummy_telescope", schedule_interval="@daily", default_args=default_args, catchup=True) as dag:
    telescope = DummyOperator(
        task_id="telescope",
        queue='remote_queue'
    )

    telescope