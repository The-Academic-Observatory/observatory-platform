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

from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}


def print_connection(**kwargs):
    # Get Azure connection information
    connection = BaseHook.get_connection("mag_snapshots_container")
    print(f'mag_snapshots_container.login: {connection.login}')

    connection = BaseHook.get_connection("mag_releases_table")
    print(f'mag_releases_table.login: {connection.login}')

    connection = BaseHook.get_connection("crossref")
    print(f'crossref.password: {connection.password}')

    var = Variable.get("download_bucket_name")
    print(f'download_bucket_name: {var}')

    var = Variable.get("transform_bucket_name")
    print(f'transform_bucket_name: {var}')


with DAG(dag_id="hello_world", schedule_interval="@once", default_args=default_args) as dag:
    # Download the GRID releases for a given interval
    task1 = PythonOperator(
        task_id='print_connection',
        provide_context=True,
        python_callable=print_connection,
        dag=dag,
        depends_on_past=False
    )
