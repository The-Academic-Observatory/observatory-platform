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
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from academic_observatory.telescopes.mag import MagTelescope

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 5, 28)
}

with DAG(dag_id=MagTelescope.DAG_ID, schedule_interval="@daily", default_args=default_args) as dag:
    # List all MAG releases that were processed in a given interval
    task_list = BranchPythonOperator(
        task_id=MagTelescope.TASK_ID_LIST,
        provide_context=True,
        python_callable=MagTelescope.list_releases,
        dag=dag,
        depends_on_past=False
    )

    # Skip all other tasks if there are no releases to process
    task_stop = DummyOperator(task_id=MagTelescope.TASK_ID_STOP)

    # Transfer all MAG releases to Google Cloud storage that were processed in the given interval
    task_transfer = PythonOperator(
        task_id=MagTelescope.TASK_ID_TRANSFER,
        provide_context=True,
        python_callable=MagTelescope.transfer,
        dag=dag,
        depends_on_past=False
    )

    # Download all MAG releases for a given interval
    task_download = PythonOperator(
        task_id=MagTelescope.TASK_ID_DOWNLOAD,
        provide_context=True,
        python_callable=MagTelescope.download,
        dag=dag,
        depends_on_past=False
    )

    # Transform all MAG releases for a given interval
    task_transform = PythonOperator(
        task_id=MagTelescope.TASK_ID_TRANSFORM,
        provide_context=True,
        python_callable=MagTelescope.transform,
        dag=dag,
        depends_on_past=False
    )

    # Upload all transformed MAG releases for a given interval to Google Cloud
    task_upload = PythonOperator(
        task_id=MagTelescope.TASK_ID_UPLOAD,
        provide_context=True,
        python_callable=MagTelescope.upload,
        dag=dag,
        depends_on_past=False
    )

    # Load all MAG releases for a given interval to BigQuery
    task_db_load = PythonOperator(
        task_id=MagTelescope.TASK_ID_DB_LOAD,
        provide_context=True,
        python_callable=MagTelescope.db_load,
        dag=dag,
        depends_on_past=True
    )

    task_list >> [task_transfer, task_stop]
    task_transfer >> task_download >> task_transform >> task_upload >> task_db_load
