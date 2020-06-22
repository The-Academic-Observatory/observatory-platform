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

from academic_observatory.telescopes.grid import GridTelescope

default_args = {
    "owner": "airflow",
    "start_date": datetime(2015, 9, 1)
}

with DAG(dag_id=GridTelescope.DAG_ID, schedule_interval="@monthly", default_args=default_args) as dag:
    # List all GRID releases for a given interval
    task_list = BranchPythonOperator(
        task_id=GridTelescope.TASK_ID_LIST,
        provide_context=True,
        python_callable=GridTelescope.list_releases,
        dag=dag,
        depends_on_past=False
    )

    task_stop = DummyOperator(task_id=GridTelescope.TASK_ID_STOP)

    # Download the GRID releases for a given interval
    task_download = PythonOperator(
        task_id=GridTelescope.TASK_ID_DOWNLOAD,
        provide_context=True,
        python_callable=GridTelescope.download,
        dag=dag,
        depends_on_past=False
    )

    # Extract the GRID releases for a given interval
    task_extract = PythonOperator(
        task_id=GridTelescope.TASK_ID_EXTRACT,
        provide_context=True,
        python_callable=GridTelescope.extract,
        dag=dag,
        depends_on_past=False
    )

    # Transform the GRID releases for a given interval
    task_transform = PythonOperator(
        task_id=GridTelescope.TASK_ID_TRANSFORM,
        provide_context=True,
        python_callable=GridTelescope.transform,
        dag=dag,
        depends_on_past=False
    )

    # Upload the transformed GRID releases for a given interval to Google Cloud Storage
    task_upload = PythonOperator(
        task_id=GridTelescope.TASK_ID_UPLOAD,
        provide_context=True,
        python_callable=GridTelescope.upload,
        dag=dag,
        depends_on_past=False
    )

    # Load the transformed GRID releases for a given interval to BigQuery
    # Depends on past so that BigQuery load jobs are not all created at once
    task_db_load = PythonOperator(
        task_id=GridTelescope.TASK_ID_DB_LOAD,
        provide_context=True,
        python_callable=GridTelescope.db_load,
        dag=dag,
        depends_on_past=True
    )

    # Task dependencies
    task_list >> [task_download, task_stop]
    task_download >> task_extract >> task_transform >> task_upload >> task_db_load
