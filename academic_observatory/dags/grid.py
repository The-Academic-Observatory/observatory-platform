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
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator

from academic_observatory.telescopes.grid import GridTelescope

default_args = {
    "owner": "airflow",
    "start_date": datetime(2015, 9, 1)
}

with DAG(dag_id=GridTelescope.DAG_ID, schedule_interval="@weekly", default_args=default_args) as dag:
    # Check that dependencies exist before starting
    task_check = PythonOperator(
        task_id=GridTelescope.TASK_ID_CHECK_DEPENDENCIES,
        provide_context=True,
        python_callable=GridTelescope.check_dependencies,
        dag=dag,
        queue=GridTelescope.QUEUE
    )

    # List releases and skip all subsequent tasks if there is no release to process
    task_list = ShortCircuitOperator(
        task_id=GridTelescope.TASK_ID_LIST,
        provide_context=True,
        python_callable=GridTelescope.list_releases,
        dag=dag,
        queue=GridTelescope.QUEUE
    )

    # Download the GRID releases for a given interval
    task_download = PythonOperator(
        task_id=GridTelescope.TASK_ID_DOWNLOAD,
        provide_context=True,
        python_callable=GridTelescope.download,
        dag=dag,
        queue=GridTelescope.QUEUE
    )

    # Upload the GRID releases for a given interval
    task_upload_downloaded = PythonOperator(
        task_id=GridTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        provide_context=True,
        python_callable=GridTelescope.upload_downloaded,
        dag=dag,
        queue=GridTelescope.QUEUE
    )

    # Extract the GRID releases for a given interval
    task_extract = PythonOperator(
        task_id=GridTelescope.TASK_ID_EXTRACT,
        provide_context=True,
        python_callable=GridTelescope.extract,
        dag=dag,
        queue=GridTelescope.QUEUE
    )

    # Transform the GRID releases for a given interval
    task_transform = PythonOperator(
        task_id=GridTelescope.TASK_ID_TRANSFORM,
        provide_context=True,
        python_callable=GridTelescope.transform,
        dag=dag,
        queue=GridTelescope.QUEUE
    )

    # Upload the transformed GRID releases for a given interval to Google Cloud Storage
    task_upload_transformed = PythonOperator(
        task_id=GridTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        provide_context=True,
        python_callable=GridTelescope.upload_transformed,
        dag=dag,
        queue=GridTelescope.QUEUE
    )

    # Load the transformed GRID releases for a given interval to BigQuery
    # Depends on past so that BigQuery load jobs are not all created at once
    task_bq_load = PythonOperator(
        task_id=GridTelescope.TASK_ID_BQ_LOAD,
        provide_context=True,
        python_callable=GridTelescope.db_load,
        dag=dag,
        queue=GridTelescope.QUEUE
    )

    task_cleanup = PythonOperator(
        task_id=GridTelescope.TASK_ID_CLEANUP,
        provide_context=True,
        python_callable=GridTelescope.cleanup,
        dag=dag,
        depends_on_past=True,
        queue=GridTelescope.QUEUE
    )

    # Task dependencies
    task_check >> task_list >> task_download >> task_upload_downloaded >> task_extract >> task_transform >> task_upload_transformed >> task_bq_load >> task_cleanup
