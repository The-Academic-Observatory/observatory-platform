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
    check = PythonOperator(
        task_id=GridTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=GridTelescope.check_dependencies,
        provide_context=True,
        queue=GridTelescope.QUEUE
    )

    # List releases and skip all subsequent tasks if there is no release to process
    list_releases = ShortCircuitOperator(
        task_id=GridTelescope.TASK_ID_LIST,
        python_callable=GridTelescope.list_releases,
        provide_context=True,
        queue=GridTelescope.QUEUE
    )

    # Download the GRID releases for a given interval
    download = PythonOperator(
        task_id=GridTelescope.TASK_ID_DOWNLOAD,
        python_callable=GridTelescope.download,
        provide_context=True,
        queue=GridTelescope.QUEUE,
        retries=GridTelescope.RETRIES
    )

    # Upload the GRID releases for a given interval
    upload_downloaded = PythonOperator(
        task_id=GridTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=GridTelescope.upload_downloaded,
        provide_context=True,
        queue=GridTelescope.QUEUE,
        retries=GridTelescope.RETRIES
    )

    # Extract the GRID releases for a given interval
    extract = PythonOperator(
        task_id=GridTelescope.TASK_ID_EXTRACT,
        python_callable=GridTelescope.extract,
        provide_context=True,
        queue=GridTelescope.QUEUE
    )

    # Transform the GRID releases for a given interval
    transform = PythonOperator(
        task_id=GridTelescope.TASK_ID_TRANSFORM,
        python_callable=GridTelescope.transform,
        provide_context=True,
        queue=GridTelescope.QUEUE
    )

    # Upload the transformed GRID releases for a given interval to Google Cloud Storage
    upload_transformed = PythonOperator(
        task_id=GridTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=GridTelescope.upload_transformed,
        provide_context=True,
        queue=GridTelescope.QUEUE,
        retries=GridTelescope.RETRIES
    )

    # Load the transformed GRID releases for a given interval to BigQuery
    # Depends on past so that BigQuery load jobs are not all created at once
    bq_load = PythonOperator(
        task_id=GridTelescope.TASK_ID_BQ_LOAD,
        python_callable=GridTelescope.bq_load,
        provide_context=True,
        queue=GridTelescope.QUEUE
    )

    cleanup = PythonOperator(
        task_id=GridTelescope.TASK_ID_CLEANUP,
        python_callable=GridTelescope.cleanup,
        provide_context=True,
        queue=GridTelescope.QUEUE
    )

    # Task dependencies
    check >> list_releases >> download >> upload_downloaded >> extract >> transform >> upload_transformed >> bq_load >> cleanup
