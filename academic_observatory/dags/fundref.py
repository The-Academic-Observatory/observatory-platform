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

# Author: Aniek Roelofs, Jamie Diprose

from datetime import datetime

from airflow import DAG
from airflow.api.common.experimental.pool import create_pool
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator

from academic_observatory.telescopes.fundref import FundrefTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2014, 3, 1)
}

with DAG(dag_id="fundref", schedule_interval="@weekly", default_args=default_args) as dag:
    # Create Gitlab pool to limit the number of connections to Gitlab, which is very quick to block requests if there
    # are too many at once.
    pool_name = 'gitlab_pool'
    num_slots = 2
    description = 'A pool to limit the connections to Gitlab.'
    create_pool(pool_name, num_slots, description)

    # Check that dependencies exist before starting
    check = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=FundrefTelescope.check_dependencies,
        provide_context=True,
        queue=FundrefTelescope.QUEUE
    )

    # List of all releases for last month
    list_releases = ShortCircuitOperator(
        task_id=FundrefTelescope.TASK_ID_LIST,
        python_callable=FundrefTelescope.list_releases,
        provide_context=True,
        queue=FundrefTelescope.QUEUE,
        pool=pool_name
    )

    # Downloads the release
    download = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_DOWNLOAD,
        python_callable=FundrefTelescope.download,
        provide_context=True,
        queue=FundrefTelescope.QUEUE,
        pool=pool_name,
        retries=FundrefTelescope.RETRIES
    )

    # Upload downloaded data for a given interval
    upload_downloaded = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        provide_context=True,
        python_callable=FundrefTelescope.upload_downloaded,
        queue=FundrefTelescope.QUEUE,
        retries=FundrefTelescope.RETRIES
    )

    # Decompresses download
    extract = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_EXTRACT,
        python_callable=FundrefTelescope.extract,
        provide_context=True,
        queue=FundrefTelescope.QUEUE
    )

    # Transforms download
    transform = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_TRANSFORM,
        python_callable=FundrefTelescope.transform,
        provide_context=True,
        queue=FundrefTelescope.QUEUE
    )

    # Upload transformed data to gcs bucket
    upload_transformed = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=FundrefTelescope.upload_transformed,
        provide_context=True,
        queue=FundrefTelescope.QUEUE,
        retries=FundrefTelescope.RETRIES
    )

    # Upload download to bigquery table
    bq_load = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_BQ_LOAD,
        python_callable=FundrefTelescope.bq_load,
        provide_context=True,
        queue=FundrefTelescope.QUEUE
    )

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_CLEANUP,
        python_callable=FundrefTelescope.cleanup,
        provide_context=True,
        queue=FundrefTelescope.QUEUE
    )

    # Task dependencies
    check >> list_releases >> download >> upload_downloaded >> extract >> transform >> upload_transformed >> bq_load >> cleanup
