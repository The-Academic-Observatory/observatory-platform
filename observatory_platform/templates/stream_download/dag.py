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
from airflow.operators.python_operator import ShortCircuitOperator

from observatory_platform.templates.stream_download.telescope import ExampleTelescope

default_args = {
    "owner": "airflow",
    "start_date": datetime(2012, 1, 1)
}

with DAG(dag_id=ExampleTelescope.telescope.dag_id, schedule_interval="@weekly", default_args=default_args,
         catchup=False, max_active_runs=1) as dag:
    # Check that variables exist
    check = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=ExampleTelescope.check_dependencies,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue
    )

    # Create a release instance and pass on with xcom
    create_release = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_CREATE_RELEASE,
        python_callable=ExampleTelescope.create_release,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue
    )

    # Download data
    download = ShortCircuitOperator(
        task_id=ExampleTelescope.TASK_ID_DOWNLOAD,
        python_callable=ExampleTelescope.download,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        retries=ExampleTelescope.telescope.max_retries
    )

    # Upload downloaded data to bucket
    upload_downloaded = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=ExampleTelescope.upload_downloaded,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        retries=ExampleTelescope.telescope.max_retries
    )

    # Transform data
    transform = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_TRANSFORM,
        python_callable=ExampleTelescope.transform,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue
    )

    # Upload transformed data to bucket
    upload_transformed = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=ExampleTelescope.upload_transformed,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        retries=ExampleTelescope.telescope.max_retries
    )

    # Upload this release as partition to BigQuery table
    bq_load_partition = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_BQ_LOAD_PARTITION,
        python_callable=ExampleTelescope.bq_load_partition,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue)

    # Delete rows in main table which are in this release
    bq_delete_old = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_BQ_DELETE_OLD,
        python_callable=ExampleTelescope.bq_delete_old,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        trigger_rule='none_failed')

    # Append the updated rows from partition to main table
    bq_append_new = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_BQ_APPEND_NEW,
        python_callable=ExampleTelescope.bq_append_new,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        wait_for_downstream=True,
        trigger_rule='none_failed')

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_CLEANUP,
        python_callable=ExampleTelescope.cleanup,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        trigger_rule='none_failed'
    )

    check >> create_release >> download >> upload_downloaded >> transform >> upload_transformed >> \
    bq_load_partition >> bq_delete_old >> bq_append_new >> cleanup
