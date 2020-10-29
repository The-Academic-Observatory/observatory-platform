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

"""

"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator

from observatory_platform.telescopes.doab import DoabTelescope

default_args = {
    "owner": "airflow",
    "start_date": datetime(2012, 1, 1)
}

with DAG(dag_id=DoabTelescope.telescope.dag_id, schedule_interval="@weekly", default_args=default_args, catchup=False,
         max_active_runs=1) as dag:
    check = PythonOperator(
        task_id=DoabTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=DoabTelescope.check_dependencies,
        provide_context=True,
        queue=DoabTelescope.telescope.queue
    )

    create_release = PythonOperator(
        task_id=DoabTelescope.TASK_ID_CREATE_RELEASE,
        python_callable=DoabTelescope.create_release,
        provide_context=True,
        queue=DoabTelescope.telescope.queue
    )

    # Downloads snapshot from url
    download = ShortCircuitOperator(
        task_id=DoabTelescope.TASK_ID_DOWNLOAD,
        python_callable=DoabTelescope.download,
        provide_context=True,
        queue=DoabTelescope.telescope.queue,
        retries=DoabTelescope.telescope.max_retries
    )

    # Upload downloaded files for safekeeping
    upload_downloaded = PythonOperator(
        task_id=DoabTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=DoabTelescope.upload_downloaded,
        provide_context=True,
        queue=DoabTelescope.telescope.queue,
        retries=DoabTelescope.telescope.max_retries
    )

    # Transforms download
    transform = PythonOperator(
        task_id=DoabTelescope.TASK_ID_TRANSFORM,
        python_callable=DoabTelescope.transform,
        provide_context=True,
        queue=DoabTelescope.telescope.queue
    )

    # Upload download to gcs bucket
    upload_transformed = PythonOperator(
        task_id=DoabTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=DoabTelescope.upload_transformed,
        provide_context=True,
        queue=DoabTelescope.telescope.queue,
        retries=DoabTelescope.telescope.max_retries
    )

    # Upload this release as shard to separate BigQuery table
    bq_load_partition = PythonOperator(
        task_id=DoabTelescope.TASK_ID_BQ_LOAD_PARTITION,
        python_callable=DoabTelescope.bq_load_partition,
        provide_context=True,
        queue=DoabTelescope.telescope.queue)

    # Delete events in main table which are edited/deleted in this release
    bq_delete_old = PythonOperator(
        task_id=DoabTelescope.TASK_ID_BQ_DELETE_OLD,
        python_callable=DoabTelescope.bq_delete_old,
        provide_context=True,
        queue=DoabTelescope.telescope.queue,
        trigger_rule='none_failed')

    # Append this release to main BigQuery table
    bq_append_new = PythonOperator(
        task_id=DoabTelescope.TASK_ID_BQ_APPEND_NEW,
        python_callable=DoabTelescope.bq_append_new,
        provide_context=True,
        queue=DoabTelescope.telescope.queue,
        wait_for_downstream=True,
        trigger_rule='none_failed')

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=DoabTelescope.TASK_ID_CLEANUP,
        python_callable=DoabTelescope.cleanup,
        provide_context=True,
        queue=DoabTelescope.telescope.queue,
        trigger_rule='none_failed'
    )

    check >> create_release >> download >> upload_downloaded >> transform >> upload_transformed >> \
    bq_load_partition >> bq_delete_old >> bq_append_new >> cleanup
