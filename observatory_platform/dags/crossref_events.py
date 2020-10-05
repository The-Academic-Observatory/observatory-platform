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
from airflow.operators.python_operator import (PythonOperator,
                                               ShortCircuitOperator)

from observatory_platform.telescopes.crossref_events import CrossrefEventsTelescope

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}

with DAG(dag_id="crossref_events", schedule_interval="@weekly", catchup=False, default_args=default_args,
         max_active_runs=1) as dag:
    # Check that dependencies exist before starting
    check = PythonOperator(
        task_id=CrossrefEventsTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=CrossrefEventsTelescope.check_dependencies,
        queue=CrossrefEventsTelescope.QUEUE
    )

    # Downloads the releases
    download = ShortCircuitOperator(
        task_id=CrossrefEventsTelescope.TASK_ID_DOWNLOAD,
        python_callable=CrossrefEventsTelescope.download,
        provide_context=True,
        retries=CrossrefEventsTelescope.MAX_RETRIES,
        queue=CrossrefEventsTelescope.QUEUE,
    )

    # Upload downloaded data for a given interval
    upload_downloaded = PythonOperator(
        task_id=CrossrefEventsTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        provide_context=True,
        python_callable=CrossrefEventsTelescope.upload_downloaded,
        queue=CrossrefEventsTelescope.QUEUE
    )

    # Transforms download
    transform = PythonOperator(
        task_id=CrossrefEventsTelescope.TASK_ID_TRANSFORM,
        python_callable=CrossrefEventsTelescope.transform,
        provide_context=True,
        queue=CrossrefEventsTelescope.QUEUE
    )

    # Upload download to gcs bucket
    upload_transformed = PythonOperator(
        task_id=CrossrefEventsTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=CrossrefEventsTelescope.upload_transformed,
        provide_context=True,
        queue=CrossrefEventsTelescope.QUEUE
    )

    # Upload release as partition to separate BigQuery table
    bq_load_partition = PythonOperator(
        task_id=CrossrefEventsTelescope.TASK_ID_BQ_LOAD_PARTITION,
        python_callable=CrossrefEventsTelescope.bq_load_partition,
        provide_context=True,
        queue=CrossrefEventsTelescope.QUEUE
    )

    # Delete events in main table which are edited/deleted in this release
    bq_delete_old = PythonOperator(
        task_id=CrossrefEventsTelescope.TASK_ID_BQ_DELETE_OLD,
        python_callable=CrossrefEventsTelescope.bq_delete_old,
        provide_context=True,
        queue=CrossrefEventsTelescope.QUEUE
    )

    # Append release to main BigQuery table
    bq_append_new = PythonOperator(
        task_id=CrossrefEventsTelescope.TASK_ID_BQ_APPEND_NEW,
        python_callable=CrossrefEventsTelescope.bq_append_new,
        provide_context=True,
        queue=CrossrefEventsTelescope.QUEUE,
        wait_for_downstream=True
    )

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=CrossrefEventsTelescope.TASK_ID_CLEANUP,
        python_callable=CrossrefEventsTelescope.cleanup,
        provide_context=True,
        queue=CrossrefEventsTelescope.QUEUE
    )

    # Task dependencies
    check >> download >> upload_downloaded >> transform >> upload_transformed >> bq_load_partition >> bq_delete_old \
        >> bq_append_new >> cleanup
