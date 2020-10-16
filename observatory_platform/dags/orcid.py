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
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from observatory_platform.telescopes.orcid import OrcidTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 8, 30)
}

#TODO adjust settings
with DAG(dag_id="orcid", schedule_interval="@daily", default_args=default_args, catchup=False,
         max_active_runs=1) as dag:
    # Check that dependencies exist before starting
    check = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=OrcidTelescope.check_dependencies,
        queue=OrcidTelescope.QUEUE)

    # Transfer records from s3 bucket to google cloud bucket
    transfer = ShortCircuitOperator(
        task_id=OrcidTelescope.TASK_ID_TRANSFER,
        python_callable=OrcidTelescope.transfer,
        provide_context=True,
        queue=OrcidTelescope.QUEUE
    )

    # Downloads the events of this release
    download = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_DOWNLOAD,
        python_callable=OrcidTelescope.download,
        provide_context=True,
        queue=OrcidTelescope.QUEUE, )

    # Transform events file
    transform = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_TRANSFORM,
        python_callable=OrcidTelescope.transform,
        provide_context=True,
        queue=OrcidTelescope.QUEUE)

    # Upload transformed events file to google cloud storage
    upload_transformed = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=OrcidTelescope.upload_transformed,
        provide_context=True,
        queue=OrcidTelescope.QUEUE)

    # Upload this release as partition to separate BigQuery table
    bq_load_partition = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_BQ_LOAD_PARTITION,
        python_callable=OrcidTelescope.bq_load_partition,
        provide_context=True,
        queue=OrcidTelescope.QUEUE)

    # Delete events in main table which are edited/deleted in this release
    bq_delete_old = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_BQ_DELETE_OLD,
        python_callable=OrcidTelescope.bq_delete_old,
        provide_context=True,
        queue=OrcidTelescope.QUEUE,
        trigger_rule='none_failed')

    # Append this release to main BigQuery table
    bq_append_new = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_BQ_APPEND_NEW,
        python_callable=OrcidTelescope.bq_append_new,
        provide_context=True,
        queue=OrcidTelescope.QUEUE,
        wait_for_downstream=True,
        trigger_rule='none_failed')

    # # Delete locally stored files
    # cleanup = PythonOperator(
    #     task_id=OrcidTelescope.TASK_ID_CLEANUP,
    #     python_callable=OrcidTelescope.cleanup,
    #     provide_context=True,
    #     queue=OrcidTelescope.QUEUE,
    #     trigger_rule='none_failed')

    # Task dependencies
    check >> transfer >> download >> transform >> upload_transformed >> bq_load_partition >> bq_delete_old >> \
    bq_append_new
    # bq_append_new >> cleanup
