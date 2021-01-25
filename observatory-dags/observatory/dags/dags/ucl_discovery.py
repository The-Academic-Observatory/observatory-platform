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

from observatory.dags.telescopes.ucl_discovery import UclDiscoveryTelescope

default_args = {
    "owner": "airflow",
    "start_date": datetime(2008, 1, 1)
}

with DAG(dag_id=UclDiscoveryTelescope.DAG_ID, schedule_interval="@monthly", catchup=True, default_args=default_args,
         max_active_runs=1) as dag:
    # Check that dependencies exist before starting
    check = PythonOperator(
        task_id=UclDiscoveryTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=UclDiscoveryTelescope.check_dependencies,
        queue=UclDiscoveryTelescope.QUEUE
    )

    # Downloads the events of this release
    download = ShortCircuitOperator(
        task_id=UclDiscoveryTelescope.TASK_ID_DOWNLOAD,
        python_callable=UclDiscoveryTelescope.download,
        provide_context=True,
        retries=UclDiscoveryTelescope.MAX_RETRIES,
        queue=UclDiscoveryTelescope.QUEUE,
    )

    # Upload downloaded events file to google cloud storage
    upload_downloaded = PythonOperator(
        task_id=UclDiscoveryTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        provide_context=True,
        python_callable=UclDiscoveryTelescope.upload_downloaded,
        queue=UclDiscoveryTelescope.QUEUE
    )

    # Upload this release as partition to separate BigQuery table
    bq_load_shard = PythonOperator(
        task_id=UclDiscoveryTelescope.TASK_ID_BQ_LOAD_SHARD,
        python_callable=UclDiscoveryTelescope.bq_load_shard,
        provide_context=True,
        queue=UclDiscoveryTelescope.QUEUE
    )

    # Delete events in main table which are edited/deleted in this release
    bq_delete_old = PythonOperator(
        task_id=UclDiscoveryTelescope.TASK_ID_BQ_DELETE_OLD,
        python_callable=UclDiscoveryTelescope.bq_delete_old,
        provide_context=True,
        queue=UclDiscoveryTelescope.QUEUE
    )

    # Append this release to main BigQuery table
    bq_append_new = PythonOperator(
        task_id=UclDiscoveryTelescope.TASK_ID_BQ_APPEND_NEW,
        python_callable=UclDiscoveryTelescope.bq_append_new,
        provide_context=True,
        queue=UclDiscoveryTelescope.QUEUE,
        wait_for_downstream=True
    )

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=UclDiscoveryTelescope.TASK_ID_CLEANUP,
        python_callable=UclDiscoveryTelescope.cleanup,
        provide_context=True,
        queue=UclDiscoveryTelescope.QUEUE
    )

    # Task dependencies
    check >> download >> upload_downloaded >> bq_load_shard >> bq_delete_old >> bq_append_new >> cleanup