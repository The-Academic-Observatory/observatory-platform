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

from observatory_platform.telescopes.orcid import OrcidTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 8, 30)
}

#TODO adjust settings
with DAG(dag_id="orcid", schedule_interval="@daily", default_args=default_args,
         max_active_runs=1) as dag:
    # Check that dependencies exist before starting
    check = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=OrcidTelescope.check_dependencies,
        queue=OrcidTelescope.QUEUE
    )

    # Downloads the releases
    download = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_DOWNLOAD,
        python_callable=OrcidTelescope.download,
        provide_context=True,
        queue=OrcidTelescope.QUEUE,
    )

    # Upload downloaded data for a given interval
    upload_downloaded = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        provide_context=True,
        python_callable=OrcidTelescope.upload_downloaded,
        queue=OrcidTelescope.QUEUE
    )

    # Transforms download
    transform = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_TRANSFORM,
        python_callable=OrcidTelescope.transform,
        provide_context=True,
        queue=OrcidTelescope.QUEUE
    )

    # Upload download to gcs bucket
    upload_transformed = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=OrcidTelescope.upload_transformed,
        provide_context=True,
        queue=OrcidTelescope.QUEUE
    )

    # Upload download to BigQuery table
    bq_load = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_BQ_LOAD,
        python_callable=OrcidTelescope.bq_load,
        provide_context=True,
        queue=OrcidTelescope.QUEUE
    )

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=OrcidTelescope.TASK_ID_CLEANUP,
        python_callable=OrcidTelescope.cleanup,
        provide_context=True,
        queue=OrcidTelescope.QUEUE
    )

    # Task dependencies
    check >> download >> upload_downloaded >> transform >> upload_transformed >> bq_load >> cleanup