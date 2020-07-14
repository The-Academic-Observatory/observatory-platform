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

from academic_observatory.telescopes.mag import MagTelescope

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 7, 1)
}

with DAG(dag_id=MagTelescope.DAG_ID, schedule_interval="@weekly", default_args=default_args) as dag:
    # Check that dependencies exist before starting
    task_check = PythonOperator(
        task_id=MagTelescope.TASK_ID_CHECK_DEPENDENCIES,
        provide_context=True,
        python_callable=MagTelescope.check_dependencies,
        dag=dag,
        depends_on_past=False,
        queue=MagTelescope.QUEUE
    )

    # List releases and skip all subsequent tasks if there is no release to process
    task_list = ShortCircuitOperator(
        task_id=MagTelescope.TASK_ID_LIST,
        provide_context=True,
        python_callable=MagTelescope.list_releases,
        dag=dag,
        queue=MagTelescope.QUEUE
    )

    # Transfer all MAG releases to Google Cloud storage that were processed in the given interval
    task_transfer = PythonOperator(
        task_id=MagTelescope.TASK_ID_TRANSFER,
        provide_context=True,
        python_callable=MagTelescope.transfer,
        dag=dag,
        queue=MagTelescope.QUEUE
    )

    # Download all MAG releases for a given interval
    task_download = PythonOperator(
        task_id=MagTelescope.TASK_ID_DOWNLOAD,
        provide_context=True,
        python_callable=MagTelescope.download,
        dag=dag,
        queue=MagTelescope.QUEUE
    )

    # Transform all MAG releases for a given interval
    task_transform = PythonOperator(
        task_id=MagTelescope.TASK_ID_TRANSFORM,
        provide_context=True,
        python_callable=MagTelescope.transform,
        dag=dag,
        queue=MagTelescope.QUEUE
    )

    # Upload all transformed MAG releases for a given interval to Google Cloud
    task_upload_transformed = PythonOperator(
        task_id=MagTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        provide_context=True,
        python_callable=MagTelescope.upload_transformed,
        dag=dag,
        queue=MagTelescope.QUEUE
    )

    # Load all MAG releases for a given interval to BigQuery
    task_bq_load = PythonOperator(
        task_id=MagTelescope.TASK_ID_BQ_LOAD,
        provide_context=True,
        python_callable=MagTelescope.bq_load,
        dag=dag,
        queue=MagTelescope.QUEUE
    )

    # Cleanup local files
    task_cleanup = PythonOperator(
        task_id=MagTelescope.TASK_ID_CLEANUP,
        provide_context=True,
        python_callable=MagTelescope.cleanup,
        dag=dag,
        queue=MagTelescope.QUEUE
    )

    task_check >> task_list >> task_transfer >> task_download >> task_transform >> task_upload_transformed >> task_bq_load >> task_cleanup
