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
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator

from academic_observatory.telescopes.crossref_metadata import CrossrefMetaTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2018, 4, 1)
}

with DAG(dag_id="crossref_metadata", schedule_interval="@monthly", default_args=default_args) as dag:
    # Get config variables
    check_setup = PythonOperator(
        task_id=CrossrefMetaTelescope.TASK_ID_SETUP,
        python_callable=CrossrefMetaTelescope.check_setup_requirements,
        provide_context=True
    )

    # List of all releases for last month
    list_releases = BranchPythonOperator(
        task_id=CrossrefMetaTelescope.TASK_ID_LIST,
        python_callable=CrossrefMetaTelescope.list_releases_last_month,
        provide_context=True
    )

    stop_workflow = DummyOperator(task_id=CrossrefMetaTelescope.TASK_ID_STOP)

    # Downloads snapshot from url
    download_local = PythonOperator(
        task_id=CrossrefMetaTelescope.TASK_ID_DOWNLOAD,
        python_callable=CrossrefMetaTelescope.download,
        provide_context=True
    )

    # Decompresses download
    decompress = PythonOperator(
        task_id=CrossrefMetaTelescope.TASK_ID_DECOMPRESS,
        python_callable=CrossrefMetaTelescope.decompress,
        provide_context=True
    )

    # Transforms download
    transform = PythonOperator(
        task_id=CrossrefMetaTelescope.TASK_ID_TRANSFORM,
        python_callable=CrossrefMetaTelescope.transform,
        provide_context=True
    )

    # Upload download to gcs bucket
    upload_to_gcs = PythonOperator(
        task_id=CrossrefMetaTelescope.TASK_ID_UPLOAD,
        python_callable=CrossrefMetaTelescope.upload_to_gcs,
        provide_context=True

    )

    # Upload download to bigquery table
    load_to_bq = PythonOperator(
        task_id=CrossrefMetaTelescope.TASK_ID_BQ_LOAD,
        python_callable=CrossrefMetaTelescope.load_to_bq,
        provide_context=True
    )

    # Delete locally stored files
    cleanup_local = PythonOperator(
        task_id=CrossrefMetaTelescope.TASK_ID_CLEANUP,
        python_callable=CrossrefMetaTelescope.cleanup_releases,
        provide_context=True
    )

    check_setup >> [list_releases, stop_workflow]
    list_releases >> [download_local, stop_workflow]
    download_local >> decompress >> transform >> upload_to_gcs >> load_to_bq >> cleanup_local
