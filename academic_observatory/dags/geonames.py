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

from academic_observatory.telescopes.geonames import GeonamesTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 6, 1)
}

with DAG(dag_id="geonames", schedule_interval="@once", default_args=default_args) as dag:
    # Get config variables
    check_setup = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_SETUP,
        python_callable=GeonamesTelescope.check_setup_requirements,
        provide_context=True
    )
    # Downloads snapshot from url
    download_local = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_DOWNLOAD,
        python_callable=GeonamesTelescope.download,
        provide_context=True
    )
    # Transforms download
    transform = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_TRANSFORM,
        python_callable=GeonamesTelescope.transform,
        provide_context=True
    )
    # Upload download to gcs bucket
    upload_to_gcs = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_UPLOAD,
        python_callable=GeonamesTelescope.upload_to_gcs,
        provide_context=True
    )
    # Upload download to bigquery table
    load_to_bq = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_BQ_LOAD,
        python_callable=GeonamesTelescope.load_to_bq,
        provide_context=True
    )
    # Delete locally stored files
    cleanup_local = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_CLEANUP,
        python_callable=GeonamesTelescope.cleanup_releases,
        provide_context=True
    )

    check_setup >> download_local >> transform >> upload_to_gcs >> load_to_bq >> cleanup_local
