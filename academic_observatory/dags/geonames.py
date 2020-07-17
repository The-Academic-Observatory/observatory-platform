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

# Author: Aniek Roelofs, James Diprose

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from academic_observatory.telescopes.geonames import GeonamesTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 6, 1)
}

with DAG(dag_id="geonames", schedule_interval="@monthly", default_args=default_args, catchup=False) as dag:
    # Get config variables
    check = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=GeonamesTelescope.check_dependencies,
        provide_context=True,
        queue=GeonamesTelescope.QUEUE
    )

    # Downloads snapshot from url
    download = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_DOWNLOAD,
        python_callable=GeonamesTelescope.download,
        provide_context=True,
        queue=GeonamesTelescope.QUEUE
    )

    # Upload downloaded files to gcs bucket
    upload_downloaded = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=GeonamesTelescope.upload_downloaded,
        provide_context=True,
        queue=GeonamesTelescope.QUEUE
    )

    # Extract downloaded file
    extract = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_EXTRACT,
        python_callable=GeonamesTelescope.extract,
        provide_context=True,
        queue=GeonamesTelescope.QUEUE
    )

    # Transform downloaded file
    transform = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_TRANSFORM,
        python_callable=GeonamesTelescope.transform,
        provide_context=True,
        queue=GeonamesTelescope.QUEUE
    )

    # Upload download to gcs bucket
    upload_transformed = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=GeonamesTelescope.upload_transformed,
        provide_context=True,
        queue=GeonamesTelescope.QUEUE
    )

    # Upload download to bigquery table
    load_to_bq = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_BQ_LOAD,
        python_callable=GeonamesTelescope.load_to_bq,
        provide_context=True,
        queue=GeonamesTelescope.QUEUE
    )

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=GeonamesTelescope.TASK_ID_CLEANUP,
        python_callable=GeonamesTelescope.cleanup,
        provide_context=True,
        queue=GeonamesTelescope.QUEUE
    )

    check >> download >> upload_downloaded >> extract >> transform >> upload_transformed >> load_to_bq >> cleanup
