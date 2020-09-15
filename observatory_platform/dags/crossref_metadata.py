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

"""
A DAG that harvests the Crossref Metadata Plus dataset: https://www.crossref.org/services/metadata-retrieval/metadata-plus/

Saved to the BigQuery table: <project_id>.crossref.crossref_metadataYYYYMMDD
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator

from observatory_platform.telescopes.crossref_metadata import CrossrefMetadataTelescope

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 6, 7)
}

with DAG(dag_id="crossref_metadata", schedule_interval="@weekly", default_args=default_args,
         max_active_runs=2) as dag:
    # Check that dependencies exist before starting
    check = PythonOperator(
        task_id=CrossrefMetadataTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=CrossrefMetadataTelescope.check_dependencies,
        provide_context=True,
        queue=CrossrefMetadataTelescope.QUEUE
    )

    # Check that the release for this month exists
    check_release_exists = ShortCircuitOperator(
        task_id=CrossrefMetadataTelescope.TASK_ID_CHECK_RELEASE,
        python_callable=CrossrefMetadataTelescope.check_release_exists,
        provide_context=True,
        queue=CrossrefMetadataTelescope.QUEUE
    )

    # Downloads the releases
    download = PythonOperator(
        task_id=CrossrefMetadataTelescope.TASK_ID_DOWNLOAD,
        python_callable=CrossrefMetadataTelescope.download,
        provide_context=True,
        queue=CrossrefMetadataTelescope.QUEUE
    )

    # Upload downloaded data for a given interval
    upload_downloaded = PythonOperator(
        task_id=CrossrefMetadataTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        provide_context=True,
        python_callable=CrossrefMetadataTelescope.upload_downloaded,
        queue=CrossrefMetadataTelescope.QUEUE
    )

    # Decompresses download
    extract = PythonOperator(
        task_id=CrossrefMetadataTelescope.TASK_ID_EXTRACT,
        python_callable=CrossrefMetadataTelescope.extract,
        provide_context=True,
        queue=CrossrefMetadataTelescope.QUEUE
    )

    # Transforms download
    transform = PythonOperator(
        task_id=CrossrefMetadataTelescope.TASK_ID_TRANSFORM,
        python_callable=CrossrefMetadataTelescope.transform,
        provide_context=True,
        queue=CrossrefMetadataTelescope.QUEUE
    )

    # Upload download to gcs bucket
    upload_transformed = PythonOperator(
        task_id=CrossrefMetadataTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=CrossrefMetadataTelescope.upload_transformed,
        provide_context=True,
        queue=CrossrefMetadataTelescope.QUEUE
    )

    # Upload download to BigQuery table
    bq_load = PythonOperator(
        task_id=CrossrefMetadataTelescope.TASK_ID_BQ_LOAD,
        python_callable=CrossrefMetadataTelescope.bq_load,
        provide_context=True,
        queue=CrossrefMetadataTelescope.QUEUE
    )

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=CrossrefMetadataTelescope.TASK_ID_CLEANUP,
        python_callable=CrossrefMetadataTelescope.cleanup,
        provide_context=True,
        queue=CrossrefMetadataTelescope.QUEUE
    )

    # Task dependencies
    check >> check_release_exists >> download >> upload_downloaded >> extract >> transform >> upload_transformed >> bq_load >> cleanup
