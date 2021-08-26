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

"""
A DAG that harvests the OpenCitations Indexes: http://opencitations.net/

Saved to the BigQuery table: <project_id>.open_citations.open_citationsYYYYMMDD
"""

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from academic_observatory_workflows.workflows.open_citations_telescope import OpenCitationsTelescope

default_args = {"owner": "airflow", "start_date": pendulum.datetime(2018, 7, 1)}

with DAG(dag_id=OpenCitationsTelescope.DAG_ID, schedule_interval="@weekly", default_args=default_args) as dag:
    # Check that dependencies exist before starting
    check_task = PythonOperator(
        task_id=OpenCitationsTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=OpenCitationsTelescope.check_dependencies,
        queue=OpenCitationsTelescope.QUEUE,
    )

    # List releases and skip all subsequent tasks if there is no release to process
    list_releases_task = ShortCircuitOperator(
        task_id=OpenCitationsTelescope.TASK_ID_LIST_RELEASES,
        python_callable=OpenCitationsTelescope.list_releases,
        queue=OpenCitationsTelescope.QUEUE,
    )

    # Download the Open Citations releases for a given interval
    download_task = PythonOperator(
        task_id=OpenCitationsTelescope.TASK_ID_DOWNLOAD,
        python_callable=OpenCitationsTelescope.download,
        queue=OpenCitationsTelescope.QUEUE,
        retries=OpenCitationsTelescope.RETRIES,
    )

    # Upload the Open Citations releases for a given interval
    upload_downloaded = PythonOperator(
        task_id=OpenCitationsTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=OpenCitationsTelescope.upload_downloaded,
        queue=OpenCitationsTelescope.QUEUE,
        retries=OpenCitationsTelescope.RETRIES,
    )

    # Extract the Open Citations releases for a given interval
    extract_task = PythonOperator(
        task_id=OpenCitationsTelescope.TASK_ID_EXTRACT,
        python_callable=OpenCitationsTelescope.extract,
        queue=OpenCitationsTelescope.QUEUE,
    )

    # Upload the transformed Open Citations releases for a given interval to Google Cloud Storage
    upload_extracted_task = PythonOperator(
        task_id=OpenCitationsTelescope.TASK_ID_UPLOAD_EXTRACTED,
        python_callable=OpenCitationsTelescope.upload_extracted,
        queue=OpenCitationsTelescope.QUEUE,
        retries=OpenCitationsTelescope.RETRIES,
    )

    # Load the transformed Open Citations releases for a given interval to BigQuery
    bq_load_task = PythonOperator(
        task_id=OpenCitationsTelescope.TASK_ID_BQ_LOAD,
        python_callable=OpenCitationsTelescope.load_to_bq,
        queue=OpenCitationsTelescope.QUEUE,
    )

    # Delete all files
    cleanup_task = PythonOperator(
        task_id=OpenCitationsTelescope.TASK_ID_CLEANUP,
        python_callable=OpenCitationsTelescope.cleanup,
        queue=OpenCitationsTelescope.QUEUE,
    )

    # Task dependencies
    (
        check_task
        >> list_releases_task
        >> download_task
        >> upload_downloaded
        >> extract_task
        >> upload_extracted_task
        >> bq_load_task
        >> cleanup_task
    )
