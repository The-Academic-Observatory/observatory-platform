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
A DAG that harvests the Unpaywall database: https://unpaywall.org/

Saved to the BigQuery table: <project_id>.our_research.unpaywallYYYYMMDD

Has been tested with the following Unpaywall releases:
* 2020-04-27, 2020-02-25, 2019-11-22, 2019-08-16, 2019-04-19, 2019-02-21, 2018-09-27, 2018-09-24

Does not work with the following releases:
* 2018-03-29, 2018-04-28, 2018-06-21, 2018-09-02, 2018-09-06
"""

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from academic_observatory_workflows.workflows.unpaywall_telescope import UnpaywallTelescope

default_args = {"owner": "airflow", "start_date": pendulum.datetime(2018, 9, 7)}

with DAG(dag_id="unpaywall", schedule_interval="@weekly", default_args=default_args) as dag:
    # Check that variables exist
    check = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=UnpaywallTelescope.check_dependencies,
        queue=UnpaywallTelescope.QUEUE,
    )

    # List releases and skip all subsequent tasks if there is no release to process
    list_releases = ShortCircuitOperator(
        task_id=UnpaywallTelescope.TASK_ID_LIST,
        python_callable=UnpaywallTelescope.list_releases,
        queue=UnpaywallTelescope.QUEUE,
    )

    # Downloads snapshot from url
    download = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_DOWNLOAD,
        python_callable=UnpaywallTelescope.download,
        queue=UnpaywallTelescope.QUEUE,
        retries=UnpaywallTelescope.RETRIES,
    )

    # Upload downloaded files for safekeeping
    upload_downloaded = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=UnpaywallTelescope.upload_downloaded,
        queue=UnpaywallTelescope.QUEUE,
        retries=UnpaywallTelescope.RETRIES,
    )

    # Decompresses download
    extract = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_EXTRACT,
        python_callable=UnpaywallTelescope.extract,
        queue=UnpaywallTelescope.QUEUE,
    )

    # Transforms download
    transform = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_TRANSFORM,
        python_callable=UnpaywallTelescope.transform,
        queue=UnpaywallTelescope.QUEUE,
    )

    # Upload download to gcs bucket
    upload_transformed = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=UnpaywallTelescope.upload_transformed,
        queue=UnpaywallTelescope.QUEUE,
        retries=UnpaywallTelescope.RETRIES,
    )

    # Upload download to BigQuery table
    bq_load = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_BQ_LOAD,
        python_callable=UnpaywallTelescope.load_to_bq,
        queue=UnpaywallTelescope.QUEUE,
    )

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_CLEANUP,
        python_callable=UnpaywallTelescope.cleanup,
        queue=UnpaywallTelescope.QUEUE,
    )

    (
        check
        >> list_releases
        >> download
        >> upload_downloaded
        >> extract
        >> transform
        >> upload_transformed
        >> bq_load
        >> cleanup
    )
