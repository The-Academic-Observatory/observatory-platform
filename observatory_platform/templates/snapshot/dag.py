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


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator

from observatory_platform.templates.snapshot import telescope
from observatory_platform.templates.snapshot.telescope import ExampleTelescope
from observatory_platform.utils.telescope_utils import on_failure_callback

default_args = {
    "owner": "airflow",
    "start_date": ExampleTelescope.telescope.start_date,
    'on_failure_callback': on_failure_callback
}

with DAG(dag_id=ExampleTelescope.telescope.dag_id, schedule_interval=ExampleTelescope.telescope.schedule_interval,
         default_args=default_args, catchup=True, doc_md=telescope.__doc__) as dag:
    # Check that variables exist
    check = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=ExampleTelescope.check_dependencies,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue
    )

    # Get a list of releases and pass on with xcom
    list_releases = ShortCircuitOperator(
        task_id=ExampleTelescope.TASK_ID_LIST_RELEASES,
        python_callable=ExampleTelescope.list_releases,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue
    )

    # Download data
    download = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_DOWNLOAD,
        python_callable=ExampleTelescope.download,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        retries=ExampleTelescope.telescope.max_retries
    )

    # Upload downloaded data to bucket
    upload_downloaded = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=ExampleTelescope.upload_downloaded,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        retries=ExampleTelescope.telescope.max_retries
    )

    # Extract data
    extract = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_EXTRACT,
        python_callable=ExampleTelescope.extract,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        retries=ExampleTelescope.telescope.max_retries
    )

    # Transform data
    transform = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_TRANSFORM,
        python_callable=ExampleTelescope.transform,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue
    )

    # Upload transformed data to bucket
    upload_transformed = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=ExampleTelescope.upload_transformed,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        retries=ExampleTelescope.telescope.max_retries
    )

    # Upload this release as BigQuery table shard
    bq_load = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_BQ_LOAD,
        python_callable=ExampleTelescope.bq_load,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue)

    cleanup = PythonOperator(
        task_id=ExampleTelescope.TASK_ID_CLEANUP,
        python_callable=ExampleTelescope.cleanup,
        provide_context=True,
        queue=ExampleTelescope.telescope.queue,
        trigger_rule='none_failed'
    )

    check >> list_releases >> download >> upload_downloaded >> extract >> transform >> upload_transformed >> \
    bq_load >> cleanup
