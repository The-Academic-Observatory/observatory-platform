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

# Author: Richard Hosking


from datetime import datetime

from airflow.operators.python_operator import PythonOperator

from academic_observatory.workflows.dois import DoiWorkflow

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}

with DAG(dag_id = DoiWorkflow.BAG_ID, schedule_interval="@once", default_args=default_args) as dag:

    # Aggregrate Crossref Events
    task_aggregate_crossref_events = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_CROSSREF_EVENTS,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_crossref_events,
        dag=dag,
        depends_on_past=True
    )

    # Aggregrate Microsoft Academic Graph
    task_aggregate_mag = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_MAG,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_mag,
        dag=dag,
        depends_on_past=True
    )

    # Compute OA colours from Unapywall
    task_compute_oa_colours = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_COMPUTE_OA_COLOURS,
        provide_context=True,
        python_callable=DoiWorkflow.compute_oa_colours,
        dag=dag,
        depends_on_past=True
    )
