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

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from academic_observatory.workflows.doi import DoiWorkflow

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1)
}

with DAG(dag_id = DoiWorkflow.DAG_ID, schedule_interval="@once", default_args=default_args) as dag:

    # Extend GRID with iso3166 and home repos
    task_extend_grid_with_iso3166_and_home_repos = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXTEND_GRID_WITH_ISO3166_AND_HOME_REPOS,
        provide_context=True,
        python_callable=DoiWorkflow.extend_grid_with_iso3166_and_home_repos,
        dag=dag,
        depends_on_past=True
    )

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

    # Aggregrate Open Citations
    task_aggregate_open_citations = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_OPEN_CITATIONS,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_open_citations,
        dag=dag,
        depends_on_past=True
    )

    # Aggregrate WoS
    task_aggregate_wos = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_WOS,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_wos,
        dag=dag,
        depends_on_past=True
    )

    # Aggregrate Scopus
    task_aggregate_scopus = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_SCOPUS,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_scopus,
        dag=dag,
        depends_on_past=True
    )

    # Build DOIs table
    task_build_dois_table = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_BUILD_DOIS_TBLE,
        provide_context=True,
        python_callable=DoiWorkflow.build_dois_table,
        dag=dag,
        depends_on_past=True
    )
