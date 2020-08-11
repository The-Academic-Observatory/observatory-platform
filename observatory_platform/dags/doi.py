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

from observatory_platform.workflows.doi import DoiWorkflow

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 8, 1)
}

with DAG(dag_id=DoiWorkflow.DAG_ID, schedule_interval="@weekly", default_args=default_args) as dag:
    # Extend GRID with iso3166 and home repos
    task_extend_grid = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXTEND_GRID,
        provide_context=True,
        python_callable=DoiWorkflow.extend_grid
    )

    # Aggregrate Crossref Events
    task_aggregate_crossref_events = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_CROSSREF_EVENTS,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_crossref_events
    )

    # Aggregrate Microsoft Academic Graph
    task_aggregate_mag = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_MAG,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_mag
    )

    # Compute OA colours from Unapywall
    task_aggregate_unpaywall = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_UNPAYWALL,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_unpaywall
    )

    # Extend Crossref with Funder Information
    task_extend_crossref_funders = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXTEND_CROSSREF_FUNDERS,
        provide_context=True,
        python_callable=DoiWorkflow.extend_crossref_funders
    )

    # Aggregrate Open Citations
    task_aggregate_open_citations = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_OPEN_CITATIONS,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_open_citations
    )

    # Aggregrate WoS
    task_aggregate_wos = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_WOS,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_wos
    )

    # Aggregate Scopus
    task_aggregate_scopus = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_SCOPUS,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_scopus
    )

    # Create DOIs snapshot
    task_create_dois_snapshot = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_DOIS_SNAPSHOT,
        provide_context=True,
        python_callable=DoiWorkflow.create_dois_snapshot
    )

    # Link tasks
    task_extend_grid >> task_create_dois_snapshot
    task_aggregate_crossref_events >> task_create_dois_snapshot
    task_aggregate_mag >> task_create_dois_snapshot
    task_aggregate_unpaywall >> task_create_dois_snapshot
    task_extend_crossref_funders >> task_create_dois_snapshot
    task_aggregate_open_citations >> task_create_dois_snapshot
    task_aggregate_wos >> task_create_dois_snapshot
    task_aggregate_scopus >> task_create_dois_snapshot
