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

# Author: Richard Hosking, James Diprose

"""
A DAG that produces the dois table and aggregated tables for the dashboards.

Each release is saved to the following BigQuery tables:
    <project_id>.observatory.bookYYYYMMDD
    <project_id>.observatory.countryYYYYMMDD
    <project_id>.observatory.doiYYYYMMDD
    <project_id>.observatory.funderYYYYMMDD
    <project_id>.observatory.groupYYYYMMDD
    <project_id>.observatory.institutionYYYYMMDD
    <project_id>.observatory.journalYYYYMMDD
    <project_id>.observatory.publisherYYYYMMDD
    <project_id>.observatory.regionYYYYMMDD
    <project_id>.observatory.subregionYYYYMMDD

Every week the following tables are overwritten for visualisation in the Data Studio dashboards:
    <project_id>.coki_dashboards.book
    <project_id>.coki_dashboards.country
    <project_id>.coki_dashboards.doi
    <project_id>.coki_dashboards.funder
    <project_id>.coki_dashboards.group
    <project_id>.coki_dashboards.institution
    <project_id>.coki_dashboards.journal
    <project_id>.coki_dashboards.publisher
    <project_id>.coki_dashboards.region
    <project_id>.coki_dashboards.subregion
"""

from datetime import datetime
from functools import partial

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from observatory.dags.telescopes.crossref_metadata import CrossrefMetadataTelescope
from observatory.dags.telescopes.crossref_fundref import CrossrefFundrefTelescope
from observatory.dags.telescopes.geonames import GeonamesTelescope
from observatory.dags.telescopes.grid import GridTelescope
from observatory.dags.telescopes.mag import MagTelescope
from observatory.dags.telescopes.open_citations import OpenCitationsTelescope
from observatory.dags.telescopes.unpaywall import UnpaywallTelescope
from observatory.dags.workflows.doi import DoiWorkflow
from observatory.platform.utils.telescope_utils import get_prev_execution_date

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 8, 30)
}

SENSOR_ID_CROSSREF_METADATA = 'crossref_metadata_sensor'
SENSOR_ID_FUNDREF = 'fundref_sensor'
SENSOR_ID_GEONAMES = 'geonames_sensor'
SENSOR_ID_GRID = 'grid_sensor'
SENSOR_ID_MAG = 'mag_sensor'
SENSOR_ID_OPEN_CITATIONS = 'open_citations_sensor'
SENSOR_ID_UNPAYWALL = 'unpaywall_sensor'

with DAG(dag_id=DoiWorkflow.DAG_ID, schedule_interval='@weekly', default_args=default_args, catchup=False) as dag:
    # Sensors
    crossref_metadata_sensor = ExternalTaskSensor(
        task_id=SENSOR_ID_CROSSREF_METADATA,
        external_dag_id=CrossrefMetadataTelescope.DAG_ID,
        execution_date_fn=lambda execution_date: get_prev_execution_date(CrossrefMetadataTelescope.SCHEDULE_INTERVAL,
                                                                         execution_date),
        mode='reschedule')

    fundref_sensor = ExternalTaskSensor(
        task_id=SENSOR_ID_FUNDREF,
        external_dag_id=CrossrefFundrefTelescope.DAG_ID,
        mode='reschedule')

    geonames_sensor = ExternalTaskSensor(
        task_id=SENSOR_ID_GEONAMES,
        external_dag_id=GeonamesTelescope.DAG_ID,
        dag=dag,
        mode='reschedule')

    grid_sensor = ExternalTaskSensor(
        task_id=SENSOR_ID_GRID,
        external_dag_id=GridTelescope.DAG_ID,
        dag=dag,
        mode='reschedule')

    mag_sensor = ExternalTaskSensor(
        task_id=SENSOR_ID_MAG,
        external_dag_id=MagTelescope.DAG_ID,
        mode='reschedule')

    open_citations_sensor = ExternalTaskSensor(
        task_id=SENSOR_ID_OPEN_CITATIONS,
        external_dag_id=OpenCitationsTelescope.DAG_ID,
        mode='reschedule')

    unpaywall_sensor = ExternalTaskSensor(
        task_id=SENSOR_ID_UNPAYWALL,
        external_dag_id=UnpaywallTelescope.DAG_ID,
        mode='reschedule')

    # Create datasets
    task_create_datasets = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_DATASETS,
        provide_context=True,
        python_callable=DoiWorkflow.create_datasets
    )

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

    # Aggregrate Crossref Events
    task_aggregate_orcid = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_AGGREGATE_ORCID,
        provide_context=True,
        python_callable=DoiWorkflow.aggregate_orcid
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

    # Create DOIs snapshot
    task_create_doi = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_DOI,
        provide_context=True,
        python_callable=DoiWorkflow.create_doi
    )

    # Create Books snapshot
    task_create_book = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_BOOK,
        provide_context=True,
        python_callable=DoiWorkflow.create_book
    )

    # Create aggregation tables
    task_create_country = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_COUNTRY,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_COUNTRY,
        python_callable=DoiWorkflow.create_aggregation
    )

    task_create_funder = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_FUNDER,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_FUNDER,
        python_callable=DoiWorkflow.create_aggregation
    )

    task_create_group = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_GROUP,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_GROUP,
        python_callable=DoiWorkflow.create_aggregation
    )

    task_create_institution = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_INSTITUTION,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_INSTITUTION,
        python_callable=DoiWorkflow.create_aggregation
    )

    task_create_author = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_AUTHOR,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_AUTHOR,
        python_callable=DoiWorkflow.create_aggregation
    )

    task_create_journal = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_JOURNAL,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_JOURNAL,
        python_callable=DoiWorkflow.create_aggregation
    )

    task_create_publisher = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_PUBLISHER,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_PUBLISHER,
        python_callable=DoiWorkflow.create_aggregation
    )

    task_create_region = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_REGION,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_REGION,
        python_callable=DoiWorkflow.create_aggregation
    )

    task_create_subregion = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_SUBREGION,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_SUBREGION,
        python_callable=DoiWorkflow.create_aggregation
    )

    task_copy_tables = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_COPY_TABLES,
        provide_context=True,
        python_callable=DoiWorkflow.copy_tables
    )

    task_create_views = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_CREATE_VIEWS,
        provide_context=True,
        python_callable=DoiWorkflow.create_views
    )

    # Export aggregation tables
    task_export_country = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXPORT_COUNTRY,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_COUNTRY,
        python_callable=DoiWorkflow.export_aggregation
    )

    task_export_funder = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXPORT_FUNDER,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_FUNDER,
        python_callable=DoiWorkflow.export_aggregation
    )

    task_export_group = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXPORT_GROUP,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_GROUP,
        python_callable=DoiWorkflow.export_aggregation
    )

    task_export_institution = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXPORT_INSTITUTION,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_INSTITUTION,
        python_callable=DoiWorkflow.export_aggregation
    )

    task_export_author = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXPORT_AUTHOR,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_AUTHOR,
        python_callable=DoiWorkflow.export_aggregation
    )

    task_export_journal = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXPORT_JOURNAL,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_JOURNAL,
        python_callable=DoiWorkflow.export_aggregation
    )

    task_export_publisher = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXPORT_PUBLISHER,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_PUBLISHER,
        python_callable=DoiWorkflow.export_aggregation
    )

    task_export_region = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXPORT_REGION,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_REGION,
        python_callable=DoiWorkflow.export_aggregation
    )

    task_export_subregion = PythonOperator(
        task_id=DoiWorkflow.TASK_ID_EXPORT_SUBREGION,
        provide_context=True,
        op_kwargs=DoiWorkflow.AGGREGATIONS_SUBREGION,
        python_callable=DoiWorkflow.export_aggregation
    )

    # Sensors
    sensors = [crossref_metadata_sensor, fundref_sensor, geonames_sensor, grid_sensor, mag_sensor,
               open_citations_sensor,
               unpaywall_sensor]

    # All pre-processing tasks run at once and when finished task_create_doi runs
    tasks_preprocessing = [task_extend_grid, task_aggregate_crossref_events, task_aggregate_orcid,
                           task_aggregate_mag, task_aggregate_unpaywall, task_extend_crossref_funders, 
                           task_aggregate_open_citations]
    sensors >> task_create_datasets >> tasks_preprocessing >> task_create_doi

    # After task_create_doi runs all of the post-processing tasks run
    tasks_postprocessing = [task_create_book, task_create_country, task_create_funder, task_create_group, task_create_institution,
                            task_create_author,  task_create_journal, task_create_publisher, task_create_region, 
                            task_create_subregion]

    # Preparing Data for Elasticsearch Export
    tasks_bigquery_exporting = [task_export_country, task_export_funder, task_export_group, task_export_institution,
                                task_export_author, task_export_journal, task_export_publisher, task_export_region,
                                task_export_subregion]

    task_create_doi >> tasks_postprocessing >> task_copy_tables >> task_create_views >> tasks_bigquery_exporting
