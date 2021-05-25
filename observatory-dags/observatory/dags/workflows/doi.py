# Copyright 2020-2021 Curtin University
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

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from pendulum import Pendulum

from observatory.dags.config import workflow_sql_templates_path
from observatory.platform.telescopes.telescope import Telescope
from observatory.platform.utils.airflow_utils import AirflowVars, set_task_state
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    copy_bigquery_table,
    create_bigquery_dataset,
    create_bigquery_table_from_query,
    create_bigquery_view,
    select_table_shard_dates,
)
from observatory.platform.utils.jinja2_utils import (
    make_sql_jinja2_filename,
    render_template,
)

MAX_QUERIES = 100


@dataclass
class Table:
    dataset_id: str
    table_id: str = None
    sharded: bool = False
    release_date: Pendulum = None


@dataclass
class Transform:
    inputs: Dict = None
    output_table: Table = None
    output_cluster: bool = False
    output_clustering_fields: List = None


@dataclass
class Aggregation:
    table_id: str
    aggregation_field: str
    group_by_time_field: str = "published_year"
    relate_to_institutions: bool = False
    relate_to_countries: bool = False
    relate_to_groups: bool = False
    relate_to_members: bool = False
    relate_to_journals: bool = False
    relate_to_funders: bool = False
    relate_to_publishers: bool = False


def make_dataset_transforms(
    dataset_id_crossref_events: str = "crossref",
    dataset_id_crossref_metadata: str = "crossref",
    dataset_id_fundref: str = "crossref",
    dataset_id_grid: str = "digital_science",
    dataset_id_iso: str = "iso",
    dataset_id_mag: str = "mag",
    dataset_id_orcid: str = "orcid",
    dataset_id_open_citations: str = "open_citations",
    dataset_id_unpaywall: str = "our_research",
    dataset_id_settings: str = "settings",
    dataset_id_observatory: str = "observatory",
    dataset_id_observatory_intermediate: str = "observatory_intermediate",
) -> Tuple[List[Transform], Transform, Transform]:
    return (
        [
            Transform(
                inputs={"crossref_events": Table(dataset_id_crossref_events, "crossref_events")},
                output_table=Table(dataset_id_observatory_intermediate, "crossref_events"),
                output_cluster=True,
                output_clustering_fields=["doi"],
            ),
            Transform(
                inputs={
                    "fundref": Table(dataset_id_fundref, "fundref", sharded=True),
                    "crossref_metadata": Table(dataset_id_crossref_metadata, "crossref_metadata", sharded=True),
                },
                output_table=Table(dataset_id_observatory_intermediate, "fundref"),
                output_cluster=True,
                output_clustering_fields=["doi"],
            ),
            Transform(
                inputs={
                    "grid": Table(dataset_id_grid, "grid", sharded=True),
                    "iso": Table(dataset_id_iso),
                    "settings": Table(dataset_id_settings),
                },
                output_table=Table(dataset_id_observatory_intermediate, "grid"),
            ),
            Transform(
                inputs={
                    "mag": Table(dataset_id_mag, "Affiliations", sharded=True),
                    "settings": Table(dataset_id_settings),
                },
                output_table=Table(dataset_id_observatory_intermediate, "mag"),
                output_cluster=True,
                output_clustering_fields=["Doi"],
            ),
            Transform(
                inputs={"orcid": Table(dataset_id_orcid, "orcid")},
                output_table=Table(dataset_id_observatory_intermediate, "orcid"),
                output_cluster=True,
                output_clustering_fields=["doi"],
            ),
            Transform(
                inputs={"open_citations": Table(dataset_id_open_citations, "open_citations", sharded=True)},
                output_table=Table(dataset_id_observatory_intermediate, "open_citations"),
                output_cluster=True,
                output_clustering_fields=["doi"],
            ),
            Transform(
                inputs={"unpaywall": Table(dataset_id_unpaywall, "unpaywall", sharded=True)},
                output_table=Table(dataset_id_observatory_intermediate, "unpaywall"),
                output_cluster=True,
                output_clustering_fields=["doi"],
            ),
        ],
        Transform(
            inputs={
                "observatory_intermediate": Table(dataset_id_observatory_intermediate),
                "unpaywall": Table(dataset_id_unpaywall),
                "crossref_metadata": Table(dataset_id_crossref_metadata, "crossref_metadata", sharded=True),
                "settings": Table(dataset_id_settings),
            },
            output_table=Table(dataset_id_observatory, "doi"),
            output_cluster=True,
            output_clustering_fields=["doi"],
        ),
        Transform(
            inputs={
                "observatory": Table(dataset_id_observatory, "doi", sharded=True),
                "crossref_events": Table(dataset_id_observatory_intermediate, "crossref_events", sharded=True),
            },
            output_table=Table(dataset_id_observatory, "book"),
            output_cluster=True,
            output_clustering_fields=["isbn"],
        ),
    )


class DoiWorkflow(Telescope):
    INT_DATASET_ID = "observatory_intermediate"
    INT_DATASET_DESCRIPTION = "Intermediate processing dataset for the Academic Observatory."
    DASHBOARDS_DATASET_ID = "coki_dashboards"
    DASHBOARDS_DATASET_DESCRIPTION = "The latest data for display in the COKI dashboards."
    FINAL_DATASET_ID = "observatory"
    FINAL_DATASET_DESCRIPTION = "The Academic Observatory dataset."
    ELASTIC_DATASET_ID = "observatory_elastic"
    ELASTIC_DATASET_ID_DATASET_DESCRIPTION = "The Academic Observatory dataset for Elasticsearch."

    AGGREGATE_DOI_FILENAME = make_sql_jinja2_filename("aggregate_doi")
    EXPORT_UNIQUE_LIST_FILENAME = make_sql_jinja2_filename("export_unique_list")
    EXPORT_ACCESS_TYPES_FILENAME = make_sql_jinja2_filename("export_access_types")
    EXPORT_DISCIPLINES_FILENAME = make_sql_jinja2_filename("export_disciplines")
    EXPORT_EVENTS_FILENAME = make_sql_jinja2_filename("export_events")
    EXPORT_METRICS_FILENAME = make_sql_jinja2_filename("export_metrics")
    EXPORT_OUTPUT_TYPES_FILENAME = make_sql_jinja2_filename("export_output_types")
    EXPORT_RELATIONS_FILENAME = make_sql_jinja2_filename("export_relations")

    SENSOR_DAG_IDS = ["crossref_metadata", "fundref", "geonames", "grid", "mag", "open_citations", "unpaywall"]

    AGGREGATIONS = [
        Aggregation(
            "country",
            "countries",
            relate_to_members=True,
            relate_to_journals=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "funder",
            "funders",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_groups=True,
            relate_to_members=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "group",
            "groupings",
            relate_to_institutions=True,
            relate_to_members=True,
            relate_to_journals=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "institution",
            "institutions",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_journals=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "author",
            "authors",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_groups=True,
            relate_to_journals=True,
            relate_to_funders=True,
            relate_to_publishers=True,
        ),
        Aggregation(
            "journal",
            "journals",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_groups=True,
            relate_to_journals=True,
            relate_to_funders=True,
        ),
        Aggregation(
            "publisher",
            "publishers",
            relate_to_institutions=True,
            relate_to_countries=True,
            relate_to_groups=True,
            relate_to_funders=True,
        ),
        Aggregation("region", "regions", relate_to_funders=True, relate_to_publishers=True),
        Aggregation("subregion", "subregions", relate_to_funders=True, relate_to_publishers=True),
    ]

    def __init__(
        self,
        *,
        intermediate_dataset_id: str = INT_DATASET_ID,
        dashboards_dataset_id: str = DASHBOARDS_DATASET_ID,
        observatory_dataset_id: str = FINAL_DATASET_ID,
        elastic_dataset_id: str = ELASTIC_DATASET_ID,
        transforms: Tuple = None,
        dag_id: Optional[str] = "doi",
        start_date: Optional[Pendulum] = datetime(2020, 8, 30),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        airflow_vars: List = None,
    ):
        """ Create the DoiWorkflow.
        :param intermediate_dataset_id: the BigQuery intermediate dataset id.
        :param dashboards_dataset_id: the BigQuery dashboards dataset id.
        :param observatory_dataset_id: the BigQuery observatory dataset id.
        :param elastic_dataset_id: the BigQuery elastic dataset id.
        :param dag_id: the DAG id.
        :param start_date: the start date.
        :param schedule_interval: the schedule interval.
        :param catchup: whether to catchup.
        :param airflow_vars: the required Airflow Variables.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        # Initialise Telesecope base class
        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
        )

        self.intermediate_dataset_id = intermediate_dataset_id
        self.dashboards_dataset_id = dashboards_dataset_id
        self.observatory_dataset_id = observatory_dataset_id
        self.elastic_dataset_id = elastic_dataset_id

        self.transforms, self.transform_doi, self.transform_book = transforms
        if transforms is None:
            self.transforms, self.transform_doi, self.transform_book = make_dataset_transforms(
                dataset_id_observatory=observatory_dataset_id
            )

        self.create_tasks()

    def create_tasks(self):
        # Add sensors
        for ext_dag_id in self.SENSOR_DAG_IDS:
            sensor = ExternalTaskSensor(task_id=f"{ext_dag_id}_sensor", external_dag_id=ext_dag_id, mode="reschedule")
            self.add_sensor(sensor)

        # Setup tasks
        self.add_setup_task(self.check_dependencies)

        # Create datasets
        self.add_task(self.create_datasets)

        # Create tasks for processing intermediate tables
        with self.parallel_tasks():
            for transform in self.transforms:
                task_id = f"create_{transform.output_table.table_id}"
                self.add_task(self.create_intermediate_table, **{"transform": transform, "task_id": task_id})

        # Create DOI Table
        self.add_task(
            self.create_intermediate_table,
            transform=self.transform_doi,
            task_id=f"create_{self.transform_doi.output_table.table_id}",
        )

        # Create Book Table
        self.add_task(
            self.create_intermediate_table,
            transform=self.transform_book,
            task_id=f"create_{self.transform_book.output_table.table_id}",
        )

        # Create final tables
        with self.parallel_tasks():
            for agg in self.AGGREGATIONS:
                task_id = f"create_{agg.table_id}"
                self.add_task(self.create_aggregate_table, **{"aggregation": agg, "task_id": task_id})

        # Copy tables and create views
        self.add_task(self.copy_to_dashboards)
        self.add_task(self.create_dashboard_views)

        # Export for Elastic
        with self.parallel_tasks():
            for agg in self.AGGREGATIONS:
                task_id = f"export_{agg.table_id}"
                self.add_task(self.export_for_elastic, **{"aggregation": agg, "task_id": task_id})

    def make_release(self, **kwargs) -> ObservatoryRelease:
        """Make a release instance. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: A release instance or list of release instances
        """

        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)

        return ObservatoryRelease(
            project_id=project_id,
            data_location=data_location,
            release_date=release_date,
            intermediate_dataset_id=self.intermediate_dataset_id,
            observatory_dataset_id=self.observatory_dataset_id,
            dashboards_dataset_id=self.dashboards_dataset_id,
            elastic_dataset_id=self.elastic_dataset_id,
        )

    def create_datasets(self, release: ObservatoryRelease, **kwargs):
        """ Create required BigQuery datasets.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        release.create_datasets()

    def create_intermediate_table(self, release: ObservatoryRelease, **kwargs):
        """ Create an intermediate table.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        transform: Transform = kwargs["transform"]
        release.create_intermediate_table(
            inputs=transform.inputs,
            output_dataset_id=transform.output_table.dataset_id,
            output_table_id=transform.output_table.table_id,
            output_cluster=transform.output_cluster,
            output_clustering_fields=transform.output_clustering_fields,
        )

    def create_aggregate_table(self, release: ObservatoryRelease, **kwargs):
        """ Runs the aggregate table query.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        print(f"CREATE AGGREGATE TABLE: {kwargs}")
        agg: Aggregation = kwargs["aggregation"]
        success = release.create_aggregate_table(
            aggregation_field=agg.aggregation_field,
            table_id=agg.table_id,
            group_by_time_field=agg.group_by_time_field,
            relate_to_institutions=agg.relate_to_institutions,
            relate_to_countries=agg.relate_to_countries,
            relate_to_groups=agg.relate_to_groups,
            relate_to_members=agg.relate_to_members,
            relate_to_journals=agg.relate_to_journals,
        )
        set_task_state(success, kwargs["task_id"])

    def copy_to_dashboards(self, release: ObservatoryRelease, **kwargs):
        """ Copy tables to dashboards dataset.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        success = release.copy_to_dashboards()
        set_task_state(success, self.copy_to_dashboards.__name__)

    def create_dashboard_views(self, release: ObservatoryRelease, **kwargs):
        """ Create views for dashboards dataset.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        release.create_dashboard_views()

    def export_for_elastic(self, release: ObservatoryRelease, **kwargs):
        """ Export data in a de-nested form for Elasticsearch.

        :param release: the ObservatoryRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        agg = kwargs["aggregation"]
        success = release.export_for_elastic(
            table_id=agg.table_id,
            relate_to_institutions=agg.relate_to_institutions,
            relate_to_countries=agg.relate_to_countries,
            relate_to_groups=agg.relate_to_groups,
            relate_to_members=agg.relate_to_members,
            relate_to_journals=agg.relate_to_journals,
            relate_to_funders=agg.relate_to_funders,
            relate_to_publishers=agg.relate_to_publishers,
        )
        set_task_state(success, kwargs["task_id"])


class ObservatoryRelease:
    def __init__(
        self,
        *,
        project_id: str,
        data_location: str,
        release_date: Pendulum,
        intermediate_dataset_id: str,
        dashboards_dataset_id: str,
        observatory_dataset_id: str,
        elastic_dataset_id: str,
    ):
        """ Construct an ObservatoryRelease.

        :param project_id: the Google Cloud project id.
        :param data_location: the location for BigQuery datasets.
        :param release_date: the release date.
        :param intermediate_dataset_id: the BigQuery intermediate dataset id.
        :param dashboards_dataset_id: the BigQuery dashboards dataset id.
        :param observatory_dataset_id: the BigQuery observatory dataset id.
        :param elastic_dataset_id: the BigQuery elastic dataset id.
        """

        self.project_id = project_id
        self.data_location = data_location
        self.release_date = release_date
        self.intermediate_dataset_id = intermediate_dataset_id
        self.dashboards_dataset_id = dashboards_dataset_id
        self.observatory_dataset_id = observatory_dataset_id
        self.elastic_dataset_id = elastic_dataset_id

    def create_datasets(self):
        """ Create the BigQuery datasets where data will be saved.
        :return: None.
        """

        datasets = [
            (self.intermediate_dataset_id, DoiWorkflow.INT_DATASET_DESCRIPTION),
            (self.dashboards_dataset_id, DoiWorkflow.DASHBOARDS_DATASET_DESCRIPTION),
            (self.observatory_dataset_id, DoiWorkflow.FINAL_DATASET_DESCRIPTION),
            (self.elastic_dataset_id, DoiWorkflow.ELASTIC_DATASET_ID_DATASET_DESCRIPTION),
        ]

        for dataset_id, description in datasets:
            create_bigquery_dataset(
                self.project_id, dataset_id, self.data_location, description=description,
            )

    def create_intermediate_table(
        self,
        *,
        inputs: Dict,
        output_dataset_id: str,
        output_table_id: str,
        output_cluster: bool,
        output_clustering_fields: List,
    ):
        """ Create an intermediate table.
        :param input_dataset_id: the input dataset id.
        :param input_table_id: the input table id.
        :param input_sharded: whether the input table is sharded or not. The most recent sharded table will be
        selected, up until the release date.
        :param output_table_id: the output table id.
        :param cluster: whether to cluster or not.
        :param clustering_fields: the fields to cluster on.
        :return: None.
        """

        def get_release_date(dataset_id: str, table_id: str):
            # Get last table shard date before current end date
            table_shard_dates = select_table_shard_dates(self.project_id, dataset_id, table_id, self.release_date)
            if len(table_shard_dates):
                shard_date = table_shard_dates[0]
            else:
                raise AirflowException(
                    f"{self.project_id}.{dataset_id}.{table_id} "
                    f"with a table shard date <= {self.release_date} not found"
                )

            return shard_date

        for k, table in inputs.items():
            if table.sharded:
                table.release_date = get_release_date(table.dataset_id, table.table_id)

        # Create processed table
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(f"create_{output_table_id}")
        )
        sql = render_template(template_path, project_id=self.project_id, release_date=self.release_date, **inputs)

        output_table_id_sharded = bigquery_sharded_table_id(output_table_id, self.release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=self.project_id,
            dataset_id=output_dataset_id,
            table_id=output_table_id_sharded,
            location=self.data_location,
            cluster=output_cluster,
            clustering_fields=output_clustering_fields,
        )

        return success

    def create_aggregate_table(
        self,
        *,
        aggregation_field: str,
        table_id: str,
        group_by_time_field: str = "published_year",
        relate_to_institutions: bool = False,
        relate_to_countries: bool = False,
        relate_to_groups: bool = False,
        relate_to_members: bool = False,
        relate_to_journals: bool = False,
        relate_to_funders: bool = False,
        relate_to_publishers: bool = False,
    ) -> bool:
        """Runs the aggregate table query.
        :param aggregation_field: the field to aggregate on, e.g. institution, publisher etc.
        :param group_by_time_field: either published_year or published_year_month depending on the granularity required for
        the time dimension
        :param table_id: the table id.
        :param relate_to_institutions: whether to generate the institution relationship output for this query
        :param relate_to_countries: whether to generate the countries relationship output for this query
        :param relate_to_groups: whether to generate the groups relationship output for this query
        :param relate_to_members: whether to generate the members relationship output for this query
        :param relate_to_journals: whether to generate the journals relationship output for this query
        :param relate_to_funders: whether to generate the funders relationship output for this query
        :param relate_to_publishers: whether to generate the publish relationship output for this query
        :return: None.
        """

        template_path = os.path.join(workflow_sql_templates_path(), make_sql_jinja2_filename("create_aggregate"))
        sql = render_template(
            template_path,
            project_id=self.project_id,
            dataset_id=self.observatory_dataset_id,
            release_date=self.release_date,
            aggregation_field=aggregation_field,
            group_by_time_field=group_by_time_field,
            relate_to_institutions=relate_to_institutions,
            relate_to_countries=relate_to_countries,
            relate_to_groups=relate_to_groups,
            relate_to_members=relate_to_members,
            relate_to_journals=relate_to_journals,
            relate_to_funders=relate_to_funders,
            relate_to_publishers=relate_to_publishers,
        )

        sharded_table_id = bigquery_sharded_table_id(table_id, self.release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=self.project_id,
            dataset_id=self.observatory_dataset_id,
            table_id=sharded_table_id,
            location=self.data_location,
            cluster=True,
            clustering_fields=["id"],
        )

        return success

    def copy_to_dashboards(self) -> bool:
        """ Copy all tables in the observatory dataset to the dashboards dataset.
        :return: whether successful or not.
        """

        results = []
        table_ids = [agg.table_id for agg in DoiWorkflow.AGGREGATIONS] + ["doi"]
        for table_id in table_ids:
            source_table_id = f"{self.project_id}.{self.observatory_dataset_id}.{bigquery_sharded_table_id(table_id, self.release_date)}"
            destination_table_id = f"{self.project_id}.{self.dashboards_dataset_id}.{table_id}"
            success = copy_bigquery_table(source_table_id, destination_table_id, self.data_location)
            if not success:
                logging.error(f"Issue copying table: {source_table_id} to {destination_table_id}")

            results.append(success)

        return all(results)

    def create_dashboard_views(self):
        """ Create views.
        :return: None.
        """

        # Create processed dataset
        template_path = os.path.join(workflow_sql_templates_path(), make_sql_jinja2_filename("comparison_view"))

        # Create views
        table_ids = ["country", "funder", "group", "institution", "publisher", "subregion"]
        for table_id in table_ids:
            view_name = f"{table_id}_comparison"
            query = render_template(
                template_path, project_id=self.project_id, dataset_id=self.dashboards_dataset_id, table_id=table_id
            )
            create_bigquery_view(self.project_id, self.dashboards_dataset_id, view_name, query)

    def export_for_elastic(
        self,
        *,
        table_id: str,
        relate_to_institutions: bool = False,
        relate_to_countries: bool = False,
        relate_to_groups: bool = False,
        relate_to_members: bool = False,
        relate_to_journals: bool = False,
        relate_to_funders: bool = False,
        relate_to_publishers: bool = False,
    ) -> bool:
        """Export data in in a de-nested form for elastic
        :param table_id:
        :param relate_to_institutions:
        :param relate_to_countries:
        :param relate_to_groups:
        :param relate_to_members:
        :param relate_to_journals:
        :param relate_to_funders:
        :param relate_to_publishers:
        :return: whether successful or not.
        """

        # Always export
        tables = [
            {"file_name": DoiWorkflow.EXPORT_UNIQUE_LIST_FILENAME, "aggregate": table_id, "facet": "unique_list", },
            {"file_name": DoiWorkflow.EXPORT_ACCESS_TYPES_FILENAME, "aggregate": table_id, "facet": "access_types",},
            {"file_name": DoiWorkflow.EXPORT_DISCIPLINES_FILENAME, "aggregate": table_id, "facet": "disciplines",},
            {"file_name": DoiWorkflow.EXPORT_OUTPUT_TYPES_FILENAME, "aggregate": table_id, "facet": "output_types",},
            {"file_name": DoiWorkflow.EXPORT_EVENTS_FILENAME, "aggregate": table_id, "facet": "events"},
            {"file_name": DoiWorkflow.EXPORT_METRICS_FILENAME, "aggregate": table_id, "facet": "metrics"},
        ]

        # Optional Relationships
        if relate_to_institutions:
            tables.append(
                {"file_name": DoiWorkflow.EXPORT_RELATIONS_FILENAME, "aggregate": table_id, "facet": "institutions",}
            )
        if relate_to_countries:
            tables.append(
                {"file_name": DoiWorkflow.EXPORT_RELATIONS_FILENAME, "aggregate": table_id, "facet": "countries",}
            )
        if relate_to_groups:
            tables.append(
                {"file_name": DoiWorkflow.EXPORT_RELATIONS_FILENAME, "aggregate": table_id, "facet": "groupings",}
            )
        if relate_to_members:
            tables.append(
                {"file_name": DoiWorkflow.EXPORT_RELATIONS_FILENAME, "aggregate": table_id, "facet": "members",}
            )
        if relate_to_journals:
            tables.append(
                {"file_name": DoiWorkflow.EXPORT_RELATIONS_FILENAME, "aggregate": table_id, "facet": "journals",}
            )

        if relate_to_funders:
            tables.append(
                {"file_name": DoiWorkflow.EXPORT_RELATIONS_FILENAME, "aggregate": table_id, "facet": "funders",}
            )

        if relate_to_publishers:
            tables.append(
                {"file_name": DoiWorkflow.EXPORT_RELATIONS_FILENAME, "aggregate": table_id, "facet": "publishers",}
            )

        # Calculate the number of parallel queries. Since all of the real work is done on BigQuery run each export task
        # in a separate thread so that they can be done in parallel.
        num_queries = min(len(tables), MAX_QUERIES)
        results = []

        with ThreadPoolExecutor(max_workers=num_queries) as executor:
            futures = list()
            futures_msgs = {}
            for table in tables:
                template_file_name = table["file_name"]
                aggregate = table["aggregate"]
                facet = table["facet"]

                msg = f"Exporting file_name={template_file_name}, aggregate={aggregate}, facet={facet}"
                logging.info(msg)
                future = executor.submit(
                    self.export_aggregate_table,
                    table_id=table_id,
                    template_file_name=template_file_name,
                    aggregate=aggregate,
                    facet=facet,
                )

                futures.append(future)
                futures_msgs[future] = msg

            # Wait for completed tasks
            for future in as_completed(futures):
                success = future.result()
                msg = futures_msgs[future]
                results.append(success)
                if success:
                    logging.info(f"Exporting feed success: {msg}")
                else:
                    logging.error(f"Exporting feed failed: {msg}")

        return all(results)

    def export_aggregate_table(self, *, table_id: str, template_file_name: str, aggregate: str, facet: str):
        """Export an aggregate table.
        :param table_id:
        :param template_file_name:
        :param aggregate:
        :param facet:
        :return:
        """

        template_path = os.path.join(workflow_sql_templates_path(), template_file_name)
        sql = render_template(
            template_path,
            project_id=self.project_id,
            dataset_id=self.observatory_dataset_id,
            table_id=table_id,
            release_date=self.release_date,
            aggregate=aggregate,
            facet=facet,
        )

        export_table_id = f"{aggregate}_{facet}"
        processed_table_id = bigquery_sharded_table_id(export_table_id, self.release_date)

        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=self.project_id,
            dataset_id=self.elastic_dataset_id,
            table_id=processed_table_id,
            location=self.data_location,
        )

        return success
