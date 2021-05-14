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

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.exceptions import AirflowException
from airflow.models import Variable
from pendulum import Pendulum

from observatory.dags.config import workflow_sql_templates_path
from observatory.dags.telescopes.crossref_metadata import CrossrefMetadataTelescope
from observatory.dags.telescopes.fundref import FundrefTelescope
from observatory.dags.telescopes.grid import GridTelescope
from observatory.dags.telescopes.mag import MagTelescope
from observatory.dags.telescopes.unpaywall import UnpaywallTelescope
from observatory.platform.utils.airflow_utils import AirflowVars, check_variables
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


def set_task_state(success: bool, task_id: str):
    if success:
        logging.info(f"{task_id} success")
    else:
        msg_failed = f"{task_id} failed"
        logging.error(msg_failed)
        raise AirflowException(msg_failed)


def create_aggregate_table(
    project_id: str,
    release_date: Pendulum,
    aggregation_field: str,
    group_by_time_field: str,
    table_id: str,
    data_location: str,
    task_id: str,
    relate_to_institutions: bool,
    relate_to_countries: bool,
    relate_to_groups: bool,
    relate_to_members: bool,
    relate_to_journals: bool,
    relate_to_funders: bool,
    relate_to_publishers: bool,
):
    """Runs the aggregate table query.

    :param project_id: the Google Cloud project id.
    :param release_date: the release date of the release.
    :param aggregation_field: the field to aggregate on, e.g. institution, publisher etc.
    :group_by_time_field: either published_year or published_year_month depending on the granularity required for the
    time dimension
    :param table_id: the table id.
    :param data_location: the location for the table.
    :param task_id: the Airflow task id (for printing messages).
    :param relate_to_institutions: whether to generate the institution relationship output for this query
    :param relate_to_countries: whether to generate the countries relationship output for this query
    :param relate_to_groups: whether to generate the groups relationship output for this query
    :param relate_to_members: whether to generate the members relationship output for this query
    :param relate_to_journals: whether to generate the journals relationship output for this query
    :param relate_to_funders: whether to generate the funders relationship output for this query
    :param relate_to_publishers: whether to generate the publish relationship output for this query
    :return: None.
    """

    # Create processed dataset
    template_path = os.path.join(workflow_sql_templates_path(), DoiWorkflow.AGGREGATE_DOI_FILENAME)
    sql = render_template(
        template_path,
        project_id=project_id,
        release_date=release_date,
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

    processed_table_id = bigquery_sharded_table_id(table_id, release_date)
    success = create_bigquery_table_from_query(
        sql=sql,
        project_id=project_id,
        dataset_id=DoiWorkflow.OBSERVATORY_DATASET_ID,
        table_id=processed_table_id,
        location=data_location,
        cluster=True,
        clustering_fields=["id"],
    )

    set_task_state(success, task_id)


def export_aggregate_table(
    project_id: str,
    release_date: Pendulum,
    data_location: str,
    table_id: str,
    template_file_name: str,
    aggregate: str,
    facet: str,
):
    template_path = os.path.join(workflow_sql_templates_path(), template_file_name)
    sql = render_template(
        template_path,
        project_id=project_id,
        dataset_id=DoiWorkflow.OBSERVATORY_DATASET_ID,
        table_id=table_id,
        release_date=release_date,
        aggregate=aggregate,
        facet=facet,
    )

    export_table_id = f"{aggregate}_{facet}"

    processed_table_id = bigquery_sharded_table_id(export_table_id, release_date)

    success = create_bigquery_table_from_query(
        sql=sql,
        project_id=project_id,
        dataset_id=DoiWorkflow.ELASTIC_DATASET_ID,
        table_id=processed_table_id,
        location=data_location,
    )

    return success


class DoiWorkflow:
    DAG_ID = "doi"
    DESCRIPTION = "Combining all raw data sources into a linked DOIs dataset"
    TASK_ID_CREATE_DATASETS = "create_datasets"
    TASK_ID_EXTEND_GRID = "extend_grid"
    TASK_ID_AGGREGATE_CROSSREF_EVENTS = "aggregate_crossref_events"
    TASK_ID_AGGREGATE_ORCID = "aggregate_orcid"
    TASK_ID_AGGREGATE_MAG = "aggregate_mag"
    TASK_ID_AGGREGATE_UNPAYWALL = "aggregate_unpaywall"
    TASK_ID_EXTEND_CROSSREF_FUNDERS = "extend_crossref_funders"
    TASK_ID_AGGREGATE_OPEN_CITATIONS = "aggregate_open_citations"
    TASK_ID_AGGREGATE_WOS = "aggregate_wos"
    TASK_ID_AGGREGATE_SCOPUS = "aggregate_scopus"
    TASK_ID_CREATE_DOI = "create_doi"
    TASK_ID_CREATE_BOOK = "create_book"
    TASK_ID_CREATE_COUNTRY = "create_country"
    TASK_ID_CREATE_FUNDER = "create_funder"
    TASK_ID_CREATE_GROUP = "create_group"
    TASK_ID_CREATE_INSTITUTION = "create_institution"
    TASK_ID_CREATE_AUTHOR = "create_author"
    TASK_ID_CREATE_JOURNAL = "create_journal"
    TASK_ID_CREATE_PUBLISHER = "create_publisher"
    TASK_ID_CREATE_REGION = "create_region"
    TASK_ID_CREATE_SUBREGION = "create_subregion"
    TASK_ID_EXPORT_COUNTRY = "export_country"
    TASK_ID_EXPORT_FUNDER = "export_funder"
    TASK_ID_EXPORT_GROUP = "export_group"
    TASK_ID_EXPORT_INSTITUTION = "export_institution"
    TASK_ID_EXPORT_AUTHOR = "export_author"
    TASK_ID_EXPORT_JOURNAL = "export_journal"
    TASK_ID_EXPORT_PUBLISHER = "export_publisher"
    TASK_ID_EXPORT_REGION = "export_region"
    TASK_ID_EXPORT_SUBREGION = "export_subregion"
    TASK_ID_COPY_TABLES = "copy_tables"
    TASK_ID_CREATE_VIEWS = "create_views"

    PROCESSED_DATASET_ID = "observatory_intermediate"
    PROCESSED_DATASET_DESCRIPTION = "Intermediate processing dataset for the Academic Observatory."
    DASHBOARDS_DATASET_ID = "coki_dashboards"
    DASHBOARDS_DATASET_DESCRIPTION = "The latest data for display in the COKI dashboards."
    OBSERVATORY_DATASET_ID = "observatory"
    OBSERVATORY_DATASET_ID_DATASET_DESCRIPTION = "The Academic Observatory dataset."
    ELASTIC_DATASET_ID = "observatory_elastic"
    ELASTIC_DATASET_ID_DATASET_DESCRIPTION = "The Academic Observatory dataset for Elasticsearch."

    AGGREGATE_DOI_FILENAME = make_sql_jinja2_filename("aggregate_doi")

    EXPORT_UNIQUE_LIST_FILENAME = make_sql_jinja2_filename("export_unique_list")
    EXPORT_AGGREGATE_ACCESS_TYPES_FILENAME = make_sql_jinja2_filename("export_access_types")
    EXPORT_AGGREGATE_DISCIPLINES_FILENAME = make_sql_jinja2_filename("export_disciplines")
    EXPORT_AGGREGATE_EVENTS_FILENAME = make_sql_jinja2_filename("export_events")
    EXPORT_AGGREGATE_METRICS_FILENAME = make_sql_jinja2_filename("export_metrics")
    EXPORT_AGGREGATE_OUTPUT_TYPES_FILENAME = make_sql_jinja2_filename("export_output_types")
    EXPORT_AGGREGATE_RELATIONS_FILENAME = make_sql_jinja2_filename("export_relations")

    TOPIC_NAME = "message"

    AGGREGATIONS_COUNTRY = {
        "aggregation_field": "countries",
        "table_id": "country",
        "relate_to_institutions": False,
        "relate_to_countries": False,
        "relate_to_groups": False,
        "relate_to_members": True,
        "relate_to_journals": True,
        "relate_to_funders": True,
        "relate_to_publishers": True
    }

    AGGREGATIONS_FUNDER = {
        "aggregation_field": "funders",
        "table_id": "funder",
        "relate_to_institutions": True,
        "relate_to_countries": True,
        "relate_to_groups": True,
        "relate_to_members": True,
        "relate_to_journals": False,
        "relate_to_funders": True,
        "relate_to_publishers": True
    }

    AGGREGATIONS_GROUP = {
        "aggregation_field": "groupings",
        "table_id": "group",
        "relate_to_institutions": True,
        "relate_to_countries": False,
        "relate_to_groups": False,
        "relate_to_members": True,
        "relate_to_journals": True,
        "relate_to_funders": True,
        "relate_to_publishers": True
    }

    AGGREGATIONS_INSTITUTION = {
        "aggregation_field": "institutions",
        "table_id": "institution",
        "relate_to_institutions": True,
        "relate_to_countries": True,
        "relate_to_groups": False,
        "relate_to_members": False,
        "relate_to_journals": True,
        "relate_to_funders": True,
        "relate_to_publishers": True
    }

    AGGREGATIONS_AUTHOR = {
        "aggregation_field": "authors",
        "table_id": "author",
        "relate_to_institutions": True,
        "relate_to_countries": True,
        "relate_to_groups": True,
        "relate_to_members": False,
        "relate_to_journals": True,
        "relate_to_funders": True,
        "relate_to_publishers": True
    }

    AGGREGATIONS_JOURNAL = {
        "aggregation_field": "journals",
        "table_id": "journal",
        "relate_to_institutions": True,
        "relate_to_countries": True,
        "relate_to_groups": True,
        "relate_to_members": False,
        "relate_to_journals": True,
        "relate_to_funders": True,
        "relate_to_publishers": False
    }

    AGGREGATIONS_PUBLISHER = {
        "aggregation_field": "publishers",
        "table_id": "publisher",
        "relate_to_institutions": True,
        "relate_to_countries": True,
        "relate_to_groups": True,
        "relate_to_members": False,
        "relate_to_journals": False,
        "relate_to_funders": True,
        "relate_to_publishers": False
    }

    AGGREGATIONS_REGION = {
        "aggregation_field": "regions",
        "table_id": "region",
        "relate_to_institutions": False,
        "relate_to_countries": False,
        "relate_to_groups": False,
        "relate_to_members": False,
        "relate_to_journals": False,
        "relate_to_funders": True,
        "relate_to_publishers": True
    }

    AGGREGATIONS_SUBREGION = {
        "aggregation_field": "subregions",
        "table_id": "subregion",
        "relate_to_institutions": False,
        "relate_to_countries": False,
        "relate_to_groups": False,
        "relate_to_members": False,
        "relate_to_journals": False,
        "relate_to_funders": True,
        "relate_to_publishers": True
    }

    @staticmethod
    def check_dependencies(**kwargs):
        """Check that all variables and connections exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        vars_valid = check_variables(
            AirflowVars.DATA_PATH,
            AirflowVars.PROJECT_ID,
            AirflowVars.DATA_LOCATION,
            AirflowVars.DOWNLOAD_BUCKET,
            AirflowVars.TRANSFORM_BUCKET,
        )

        if not vars_valid:
            raise AirflowException("Required variables are missing")

    @staticmethod
    def create_datasets(**kwargs):
        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)

        # Create intermediate dataset
        create_bigquery_dataset(
            project_id,
            DoiWorkflow.PROCESSED_DATASET_ID,
            data_location,
            description=DoiWorkflow.PROCESSED_DATASET_DESCRIPTION,
        )

        # Create dashboards dataset
        create_bigquery_dataset(
            project_id,
            DoiWorkflow.DASHBOARDS_DATASET_ID,
            data_location,
            description=DoiWorkflow.DASHBOARDS_DATASET_DESCRIPTION,
        )

        # Create observatory dataset
        create_bigquery_dataset(
            project_id,
            DoiWorkflow.OBSERVATORY_DATASET_ID,
            data_location,
            DoiWorkflow.OBSERVATORY_DATASET_ID_DATASET_DESCRIPTION,
        )

        # Create elastic dataset
        create_bigquery_dataset(
            project_id,
            DoiWorkflow.ELASTIC_DATASET_ID,
            data_location,
            DoiWorkflow.ELASTIC_DATASET_ID_DATASET_DESCRIPTION,
        )

    @staticmethod
    def extend_grid(**kwargs):
        """Extend a GRID Release with a list of home_repos and iso3166 information.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)

        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()
        grid_release_date = select_table_shard_dates(
            project_id, GridTelescope.DATASET_ID, GridTelescope.DAG_ID, release_date
        )
        if len(grid_release_date):
            grid_release_date = grid_release_date[0]
        else:
            raise AirflowException(f"No GRID release with a table suffix <= {release_date} found")

        # Create processed table
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_EXTEND_GRID)
        )
        sql = render_template(template_path, project_id=project_id, grid_release_date=grid_release_date)

        processed_table_id = bigquery_sharded_table_id("grid_extended", release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
            cluster=True,
            clustering_fields=["id"],
        )

        set_task_state(success, DoiWorkflow.TASK_ID_EXTEND_GRID)

    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """Aggregate the current state of Crossref Events into a single table grouped by DOI.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Create processed table
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_CROSSREF_EVENTS)
        )
        sql = render_template(template_path, project_id=project_id)
        # TODO: perhaps only include records up until the end date of this query?

        processed_table_id = bigquery_sharded_table_id("crossref_events", release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
            cluster=True,
            clustering_fields=["doi"],
        )

        set_task_state(success, DoiWorkflow.TASK_ID_AGGREGATE_CROSSREF_EVENTS)

    @staticmethod
    def aggregate_orcid(**kwargs):
        """Aggregate the current state of ORCID into a single table with authors linked to the DOI's of their work.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Create processed table
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_ORCID)
        )
        sql = render_template(template_path, project_id=project_id)

        processed_table_id = bigquery_sharded_table_id("orcid", release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
            cluster=True,
            clustering_fields=["doi"],
        )

        set_task_state(success, DoiWorkflow.TASK_ID_AGGREGATE_ORCID)

    @staticmethod
    def aggregate_mag(**kwargs):
        """Aggregate MAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Get last MAG release date before current end date
        table_id = "Affiliations"
        mag_release_date = select_table_shard_dates(project_id, MagTelescope.DATASET_ID, table_id, release_date)
        if len(mag_release_date):
            mag_release_date = mag_release_date[0]
        else:
            raise AirflowException(f"No MAG release with a table suffix <= {release_date} found")

        # Create processed table
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_MAG)
        )
        sql = render_template(template_path, project_id=project_id, release_date=mag_release_date)

        processed_table_id = bigquery_sharded_table_id(MagTelescope.DAG_ID, release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
            cluster=True,
            clustering_fields=["doi"],
        )

        set_task_state(success, DoiWorkflow.TASK_ID_AGGREGATE_MAG)

    @staticmethod
    def aggregate_unpaywall(**kwargs):
        """Compute the Open Access colours from Unpaywall.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Get last Unpaywall release date before current end date
        unpaywall_release_date = select_table_shard_dates(
            project_id, UnpaywallTelescope.DATASET_ID, UnpaywallTelescope.DAG_ID, release_date
        )
        if len(unpaywall_release_date):
            unpaywall_release_date = unpaywall_release_date[0]
        else:
            raise AirflowException(f"Unpaywall release with a table suffix <= {release_date} not found")

        # Create processed table
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_UNPAYWALL)
        )
        sql = render_template(template_path, project_id=project_id, release_date=unpaywall_release_date)

        processed_table_id = bigquery_sharded_table_id(UnpaywallTelescope.DAG_ID, release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
            cluster=True,
            clustering_fields=["doi"],
        )

        set_task_state(success, DoiWorkflow.TASK_ID_AGGREGATE_UNPAYWALL)

    @staticmethod
    def extend_crossref_funders(**kwargs):
        """Extend Crossref Funders with Crossref Funders information.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Get last Funref and Crossref Metadata release dates before current end date
        fundref_release_date = select_table_shard_dates(
            project_id, FundrefTelescope.DATASET_ID, FundrefTelescope.DAG_ID, release_date
        )
        crossref_metadata_release_date = select_table_shard_dates(
            project_id, CrossrefMetadataTelescope.DATASET_ID, CrossrefMetadataTelescope.DAG_ID, release_date
        )
        if len(fundref_release_date) and len(crossref_metadata_release_date):
            fundref_release_date = fundref_release_date[0]
            crossref_metadata_release_date = crossref_metadata_release_date[0]
        else:
            raise AirflowException(
                f"Fundref and Crossref Metadata release with a table suffix <= {release_date} not found"
            )

        # Create processed table
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_EXTEND_CROSSREF_FUNDERS)
        )
        sql = render_template(
            template_path,
            project_id=project_id,
            crossref_metadata_release_date=crossref_metadata_release_date,
            fundref_release_date=fundref_release_date,
        )

        processed_table_id = bigquery_sharded_table_id("crossref_funders_extended", release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
            cluster=True,
            clustering_fields=["doi"],
        )

        set_task_state(success, DoiWorkflow.TASK_ID_EXTEND_CROSSREF_FUNDERS)

    @staticmethod
    def aggregate_open_citations(**kwargs):
        """Aggregate Open Citations.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Get last Open Citations release date before current end date
        open_citations_release_date = select_table_shard_dates(
            project_id, "open_citations", "open_citations", release_date
        )
        if len(open_citations_release_date):
            open_citations_release_date = open_citations_release_date[0]
        else:
            raise AirflowException(f"Open citations release with a table suffix <= {release_date} not found")

        # Create processed dataset
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_OPEN_CITATIONS)
        )
        sql = render_template(template_path, project_id=project_id, release_date=open_citations_release_date)

        processed_table_id = bigquery_sharded_table_id("open_citations", release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
            cluster=True,
            clustering_fields=["doi"],
        )

        set_task_state(success, DoiWorkflow.TASK_ID_AGGREGATE_OPEN_CITATIONS)

    @staticmethod
    def aggregate_wos(**kwargs):
        """Aggregate Web of Science.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Create
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_WOS)
        )
        sql = render_template(template_path, project_id=project_id)
        # TODO: only include records up until the end date

        processed_table_id = bigquery_sharded_table_id("wos", release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
        )

        set_task_state(success, DoiWorkflow.TASK_ID_AGGREGATE_WOS)

    @staticmethod
    def aggregate_scopus(**kwargs):
        """Aggregate Scopus.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Create processed dataset
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_SCOPUS)
        )
        sql = render_template(template_path, project_id=project_id)

        processed_table_id = bigquery_sharded_table_id("scopus", release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
        )

        set_task_state(success, DoiWorkflow.TASK_ID_AGGREGATE_SCOPUS)

    @staticmethod
    def create_doi(**kwargs):
        """Create DOIs snapshot.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Get last Crossref Metadata release date before current end date
        crossref_metadata_release_date = select_table_shard_dates(
            project_id, CrossrefMetadataTelescope.DATASET_ID, CrossrefMetadataTelescope.DAG_ID, release_date
        )
        if len(crossref_metadata_release_date):
            crossref_metadata_release_date = crossref_metadata_release_date[0]
        else:
            raise AirflowException(f"Crossref Metadata release with a table suffix <= {release_date} not found")

        # Create processed dataset
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_CREATE_DOI)
        )
        sql = render_template(
            template_path,
            project_id=project_id,
            dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
            release_date=release_date,
            crossref_metadata_release_date=crossref_metadata_release_date,
        )

        processed_table_id = bigquery_sharded_table_id("doi", release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.OBSERVATORY_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
            cluster=True,
            clustering_fields=["doi"],
        )

        set_task_state(success, DoiWorkflow.TASK_ID_CREATE_DOI)

    @staticmethod
    def create_book(**kwargs):
        """ Create Books snapshot.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()

        # Create processed dataset
        template_path = os.path.join(
            workflow_sql_templates_path(), make_sql_jinja2_filename(DoiWorkflow.TASK_ID_CREATE_BOOK)
        )
        sql = render_template(
            template_path, project_id=project_id, dataset_id=DoiWorkflow.PROCESSED_DATASET_ID, release_date=release_date
        )

        processed_table_id = bigquery_sharded_table_id("book", release_date)
        success = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=DoiWorkflow.OBSERVATORY_DATASET_ID,
            table_id=processed_table_id,
            location=data_location,
            cluster=True,
            clustering_fields=["isbn"],
        )

        set_task_state(success, DoiWorkflow.TASK_ID_CREATE_BOOK)

    @staticmethod
    def create_aggregation(**kwargs):
        """ Create aggregation snapshot.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()
        aggregation_field = kwargs["aggregation_field"]
        group_by_time_field = "published_year"
        table_id = kwargs["table_id"]

        # Optional Relationships
        relate_to_institutions = kwargs["relate_to_institutions"]
        relate_to_countries = kwargs["relate_to_countries"]
        relate_to_groups = kwargs["relate_to_groups"]
        relate_to_members = kwargs["relate_to_members"]
        relate_to_journals = kwargs["relate_to_journals"]
        relate_to_funders = kwargs["relate_to_funders"]
        relate_to_publishers = kwargs["relate_to_publishers"]

        # Aggregate
        create_aggregate_table(
            project_id=project_id,
            release_date=release_date,
            aggregation_field=aggregation_field,
            group_by_time_field=group_by_time_field,
            table_id=table_id,
            data_location=data_location,
            task_id=DoiWorkflow.TASK_ID_CREATE_COUNTRY,
            relate_to_institutions=relate_to_institutions,
            relate_to_countries=relate_to_countries,
            relate_to_groups=relate_to_groups,
            relate_to_members=relate_to_members,
            relate_to_journals=relate_to_journals,
            relate_to_funders=relate_to_funders,
            relate_to_publishers=relate_to_publishers
        )

    @staticmethod
    def export_aggregation(**kwargs):
        """ Export aggregation snapshot.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()
        aggregation_field = kwargs["aggregation_field"]
        group_by_time_field = "published_year"
        table_id = kwargs["table_id"]

        # Always export
        tables = [
            {"file_name": DoiWorkflow.EXPORT_UNIQUE_LIST_FILENAME, "aggregate": table_id, "facet": "none"},
            {
                "file_name": DoiWorkflow.EXPORT_AGGREGATE_ACCESS_TYPES_FILENAME,
                "aggregate": table_id,
                "facet": "access_types",
            },
            {
                "file_name": DoiWorkflow.EXPORT_AGGREGATE_DISCIPLINES_FILENAME,
                "aggregate": table_id,
                "facet": "disciplines",
            },
            {
                "file_name": DoiWorkflow.EXPORT_AGGREGATE_OUTPUT_TYPES_FILENAME,
                "aggregate": table_id,
                "facet": "output_types",
            },
            {"file_name": DoiWorkflow.EXPORT_AGGREGATE_EVENTS_FILENAME, "aggregate": table_id, "facet": "events"},
            {"file_name": DoiWorkflow.EXPORT_AGGREGATE_METRICS_FILENAME, "aggregate": table_id, "facet": "metrics"},
        ]

        # Optional Relationships
        if kwargs["relate_to_institutions"]:
            tables.append(
                {
                    "file_name": DoiWorkflow.EXPORT_AGGREGATE_RELATIONS_FILENAME,
                    "aggregate": table_id,
                    "facet": "institutions",
                }
            )
        if kwargs["relate_to_countries"]:
            tables.append(
                {
                    "file_name": DoiWorkflow.EXPORT_AGGREGATE_RELATIONS_FILENAME,
                    "aggregate": table_id,
                    "facet": "countries",
                }
            )
        if kwargs["relate_to_groups"]:
            tables.append(
                {
                    "file_name": DoiWorkflow.EXPORT_AGGREGATE_RELATIONS_FILENAME,
                    "aggregate": table_id,
                    "facet": "groupings",
                }
            )
        if kwargs["relate_to_members"]:
            tables.append(
                {
                    "file_name": DoiWorkflow.EXPORT_AGGREGATE_RELATIONS_FILENAME,
                    "aggregate": table_id,
                    "facet": "members",
                }
            )
        if kwargs["relate_to_journals"]:
            tables.append(
                {
                    "file_name": DoiWorkflow.EXPORT_AGGREGATE_RELATIONS_FILENAME,
                    "aggregate": table_id,
                    "facet": "journals",
                }
            )

        if kwargs["relate_to_funders"]:
            tables.append(
                {
                    "file_name": DoiWorkflow.EXPORT_AGGREGATE_RELATIONS_FILENAME,
                    "aggregate": table_id,
                    "facet": "funders"}
            )

        if kwargs["relate_to_publishers"]:
            tables.append(
                {
                    "file_name": DoiWorkflow.EXPORT_AGGREGATE_RELATIONS_FILENAME,
                    "aggregate": table_id,
                    "facet": "publishers"}
            )

        results = []

        # Calculate the number of parallel queries. Since all of the real work is done on BigQuery run each export task
        # in a separate thread so that they can be done in parallel.
        num_queries = len(tables)

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
                    export_aggregate_table,
                    project_id,
                    release_date,
                    data_location,
                    table_id,
                    template_file_name,
                    aggregate,
                    facet,
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

    @staticmethod
    def copy_tables(**kwargs):
        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()
        table_names = [
            "country",
            "doi",
            "funder",
            "group",
            "institution",
            "journal",
            "publisher",
            "region",
            "subregion",
        ]

        # Copy the latest data for display in the dashboards
        results = []
        for table_name in table_names:
            source_table_id = f"{project_id}.observatory.{bigquery_sharded_table_id(table_name, release_date)}"
            destination_table_id = f"{project_id}.{DoiWorkflow.DASHBOARDS_DATASET_ID}.{table_name}"
            success = copy_bigquery_table(source_table_id, destination_table_id, data_location)
            if not success:
                logging.error(f"Issue copying table: {source_table_id} to {destination_table_id}")

            results.append(success)

        if not all(results):
            raise ValueError("Problem copying tables")

    @staticmethod
    def create_views(**kwargs):
        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        table_names = ["country", "funder", "group", "institution", "publisher", "subregion"]

        # Create processed dataset
        dataset_id = DoiWorkflow.DASHBOARDS_DATASET_ID
        template_path = os.path.join(workflow_sql_templates_path(), make_sql_jinja2_filename("comparison_view"))

        # Create views
        for table_name in table_names:
            view_name = f"{table_name}_comparison"
            query = render_template(template_path, project_id=project_id, dataset_id=dataset_id, table_id=table_name)
            create_bigquery_view(project_id, dataset_id, view_name, query)
