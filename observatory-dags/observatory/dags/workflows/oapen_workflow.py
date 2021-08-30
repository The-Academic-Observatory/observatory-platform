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
#
#
# Author: Richard Hosking

import os
import shutil
from functools import partial, update_wrapper
from pathlib import Path
from typing import List, Optional

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from google.cloud.bigquery import SourceFormat
from observatory.dags.config import sql_folder

from observatory.platform.workflows.workflow import AbstractRelease, Workflow
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    create_bigquery_dataset,
    create_bigquery_table_from_query,
    run_bigquery_query,
    select_table_shard_dates,
    copy_bigquery_table
)
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.workflow_utils import (
    bq_load_shard_v2,
    table_ids_from_path,
    make_dag_id,
)


class OapenWorkflowRelease(AbstractRelease):
    """
    Release information for OapenWorkflow.
    """

    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.DateTime,
        gcp_project_id: str,
        ao_gcp_project_id: str = "academic-observatory",
        oapen_metadata_dataset_id: str = "oapen",
        oapen_metadata_table_id: str = "metadata",
        public_book_metadata_dataset_id: str = "observatory",
        public_book_metadata_table_id: str = "book",
        irus_uk_dag_id_prefix: str = "oapen_irus_uk",
        irus_uk_dataset_id: str = "oapen",
        irus_uk_table_id: str = "oapen_irus_uk",
        oaebu_dataset: str = "oaebu",
        oaebu_onix_dataset: str = "oapen_onix",
        oaebu_intermediate_dataset: str = "oaebu_intermediate",
        oaebu_elastic_dataset: str = "data_export",
        dataset_location: str = "us",
        dataset_description: str = "Oapen workflow tables",
    ):
        """
        :param dag_id: DAG ID.
        :param release_date: The release date. It's the current execution date.
        :param oapen_release_date: the OAPEN release date.
        :param gcp_project_id: GCP Project ID.
        :param ao_gcp_project_id: GCP project ID for the Academic Observatory.
        :param oapen_metadata_dataset_id: GCP dataset ID for the oapen data.
        :param oapen_metadata_table_id: GCP table ID for the oapen data.
        :param public_book_dataset_id: GCP dataset ID for the public book data.
        :param public_book_table_id: GCP table ID for the public book data.
        :param irus_uk_dag_id_prefix: OAEBU IRUS_UK dag id prefix.
        :param irus_uk_dataset_id: OAEBU IRUS_UK dataset id.
        :param irus_uk_table_id: OAEBU IRUS_UK table id.
        :param oaebu_dataset: OAEBU dataset.
        :param oaebu_intermediate_dataset: OAEBU intermediate dataset.
        :param oaebu_elastic_dataset: OAEBU elastic dataset.
        """

        self.dag_id = dag_id
        self.release_date = release_date

        # GCP parameters for oaebu_oapen project
        self.gcp_project_id = gcp_project_id
        self.dataset_location = dataset_location
        self.dataset_description = dataset_description

        self.oaebu_dataset = oaebu_dataset
        self.oaebu_onix_dataset = oaebu_onix_dataset
        self.oaebu_intermediate_dataset = oaebu_intermediate_dataset
        self.oaebu_elastic_dataset = oaebu_elastic_dataset

        # Academic Observatory Reference
        self.ao_gcp_project_id = ao_gcp_project_id

        # OAPEN Metadata
        self.oapen_metadata_dataset_id = oapen_metadata_dataset_id
        self.oapen_metadata_table_id = oapen_metadata_table_id

        # Public Book Data
        self.public_book_metadata_dataset_id = public_book_metadata_dataset_id
        self.public_book_metadata_table_id = public_book_metadata_table_id

        # IRUS-UK
        self.irus_uk_dag_id_prefix = irus_uk_dag_id_prefix
        self.irus_uk_dataset_id = irus_uk_dataset_id
        self.irus_uk_table_id = irus_uk_table_id


    @property
    def transform_bucket(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    @property
    def transform_folder(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    @property
    def transform_files(self) -> List[str]:
        """Not used.
        :return: Empty list.
        """
        return list()

    @property
    def download_bucket(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    @property
    def download_files(self) -> List[str]:
        """Not used.
        :return: Empty list.
        """
        return list()

    @property
    def extract_files(self) -> List[str]:
        """Not used.
        :return: Empty list.
        """
        return list()

    @property
    def download_folder(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    @property
    def extract_folder(self) -> str:
        """Not used.
        :return: Empty string.
        """
        return str()

    def cleanup(self):
        """Delete all files and folders associated with this release.
        :return: None.
        """
        pass


def make_table_id(*, project_id: str, dataset_id: str, table_id: str, end_date: pendulum.datetime, sharded: bool):
    """
    Make a BQ table ID.
    :param project_id: GCP Project ID.
    :param dataset_id: GCP Dataset ID.
    :param table_id: Table name to convert to the suitable BQ table ID.
    :param end_date: Latest date considered.
    :param sharded: whether the table is sharded or not.
    """

    new_table_id = table_id
    if sharded:
        table_date = select_table_shard_dates(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            end_date=end_date,
        )[0]
        new_table_id = f"{table_id}{table_date.strftime('%Y%m%d')}"

    return new_table_id


class OapenWorkflow(Workflow):
    """
    Workflow for processing the OAPEN metadata and IRUS-UK metrics data
    """

    DAG_ID_PREFIX = "oapen_workflow"
    ORG_NAME = "OAPEN"

    def __init__(
        self,
        *,
        irus_uk_dag_id_prefix: str = "oapen_irus_uk",
        dag_id: Optional[str] = None,
        start_date: Optional[pendulum.datetime] = pendulum.datetime(2021, 3, 28),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        airflow_vars: List = None,
    ):
        """Initialises the workflow object.
        :param gcp_project_id: Project ID in GCP.
        :param gcp_dataset_id: Dataset ID in GCP.
        :param irus_uk_dag_id_prefix: OAEBU IRUS_UK dag id prefix.
        :param dag_id: DAG ID.
        :param start_date: Start date of the DAG.
        :param schedule_interval: Scheduled interval for running the DAG.
        :param catchup: Whether to catch up missed DAG runs.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        """

        self.dag_id = dag_id
        if dag_id is None:
            self.dag_id = make_dag_id(self.DAG_ID_PREFIX, self.ORG_NAME)

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        self.org_name = self.ORG_NAME
        self.irus_uk_dag_id_prefix = irus_uk_dag_id_prefix

        # Initialise Telesecope base class
        super().__init__(
            dag_id=self.dag_id, start_date=start_date, schedule_interval=schedule_interval, catchup=catchup, airflow_vars=airflow_vars
        )

        # Wait for irus_uk workflow to finish
        ext_dag_id = make_dag_id(irus_uk_dag_id_prefix, self.ORG_NAME)
        sensor = ExternalTaskSensor(task_id=f"{ext_dag_id}_sensor", external_dag_id=ext_dag_id, mode="reschedule")
        self.add_sensor(sensor)

        # Format OAPEN Metadata like ONIX to enable the next steps
        self.add_task(
            self.create_onix_formatted_metadata_output_tasks,
            task_id="create_onix_formatted_metadata_output_tasks"
        )

        # Copy IRUS-UK data and add release date
        self.add_task(
            self.copy_irus_uk_release,
            task_id="copy_irus_uk_release"
        )

        # Create OAEBU book product table
        self.add_task(
            self.create_oaebu_book_product_table,
            task_id="create_oaebu_book_product_table"
        )

        # Create OAEBU Elastic Export tables
        self.create_oaebu_export_tasks()

        # Cleanup tasks
        self.add_task(self.cleanup)


    def make_release(self, **kwargs) -> OapenWorkflowRelease:
        """Creates a release object.
        :param kwargs: From Airflow. Contains the execution_date.
        :return: an OapenWorkflowRelease object.
        """

        # Make release date
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()
        project_id = Variable.get(AirflowVars.PROJECT_ID)

        return OapenWorkflowRelease(
            dag_id=self.dag_id,
            release_date=release_date,
            gcp_project_id=project_id,
        )


    def cleanup(self, release: OapenWorkflowRelease, **kwargs):
        """Cleanup temporary files.

        :param release: Workflow release objects.
        :param kwargs: Unused.
        """

        release.cleanup()


    def copy_irus_uk_release(
            self,
            release: OapenWorkflowRelease,
            **kwargs,
    ):
        """Copy state of the oapen-irus-uk dataset and create a labled release
        :param release: Oapen workflow release information.
        """

        source_table_id = f"{release.gcp_project_id}.{release.irus_uk_dataset_id}.{release.irus_uk_table_id}"
        destination_table_id = f"{release.gcp_project_id}.{release.oaebu_intermediate_dataset}.{release.irus_uk_dataset_id}_{bigquery_sharded_table_id('oapen_irus_uk_matched', release.release_date)}"

        create_bigquery_dataset(project_id=release.gcp_project_id, dataset_id=release.oaebu_intermediate_dataset, location=release.dataset_location)

        status = copy_bigquery_table(source_table_id, destination_table_id, release.dataset_location)

        if not status:
            raise AirflowException(
                f"Issue copying table: {source_table_id} to {destination_table_id}"
            )

    def create_onix_formatted_metadata_output_tasks(
            self,
            release: OapenWorkflowRelease,
            **kwargs,
    ):
        """Create the Book Product Table
        :param release: Oapen workflow release information.
        """

        output_dataset = release.oaebu_onix_dataset
        data_location = release.dataset_location
        project_id = release.gcp_project_id

        output_table = "onix"
        release_date = release.release_date
        table_id = bigquery_sharded_table_id(output_table, release_date)

        # SQL reference
        table_joining_template_file = "create_mock_onix_data.sql.jinja2"
        template_path = os.path.join(sql_folder(), table_joining_template_file)

        sql = render_template(
            template_path,
            project_id=release.ao_gcp_project_id,
            dataset_id=release.oapen_metadata_dataset_id,
            table_id=release.oapen_metadata_table_id,
        )

        create_bigquery_dataset(project_id=project_id, dataset_id=output_dataset, location=data_location)

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=data_location,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {project_id}.{output_dataset}.{table_id}"
            )


    def create_oaebu_book_product_table(
        self,
        release: OapenWorkflowRelease,
        **kwargs,
    ):
        """Create the Book Product Table
        :param release: Oapen workflow release information.
        :param oapen_dataset: dataset_id if it is  a relevant data source for this publisher
        """

        output_table = "book_product"
        output_dataset = release.oaebu_dataset
        project_id = release.gcp_project_id

        data_location = release.dataset_location
        release_date = release.release_date

        table_joining_template_file = "create_book_products.sql.jinja2"
        template_path = os.path.join(sql_folder(), table_joining_template_file)

        table_id = bigquery_sharded_table_id(output_table, release_date)

        # Identify latest Book release from the Academic Observatory
        public_book_release_date = select_table_shard_dates(
            project_id=release.ao_gcp_project_id,
            dataset_id=release.public_book_metadata_dataset_id,
            table_id=release.public_book_metadata_table_id,
            end_date=release.release_date,
        )[0]

        sql = render_template(
            template_path,
            project_id=project_id,
            onix_dataset_id=release.oaebu_onix_dataset,
            dataset_id=release.oaebu_intermediate_dataset,
            release_date=release_date,
            onix_release_date=release_date,
            google_analytics=False,
            google_books=False,
            jstor=False,
            oapen=True,
            ucl=False,
            onix_workflow=False,
            onix_workflow_dataset='',
            google_analytics_dataset='',
            google_books_dataset='',
            jstor_dataset='',
            oapen_dataset=release.irus_uk_dataset_id,
            ucl_dataset='',
            public_book_release_date=public_book_release_date,
        )

        create_bigquery_dataset(project_id=project_id, dataset_id=output_dataset, location=data_location)

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=data_location,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {project_id}.{output_dataset}.{table_id}"
            )


    def export_oaebu_table(
        self,
        release: OapenWorkflowRelease,
        *,
        output_table: str,
        query_template: str,
        **kwargs,
    ):
        """Create an exported oaebu table.
        :param release: Oapen workflow release information.
        """

        project_id = release.gcp_project_id
        output_dataset = release.oaebu_elastic_dataset
        data_location = release.dataset_location
        release_date = release.release_date

        create_bigquery_dataset(project_id=project_id, dataset_id=output_dataset, location=data_location)

        table_id = bigquery_sharded_table_id(f"{project_id.replace('-', '_')}_{output_table}", release_date)
        template_path = os.path.join(sql_folder(), query_template)

        sql = render_template(
            template_path,
            project_id=project_id,
            dataset_id=release.oaebu_dataset,
            release=release_date,
        )

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=output_dataset,
            table_id=table_id,
            location=data_location,
        )

        if not status:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {project_id}.{output_dataset}.{table_id}"
            )


    def create_oaebu_export_tasks(self):
        """Create tasks for exporting final metrics from our OAEBU data.  It will create output tables in the oaebu_elastic dataset.
        :param data_partners: Oapen workflow release information.
        """

        export_tables = [
            {"output_table": "book_product_list", "query_template": "export_book_list.sql.jinja2", "file_type": "json"},
            {
                "output_table": "book_product_metrics",
                "query_template": "export_book_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_country",
                "query_template": "export_book_metrics_country.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_institution",
                "query_template": "export_book_metrics_institution.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_city",
                "query_template": "export_book_metrics_city.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_referrer",
                "query_template": "export_book_metrics_referrer.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_metrics_events",
                "query_template": "export_book_metrics_event.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_publisher_metrics",
                "query_template": "export_book_publisher_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_metrics",
                "query_template": "export_book_subject_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_year_metrics",
                "query_template": "export_book_year_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_subject_year_metrics",
                "query_template": "export_book_subject_year_metrics.sql.jinja2",
                "file_type": "json",
            },
            {
                "output_table": "book_product_author_metrics",
                "query_template": "export_book_author_metrics.sql.jinja2",
                "file_type": "json",
            },
        ]

        # Create each export table in BiqQuery
        for export_table in export_tables:
            fn = partial(
                self.export_oaebu_table,
                output_table=export_table["output_table"],
                query_template=export_table["query_template"],
            )

            # Populate the __name__ attribute of the partial object (it lacks one by default).
            update_wrapper(fn, self.export_oaebu_table)
            fn.__name__ += f".{export_table['output_table']}"

            self.add_task(fn)
