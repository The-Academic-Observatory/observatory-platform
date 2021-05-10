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
# Author: Tuan Chien

import json
import os
import shutil
from functools import partial, update_wrapper
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from google.cloud.bigquery import SourceFormat
from jinja2 import Environment, PackageLoader
from observatory.dags.config import workflow_sql_templates_path
from observatory.dags.telescopes.onix import OnixTelescope
from observatory.dags.workflows.oaebu_partners import OaebuPartners
from observatory.dags.workflows.onix_work_aggregation import (
    BookWorkAggregator,
    BookWorkFamilyAggregator,
)
from observatory.platform.telescopes.telescope import AbstractRelease, Telescope
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.gc_utils import (
    bigquery_partitioned_table_id,
    create_bigquery_dataset,
    create_bigquery_table_from_query,
    run_bigquery_query,
    select_table_suffixes,
    upload_files_to_cloud_storage,
)
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.telescope_utils import (
    list_to_jsonl_gz,
    make_dag_id,
    make_telescope_sensor,
    write_to_file,
)
from observatory.platform.utils.template_utils import (
    blob_name,
    bq_load_shard_v2,
    table_ids_from_path,
    upload_files_from_list,
)


class OnixWorkflowRelease(AbstractRelease):
    """
    Release information for OnixWorkflow.
    """

    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.Pendulum,
        gcp_project_id: str,
        onix_dataset_id: str = "onix",
        onix_table_id: str = "onix",
        gcp_bucket_name: str,
    ):
        """
        :param dag_id: DAG ID.
        :param release_date: The release date. It's the current execution date.
        :param gcp_project_id: GCP Project ID.
        :param gcp_bucket_name: GCP bucket name.
        :param onix_dataset_id: GCP dataset ID for the onix data.
        :param onix_table_id: GCP table ID for the onix data.
        """

        self.dag_id = dag_id
        self.release_date = release_date

        # Prepare filesystem
        self.worksid_table = "onix_workid_isbn"
        self.worksid_error_table = "onix_workid_isbn_errors"
        self.workfamilyid_table = "onix_workfamilyid_isbn"

        self.workslookup_filename = os.path.join(self.transform_folder, f"{self.worksid_table}.jsonl.gz")
        self.workslookup_errors_filename = os.path.join(self.transform_folder, f"{self.worksid_error_table}.jsonl.gz")
        self.worksfamilylookup_filename = os.path.join(self.transform_folder, f"{self.workfamilyid_table}.jsonl.gz")

        # GCP parameters
        self.workflow_dataset_id = "onix_workflow"
        self.project_id = gcp_project_id
        self.onix_dataset_id = onix_dataset_id
        self.dataset_location = "us"
        self.dataset_description = "ONIX workflow tables"

        # ONIX release info
        self.onix_table_id = onix_table_id
        self.bucket_name = gcp_bucket_name

        Path(".", self.transform_folder).mkdir(exist_ok=True, parents=True)

        # OAEBU intermediate tables
        self.oaebu_intermediate_dataset = "oaebu_intermediate"
        self.oaebu_data_qa_dataset = "oaebu_data_qa"
        self.oaebu_intermediate_match_suffix = "_matched"

    @property
    def transform_bucket(self) -> str:
        """
        :return: The transform bucket name.
        """
        return self.bucket_name

    @property
    def transform_folder(self) -> str:
        """The transform folder path. This is the folder hierarchy locally and on GCP.
        :return: The transform folder path.
        """

        folder = os.path.join(str(self.dag_id), self.release_date.strftime("%Y%m%d"))

        return folder

    @property
    def transform_files(self) -> List[str]:
        """
        :return: List of transformed files.
        """
        return [self.workslookup_filename, self.workslookup_errors_filename, self.worksfamilylookup_filename]

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
        shutil.rmtree(self.transform_folder)


class OnixWorkflow(Telescope):
    """This workflow telescope:
    1. [Not implemented] Creates an ISBN13-> internal identifier lookup table.
    2. Creates an ISBN13 -> WorkID lookup table.
      a. Aggregates Product records into Work clusters.
      b. Writes the lookup table to BigQuery.
      c. Writes an error table to BigQuery.
    3. Create an ISBN13 -> Work Family ID lookup table.  Clusters editions together.
      a. Aggregate Works into Work Families.
      b. Writes the lookup table to BigQuery.
    4. Create OAEBU intermediate tables.
      a. For each data partner, create new tables in oaebu_intermediate dataset where existing tables are augmented
         with work_id and work_family_id columns.
    5. Create OAEBU QA tables for looking at metrics and problems arising from the data sets (and for eventual automatic reporting).
    """

    DAG_ID_PREFIX = "onix_workflow"

    def __init__(
        self,
        *,
        org_name: str,
        gcp_project_id: str,
        gcp_bucket_name: str,
        onix_dataset_id: str = "onix",
        onix_table_id: str = "onix",
        dag_id: Optional[str] = None,
        start_date: Optional[pendulum.Pendulum] = pendulum.Pendulum(2021, 3, 28),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        data_partners: List[OaebuPartners] = None,
    ):
        """Initialises the workflow object.
        :param org_name: Organisation name.
        :param gcp_project_id: Project ID in GCP.
        :param gcp_bucket_name: GCP bucket name to store files.
        :param onix_dataset_id: GCP dataset ID of the onix data.
        :param onix_table_id: GCP table ID of the onix data.
        :param dag_id: DAG ID.
        :param start_date: Start date of the DAG.
        :param schedule_interval: Scheduled interval for running the DAG.
        :param catchup: Whether to catch up missed DAG runs.
        :param data_partners: OAEBU data sources.
        """

        self.dag_id = dag_id
        if dag_id is None:
            self.dag_id = make_dag_id(self.DAG_ID_PREFIX, org_name)

        if data_partners is None:
            data_partners = list()

        self.org_name = org_name
        self.gcp_project_id = gcp_project_id
        self.gcp_bucket_name = gcp_bucket_name
        self.onix_dataset_id = onix_dataset_id
        self.onix_table_id = onix_table_id

        # Initialise Telesecope base class
        super().__init__(
            dag_id=self.dag_id, start_date=start_date, schedule_interval=schedule_interval, catchup=catchup
        )

        # Wait on ONIX telescope to finish
        telescope_sensor = make_telescope_sensor(org_name, OnixTelescope.DAG_ID_PREFIX)
        self.add_sensor(telescope_sensor)

        # Aggregate Works
        self.add_task(self.aggregate_works)
        self.add_task(self.upload_aggregation_tables)
        self.add_task(self.bq_load_workid_lookup)
        self.add_task(self.bq_load_workid_lookup_errors)
        self.add_task(self.bq_load_workfamilyid_lookup)

        # Create OAEBU Intermediate tables
        self.create_oaebu_intermediate_table_tasks(data_partners)

        # Create QA metrics tables
        self.create_oaebu_data_qa_tasks(data_partners)

        # Cleanup tasks
        self.add_task(self.cleanup)

    def get_isbn_utils_sql_string(self) -> str:
        """Load the ISBN utils sql functions.
        :return BQ SQL string.
        """

        isbn_utils_file = "isbn_utils.sql"
        isbn_utils_path = os.path.join(workflow_sql_templates_path(), isbn_utils_file)
        with open(isbn_utils_path, "r") as f:
            isbn_utils_sql = f.read()

        return isbn_utils_sql

    def make_release(self, **kwargs) -> List[OnixWorkflowRelease]:
        """Creates a release object.
        :param kwargs: From Airflow. Contains the execution_date.
        :return: List of OnixWorkflowRelease objects.
        """

        releases = list()

        ti: TaskInstance = kwargs["ti"]
        records = ti.xcom_pull(
            dag_id=make_dag_id("onix", self.org_name),
            key=OnixTelescope.RELEASE_INFO,
            task_ids="list_release_info",
            include_prior_dates=False,
        )

        for record in records:
            release_date = record["release_date"]
            release = OnixWorkflowRelease(
                dag_id=self.dag_id,
                release_date=release_date,
                gcp_project_id=self.gcp_project_id,
                gcp_bucket_name=self.gcp_bucket_name,
                onix_dataset_id=self.onix_dataset_id,
                onix_table_id=self.onix_table_id,
            )
            releases.append(release)

        return releases

    def get_onix_records(self, project_id: str, dataset_id: str, table_id: str) -> List[dict]:
        """Fetch the latest onix snapshot from BigQuery.
        :param project_id: Project ID.
        :param dataset_id: Dataset ID.
        :param table_id: Table ID.
        :return: List of onix product records.
        """

        sql = f"SELECT * FROM {project_id}.{dataset_id}.{table_id}"
        records = run_bigquery_query(sql)

        products = [
            {
                "ISBN13": records[i]["ISBN13"],
                "RelatedWorks": records[i]["RelatedWorks"],
                "RelatedProducts": records[i]["RelatedProducts"],
            }
            for i in range(len(records))
        ]

        return products

    def aggregate_works(self, releases: List[OnixWorkflowRelease], **kwargs):
        """Fetches the ONIX product records from our ONIX database, aggregates them into works, workfamilies,
        and outputs it into jsonl files.

        :param releases: Workflow release objects.
        :param kwargs: Unused.
        """

        for release in releases:
            # Fetch ONIX data
            table_id = release.onix_table_id + release.release_date.strftime("%Y%m%d")
            products = self.get_onix_records(release.project_id, release.onix_dataset_id, table_id)

            # Aggregate into works
            agg = BookWorkAggregator(products)
            works = agg.aggregate()
            lookup_table = agg.get_works_lookup_table()
            list_to_jsonl_gz(release.workslookup_filename, lookup_table)

            # Save errors from aggregation
            error_table = [{"Error": error} for error in agg.errors]
            list_to_jsonl_gz(release.workslookup_errors_filename, error_table)

            # Aggregate work families
            agg = BookWorkFamilyAggregator(works)
            works_families = agg.aggregate()
            lookup_table = agg.get_works_family_lookup_table()
            list_to_jsonl_gz(release.worksfamilylookup_filename, lookup_table)

    def upload_aggregation_tables(self, releases: List[OnixWorkflowRelease], **kwargs):
        """Upload the aggregation tables and error tables to a GCP bucket in preparation for BQ loading.

        :param releases: Workflow release objects.
        :param kwargs: Unused.
        """

        for release in releases:
            files = release.transform_files
            blobs = [os.path.join(release.transform_folder, os.path.basename(file)) for file in files]
            upload_files_to_cloud_storage(bucket_name=release.transform_bucket, blob_names=blobs, file_paths=files)

    def bq_load_workid_lookup(self, releases: List[OnixWorkflowRelease], **kwargs):
        """Load the WorkID lookup table into BigQuery.

        :param releases: Workflow release objects.
        :param kwargs: Unused.
        """

        for release in releases:
            blob = os.path.join(release.transform_folder, os.path.basename(release.workslookup_filename))
            table_id, _ = table_ids_from_path(release.workslookup_filename)
            bq_load_shard_v2(
                project_id=release.project_id,
                transform_bucket=release.transform_bucket,
                transform_blob=blob,
                dataset_id=release.workflow_dataset_id,
                dataset_location=release.dataset_location,
                table_id=table_id,
                release_date=release.release_date,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                dataset_description=release.dataset_description,
                **{},
            )

    def bq_load_workid_lookup_errors(self, releases: List[OnixWorkflowRelease], **kwargs):
        """Load the WorkID lookup table errors into BigQuery.

        :param releases: Workflow release objects.
        :param kwargs: Unused.
        """

        for release in releases:
            blob = os.path.join(release.transform_folder, os.path.basename(release.workslookup_errors_filename))
            table_id, _ = table_ids_from_path(release.workslookup_errors_filename)
            bq_load_shard_v2(
                project_id=release.project_id,
                transform_bucket=release.transform_bucket,
                transform_blob=blob,
                dataset_id=release.workflow_dataset_id,
                dataset_location=release.dataset_location,
                table_id=table_id,
                release_date=release.release_date,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                prefix="",
                schema_version="",
                dataset_description=release.dataset_description,
                **{},
            )

    def bq_load_workfamilyid_lookup(self, releases: List[OnixWorkflowRelease], **kwargs):
        """Load the WorkFamilyID lookup table into BigQuery.

        :param releases: Workflow release objects.
        :param kwargs: Unused.
        """

        for release in releases:
            blob = os.path.join(release.transform_folder, os.path.basename(release.worksfamilylookup_filename))
            table_id, _ = table_ids_from_path(release.worksfamilylookup_filename)
            bq_load_shard_v2(
                project_id=release.project_id,
                transform_bucket=release.transform_bucket,
                transform_blob=blob,
                dataset_id=release.workflow_dataset_id,
                dataset_location=release.dataset_location,
                table_id=table_id,
                release_date=release.release_date,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                prefix="",
                schema_version="",
                dataset_description=release.dataset_description,
                **{},
            )

    def cleanup(self, releases: List[OnixWorkflowRelease], **kwargs):
        """Cleanup temporary files.

        :param release: Workflow release objects.
        :param kwargs: Unused.
        """

        for release in releases:
            release.cleanup()

    def create_oaebu_intermediate_table(
        self,
        releases: List[OnixWorkflowRelease],
        *args,
        orig_project_id: str,
        orig_dataset: str,
        orig_table: str,
        orig_isbn: str,
        table_date: Union[None, pendulum.Pendulum],
        **kwargs,
    ):
        """Create an intermediate oaebu table.  They are of the form datasource_matched<date>
        :param releases: Onix workflow release information.
        :param args: Catching any other positional args (unused).
        :param orig_project_id: Project ID for the partner data.
        :param orig_dataset: Dataset ID for the partner data.
        :param orig_table: Table ID for the partner data.
        :param orig_isbn: Name of the ISBN field in the partner data table.
        :param table_date: Date of table
        """

        for release in releases:
            if table_date == None:
                table_date = select_table_suffixes(
                    project_id=orig_project_id,
                    dataset_id=orig_dataset,
                    table_id=orig_table,
                    end_date=release.release_date,
                )[0]

            output_table = f"{orig_dataset}_{orig_table}{release.oaebu_intermediate_match_suffix}"
            output_dataset = release.oaebu_intermediate_dataset

            data_location = release.dataset_location
            release_date = release.release_date
            table_joining_template_file = "assign_workid_workfamilyid.sql.jinja2"
            template_path = os.path.join(workflow_sql_templates_path(), table_joining_template_file)
            table_id = bigquery_partitioned_table_id(output_table, release_date)
            orig_table_suffix = table_date.strftime("%Y%m%d")
            dst_table_suffix = release_date.strftime("%Y%m%d")

            sql = render_template(
                template_path,
                project_id=orig_project_id,
                orig_dataset=orig_dataset,
                orig_table=f"{orig_table}{orig_table_suffix}",
                orig_isbn=orig_isbn,
                onix_workflow_dataset=release.workflow_dataset_id,
                wid_table=release.worksid_table + dst_table_suffix,
                wfam_table=release.workfamilyid_table + dst_table_suffix,
            )

            create_bigquery_dataset(project_id=release.project_id, dataset_id=output_dataset, location=data_location)

            status = create_bigquery_table_from_query(
                sql=sql,
                project_id=release.project_id,
                dataset_id=output_dataset,
                table_id=table_id,
                location=release.dataset_location,
            )

            if status != True:
                raise AirflowException(
                    f"create_bigquery_table_from_query failed on {release.project_id}.{output_dataset}.{table_id}"
                )

    def create_oaebu_intermediate_table_tasks(self, data_partners: List[OaebuPartners]):
        """Create tasks for generating oaebu intermediate tables for each OAEBU data partner.
        :param oaebu_data: List of oaebu partner data.
        """

        for data in data_partners:
            fn = partial(
                self.create_oaebu_intermediate_table,
                orig_project_id=data.gcp_project_id,
                orig_dataset=data.gcp_dataset_id,
                orig_table=data.gcp_table_id,
                orig_isbn=data.isbn_field_name,
                table_date=data.gcp_table_date,
            )

            # Populate the __name__ attribute of the partial object (it lacks one by default).
            # Scheme: create_oaebu_intermediate_table.dataset.table
            update_wrapper(fn, self.create_oaebu_intermediate_table)
            fn.__name__ += f".{data.gcp_dataset_id}.{data.gcp_table_id}"

            self.add_task(fn)

    def create_oaebu_data_qa_tasks(self, data_partners: List[OaebuPartners]):
        """Create tasks for outputing QA metrics from our OAEBU data.  It will create output tables in the oaebu_data_qa dataset.
        :param oaebu_data: List of oaebu partner data.
        """

        self.create_oaebu_data_qa_onix()

        for data_partner in data_partners:
            if data_partner.name == "JSTOR":
                self.create_oaebu_data_qa_jstor_tasks(data_partner)
            # elif data_partner.name == "OAPEN IRUS UK":
            #     self.create_oaebu_data_qa_oapen_irus_uk_tasks(data_partner)
            elif data_partner.name == "Google Books Sales":
                self.create_oaebu_data_qa_google_books_sales_tasks(data_partner)
            elif data_partner.name == "Google Books Traffic":
                self.create_oaebu_data_qa_google_books_traffic_tasks(data_partner)

            self.create_oaebu_data_qa_intermediate_tasks(data_partner)

    def create_oaebu_data_qa_onix(self, *args, **kwargs):
        """Create ONIX quality assurance metrics."""

        # isbn validation
        self.add_task(self.create_oaebu_data_qa_onix_isbn)

        # aggregate metrics
        self.add_task(self.create_oaebu_data_qa_onix_aggregate)

    def create_oaebu_data_qa_onix_aggregate(self, releases: List[OnixWorkflowRelease], *args, **kwargs):
        """Create a bq table of some aggregate metrics for the ONIX data set.
        :param releases: List of workflow releases.
        """

        template_file = "onix_aggregate_metrics.sql.jinja2"
        template_path = os.path.join(workflow_sql_templates_path(), template_file)

        for release in releases:
            onix_project_id = release.project_id
            onix_dataset_id = release.onix_dataset_id
            onix_table_id = release.onix_table_id + release.release_date.strftime("%Y%m%d")
            output_dataset = release.oaebu_data_qa_dataset
            output_table = "onix_aggregate_metrics"
            release_date = release.release_date
            output_table_id = bigquery_partitioned_table_id(output_table, release_date)

            sql = render_template(
                template_path,
                project_id=onix_project_id,
                dataset_id=onix_dataset_id,
                table_id=onix_table_id,
                isbn="ISBN13",
            )

            create_bigquery_dataset(
                project_id=release.project_id,
                dataset_id=release.oaebu_data_qa_dataset,
                location=release.dataset_location,
            )

            # Fix
            status = create_bigquery_table_from_query(
                sql=sql,
                project_id=release.project_id,
                dataset_id=release.oaebu_data_qa_dataset,
                table_id=output_table_id,
                location=release.dataset_location,
            )

            if status != True:
                raise AirflowException(
                    f"create_bigquery_table_from_query failed on {release.project_id}.{output_dataset}.{output_table_id}"
                )

    def create_oaebu_data_qa_onix_isbn(self, releases: List[OnixWorkflowRelease], *args, **kwargs):
        """Create a BQ table of invalid ISBNs for the ONIX feed that can be fed back to publishers.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param releases: List of Onix workflow releases.
        """

        for release in releases:
            release_date = release.release_date
            project_id = release.project_id
            orig_dataset_id = release.onix_dataset_id
            orig_table_id = release.onix_table_id + release_date.strftime("%Y%m%d")
            output_dataset_id = release.oaebu_data_qa_dataset
            output_table = "onix_invalid_isbn"
            output_table_id = bigquery_partitioned_table_id(output_table, release_date)
            dataset_location = release.dataset_location

            self.oaebu_data_qa_validate_isbn(
                project_id=project_id,
                orig_dataset_id=orig_dataset_id,
                orig_table_id=orig_table_id,
                output_dataset_id=output_dataset_id,
                output_table_id=output_table_id,
                dataset_location=dataset_location,
                isbn="ISBN13",
            )

    def oaebu_data_qa_validate_isbn(
        self,
        *,
        project_id: str,
        orig_dataset_id,
        orig_table_id: str,
        output_dataset_id: str,
        output_table_id: str,
        dataset_location: str,
        isbn: str,
    ):
        """Create a BQ table of invalid ISBNs for the ONIX feed that can be fed back to publishers.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param output_table: Name of the output table.
        :param project_id: GCP Project ID of the data.
        :param orig_dataset_id: GCP Dataset ID of the source data.
        :param orig_table_id: Table ID of the source data (excluding date).
        :param output_dataset_id: Dataset ID for the output data.
        :param output_table_id: Table ID for the output data.
        :apram dataset_location: Location of GCP servers.
        :param isbn: Name of the isbn field in source table.
        """

        isbn_utils_sql = self.get_isbn_utils_sql_string()

        isbn_validate_template_file = "validate_isbn.sql.jinja2"
        template_path = os.path.join(workflow_sql_templates_path(), isbn_validate_template_file)

        sql = render_template(
            template_path,
            project_id=project_id,
            dataset_id=orig_dataset_id,
            table_id=orig_table_id,
            isbn=isbn,
        )

        sql = isbn_utils_sql + sql

        create_bigquery_dataset(
            project_id=project_id,
            dataset_id=output_dataset_id,
            location=dataset_location,
        )

        status = create_bigquery_table_from_query(
            sql=sql,
            project_id=project_id,
            dataset_id=output_dataset_id,
            table_id=output_table_id,
            location=dataset_location,
        )

        if status != True:
            raise AirflowException(
                f"create_bigquery_table_from_query failed on {project_id}.{output_dataset_id}.{output_table_id}"
            )

    def create_oaebu_data_qa_jstor_tasks(self, data_partner: OaebuPartners):
        """Create JSTOR quality assurance metrics.
        :param data_partner: OaebuPartner metadata.
        """

        # isbn validation
        fn = partial(
            self.create_oaebu_data_qa_jstor_isbn,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            table_date=data_partner.gcp_table_date,
        )
        update_wrapper(fn, self.create_oaebu_data_qa_jstor_isbn)
        self.add_task(fn)

    def create_oaebu_data_qa_jstor_isbn(
        self,
        releases: List[OnixWorkflowRelease],
        *args,
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        table_date: Union[None, pendulum.Pendulum],
        **kwargs,
    ):
        """Create a BQ table of invalid ISBNs for the JSTOR feed.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param releases: List of workflow release objects.
        :param project_id: GCP project ID.
        :param orig_dataset_id: Dataset ID for jstor data.
        :param orig_table: Table ID for the jstor data.
        :table_date: Table suffix of jstor release if it exists.
        """

        for release in releases:
            release_date = table_date
            if release_date is None:
                release_date = select_table_suffixes(
                    project_id=project_id,
                    dataset_id=orig_dataset_id,
                    table_id=orig_table,
                    end_date=release.release_date,
                )[0]

            # select table suffixes to get table suffix
            output_dataset_id = release.oaebu_data_qa_dataset
            orig_table_id = orig_table + release_date.strftime("%Y%m%d")

            # Validate the ISBN field
            output_table = "jstor_invalid_isbn"
            output_table_id = bigquery_partitioned_table_id(output_table, release_date)
            dataset_location = release.dataset_location
            self.oaebu_data_qa_validate_isbn(
                project_id=project_id,
                orig_dataset_id=orig_dataset_id,
                orig_table_id=orig_table_id,
                output_dataset_id=output_dataset_id,
                output_table_id=output_table_id,
                dataset_location=dataset_location,
                isbn="ISBN",
            )

            # Validate the eISBN field
            output_table = "jstor_invalid_eisbn"
            output_table_id = bigquery_partitioned_table_id(output_table, release_date)
            self.oaebu_data_qa_validate_isbn(
                project_id=project_id,
                orig_dataset_id=orig_dataset_id,
                orig_table_id=orig_table_id,
                output_dataset_id=output_dataset_id,
                output_table_id=output_table_id,
                dataset_location=dataset_location,
                isbn="eISBN",
            )

    def create_oaebu_data_qa_google_books_sales_tasks(self, data_partner: OaebuPartners):
        """Create Google Books Sales quality assurance metrics.
        :param data_partner: OaebuPartner metadata.
        """

        # isbn validation
        fn = partial(
            self.create_oaebu_data_qa_google_books_sales_isbn,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            table_date=data_partner.gcp_table_date,
        )
        update_wrapper(fn, self.create_oaebu_data_qa_google_books_sales_isbn)
        self.add_task(fn)

    def create_oaebu_data_qa_google_books_sales_isbn(
        self,
        releases: List[OnixWorkflowRelease],
        *args,
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        table_date: Union[None, pendulum.Pendulum],
        **kwargs,
    ):
        """Create a BQ table of invalid ISBNs for the Google Books Sales feed.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param releases: List of workflow release objects.
        :param project_id: GCP project ID.
        :param orig_dataset_id: Dataset ID for jstor data.
        :param orig_table: Table ID for the jstor data.
        :table_date: Table suffix of jstor release if it exists.
        """
        for release in releases:
            release_date = table_date
            if release_date is None:
                release_date = select_table_suffixes(
                    project_id=project_id,
                    dataset_id=orig_dataset_id,
                    table_id=orig_table,
                    end_date=release.release_date,
                )[0]

            # select table suffixes to get table suffix
            output_dataset_id = release.oaebu_data_qa_dataset
            orig_table_id = orig_table + release_date.strftime("%Y%m%d")

            # Validate the ISBN field
            output_table = "google_books_sales_invalid_isbn"
            output_table_id = bigquery_partitioned_table_id(output_table, release_date)
            dataset_location = release.dataset_location
            self.oaebu_data_qa_validate_isbn(
                project_id=project_id,
                orig_dataset_id=orig_dataset_id,
                orig_table_id=orig_table_id,
                output_dataset_id=output_dataset_id,
                output_table_id=output_table_id,
                dataset_location=dataset_location,
                isbn="Primary_ISBN",
            )

    def create_oaebu_data_qa_google_books_traffic_tasks(self, data_partner: OaebuPartners):
        """Create Google Books Traffic quality assurance metrics.
        :param data_partner: OaebuPartner metadata.
        """

        # isbn validation
        fn = partial(
            self.create_oaebu_data_qa_google_books_traffic_isbn,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            table_date=data_partner.gcp_table_date,
        )
        update_wrapper(fn, self.create_oaebu_data_qa_google_books_traffic_isbn)
        self.add_task(fn)

    def create_oaebu_data_qa_google_books_traffic_isbn(
        self,
        releases: List[OnixWorkflowRelease],
        *args,
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        table_date: Union[None, pendulum.Pendulum],
        **kwargs,
    ):
        """Create a BQ table of invalid ISBNs for the Google Books Traffic feed.
        No attempt is made to normalise the string so we catch as many string issues as we can.

        :param releases: List of workflow release objects.
        :param project_id: GCP project ID.
        :param orig_dataset_id: Dataset ID for jstor data.
        :param orig_table: Table ID for the jstor data.
        :table_date: Table suffix of jstor release if it exists.
        """
        for release in releases:
            release_date = table_date
            if release_date is None:
                release_date = select_table_suffixes(
                    project_id=project_id,
                    dataset_id=orig_dataset_id,
                    table_id=orig_table,
                    end_date=release.release_date,
                )[0]

            # select table suffixes to get table suffix
            output_dataset_id = release.oaebu_data_qa_dataset
            orig_table_id = orig_table + release_date.strftime("%Y%m%d")

            # Validate the ISBN field
            output_table = "google_books_traffic_invalid_isbn"
            output_table_id = bigquery_partitioned_table_id(output_table, release_date)
            dataset_location = release.dataset_location
            self.oaebu_data_qa_validate_isbn(
                project_id=project_id,
                orig_dataset_id=orig_dataset_id,
                orig_table_id=orig_table_id,
                output_dataset_id=output_dataset_id,
                output_table_id=output_table_id,
                dataset_location=dataset_location,
                isbn="Primary_ISBN",
            )

    def create_oaebu_data_qa_intermediate_tasks(self, data_partner: OaebuPartners):
        """Create tasks for generating oaebu intermediate metrics for each OAEBU data partner.
        :param oaebu_data: List of oaebu partner data.
        """

        # Record unmatched work_id. This is indicative of invalid ISBN or missing ONIX product records.

        fn = partial(
            self.create_oaebu_data_qa_intermediate_unmatched_workid,
            project_id=data_partner.gcp_project_id,
            orig_dataset_id=data_partner.gcp_dataset_id,
            orig_table=data_partner.gcp_table_id,
            orig_isbn=data_partner.isbn_field_name,
        )

        # Populate the __name__ attribute of the partial object (it lacks one by default).
        # Scheme: create_oaebu_intermediate_table.dataset.table
        update_wrapper(fn, self.create_oaebu_data_qa_intermediate_unmatched_workid)
        fn.__name__ += f".{data_partner.gcp_dataset_id}.{data_partner.gcp_table_id}"

        self.add_task(fn)

        # Other tasks

    def create_oaebu_data_qa_intermediate_unmatched_workid(
        self,
        releases: List[OnixWorkflowRelease],
        project_id: str,
        orig_dataset_id: str,
        orig_table: str,
        orig_isbn: str,
        *args,
        **kwargs,
    ):
        """Create quality assurance metrics for the OAEBU intermediate tables.
        :param oaebu_data: List of workflow release objects.
        :param project_id: GCP Project ID for the data.
        :param orig_dataset_id: Dataset ID of the original partner data.
        :param orig_table: Original table name for the partner data.
        :param orig_isbn: Name of the ISBN field we're checking.
        """

        template_file = "oaebu_intermediate_metrics.sql.jinja2"
        template_path = os.path.join(workflow_sql_templates_path(), template_file)

        for release in releases:
            intermediate_table = f"{orig_dataset_id}_{orig_table}{release.oaebu_intermediate_match_suffix}"
            release_date = select_table_suffixes(
                project_id=project_id,
                dataset_id=release.oaebu_intermediate_dataset,
                table_id=intermediate_table,
                end_date=release.release_date,
            )[0]

            intermediate_table_id = intermediate_table + release_date.strftime("%Y%m%d")

            output_dataset = release.oaebu_data_qa_dataset
            output_table = f"{orig_table}_unmatched_{orig_isbn}"
            output_table_id = bigquery_partitioned_table_id(output_table, release_date)

            sql = render_template(
                template_path,
                project_id=project_id,
                dataset_id=release.oaebu_intermediate_dataset,
                table_id=intermediate_table_id,
                isbn=orig_isbn,
            )

            create_bigquery_dataset(
                project_id=release.project_id,
                dataset_id=release.oaebu_data_qa_dataset,
                location=release.dataset_location,
            )

            status = create_bigquery_table_from_query(
                sql=sql,
                project_id=release.project_id,
                dataset_id=release.oaebu_data_qa_dataset,
                table_id=output_table_id,
                location=release.dataset_location,
            )

            if status != True:
                raise AirflowException(
                    f"create_bigquery_table_from_query failed on {release.project_id}.{output_dataset}.{output_table_id}"
                )
