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

# Author: James Diprose

from __future__ import annotations

import glob
import json
import logging
import os
import re
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from typing import List
from typing import Optional

import elasticsearch.exceptions
import google.cloud.bigquery as bigquery
import pendulum
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance
from airflow.operators.sensors import ExternalTaskSensor
from natsort import natsorted
from pendulum import Pendulum, Date

from observatory.platform.elastic.elastic import (
    Elastic,
    make_sharded_index,
    make_elastic_uri,
    elastic_mappings_path,
)
from observatory.platform.elastic.kibana import Kibana, ObjectType
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease
from observatory.platform.telescopes.telescope import Telescope
from observatory.platform.utils.airflow_utils import AirflowVars, AirflowConns
from observatory.platform.utils.file_utils import yield_jsonl, yield_csv
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    select_table_shard_dates,
    download_blobs_from_cloud_storage,
)
from observatory.platform.utils.jinja2_utils import render_template

CSV_TYPES = ["csv", "csv.gz"]
JSONL_TYPES = ["jsonl", "jsonl.gz"]


def make_index_prefix(table_id: str):
    return table_id.replace("_", "-")


def make_table_prefix(table_id: str):
    return table_id[:-8]


def extract_table_id(table_id: str):
    results = re.search(r"\d{8}$", table_id)

    if results is None:
        return table_id, None

    return table_id[:-8], results.group(0)


def list_table_ids(project_id: str, dataset_id: str, release_date: pendulum.Pendulum):
    src_client = bigquery.Client(project=project_id)

    table_ids = []
    table_id_set = set()
    tables = src_client.list_tables(dataset_id, max_results=10000)
    for table in tables:
        table_id, shard_date = extract_table_id(table.table_id)
        table_id_set.add(table_id)

    # For all sharded tables, find the latest version and add it to table_ids
    for table_id in table_id_set:
        table_dates = select_table_shard_dates(project_id, dataset_id, table_id, release_date)
        if len(table_dates):
            table_ids.append(bigquery_sharded_table_id(table_id, table_dates[0]))
        else:
            print(f"Error: {table_id}")

    return table_ids


def export_bigquery_table(
    project_id: str, dataset_id: str, table_id: str, location: str, file_type: str, destination_uri: str
) -> bool:
    """ Export a BigQuery table.

    :param project_id: the Google Cloud project ID.
    :param dataset_id: the BigQuery dataset ID of the Observatory Platform dataset.
    :param table_id: the name of the BigQuery table.
    :param location: the location of the BigQuery dataset.
    :param file_type: the type of file to save the exported data as; csv or jsonl.
    :param destination_uri: the Google Cloud storage bucket destination URI.
    :return: whether the dataset was exported successfully or not.
    """

    # Set destination format
    if file_type in CSV_TYPES:
        destination_format = bigquery.DestinationFormat.CSV
    elif file_type in JSONL_TYPES:
        destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    else:
        raise ValueError(f"export_bigquery_table: file type '{file_type}' is not supported")

    # Create and run extraction job
    client = bigquery.Client()
    source_table_id = f"{project_id}.{dataset_id}.{table_id}"
    extract_job_config = bigquery.ExtractJobConfig()

    # Set gz compression if file type ends in .gz
    if file_type.endswith(".gz"):
        extract_job_config.compression = bigquery.Compression.GZIP

    extract_job_config.destination_format = destination_format
    extract_job: bigquery.ExtractJob = client.extract_table(
        source_table_id, destination_uri, job_config=extract_job_config, location=location
    )
    extract_job.result()

    return extract_job.state == "DONE"


def load_elastic_index(
    data_path: str,
    table_id: str,
    release_date: Date,
    file_type: str,
    elastic_host: str,
    chunk_size: int,
    num_threads: int,
) -> bool:
    """ Load an observatory index into Elasticsearch.

    :param data_path: the path to the data.
    :param table_id: the id of the table that will be loaded into Elasticsearch.
    :param file_type: the file type of the data that will be loaded.
    :param release_date: the release date.
    :param elastic_host: the full Elasticsearch host including username and password.
    :param chunk_size: the size of the batches to load.
    :param num_threads: the number of threads to use for loading.
    :return: whether the data loading successfully or not.
    """

    results = []

    # Break table_id into various properties
    table_prefix = make_table_prefix(table_id)
    index_prefix = make_index_prefix(table_prefix)
    aggregate, facet = table_prefix.split("_", 1)
    mappings_file_name = "relations_mappings.json.jinja2"
    is_fixed_facet = facet in ["unique_list", "access_types", "disciplines", "output_types", "events", "metrics"]
    if is_fixed_facet:
        mappings_file_name = f"{facet}_mappings.json.jinja2"

    # Make mapping for aggregation and facet
    mappings_path = elastic_mappings_path(mappings_file_name)
    mappings_render = render_template(mappings_path, aggregate=aggregate, facet=facet)
    mappings = json.loads(mappings_render)

    # Fetch all files that should be loaded into this index
    file_pattern = os.path.join(data_path, f"{table_id}_*.{file_type}")
    file_paths = natsorted(glob.glob(file_pattern))

    if len(file_paths) == 0:
        # If no files found then set result to False
        results.append(False)
    else:
        # Load function
        if file_type in CSV_TYPES:
            load_func = yield_csv
        elif file_type in JSONL_TYPES:
            load_func = yield_jsonl
        else:
            raise ValueError(f"load_index: file type '{file_type}' is not supported")

        client = Elastic(host=elastic_host, chunk_size=chunk_size, thread_count=num_threads)

        # Delete existing index
        index_id_sharded = make_sharded_index(index_prefix, release_date)
        client.delete_index(index_id_sharded)

        # Load files into index
        for file_path in file_paths:
            try:
                result = client.index_documents(index_id_sharded, mappings, load_func(file_path))
            except Exception as e:
                logging.error(f"load_index error: {e}")
                result = False
            results.append(result)

    return all(results)


class ElasticImportRelease(SnapshotRelease):
    MAX_PARALLEL_QUERIES = 100

    def __init__(
        self,
        *,
        dag_id: str,
        release_date: Pendulum,
        dataset_id: str,
        file_type: str,
        table_ids: List,
        project_id: str,
        bucket_name: str,
        data_location: str,
        elastic_host: str,
        kibana_host: str,
        kibana_username: str,
        kibana_password: str,
        kibana_spaces: List,
        chunk_size: int = 10000,
        num_threads: int = 2,
        num_workers: int = cpu_count(),
    ):
        super().__init__(dag_id, release_date, "", "", "")
        self.dataset_id = dataset_id
        self.file_type = file_type
        self.table_ids = table_ids
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.data_location = data_location
        self.elastic_host = elastic_host
        self.kibana_host = kibana_host
        self.kibana_username = kibana_username
        self.kibana_password = kibana_password
        self.kibana_spaces = kibana_spaces
        self.chunk_size = chunk_size
        self.num_threads = num_threads
        self.num_workers = num_workers
        self.bucket_prefix = f"telescopes/{dag_id}/{self.release_id}"

    def export_bigquery_tables(self):
        # Calculate the number of parallel queries. Since all of the real work is done on BigQuery run each export task
        # in a separate thread so that they can be done in parallel.
        num_queries = min(len(self.table_ids), self.MAX_PARALLEL_QUERIES)

        results = []
        with ThreadPoolExecutor(max_workers=num_queries) as executor:
            futures = list()
            futures_msgs = {}
            for table_id in self.table_ids:
                destination_uri = f"gs://{self.bucket_name}/{self.bucket_prefix}/{table_id}_*.{self.file_type}"
                msg = f"Exporting table_id={table_id} to: {destination_uri}"
                logging.info(msg)
                future = executor.submit(
                    export_bigquery_table,
                    self.project_id,
                    self.dataset_id,
                    table_id,
                    self.data_location,
                    self.file_type,
                    destination_uri,
                )
                futures.append(future)
                futures_msgs[future] = msg

            # Wait for completed tasks
            for future in as_completed(futures):
                success = future.result()
                msg = futures_msgs[future]
                results.append(success)
                if success:
                    logging.info(f"Export success: {msg}")
                else:
                    logging.error(f"Export failed: {msg}")

        if not all(results):
            raise AirflowException("export_bigquery_tables task: failed to export tables")

    def download_exported_data(self):
        success = download_blobs_from_cloud_storage(self.bucket_name, self.bucket_prefix, self.download_folder)

        if not success:
            raise AirflowException(
                "download_exported_data task: data failed to download from " "Google Cloud Storage successfully"
            )

    def import_to_elastic(self):
        results = []
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = list()
            futures_msgs = {}

            # Load each table into an Elastic index
            for table_id in self.table_ids:
                logging.info(f"Starting indexing task: {table_id}")
                future = executor.submit(
                    load_elastic_index,
                    self.download_folder,
                    table_id,
                    self.release_date,
                    self.file_type,
                    self.elastic_host,
                    self.chunk_size,
                    self.num_threads,
                )

                futures.append(future)
                futures_msgs[future] = table_id

            # Wait for completed tasks
            for future in as_completed(futures):
                success = future.result()
                msg = futures_msgs[future]
                results.append(success)
                if success:
                    logging.info(f"Loading index success: {msg}")
                else:
                    logging.error(f"Loading index failed: {msg}")

        if not all(results):
            raise AirflowException("import_to_elastic task: failed to load Elasticsearch indexes")

    def update_elastic_aliases(self):
        client = Elastic(host=self.elastic_host)

        # Make aliases and indexes
        aliases = []
        indexes = []
        for table_id in self.table_ids:
            table_prefix = make_table_prefix(table_id)
            alias = make_index_prefix(table_prefix)
            index = make_sharded_index(alias, self.release_date)
            aliases.append(alias)
            indexes.append(index)

        # Create actions for deleting all indexes currently assigned to aliases
        actions = []
        for alias in aliases:
            current_alias_indexes = client.get_alias_indexes(alias)
            for alias_index in current_alias_indexes:
                actions.append({"remove": {"index": alias_index, "alias": alias}})

        # Create actions to add new index aliases
        for alias, index in zip(aliases, indexes):
            actions.append({"add": {"index": index, "alias": alias}})

        # Update all aliases at once
        success = False
        try:
            result = client.es.indices.update_aliases({"actions": actions})
            success = result.get("acknowledged", False)
        except elasticsearch.exceptions.NotFoundError:
            pass

        if not success:
            raise AirflowException("update_elastic_aliases failed")

    def create_kibana_index_patterns(self):
        kibana = Kibana(host=self.kibana_host, username=self.kibana_username, password=self.kibana_password)

        results = []
        for table_id in self.table_ids:
            table_prefix = make_table_prefix(table_id)
            index_pattern_id = make_index_prefix(table_prefix)
            attributes = {"title": index_pattern_id, "timeFieldName": "published_year"}

            # Create an index pattern for each space
            for space_id in self.kibana_spaces:
                result = kibana.create_object(
                    ObjectType.index_pattern, index_pattern_id, attributes=attributes, space_id=space_id, exists_ok=True
                )
                results.append(result)

        if not all(results):
            raise AirflowException("create_kibana_index_patterns failed")


class ElasticImportWorkflow(Telescope):
    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        bucket_name: str,
        data_location="us",
        file_type: str = "csv.gz",
        sensor_dag_ids: List[str] = None,
        kibana_spaces: List[str] = None,
        dag_id: Optional[str] = "elastic_import",
        start_date: Optional[Pendulum] = Pendulum(2020, 11, 1),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        airflow_vars: List = None,
        airflow_conns: List = None,
    ):
        """ Create the DoiWorkflow.
        :param project_id: the project id to import data from.
        :param dataset_id: the dataset id to import data from.
        :param bucket_name: the bucket name where the exported BigQuery data will be saved.
        :param data_location: the location of?
        :param file_type:  the file type to import, can be csv or jsonl.
        :param sensor_dag_ids: a list of the DAG ids to wait for with sensors.
        :param kibana_spaces: the kibana spaces to update after Elastic indexes.
        :param dag_id: the DAG id.
        :param start_date: the start date.
        :param schedule_interval: the schedule interval.
        :param catchup: whether to catchup.
        :param airflow_vars: the required Airflow Variables.
        :param airflow_conns: the required Airflow Connections.
        """

        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH]

        if airflow_conns is None:
            airflow_conns = [AirflowConns.ELASTIC, AirflowConns.KIBANA]

        # Initialise Telesecope base class
        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
        )

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.data_location = data_location
        self.file_type = file_type

        self.sensor_dag_ids = sensor_dag_ids
        if sensor_dag_ids is None:
            self.sensor_dag_ids = []

        self.kibana_spaces = kibana_spaces
        if kibana_spaces is None:
            self.kibana_spaces = []

        # Add sensors
        for ext_dag_id in self.sensor_dag_ids:
            sensor = ExternalTaskSensor(task_id=f"{ext_dag_id}_sensor", external_dag_id=ext_dag_id, mode="reschedule")
            self.add_sensor(sensor)

        # Setup tasks
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.list_release_info)

        # Tasks
        self.add_task(self.export_bigquery_tables)
        self.add_task(self.download_exported_data)
        self.add_task(self.import_to_elastic)
        self.add_task(self.update_elastic_aliases)
        self.add_task(self.create_kibana_index_patterns)
        self.add_task(self.cleanup)

    def list_release_info(self, **kwargs):
        """ List the table ids that should be exported.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return:
        """

        # Get release date
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()
        table_ids = list_table_ids(self.project_id, self.dataset_id, release_date)
        table_ids = [table_id for table_id in table_ids if table_id.startswith("institution")]

        # Push table ids and release date
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(Telescope.RELEASE_INFO, {"release_date": release_date, "table_ids": table_ids})

        return True

    def make_release(self, **kwargs) -> ElasticImportRelease:
        """Make a release instance. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: A release instance or list of release instances
        """

        ti: TaskInstance = kwargs["ti"]
        record = ti.xcom_pull(
            key=Telescope.RELEASE_INFO, task_ids=self.list_release_info.__name__, include_prior_dates=False
        )

        release_date = record["release_date"]
        table_ids = record["table_ids"]
        scheme = "https"

        # Get Airflow connections
        elastic_conn = BaseHook.get_connection(AirflowConns.ELASTIC)
        elastic_host = make_elastic_uri(
            scheme, elastic_conn.login, elastic_conn.password, elastic_conn.host, elastic_conn.port
        )
        kibana_conn = BaseHook.get_connection(AirflowConns.KIBANA)
        kibana_host = f"{scheme}://{kibana_conn.host}:{kibana_conn.port}"

        return ElasticImportRelease(
            dag_id=self.dag_id,
            release_date=release_date,
            dataset_id=self.dataset_id,
            file_type=self.file_type,
            table_ids=table_ids,
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            data_location=self.data_location,
            elastic_host=elastic_host,
            kibana_host=kibana_host,
            kibana_username=kibana_conn.login,
            kibana_password=kibana_conn.password,
            kibana_spaces=self.kibana_spaces,
        )

    def export_bigquery_tables(self, release: ElasticImportRelease, **kwargs):
        """ Export tables from BigQuery.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        release.export_bigquery_tables()

    def download_exported_data(self, release: ElasticImportRelease, **kwargs):
        """ Download the exported data.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        release.download_exported_data()

    def import_to_elastic(self, release: ElasticImportRelease, **kwargs):
        """ Import the data into Elasticsearch.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        # pull xcom

        release.import_to_elastic()

    def update_elastic_aliases(self, release: ElasticImportRelease, **kwargs):
        """ Update Elasticsearch aliases.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        release.update_elastic_aliases()

    def create_kibana_index_patterns(self, release: ElasticImportRelease, **kwargs):
        """ Create Kibana index patterns.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        release.create_kibana_index_patterns()

    def cleanup(self, release: ElasticImportRelease, **kwargs):
        """ Cleanup local files.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        release.cleanup()
