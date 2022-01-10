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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from multiprocessing import cpu_count
from typing import Callable, Dict, List, Optional, Tuple

import elasticsearch.exceptions
import google.cloud.bigquery as bigquery
import pendulum
from airflow.exceptions import AirflowException
import airflow.hooks.base
from airflow.models import TaskInstance
from airflow.sensors.external_task import ExternalTaskSensor
from natsort import natsorted
from observatory.platform.elastic.elastic import (
    Elastic,
    KeepInfo,
    KeepOrder,
    make_sharded_index,
)
from observatory.platform.elastic.kibana import Kibana, ObjectType, TimeField
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.file_utils import (
    load_file,
    write_to_file,
    yield_csv,
    yield_jsonl,
)
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    download_blobs_from_cloud_storage,
    select_table_shard_dates,
)
from observatory.platform.utils.workflow_utils import delete_old_xcoms
from observatory.platform.workflows.snapshot_telescope import SnapshotRelease
from observatory.platform.workflows.workflow import Workflow
from pendulum import Date

CSV_TYPES = ["csv", "csv.gz"]
JSONL_TYPES = ["jsonl", "jsonl.gz"]


def default_index_keep_info():
    """Construct a default lookup dictionary for index_keep_info settings."""

    keep_info = {"": KeepInfo(ordering=KeepOrder.newest, num=2)}
    return keep_info


@dataclass
class ElasticImportConfig:
    dag_id: str
    project_id: str
    dataset_id: str
    bucket_name: str
    elastic_conn_key: str
    kibana_conn_key: str
    data_location: str = None
    file_type: str = None
    sensor_dag_ids: List[str] = None
    elastic_mappings_path: str = None
    elastic_mappings_func: Callable = None
    kibana_spaces: List[str] = None
    kibana_time_fields: List[TimeField] = None
    index_keep_info: Dict[str, KeepInfo] = field(default_factory=default_index_keep_info)


def load_elastic_mappings_simple(path: str, table_prefix: str) -> Dict:
    """Load the Elastic mappings for a given table prefix.

    :param path: the path to the Elastic mappings.
    :param table_prefix: the table prefix.
    :return: the rendered mapping as a Dict.
    """

    return json.loads(load_file(os.path.join(path, f"{make_index_prefix(table_prefix)}-mappings.json")))


def make_index_prefix(table_id: str):
    """Convert a table_id into an Elastic / Kibana index.

    :param table_id: the table_id.
    :return: the Elastic / Kibana index.
    """

    return table_id.replace("_", "-")


def make_table_prefix(table_id: str):
    """Remove the date from a table_id.

    :param table_id: the table id including a shard date.
    :return: the table id.
    """

    # -8 is removing the date from the string.
    return table_id[:-8]


def extract_table_id(table_id: str) -> Tuple[str, Optional[str]]:
    """Extract the table_id and and the date from an index.

    :param table_id: the table id.
    :return: table_id and date.
    """

    results = re.search(r"\d{8}$", table_id)

    if results is None:
        return table_id, None

    return make_table_prefix(table_id), results.group(0)


def list_table_ids(project_id: str, dataset_id: str, release_date: pendulum.DateTime) -> List[str]:
    """List all of the table_ids within a BigQuery dataset.

    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :param release_date: the release date.
    :return: the table ids.
    """

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
    """Export a BigQuery table.

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
    *,
    data_path: str,
    table_id: str,
    release_date: Date,
    elastic_mappings_folder: str,
    elastic_mappings_func: Callable,
    file_type: str,
    elastic_host: str,
    elastic_api_key_id: str,
    elastic_api_key: str,
    chunk_size: int,
    num_threads: int,
) -> bool:
    """Load an observatory index into Elasticsearch.

    :param data_path: the path to the data.
    :param table_id: the id of the table that will be loaded into Elasticsearch.
    :param release_date: the release date.
    :param file_type: the file type of the data that will be loaded.
    :param elastic_mappings_folder: the mappings path.
    :param elastic_mappings_func: the mappings Callable.
    :param elastic_host: the full Elasticsearch host.
    :param elastic_api_key_id: the Elastic API key id.
    :param elastic_api_key: the Elastic API key.
    :param chunk_size: the size of the batches to load.
    :param num_threads: the number of threads to use for loading.
    :return: whether the data loading successfully or not.
    """

    results = []

    # Break table_id into various properties
    table_prefix = make_table_prefix(table_id)
    index_prefix = make_index_prefix(table_prefix)

    # Fetch all files that should be loaded into this index
    file_pattern = os.path.join(data_path, f"{table_id}_*.{file_type}")
    file_paths = natsorted(glob.glob(file_pattern))

    # Load mappings file
    mappings = elastic_mappings_func(elastic_mappings_folder, table_prefix)

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

        client = Elastic(
            host=elastic_host,
            chunk_size=chunk_size,
            thread_count=num_threads,
            api_key_id=elastic_api_key_id,
            api_key=elastic_api_key,
        )

        # Delete existing index
        index_id_sharded = make_sharded_index(index_prefix, release_date)
        client.delete_index(index_id_sharded)

        # Load files into index
        for file_path in file_paths:
            logging.info(f"Loading file: {file_path}")
            try:
                result = client.index_documents(index_id_sharded, mappings, load_func(file_path))
            except Exception as e:
                logging.error(f"Loading file error: {file_path}, {e}")
                result = False
            results.append(result)

    return all(results)


class ElasticImportRelease(SnapshotRelease):
    MAX_PARALLEL_QUERIES = 100

    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.DateTime,
        dataset_id: str,
        file_type: str,
        table_ids: List,
        project_id: str,
        bucket_name: str,
        data_location: str,
        elastic_host: str,
        elastic_api_key_id: str,
        elastic_api_key: str,
        elastic_mappings_folder: str,
        elastic_mappings_func: Callable,
        kibana_host: str,
        kibana_api_key_id: str,
        kibana_api_key: str,
        kibana_spaces: List,
        kibana_time_fields: List[TimeField],
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
        self.elastic_api_key_id = elastic_api_key_id
        self.elastic_api_key = elastic_api_key
        self.elastic_mappings_folder = elastic_mappings_folder
        self.elastic_mappings_func = elastic_mappings_func
        self.kibana_host = kibana_host
        self.kibana_api_key_id = kibana_api_key_id
        self.kibana_api_key = kibana_api_key
        self.kibana_spaces = kibana_spaces
        self.kibana_time_fields = kibana_time_fields
        self.chunk_size = chunk_size
        self.num_threads = num_threads
        self.num_workers = num_workers
        self.bucket_prefix = f"telescopes/{dag_id}/{self.release_id}"
        self.elastic_import_task_state_path = os.path.join(self.extract_folder, "elastic_import_task_state.json")

    def get_keep_info(self, *, index: str, index_keep_info: Dict) -> KeepInfo:
        """Get the best KeepInfo for a given index prefix in self.index_keep_info.
        If there is an exact index match, it will return the info for that.  Otherwise it will keep looking for higher
        level matches.  If it completely fails to match, it will return the default configuration.

        Assumes index names are in the format: {level0 name}-{level1 name}-...-{leveln name}.
        Index should not include the -YYYYMMD suffix.

        :param index: Index pattern to match.
        :param index_keep_info: Dictionary of settings on how to retain Elastic indices.
        :return: KeepInfo for an index.
        """

        index_name = index
        while True:
            if index_name == "" and index_name not in index_keep_info:
                break

            if index_name in index_keep_info:
                return index_keep_info[index_name]

            end_ptr = index_name.rfind("-")
            index_name = index_name[:end_ptr]

        # Return a default configuration. Keep newest 2.
        return KeepInfo(ordering=KeepOrder.newest, num=2)

    def export_bigquery_tables(self) -> bool:
        """Export the BigQuery tables to Google Cloud Storage.

        :return: whether the tables were exported successfully.
        """

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

        return all(results)

    def download_exported_data(self) -> bool:
        """Download the exported data from Cloud Storage.

        :return: whether the data was downloaded successfully or not.
        """

        return download_blobs_from_cloud_storage(self.bucket_name, self.bucket_prefix, self.download_folder)

    def read_import_state(self) -> List[str]:
        """Loads which tables have been indexed.

        :return: the list of table ids.
        """

        if os.path.isfile(self.elastic_import_task_state_path):
            return json.loads(load_file(self.elastic_import_task_state_path))
        return []

    def write_import_state(self, indexed_table_ids: List[str]):
        """Saves which tables have been indexed.

        :param indexed_table_ids: the table ids.
        :return: None.
        """

        write_to_file(json.dumps(indexed_table_ids), self.elastic_import_task_state_path)

    def import_to_elastic(self) -> bool:
        """Import data into Elasticsearch.

        :return: whether the data imported successfully or not.
        """

        results = []
        indexed_table_ids = self.read_import_state()
        logging.info(f"The following tables have already been indexed: {indexed_table_ids}")

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = list()
            futures_msgs = {}

            # Decide what tables need indexing
            to_index_table_ids = list(set(self.table_ids) - set(indexed_table_ids))
            logging.info(f"The following tables will be indexed: {to_index_table_ids}")

            # Load each table into an Elastic index
            for table_id in to_index_table_ids:
                logging.info(f"Starting indexing task: {table_id}")
                future = executor.submit(
                    load_elastic_index,
                    data_path=self.download_folder,
                    table_id=table_id,
                    release_date=self.release_date,
                    elastic_mappings_folder=self.elastic_mappings_folder,
                    elastic_mappings_func=self.elastic_mappings_func,
                    file_type=self.file_type,
                    elastic_host=self.elastic_host,
                    elastic_api_key_id=self.elastic_api_key_id,
                    elastic_api_key=self.elastic_api_key,
                    chunk_size=self.chunk_size,
                    num_threads=self.num_threads,
                )

                futures.append(future)
                futures_msgs[future] = table_id

            # Wait for completed tasks
            for future in as_completed(futures):
                success = future.result()
                results.append(success)
                if success:
                    # Update the state of table_ids that have been indexed
                    table_id = futures_msgs[future]
                    indexed_table_ids.append(table_id)
                    self.write_import_state(indexed_table_ids)
                    logging.info(f"Loading index success: {table_id}")
                else:
                    logging.error(f"Loading index failed: {table_id}")

        return all(results)

    def update_elastic_aliases(self) -> bool:
        """Update the elasticsearch aliases.

        :return: whether the aliases updated correctly or not,
        """

        client = Elastic(host=self.elastic_host, api_key_id=self.elastic_api_key_id, api_key=self.elastic_api_key)

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

        return success

    def get_kibana_time_field(self, index_pattern_id):
        time_field_name = None

        for time_field in self.kibana_time_fields:
            if re.match(time_field.pattern, index_pattern_id):
                time_field_name = time_field.field_name
                break

        return time_field_name

    def create_kibana_index_patterns(self) -> bool:
        """Create the Kibana index patterns.

        :return: whether the index patterns were created successfully or not.
        """

        kibana = Kibana(host=self.kibana_host, api_key_id=self.kibana_api_key_id, api_key=self.kibana_api_key)

        results = []
        for table_id in self.table_ids:
            table_prefix = make_table_prefix(table_id)
            index_pattern_id = make_index_prefix(table_prefix)
            attributes = {"title": index_pattern_id, "timeFieldName": self.get_kibana_time_field(index_pattern_id)}

            # Create an index pattern for each space
            for space_id in self.kibana_spaces:
                result = kibana.create_object(
                    ObjectType.index_pattern, index_pattern_id, attributes=attributes, space_id=space_id, exists_ok=True
                )
                results.append(result)

        return all(results)

    def delete_stale_indices(self, index_keep_info: Dict):
        """Delete all the stale indices.

        :param index_keep_info: Dictionary of settings on how to retain Elastic indices.
        """

        client = Elastic(host=self.elastic_host, api_key_id=self.elastic_api_key_id, api_key=self.elastic_api_key)

        # If you want to do a global cleanup of all indices, we can just call delete_stale_indices on index="*" instead
        indexed_table_ids = self.read_import_state()

        for table_id in indexed_table_ids:
            table_prefix = make_table_prefix(table_id)
            index_prefix = make_index_prefix(table_prefix)
            keep_info = self.get_keep_info(index=index_prefix, index_keep_info=index_keep_info)
            client.delete_stale_indices(index=f"{index_prefix}-*", keep_info=keep_info)


class ElasticImportWorkflow(Workflow):
    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        bucket_name: str,
        elastic_conn_key: str,
        kibana_conn_key: str,
        data_location: str = "us",
        file_type: str = "csv.gz",
        sensor_dag_ids: List[str] = None,
        elastic_mappings_folder: str = None,
        elastic_mappings_func: Callable = None,
        kibana_spaces: List[str] = None,
        kibana_time_fields: List[TimeField] = None,
        dag_id: Optional[str] = "elastic_import",
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2020, 11, 1),
        schedule_interval: Optional[str] = "@weekly",
        catchup: Optional[bool] = False,
        airflow_vars: List = None,
        airflow_conns: List = None,
        index_keep_info: Dict = None,
    ):
        """Create the ElasticImportWorkflow.

        :param project_id: the project id to import data from.
        :param dataset_id: the dataset id to import data from.
        :param bucket_name: the bucket name where the exported BigQuery data will be saved.
        :param elastic_conn_key: the key of the Airflow connection with elasticsearch info
        :param kibana_conn_key: the key of the Airflow connection with kibana info
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
        :param index_keep_info: Index retention policy info.
        """

        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH]

        if airflow_conns is None:
            airflow_conns = [elastic_conn_key, kibana_conn_key]

        self.index_keep_info = index_keep_info if index_keep_info is not None else default_index_keep_info()

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
        self.elastic_conn_key = elastic_conn_key
        self.kibana_conn_key = kibana_conn_key
        self.data_location = data_location
        self.file_type = file_type
        self.elastic_mappings_folder = elastic_mappings_folder
        self.elastic_mappings_func = elastic_mappings_func
        self.kibana_time_fields = kibana_time_fields

        self.sensor_dag_ids = sensor_dag_ids
        if sensor_dag_ids is None:
            self.sensor_dag_ids = []

        self.kibana_spaces = kibana_spaces
        if kibana_spaces is None:
            self.kibana_spaces = []

        # Add sensors
        with self.parallel_tasks():
            for ext_dag_id in self.sensor_dag_ids:
                sensor = ExternalTaskSensor(
                    task_id=f"{ext_dag_id}_sensor", external_dag_id=ext_dag_id, mode="reschedule"
                )
                self.add_operator(sensor)

        # Setup tasks
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.list_release_info)

        # Tasks
        self.add_task(self.export_bigquery_tables)
        self.add_task(self.download_exported_data)
        self.add_task(self.import_to_elastic)
        self.add_task(self.update_elastic_aliases)
        self.add_task(self.delete_stale_indices)
        self.add_task(self.create_kibana_index_patterns)
        self.add_task(self.cleanup)

    def list_release_info(self, **kwargs):
        """List the table ids that should be exported.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return:
        """

        # Get release date
        release_date = kwargs["next_execution_date"].subtract(microseconds=1).date()
        table_ids = list_table_ids(self.project_id, self.dataset_id, release_date)

        # Push table ids and release date
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(Workflow.RELEASE_INFO, {"release_date": release_date.format("YYYYMMDD"), "table_ids": table_ids})

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
            key=Workflow.RELEASE_INFO, task_ids=self.list_release_info.__name__, include_prior_dates=False
        )

        release_date = pendulum.parse(record["release_date"])
        table_ids = record["table_ids"]

        # Get Airflow connections
        elastic_conn = airflow.hooks.base.BaseHook.get_connection(self.elastic_conn_key)
        elastic_host = f"{elastic_conn.conn_type}://{elastic_conn.host}:{elastic_conn.port}"
        elastic_api_key_id = elastic_conn.login
        elastic_api_key = elastic_conn.password

        kibana_conn = airflow.hooks.base.BaseHook.get_connection(self.kibana_conn_key)
        kibana_host = f"{kibana_conn.conn_type}://{kibana_conn.host}:{kibana_conn.port}"
        kibana_api_key_id = kibana_conn.login
        kibana_api_key = kibana_conn.password

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
            elastic_api_key_id=elastic_api_key_id,
            elastic_api_key=elastic_api_key,
            elastic_mappings_folder=self.elastic_mappings_folder,
            elastic_mappings_func=self.elastic_mappings_func,
            kibana_host=kibana_host,
            kibana_api_key_id=kibana_api_key_id,
            kibana_api_key=kibana_api_key,
            kibana_spaces=self.kibana_spaces,
            kibana_time_fields=self.kibana_time_fields,
        )

    def export_bigquery_tables(self, release: ElasticImportRelease, **kwargs):
        """Export tables from BigQuery.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        success = release.export_bigquery_tables()
        if not success:
            raise AirflowException("export_bigquery_tables task: failed to export tables")

    def download_exported_data(self, release: ElasticImportRelease, **kwargs):
        """Download the exported data.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        success = release.download_exported_data()
        if not success:
            raise AirflowException(
                "download_exported_data task: data failed to download from " "Google Cloud Storage successfully"
            )

    def import_to_elastic(self, release: ElasticImportRelease, **kwargs):
        """Import the data into Elasticsearch.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        success = release.import_to_elastic()
        if not success:
            raise AirflowException("import_to_elastic task: failed to load Elasticsearch indexes")

    def update_elastic_aliases(self, release: ElasticImportRelease, **kwargs):
        """Update Elasticsearch aliases.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        success = release.update_elastic_aliases()
        if not success:
            raise AirflowException("update_elastic_aliases failed")

    def delete_stale_indices(self, release: ElasticImportRelease, **kwargs):
        """Delete stale Elasticsearch indices.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        release.delete_stale_indices(self.index_keep_info)

    def create_kibana_index_patterns(self, release: ElasticImportRelease, **kwargs):
        """Create Kibana index patterns.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        success = release.create_kibana_index_patterns()
        if not success:
            raise AirflowException("create_kibana_index_patterns failed")

    def cleanup(self, release: ElasticImportRelease, **kwargs):
        """Cleanup local files. Deletes old xcoms.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        release.cleanup()

        execution_date = kwargs["execution_date"]
        delete_old_xcoms(dag_id=self.dag_id, execution_date=execution_date)
