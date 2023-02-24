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
from pydoc import locate
from typing import Callable, Dict, List, Optional, Union

import airflow.hooks.base
import elasticsearch.exceptions
import pendulum
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.sensors.external_task import ExternalTaskSensor
from elasticsearch.helpers.errors import BulkIndexError
from natsort import natsorted
from pendulum import Date

from observatory.platform.bigquery import (
    bq_list_tables,
    bq_export_table,
    bq_table_id_parts,
)
from observatory.platform.elastic.elastic import (
    Elastic,
    KeepInfo,
    KeepOrder,
    make_sharded_index,
)
from observatory.platform.elastic.kibana import Kibana, ObjectType, TimeField
from observatory.platform.files import load_file, write_to_file, yield_csv, yield_jsonl
from observatory.platform.gcs import gcs_download_blobs, gcs_blob_name_from_path
from observatory.platform.observatory_config import CloudWorkspace
from observatory.platform.workflows.workflow import Workflow, SnapshotRelease, cleanup

ELASTIC_CONN_ID = "elastic_main"
KIBANA_CONN_ID = "kibana_main"
MAX_PARALLEL_QUERIES = 100

CSV_TYPES = ["csv", "csv.gz"]
JSONL_TYPES = ["jsonl", "jsonl.gz"]


def default_index_keep_info():
    """Construct a default lookup dictionary for index_keep_info settings."""

    keep_info = {"": KeepInfo(ordering=KeepOrder.newest, num=2)}
    return keep_info


@dataclass
class ElasticImportConfig:
    elastic_mappings_path: str = None
    elastic_mappings_func: Callable = None
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


def load_elastic_index(
    *,
    data_path: str,
    table_id: str,
    snapshot_date: Date,
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
    :param snapshot_date: the release date.
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
    _, _, _, table_name, _ = bq_table_id_parts(table_id)
    index_prefix = make_index_prefix(table_name)

    # Fetch all files that should be loaded into this index
    file_pattern = os.path.join(data_path, f"{table_id}_*.{file_type}")
    file_paths = natsorted(glob.glob(file_pattern))

    # Load mappings file
    mappings = elastic_mappings_func(elastic_mappings_folder, table_name)

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
        index_id_sharded = make_sharded_index(index_prefix, snapshot_date)
        client.delete_index(index_id_sharded)

        # Load files into index
        for file_path in file_paths:
            logging.info(f"Loading file: {file_path}")
            try:
                result = client.index_documents(index_id_sharded, mappings, load_func(file_path))
            except BulkIndexError as e:
                logging.error(f"BulkIndexError loading file: {file_path}, {e}")
                for error in e.errors:
                    logging.error(error)
                result = False
            except Exception as e:
                logging.error(f"Loading file error: {file_path}, {type(e).__name__}, {e.args}")
                result = False
            results.append(result)

    return all(results)


def get_keep_info(*, index: str, index_keep_info: Dict) -> KeepInfo:
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


def get_kibana_time_field(kibana_time_fields: List[TimeField], index_pattern_id):
    time_field_name = None

    for time_field in kibana_time_fields:
        if re.match(time_field.pattern, index_pattern_id):
            time_field_name = time_field.field_name
            break

    return time_field_name


def get_elastic_conn(elastic_conn_id: str):
    elastic_conn = airflow.hooks.base.BaseHook.get_connection(elastic_conn_id)
    elastic_host = f"{elastic_conn.conn_type}://{elastic_conn.host}:{elastic_conn.port}"
    elastic_api_key_id = elastic_conn.login
    elastic_api_key = elastic_conn.password
    return elastic_host, elastic_api_key_id, elastic_api_key


def get_kibana_conn(kibana_conn_id: str):
    kibana_conn = airflow.hooks.base.BaseHook.get_connection(kibana_conn_id)
    kibana_host = f"{kibana_conn.conn_type}://{kibana_conn.host}:{kibana_conn.port}"
    kibana_api_key_id = kibana_conn.login
    kibana_api_key = kibana_conn.password

    return kibana_host, kibana_api_key_id, kibana_api_key


class ElasticImportRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        snapshot_date: pendulum.DateTime,
        table_ids: List,
    ):
        super().__init__(dag_id=dag_id, run_id=run_id, snapshot_date=snapshot_date)
        self.table_ids = table_ids
        self.bucket_prefix = gcs_blob_name_from_path(self.download_folder)
        self.elastic_import_task_state_path = os.path.join(self.extract_folder, "elastic_import_task_state.json")

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


class ElasticImportWorkflow(Workflow):
    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        sensor_dag_ids: List[str],
        kibana_spaces: List[str],
        elastic_import_config: Union[ElasticImportConfig, str],
        bq_dataset_id: str = "data_export",
        elastic_conn_id: str = ELASTIC_CONN_ID,
        kibana_conn_id: str = KIBANA_CONN_ID,
        file_type: str = "jsonl.gz",
        chunk_size: int = 10000,
        num_threads: int = 2,
        num_workers: int = cpu_count(),
        start_date: Optional[pendulum.DateTime] = pendulum.datetime(2020, 11, 1),
        schedule_interval: Optional[str] = "@weekly",
        tags: List[str] = None,
        **kwargs,
    ):
        """Create the ElasticImportWorkflow.

        :param dag_id: the DAG id.
        :param cloud_workspace: the project id to import data from.
        :param sensor_dag_ids: a list of the DAG ids to wait for with sensors.
        :param kibana_spaces: the kibana spaces to update after Elastic indexes.
        :param elastic_import_config: Elastic Import configuration. Either an ElasticImportConfig
        instance or a fully qualified path to an ElasticImportConfig instance.
        :param bq_dataset_id: the BigQuery dataset id to import data from.
        :param elastic_conn_id: the id of the Airflow connection with elasticsearch info
        :param kibana_conn_id: the id of the Airflow connection with kibana info
        :param file_type: the file type to import, can be csv or jsonl.
        :param chunk_size:
        :param num_threads:
        :param num_workers:
        :param start_date: the start date.
        :param schedule_interval: the schedule interval.
        :param tags: List of dag tags.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=False,
            airflow_conns=[elastic_conn_id, kibana_conn_id],
            tags=tags,
            **kwargs,
        )

        self.project_id = cloud_workspace.project_id
        self.bucket_name = cloud_workspace.transform_bucket
        self.data_location = cloud_workspace.data_location
        self.bq_dataset_id = bq_dataset_id
        self.elastic_conn_id = elastic_conn_id
        self.kibana_conn_id = kibana_conn_id
        self.file_type = file_type
        self.chunk_size = chunk_size
        self.num_threads = num_threads
        self.num_workers = num_workers

        if isinstance(elastic_import_config, str):
            elastic_import_config = locate(elastic_import_config)
        self.elastic_import_config = elastic_import_config  # this variable is here for tests

        self.elastic_mappings_folder = elastic_import_config.elastic_mappings_path
        self.elastic_mappings_func = elastic_import_config.elastic_mappings_func
        self.kibana_time_fields = elastic_import_config.kibana_time_fields
        self.index_keep_info = (
            elastic_import_config.index_keep_info
            if elastic_import_config.index_keep_info is not None
            else default_index_keep_info()
        )

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

        # Select the latest table ids before the snapshot
        # Get table ids
        all_table_ids = bq_list_tables(self.project_id, self.bq_dataset_id)

        # Create a dictionary to store the latest table IDs for each table name
        latest_table_ids = dict()
        snapshot_date = kwargs["next_execution_date"].subtract(microseconds=1).date()
        snapshot_date = pendulum.datetime(snapshot_date.year, snapshot_date.month, snapshot_date.day)
        for table_id in all_table_ids:
            _, _, _, table_name, shard_date = bq_table_id_parts(table_id)
            if shard_date is None:
                raise AirflowException(
                    f"list_release_info: all data export tables should be sharded, however {table_id} is not."
                )

            # Only include tables made on or before the current snapshot_date
            if shard_date > snapshot_date:
                continue

            # Sort tables by their shard date
            if table_name not in latest_table_ids:
                latest_table_ids[table_name] = table_id
            else:
                latest_table_id = latest_table_ids[table_name]
                _, _, _, _, latest_shard_date = bq_table_id_parts(latest_table_id)
                if shard_date > latest_shard_date:
                    latest_table_ids[shard_date] = table_id

        # Make a list of the latest table_ids
        table_ids = []
        for _, table_id in latest_table_ids.items():
            table_ids.append(table_id)

        # Push table ids and release date
        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(Workflow.RELEASE_INFO, {"snapshot_date": snapshot_date.format("YYYYMMDD"), "table_ids": table_ids})

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

        snapshot_date = pendulum.parse(record["snapshot_date"])
        table_ids = record["table_ids"]

        return ElasticImportRelease(
            dag_id=self.dag_id,
            run_id=kwargs["run_id"],
            snapshot_date=snapshot_date,
            table_ids=table_ids,
        )

    def export_bigquery_tables(self, release: ElasticImportRelease, **kwargs):
        """Export the BigQuery tables to Google Cloud Storage.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        # Calculate the number of parallel queries. Since all of the real work is done on BigQuery run each export task
        # in a separate thread so that they can be done in parallel.
        num_queries = min(len(release.table_ids), MAX_PARALLEL_QUERIES)

        results = []
        with ThreadPoolExecutor(max_workers=num_queries) as executor:
            futures = list()
            futures_msgs = {}
            for table_id in release.table_ids:
                destination_uri = f"gs://{self.bucket_name}/{release.bucket_prefix}/{table_id}_*.{self.file_type}"
                msg = f"Exporting table_id={table_id} to: {destination_uri}"
                logging.info(msg)

                future = executor.submit(
                    bq_export_table,
                    table_id=table_id,
                    file_type=self.file_type,
                    destination_uri=destination_uri,
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

    def download_exported_data(self, release: ElasticImportRelease, **kwargs):
        """Download the exported data.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        success = gcs_download_blobs(
            bucket_name=self.bucket_name, prefix=release.bucket_prefix, destination_path=release.download_folder
        )
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

        results = []
        indexed_table_ids = release.read_import_state()
        logging.info(f"The following tables have already been indexed: {indexed_table_ids}")
        elastic_host, elastic_api_key_id, elastic_api_key = get_elastic_conn(self.elastic_conn_id)

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = list()
            futures_msgs = {}

            # Decide what tables need indexing
            to_index_table_ids = list(set(release.table_ids) - set(indexed_table_ids))
            logging.info(f"The following tables will be indexed: {to_index_table_ids}")

            # Load each table into an Elastic index
            for table_id in to_index_table_ids:
                logging.info(f"Starting indexing task: {table_id}")
                future = executor.submit(
                    load_elastic_index,
                    data_path=release.download_folder,
                    table_id=table_id,
                    snapshot_date=release.snapshot_date,
                    elastic_mappings_folder=self.elastic_mappings_folder,
                    elastic_mappings_func=self.elastic_mappings_func,
                    file_type=self.file_type,
                    elastic_host=elastic_host,
                    elastic_api_key_id=elastic_api_key_id,
                    elastic_api_key=elastic_api_key,
                    chunk_size=self.chunk_size,
                    num_threads=self.num_threads,
                )

                futures.append(future)
                futures_msgs[future] = table_id

            # Wait for completed tasks
            for future in as_completed(futures):
                table_id = futures_msgs[future]
                success = future.result()
                results.append(success)
                if success:
                    # Update the state of table_ids that have been indexed
                    indexed_table_ids.append(table_id)
                    release.write_import_state(indexed_table_ids)
                    logging.info(f"Loading index success: {table_id}")
                else:
                    logging.error(f"Loading index failed: {table_id}")

        if not all(results):
            raise AirflowException("import_to_elastic task: failed to load Elasticsearch indexes")

    def update_elastic_aliases(self, release: ElasticImportRelease, **kwargs):
        """Update Elasticsearch aliases.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        elastic_host, elastic_api_key_id, elastic_api_key = get_elastic_conn(self.elastic_conn_id)
        client = Elastic(host=elastic_host, api_key_id=elastic_api_key_id, api_key=elastic_api_key)

        # Make aliases and indexes
        aliases = []
        indexes = []
        for table_id in release.table_ids:
            _, _, _, table_name, _ = bq_table_id_parts(table_id)
            alias = make_index_prefix(table_name)
            index = make_sharded_index(alias, release.snapshot_date)
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
            result = client.es.indices.update_aliases(actions=actions)
            success = result.get("acknowledged", False)
        except elasticsearch.exceptions.NotFoundError:
            pass

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

        elastic_host, elastic_api_key_id, elastic_api_key = get_elastic_conn(self.elastic_conn_id)
        client = Elastic(host=elastic_host, api_key_id=elastic_api_key_id, api_key=elastic_api_key)

        # If you want to do a global cleanup of all indices, we can just call delete_stale_indices on index="*" instead
        indexed_table_ids = release.read_import_state()

        for table_id in indexed_table_ids:
            _, _, _, table_name, _ = bq_table_id_parts(table_id)
            index_prefix = make_index_prefix(table_name)
            keep_info = get_keep_info(index=index_prefix, index_keep_info=self.index_keep_info)
            client.delete_stale_indices(index=f"{index_prefix}-*", keep_info=keep_info)

    def create_kibana_index_patterns(self, release: ElasticImportRelease, **kwargs):
        """Create Kibana index patterns.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        kibana_host, kibana_api_key_id, kibana_api_key = get_kibana_conn(self.kibana_conn_id)
        kibana = Kibana(host=kibana_host, api_key_id=kibana_api_key_id, api_key=kibana_api_key)

        results = []
        for table_id in release.table_ids:
            _, _, _, table_name, _ = bq_table_id_parts(table_id)
            index_pattern_id = make_index_prefix(table_name)
            attributes = {
                "title": index_pattern_id,
                "timeFieldName": get_kibana_time_field(self.kibana_time_fields, index_pattern_id),
            }

            # Create an index pattern for each space
            for space_id in self.kibana_spaces:
                result = kibana.create_object(
                    ObjectType.index_pattern, index_pattern_id, attributes=attributes, space_id=space_id, exists_ok=True
                )
                results.append(result)

        if not all(results):
            raise AirflowException("create_kibana_index_patterns failed")

    def cleanup(self, release: ElasticImportRelease, **kwargs):
        """Cleanup local files. Deletes old xcoms.

        :param release: the ElasticRelease.
        :param kwargs: the context passed from the Airflow Operator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        cleanup(dag_id=self.dag_id, execution_date=kwargs["execution_date"], workflow_folder=release.workflow_folder)
