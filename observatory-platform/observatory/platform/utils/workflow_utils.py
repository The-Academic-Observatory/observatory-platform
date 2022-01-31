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

# Author: Tuan Chien, Aniek Roelofs, James Diprose

""" Utility functions that support (almost) all telescopes/the template """

import calendar
import json
import logging
import os
import pathlib
import re
import traceback
from base64 import b64decode
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, List, Optional, Tuple, Union

import paramiko
import pendulum
import pysftp
import six
from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import DagBag, DagRun, Variable, XCom
from airflow.secrets.environment_variables import EnvironmentVariablesBackend
from airflow.utils.db import provide_session
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from observatory.platform.observatory_config import Environment
from observatory.platform.utils.airflow_utils import (
    AirflowConns,
    AirflowVars,
    send_slack_msg,
)
from observatory.platform.utils.api import make_observatory_api
from observatory.platform.utils.config_utils import find_schema, utils_templates_path
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    copy_bigquery_table,
    create_bigquery_dataset,
    load_bigquery_table,
    run_bigquery_query,
    select_table_shard_dates,
    upload_files_to_cloud_storage,
)
from observatory.platform.utils.jinja2_utils import (
    make_sql_jinja2_filename,
    render_template,
)
from sqlalchemy import and_
from sqlalchemy.orm import Session

ScheduleInterval = Union[str, timedelta, relativedelta]


def fetch_dags_modules() -> dict:
    """Get the dags modules from the Airflow Variable

    :return: Dags modules
    """

    # Try to get value from env variable first, saving costs from GC secret usage
    dags_modules_str = EnvironmentVariablesBackend().get_variable(AirflowVars.DAGS_MODULE_NAMES)
    if not dags_modules_str:
        dags_modules_str = Variable.get(AirflowVars.DAGS_MODULE_NAMES)
    logging.info(f"dags_modules str: {dags_modules_str}")
    dags_modules_ = json.loads(dags_modules_str)
    logging.info(f"dags_modules: {dags_modules_}")
    return dags_modules_


def fetch_dag_bag(path: str, include_examples: bool = False) -> DagBag:
    """Load a DAG Bag from a given path.

    :param path: the path to the DAG bag.
    :param include_examples: whether to include example DAGs or not.
    :return: None.
    """
    logging.info(f"Loading DAG bag from path: {path}")
    dag_bag = DagBag(path, include_examples=include_examples)

    if dag_bag is None:
        raise Exception(f"DagBag could not be loaded from path: {path}")

    if len(dag_bag.import_errors):
        # Collate loading errors as single string and raise it as exception
        results = []
        for path, exception in dag_bag.import_errors.items():
            results.append(f"DAG import exception: {path}\n{exception}\n\n")
        raise Exception("\n".join(results))

    return dag_bag


def workflow_path(*sub_dirs) -> str:
    """Return a path for saving telescope data. Create it if it doesn't exist.
    :param sub_dirs: the subdirectories.
    :return: the path.
    """

    logging.info("workflow_path: requesting data_path variable")
    # Try to get value from env variable first, saving costs from GC secret usage
    data_path = EnvironmentVariablesBackend().get_variable(AirflowVars.DATA_PATH)
    if data_path is None:
        data_path = Variable.get(AirflowVars.DATA_PATH)

    sub_dirs_ = [subdir.value if isinstance(subdir, Enum) else subdir for subdir in sub_dirs]

    # Create telescope path
    path = os.path.join(data_path, "telescopes", *sub_dirs_)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    return path


def blob_name(path: str) -> str:
    """Convert a file path into the full path of the Blob, excluding the bucket name.
    E.g.: '/workdir/data/telescopes/transform/dag_id/dag_id_2021_03_01/file.txt' ->
    'telescopes/dag_id/dag_id_2021_03_01/file.txt'

    :param path: the path to the file.
    :return: the full path of the Blob.
    """

    # Remove everything before 'telescopes' and make sure that we are using posix file paths
    # Which use / forward slashes rather than backslashes
    sub_paths = pathlib.Path(os.path.relpath(path, workflow_path())).parts[1:]
    relative_path = pathlib.Path(*sub_paths).as_posix()

    return pathlib.Path(os.path.join("telescopes", relative_path)).as_posix()


def batch_blob_name(path: str) -> str:
    """

    :param path:
    :return:
    """
    return os.path.join(blob_name(path), "*")


def upload_files_from_list(files_list: List[str], bucket_name: str) -> bool:
    """Upload all files in a list to the google cloud download bucket.

    :param files_list: List of full path of files that will be uploaded
    :param bucket_name: The name of the google cloud bucket
    :return: True if upload was successful, else False.
    """
    blob_names = []
    for file_path in files_list:
        blob_names.append(blob_name(file_path))

    success = upload_files_to_cloud_storage(bucket_name, blob_names, files_list)
    if not success:
        raise AirflowException()

    return success


def table_ids_from_path(transform_path: str) -> Tuple[str, str]:
    """
    To create the table ids the basename of the transform path is taken and file extensions are stripped.
    E.g.: /opt/observatory/data/telescopes/transform/telescope/2020_01_01-2020_02_01/telescope.jsonl.gz -> 'telescope'
    and 'telescope_partitions'
    :param transform_path: The full path of a file in the transform folder
    :return: Main table id and partition table id
    """
    logging.info(f"Creating table ids from path: {transform_path}")

    main_table_id = os.path.splitext(pathlib.Path(transform_path).stem)[0]
    partition_table_id = f"{main_table_id}_partitions"

    logging.info(f"Table id: {main_table_id}, partition table id: {partition_table_id}")
    return main_table_id, partition_table_id


def create_date_table_id(table_id: str, date: datetime, partition_type: bigquery.TimePartitioningType):
    """Create a table id string, which includes the date in the correct format corresponding to the partition type.

    :param table_id: The table id
    :param date: The date used for the partition identifier
    :param partition_type: The partition type
    :return: The updated table id
    """
    time_type = bigquery.TimePartitioningType
    type_map = {time_type.HOUR: "%Y%m%d%H", time_type.DAY: "%Y%m%d", time_type.MONTH: "%Y%m", time_type.YEAR: "%Y"}

    date_format = type_map.get(partition_type)
    if date_format is None:
        raise TypeError("Invalid partition type")

    date_str = date.strftime(date_format)

    return f"{table_id}${date_str}"


def prepare_bq_load(
    schema_folder: str,
    dataset_id: str,
    table_id: str,
    release_date: pendulum.DateTime,
    prefix: str,
    schema_version: str,
    dataset_description: str = "",
) -> Tuple[str, str, str, str]:
    """
    Prepare to load data into BigQuery. This will:
     - create the dataset if it does not exist yet
     - get the path to the schema
     - return values of project id, bucket name, and data location
    :param schema_folder: the path to the SQL schemas.
    :param dataset_id: Dataset id.
    :param table_id: Table id.
    :param release_date: The release date used for schema lookup.
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: dataset description.
    :return: The project id, bucket name, data location and schema path
    """

    logging.info("requesting project_id variable")
    project_id = Variable.get(AirflowVars.PROJECT_ID)

    logging.info("requesting transform_bucket variable")
    bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET)

    logging.info("requesting data_location variable")
    data_location = Variable.get(AirflowVars.DATA_LOCATION)

    # Create dataset
    create_bigquery_dataset(project_id, dataset_id, data_location, dataset_description)

    # Select schema file based on release date
    schema_file_path = find_schema(schema_folder, table_id, release_date, prefix, schema_version)
    if schema_file_path is None:
        exit(os.EX_CONFIG)
    return project_id, bucket_name, data_location, schema_file_path


def prepare_bq_load_v2(
    schema_folder: str,
    project_id: str,
    dataset_id: str,
    dataset_location: str,
    table_id: str,
    release_date: pendulum.DateTime,
    prefix: str,
    schema_version: str,
    dataset_description: str = "",
) -> str:
    """
    Prepare to load data into BigQuery. This will:
     - create the dataset if it does not exist yet
     - get the path to the schema
     - return values of project id, bucket name, and data location
    :param schema_folder: the path to the SQL schemas.
    :param project_id: project id.
    :param dataset_id: Dataset id.
    :param dataset_location: location of dataset.
    :param table_id: Table id.
    :param release_date: The release date used for schema lookup.
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: dataset description.
    :return: The project id, bucket name, data location and schema path
    """

    # Create dataset
    dataset_id = dataset_id
    create_bigquery_dataset(project_id, dataset_id, dataset_location, dataset_description)

    # Select schema file based on release date
    schema_file_path = find_schema(schema_folder, table_id, release_date, prefix, schema_version)
    if schema_file_path is None:
        exit(os.EX_CONFIG)
    return schema_file_path


def bq_load_shard(
    schema_folder: str,
    release_date: pendulum.DateTime,
    transform_blob: str,
    dataset_id: str,
    table_id: str,
    source_format: str,
    prefix: str = "",
    schema_version: str = None,
    dataset_description: str = "",
    **load_bigquery_table_kwargs,
):
    """Load data from a specific file (blob) in the transform bucket to a BigQuery shard.
    :param schema_folder: the path to the SQL schema folder.
    :param release_date: Release date.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param table_id: Table id.
    :param source_format: the format of the data to load into BigQuery.
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: description of the BigQuery dataset.
    :return: None.
    """
    _, bucket_name, data_location, schema_file_path = prepare_bq_load(
        schema_folder, dataset_id, table_id, release_date, prefix, schema_version, dataset_description
    )

    # Create table id
    table_id = bigquery_sharded_table_id(table_id, release_date)

    # Load BigQuery table
    uri = f"gs://{bucket_name}/{transform_blob}"
    logging.info(f"URI: {uri}")

    success = load_bigquery_table(
        uri, dataset_id, data_location, table_id, schema_file_path, source_format, **load_bigquery_table_kwargs
    )
    if not success:
        raise AirflowException()


def bq_load_shard_v2(
    schema_folder: str,
    project_id: str,
    transform_bucket: str,
    transform_blob: str,
    dataset_id: str,
    dataset_location: str,
    table_id: str,
    release_date: pendulum.Date,
    source_format: str,
    prefix: str = "",
    schema_version: str = None,
    dataset_description: str = "",
    **load_bigquery_table_kwargs,
):
    """Load data from a specific file (blob) in the transform bucket to a BigQuery shard.

    :param schema_folder: the path to the SQL schemas folder.
    :param project_id: project id.
    :param transform_bucket: transform bucket name.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param dataset_location: location of dataset.
    :param table_id: Table id.
    :param release_date: Release date.
    :param source_format: the format of the data to load into BigQuery.
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: description of the BigQuery dataset.
    :return: None.
    """

    schema_file_path = prepare_bq_load_v2(
        schema_folder,
        project_id,
        dataset_id,
        dataset_location,
        table_id,
        release_date,
        prefix,
        schema_version,
        dataset_description,
    )

    # Create table id
    table_id = bigquery_sharded_table_id(table_id, release_date)

    # Load BigQuery table
    uri = f"gs://{transform_bucket}/{transform_blob}"
    logging.info(f"URI: {uri}")

    success = load_bigquery_table(
        uri,
        dataset_id,
        dataset_location,
        table_id,
        schema_file_path,
        source_format,
        project_id=project_id,
        **load_bigquery_table_kwargs,
    )
    if not success:
        raise AirflowException()


def bq_load_ingestion_partition(
    schema_folder: str,
    end_date: pendulum.DateTime,
    transform_blob: str,
    dataset_id: str,
    main_table_id: str,
    partition_table_id: str,
    source_format: str,
    prefix: str = "",
    schema_version: str = None,
    dataset_description: str = "",
    partition_type: bigquery.TimePartitioningType = bigquery.TimePartitioningType.DAY,
    **load_bigquery_table_kwargs,
):
    """Load data from a specific file (blob) in the transform bucket to a partition. Since no partition field is
    given it will automatically partition by ingestion datetime.

    :param schema_folder: the path to the SQL schema folder.
    :param end_date: Release end date, used to find the schema and to create table id.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param partition_table_id: Partition table id (should include date as data in table is overwritten).
    :param source_format: the format of the data to load into BigQuery.
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: The description for the dataset
    :param partition_type: The partitioning type (hour, day, month or year)
    :return: None.
    """
    _, bucket_name, data_location, schema_file_path = prepare_bq_load(
        schema_folder, dataset_id, main_table_id, end_date, prefix, schema_version, dataset_description
    )

    uri = f"gs://{bucket_name}/{transform_blob}"

    # Include date in table id, so data in table is not overwritten
    partition_table_id = create_date_table_id(partition_table_id, end_date, partition_type)
    success = load_bigquery_table(
        uri,
        dataset_id,
        data_location,
        partition_table_id,
        schema_file_path,
        source_format,
        partition=True,
        partition_type=partition_type,
        **load_bigquery_table_kwargs,
    )
    if not success:
        raise AirflowException()


def bq_load_partition(
    schema_folder: str,
    project_id: str,
    transform_bucket: str,
    transform_blob: str,
    dataset_id: str,
    dataset_location: str,
    table_id: str,
    release_date: pendulum.DateTime,
    source_format: str,
    partition_type: bigquery.TimePartitioningType,
    prefix: str = "",
    schema_version: str = None,
    dataset_description: str = "",
    partition_field: str = "release_date",
    **load_bigquery_table_kwargs,
):
    """Load data from a specific file (blob) in the transform bucket to a partition.

    :param schema_folder: the path to the SQL schema path.
    :param project_id: project id.
    :param transform_bucket: transform bucket name.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param dataset_location: location of dataset.
    :param table_id: Table id.
    :param release_date: Release date.
    :param source_format: the format of the data to load into BigQuery.
    :param partition_type: The partitioning type (hour, day, month or year)
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: description of the BigQuery dataset.
    :param partition_field: The name of the partition field in the BigQuery table
    :return: None.
    """

    schema_file_path = prepare_bq_load_v2(
        schema_folder,
        project_id,
        dataset_id,
        dataset_location,
        table_id,
        release_date,
        prefix,
        schema_version,
        dataset_description,
    )

    uri = f"gs://{transform_bucket}/{transform_blob}"

    # Include date in table id, so data in table is not overwritten
    table_id = create_date_table_id(table_id, release_date, partition_type)
    success = load_bigquery_table(
        uri,
        dataset_id,
        dataset_location,
        table_id,
        schema_file_path,
        source_format,
        project_id=project_id,
        partition=True,
        partition_field=partition_field,
        partition_type=partition_type,
        **load_bigquery_table_kwargs,
    )
    if not success:
        raise AirflowException()


def bq_delete_old(
    ingestion_date: pendulum.DateTime,
    dataset_id: str,
    main_table_id: str,
    partition_table_id: str,
    merge_partition_field: str,
    bytes_budget: Optional[int] = None,
):
    """Will run a BigQuery query that deletes rows from the main table that are matched with rows from
    a specific partition of the partition table.
    The query is created from a template and the given info.

    :param ingestion_date: The ingestion date of the partition that will be used.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param partition_table_id: Partition table id.
    :param merge_partition_field: Merge partition field.
    :param bytes_budget: Maximum bytes allowed to be processed.
    :return: None.
    """
    ingestion_date = ingestion_date.strftime("%Y-%m-%d")
    # Get merge variables
    dataset_id = dataset_id
    main_table = main_table_id
    partitioned_table = partition_table_id
    merge_condition_field = merge_partition_field

    template_path = os.path.join(utils_templates_path(), make_sql_jinja2_filename("merge_delete_matched"))
    query = render_template(
        template_path,
        dataset=dataset_id,
        main_table=main_table,
        partitioned_table=partitioned_table,
        merge_condition_field=merge_condition_field,
        ingestion_date=ingestion_date,
    )
    run_bigquery_query(query, bytes_budget=bytes_budget)


def bq_append_from_partition(
    ingestion_date: pendulum.DateTime,
    dataset_id: str,
    main_table_id: str,
    partition_table_id: str,
):
    """Appends rows to the main table by coping a specific partition from the partition table to the main table.

    :param ingestion_date: The ingestion date of the partition that will be used.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param partition_table_id: Partition table id.
    :return: None.
    """
    logging.info("requesting project_id variable")
    project_id = Variable.get(AirflowVars.PROJECT_ID)

    logging.info("requesting data_location variable")
    data_location = Variable.get(AirflowVars.DATA_LOCATION)

    src_table_id = f"{project_id}.{dataset_id}.{partition_table_id}${ingestion_date.strftime('%Y%m%d')}"
    dst_table_id = f"{project_id}.{dataset_id}.{main_table_id}"
    success = copy_bigquery_table(src_table_id, dst_table_id, data_location, bigquery.WriteDisposition.WRITE_APPEND)
    if not success:
        raise AirflowException("Error copying BigQuery table")


def bq_append_from_file(
    schema_folder: str,
    end_date: pendulum.DateTime,
    transform_blob: str,
    dataset_id: str,
    main_table_id: str,
    source_format: str,
    prefix: str = "",
    schema_version: str = None,
    dataset_description: str = "",
    **load_bigquery_table_kwargs,
):
    """Appends rows to the main table by loading data from a specific file (blob) in the transform bucket.
    :param schema_folder: the path to the SQL schema folder.
    :param end_date: End date.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param source_format: the format of the data to load into BigQuery.
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: The dataset description.
    :return: None.
    """
    project_id, bucket_name, data_location, schema_file_path = prepare_bq_load(
        schema_folder, dataset_id, main_table_id, end_date, prefix, schema_version, dataset_description
    )

    # Load BigQuery table
    uri = f"gs://{bucket_name}/{transform_blob}"
    logging.info(f"URI: {uri}")

    # Append to table table
    table_id = main_table_id
    success = load_bigquery_table(
        uri,
        dataset_id,
        data_location,
        table_id,
        schema_file_path,
        source_format,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        **load_bigquery_table_kwargs,
    )
    if not success:
        raise AirflowException()


def on_failure_callback(context):
    """
    Function that is called on failure of an airflow task. Will create a slack webhook and send a notification.

    :param context: the context passed from the PythonOperator. See
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
    this  argument.
    :return: None.
    """

    logging.info("requesting environment variable")
    environment = Variable.get(AirflowVars.ENVIRONMENT)

    logging.info("requesting project_id variable")
    project_id = Variable.get(AirflowVars.PROJECT_ID)

    if environment == Environment.develop.value:
        logging.info("Not sending slack notification in develop environment.")
    else:
        exception = context.get("exception")
        if isinstance(exception, Exception):
            formatted_exception = "".join(
                traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)
            ).strip()
        else:
            formatted_exception = exception

        comments = f"Task failed, exception:\n{formatted_exception}"
        ti = context["ti"]
        execution_date = context["execution_date"]
        send_slack_msg(ti=ti, execution_date=execution_date, comments=comments, project_id=project_id)


class SubFolder(Enum):
    """The type of subfolder to create for telescope data"""

    downloaded = "download"
    extracted = "extract"
    transformed = "transform"


def normalized_schedule_interval(schedule_interval: Optional[str]) -> Optional[ScheduleInterval]:
    """
    Copied from https://github.com/apache/airflow/blob/main/airflow/models/dag.py#L851-L866

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
    Returns Normalized Schedule Interval. This is used internally by the Scheduler to
    schedule DAGs.

    1. Converts Cron Preset to a Cron Expression (e.g ``@monthly`` to ``0 0 1 * *``)
    2. If Schedule Interval is "@once" return "None"
    3. If not (1) or (2) returns schedule_interval
    """
    cron_presets = {
        "@hourly": "0 * * * *",
        "@daily": "0 0 * * *",
        "@weekly": "0 0 * * 0",
        "@monthly": "0 0 1 * *",
        "@quarterly": "0 0 1 */3 *",
        "@yearly": "0 0 1 1 *",
    }
    if isinstance(schedule_interval, six.string_types) and schedule_interval in cron_presets:
        _schedule_interval = cron_presets.get(schedule_interval)  # type: Optional[ScheduleInterval]
    elif schedule_interval == "@once":
        _schedule_interval = None
    else:
        _schedule_interval = schedule_interval
    return _schedule_interval


def make_sftp_connection() -> pysftp.Connection:
    """Create a SFTP connection using credentials from the airflow SFTP_SERVICE connection.

    :return: SFTP connection
    """
    conn = BaseHook.get_connection(AirflowConns.SFTP_SERVICE)
    host = conn.host

    # Add public host key
    public_key = conn.extra_dejson.get("host_key", None)
    if public_key is not None:
        key = paramiko.RSAKey(data=b64decode(public_key))
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys.add(host, "ssh-rsa", key)
    else:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

    # set up connection
    return pysftp.Connection(host, port=conn.port, username=conn.login, password=conn.password, cnopts=cnopts)


class SftpFolders:
    def __init__(self, dag_id: str, organisation_name: str, sftp_root: str = "/"):
        """Initialise SftpFolders.

        :param dag_id: the dag id (namespace + organisation name)
        :param organisation_name: the organisation name.
        :param sftp_root: optional root to be added to sftp home path
        """
        self.dag_id = dag_id
        self.organisation_name = organisation_name
        self.sftp_root = sftp_root

    @property
    def sftp_home(self) -> str:
        """Make the SFTP home folder for an organisation.

        :return: the path to the folder.
        """

        organisation_id = make_org_id(self.organisation_name)
        dag_id_prefix = self.dag_id[: -len(organisation_id) - 1]
        return os.path.join(self.sftp_root, "telescopes", dag_id_prefix, organisation_id)

    @property
    def upload(self) -> str:
        """The organisation's SFTP upload folder.

        :return: path to folder.
        """
        return os.path.join(self.sftp_home, "upload")

    @property
    def in_progress(self) -> str:
        """The organisation's SFTP in_progress folder.

        :return: path to folder.
        """
        return os.path.join(self.sftp_home, "in_progress")

    @property
    def finished(self) -> str:
        """The organisation's SFTP finished folder.

        :return: path to folder.
        """
        return os.path.join(self.sftp_home, "finished")

    def move_files_to_in_progress(self, upload_files: Union[list, str]):
        """Move files in list from upload to in-progress folder.

        :param upload_files: File name or list of file names that are in the upload folder and will be moved to the
        in_progress folder (can be full path or just file name)
        :return: None.
        """

        print(f"Files are: {upload_files}")

        if isinstance(upload_files, str):
            upload_files = [upload_files]

        with make_sftp_connection() as sftp:
            sftp.makedirs(self.in_progress)
            for file in upload_files:
                file_name = os.path.basename(file)
                upload_file = os.path.join(self.upload, file_name)
                in_progress_file = os.path.join(self.in_progress, file_name)
                sftp.rename(upload_file, in_progress_file)

    def move_files_to_finished(self, in_progress_files: Union[list, str]):
        """Move files in list from in_progress to finished folder.

        :param in_progress_files: File name or list of file names that are in the in_progress folder and will be moved
        to the finished folder (can be full path or just file name)
        :return: None.
        """
        if isinstance(in_progress_files, str):
            in_progress_files = [in_progress_files]

        with make_sftp_connection() as sftp:
            sftp.makedirs(self.finished)
            for file in in_progress_files:
                file_name = os.path.basename(file)
                in_progress_file = os.path.join(self.in_progress, file_name)
                finished_file = os.path.join(self.finished, file_name)
                sftp.rename(in_progress_file, finished_file)


def make_dag_id(namespace: str, organisation_name: str) -> str:
    """Make a DAG id from a namespace and an organisation name.

    :param namespace: the namespace for the DAG id.
    :param organisation_name: the organisation name.
    :return: the DAG id.
    """

    return f'{namespace}_{organisation_name.strip().lower().replace(" ", "_")}'


def make_org_id(organisation_name: str) -> str:
    """Make an organisation id from the organisation name. Converts the organisation name to lower case,
    strips whitespace and replaces internal spaces with underscores.

    :param organisation_name: the organisation name.
    :return: the organisation id.
    """

    return organisation_name.strip().replace(" ", "_").lower()


def args_list(args) -> list:
    return args


def convert(k: str) -> str:
    """Convert a key name.
    BigQuery specification for field names: Fields must contain only letters, numbers, and underscores, start with a
    letter or underscore, and be at most 128 characters long.
    :param k: Key.
    :return: Converted key.
    """
    # Trim special characters at start:
    k = re.sub("^[^A-Za-z0-9]+", "", k)
    # Replace other special characters (except '_') in remaining string:
    k = re.sub(r"\W+", "_", k)
    return k


def change_keys(obj, convert):
    """Recursively goes through the dictionary obj and replaces keys with the convert function.
    :param obj: Dictionary object.
    :param convert: Convert function.
    :return: Updated dictionary object.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            new[convert(k)] = change_keys(v, convert)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert) for v in obj)
    else:
        return obj
    return new


def build_schedule(sched_start_date: pendulum.DateTime, sched_end_date: pendulum.DateTime):
    """Useful for API based data sources.

    Create a fetch schedule to specify what date ranges to use for each API call. Will default to once a month
    for now, but in the future if we are minimising API calls, this can be a more complicated scheme.

    :param sched_start_date: the schedule start date.
    :param sched_end_date: the end date of the schedule.
    :return: list of (section_start_date, section_end_date) pairs from start_date to current Airflow DAG start date.
    """

    schedule = []
    sched_start_date = sched_start_date.date()
    sched_end_date = sched_end_date.date()

    for start_date in pendulum.Period(start=sched_start_date, end=sched_end_date).range("months"):
        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
        end_date = pendulum.date(start_date.year, start_date.month, last_day)
        schedule.append(pendulum.Period(start_date, end_date))

    return schedule


def get_as_list(base: dict, target):
    """Helper function that returns the target as a list.

    :param base: dictionary to query.
    :param target: target key.
    :return: base[target] as a list (if it isn't already).
    """

    if target not in base:
        return list()

    if not isinstance(base[target], list):
        return [base[target]]

    return base[target]


def get_as_list_or_none(base: dict, key, sub_key):
    """Helper function that returns a list or None if key is missing.

    :param base: dictionary to query.
    :param key: target key.
    :param sub_key: sub_key to target.
    :return: entry or None.
    """

    if key not in base or base[key]["@count"] == "0":
        return None

    return get_as_list(base[key], sub_key)


def get_entry_or_none(base: dict, target, var_type=None):
    """Helper function that returns an entry or None if key is missing.

    :param base: dictionary to query.
    :param target: target key.
    :param var_type: Type of variable this is supposed to be (for casting).
    :return: entry or None.
    """

    if target not in base:
        return None

    if var_type is not None:
        return var_type(base[target])

    return base[target]


def add_partition_date(
    list_of_dicts: List[dict],
    partition_date: datetime,
    partition_type: bigquery.TimePartitioningType = bigquery.TimePartitioningType.DAY,
    partition_field: str = "release_date",
):
    """Add a partition date key/value pair to each dictionary in the list of dicts.
    Used to load data into a BigQuery partition.

    :param list_of_dicts: List of dictionaries with original data
    :param partition_date: The partition date
    :param partition_type: The partition type
    :param partition_field: The name of the partition field in the BigQuery table
    :return: Updated list of dicts with partition dates
    """
    if partition_type == bigquery.TimePartitioningType.HOUR:
        partition_date = partition_date.isoformat()
    else:
        partition_date = partition_date.strftime("%Y-%m-%d")

    for entry in list_of_dicts:
        entry[partition_field] = partition_date
    return list_of_dicts


def make_release_date(**kwargs) -> pendulum.DateTime:
    """Make a release date"""

    return kwargs["next_execution_date"].subtract(days=1).start_of("day")


def is_first_dag_run(dag_run: DagRun) -> bool:
    """Whether the DAG Run is the first run or not

    :param dag_run: A Dag Run instance
    :return: Whether the DAG run is the first run or not
    """

    return dag_run.get_previous_dagrun() is None


def make_table_name(
    *, project_id: str, dataset_id: str, table_id: str, end_date: Union[pendulum.DateTime, pendulum.Date], sharded: bool
):
    """Make a BQ table name.
    :param project_id: GCP Project ID.
    :param dataset_id: GCP Dataset ID.
    :param table_id: Table name to convert to the suitable BQ table ID.
    :param end_date: Latest date considered.
    :param sharded: whether the table is sharded or not.
    """

    new_table_name = table_id
    if sharded:
        table_date = select_table_shard_dates(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            end_date=end_date,
        )[0]
        new_table_name = f"{table_id}{table_date.strftime('%Y%m%d')}"

    return new_table_name


def get_chunks(*, input_list: List[Any], chunk_size: int = 8) -> List[Any]:
    """Generator that splits a list into chunks of a fixed size.

    :param input_list: Input list.
    :param chunk_size: Size of chunks.
    :return: The next chunk from the input list.
    """

    n = len(input_list)
    for i in range(0, n, chunk_size):
        yield input_list[i : i + chunk_size]


@provide_session
def delete_old_xcoms(
    session: Session = None,
    dag_id: str = None,
    execution_date: pendulum.DateTime = None,
    retention_days: int = 31,
):
    """Delete XCom messages created by the DAG with the given ID that are as old or older than than
    execution_date - retention_days.  Defaults to 31 days of retention.

    :param session: DB session.
    :param dag_id: DAG ID.
    :param execution_date: DAG execution date.
    :param retention_days: Days of messages to retain.
    """

    cut_off_date = execution_date.subtract(days=retention_days)
    session.query(XCom).filter(
        and_(
            XCom.dag_id == dag_id,
            XCom.execution_date <= cut_off_date,
        )
    ).delete()
