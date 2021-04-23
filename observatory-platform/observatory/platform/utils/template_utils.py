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

# Author: Aniek Roelofs

""" Utility functions that support (almost) all telescopes/the template """

import logging
import os
import pathlib
import traceback
from datetime import timedelta
from enum import Enum
from typing import List, Tuple, Callable

import pendulum
import six
from airflow import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.dates import cron_presets
from croniter import croniter
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
import functools

from observatory.dags.config import schema_path, workflow_sql_templates_path
from observatory.platform.observatory_config import Environment
from observatory.platform.utils.airflow_utils import AirflowVariable, AirflowVars, create_slack_webhook
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 copy_bigquery_table,
                                                 create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 run_bigquery_query,
                                                 upload_files_to_cloud_storage)
from observatory.platform.utils.jinja2_utils import make_sql_jinja2_filename, render_template

# To avoid hitting the airflow database and the secret backend unnecessarily, some variables are stored as a global
# variable and only requested once
data_path = None
test_data_path_val_ = None


def reset_variables():
    """ Rest Airflow variables.

    :return: None.
    """

    global data_path
    global test_data_path_val_

    data_path = None
    test_data_path_val_ = None


def telescope_path(*subdirs) -> str:
    """ Return a path for saving telescope data. Create it if it doesn't exist.
    :param subdirs: the subdirectories.
    :return: the path.
    """
    global data_path
    if data_path is None:
        logging.info('telescope_path: requesting data_path variable')
        data_path = AirflowVariable.get(AirflowVars.DATA_PATH)

    subdirs = [subdir.value if isinstance(subdir, Enum) else subdir for subdir in subdirs]

    # Create telescope path
    path = os.path.join(data_path, 'telescopes', *subdirs)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    return path


def test_data_path() -> str:
    """ Return the path for the test data.

    :return: the path.
    """
    global test_data_path_val_
    if test_data_path_val_ is None:
        logging.info('test_data_path: requesting test_data_path variable')
        test_data_path_val_ = AirflowVariable.get(AirflowVars.TEST_DATA_PATH)

    return test_data_path_val_


def blob_name(path: str) -> str:
    """ Convert a file path into the full path of the Blob, excluding the bucket name.
    :param path: the path to the file.
    :return: the full path of the Blob.
    """

    # Remove everything before 'telescopes' and make sure that we are using posix file paths
    # Which use / forward slashes rather than backslashes
    sub_paths = pathlib.Path(os.path.relpath(path, telescope_path())).parts[1:]
    relative_path = pathlib.Path(*sub_paths).as_posix()

    return pathlib.Path(os.path.join('telescopes', relative_path)).as_posix()


def upload_files_from_list(files_list: List[str], bucket_name: str) -> bool:
    """ Upload all files in a list to the google cloud download bucket.
    :param files_list: List of full path of files that will be uploaded
    :param bucket_name: The name of the google cloud bucket
    :return: True if upload was successful, else False.
    """
    blob_names = []
    for file_path in files_list:
        blob_names.append(blob_name(file_path))

    success = upload_files_to_cloud_storage(bucket_name, blob_names, files_list)
    return success


def table_ids_from_path(transform_path: str) -> Tuple[str, str]:
    """
    To create the table ids the basename of the transform path is taken and file extensions are stripped.
    E.g.: /opt/observatory/
    :param transform_path: The full path of a file in the transform folder
    :return: Main table id and partition table id
    """
    logging.info(f'Creating table ids from path: {transform_path}')

    main_table_id = os.path.splitext(pathlib.Path(transform_path).stem)[0]
    partition_table_id = f'{main_table_id}_partitions'

    logging.info(f'Table id: {main_table_id}, partition table id: {partition_table_id}')
    return main_table_id, partition_table_id


def prepare_bq_load(dataset_id: str, table_id: str, release_date: pendulum.Pendulum, prefix: str,
                    schema_version: str, dataset_description: str = '') -> [str, str, str, str]:
    """
    Prepare to load data into BigQuery. This will:
     - create the dataset if it does not exist yet
     - get the path to the schema
     - return values of project id, bucket name, and data location
    :param dataset_id: Dataset id.
    :param table_id: Table id.
    :param release_date: The release date used for schema lookup.
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: dataset description.
    :return: The project id, bucket name, data location and schema path
    """

    logging.info('requesting project_id variable')
    project_id = AirflowVariable.get(AirflowVars.PROJECT_ID)

    logging.info('requesting transform_bucket variable')
    bucket_name = AirflowVariable.get(AirflowVars.TRANSFORM_BUCKET)

    logging.info('requesting data_location variable')
    data_location = AirflowVariable.get(AirflowVars.DATA_LOCATION)

    # Create dataset
    create_bigquery_dataset(project_id, dataset_id, data_location, dataset_description)

    # Select schema file based on release date
    analysis_schema_path = schema_path()
    schema_file_path = find_schema(analysis_schema_path, table_id, release_date, prefix, schema_version)
    if schema_file_path is None:
        exit(os.EX_CONFIG)
    return project_id, bucket_name, data_location, schema_file_path


def prepare_bq_load_v2(project_id: str, dataset_id: str, dataset_location: str, table_id: str,
                       release_date: pendulum.Pendulum, prefix: str, schema_version: str,
                       dataset_description: str = '') -> str:
    """
    Prepare to load data into BigQuery. This will:
     - create the dataset if it does not exist yet
     - get the path to the schema
     - return values of project id, bucket name, and data location
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
    analysis_schema_path = schema_path()
    schema_file_path = find_schema(analysis_schema_path, table_id, release_date, prefix, schema_version)
    if schema_file_path is None:
        exit(os.EX_CONFIG)
    return schema_file_path


def bq_load_shard(release_date: pendulum.Pendulum, transform_blob: str, dataset_id: str, table_id: str,
                  source_format: str, prefix: str = '', schema_version: str = None, dataset_description: str = '',
                  **load_bigquery_table_kwargs):
    """ Load data from a specific file (blob) in the transform bucket to a BigQuery shard.
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
    _, bucket_name, data_location, schema_file_path = prepare_bq_load(dataset_id, table_id, release_date, prefix,
                                                                      schema_version, dataset_description)

    # Create table id
    table_id = bigquery_partitioned_table_id(table_id, release_date)

    # Load BigQuery table
    uri = f"gs://{bucket_name}/{transform_blob}"
    logging.info(f"URI: {uri}")

    success = load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path, source_format,
                                  **load_bigquery_table_kwargs)
    if not success:
        raise AirflowException()


def bq_load_shard_v2(project_id: str, transform_bucket: str, transform_blob: str, dataset_id: str,
                     dataset_location: str, table_id: str, release_date: pendulum.Pendulum, source_format: str,
                     prefix: str = '', schema_version: str = None, dataset_description: str = '',
                     **load_bigquery_table_kwargs):
    """ Load data from a specific file (blob) in the transform bucket to a BigQuery shard.

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

    schema_file_path = prepare_bq_load_v2(project_id, dataset_id, dataset_location, table_id, release_date, prefix,
                                          schema_version, dataset_description)

    # Create table id
    table_id = bigquery_partitioned_table_id(table_id, release_date)

    # Load BigQuery table
    uri = f"gs://{transform_bucket}/{transform_blob}"
    logging.info(f"URI: {uri}")

    success = load_bigquery_table(uri, dataset_id, dataset_location, table_id, schema_file_path, source_format,
                                  project_id=project_id, **load_bigquery_table_kwargs)
    if not success:
        raise AirflowException()


def bq_load_partition(end_date: pendulum.Pendulum, transform_blob: str, dataset_id: str, main_table_id: str,
                      partition_table_id: str, prefix: str = '', schema_version: str = None,
                      dataset_description: str = '', **load_bigquery_table_kwargs):
    """ Load data from a specific file (blob) in the transform bucket to a partition. Since no partition field is
    given it will automatically partition by ingestion datetime.
    :param end_date: End date.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param partition_table_id: Partition table id (should include date as data in table is overwritten).
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: The description for the dataset
    :return: None.
    """
    _, bucket_name, data_location, schema_file_path = prepare_bq_load(dataset_id, main_table_id, end_date, prefix,
                                                                      schema_version, dataset_description)

    uri = f"gs://{bucket_name}/{transform_blob}"

    success = load_bigquery_table(uri, dataset_id, data_location, partition_table_id, schema_file_path,
                                  SourceFormat.NEWLINE_DELIMITED_JSON, partition=True,
                                  require_partition_filter=False, **load_bigquery_table_kwargs)
    if not success:
        raise AirflowException()


def bq_delete_old(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, dataset_id: str, main_table_id: str,
                  partition_table_id: str, merge_partition_field: str, updated_date_field: str):
    """ Will run a BigQuery query that deletes rows from the main table that are matched with rows from
    specific partitions of the partition table.
    The query is created from a template and the given info.
    :param start_date: Start date.
    :param end_date: End date.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param partition_table_id: Partition table id.
    :param merge_partition_field: Merge partition field.
    :param updated_date_field: Updated date field.
    :return: None.
    """
    # include end date in period
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
    # Get merge variables
    dataset_id = dataset_id
    main_table = main_table_id
    partitioned_table = partition_table_id

    merge_condition_field = merge_partition_field
    updated_date_field = updated_date_field

    template_path = os.path.join(workflow_sql_templates_path(), make_sql_jinja2_filename('merge_delete_matched'))
    query = render_template(template_path, dataset=dataset_id, main_table=main_table,
                            partitioned_table=partitioned_table, merge_condition_field=merge_condition_field,
                            start_date=start_date, end_date=end_date, updated_date_field=updated_date_field)
    run_bigquery_query(query)


def bq_append_from_partition(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, dataset_id: str,
                             main_table_id: str, partition_table_id: str, prefix: str = '', schema_version: str = None):
    """ Appends rows to the main table by coping specific partitions from the partition table to the main table.
    :param start_date: Start date.
    :param end_date: End date.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param partition_table_id: Partition table id.
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :return: None.
    """
    project_id, bucket_name, data_location, schema_file_path = prepare_bq_load(dataset_id, main_table_id, end_date,
                                                                               prefix, schema_version)
    # include end date in period
    period = pendulum.period(start_date, end_date + timedelta(days=1))
    logging.info(f'Getting table partitions: ')
    source_table_ids = []
    for dt in period:
        table_id = f"{project_id}.{dataset_id}.{partition_table_id}${dt.strftime('%Y%m%d')}"
        source_table_ids.append(table_id)
        logging.info(f'Adding table_id: {table_id}')
    destination_table_id = f"{project_id}.{dataset_id}.{main_table_id}"
    success = copy_bigquery_table(source_table_ids, destination_table_id, data_location,
                                  bigquery.WriteDisposition.WRITE_APPEND)
    if not success:
        raise AirflowException()


def bq_append_from_file(end_date: pendulum.Pendulum, transform_blob: str, dataset_id: str, main_table_id: str,
                        prefix: str = '', schema_version: str = None, dataset_description: str = '',
                        **load_bigquery_table_kwargs):
    """ Appends rows to the main table by loading data from a specific file (blob) in the transform bucket.
    :param end_date: End date.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param prefix: The prefix for the schema.
    :param schema_version: Schema version.
    :param dataset_description: The dataset description.
    :return: None.
    """
    project_id, bucket_name, data_location, schema_file_path = prepare_bq_load(dataset_id, main_table_id, end_date,
                                                                               prefix, schema_version, dataset_description)

    # Load BigQuery table
    uri = f"gs://{bucket_name}/{transform_blob}"
    logging.info(f"URI: {uri}")

    # Append to table table
    table_id = main_table_id
    success = load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path, SourceFormat.NEWLINE_DELIMITED_JSON,
                                  write_disposition=bigquery.WriteDisposition.WRITE_APPEND, **load_bigquery_table_kwargs)
    if not success:
        raise AirflowException()


def on_failure_callback(kwargs):
    """
    Function that is called on failure of an airflow task. Will create a slack webhook and send a notification.
    :param kwargs:
    :return: None.
    """

    logging.info('requesting environment variable')
    environment = AirflowVariable.get(AirflowVars.ENVIRONMENT)

    logging.info('requesting project_id variable')
    project_id = AirflowVariable.get(AirflowVars.PROJECT_ID)

    if environment == Environment.develop:
        logging.info('Not sending slack notification in develop environment.')
    else:
        exception = kwargs.get('exception')
        formatted_exception = ''.join(
            traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)).strip()
        comments = f'Task failed, exception:\n{formatted_exception}'
        slack_hook = create_slack_webhook(comments, project_id, **kwargs)
        slack_hook.execute()


def valid_cron_expression(field, value, error):
    if not croniter.is_valid(normalize_schedule_interval(value)):
        error(field, "Must be a valid cron expression")


def normalize_schedule_interval(schedule_interval: str):
    """
    Returns Normalized Schedule Interval. This is used internally by the Scheduler to
    schedule DAGs.
    1. Converts Cron Preset to a Cron Expression (e.g ``@monthly`` to ``0 0 1 * *``)
    2. If Schedule Interval is "@once" return "None"
    3. If not (1) or (2) returns schedule_interval
    """
    if isinstance(schedule_interval, six.string_types) and schedule_interval in cron_presets:
        _schedule_interval = cron_presets.get(schedule_interval)
    elif schedule_interval == '@once':
        _schedule_interval = None
    else:
        _schedule_interval = schedule_interval
    return _schedule_interval


class SubFolder(Enum):
    """ The type of subfolder to create for telescope data """

    downloaded = 'download'
    extracted = 'extract'
    transformed = 'transform'
