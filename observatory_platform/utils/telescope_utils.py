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

# Author: Tuan Chien, Aniek Roelofs

import calendar
import gzip
import json
import logging
import os
import re
import shutil
import sys
from collections import deque
from dataclasses import dataclass
from math import ceil
from pathlib import Path
import traceback
from types import SimpleNamespace
from typing import Any, List, Tuple, Type, Union

import jsonlines
import pendulum
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from observatory_platform.utils.airflow_utils import AirflowVariable as Variable
from observatory_platform.utils.config_utils import (AirflowConn,
                                                     AirflowVar,
                                                     Environment,
                                                     SubFolder,
                                                     find_schema,
                                                     schema_path,
                                                     telescope_path,
                                                     telescope_templates_path)
from observatory_platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 copy_bigquery_table,
                                                 create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 run_bigquery_query,
                                                 upload_file_to_cloud_storage,
                                                 upload_files_to_cloud_storage)
from observatory_platform.utils.jinja2_utils import (make_jinja2_filename, make_sql_jinja2_filename, render_template)


class TelescopeRelease:
    """ Used to store info on a given release"""
    def __init__(self, start_date: Union[pendulum.Pendulum, None], end_date: pendulum.Pendulum,
                 telescope: SimpleNamespace, first_release: bool = False):
        self.start_date = start_date
        self.end_date = end_date
        self.release_date = end_date
        self.first_release = first_release
        self.telescope = telescope

        self.date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")

        # paths
        self.file_name = f"{telescope.dag_id}_{self.date_str}"
        self.blob_dir = f'telescopes/{self.telescope.dag_id}'

        self.download_path = self.get_path(SubFolder.downloaded, self.file_name, self.telescope.download_ext)
        self.download_dir = self.subdir(SubFolder.downloaded)
        self.download_blob = os.path.join(self.blob_dir, f"{self.file_name}.{self.telescope.download_ext}")

        self.extract_path = self.get_path(SubFolder.extracted, self.file_name, self.telescope.extract_ext)
        self.extract_dir = self.subdir(SubFolder.extracted)

        self.transform_path = self.get_path(SubFolder.transformed, self.file_name, self.telescope.transform_ext)
        self.transform_dir = self.subdir(SubFolder.transformed)
        self.transform_blob = os.path.join(self.blob_dir, f"{self.file_name}.{self.telescope.transform_ext}")

    def subdir(self, sub_folder: SubFolder) -> str:
        """ Path to subdirectory of a specific release for either downloaded/extracted/transformed files.
        Will also create the directory if it doesn't exist yet.
        :param sub_folder: Name of the subfolder
        :return: Path to the directory
        """
        subdir = os.path.join(telescope_path(sub_folder, self.telescope.dag_id), self.date_str)
        if not os.path.exists(subdir):
            os.makedirs(subdir, exist_ok=True)
        return subdir

    def get_path(self, sub_folder: SubFolder, file_name: str, ext: str) -> str:
        """
        Gets path to file based on subfolder, file name and extension.
        :param sub_folder: Name of the subfolder
        :param file_name: Name of the file
        :param ext: The extension of the file
        :return: The file path.
        """
        path = os.path.join(self.subdir(sub_folder), f"{file_name}.{ext}")
        return path


def check_dependencies_example(kwargs):
    pass


def transfer_example(release: TelescopeRelease) -> bool:
    return True


def download_example(release: TelescopeRelease) -> bool:
    return True


def download_transferred_example(release: TelescopeRelease):
    pass


def extract_example(release: TelescopeRelease):
    pass


def transform_example(release: TelescopeRelease):
    pass


def upload_downloaded(download_dir: str, blob_dir: str) -> bool:
    """ Upload all files in download dir to the google cloud download bucket.

    :param download_dir: Path to the download directory.
    :param blob_dir: Name of the bucket blob directory.
    :return: True if upload was successful, else False.
    """
    bucket_name = Variable.get(AirflowVar.download_bucket_name.get())
    blob_names = []
    file_paths = []
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            blob_names.append(os.path.join(blob_dir, file))
            file_paths.append(os.path.join(root, file))

    success = upload_files_to_cloud_storage(bucket_name, blob_names, file_paths)
    return success


def upload_transformed(transform_path: str, transform_blob: str) -> bool:
    """ Upload the transformed file to the google cloud transform bucket.

    :param transform_path: Path to the transformed file.
    :param transform_blob: Name of the transform blob.
    :return: True if upload was successful, else False.
    """
    bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())
    success = upload_file_to_cloud_storage(bucket_name, transform_blob, transform_path)
    return success


def bq_load_shard(release_date: pendulum.Pendulum, transform_blob: str, dataset_id: str, table_id: str,
                  schema_version: str):
    """ Load data from a specific file (blob) in the transform bucket to a BigQuery shard.

    :param release_date: Release date.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param table_id: Table id.
    :param schema_version: Schema version.
    :return: None.
    """
    bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())
    data_location = Variable.get(AirflowVar.data_location.get())

    # Select schema file based on release date
    analysis_schema_path = schema_path('telescopes')
    schema_file_path = find_schema(analysis_schema_path, table_id, release_date, ver=schema_version)
    if schema_file_path is None:
        logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                      f'table_name={table_id}, release_date={release_date}')
        exit(os.EX_CONFIG)

    table_id = bigquery_partitioned_table_id(table_id, release_date)
    dataset_id = dataset_id
    uri = f"gs://{bucket_name}/{transform_blob}"

    load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path, SourceFormat.NEWLINE_DELIMITED_JSON)


def bq_load_partition(end_date: pendulum.Pendulum, transform_blob: str, dataset_id: str, main_table_id: str,
                      partition_table_id: str, schema_version: str):
    """ Load data from a specific file (blob) in the transform bucket to a partition. Since no partition field is
    given it will automatically partition by ingestion datetime.

    :param end_date: End date.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param partition_table_id: Partition table id.
    :param schema_version: Schema version.
    :return: None.
    """
    bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())
    data_location = Variable.get(AirflowVar.data_location.get())

    # Select schema file based on release date
    analysis_schema_path = schema_path('telescopes')
    schema_file_path = find_schema(analysis_schema_path, main_table_id, end_date, ver=schema_version)
    if schema_file_path is None:
        logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                      f'table_name={main_table_id}, release_date={end_date}, '
                      f'version={schema_version}')
        exit(os.EX_CONFIG)

    uri = f"gs://{bucket_name}/{transform_blob}"

    load_bigquery_table(uri, dataset_id, data_location, partition_table_id, schema_file_path,
                        SourceFormat.NEWLINE_DELIMITED_JSON, partition=True, require_partition_filter=False)


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
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")
    # Get merge variables
    dataset_id = dataset_id
    main_table = main_table_id
    partitioned_table = partition_table_id

    merge_condition_field = merge_partition_field
    updated_date_field = updated_date_field

    template_path = os.path.join(telescope_templates_path(), make_sql_jinja2_filename('merge_delete_matched'))
    query = render_template(template_path, dataset=dataset_id, main_table=main_table,
                            partitioned_table=partitioned_table, merge_condition_field=merge_condition_field,
                            start_date=start_date, end_date=end_date, updated_date_field=updated_date_field)
    run_bigquery_query(query)


def bq_append_from_partition(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, dataset_id: str,
                             main_table_id: str, partition_table_id: str, schema_version: str, description: str):
    """ Appends rows to the main table by coping specific partitions from the partition table to the main table.

    :param start_date: Start date.
    :param end_date: End date.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param partition_table_id: Partition table id.
    :param schema_version: Schema version.
    :param description: Description.
    :return:
    """
    project_id = Variable.get(AirflowVar.project_id.get())
    data_location = Variable.get(AirflowVar.data_location.get())

    # Create dataset
    dataset_id = dataset_id
    create_bigquery_dataset(project_id, dataset_id, data_location, description)

    # Select schema file based on release date
    analysis_schema_path = schema_path('telescopes')
    schema_file_path = find_schema(analysis_schema_path, main_table_id, end_date, ver=schema_version)
    if schema_file_path is None:
        logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                      f'table_name={main_table_id}, end_date={end_date}, version={schema_version}')
        exit(os.EX_CONFIG)

    period = pendulum.period(start_date, end_date)
    source_table_ids = []
    for dt in period:
        table_id = f"{project_id}.{dataset_id}.{partition_table_id}${dt.strftime('%Y%m%d')}"
        source_table_ids.append(table_id)
    destination_table_id = f"{project_id}.{dataset_id}.{main_table_id}"
    copy_bigquery_table(source_table_ids, destination_table_id, data_location, bigquery.WriteDisposition.WRITE_APPEND)


def bq_append_from_file(end_date: pendulum.Pendulum, transform_blob: str, dataset_id: str, main_table_id: str,
                        schema_version: str, description: str):
    """ Appends rows to the main table by loading data from a specific file (blob) in the transform bucket.

    :param end_date: End date.
    :param transform_blob: Name of the transform blob.
    :param dataset_id: Dataset id.
    :param main_table_id: Main table id.
    :param schema_version: Schema version.
    :param description: The description.
    :return: None.
    """
    project_id = Variable.get(AirflowVar.project_id.get())
    data_location = Variable.get(AirflowVar.data_location.get())
    bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

    # Create dataset
    dataset_id = dataset_id
    create_bigquery_dataset(project_id, dataset_id, data_location, description)

    # Select schema file based on release date
    analysis_schema_path = schema_path('telescopes')
    release_date = pendulum.instance(end_date)
    schema_file_path = find_schema(analysis_schema_path, main_table_id, release_date, ver=schema_version)
    if schema_file_path is None:
        logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                      f'table_name={main_table_id}, release_date={release_date}, '
                      f'version={schema_version}')
        exit(os.EX_CONFIG)

    # Load BigQuery table
    uri = f"gs://{bucket_name}/{transform_blob}"
    logging.info(f"URI: {uri}")

    # Append to table table
    table_id = main_table_id
    load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path, SourceFormat.NEWLINE_DELIMITED_JSON,
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND)


def cleanup(download_dir: str, extract_dir: str, transform_dir: str):
    """ Delete release directories from disk.

    :param download_dir: Download dir.
    :param extract_dir: Extract dir.
    :param transform_dir: Transform dir.
    :return: None.
    """
    try:
        shutil.rmtree(download_dir)
    except FileNotFoundError as e:
        logging.warning(e)

    try:
        shutil.rmtree(extract_dir)
    except FileNotFoundError as e:
        logging.warning(e)

    try:
        shutil.rmtree(transform_dir)
    except FileNotFoundError as e:
        logging.warning(e)


def args_list(args) -> list:
    return args


def write_boto_config(s3_host: str, aws_access_key_id: str, aws_secret_access_key: str, boto_config_path: str):
    """ Write a boto3 configuration file, created by using a template and given info.

    :param s3_host: Host of the s3 bucket.
    :param aws_access_key_id: Access key id to bucket.
    :param aws_secret_access_key: Secret access key to bucket.
    :param boto_config_path: Path to write the boto config file to.
    :return: None.
    """
    logging.info(f'Writing boto config file to {boto_config_path}')

    template_path = os.path.join(telescope_templates_path(), make_jinja2_filename('boto'))
    rendered = render_template(template_path, s3_host=s3_host, aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)
    with open(boto_config_path, 'w') as f:
        f.write(rendered)


def convert(k: str) -> str:
    """ Convert a key name.
    BigQuery specification for field names: Fields must contain only letters, numbers, and underscores, start with a
    letter or underscore, and be at most 128 characters long.

    :param k: Key.
    :return: Converted key.
    """
    # Trim special characters at start:
    k = re.sub('^[^A-Za-z0-9]+', "", k)
    # Replace other special characters (except '_') in remaining string:
    k = re.sub('\W+', '_', k)
    return k


def change_keys(obj, convert):
    """ Recursively goes through the dictionary obj and replaces keys with the convert function.
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


def create_slack_webhook(comments: str = "", **kwargs) -> SlackWebhookHook:
    """
    Creates a slack webhook using the token in the slack airflow connection.
    :param comments: Additional comments in slack message
    :param kwargs: the context passed from the PythonOperator. See
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
    this  argument.
    :return: slack webhook
    """
    ti: TaskInstance = kwargs['ti']
    message = """
    :red_circle: Task Alert. 
    *Task*: {task}  
    *Dag*: {dag} 
    *Execution Time*: {exec_date}  
    *Log Url*: {log_url} 
    *Comments*: {comments}
    """.format(task=ti.task_id, dag=ti.dag_id, ti=ti, exec_date=kwargs['execution_date'], log_url=ti.log_url,
               comments=comments)
    slack_conn = BaseHook.get_connection(AirflowConn.slack.get())
    slack_hook = SlackWebhookHook(http_conn_id=slack_conn.conn_id, webhook_token=slack_conn.password, message=message)
    return slack_hook


def on_failure_callback(kwargs):
    environment = Variable.get(AirflowVar.environment.get())
    if environment == Environment.dev:
        logging.info('Not sending slack notification in dev environment.')
    else:
        exception = kwargs.get('exception')
        formatted_exception = ''.join(traceback.format_exception(etype=type(exception), value=exception,
                                                                 tb=exception.__traceback__)).strip()
        comments = f'Task failed, exception:\n{formatted_exception}'
        slack_hook = create_slack_webhook(comments, **kwargs)
        slack_hook.execute()


def build_schedule(sched_start_date, sched_end_date):
    """ Useful for API based data sources.
    Create a fetch schedule to specify what date ranges to use for each API call. Will default to once a month
    for now, but in the future if we are minimising API calls, this can be a more complicated scheme.

    :param sched_start_date: the schedule start date.
    :param sched_end_date: the end date of the schedule.
    :return: list of (section_start_date, section_end_date) pairs from start_date to current Airflow DAG start date.
    """

    schedule = []

    for start_date in pendulum.Period(start=sched_start_date, end=sched_end_date).range('months'):
        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
        end_date = pendulum.date(start_date.year, start_date.month, last_day)
        schedule.append(pendulum.Period(start_date, end_date))

    return schedule


def delete_msg_files(ti: TaskInstance, topic: str, task_id: str, msg_key: str = ''):
    """ Pull messages from a topic and delete the relevant paths.

    :param ti: TaskInstance.
    :param topic: Message topic.
    :param task_id: Task ID who sent message.
    :param msg_key: Key of specific messages.
    """

    msgs = ti.xcom_pull(key=topic, task_ids=task_id, include_prior_dates=True)

    if msg_key != '':
        files = msgs[msg_key]
    else:
        files = msgs

    for file in files:
        try:
            logging.info(f'delete_msg_files: Deleting {file}')
            Path(file).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {file}: {e}")


def get_as_list(base: dict, target):
    """ Helper function that returns the target as a list.

    :param base: dictionary to query.
    :param target: target key.
    :return: base[target] as a list (if it isn't already).
    """

    if target not in base:
        return None

    if not isinstance(base[target], list):
        return [base[target]]

    return base[target]


def get_as_list_or_none(base: dict, key, sub_key):
    """ Helper function that returns a list or None if key is missing.

    :param base: dictionary to query.
    :param key: target key.
    :param sub_key: sub_key to target.
    :return: entry or None.
    """

    if key not in base or base[key]['@count'] == "0":
        return None

    return get_as_list(base[key], sub_key)


def get_entry_or_none(base: dict, target, var_type=None):
    """ Helper function that returns an entry or None if key is missing.

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


def json_to_db(json_list: List[Tuple[Any]], release_date: str, parser, institutes: List[str],
               path_prefix: str = None) -> List[str]:
    """ Transform json from query into database format.

    :param json_list: json data to transform.
    :param release_date: release date of the snapshot.
    :param parser: Parser function accepting (json entry, harvest date, release date, institutes), and returning
                    schema conforming data structure.
    :param institutes: List of institution ids used in the query.
    :param path_prefix: If specified, gives the new path prefix for the file you want to save.
    :return: Saved file name.
    """

    jsonlines_files = list()
    if len(json_list) == 0:
        return jsonlines_files

    first_file = json_list[0][0]
    save_file = f'{first_file}l'

    if path_prefix:
        filename = os.path.basename(save_file)
        save_file = os.path.join(path_prefix, filename)
        Path(path_prefix).mkdir(parents=True, exist_ok=True)

    jsonlines_files.append(save_file)

    with jsonlines.open(save_file, mode='w') as writer:
        for (file, harvest_date) in json_list:
            logging.info(f'Parsing {file} into db format and writing to jsonlines')
            with open(file, 'r') as f:
                data = json.load(f)

            parsed_entries = list()
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                parsed_entry = parser(entry, harvest_date, release_date, institutes)
                parsed_entries.append(parsed_entry)

            for entry in parsed_entries:
                writer.write(entry)

    return jsonlines_files


def load_file(file_name: str, modes='r'):
    """ Load a file.

    :param file_name: file to load.
    :param modes: File open modes. Defaults to 'r'
    :return: contents of file.
    """

    with open(file_name, modes) as f:
        return f.read()


def validate_date(date_string):
    """ Validate a date string is pendulum parsable.

    :param date_string: date string to check.
    :return: True if parsable, false otherwise.
    """
    try:
        pendulum.parse(date_string)
    except Exception as e:
        print(f'Pendulum parsing encountered exception: {e}')
        return False
    return True


def write_to_file(record, file_name: str):
    """ Write a structure to file.

    :param record: Structure to write.
    :param file_name: File name to write to.
    """

    directory = os.path.dirname(file_name)
    Path(directory).mkdir(parents=True, exist_ok=True)

    with open(file_name, 'w') as f:
        f.write(record)


def write_xml_to_json(transform_path: str, release_date: str, inst_id: str, in_files: List[str], parser):
    """ Write a list of web responses to json.

    :param transform_path: base path to store transformed files.
    :param release_date: release date.
    :param inst_id: institution id from airflow connection id.
    :param in_files: list of xml web response files.
    :param parser: Parsing function that parses the response into json compatible data.
    :return: List of json files written to, and list of schema versions per response.
    """

    json_file_list = list()
    schema_vers = list()

    for file in in_files:
        logging.info(f'Transforming {file} to json')
        xml_data = load_file(file)

        parsed_list = list()
        parsed_record, schema_ver = parser(xml_data)
        if parsed_record is None:
            logging.info(f'Empty record received for {file}')
            continue
        parsed_list = parsed_list + parsed_record
        schema_vers.append(schema_ver)

        # Save it in the transform bucket.
        filename = os.path.basename(file)
        json_file = f'{filename[:-3]}json'
        json_path = os.path.join(transform_path, release_date, inst_id, json_file)
        json_file_list.append(json_path)
        json_record = json.dumps(parsed_list)
        write_to_file(json_record, json_path)

    return json_file_list, schema_vers


def zip_files(file_list: List[str]):
    """ GZip up the list of files.

    :param file_list: List of files to zip up.
    :return: List of zipped up file names.
    """

    zip_list = list()
    for file_path in file_list:
        logging.info(f'Zipping file {file_path}')
        zip_file = f'{file_path}.gz'
        zip_list.append(zip_file)
        with open(file_path, 'rb') as f_in:
            with gzip.open(zip_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    return zip_list


@dataclass
class PeriodCount:
    """ Descriptive wrapper for a (period, count) object. """
    period: pendulum.Period  # The schedule period in question
    count: int  # Number of results for this period.


class ScheduleOptimiser:
    """ Calculate a schedule that minimises API calls using historic retrieval data.
        Given a list of tuples (period, count) that indicates how many results were retrieved for a given period from a
        historical query, the maximum number of results per API call, and the maximum number of results per query, get
        a schedule that minimises the number of API calls made.
    """

    @staticmethod
    def get_num_calls(num_results: int, max_per_call: int) -> int:
        """ Calculate the number of required API calls based on number of results and max results per call.

        :param num_results: Number of results.
        :param max_per_call: The max returnable results per call.
        :return: Number of API calls you need to make.
        """

        return ceil(float(num_results) / max_per_call)

    @staticmethod
    def extract_schedule(historic_counts: List[PeriodCount], moves: List[int]) -> List[Type[pendulum.Period]]:
        """ Extract a solution schedule from the optimisation.

        :param historic_counts: the histogram of periods and their counts.
        :param moves: the moves the optimiser took to compute the minimum.
        :return: Optimised schedule.
        """

        stack = deque()

        j = len(moves) - 1
        while j >= 0:
            i = moves[j]
            period = pendulum.Period(historic_counts[i].period.start, historic_counts[j].period.end)
            stack.append(period)
            j = i - 1

        schedule = list()
        while len(stack) > 0:
            schedule.append(stack.pop())

        return schedule

    @staticmethod
    def optimise(max_per_call: int, max_per_query: int, historic_counts: List[Type[PeriodCount]]) -> Tuple[
        List[Type[pendulum.Period]], int]:
        """ Calculate and return a schedule that minimises the number of API calls with the given constraints. Behaviour
            if there are 0 results in any of the periods is still to return 1 period covering the entire span, but the
            minimum number of calls reported will be 0.

        :param max_per_call: Maximum number of results returned per API call.
        :param max_per_query: Maximum number of results returned per query.
        :param historic_counts: List of results per period, i.e., tuples of form (period, count). Please sort by
                                date beforehand.
        :return: New schedule of periods that minimises API calls, and the api calls required for it.
        """

        n = len(historic_counts)

        if n == 0:
            raise Exception('Empty historic_counts received.')

        if n == 1:
            return historic_counts, ScheduleOptimiser.get_num_calls(historic_counts[0].count, max_per_call)

        min_calls = [sys.maxsize] * n
        moves = [0] * n
        min_calls[0] = ScheduleOptimiser.get_num_calls(historic_counts[0].count, max_per_call)

        for i in range(1, n):
            result_count = 0
            min_calls[i] = ScheduleOptimiser.get_num_calls(historic_counts[i].count, max_per_call) + min_calls[i - 1]

            for j in range(i, -1, -1):
                curr_count = historic_counts[j].count
                result_count += curr_count
                if result_count > max_per_query:
                    break

                candidate = ScheduleOptimiser.get_num_calls(result_count, max_per_call)
                if j - 1 >= 0:
                    candidate += min_calls[j - 1]

                if candidate <= min_calls[i]:
                    min_calls[i] = candidate
                    moves[i] = j

        schedule = ScheduleOptimiser.extract_schedule(historic_counts, moves)
        return schedule, min_calls[-1]
