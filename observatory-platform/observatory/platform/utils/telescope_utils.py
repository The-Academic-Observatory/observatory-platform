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

""" Utility functions that support specific telescope(s) """

import calendar
import json
import logging
import os
import re
import sys
from base64 import b64decode
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Any, List, Tuple, Type, Union, Optional

import dateutil
import jsonlines
import paramiko
import pendulum
import pysftp
import six
from airflow.hooks.base_hook import BaseHook
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from croniter import croniter
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client.api_client import ApiClient
from observatory.api.client.configuration import Configuration
from observatory.api.server.api import Response
from observatory.dags.config import workflow_sql_templates_path
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.file_utils import load_file, write_to_file
from observatory.platform.utils.gc_utils import upload_file_to_cloud_storage

ScheduleInterval = Union[str, timedelta, relativedelta]


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


def get_prev_execution_date(schedule_interval_target: str, execution_date: pendulum.Pendulum) -> datetime:
    """ Get the previous execution date of a target DAG based on the given execution date and schedule interval of
    the target DAG.
    This can be used as a callable for the execution_delta_fn of the ExternalTaskSensor.
    For example:
    execution_date_fn=lambda execution_date: get_prev_execution_date(ExternalTelescope.SCHEDULE_INTERVAL,
                                                                     execution_date)

    :param schedule_interval_target: Schedule interval of target DAG (can be e.g. '@monthly' or '0 0 1 * *')
    :param execution_date: The execution date
    :return: The previous execution date of the target DAG.
    """
    # Normalize schedule, converting cron preset to cron expression
    normalized_schedule = normalized_schedule_interval(schedule_interval_target)

    # Convert exec date to datetime, using pendulum exec date gives error 'list index out of range' related to tz info
    tz_info = dateutil.tz.gettz(execution_date.timezone_name)
    dt_execution_date = datetime(
        execution_date.year,
        execution_date.month,
        execution_date.day,
        execution_date.hour,
        execution_date.minute,
        execution_date.second,
        execution_date.microsecond,
    ).astimezone(tz_info)

    logging.info(
        f"Getting last execution date with normalized schedule '{normalized_schedule}' and execution_date "
        f"'{dt_execution_date}'"
    )
    # Create croniter object
    cron_iter = croniter(normalized_schedule, dt_execution_date)
    # Get previous execution date
    execution_date_target = cron_iter.get_prev(datetime)
    logging.info(f"Found execution date: {execution_date_target}")
    return execution_date_target


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


def make_observatory_api() -> ObservatoryApi:
    """Make the ObservatoryApi object, configuring it with a host and api_key.

    :return: the ObservatoryApi.
    """

    # Get connection
    conn = BaseHook.get_connection(AirflowConns.OBSERVATORY_API)

    # Assert connection has required fields
    assert (
        conn.conn_type != "" and conn.conn_type is not None
    ), f"Airflow Connection {AirflowConns.OBSERVATORY_API} conn_type must not be None"
    assert (
        conn.host != "" and conn.host is not None
    ), f"Airflow Connection {AirflowConns.OBSERVATORY_API} host must not be None"
    assert (
        conn.password != "" and conn.password is not None
    ), f"Airflow Connection {AirflowConns.OBSERVATORY_API} password must not be None"

    # Make host
    host = f'{str(conn.conn_type).replace("_", "-").lower()}://{conn.host}'
    if conn.port:
        host += f":{conn.port}"

    # Return ObservatoryApi
    config = Configuration(host=host, api_key={"api_key": conn.password})
    api_client = ApiClient(config)
    return ObservatoryApi(api_client=api_client)


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


def build_schedule(sched_start_date, sched_end_date):
    """Useful for API based data sources.

    Create a fetch schedule to specify what date ranges to use for each API call. Will default to once a month
    for now, but in the future if we are minimising API calls, this can be a more complicated scheme.

    :param sched_start_date: the schedule start date.
    :param sched_end_date: the end date of the schedule.
    :return: list of (section_start_date, section_end_date) pairs from start_date to current Airflow DAG start date.
    """

    schedule = []

    for start_date in pendulum.Period(start=sched_start_date, end=sched_end_date).range("months"):
        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
        end_date = pendulum.date(start_date.year, start_date.month, last_day)
        schedule.append(pendulum.Period(start_date, end_date))

    return schedule


def delete_msg_files(ti: TaskInstance, topic: str, task_id: str, msg_key: str = ""):
    """Pull messages from a topic and delete the relevant paths.

    :param ti: TaskInstance.
    :param topic: Message topic.
    :param task_id: Task ID who sent message.
    :param msg_key: Key of specific messages.
    """

    msgs = ti.xcom_pull(key=topic, task_ids=task_id, include_prior_dates=True)

    if msg_key != "":
        files = msgs[msg_key]
    else:
        files = msgs

    for file in files:
        try:
            logging.info(f"delete_msg_files: Deleting {file}")
            Path(file).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {file}: {e}")


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


def json_to_db(
    json_list: List[Tuple[Any]], release_date: str, parser, institutes: List[str], path_prefix: str = None
) -> List[str]:
    """Transform json from query into database format.

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
    save_file = f"{first_file}l"

    if path_prefix:
        filename = os.path.basename(save_file)
        save_file = os.path.join(path_prefix, filename)
        Path(path_prefix).mkdir(parents=True, exist_ok=True)

    jsonlines_files.append(save_file)

    with jsonlines.open(save_file, mode="w") as writer:
        for (file, harvest_date) in json_list:
            logging.info(f"Parsing {file} into db format and writing to jsonlines")
            with open(file, "r") as f:
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


def validate_date(date_string):
    """Validate a date string is pendulum parsable.

    :param date_string: date string to check.
    :return: True if parsable, false otherwise.
    """
    try:
        pendulum.parse(date_string)
    except Exception as e:
        print(f"Pendulum parsing encountered exception: {e}")
        return False
    return True


def write_xml_to_json(transform_path: str, release_date: str, inst_id: str, in_files: List[str], parser):
    """Write a list of web responses to json.

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
        logging.info(f"Transforming {file} to json")
        xml_data = load_file(file)

        parsed_list = list()
        parsed_record, schema_ver = parser(xml_data)
        if parsed_record is None:
            logging.info(f"Empty record received for {file}")
            continue
        parsed_list = parsed_list + parsed_record
        schema_vers.append(schema_ver)

        # Save it in the transform bucket.
        filename = os.path.basename(file)
        json_file = f"{filename[:-3]}json"
        json_path = os.path.join(transform_path, release_date, inst_id, json_file)
        json_file_list.append(json_path)
        json_record = json.dumps(parsed_list)
        write_to_file(json_record, json_path)

    return json_file_list, schema_vers


def make_telescope_sensor(telescope_name: str, dag_prefix: str) -> ExternalTaskSensor:
    """Create an ExternalTaskSensor to monitor when a telescope has finished execution.

    :param telescope_name: Name of the telescope.
    :param dag_prefix: DAG ID prefix.
    :return: ExternalTaskSensor object that monitors a telescope.
    """

    dag_id = make_dag_id(dag_prefix, telescope_name)

    return ExternalTaskSensor(
        task_id=f"{dag_id}_sensor", external_dag_id=dag_id, mode="reschedule", start_date=datetime(2021, 3, 28)
    )


@dataclass
class PeriodCount:
    """ Descriptive wrapper for a (period, count) object. """

    period: pendulum.Period  # The schedule period in question
    count: int  # Number of results for this period.


class ScheduleOptimiser:
    """Calculate a schedule that minimises API calls using historic retrieval data.
    Given a list of tuples (period, count) that indicates how many results were retrieved for a given period from a
    historical query, the maximum number of results per API call, and the maximum number of results per query, get
    a schedule that minimises the number of API calls made.
    """

    @staticmethod
    def get_num_calls(num_results: int, max_per_call: int) -> int:
        """Calculate the number of required API calls based on number of results and max results per call.

        :param num_results: Number of results.
        :param max_per_call: The max returnable results per call.
        :return: Number of API calls you need to make.
        """

        return ceil(float(num_results) / max_per_call)

    @staticmethod
    def extract_schedule(historic_counts: List[PeriodCount], moves: List[int]) -> List[Type[pendulum.Period]]:
        """Extract a solution schedule from the optimisation.

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
    def optimise(
        max_per_call: int, max_per_query: int, historic_counts: List[Type[PeriodCount]]
    ) -> Tuple[List[Type[pendulum.Period]], int]:
        """Calculate and return a schedule that minimises the number of API calls with the given constraints. Behaviour
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
            raise Exception("Empty historic_counts received.")

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


def upload_telescope_file_list(bucket_name: str, inst_id: str, telescope_path: str, file_list: List[str]) -> List[str]:
    """Upload list of files to cloud storage.

    :param bucket_name: Name of storage bucket.
    :param inst_id: institution id from airflow connection id.
    :param telescope_path: Path to upload telescope data.
    :param file_list: List of files to upload.
    :return: List of location paths in the cloud.
    """

    blob_list = list()
    for file in file_list:
        file_name = os.path.basename(file)
        blob_name = f"{telescope_path}/{inst_id}/{file_name}"
        blob_list.append(blob_name)
        upload_file_to_cloud_storage(bucket_name, blob_name, file_path=file)
    return blob_list


def add_partition_date(
    list_of_dicts: List[dict],
    partition_date: datetime,
    partition_type: bigquery.TimePartitioningType = bigquery.TimePartitioningType.DAY,
    partition_field: str = "release_date",
):
    """ Add a partition date key/value pair to each dictionary in the list of dicts.
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
