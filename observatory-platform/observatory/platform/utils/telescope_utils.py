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

# Author: Tuan Chien


import calendar
import gzip
import json
import logging
import os
import shutil
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Any, Type

import jsonlines
import pendulum
import sys
from airflow.models.taskinstance import TaskInstance
from math import ceil


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
        return list()

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
