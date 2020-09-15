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
import jsonlines
import logging
import os
import pathlib
import pendulum
import pickle
import shutil

from airflow.models.taskinstance import TaskInstance
from typing import List


def build_schedule(sched_start_date, sched_end_date):
    """ Useful for API based data sources.
    Create a fetch schedule to specify what date ranges to use for each API call. Will default to once a month
    for now, but in the future if we are minimising API calls, this can be a more complicated scheme.

    :param sched_start_date: the schedule start date.
    :param sched_end_date: the end date of the schedule.
    :return: list of (section_start_date, section_end_date) pairs from start_date to current Airflow DAG start date.
    """

    schedule = []
    for year in range(sched_start_date.year, sched_end_date.year + 1, 1):
        for month in range(1, 13):
            start_date = pendulum.date(year, month, 1)
            last_day_start_month = calendar.monthrange(year, month)[1]
            end_date = pendulum.date(year, month, last_day_start_month)
            if sched_start_date <= start_date <= sched_end_date:
                if end_date > sched_end_date:
                    end_date = sched_end_date
                schedule.append((start_date, end_date))
    return schedule


def delete_msg_files(ti: TaskInstance, topic: str, task_id: str, msg_key: str, dag_id):
    """ Pull messages from a topic and delete the relevant paths.

    :param ti: TaskInstance.
    :param topic: Message topic.
    :param task_id: Task ID who sent message.
    :param msg_key: Key of specific messages.
    :param dag_id: DAG ID.
    """

    msgs = ti.xcom_pull(key=topic, task_ids=task_id, include_prior_dates=True, dag_id=dag_id)
    keyed_files = filter(lambda x: msg_key in x, msgs)
    files = [msg[msg_key] for msg in keyed_files]

    for file in files:
        try:
            pathlib.Path(file).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {file}: {e}")


def json_to_db(json_list: List[str], release_date: str, parser) -> List[str]:
    """ Transform json from query into database format.

    :param json_list: json data to transform.
    :param release_date: release date of the snapshot.
    :param parser: Parser function accepting (json entry, harvest date, release date), and returning schema conforming
                    data structure.
    :return: Saved file name.
    """

    jsonlines_files = list()
    if len(json_list) == 0:
        return jsonlines_files

    first_file = json_list[0][0]
    harvest_date = json_list[0][1]
    end_boundary = first_file.find('-')
    inst_tag = first_file[:end_boundary]
    save_file = f'{inst_tag}_{release_date}_{harvest_date}.jsonl'
    jsonlines_files.append(save_file)

    with jsonlines.open(save_file, mode='w') as writer:
        for (file, harvest_date) in json_list:
            with open(file, 'r') as f:
                data = json.load(f)

            parsed_entries = list()
            for entry in data:
                parsed_entry = parser(entry, harvest_date, release_date)
                parsed_entries.append(parsed_entry)

            for entry in parsed_entries:
                writer.write(entry)

    return jsonlines_files


def load_pickle(file_name: str):
    """ Load a pickle file.

    :param file_name: file to load.
    :return: contents of pickle file.
    """

    with open(file_name, 'rb') as f:
        return pickle.load(f)


def validate_date(date_string):
    """ Validate a date string is pendulum parsable.

    :param date_string: date string to check.
    :return: True if parsable, false otherwise.
    """
    try:
        pendulum.parse(date_string)
    except:
        return False
    return True


def write_json(record, file_name: str):
    """ Write a structure to json file.

    :param record: Structure to write.
    :param file_name: File name to write to.
    """
    with open(file_name, 'w') as f:
        f.write(record)


def write_pickle(record, file_name: str):
    """ Write out a pickle file of a python structure.

    :param record: Structure to write out.
    :param file_name: File name to write to.
    """

    with open(file_name, 'wb') as f:
        pickle.dump(record, f)


def write_pickled_xml_to_json(pickle_files, parser):
    """ Write a list of pickled web responses to json.

    :param pickle_files: list of pickled web response files.
    :param parser: Parsing function that parses the response into json compatible data.
    :return: List of json files written to.
    """

    json_file_list = list()

    for file in pickle_files:
        xml_list = load_pickle(file)

        parsed_list = list()
        for record in xml_list:
            parsed_record = parser(record)
            parsed_list = parsed_list + parsed_record

        json_file = f'{file[:-3]}json'
        json_file_list.append(json_file)
        json_record = json.dumps(parsed_list)
        write_json(json_record, json_file)

    return json_file_list


def zip_files(file_list: List[str]):
    """ GZip up the list of files.

    :param file_list: List of files to zip up.
    :return: List of zipped up file names.
    """

    zip_list = list()
    for file_path in file_list:
        zip_file = f'{file_path}.gz'
        zip_list.append(zip_file)
        with open(file_path, 'rb') as f_in:
            with gzip.open(zip_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    return zip_list
