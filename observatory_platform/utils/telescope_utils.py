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
import pendulum
import shutil

from airflow.models.taskinstance import TaskInstance
from pathlib import Path
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
    save_file = f'{first_file}l'
    jsonlines_files.append(save_file)

    with jsonlines.open(save_file, mode='w') as writer:
        for (file, harvest_date) in json_list:
            logging.info(f'Parsing {file} into db format and writing to jsonlines')
            with open(file, 'r') as f:
                data = json.load(f)

            parsed_entries = list()
            for entry in data:
                parsed_entry = parser(entry, harvest_date, release_date)
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
    except:
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
