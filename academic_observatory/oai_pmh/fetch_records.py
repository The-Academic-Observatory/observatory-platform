#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#

import datetime
import io
import logging
import os
from multiprocessing import cpu_count
from typing import List
from typing import Union

import pandas as pd
import ray

from academic_observatory.oai_pmh.oai_pmh import fetch_endpoint_records, get_default_oai_pmh_path, \
    get_default_oai_pmh_endpoints_path, oai_pmh_serialize_custom_types
from academic_observatory.oai_pmh.schema import Endpoint, Record
from academic_observatory.utils import wait_for_tasks, to_json_lines


@ray.remote
def task_fetch_records(endpoint: Endpoint, from_date: datetime.datetime, until_date: datetime.datetime) -> List[Record]:
    """ A remote task, for a given OAI-PMH endpoint, fetch a list of OAI-PMH records between two datetimes.

    :param endpoint: an OAI-PMH Endpoint object.
    :param from_date: the datetime to begin fetching records from.
    :param until_date: the datetime to fetch records until.
    :return: a list of Record objects.
    """

    try:
        results = fetch_endpoint_records(endpoint, from_date, until_date)
    except Exception as ex:
        results = [{"source_url": endpoint.source_url, "ex": str(ex)}]

    return results


def load_endpoints(input: Union[io.FileIO, str]) -> List[Endpoint]:
    """ Load a list of Endpoint objects from file.

    :param input: a file path or FileType object.
    :return: a list of Endpoint objects.
    """

    logging.info("Loading endpoints")
    df = pd.read_csv(input, delimiter=',')
    df = df.replace({pd.np.nan: None})
    endpoints = []

    for i, row in df.iterrows():
        endpoint = Endpoint.from_dict(row.to_dict())
        endpoints.append(endpoint)

    return endpoints


def fetch_records(start_date: datetime.date, end_date: datetime.date,
                  input: Union[io.FileIO, str, None] = None,
                  output: Union[str, None] = None,
                  num_processes: int = cpu_count(),
                  local_mode: bool = False, timeout: float = 10.) -> None:
    """ Fetch a list of OAI-PMH records from multiple OAI-PMH endpoints, between two datetimes.

    :param start_date: the date to begin harvesting records from.
    :param end_date: the date to harvest records until.
    :param input: a path or io.FileIO object to load a list of OAI-PMH endpoints from.
    :param output: a path to the directory where the results should be saved.
    :param num_processes: the number of processes to use.
    :param local_mode: whether to run the tasks serially or in parallel.
    :param timeout: the setting used for network and task fetching timeout.
    :return: None.
    """

    # Further validation checks
    assert start_date <= end_date, \
        "argument -s/--start_date, -e/--end_date: start_date must be less than or equal to end_date."

    if not ray.is_initialized():
        ray.init(num_cpus=num_processes, local_mode=local_mode)

    # Load endpoints
    if input is None:
        input = get_default_oai_pmh_endpoints_path()
    endpoints = load_endpoints(input)
    logging.info(f"Total endpoints to harvest: {len(endpoints)}")

    # Create harvest dates
    from_date = datetime.datetime(start_date.year, start_date.month, start_date.day, tzinfo=datetime.timezone.utc)
    until_date = datetime.datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59,
                                   tzinfo=datetime.timezone.utc)
    logging.info(f"Dates to harvest: from {from_date} to {until_date}")

    # Create tasks
    logging.info(f"Spawning tasks")
    task_ids = []
    for endpoint in endpoints:
        task_id = task_fetch_records.remote(endpoint, from_date, until_date)
        task_ids.append(task_id)

    # Wait for results
    task_results = wait_for_tasks(task_ids, wait_time=timeout)

    # Format results
    record_list = []
    error_list = []
    for task_result in task_results:
        for result in task_result:
            if isinstance(result, Record):
                record_list.append(result.to_dict())
            else:
                error_list.append(result)

    # If user supplied no input path use default
    if output is None:
        output = get_default_oai_pmh_path()

    date_now = datetime.datetime.now()
    file_name = os.path.join(output, f"oai_pmh_records_{date_now}.json")
    with open(file_name, 'w') as file:
        json_records = to_json_lines(record_list, serialize_custom_types_func=oai_pmh_serialize_custom_types)
        file.write(json_records)
    logging.info(f"Saved OAI-PMH records to: {file_name}")

    file_name = os.path.join(output, f"oai_pmh_record_errors_{date_now}.json")
    with open(file_name, 'w') as file:
        json_errors = to_json_lines(error_list, serialize_custom_types_func=oai_pmh_serialize_custom_types)
        file.write(json_errors)
    logging.info(f"Saved OAI-PMH record fetching errors to: {file_name}")
