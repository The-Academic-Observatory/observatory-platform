#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#

import datetime
import io
import logging
import os
import time
from typing import List, Union, Tuple

import ray
from dateutil import rrule
from psutil import cpu_count

from academic_observatory.common_crawl.cc_fetcher import CCFullTextFetcher
from academic_observatory.common_crawl.common_crawl import get_default_common_crawl_path, save_page_infos
from academic_observatory.common_crawl.db import get_grid_ids, get_page_infos
from academic_observatory.common_crawl.schema import PageInfo
from academic_observatory.grid import download_grid_dataset, index_grid_dataset, get_default_grid_index_path

RECORD_SAVE_THRESHOLD = 10000


def get_fetch_months(start_month: datetime.date, end_month: datetime.date) -> List[datetime.date]:
    """ Fetch the months between two dates.

    :param start_month: the start month.
    :param end_month: the end month.
    :return: a list of months.
    """
    months = []
    for month in rrule.rrule(rrule.MONTHLY, dtstart=start_month, until=end_month):
        months.append(month)
    return months


def split_page_infos(page_infos: List[PageInfo]) -> Tuple[List[PageInfo], List[PageInfo]]:
    """ Split a list of PageInfo objects into two lists, tasks with fetch status of 200 and those
    that failed.

    :param page_infos: the list of PageInfo objects to split.
    :return: a list of PageInfo objects with success status 200 and another list with PageInfo objects that contains
    information about requests that failed.
    """
    success_page_infos: List[PageInfo] = []
    failed_page_infos: List[PageInfo] = []
    for i, page_info in enumerate(page_infos):
        if page_info.warc_index_info.fetch_status == 200:
            failed_page_infos.append(page_info)
        else:
            success_page_infos.append(page_info)

    return success_page_infos, failed_page_infos


def cc_fetch_full_text(table_name: str, start_date: datetime.date, end_date: datetime.date, output: Union[str, None],
                       grid_index: Union[io.FileIO, None] = None, url_type_index: Union[io.FileIO, None] = None,
                       num_processes: int = cpu_count(), local_mode: bool = False, timeout: float = 10.):
    """ Fetch the full text of a set of Common Crawl index records, between a start and end date. Saves them as
    gzipped new line delimited JSON.

    This function requires: a table in BigQuery with the Common Crawl index and the GRID index
    joined and the institutes you would like to get the full text data for in the table. The environment variable
    GOOGLE_APPLICATION_CREDENTIALS needs to be set. See https://cloud.google.com/docs/authentication/getting-started
    for more details.

    :param table_name: the name of the BigQuery table, in the format project-name.dataset-name.table-name
    :param start_date: the start date of the query.
    :param end_date: the end date of the query.
    :param output: the path to a folder where the results should be saved.
    :param grid_index: the path to the GRID index CSV file.
    :param url_type_index: the path to the URL index CSV file.
    :param num_processes: the number of processes to use.
    :param local_mode: whether to run the program serially.
    :param timeout: the time in seconds to wait for completed tasks and to wait for network timeouts.
    :return: None.
    """

    # Further validation checks
    assert start_date <= end_date, \
        "argument -s/--start_date, -e/--end_date: start_date must be less than or equal to end_date."

    start_time = datetime.datetime.now(datetime.timezone.utc)
    start = time.monotonic()

    # Get default GRID index
    if not ray.is_initialized():
        ray.init(num_cpus=num_processes, local_mode=local_mode)

    if grid_index is None:
        grid_dataset_path, updated = download_grid_dataset(num_processes=num_processes, local_mode=local_mode,
                                                           timeout=timeout)
        # GRID index less than GRID dataset
        grid_index = get_default_grid_index_path()

        if not os.path.exists(grid_index) or updated:
            index_grid_dataset()

    # Create output directory
    if output is None:
        output = get_default_common_crawl_path()

    # Create the CCFullTextFetcher workers
    workers: List[CCFullTextFetcher] = []
    for i in range(num_processes):
        worker = CCFullTextFetcher.remote(grid_index_path=grid_index, url_index_path=url_type_index)
        workers.append(worker)
        logging.info(f"Creating workers: {(i + 1) / float(num_processes) * 100.:.0f}%")

    # Get the fetch months from start and end date
    fetch_months = get_fetch_months(start_date, end_date)

    # Iterate through each month and then each grid_id
    for fetch_month in fetch_months:
        grid_ids = get_grid_ids(table_name, fetch_month)

        for grid_id in grid_ids:
            # Get Warc records from a table
            logging.info(f"Fetching WARC indicies for fetch_month: {fetch_month.strftime('%Y-%m')} and "
                         f"grid_id: {grid_id}")
            page_infos = get_page_infos(table_name, grid_id, fetch_month)
            logging.info(f"Number of pages found: {len(page_infos)}")

            ###############################################################
            # Save all non-200 responses and cache the rest for harvesting
            ###############################################################
            batch = []
            to_harvest = []
            batch_index = 0
            for i, page_info in enumerate(page_infos):
                if page_info.warc_index_info.fetch_status == 200:
                    to_harvest.append(page_info)
                else:
                    batch.append(page_info)

                # Save results to google cloud storage if: the size of the save_tasks gets larger than a threshold or
                # we are on the last iteration
                if (i + 1) >= len(page_infos) or len(batch) >= RECORD_SAVE_THRESHOLD:
                    save_page_infos(batch, output, table_name, start_time, grid_id, fetch_month, batch_index)
                    batch_index += 1
                    batch.clear()
            page_infos.clear()  # Don't need this array anymore

            logging.info(f"Number of pages to harvest: {len(to_harvest)}")
            if len(to_harvest) == 0:
                logging.warning(f"Continuing: no PageInfos found in table with a 200 response {table_name}")
                continue

            ###############################################################
            # Harvest the information for the remaining page_infos
            ###############################################################

            # Spawn jobs
            logging.info(f"Spawning jobs")
            task_ids = []
            page_info_index = dict()
            for i, page_info in enumerate(to_harvest):
                worker_index = i % len(workers)
                task_id = workers[worker_index].fetch_page.remote(page_info.warc_index)
                page_info_index[task_id] = page_info
                task_ids.append(task_id)
                logging.info(f"Creating tasks: {(i + 1) / len(to_harvest) * 100.:.0f}%")
            to_harvest.clear()  # Don't need this array anymore

            logging.info(f"Waiting for tasks")
            batch = []
            total_tasks_finished = 0
            while True:
                num_tasks = len(task_ids)
                ready_ids, remaining_ids = ray.wait(task_ids, num_returns=num_tasks, timeout=timeout)
                num_ready = len(ready_ids)
                num_remaining = len(remaining_ids)
                logging.info(f"Tasks finished: {total_tasks_finished}. "
                             f"Tasks ready: {num_ready}. Tasks: remaining: {num_remaining}.")
                total_tasks_finished += num_ready

                # Add all page infos that we currently have
                for ready_id in ready_ids:
                    results = ray.get(ready_id)
                    if results is not None:
                        # Update the page info object with new data
                        title, links, text_content, raw_content = results
                        page_info = page_info_index[ready_id]
                        page_info.title = title
                        page_info.links = links
                        page_info.text_content = text_content
                        page_info.raw_content = raw_content
                        batch.append(page_info)
                task_ids = remaining_ids

                # Save results to google cloud storage if: the size of the returned info gets larger than a threshold or
                # all tasks have finished
                all_tasks_finished = len(remaining_ids) == 0
                if all_tasks_finished or len(batch) >= RECORD_SAVE_THRESHOLD:
                    save_page_infos(batch, output, table_name, start_time, grid_id, fetch_month, batch_index)
                    batch.clear()
                    batch_index += 1

                # If all tasks finished then break
                if all_tasks_finished:
                    break

    end = time.monotonic()
    duration = (end - start) / 60.
    logging.info("All tasks finished")
    logging.info(f"Run time: {duration:.2f} minutes")
    logging.info(f"Data saved to: {output}")
