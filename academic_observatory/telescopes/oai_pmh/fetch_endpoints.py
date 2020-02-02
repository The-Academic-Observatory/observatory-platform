#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#

import csv
import io
import logging
import os
from multiprocessing import cpu_count
from typing import List, Union
from urllib.parse import urlparse, urljoin

import pandas as pd
import ray
import validators

from academic_observatory.telescopes.grid import grid_index_path, load_grid_index, download_grid_dataset, \
    index_grid_dataset
from academic_observatory.telescopes.oai_pmh.oai_pmh import fetch_context_urls, InvalidOaiPmhContextPageException, \
    fetch_endpoint, oai_pmh_path, __OAI_PMH_ENDPOINTS_FILENAME
from academic_observatory.telescopes.oai_pmh.schema import Endpoint
from academic_observatory.utils import wait_for_tasks, strip_query_params, get_url_domain_suffix

FETCH_ENDPOINTS_PROCESS_MULTIPLIER = 10


def possible_oai_pmh_urls(source_url: str) -> List[str]:
    """ Create two URLs: the source_url with query parameters stripped and another the same but with the last element
    of the URL path removed. The purpose of the second URL is to help with finding OAI-PMH context pages when only
    one of the OAI-PMH endpoint context URL has been given.

    :param source_url: the OAI-PMH source URL.
    :return: potential OAI-PMH URLs.
    """

    urls = []

    # Remove any query parameters
    stripped = strip_query_params(source_url)
    urls.append(stripped)

    # Remove last component of path
    url = urlparse(stripped)
    path_components = url.path.split('/')
    if len(path_components) > 0:
        urls.append(urljoin(stripped[:-len(url.path)], '/'.join(path_components[:-1])))

    return urls


def oai_pmh_search_urls(base_url: str) -> List[str]:
    """ Given an OAI-PMH base URL, this method gets a list of potential other URLs to search, including any
    OAI-PMH contexts contained in the base URL page and one path back from the current page.

    :param base_url: a potential OAI-PMH URL.
    :return: a list of URLs.
    """

    urls = possible_oai_pmh_urls(base_url)
    search_urls = []
    for url in urls:
        try:
            logging.info(f"Fetching context urls from url: {url}")
            context_urls = fetch_context_urls(url)
            search_urls += context_urls
        except InvalidOaiPmhContextPageException as e:
            # Not a valid context page, but could be a valid OAI-PMH endpoint
            search_urls.append(url)
            logging.warning(f"Invalid context page: {e}")
        except Exception:
            # Something wen't wrong trying to fetch the page, but we will try to see if it is an endpoint later
            search_urls.append(url)

    # Only return unique URLs
    return list(set(search_urls))


@ray.remote
def task_fetch_endpoint(source_url: str, local_mode: bool):
    """ Fetch meta-data about an OAI-PMH  endpoint.

    :param source_url: the endpoint URL.
    :param local_mode: whether the system should run in local mode or not.
    :return: a list of endpoints or errors that were found from the source_url.
    """

    if not local_mode:
        logging.basicConfig(level=logging.INFO)

    results = []

    if not validators.url(source_url):
        ex = f"Invalid URL: {source_url}"
        results.append({"source_url": source_url, "ex": str(ex)})
        logging.error(ex)
    else:
        urls = oai_pmh_search_urls(source_url)

        for url in urls:
            logging.info(f"Fetching identity for: {url}")

            grid_id = None
            url_ = get_url_domain_suffix(url)
            if url_ in task_fetch_endpoint.grid_index:
                grid_id = task_fetch_endpoint.grid_index[url_]

            logging.info(f"GRID ID for: {url} is {grid_id}")

            try:
                result = fetch_endpoint(url)
                result.grid_id = grid_id
            except Exception as ex:
                result = {"source_url": url, "ex": str(ex), "grid_id": grid_id}

            results.append(result)
    return results


def fetch_endpoints(input: Union[io.FileIO, None], key: str, output: Union[str, None],
                    num_processes: int = cpu_count() * FETCH_ENDPOINTS_PROCESS_MULTIPLIER, local_mode: bool = False,
                    timeout: float = 10.):
    """ Given a list of potential OAI-PMH URLs, fetch their meta-data and verify if they are real endpoints.

    :param input: the path to the CSV file that contains the list of potential OAI-PMH endpoint URLs.
    :param key: the name of the column that contains the OAI-PMH endpoint URLs in the input CSV.
    :param output: The path to the directory where the results should be saved.
    :param num_processes: the number of processes to use when processing jobs. By default it is the number of CPU cores
    multiplied by 16.
    :param local_mode: whether to run the program serially.
    :param timeout: the timeout to use when fetching resources over the network.
    :return: None.
    """

    # TODO later start and stop ray and give different num_cpus
    if not ray.is_initialized():
        ray.init(num_cpus=num_processes, local_mode=local_mode)

    grid_dataset_path, updated = download_grid_dataset(num_processes=num_processes, local_mode=local_mode,
                                                       timeout=timeout)
    # TODO get modification time for each file and update if
    # GRID index less than GRID dataset
    grid_index_path_ = grid_index_path()

    if not os.path.exists(grid_index_path_) or updated:
        index_grid_dataset()

    logging.info("Load CSV")
    df = pd.read_csv(input, delimiter=',')
    df.dropna(subset=[key], inplace=True)
    df = df.replace({pd.np.nan: None})
    total_sources = df.shape[0]
    logging.info(f"Total sources to load: {total_sources}")

    # Spawn tasks
    task_fetch_endpoint.grid_index = load_grid_index(grid_index_path_)

    logging.info(f"Spawning tasks")
    task_ids = []
    for i, row in df.iterrows():
        source_url = row[key]
        task_id = task_fetch_endpoint.remote(source_url, local_mode)
        task_ids.append(task_id)

    # Wait for results
    task_results = wait_for_tasks(task_ids)

    # Format results
    endpoint_list = []
    error_list = []
    for task_result in task_results:
        for result in task_result:
            if isinstance(result, Endpoint):
                endpoint_list.append(result.to_dict())
            else:
                error_list.append(result)

    if output is None:
        output = oai_pmh_path()

    if len(endpoint_list) > 0:
        results_file_name = os.path.join(output, __OAI_PMH_ENDPOINTS_FILENAME)
        with open(results_file_name, 'w') as file:
            results_writer = csv.DictWriter(file, fieldnames=endpoint_list[0].keys())
            results_writer.writeheader()
            results_writer.writerows(endpoint_list)
        logging.info(f"Saved endpoints to file: {results_file_name}")

    if len(error_list) > 0:
        errors_file_name = os.path.join(output, "oai_pmh_endpoint_errors.csv")
        with open(errors_file_name, 'w') as file:
            error_writer = csv.DictWriter(file, fieldnames=error_list[0].keys())
            error_writer.writeheader()
            error_writer.writerows(error_list)
        logging.info(f"Saved errors to file: {errors_file_name}")
