#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#

import argparse
import csv
import logging
import os
from multiprocessing import cpu_count
from typing import List, Union
from urllib.parse import urlparse, urljoin

import pandas as pd
import ray
import validators

from academic_observatory.grid import download_grid_dataset, index_grid_dataset, get_default_grid_index_path, \
    load_grid_index
from academic_observatory.oai_pmh.oai_pmh import fetch_context_urls, InvalidOaiPmhContextPageException, parse_value, \
    fetch_identify, parse_utc_str_to_date
from academic_observatory.oai_pmh.schema import Identity
from academic_observatory.utils import wait_for_tasks, strip_query_params, get_url_domain_suffix

FETCH_ENDPOINTS_PROCESS_MULTIPLIER = 10


def _get_potential_oai_pmh_urls(source_url: str) -> List[str]:
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


def get_oai_pmh_search_urls(base_url: str) -> List[str]:
    """ Given an OAI-PMH base URL, this method gets a list of potential other URLs to search, including any
    OAI-PMH contexts contained in the base URL page and one path back from the current page.

    :param base_url: a potential OAI-PMH URL.
    :return: a list of URLs.
    """

    urls = _get_potential_oai_pmh_urls(base_url)
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
        except Exception as e:
            # Something wen't wrong trying to fetch the page, but we will try to see if it is an endpoint later
            search_urls.append(url)

    # Only return unique URLs
    return list(set(search_urls))


@ray.remote
def fetch_endpoint(source_url: str, local_mode: bool):
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
        urls = get_oai_pmh_search_urls(source_url)

        for url in urls:
            logging.info(f"Fetching identity for: {url}")

            grid_id = None
            url_ = get_url_domain_suffix(url)
            if url_ in fetch_endpoint.grid_index:
                grid_id = fetch_endpoint.grid_index[url_]

            logging.info(f"GRID ID for: {url} is {grid_id}")
            identify, ex = fetch_identify(url)

            if ex is not None:
                result = {"source_url": url, "ex": str(ex), "grid_id": grid_id}
            else:
                result = Identity(None,
                                  url,
                                  parse_value(identify, "adminEmail"),
                                  parse_value(identify, "author"), parse_value(identify, "baseURL"),
                                  parse_value(identify, "comment"), parse_value(identify, "compression"),
                                  parse_value(identify, "content"), parse_value(identify, "creator"),
                                  parse_value(identify, "dataPolicy"), parse_value(identify, "dc"),
                                  parse_value(identify, "deletedRecord"), parse_value(identify, "delimiter"),
                                  parse_value(identify, "description"),
                                  parse_utc_str_to_date(parse_value(identify, "earliestDatestamp")),
                                  parse_value(identify, "email"), parse_value(identify, "eprints"),
                                  parse_value(identify, "friends"), parse_value(identify, "granularity"),
                                  parse_value(identify, "identifier"), parse_value(identify, "institution"),
                                  parse_value(identify, "metadataPolicy"), parse_value(identify, "name"),
                                  parse_value(identify, "oai-identifier"), parse_value(identify, "protocolVersion"),
                                  parse_value(identify, "purpose"), parse_value(identify, "repositoryIdentifier"),
                                  parse_value(identify, "repositoryName"), parse_value(identify, "rights"),
                                  parse_value(identify, "rightsDefinition"), parse_value(identify, "rightsManifest"),
                                  parse_value(identify, "sampleIdentifier"), parse_value(identify, "scheme"),
                                  parse_value(identify, "submissionPolicy"), parse_value(identify, "text"),
                                  parse_value(identify, "title"), parse_value(identify, "toolkit"),
                                  parse_value(identify, "toolkitIcon"), parse_value(identify, "URL"),
                                  parse_value(identify, "version"), parse_value(identify, "XOAIDescription"),
                                  grid_id)
            results.append(result)
    return results


def fetch_endpoints(input: Union[argparse.FileType, None], key: str, output: Union[argparse.FileType, None],
                    error: Union[argparse.FileType, None], associate_grid: bool,
                    num_processes: int = cpu_count() * FETCH_ENDPOINTS_PROCESS_MULTIPLIER, local_mode: bool = False,
                    timeout: float = 10.):
    """ Given a list of potential OAI-PMH URLs, fetch their meta-data and verify if they are real endpoints.

    :param input: the path to the CSV file that contains the list of potential OAI-PMH endpoint URLs.
    :param key: the name of the column that contains the OAI-PMH endpoint URLs in the input CSV.
    :param output: The path to the CSV file where the valid OAI-PMH endpoint URLs and associated meta-data will be
    saved.
    :param error: the path to the CSV file where the errors will be saved.
    :param associate_grid: whether to associate each OAI-PMH endpoint with a GRID id.
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
    grid_index_path = get_default_grid_index_path()

    if not os.path.exists(grid_index_path) or updated:
        index_grid_dataset()

    logging.info("Load CSV")
    df = pd.read_csv(input, delimiter=',')
    df.dropna(subset=[key], inplace=True)
    df = df.replace({pd.np.nan: None})
    total_sources = df.shape[0]
    logging.info(f"Total sources to load: {total_sources}")

    # Spawn tasks
    fetch_endpoint.grid_index = load_grid_index(grid_index_path)

    logging.info(f"Spawning tasks")
    task_ids = []
    for i, row in df.iterrows():
        source_url = row[key]
        task_id = fetch_endpoint.remote(source_url, local_mode)
        task_ids.append(task_id)

    # Wait for results
    task_results = wait_for_tasks(task_ids)

    # Format results
    endpoint_list = []
    error_list = []
    for task_result in task_results:
        for result in task_result:
            if isinstance(result, Identity):
                endpoint_list.append(result.to_dict())
            else:
                error_list.append(result)

    results_writer = csv.DictWriter(output, fieldnames=endpoint_list[0].keys())
    results_writer.writeheader()
    results_writer.writerows(endpoint_list)
    logging.info(f"Saved endpoints to file: {output.name}")

    error_writer = csv.DictWriter(error, fieldnames=error_list[0].keys())
    error_writer.writeheader()
    error_writer.writerows(error_list)
    logging.info(f"Saved errors to file: {error.name}")
