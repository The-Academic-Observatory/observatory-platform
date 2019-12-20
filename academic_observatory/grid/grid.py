#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#

import io
import logging
import os
from typing import Tuple, List, Union
from urllib.parse import urlparse

import pandas as pd

from academic_observatory.utils import get_home_dir, get_url_domain_suffix

GRID_CACHE_SUBDIR = "datasets/grid"
GRID_INDEX_FILENAME = "grid_index.csv"


def get_default_grid_path() -> str:
    """ Get the default path to the GRID dataset.
    :return: the default path to the GRID dataset.
    """

    cache_dir, cache_subdir, datadir = get_home_dir(cache_subdir=GRID_CACHE_SUBDIR)
    return datadir


def get_default_grid_index_path() -> str:
    """ Get default grid dataset path
    :return:
    """

    cache_dir, cache_subdir, datadir = get_home_dir(cache_subdir=GRID_CACHE_SUBDIR)
    return os.path.join(datadir, GRID_INDEX_FILENAME)


def load_grid_index(grid_index_path: Union[str, io.FileIO]) -> dict:
    """ Load the GRID Index.

    :param grid_index_path: the path to the GRID Index.
    :return: the GRID Index.
    """

    grid_index = dict()

    if grid_index_path is not None:
        df = pd.read_csv(grid_index_path,
                         names=['grid_id', 'name', 'type', 'url', 'url_hostname', 'url_domain_suffix', 'county_code'])
        for i, row in df.iterrows():
            url_domain_suffix = row['url_domain_suffix']
            grid_id = row['grid_id']
            grid_index[url_domain_suffix] = grid_id
    else:
        logging.warning("No grid_index_path path specified so grid_index not loaded.")

    return grid_index


def parse_institute(institute: dict) -> Union[None, Tuple]:
    """ Parse an institute from the GRID.ac dataset.

    :param institute: an institute dict.
    :return: a tuple with the institute details.
    """

    result = None
    grid_id = institute["id"]
    status = institute["status"]

    if status == "active":
        name = institute["name"]
        types = institute["types"]
        links = institute["links"]

        # Only create if there is a URL
        if len(links) >= 1:
            url = links[0]
            type = None
            if len(types) >= 1:
                type = types[0]

            # Get country code
            country_code = None
            if "addresses" in institute and len(institute["addresses"]) >= 1 and "country_code" in \
                    institute["addresses"][0]:
                country_code = institute["addresses"][0]["country_code"]

            # Create derivative urls
            parsed_url = urlparse(url)
            url_hostname = parsed_url.netloc  # Strip the protocol, path and query from the URL
            url_domain_suffix = get_url_domain_suffix(url)  # Remove www. from the URL

            result = (grid_id, name, type, url, url_hostname, url_domain_suffix, country_code)

    return result


def parse_grid_release(grid_release: dict) -> Tuple[str, List[Tuple]]:
    """ Parse an entire GRID release.

    :param grid_release: a GRID release dict.
    :return: the version of the GRID release and a list of GRID institute records.
    """
    version = grid_release["version"].replace("release_", "")
    institutes = grid_release["institutes"]
    results = []

    for institute in institutes:
        record = parse_institute(institute)

        if record is not None:
            results.append(record)

    return version, results


def save_grid_index(path: Union[str, io.FileIO], data: List, header=False) -> None:
    """ Save the GRID Index as a CSV.

    :param path: the path to save the GRID Index.
    :param data: the GRID Index records.
    :param header: whether to save the column names as a header in the CSV.
    :return: None.
    """
    columns = ['grid_id', 'name', 'type', 'url', 'url_hostname', 'url_domain_suffix', 'country_code']
    df = pd.DataFrame(data=data, columns=columns)
    df.to_csv(path, index=False, header=header)
