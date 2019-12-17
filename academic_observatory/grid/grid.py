import logging
from typing import Tuple, List, Union
from urllib.parse import urlparse

import pandas as pd
import tldextract

GRID_CACHE_SUBDIR = "datasets/grid"


def get_url_domain_suffix(url):
    result = tldextract.extract(url)
    return f"{result.domain}.{result.suffix}"


def load_grid_index(grid_index_path):
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
    version = grid_release["version"].replace("release_", "")
    institutes = grid_release["institutes"]
    results = []

    for institute in institutes:
        record = parse_institute(institute)

        if record is not None:
            results.append(record)

    return version, results


def save_grid(path: str, data: List, header=False):
    columns = ['grid_id', 'name', 'type', 'url', 'url_hostname', 'url_domain_suffix', 'country_code']
    df = pd.DataFrame(data=data, columns=columns)
    df.to_csv(path, index=False, header=header)
