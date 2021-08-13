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

# Author: Aniek Roelofs

from typing import List, Tuple, Union
import pendulum
from elasticsearch import Elasticsearch
from flask import request

QUERY_FILTER_PARAMETERS = [
    "id",
    "name",
    "published_year",
    "coordinates",
    "country",
    "country_code",
    "region",
    "subregion",
    "access_type",
    "label",
    "status",
    "collaborator_coordinates",
    "collaborator_country",
    "collaborator_country_code",
    "collaborator_id",
    "collaborator_name",
    "collaborator_region",
    "collaborator_subregion",
    "field",
    "source",
    "funder_country_code",
    "funder_name",
    "funder_sub_type",
    "funder_type",
    "journal",
    "output_type",
    "publisher",
]


def create_es_connection(address: str, api_key: str) -> Union[Elasticsearch, None]:
    """Create an elasticsearch connection

    :param address: elasticsearch address
    :param api_key: elasticsearch API key
    :return: elasticsearch connection
    """
    for value in [address, api_key]:
        if value is None or value == "":
            return None
    es = Elasticsearch(address, api_key=api_key)
    if not es.ping():
        raise ConnectionError(
            "Could not connect to elasticsearch server. Host and/or api_key are not empty, " "but might be invalid."
        )
    return es


def list_available_index_dates(es: Elasticsearch, alias: str) -> List[str]:
    """For a given index name (e.g. journals-institution), list which dates are available

    :param es: elasticsearch connection
    :param alias: index alias
    :return: list of available dates for given index
    """
    available_dates = []
    # search for indices that include alias, is not an exact match
    available_indices = es.cat.indices(alias, format="json")
    for index in available_indices:
        index_date = index["index"][-8:]
        available_dates.append(index_date)
    return available_dates


def create_search_body(from_year: Union[str, None], to_year: Union[str, None], filter_fields: dict, size: int) -> dict:
    """Create a search body that is passed on to the elasticsearch 'search' method.

    :param from_year: Refers to published year, add to 'range'. Include results where published year >= from_year
    :param to_year: Refers to published year, add to 'rangen'. Include results where published year < to_year
    :param filter_fields: Add each field and their value in filter_fields as a filter term.
    :param size: The returned size (number of hits)
    :return: search body
    """
    filter_list = []
    for field in filter_fields:
        # add if value is not None
        if filter_fields[field]:
            filter_list.append({"terms": {f"{field}.keyword": filter_fields[field]}})
    if from_year or to_year:
        range_dict = {"range": {"published_year": {"format": "yyyy-MM-dd"}}}
        if from_year:
            range_dict["range"]["published_year"]["gte"] = from_year
        if to_year:
            range_dict["range"]["published_year"]["lt"] = to_year
        filter_list.append(range_dict)
    query_body = {"bool": {"filter": filter_list}}

    search_body = {"size": size, "query": query_body, "sort": ["_doc"]}
    return search_body


def process_response(res: dict) -> Tuple[str, list]:
    """Get the scroll id and hits from the response of an elasticsearch search query.

    :param res: The response.
    :return: scroll id and hits
    """
    scroll_id = res["_scroll_id"]
    # flatten nested dictionary '_source'
    for hit in res["hits"]["hits"]:
        source = hit.pop("_source")
        for k, v in source.items():
            hit[k] = v
    hits = res["hits"]["hits"]
    return scroll_id, hits


def create_schema():
    """Create schema for the given index that is queried. Useful if there are no results returned.

    :return: schema of index
    """
    return {"schema": "to_be_created"}


def parse_args() -> Tuple[str, str, str, str, dict, int, str]:
    """Parse the arguments coming in from the request.
    alias: concatenate 'subset' and 'agg'
    index_date: directly from requests.args. None allowed
    from_date: from_date + '-12-31'. None allowed
    to_date: to_date + '-12-31'. None allowed
    filter_fields: directly from requests.args for each item in 'query_filter_parameters'. Empty dict allowed
    size: If 'limit' is given -> set to 'limit', can't be more than 10000. If no 'limit' -> 10000
    scroll_id: directly from requests.args

    :return: alias, index_date, from_date, to_date, filter_fields, size, scroll_id
    """

    agg = request.args.get("agg")
    subset = request.args.get("subset")
    index_date = request.args.get("index_date")
    from_date = request.args.get("from")
    to_date = request.args.get("to")
    limit = request.args.get("limit")
    scroll_id = request.args.get("scroll_id")

    # get filter keys/values from list of filter parameters
    filter_fields = {}
    for field in QUERY_FILTER_PARAMETERS:
        value = request.args.get(field)
        if value:
            value = value.split(",")
        filter_fields[field] = value

    index_date = pendulum.parse(index_date).strftime("%Y%m%d") if index_date else None
    from_date = f"{from_date}-12-31" if from_date else None
    to_date = f"{to_date}-12-31" if to_date else None

    # TODO determine which combinations/indices we can use
    if agg == "author" or agg == "funder":
        agg += "_test"
    if agg == "publisher" and subset == "collaborations":
        return "", "", "", "", {}, 0, ""
    alias = f"{subset}-{agg}"

    max_size = 10000
    if limit:
        limit = int(limit)
        size = min(max_size, limit)
    else:
        size = max_size

    return alias, index_date, from_date, to_date, filter_fields, size, scroll_id
