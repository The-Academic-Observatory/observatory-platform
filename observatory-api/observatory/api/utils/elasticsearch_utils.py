# Copyright 2022 Curtin University
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

from typing import List, Optional, Tuple, Union

import pendulum
from connexion import request

from elasticsearch import Elasticsearch
import os
from observatory.api.utils.exception_utils import AuthError, APIError
from observatory.api.utils.auth_utils import has_scope
from abc import abstractmethod
from typing import Dict


class ElasticsearchIndex:
    def __init__(self, es: Elasticsearch, agg: str, subagg: Optional[str], index_date: Optional[str]):
        self.es = es
        self.agg = agg
        self.subagg = subagg

        # Create alias from aggregate and subaggregate
        self.alias = self.set_alias()

        # Check if scope is in access token
        required_scope = self.get_required_scope()
        if not has_scope(required_scope):
            raise AuthError(
                {
                    "code": "missing_scope",
                    "description": f"Required scope '{required_scope}' for this alias is not available in access token.",
                }
            )

        # Set index date
        index_dates = list_available_index_dates(es, self.alias)
        if index_date:
            self.index_date = index_date

            # Check if given index date is in available dates
            if self.index_date not in index_dates:
                index_dates_str = "\n".join(index_dates)
                raise APIError(
                    {
                        "code": "index_error",
                        "description": f"Index not available for given index date: {index_date}.\nAvailable "
                        f"index dates: {index_dates_str}",
                    },
                    500,
                )
        else:
            if index_dates:
                self.index_date = index_dates[0]
            else:
                raise APIError({"code": "index_error", "description": "No dates available for given index"}, 500)

    @property
    def name(self) -> str:
        """"""
        return f"{self.alias}-{self.index_date}"

    @property
    def agg_field(self) -> str:
        """"""
        return self.agg_mappings.get(self.agg)

    @property
    def subagg_field(self) -> Optional[str]:
        """"""
        return self.subagg_mappings.get(self.subagg)

    @property
    @abstractmethod
    def agg_mappings(self) -> Dict[str, str]:
        pass

    @property
    @abstractmethod
    def subagg_mappings(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def get_required_scope(self) -> str:
        pass

    @abstractmethod
    def set_alias(self) -> [bool, str]:
        pass


def parse_args() -> Tuple[List[str], List[str], str, str, str, int, str, str, bool]:
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

    agg_ids = request.args.getlist("agg_id")
    subagg_ids = request.args.getlist("subagg_id")
    index_date = request.args.get("index_date")
    from_date = request.args.get("from")
    to_date = request.args.get("to")
    limit = request.args.get("limit")
    search_after = request.args.get("search_after")
    pit = request.args.get("pit")
    pretty_print = request.args.get("pretty", False)

    # Convert index date to YYYYMMDD format
    index_date = pendulum.parse(index_date).strftime("%Y%m%d") if index_date else None

    # Convert from/to date to YYYY-12-31 format
    from_date = f"{from_date}-12-31" if from_date else None
    to_date = f"{to_date}-12-31" if to_date else None

    # Set size to limit when given and under 10000, else set size to 10000
    max_size = 10000
    if limit:
        limit = int(limit)
        size = min(max_size, limit)
    else:
        size = max_size

    return agg_ids, subagg_ids, index_date, from_date, to_date, size, search_after, pit, pretty_print


def create_es_connection() -> Union[Elasticsearch, str]:
    """Create an elasticsearch connection

    :return: elasticsearch connection
    """
    api_key = os.environ.get("ES_API_KEY", "")
    address = os.environ.get("ES_HOST", "")

    for value in [address, api_key]:
        if value == "":
            raise APIError(
                {
                    "code": "invalid_es_connection",
                    "description": "Environment variable(s) for Elasticsearch host and/or api key is empty",
                },
                500,
            )
    es = Elasticsearch(address, api_key=api_key)
    if not es.ping():
        raise APIError(
            {
                "code": "invalid_es_connection",
                "description": "Could not connect to elasticsearch server. Host and/or api_key are not empty, "
                "but might be invalid.",
            },
            500,
        )
    return es


def list_available_index_dates(es: Elasticsearch, alias: str) -> List[str]:
    """For a given index name (e.g. journals-institution), list which dates are available

    :param es: elasticsearch connection
    :param alias: index alias
    :return: list of available dates for given index
    """
    available_dates = []
    # Search for indices that include alias, is not an exact match
    available_indices = es.cat.indices(alias, format="json")
    for index in available_indices:
        if index["index"].startswith(alias):
            index_date = index["index"][-8:]
            available_dates.append(index_date)
    return available_dates


def create_search_body(
    agg_field: str,
    agg_ids: Optional[List[str]],
    subagg_field: Optional[str],
    subagg_ids: Optional[List[str]],
    from_year: Optional[str],
    to_year: Optional[str],
    size: int,
    search_after: str = None,
    pit_id: str = None,
) -> dict:
    """Create a search body that is passed on to the elasticsearch 'search' method.

    :param agg_field: The aggregate that is queried
    :param agg_ids: List of aggregate values on which is filtered
    :param subagg_field: The subaggregate that is queried
    :param subagg_ids: List of subaggregate values on which is filtered
    :param from_year: Refers to published year, add to 'range'. Include results where published year >= from_year
    :param to_year: Refers to published year, add to 'rangen'. Include results where published year < to_year
    :param size: The returned size (number of hits)
    :param search_after: Return results from after this unique id (used for pagination)
    :param pit_id: The unique point in time IDn (used for pagination)
    :return: search body
    """
    filter_list = []
    # Add filters for aggregate and subaggregate
    if agg_ids:
        filter_list.append({"terms": {f"{agg_field}.keyword": agg_ids}})
    if subagg_field and subagg_ids:  # ignore subagg ids for ao_*_metrics index
        filter_list.append({"terms": {f"{subagg_field}.keyword": subagg_ids}})

    # Add filters for year range
    if from_year or to_year:
        range_dict = {"range": {"published_year": {"format": "yyyy-MM-dd"}}}
        if from_year:
            range_dict["range"]["published_year"]["gte"] = from_year
        if to_year:
            range_dict["range"]["published_year"]["lte"] = to_year
        filter_list.append(range_dict)

    # Sort on agg id and subagg id if available
    search_body = {"size": size, "query": {"bool": {"filter": filter_list}}, "track_total_hits": True}

    # Use search after text to continue search
    if search_after:
        search_body["search_after"] = [search_after]

    # Use Point In Time id to query index frozen at specific time
    if pit_id:
        search_body["pit"] = {"id": pit_id, "keep_alive": "1m"}  # Extend PIT with 1m
        # Use _shard_doc to sort, more efficient but only available with PIT
        search_body["sort"] = "_shard_doc"
    else:
        # Use doc id to sort
        search_body["sort"] = "_id"
    return search_body


def process_response(res: dict) -> Tuple[Optional[str], Optional[str], list, Optional[str]]:
    """Get the search_after id and hits from the response of an elasticsearch search query.

    :param res: The response.
    :return: pit id, search after and hits
    """
    # TODO return only source fields?
    # # Return source fields only
    # source_hits = []
    # for hit in res["hits"]["hits"]:
    #     source = {}
    #     # Flatten nested dictionary '_source'
    #     for k, v in hit["_source"].items():
    #         source[k] = v
    #     source_hits.append(source)
    # search_after_text = None
    # if res["hits"]["hits"]:
    #     search_after = res["hits"]["hits"][-1]['sort']
    #     search_after_text = search_after[0]

    # Flatten nested dictionary '_source'
    hits = res["hits"]["hits"]
    for hit in hits:
        source = hit.pop("_source")
        for k, v in source.items():
            hit[k] = v

    # Get the sort value for the last item (sorted by aggregate id)
    search_after_text = None
    if hits:
        search_after = hits[-1]["sort"]
        search_after_text = search_after[0]

    # Get PIT id which might be updated after search
    new_pit_id = res.get("pit_id")

    # Get how long request took
    took = res.get("took")
    return new_pit_id, search_after_text, hits, took
