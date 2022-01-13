# Copyright 2020, 2021 Curtin University
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

# Author: James Diprose, Tuan Chien


import logging
from dataclasses import dataclass
from enum import Enum
from multiprocessing import cpu_count
from typing import Dict, Iterable, Iterator, List

import elasticsearch.exceptions
import pendulum
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan
from pendulum import Date


def make_sharded_index(index_prefix: str, release_date: Date) -> str:
    """Make a sharded Elasticsearch index given an index prefix and a date.

    :param index_prefix: the index prefix.
    :param release_date: the date.
    :return: the sharded index.
    """

    return f'{index_prefix}-{release_date.strftime("%Y%m%d")}'


def make_elastic_uri(schema: str, user: str, secret: str, hostname: str, port: int) -> str:
    """Make an Elasticsearch URI.

    :param schema: the schema, e.g. http or https.
    :param user: Elasticsearch username.
    :param secret: Elasticsearch secret.
    :param hostname: Elasticsearch hostname.
    :param port: Elasticsearch port.
    :return: the full Elasticsearch URI.
    """

    return f"{schema}://{user}:{secret}@{hostname}:{port}"


class KeepOrder(Enum):
    """Specify to stale index cleaner whether to keep newest, or oldest indices."""

    newest = "newest"
    oldest = "oldest"


@dataclass
class KeepInfo:
    """Information on how do delete stale Elastic indices."""

    num: int = 2
    ordering: KeepOrder = KeepOrder.newest


class StaleIndexCleaner:
    """Helper class to delete stale elastic search indices."""

    @staticmethod
    def has_yyyymmdd_suffix(index: str) -> bool:
        """Check if a string ends with YYYYMMDD.

        :param index: Index string to check.
        :return: Whether it has YYYYMMDD suffix.
        """

        suffix = index[-8:]  # Last 8 chars should be YYYYMMDD
        try:
            pendulum.parse(suffix)
            return True
        except:
            return False

    @staticmethod
    def bucket_indices(indices: Iterable) -> dict:
        """Organises the indices into buckets, where each index in the same bucket has the same string before the
        YYYYMMDD suffix.

        :param indices: List of indices to bucket.
        :return: Dictionary of index buckets.
        """

        buckets = {}

        for index in indices:
            key = index[:-9]  # string before -YYYYMMDD
            buckets[key] = buckets.get(key, list())
            buckets[key].append(index)

        return buckets

    @staticmethod
    def sort_buckets(*, buckets: dict, order: KeepOrder) -> dict:
        """Sort the buckets into either increasing date or decreasing date sequences.

        :param buckets: Buckets to sort.
        :param order: Sorting order.
        :return: Sorted buckets.
        """

        sorted_buckets = {}

        for key, bucket in buckets.items():
            if order == KeepOrder.oldest:
                sorted_buckets[key] = sorted(bucket)
            else:
                sorted_buckets[key] = sorted(bucket, reverse=True)

        return sorted_buckets

    @staticmethod
    def get_deletion_list(*, buckets: dict, num_keep: int) -> List[str]:
        """Get a list of stale indices to delete.

        :param buckets: Buckets to get indices from.
        :param num_keep: Maximum number of indices to keep from each bucket.
        :return: List of indices to delete.
        """

        result = list()

        for bucket in buckets.values():
            result.extend(bucket[num_keep:])

        return result


class Elastic:
    def __init__(
        self,
        host: str = "http://elasticsearch:9200/",
        thread_count: int = cpu_count(),
        chunk_size: int = 10000,
        timeout: int = 30000,
        api_key_id: str = None,
        api_key: str = None,
    ):
        """Create an Elastic API client.

        :param host: the host including the hostname and port.
        :param thread_count: the number of threads to use when loading data into Elastic.
        :param chunk_size: the batch size for loading documents.
        :param timeout: the timeout in seconds.
        :param api_key_id: the Elastic API key id.
        :param api_key: the Elastic API key.
        """

        self.host = host
        self.thread_count = thread_count
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.es = Elasticsearch(hosts=[self.host], timeout=timeout, api_key=(api_key_id, api_key))

    def query(self, index: str, query: Dict = None):
        if query is None:
            query = {"query": {"match_all": {}}}
        records = []
        for result in scan(self.es, query=query, index=index):
            records.append(result["_source"])

        return records

    def delete_index(self, index_id: str) -> None:
        """Delete an Elastic index.

        :param index_id: the index ID.
        :return: None.
        """

        try:
            self.es.indices.delete(index=index_id)
        except elasticsearch.exceptions.TransportError as e:
            logging.warning(f"Index not found: {e}")

    def delete_indices(self, indices: List[str]):
        """Delete a list of Elastic indices.

        :param indices: List of Elastic indices to delete.
        """

        for index in indices:
            self.delete_index(index)

    def get_alias_indexes(self, alias_id: str) -> List:
        """Return the list of indexes associated with an alias.

        :param alias_id: the alias identifier.
        :return: the list of indexes.
        """

        index_ids = []
        try:
            indexes: Dict = self.es.indices.get(alias_id)
            for key, val in indexes.items():
                index_ids.append(key)
        except elasticsearch.exceptions.NotFoundError:
            pass

        return index_ids

    def index_documents(self, index_id: str, mappings: Dict, documents: Iterator[Dict]) -> bool:
        """Index documents supplied as an iterator.

        :param index_id: the index ID.
        :param mappings: the mappings supplied as a Dict.
        :param documents: the documents supplied as an iterator.
        :return: whether the documents were loaded successfully or not.
        """

        # Load mapping and create index
        body = {
            "settings": {
                "index": {"number_of_shards": 1, "number_of_replicas": 1},
                "analysis": {
                    "analyzer": {
                        "text_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding"],
                        }
                    }
                },
            },
            "mappings": mappings["mappings"],
        }
        self.es.indices.create(index=index_id, body=body, ignore=400)

        # Ingest data
        results = []
        for success, info in parallel_bulk(
            self.es,
            self._yield_bulk_documents(index_id, documents),
            refresh="wait_for",
            queue_size=self.thread_count,
            thread_count=self.thread_count,
            chunk_size=self.chunk_size,
            request_timeout=self.timeout,
        ):
            results.append(success)
            if not success:
                logging.error(f"Document failed to index: {success}")
            else:
                logging.debug(f"Indexed document: {success}")

        return all(results)

    def _yield_bulk_documents(self, index_id: str, documents: Iterator[Dict]):
        """Yields documents for indexing.

        :param index_id: the index ID.
        :param documents: the document iterator.
        :return: None.
        """

        for doc in documents:
            yield {"_op_type": "index", "_index": index_id, "_source": doc}

    def create_index(self, index: str, **kwargs):
        """Create a new Elastic index.

        :param index: Index to create.
        :param **kwargs: Any additional arguments to pass to the Elasticsearch client.
        """

        self.es.indices.create(index=index, **kwargs)

    def list_indices(self, index: str) -> List[str]:
        """List all indices matching the query name. The name can contain wildcards.
        :param index: Index name to query.
        :return: List of matching index names.
        """

        try:
            response = self.es.indices.get_alias(index=index)
            indices = response.keys()
            return list(indices)
        except:
            return list()

    def delete_stale_indices(self, *, index: str, keep_info: KeepInfo):
        """Keep a certain number of most recent releases (or oldest) for an index name.  The index name can
        have wildcard if you are not matching an exact index name. For example, test-name* will match all indices with
        the test-name prefix. The indices must end in the suffix -YYYYMMDD.  Indices not ending with -YYYYMMDD will be
        ignored.

        If more than one result is returned, the names will be clustered so that indices with the same name before the
        -YYYYMMDD suffix are together.  Each cluster will be sorted by the date in the suffix string, and the stale
        indices will be deleted.

        :param index: Index search string to delete stale versions of.
        :param keep_info: Metadata about how to order indices, and how many indices to keep.
        """

        # Get a list of indices matching our string.
        indices = self.list_indices(index)

        # Prune names that are too short.
        indices = list(filter(lambda x: len(x) > 9, indices))  # Length of -YYYYMMDD is 9

        # Prune names that dont have -YYYYMMDD suffix.
        indices = list(filter(StaleIndexCleaner.has_yyyymmdd_suffix, indices))

        # Put same prefixes into the same bucket. Use hash table.
        buckets = StaleIndexCleaner.bucket_indices(indices)

        # Sort each bucket. If reverse is false, sort descending, else sort ascending.
        sorted_buckets = StaleIndexCleaner.sort_buckets(buckets=buckets, order=keep_info.ordering)

        # Calculate indices to delete
        deletion_list = StaleIndexCleaner.get_deletion_list(buckets=sorted_buckets, num_keep=keep_info.num)

        # For each index in the tail, delete the index.
        self.delete_indices(deletion_list)
