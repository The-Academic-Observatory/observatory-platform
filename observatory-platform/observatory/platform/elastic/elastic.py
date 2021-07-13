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

# Author: James Diprose


import logging
import os
from multiprocessing import cpu_count
from typing import Dict, Iterator, List
from elasticsearch.helpers import parallel_bulk, scan
import elasticsearch.exceptions
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from pendulum import Date

from observatory.platform.utils.config_utils import module_file_path


def elastic_mappings_path(mappings_filename: str) -> str:
    """ Get the Elasticsearch mappings path.

    :param mappings_filename: the name of the Elasticsearch mapping.
    :return: the elastic search schema path.
    """

    return os.path.join(module_file_path("observatory.dags.database.mappings"), mappings_filename)


def make_sharded_index(index_prefix: str, release_date: Date) -> str:
    """ Make a sharded Elasticsearch index given an index prefix and a date.

    :param index_prefix: the index prefix.
    :param release_date: the date.
    :return: the sharded index.
    """

    return f'{index_prefix}-{release_date.strftime("%Y%m%d")}'


def make_elastic_uri(schema: str, user: str, secret: str, hostname: str, port: int) -> str:
    """ Make an Elasticsearch URI.

    :param schema: the schema, e.g. http or https.
    :param user: Elasticsearch username.
    :param secret: Elasticsearch secret.
    :param hostname: Elasticsearch hostname.
    :param port: Elasticsearch port.
    :return: the full Elasticsearch URI.
    """

    return f"{schema}://{user}:{secret}@{hostname}:{port}"


# def make_index_prefix(feed_name: str, table_name: str):
#     """ Make an index prefix from a feed name and a table name.
#
#     :param feed_name: the feed name.
#     :param table_name: the table name
#     :return: the index prefix.
#     """
#
#     return f"{feed_name}-{table_name}"


class Elastic:
    def __init__(
        self,
        host: str = "http://elasticsearch:9200/",
        thread_count: int = cpu_count(),
        chunk_size: int = 10000,
        timeout: int = 30000,
    ):
        """ Create an Elastic API client.

        :param host: the host including the hostname and port.
        :param thread_count: the number of threads to use when loading data into Elastic.
        :param chunk_size: the batch size for loading documents.
        :param timeout: the timeout in seconds.
        """

        self.host = host
        self.thread_count = thread_count
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.es = Elasticsearch(hosts=[self.host], timeout=timeout)

        # logging.basicConfig()
        # logging.getLogger().setLevel(logging.WARNING)

    def query(self, index: str, query: Dict = None):
        if query is None:
            query = {"query": {"match_all": {}}}
        records = []
        for result in scan(self.es, query=query, index=index):
            records.append(result["_source"])

        return records

    def delete_index(self, index_id: str) -> None:
        """ Delete an Elastic index.

        :param index_id: the index ID.
        :return: None.
        """

        try:
            self.es.indices.delete(index=index_id)
        except elasticsearch.exceptions.TransportError as e:
            logging.warning(f"Index not found: {e}")

    def get_alias_indexes(self, alias_id: str) -> List:
        """ Return the list of indexes associated with an alias.

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
        """ Index documents supplied as an iterator.

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
        """ Yields documents for indexing.

        :param index_id: the index ID.
        :param documents: the document iterator.
        :return: None.
        """

        for doc in documents:
            yield {"_op_type": "index", "_index": index_id, "_source": doc}
