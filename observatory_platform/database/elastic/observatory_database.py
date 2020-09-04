# Copyright 2020 Artificial Dimensions Ltd.
# Copyright 2020 Curtin University.
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


import json
import logging
import os
import pathlib
from typing import Dict, Iterator

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

import observatory_platform.database.elastic.schema


def elastic_schema_path() -> str:
    file_path = pathlib.Path(observatory_platform.database.elastic.schema.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1])
    return str(path.resolve())


class ObservatoryDatabase:

    def __init__(self, host: str = 'http://elasticsearch:9200/'):
        self.host = host
        self.es = Elasticsearch(hosts=[self.host])

    def index_documents(self, index_id: str, mappings_filename: str, documents: Iterator[Dict]):
        # Load mapping and create index
        mappings_path = os.path.join(elastic_schema_path(), mappings_filename)
        with open(mappings_path, 'r') as f:
            mappings = json.load(f)
        body = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": mappings["mappings"]
        }
        self.es.indices.create(index=index_id, body=body, ignore=400)

        # Ingest data
        results = []
        for success, info in parallel_bulk(self.es,
                                           self.__yield_bulk_documents(index_id, documents),
                                           refresh='wait_for'):
            results.append(success)
            if not success:
                logging.error(f'Document failed to index: {success}')
            else:
                logging.info(f'Indexed document: {success}')

        return all(results)

    def __yield_bulk_documents(self, index_id: str, documents: Iterator[Dict]):
        for doc in documents:
            yield {
                '_op_type': 'index',
                '_index': index_id,
                '_source': doc
            }
