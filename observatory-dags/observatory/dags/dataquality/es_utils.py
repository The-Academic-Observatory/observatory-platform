#!/usr/bin/python3

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

# Author: Tuan Chien

from elasticsearch_dsl import Search, Index, Document, connections
from elasticsearch import NotFoundError
from elasticsearch_dsl.document import IndexMeta
from elasticsearch.helpers import bulk

from urllib3.exceptions import ReadTimeoutError
from typing import Union, List
import logging


def init_doc(es_doc: IndexMeta):
    """ Initialise the mappings in elastic search if the document doesn't exist. Will not reinitialise if a mapping
    already exists.

    @param es_doc: Document to initialise.
    """

    try:
        name = es_doc.Index.name
        Search(index=name).count()
        logging.info(f'Elastic search index {name} already mapped. Skipping initialisation.')
    except NotFoundError:
        logging.info(f'Initialising elastic search index {name}.')
        es_doc.init()


def get_or_init_doc_count(es_doc: IndexMeta) -> int:
    """ Get the number of documents stored in elastic search.  If the index doesn't exist, create it.
    @param es_doc: IndexMeta class for the document we want to check.
    @return: Number of documents currently stored in elastic search.
    """

    try:
        # Query number of documents in elastic search.
        name = es_doc.Index.name
        count = Search(index=name).count()
        logging.info(f'Elastic search index {name} already mapped. Skipping initialisation.')
    except NotFoundError:
        logging.info(f'Initialising elastic search index {name}.')
        count = 0
        es_doc.init()

    return count


def clear_index(es_doc: IndexMeta):
    """ Remove all documents from an index.
    @param es_doc: IndexMeta class for the name to delete.
    """

    name = es_doc.Index.name
    logging.info(f'Deleting all elastic search documents from index: {name}')
    Search(index=name).query().delete()


def delete_index(es_doc: IndexMeta):
    """ Delete an index from elastic search.
    @param es_doc: IndexMeta class for the name to delete.
    """

    name = es_doc.Index.name
    logging.info(f'Deleting elastic search index: {name}')
    Index(name).delete()


def search_count_by_release(es_doc: IndexMeta, release: str):
    """ Get the number of documents found in an index for a release.
    @param es_doc: IndexMeta class for the index name.
    @param release: Release date.
    @return: Number of documents found.
    """

    return Search(index=es_doc.Index.name).query('match', release=release).count()


def search(es_doc: IndexMeta, sort_field: Union[None, str] = None, **kwargs):
    s = Search(index=es_doc.Index.name).query('match', **kwargs)
    if sort_field:
        s = s.sort(sort_field)
    return list(s.scan())


def search_by_release(es_doc: IndexMeta, release: str, sort_field: Union[None, str] = None):
    """ Search elastic search index for a particular release document.
    @param es_doc: IndexMeta class for the index name.
    @param release: Release date.
    @param sort_field: Field to sort by or None if not sorting.
    @return: List of documents found.
    """

    return search(es_doc, sort_field=sort_field, release=release)


def bulk_index(docs: List[Document]):
    """ Perform a batched index on a list of documents in elastic search.
    @param docs: List of documents to index.
    """

    try:
        n = len(docs)
        logging.info(f'Bulk indexing {n} documents.')
        bulk(connections.get_connection(), (doc.to_dict(include_meta=True) for doc in docs), refresh=True)
    except ReadTimeoutError:
        logging.error(f'Timed out trying to bulk index {n} documents.')

# def update_by_query():
#     ubq = UpdateByQuery(index=MagReleaseEs.Index.name).query("match", release="2020-09-11") \
#         .script(source="ctx._source.release = '2020-09-12'")
#     ures = ubq.execute()
