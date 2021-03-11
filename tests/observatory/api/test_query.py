# Copyright 2021 Curtin University
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

import copy
import unittest
from unittest.mock import patch

from elasticsearch import Elasticsearch

from tests.observatory.api.test_elastic import SCROLL_ID, RES_EXAMPLE

from observatory.api.api import queryv1, create_app


class TestQuery(unittest.TestCase):
    """ Tests for the 'query' endpoint of the API. """

    @patch('observatory.api.elastic.Elasticsearch.scroll')
    @patch('observatory.api.elastic.Elasticsearch.search')
    @patch('observatory.api.elastic.create_es_connection')
    @patch('observatory.api.elastic.parse_args')
    def test_query(self, mock_parse_args, mock_create_connection, mock_es_search, mock_es_scroll):
        """ Test elasticsearch search query with different args.

        :return: None.
        """

        app = create_app()
        with app.app.test_client() as test_client:
            # set parse_args return values
            alias = 'subset-agg'
            index_date = None
            from_date = None
            to_date = None
            filter_fields = {}
            size = 10000
            scroll_id = None

            # test es connection is None
            mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, scroll_id)
            mock_create_connection.return_value = None

            error, return_status = queryv1()
            self.assertEqual(400, return_status)

            # mock es connection
            mock_create_connection.return_value = Elasticsearch()

            # test with scroll id
            mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, SCROLL_ID)
            res = copy.deepcopy(RES_EXAMPLE)
            mock_es_scroll.return_value = res

            results = queryv1()
            expected_results = {
                'version': 'v1',
                'index': 'N/A',
                'scroll_id': SCROLL_ID,
                'returned_hits': len(res['hits']['hits']),
                'total_hits': res['hits']['total']['value'],
                'schema': {
                    'schema': 'to_be_created'
                },
                'results': res['hits']['hits']
            }
            self.assertEqual(expected_results, results)

            # with search body, test with empty (invalid) alias
            mock_es_search.return_value = copy.deepcopy(RES_EXAMPLE)
            mock_parse_args.return_value = ('', index_date, from_date, to_date, filter_fields, size, scroll_id)

            error, return_status = queryv1()
            self.assertEqual(400, return_status)

            # with search body, test with valid alias and without index date
            with patch('elasticsearch.client.CatClient.aliases') as mock_es_cat:
                res = copy.deepcopy(RES_EXAMPLE)
                mock_es_search.return_value = res
                mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, scroll_id)
                index_name = 'subset-agg-20201212'
                mock_es_cat.return_value = [{
                    'index': index_name
                }]

                results = queryv1()
                expected_results = {
                    'version': 'v1',
                    'index': index_name,
                    'scroll_id': SCROLL_ID,
                    'returned_hits': len(res['hits']['hits']),
                    'total_hits': res['hits']['total']['value'],
                    'schema': {
                        'schema': 'to_be_created'
                    },
                    'results': res['hits']['hits']
                }
                self.assertEqual(expected_results, results)

            # with search body, test with valid index date
            with patch('elasticsearch.client.IndicesClient.exists') as mock_es_indices:
                res = copy.deepcopy(RES_EXAMPLE)
                mock_es_search.return_value = res
                index_date = '20200101'
                mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, scroll_id)
                mock_es_indices.return_value = True

                results = queryv1()
                expected_results = {
                    'version': 'v1',
                    'index': alias + f"-{index_date}",
                    'scroll_id': SCROLL_ID,
                    'returned_hits': len(res['hits']['hits']),
                    'total_hits': res['hits']['total']['value'],
                    'schema': {
                        'schema': 'to_be_created'
                    },
                    'results': res['hits']['hits']
                }
                self.assertEqual(expected_results, results)

                # with search body, test with invalid index date
                with patch('observatory.api.elastic.list_available_index_dates') as mock_index_dates:
                    res = copy.deepcopy(RES_EXAMPLE)
                    mock_es_search.return_value = res
                    index_date = '20200101'
                    mock_parse_args.return_value = (alias, index_date, from_date, to_date, filter_fields, size, scroll_id)
                    mock_es_indices.return_value = False
                    available_date = '20201212'
                    mock_index_dates.return_value = [available_date]

                    error, return_status = queryv1()
                    self.assertEqual(400, return_status)
                    self.assertIn(available_date, error)
