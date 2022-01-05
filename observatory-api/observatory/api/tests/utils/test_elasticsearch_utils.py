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

import os
import unittest
from typing import Dict
from unittest.mock import patch

from elasticsearch import Elasticsearch
from observatory.api.server.api import create_app
from observatory.api.utils.elasticsearch_utils import (
    ElasticsearchIndex,
    create_es_connection,
    create_search_body,
    list_available_index_dates,
    parse_args,
    process_response,
)
from observatory.api.utils.exception_utils import APIError, AuthError


class UnittestElasticsearchIndex(ElasticsearchIndex):
    def __init__(self, es: Elasticsearch, agg: str, subagg: str = None, index_date: str = None):
        super().__init__(es, agg, subagg, index_date)

    @property
    def agg_mappings(self) -> Dict[str, str]:
        return {
            "agg": "es_agg_id",
        }

    @property
    def subagg_mappings(self) -> Dict[str, str]:
        return {"subagg": "es_subagg_id"}

    def get_required_scope(self) -> str:
        scope = "group:COKI%20Team"
        return scope

    def set_alias(self) -> [bool, str]:
        if self.subagg:
            alias = f"{self.agg}-{self.subagg}"
        else:
            alias = f"{self.agg}-unique-list"
        return alias


@patch("observatory.api.utils.elasticsearch_utils.Elasticsearch.ping")
@patch("observatory.api.utils.elasticsearch_utils.has_scope")
@patch("observatory.api.utils.elasticsearch_utils.list_available_index_dates")
@patch.dict(os.environ, {"ES_HOST": "address", "ES_API_KEY": "api_key"})
class TestElasticsearchIndex(unittest.TestCase):
    def test_valid_init(self, mock_list_dates, mock_has_scope, mock_es_ping):
        # Set up mock values
        mock_list_dates.return_value = ["20220101", "20220201"]
        mock_has_scope.return_value = True
        mock_es_ping.return_value = True

        es = create_es_connection()

        # Create class instance with agg and subagg
        es_index = UnittestElasticsearchIndex(es, "agg", "subagg", "20220101")

        self.assertIsInstance(es_index, ElasticsearchIndex)
        self.assertEqual("agg", es_index.agg)
        self.assertEqual("es_agg_id", es_index.agg_field)
        self.assertDictEqual(
            {
                "agg": "es_agg_id",
            },
            es_index.agg_mappings,
        )
        self.assertEqual("agg-subagg", es_index.alias)
        self.assertEqual(es, es_index.es)
        self.assertEqual("20220101", es_index.index_date)
        self.assertEqual("agg-subagg-20220101", es_index.name)
        self.assertEqual("subagg", es_index.subagg)
        self.assertEqual("es_subagg_id", es_index.subagg_field)
        self.assertDictEqual(
            {
                "subagg": "es_subagg_id",
            },
            es_index.subagg_mappings,
        )

        # Create class instance with agg only
        es_index = UnittestElasticsearchIndex(es, "agg", None, "20220101")

        self.assertEqual("agg-unique-list", es_index.alias)
        self.assertEqual("agg-unique-list-20220101", es_index.name)
        self.assertEqual(None, es_index.subagg)
        self.assertEqual(None, es_index.subagg_field)

        # Create class instance without given index date
        es_index = UnittestElasticsearchIndex(es, "agg", None, None)
        self.assertEqual("20220101", es_index.index_date)

    def test_invalid_scope_init(self, mock_list_dates, mock_has_scope, mock_es_ping):
        # Set up mock values
        mock_list_dates.return_value = ["20220101"]
        mock_has_scope.return_value = False
        mock_es_ping.return_value = True

        # Create class instance
        es = create_es_connection()
        with self.assertRaises(AuthError):
            UnittestElasticsearchIndex(es, "agg", "subagg", "20220101")

    def test_invalid_date_init(self, mock_list_dates, mock_has_scope, mock_es_ping):
        # Set up mock values
        mock_list_dates.side_effect = [["20220101"], []]
        mock_has_scope.return_value = True
        mock_es_ping.return_value = True

        # Create class instance
        es = create_es_connection()

        # Test when date given, but not in dates available
        with self.assertRaises(APIError):
            UnittestElasticsearchIndex(es, "agg", "subagg", "20211201")

        # Test when no date given and no dates are available
        with self.assertRaises(APIError):
            UnittestElasticsearchIndex(es, "agg", "subagg", None)


class TestElasticsearchUtils(unittest.TestCase):
    def test_parse_args(self):
        """Test that parse_args returns values as expected when using different query parameters.

        :return: None.
        """

        app = create_app()
        with app.app.test_client() as test_client:
            # Test with no parameters set
            test_client.get()
            args = parse_args()
            expected_args = [], [], None, None, None, 10000, None, None, False
            self.assertEqual(expected_args, args)

            # Test with all parameters set
            parameters = {
                "agg_id": ["id1", "id2"],
                "subagg_id": "id3",
                "index_date": "2022-01-01",
                "from": 2000,
                "to": 2020,
                "limit": 100,
                "search_after": "1234",
                "pit": "dfg9asbc0",
                "pretty": True,
            }
            test_client.get(query_string=parameters)
            args = parse_args()
            expected_args = (
                ["id1", "id2"],
                ["id3"],
                "20220101",
                "2000-12-31",
                "2020-12-31",
                100,
                "1234",
                "dfg9asbc0",
                "True",
            )
            self.assertEqual(expected_args, args)

            # Test with limit set above max
            parameters["limit"] = 100000
            test_client.get(query_string=parameters)
            args = parse_args()
            expected_args = (
                ["id1", "id2"],
                ["id3"],
                "20220101",
                "2000-12-31",
                "2020-12-31",
                10000,
                "1234",
                "dfg9asbc0",
                "True",
            )
            self.assertEqual(expected_args, args)

    @patch("observatory.api.utils.elasticsearch_utils.Elasticsearch.ping")
    def test_create_es_connection(self, mock_elasticsearch_ping):
        """Test creating elasticsearch connection with (in)valid address and api_key

        :return:
        """
        with patch.dict(os.environ, {"ES_HOST": "address", "ES_API_KEY": "api_key"}):
            # Test elasticsearch instance is returned
            mock_elasticsearch_ping.return_value = True
            es = create_es_connection()
            self.assertIsInstance(es, Elasticsearch)

            # Test APIError error is raised when no ping is returned
            mock_elasticsearch_ping.return_value = False
            with self.assertRaises(APIError):
                create_es_connection()

        with patch.dict(os.environ, {"ES_HOST": "address", "ES_API_KEY": ""}):
            # Test APIError is raised when api_key is empty
            with self.assertRaises(APIError):
                create_es_connection()

        with patch.dict(os.environ, {"ES_HOST": "", "ES_API_KEY": "api_key"}):
            # Test APIError is raised when address is empty
            with self.assertRaises(APIError):
                create_es_connection()

    @patch("elasticsearch.client.CatClient.indices")
    def test_list_available_index_dates(self, mock_es_indices):
        """Test parsing of available dates from elasticsearch's cat.indices() response.

        :return: None.
        """
        dates = ["20201212", "20201201"]
        alias = "subset-agg"
        mock_es_indices.return_value = [{"index": f"{alias}-{dates[0]}"}, {"index": f"{alias}-{dates[1]}"}]

        available_dates = list_available_index_dates(Elasticsearch(), alias)
        self.assertEqual(dates, available_dates)

    def test_create_search_body(self):
        """Test that resulting search_body is as expected with different arguments.

        :return: None.
        """
        agg_field = "agg"
        subagg_field = None
        subagg_ids = None
        size = 500
        search_after = None
        pit_id = None
        from_date = None
        to_date = None

        expected = {"size": size, "query": {"bool": {"filter": []}}, "track_total_hits": True, "sort": "_id"}

        # Test aggregate & subaggregate filters
        agg_ids = ["id1", "id2"]
        search_body = create_search_body(
            agg_field, agg_ids, subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
        )
        expected["query"]["bool"]["filter"] = [{"terms": {"agg.keyword": agg_ids}}]
        self.assertDictEqual(expected, search_body)

        subagg_field = "subagg"
        subagg_ids = ["id1", "id2"]
        search_body = create_search_body(
            agg_field, agg_ids, subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
        )
        expected["query"]["bool"]["filter"] = [
            {"terms": {"agg.keyword": agg_ids}},
            {"terms": {"subagg.keyword": subagg_ids}},
        ]
        self.assertDictEqual(expected, search_body)

        agg_ids = None
        subagg_ids = None
        search_body = create_search_body(
            agg_field, agg_ids, subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
        )
        expected["query"]["bool"]["filter"] = []
        self.assertDictEqual(expected, search_body)

        # Test date filters
        from_date = "2000-12-31"
        to_date = "2010-12-31"
        search_body = create_search_body(
            agg_field, agg_ids, subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
        )
        expected["query"]["bool"]["filter"] = [
            {"range": {"published_year": {"format": "yyyy-MM-dd", "gte": from_date, "lte": to_date}}}
        ]
        self.assertDictEqual(expected, search_body)

        from_date = None
        to_date = "2010-12-31"
        search_body = create_search_body(
            agg_field, agg_ids, subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
        )
        expected["query"]["bool"]["filter"] = [{"range": {"published_year": {"format": "yyyy-MM-dd", "lte": to_date}}}]
        self.assertDictEqual(expected, search_body)

        from_date = "2000-12-31"
        to_date = None
        search_body = create_search_body(
            agg_field, agg_ids, subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
        )
        expected["query"]["bool"]["filter"] = [
            {"range": {"published_year": {"format": "yyyy-MM-dd", "gte": from_date}}}
        ]
        self.assertDictEqual(expected, search_body)

        from_date = None
        to_date = None
        search_body = create_search_body(
            agg_field, agg_ids, subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
        )
        expected["query"]["bool"]["filter"] = []
        self.assertDictEqual(expected, search_body)

        # Test search after option
        search_after = "1234"
        search_body = create_search_body(
            agg_field, agg_ids, subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
        )
        expected["search_after"] = [search_after]
        self.assertDictEqual(expected, search_body)

        # Test pit_id option
        pit_id = "4810vac9w0"
        search_body = create_search_body(
            agg_field, agg_ids, subagg_field, subagg_ids, from_date, to_date, size, search_after, pit_id
        )
        expected["pit"] = {"id": pit_id, "keep_alive": "1m"}
        expected["sort"] = "_shard_doc"
        self.assertDictEqual(expected, search_body)

    def test_process_response(self):
        """Test that the response from the elasticsearch search query is as expected.

        :return:
        """
        res_example = {
            "pit_id": "238cvjhs9",
            "took": "10",
            "hits": {
                "total": {"value": 452},
                "hits": [
                    {
                        "_index": "journals-institution-20201205",
                        "_type": "_doc",
                        "_id": "bQ88QXYBGinIh2YA4IaO",
                        "_score": None,
                        "_source": {"id": "example_id", "name": "Example Name", "published_year": "2018-12-31"},
                        "sort": [77250],
                    },
                    {
                        "_index": "journals-institution-20201205",
                        "_type": "_doc",
                        "_id": "bQ88QXYBGinIh2YA4Ia1",
                        "_score": None,
                        "_source": {"id": "example_id2", "name": "Example Name2", "published_year": "2018-12-31"},
                        "sort": [77251],
                    },
                ],
            },
        }

        new_pit_id, search_after_text, hits, took = process_response(res_example)

        self.assertEqual(res_example["pit_id"], new_pit_id)
        self.assertEqual(res_example["hits"]["hits"][-1]["sort"][0], search_after_text)
        self.assertEqual(res_example["hits"]["hits"], hits)
        self.assertEqual(res_example["took"], took)
