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

from observatory.api.server.api import create_app
from observatory.api.utils.elasticsearch_utils import (
    create_es_connection,
    create_search_body,
    list_available_index_dates,
    parse_args,
    process_response,
)

AGGREGATIONS = ["author", "country", "funder", "group", "institution", "publisher"]
SUBSETS = [
    "citations",
    "collaborations",
    "disciplines",
    "events",
    "funders",
    "journals",
    "oa-metrics",
    "output-types",
    "publishers",
]
SCROLL_ID = "FGluY2x1ZGVfY29udGV4dF91dWlkDXF1ZXJ5QW5kRmV0Y2gBFlVrbkJuSmNzVFMtSUh3MkcxVmxXRVEAAAAAAJDtdhZkejJRLUUwZ1N0T05nUTV6ZUlVNGRn "
RES_EXAMPLE = {
    "_scroll_id": SCROLL_ID,
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


class TestElastic(unittest.TestCase):
    """Tests for the 'query' endpoint of the API."""

    @patch("observatory.api.server.elastic.Elasticsearch.ping")
    def test_create_es_connection(self, mock_elasticsearch_ping):
        """Test creating elasticsearch connection with (in)valid address and api_key

        :return:
        """
        # test elasticsearch instance is returned
        mock_elasticsearch_ping.return_value = True
        es = create_es_connection("address", "api_key")
        self.assertIsInstance(es, Elasticsearch)

        # test connection error is raised
        mock_elasticsearch_ping.return_value = False
        with self.assertRaises(ConnectionError):
            create_es_connection("address", "api_key")

        # test None is returned when address or api_key is empty
        es = create_es_connection("address", "")
        self.assertIsNone(es)

        es = create_es_connection(None, "api_key")
        self.assertIsNone(es)

    def test_parse_args(self):
        """Test that parse_args returns values as expected when using different query parameters.

        :return: None.
        """

        app = create_app()
        with app.app.test_client() as test_client:
            # test minimal parameters
            parameters = {"agg": "author", "subset": "citations"}
            test_client.get("/query", query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertIsNone(from_date)
            self.assertIsNone(to_date)
            for field in filter_fields:
                self.assertIsNone(filter_fields[field])
            self.assertEqual(10000, size)
            self.assertIsNone(scroll_id)

            # test filter parameters
            parameters = {}
            for field in QUERY_FILTER_PARAMETERS:
                parameters[field] = "test1,test2"
            test_client.get("/query", query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            for field in filter_fields:
                self.assertEqual(["test1", "test2"], filter_fields[field])

            # test from & to dates
            parameters = {"from": 2000, "to": 2010}
            test_client.get("/query", query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual("2000-12-31", from_date)
            self.assertEqual("2010-12-31", to_date)

            # test aliases
            parameters = {}
            for agg in AGGREGATIONS:
                for subset in SUBSETS:
                    parameters["agg"] = agg
                    parameters["subset"] = subset
                    test_client.get("/query", query_string=parameters)
                    alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()

                    if agg == "publisher" and subset == "collaborations":
                        self.assertEqual("", alias)
                    else:
                        if agg == "author" or agg == "funder":
                            self.assertEqual(f"{subset}-{agg}_test", alias)
                        else:
                            self.assertEqual(f"{subset}-{agg}", alias)

            # test index date
            parameters = {"index_date": "2020-01-01"}
            test_client.get("/query", query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual("20200101", index_date)

            test_client.get("/query")
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(None, index_date)

            # test limit
            parameters = {"limit": 500}
            test_client.get("/query", query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(500, size)

            test_client.get("/query")
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(10000, size)

            # test scroll_id
            parameters = {"scroll_id": SCROLL_ID}
            test_client.get("/query", query_string=parameters)
            alias, index_date, from_date, to_date, filter_fields, size, scroll_id = parse_args()
            self.assertEqual(SCROLL_ID, scroll_id)

    def test_create_search_body(self):
        """Test that resulting search_body is as expected with different arguments.

        :return: None.
        """
        # test dates
        from_year = "2000-12-31"
        to_year = "2010-12-31"
        filter_fields = dict.fromkeys(QUERY_FILTER_PARAMETERS)
        size = 500
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        expected_range_dict = {"range": {"published_year": {"format": "yyyy-MM-dd", "gte": from_year, "lt": to_year}}}
        self.assertIn(expected_range_dict, search_body["query"]["bool"]["filter"])

        from_year = None
        to_year = "2010-12-31"
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        expected_range_dict = {"range": {"published_year": {"format": "yyyy-MM-dd", "lt": to_year}}}
        self.assertIn(expected_range_dict, search_body["query"]["bool"]["filter"])

        from_year = "2000-12-31"
        to_year = None
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        expected_range_dict = {"range": {"published_year": {"format": "yyyy-MM-dd", "gte": from_year}}}
        self.assertIn(expected_range_dict, search_body["query"]["bool"]["filter"])

        # test empty dates and empty filter fields
        from_year = None
        to_year = None
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        self.assertEqual([], search_body["query"]["bool"]["filter"])

        # test filter fields
        from_year = "2000-12-31"
        to_year = "2010-12-31"
        filter_fields = {}
        for field in QUERY_FILTER_PARAMETERS:
            filter_fields[field] = ["test1", "test2"]
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        for field in filter_fields:
            list_entry = {"terms": {f"{field}.keyword": filter_fields[field]}}
            self.assertIn(list_entry, search_body["query"]["bool"]["filter"])

        # test size
        size = 500
        search_body = create_search_body(from_year, to_year, filter_fields, size)
        self.assertEqual(500, search_body["size"])

    def test_process_response(self):
        """Test that the response from the elasticsearch search/scroll query is as expected.

        :return:
        """
        scroll_id, hits = process_response(copy.deepcopy(RES_EXAMPLE))
        self.assertEqual(SCROLL_ID, scroll_id)
        expected_hits = [
            {
                "_index": "journals-institution-20201205",
                "_type": "_doc",
                "_id": "bQ88QXYBGinIh2YA4IaO",
                "_score": None,
                "sort": [77250],
                "id": "example_id",
                "name": "Example Name",
                "published_year": "2018-12-31",
            },
            {
                "_index": "journals-institution-20201205",
                "_type": "_doc",
                "_id": "bQ88QXYBGinIh2YA4Ia1",
                "_score": None,
                "sort": [77251],
                "id": "example_id2",
                "name": "Example Name2",
                "published_year": "2018-12-31",
            },
        ]
        self.assertEqual(expected_hits, hits)

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
