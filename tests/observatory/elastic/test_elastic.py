# Copyright 2020, 2021 Curtin University. All Rights Reserved.
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

import json
import os
import unittest

import pendulum

from observatory.platform.elastic.elastic import (
    Elastic,
    make_sharded_index,
    elastic_mappings_path,
    make_elastic_uri,
    make_index_prefix,
)
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.file_utils import load_csv_gz, load_file
from observatory.platform.utils.test_utils import random_id, test_fixtures_path


class TestElastic(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestElastic, self).__init__(*args, **kwargs)
        self.host: str = os.getenv("TEST_ELASTIC_HOST")
        assert self.host is not None, "TestElastic: please set the TEST_ELASTIC_HOST environment variable."

        fixtures_path = test_fixtures_path("elastic")
        self.csv_file_path = os.path.join(fixtures_path, "load_csv_gz.csv.gz")
        self.jsonl_file_path = os.path.join(fixtures_path, "load_json_gz.jsonl.gz")
        self.mappings_file_path = os.path.join(fixtures_path, "the-expanse-mappings.json")
        self.expected_records = [
            {"first_name": "Jim", "last_name": "Holden"},
            {"first_name": "Alex", "last_name": "Kamal"},
            {"first_name": "Naomi", "last_name": "Nagata"},
            {"first_name": "Amos", "last_name": "Burton"},
        ]

    def test_elastic_schema_path(self):
        """ Test that the Elasticsearch schema path is correct """

        file_name = "schema.json"
        expected_path = os.path.join(module_file_path("academic_observatory.dags.database.schema"), file_name)
        actual_path = elastic_mappings_path(file_name)
        self.assertEqual(expected_path, actual_path)

    def test_make_sharded_index(self):
        """ Test making an Elasticsearch sharded index name """

        index_prefix = "oa-metrics-country"
        release_date = pendulum.date(year=2020, month=1, day=1)
        expected_index = "oa-metrics-country-20200101"
        actual_index = make_sharded_index(index_prefix, release_date)
        self.assertEqual(expected_index, actual_index)

    def test_make_elastic_uri(self):
        """ Test building an Elasticsearch URI """

        schema = "https"
        user = "user"
        secret = "secret"
        hostname = "localhost"
        port = 8080
        expected_uri = "https://user:secret@localhost:8080"
        actual_uri = make_elastic_uri(schema, user, secret, hostname, port)
        self.assertEqual(expected_uri, actual_uri)

    def test_make_index_prefix(self):
        """ Test building an Elasticsearch index prefix """

        feed_name = "oa-metrics"
        table_name = "country"
        expected_prefix = "oa-metrics-country"
        actual_prefix = make_index_prefix(feed_name, table_name)
        self.assertEqual(expected_prefix, actual_prefix)

    ################
    # Elastic tests
    ################

    def test_index_delete_documents(self):
        # Make client and identifiers
        client = Elastic(host=self.host)
        index_id = random_id()

        def index_exists():
            return client.es.indices.exists(index_id)

        def doc_count():
            return client.es.count(index=index_id, body={"query": {"match_all": {}}})["count"]

        try:
            # Index
            mapping_dict = json.loads(load_file(self.mappings_file_path))
            success = client.index_documents(index_id, mapping_dict, load_csv_gz(self.csv_file_path, yield_items=True))
            self.assertTrue(success)
            self.assertTrue(index_exists())
            self.assertEqual(4, doc_count())

            # Delete
            client.delete_index(index_id)
            self.assertFalse(index_exists())
        finally:
            # Cleanup
            client.delete_index(index_id)

    def test_get_alias_indexes(self):
        """ Test that a list of indexes associated with an alias are returned """

        # Make client and identifiers
        client = Elastic(host=self.host)
        index_id = random_id()
        alias_id = random_id()

        try:
            # Make index
            client.es.indices.create(index=index_id)

            # Make aliases
            client.es.indices.update_aliases({"actions": [{"add": {"index": index_id, "alias": alias_id}}]})

            index_ids = client.get_alias_indexes(alias_id)
            self.assertListEqual([index_id], index_ids)
        finally:
            # Delete index and aliases
            client.es.indices.update_aliases({"actions": [{"remove": {"index": index_id, "alias": alias_id}}]})
            client.delete_index(index_id)
