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

# Author: James Diprose, Aniek Roelofs

import json
import os
import unittest

import pendulum
from click.testing import CliRunner

from observatory.platform.elastic.elastic import (
    Elastic,
    KeepInfo,
    KeepOrder,
    make_elastic_uri,
    make_sharded_index,
)
from observatory.platform.elastic.elastic_environment import ElasticEnvironment
from observatory.platform.files import load_file, yield_csv
from observatory.platform.observatory_environment import find_free_port
from observatory.platform.observatory_environment import random_id, test_fixtures_path


class TestElastic(unittest.TestCase):
    es: ElasticEnvironment = None

    @classmethod
    def setUpClass(cls) -> None:
        with CliRunner().isolated_filesystem() as temp_dir:
            # Start an Elastic environment
            elastic_build_path = os.path.join(temp_dir, "elastic")
            elastic_port = find_free_port()
            kibana_port = find_free_port()
            cls.es = ElasticEnvironment(
                build_path=elastic_build_path, elastic_port=elastic_port, kibana_port=kibana_port
            )
            cls.es.start()

            # Create elasticsearch client
            cls.client = Elastic(host=cls.es.elastic_uri)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.es.stop()

    def __init__(self, *args, **kwargs):
        super(TestElastic, self).__init__(*args, **kwargs)

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

    def test_make_sharded_index(self):
        """Test making an Elasticsearch sharded index name"""

        index_prefix = "oa-metrics-country"
        snapshot_date = pendulum.date(year=2020, month=1, day=1)
        expected_index = "oa-metrics-country-20200101"
        actual_index = make_sharded_index(index_prefix, snapshot_date)
        self.assertEqual(expected_index, actual_index)

    def test_make_elastic_uri(self):
        """Test building an Elasticsearch URI"""

        schema = "https"
        user = "user"
        secret = "secret"
        hostname = "localhost"
        port = 8080
        expected_uri = "https://user:secret@localhost:8080"
        actual_uri = make_elastic_uri(schema, user, secret, hostname, port)
        self.assertEqual(expected_uri, actual_uri)

    ################
    # Elastic tests
    ################

    def test_index_delete_documents(self):
        index_id = random_id()

        def index_exists():
            return self.client.es.indices.exists(index=index_id)

        def doc_count():
            return self.client.es.count(index=index_id, body={"query": {"match_all": {}}})["count"]

        try:
            # Index
            mapping_dict = json.loads(load_file(self.mappings_file_path))
            success = self.client.index_documents(index_id, mapping_dict, yield_csv(self.csv_file_path))
            self.assertTrue(success)
            self.assertTrue(index_exists())
            self.assertEqual(4, doc_count())

            # Delete
            self.client.delete_index(index_id)
            self.assertFalse(index_exists())
        finally:
            # Cleanup
            self.client.delete_index(index_id)

    def test_get_alias_indexes(self):
        """Test that a list of indexes associated with an alias are returned"""

        # Make identifiers
        index_id = random_id()
        alias_id = random_id()

        try:
            # Make index
            self.client.es.indices.create(index=index_id)

            # Make aliases
            self.client.es.indices.update_aliases(actions=[{"add": {"index": index_id, "alias": alias_id}}])

            index_ids = self.client.get_alias_indexes(alias_id)
            self.assertListEqual([index_id], index_ids)
        finally:
            # Delete index and aliases
            self.client.es.indices.update_aliases(actions=[{"remove": {"index": index_id, "alias": alias_id}}])
            self.client.delete_index(index_id)

    def test_index_create_list_delete(self):
        index = random_id()
        index2 = random_id()

        self.client.create_index(index)
        self.client.create_index(index2)
        indices = self.client.list_indices(index)
        self.assertEqual(len(indices), 1)

        indices = self.client.list_indices(index2)
        self.assertEqual(len(indices), 1)

        self.client.delete_indices([index, index2])

        indices = self.client.list_indices(index)
        self.assertEqual(len(indices), 0)

        indices = self.client.list_indices(index2)
        self.assertEqual(len(indices), 0)

    def test_delete_stale_indices_newest(self):
        prefix = random_id()

        index1 = f"{prefix}-first-notendinginyyyymmdd"
        index2 = f"{prefix}-first-20210101"
        index3 = f"{prefix}-first-20210102"
        index4 = f"{prefix}-first-20210103"

        index5 = f"{prefix}-second-20210101"
        index6 = f"{prefix}-second-20210102"
        index7 = f"{prefix}-second-20210103"

        indices = [index1, index2, index3, index4, index5, index6, index7]
        for idx in indices:
            self.client.create_index(idx)

        # Test stale deletion
        self.client.delete_stale_indices(index=f"{prefix}*", keep_info=KeepInfo(ordering=KeepOrder.newest, num=2))
        val_indices = self.client.list_indices(index1)
        self.assertEqual(len(val_indices), 1)

        val_indices = self.client.list_indices(index2)
        self.assertEqual(len(val_indices), 0)

        val_indices = self.client.list_indices(index3)
        self.assertEqual(len(val_indices), 1)

        val_indices = self.client.list_indices(index4)
        self.assertEqual(len(val_indices), 1)

        val_indices = self.client.list_indices(index5)
        self.assertEqual(len(val_indices), 0)

        val_indices = self.client.list_indices(index6)
        self.assertEqual(len(val_indices), 1)

        val_indices = self.client.list_indices(index7)
        self.assertEqual(len(val_indices), 1)

        self.client.delete_indices(indices)

    def test_delete_stale_indices_oldest(self):
        prefix = random_id()

        index1 = f"{prefix}-first-notendinginyyyymmdd"
        index2 = f"{prefix}-first-20210101"
        index3 = f"{prefix}-first-20210102"
        index4 = f"{prefix}-first-20210103"

        index5 = f"{prefix}-second-20210101"
        index6 = f"{prefix}-second-20210102"
        index7 = f"{prefix}-second-20210103"

        indices = [index1, index2, index3, index4, index5, index6, index7]
        for idx in indices:
            self.client.create_index(idx)

        # Test stale deletion
        self.client.delete_stale_indices(index=f"{prefix}*", keep_info=KeepInfo(ordering=KeepOrder.oldest, num=2))
        val_indices = self.client.list_indices(index1)
        self.assertEqual(len(val_indices), 1)

        val_indices = self.client.list_indices(index2)
        self.assertEqual(len(val_indices), 1)

        val_indices = self.client.list_indices(index3)
        self.assertEqual(len(val_indices), 1)

        val_indices = self.client.list_indices(index4)
        self.assertEqual(len(val_indices), 0)

        val_indices = self.client.list_indices(index5)
        self.assertEqual(len(val_indices), 1)

        val_indices = self.client.list_indices(index6)
        self.assertEqual(len(val_indices), 1)

        val_indices = self.client.list_indices(index7)
        self.assertEqual(len(val_indices), 0)

        self.client.delete_indices(indices)
