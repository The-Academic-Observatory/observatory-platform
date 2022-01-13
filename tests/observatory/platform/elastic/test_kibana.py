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

import os
import unittest

from observatory.platform.elastic.elastic import Elastic
from observatory.platform.elastic.elastic_environment import ElasticEnvironment
from observatory.platform.elastic.kibana import Kibana, ObjectType
from observatory.platform.utils.test_utils import random_id
from click.testing import CliRunner


class TestKibana(unittest.TestCase):
    es: ElasticEnvironment = None

    @classmethod
    def setUpClass(cls) -> None:
        with CliRunner().isolated_filesystem() as temp_dir:
            # Start an Elastic environment
            elastic_build_path = os.path.join(temp_dir, "elastic")
            cls.es = ElasticEnvironment(build_path=elastic_build_path)
            cls.es.start()

            cls.elastic = Elastic(host=cls.es.elastic_uri)
            cls.kibana = Kibana(host=cls.es.kibana_uri, username="elastic", password="observatory")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.es.stop()

    def __init__(self, *args, **kwargs):
        super(TestKibana, self).__init__(*args, **kwargs)

    def test_kibana(self):
        """Test initialisation of Kibana class with different arguments"""
        kibana = Kibana(host="host")
        self.assertEqual("host", kibana.host)
        self.assertDictEqual({"Content-Type": "application/json", "kbn-xsrf": "true"}, kibana.headers)

        kibana = Kibana(username="user", password="password")
        self.assertDictEqual(
            {"Content-Type": "application/json", "kbn-xsrf": "true", "Authorization": "Basic dXNlcjpwYXNzd29yZA=="},
            kibana.headers,
        )

        kibana = Kibana(api_key_id="id", api_key="key")
        self.assertDictEqual(
            {"Content-Type": "application/json", "kbn-xsrf": "true", "Authorization": "ApiKey aWQ6a2V5"}, kibana.headers
        )

    def test_create_delete_space(self):
        """Test the creation and deletion of spaces"""
        # Parameters
        space_id = random_id()

        try:
            # Create spaces
            result = self.kibana.create_space(space_id, "Test Space")
            self.assertTrue(result)

            # Delete space
            result = self.kibana.delete_space(space_id)
            self.assertFalse(result)
        finally:
            # Delete space
            self.kibana.delete_space(space_id)

    def test_create_delete_index_pattern(self):
        """Test the creation and deletion of index patterns"""
        # Parameters
        index_id = random_id()
        space_id = random_id()
        object_id = index_id
        object_type = ObjectType.index_pattern
        attributes = {"title": object_id, "timeFieldName": "published_year"}

        try:
            # Create space
            result = self.kibana.create_space(space_id, "Test Space")
            self.assertTrue(result)

            # Create index
            body = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"published_year": {"type": "date", "format": "yyyy-MM-dd"}}},
            }
            self.elastic.es.indices.create(index=index_id, body=body, ignore=400)

            # Create index pattern
            result = self.kibana.create_object(object_type, object_id, attributes, space_id=space_id)
            self.assertTrue(result)

            # Delete index pattern
            result = self.kibana.delete_object(object_type, object_id, space_id=space_id)
            self.assertTrue(result)
        finally:
            # Delete index
            self.elastic.delete_index(index_id)

            # Delete index pattern
            self.kibana.delete_object(object_type, object_id, space_id=space_id, force=True)

            # Delete spaces
            self.kibana.delete_space(space_id)
