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

import os
import unittest

from observatory.platform.elastic.elastic import Elastic
from observatory.platform.elastic.kibana import Kibana, ObjectType, parse_kibana_url
from observatory.platform.utils.test_utils import random_id


class TestParseKibanaUrl(unittest.TestCase):
    def test_parse_kibana_url(self):
        """Parse Kibana URL"""

        url = "https://api_key_id:api_key@random-id.us-west1.gcp.cloud.es.io:9243"
        expected_host = "https://random-id.us-west1.gcp.cloud.es.io:9243"
        expected_key_id = "api_key_id"
        expected_key = "api_key"
        kibana_host, username, password = parse_kibana_url(url)
        self.assertEqual(expected_host, kibana_host)
        self.assertEqual(expected_key_id, username)
        self.assertEqual(expected_key, password)


class TestKibana(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestKibana, self).__init__(*args, **kwargs)
        self.elastic_host: str = os.getenv("TEST_ELASTIC_HOST")
        assert self.elastic_host is not None, "TestKibana: please set the TEST_ELASTIC_HOST environment variable."

        kibana_host: str = os.getenv("TEST_KIBANA_HOST")
        assert kibana_host is not None, "TestKibana: please set the TEST_KIBANA_HOST environment variable."
        self.kibana_host, self.key_id, self.api_key = parse_kibana_url(kibana_host)

    def test_create_delete_space(self):
        """Test the creation and deletion of spaces"""

        # Make clients
        kibana = Kibana(host=self.kibana_host, api_key_id=self.key_id, api_key=self.api_key)

        # Parameters
        space_id = random_id()

        try:
            # Create spaces
            result = kibana.create_space(space_id, "Test Space")
            self.assertTrue(result)

            # Delete space
            result = kibana.delete_space(space_id)
            self.assertFalse(result)
        finally:
            # Delete space
            kibana.delete_space(space_id)

    def test_create_delete_index_pattern(self):
        """Test the creation and deletion of index patterns"""

        # Make clients
        kibana = Kibana(host=self.kibana_host, api_key_id=self.key_id, api_key=self.api_key)
        elastic = Elastic(host=self.elastic_host)

        # Parameters
        index_id = random_id()
        space_id = random_id()
        object_id = index_id
        object_type = ObjectType.index_pattern
        attributes = {"title": object_id, "timeFieldName": "published_year"}

        try:
            # Create space
            result = kibana.create_space(space_id, "Test Space")
            self.assertTrue(result)

            # Create index
            body = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"published_year": {"type": "date", "format": "yyyy-MM-dd"}}},
            }
            elastic.es.indices.create(index=index_id, body=body, ignore=400)

            # Create index pattern
            result = kibana.create_object(object_type, object_id, attributes, space_id=space_id)
            self.assertTrue(result)

            # Delete index pattern
            result = kibana.delete_object(object_type, object_id, space_id=space_id)
            self.assertTrue(result)
        finally:
            # Delete index
            elastic.delete_index(index_id)

            # Delete index pattern
            kibana.delete_object(object_type, object_id, space_id=space_id, force=True)

            # Delete spaces
            kibana.delete_space(space_id)
