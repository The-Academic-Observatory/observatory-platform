# Copyright 2021 Curtin University. All Rights Reserved.
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

import os.path
import time
import unittest

from click.testing import CliRunner
from elasticsearch import Elasticsearch

from observatory.platform.elastic.elastic_environment import ElasticEnvironment


class TestElasticEnvironment(unittest.TestCase):
    def test_elastic_environment(self):
        """ Test that the elastic kibana environment starts and stops """

        with CliRunner().isolated_filesystem() as t:
            env = ElasticEnvironment(build_path=t)
            try:
                # Test start
                response = env.start()
                self.assertEqual(response.return_code, 0)

                # Check that files are copied
                self.assertTrue(os.path.isfile(os.path.join(t, ElasticEnvironment.ELASTICSEARCH_FILE_NAME)))
                self.assertTrue(os.path.isfile(os.path.join(t, ElasticEnvironment.COMPOSE_FILE_NAME)))

                # Test ping elastic
                es = Elasticsearch(["http://localhost:9200/"])
                start = time.time()
                while True:
                    elastic_found = es.ping()
                    if elastic_found:
                        break
                    elapsed = time.time() - start
                    if elapsed > 30:
                        break
                self.assertTrue(elastic_found)

                # Stop
                response = env.stop()
                self.assertEqual(response.return_code, 0)
            finally:
                env.stop()
