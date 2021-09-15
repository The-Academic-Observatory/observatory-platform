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
import unittest

from click.testing import CliRunner

from observatory.platform.elastic.elastic_environment import ElasticEnvironment


class TestElasticEnvironment(unittest.TestCase):
    def setUp(self) -> None:
        self.elastic_port = 9201
        self.kibana_port = 5602
        self.wait_time_secs = 120

    def test_elastic_environment(self):
        """Test that the elastic kibana environment starts and stops"""

        with CliRunner().isolated_filesystem() as t:
            build_path = os.path.join(t, "docker")
            env = ElasticEnvironment(
                build_path=build_path,
                elastic_port=self.elastic_port,
                kibana_port=self.kibana_port,
                wait=False,
                wait_time_secs=self.wait_time_secs,
            )
            try:
                # Test start
                response = env.start()
                self.assertEqual(response.return_code, 0)

                # Check that files are copied
                self.assertTrue(os.path.isfile(os.path.join(build_path, "elasticsearch.yml")))
                self.assertTrue(os.path.isfile(os.path.join(build_path, "docker-compose.yml")))

                # Wait until found
                services_found = env.wait_until_started()
                self.assertTrue(services_found)

                # Stop
                response = env.stop()
                self.assertEqual(response.return_code, 0)
            finally:
                env.stop()
