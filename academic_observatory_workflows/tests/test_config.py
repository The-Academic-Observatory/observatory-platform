# Copyright 2020-2021 Curtin University. All Rights Reserved.
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


import unittest
from observatory.platform.utils.config_utils import module_file_path
from academic_observatory_workflows.config import elastic_mappings_folder


class AcademicObservatoryWorkflowsConfig(unittest.TestCase):
    def test_elastic_schema_path(self):
        """ Test that the Elasticsearch schema path is correct """

        expected_path = module_file_path("academic_observatory_workflows.database.mappings")
        actual_path = elastic_mappings_folder()
        self.assertEqual(expected_path, actual_path)
