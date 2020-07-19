# Copyright 2019 Curtin University
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

from academic_observatory.utils.test_utils import test_data_dir


class TestTestUtils(unittest.TestCase):

    def test_test_data_dir(self):
        # Test that the correct directory is found
        path = test_data_dir(__file__)
        self.assertTrue(path.endswith(os.path.join('academic-observatory', 'tests', 'data')))
