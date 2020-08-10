# Copyright 2020 Curtin University
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

# Author: Richard Hosking

import unittest
import pendulum
from observatory_platform.workflows.doi import select_table_suffixes


class TestDoi(unittest.TestCase):
    """ Tests for the functions used by the Doi workflow """

    def test_select_table_suffixes(self):
        suffixes = select_table_suffixes('academic-observatory-dev', 'fundref', 'fundref',
                                         pendulum.date(year=2019, month=5, day=1))
        a = 1
