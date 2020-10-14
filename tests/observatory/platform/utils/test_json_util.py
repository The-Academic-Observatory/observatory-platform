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

import unittest

from observatory.platform.utils.json_util import to_json_lines


class TestJsonUtil(unittest.TestCase):

    def test_to_json_lines(self):
        hello = dict()
        hello['hello'] = 'world'
        hello_list = [hello] * 3

        # Multiple items
        expected = '{"hello": "world"}\n{"hello": "world"}\n{"hello": "world"}\n'
        actual = to_json_lines(hello_list)
        self.assertEqual(expected, actual)

        # One item
        expected = '{"hello": "world"}\n'
        actual = to_json_lines([hello])
        self.assertEqual(expected, actual)

        # No items
        expected = ''
        actual = to_json_lines([])
        self.assertEqual(expected, actual)
