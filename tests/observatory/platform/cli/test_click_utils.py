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

# Author: James Diprose

import unittest

from observatory.platform.cli.click_utils import indent, INDENT1, INDENT2, INDENT3, INDENT4


class TestClick(unittest.TestCase):
    def test_indent(self):
        original_str = "hello world"

        # 2 spaces
        output = indent(original_str, INDENT1)
        self.assertEqual(f"  {original_str}", output)

        # 3 spaces
        output = indent(original_str, INDENT2)
        self.assertEqual(f"   {original_str}", output)

        # 4 spaces
        output = indent(original_str, INDENT3)
        self.assertEqual(f"    {original_str}", output)

        # 5 spaces
        output = indent(original_str, INDENT4)
        self.assertEqual(f"     {original_str}", output)

        # Check that values below 0 raise assertion error
        with self.assertRaises(AssertionError):
            indent(original_str, 0)

        with self.assertRaises(AssertionError):
            indent(original_str, -1)
