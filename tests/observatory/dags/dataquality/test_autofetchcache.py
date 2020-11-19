#!/usr/bin/python3

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

# Author: Tuan Chien

import unittest
from observatory_platform.utils.autofetchcache import AutoFetchCache

class TestAutoFetchCache(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_cache(self):
        cache = AutoFetchCache(2)

        def fn(key):
            return f'{key} response'

        cache[1] = 1
        cache[2] = 2
        cache[3] = 3

        self.assertEqual(len(cache), 2)
        cache.set_fetcher('test', fn)
        self.assertTrue('test' not in cache)
        self.assertEqual(cache['test'], 'test response')
        cache['test'] = 'testing'
        self.assertEqual(cache['test'], 'testing')