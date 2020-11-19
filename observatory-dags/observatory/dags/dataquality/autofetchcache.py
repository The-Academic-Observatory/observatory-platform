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

from cachetools import LRUCache
from typing import Hashable, Any, Callable


class AutoFetchCache(LRUCache):
    '''
    LRU to store cached results. Adds ability to auto fetch certain data with user specified functions if it does not
    exist.
    '''

    LIMIT = 10000

    def __init__(self, maxsize: int = LIMIT):
        """ Add auto fetching functionality to LRUCache.
        @param maxsize: Size of the LRU.
        """

        super().__init__(maxsize)
        self._fetchers = {}

    def __getitem__(self, key: Hashable, **kwargs) -> Any:
        """ Overloads base class and adds auto fetch functionality.
        @param key: key to lookup.
        @return
        """
        if not self.__contains__(key) and key in self._fetchers:
            super().__setitem__(key, self._fetchers[key](key))
        return super().__getitem__(key, **kwargs)

    def set_fetcher(self, key: Hashable, fn: Callable[..., Any]):
        """
        Set an automatic fetcher that can fetch information if no value is found in the cache.
        @param key: Key for the fetcher.
        @param fn: Callable object that can fetch missing values and put it in the cache.
        """
        if not callable(fn):
            raise ValueError(f'{fn} is not callable.')
        self._fetchers[key] = fn
