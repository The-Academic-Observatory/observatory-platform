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

from academic_observatory.telescopes.oai_pmh import possible_oai_pmh_urls


class TestFetchEndpoints(unittest.TestCase):

    def test_potential_oai_pmh_urls(self):
        base_urls = ['http://dspace.nwu.ac.za/oai/request?verb=ListIdentifiers&metadataPrefix=oai_dc',
                     'https://era.ed.ac.uk/dspace-oai',
                     'http://earsiv.ebyu.edu.tr/oai/request']
        expected_results = [['http://dspace.nwu.ac.za/oai/request', 'http://dspace.nwu.ac.za/oai'],
                            ['https://era.ed.ac.uk/dspace-oai', 'https://era.ed.ac.uk'],
                            ['http://earsiv.ebyu.edu.tr/oai/request', 'http://earsiv.ebyu.edu.tr/oai']]
        results = [possible_oai_pmh_urls(url) for url in base_urls]
        self.assertListEqual(results, expected_results)
