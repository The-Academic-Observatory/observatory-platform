# Copyright 2021 Curtin University
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

# Author: Cameron Neylon

import unittest

from observatory.reports.es_tables import ESTable


class TestESTable(unittest.TestCase):

    def setUp(self) -> None:
        self.aggregations = [  # "author",
            "institution",
            "country",
            # "funder",
            "group",
            "publisher"]

        self.subsets = [  # "citations",
            "oa-metrics",
            "output-types",
            "publishers",
            "journals",
            "collaborations",
            "disciplines",
            "events",
            "funders"
        ]

        self.filters = [{'id': v} for v in [  # "http://orcid.org/0000-0001-5026-7510",
            "grid.1032.0",
            "AUS",
            # "Bill and Melinda Gates Foundation",
            "us_btaa",
            "Springer Science and Business Media LLC"]]

    def test_paginated_query(self):
        aggregation = 'institution'
        subset = 'journals'
        filter = dict(id='grid.4691.a,grid.469280.1')
        test = ESTable(aggregation=aggregation, subset=subset, filter=filter)

    def test_empty_response(self):
        aggregation = 'institution'
        subset = 'oa-metrics'
        filter = dict(id='grid.1032.0')
        year_range = (1850, 1860)
        test = ESTable(aggregation=aggregation, subset=subset, filter=filter, year_range=year_range)
        assert not test.df

    # def test_es_tables(self) -> None:
    #     year_range = (2018, 2020)
    #     for aggregation, filter in zip(self.aggregations, self.filters):
    #         for subset in self.subsets:
    #             test = ESTable(aggregation, subset, year_range=year_range, filter=filter)
