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

import requests
import datetime
import pandas as pd
import yaml
import inspect
from typing import Tuple, Optional
from pathlib import Path

from observatory.reports import chart_utils
# from observatory.platform import elasticsearch
from .secrets import api_key

# api_definition_path = Path(inspect(elasticsearch.app.openapi2.yml))
# with open(api_definition_path) as f:
#     api_definition = yaml.load(f)
#     print(api_definition)


class ESTable:
    """
    Abstract Class for Tables Filled via an Elastic API Query
    """

    api_url = "https://api.observatory.academy/v1/query"

    def __init__(self,
                 aggregation: str,
                 subset: str,
                 filter: Optional[dict] = {},
                 year_range: Optional[Tuple[int, int]] = None,
                 collect_and_run: bool = True,
                 **kwargs):

        # TODO Pull these from the explicit instantiation in openapi2.yml
        # TODO Check the filter parameters against the API definition
        assert aggregation in ["author",
                               "country",
                               "region",
                               "subregion",
                               "funder",
                               "group",
                               "institution",
                               "publisher"]
        assert subset in ["citations",
                          "collaborations",
                          "disciplines",
                          "events",
                          "funders",
                          "journals",
                          "oa-metrics",
                          "output-types",
                          "publishers"]
        self.aggregation = aggregation
        self.subset = subset
        self.filter = filter
        if year_range:
            self.year_range = year_range
            self.collate_daterange()
        if collect_and_run:
            self.df = self.collect_data()
            if self.df is not None:
                self.clean_data()
            else:
                pass

    def collect_data(self):
        """Format the URL and Request Data From the API

        The code separates the process of requesting the JSON from generating the dataframe. The
        idea is that this gives a richer set of error messages from the the underlying libraries
        that should make it clearer what has gone wrong when it does.
        """

        params = {k: v for k,v in self.filter.items()}
        params.update(dict(
            agg=self.aggregation,
            subset=self.subset)
        )

        headers = {'X-API-Key': api_key}
        with requests.Session() as s:
            r = s.get(self.api_url, params=params, headers=headers)
            r.raise_for_status()
            j = r.json()
            results = j.get('results')
            scroll_id = j.get('scroll_id')
            total_hits = j.get('total_hits')
            while len(results) < total_hits:
                params.update(dict(scroll_id=scroll_id))
                r = s.get(self.api_url, params=params, headers=headers)
                r.raise_for_status()
                j = r.json()
                page = j.get('results')
                scroll_id = j.get('scroll_id')
                results.extend(page)
        if len(results) > 0:
            df = pd.DataFrame(results)
            df.drop(axis=1, columns=['_id', '_score', '_type'], inplace=True)
        else:
            df = None
        return df

    def collate_daterange(self):
        self.filter['from'] = self.year_range[0]
        self.filter['to'] = self.year_range[1]

    def clean_data(self):
        # TODO Figure out if this is a good way of handling years which are returned as date strings
        temp = pd.to_datetime(self.df.published_year)
        self.df.published_year = temp.dt.year
        chart_utils.clean_geo_names(self.df)
        chart_utils.nice_column_names(self.df)


