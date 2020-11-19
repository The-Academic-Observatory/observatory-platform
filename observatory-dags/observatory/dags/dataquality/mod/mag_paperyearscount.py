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


import logging
import pandas as pd
import datetime

from typing import Tuple, List
from observatory.dags.dataquality.analyser import MagAnalyserModule
from jinja2 import Environment, PackageLoader

from observatory.dags.dataquality.utils import proportion_delta
from observatory.dags.dataquality.config import JinjaParams, MagCacheKey, MagTableKey
from observatory.dags.dataquality.es_mag import MagPapersYearCount
from observatory.dags.dataquality.es_utils import (
    init_doc,
    search_count_by_release,
    clear_index,
    bulk_index,
    delete_index,
)


class PaperYearsCountModule(MagAnalyserModule):
    """ MagAnalyser module to compute paper counts by year from MAG. """

    def __init__(self, project_id: str, dataset_id: str, cache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param cache: Analyser cache to use.
        """

        logging.info(f'Initialising {self.name()}')
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache
        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_group_count = self._tpl_env.get_template('group_count.sql.jinja2')
        init_doc(MagPapersYearCount)

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]
        num_releases = len(releases)

        docs = list()
        for i in range(num_releases):
            if search_count_by_release(MagPapersYearCount, releases[i]) > 0:
                continue
            year, counts = self._get_paper_year_count(releases[i])

            # Proportional difference between years
            prev = [0] + counts[:-1]
            delta = proportion_delta(counts, prev)

            for j in range(len(year)):
                paper_count = MagPapersYearCount(release=releases[i].isoformat(), year=str(int(year[j])), count=counts[j],
                                                 delta_pcount=delta[j], delta_count=counts[j]-prev[j])
                docs.append(paper_count)

        logging.info(f'Constructed {len(docs)} MagPapersYearCount documents.')

        if len(docs) > 0:
            bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagPapersYearCount)

        if index:
            delete_index(MagPapersYearCount)

    def _get_paper_year_count(self, release: datetime.date) -> Tuple[List[int], List[int]]:
        """ Get paper counts by year.
        @param release: Relevant release to get data for.
        @return: Tuple of year and count information.
        """

        ts = release.strftime('%Y%m%d')
        table_id = f'{MagTableKey.TID_PAPERS}{ts}'
        sql = self._tpl_group_count.render(
            project_id=self._project_id, dataset_id=self._dataset_id, table_id=table_id,
            column='Year', where='Year IS NOT NULL'
        )
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df['Year'].to_list(), df['count'].to_list()
