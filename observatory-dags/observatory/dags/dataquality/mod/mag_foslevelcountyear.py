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
import datetime
import pandas as pd

from typing import List
from jinja2 import Environment, PackageLoader

from observatory.dags.dataquality.autofetchcache import AutoFetchCache
from observatory.dags.dataquality.config import JinjaParams, MagCacheKey, MagTableKey
from observatory.dags.dataquality.es_mag import MagFosLevelCountYear
from observatory.dags.dataquality.analyser import MagAnalyserModule
from observatory.dags.dataquality.es_utils import (
    init_doc,
    search_count_by_release,
    clear_index,
    bulk_index,
    delete_index,
)


class FosLevelCountYearModule(MagAnalyserModule):
    """ MagAnalyser module to compute the number of fields of study labels assigned to papers per level, per year. """

    BQ_COUNT = 'count'  # SQL column for the count.
    YEAR_START = 2000  # Year to start fetching information from.

    def __init__(self, project_id: str, dataset_id: str, cache: AutoFetchCache):
        """ Initialise the module.
        @param project_id: Project ID in BigQuery.
        @param dataset_id: Dataset ID in BigQuery.
        @param cache: Analyser cache to use.
        """

        logging.info(f'{self.name()}: initialising.')
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._cache = cache

        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_foslevelcountyear = self._tpl_env.get_template('mag_fos_levelcountyear.sql.jinja2')

        init_doc(MagFosLevelCountYear)

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'{self.name()}: executing.')
        releases = self._cache[MagCacheKey.RELEASES]

        for release in releases:
            docs = self._construct_es_docs(release)

            logging.info(f'{self.name()}: indexing {len(docs)} docs of type MagFosLevelCountYear.')
            if len(docs) > 0:
                bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagFosLevelCountYear)

        if index:
            delete_index(MagFosLevelCountYear)

    def _construct_es_docs(self, release: datetime.date) -> List[MagFosLevelCountYear]:
        """
        Construct the elastic search documents for a given release.
        @param release: Release timestamp.
        @return List of MagFosLevelCountYear docs.
        """

        ts = release.strftime('%Y%m%d')
        logging.info(f'{self.name()}: processing release {ts}')

        docs = list()

        # If records exist in elastic search, skip.  This is not robust to partial records (past interrupted loads).
        if search_count_by_release(MagFosLevelCountYear, release.isoformat()) > 0:
            logging.info(f'{self.name()}: release {ts} already in elastic search. Skipping.')
            return docs

        counts = self._get_bq_counts(ts)

        # Construct elastic search documents.
        for i in range(len(counts)):
            year = counts[MagTableKey.COL_YEAR][i]
            if pd.isnull(year):
                year = 'null'
            else:
                year = str(year)

            level = counts[MagTableKey.COL_LEVEL][i]
            if pd.isnull(level):
                level = -1  # Don't want to break Kibana interface for now.  Consider changing to null str later.

            count = counts[FosLevelCountYearModule.BQ_COUNT][i]
            doc = MagFosLevelCountYear(release=release, year=str(year), count=count, level=level)
            docs.append(doc)

        logging.info(f'{self.name()}: release {ts} constructed {len(docs)} documents.')
        return docs

    def _get_bq_counts(self, ts: str) -> pd.DataFrame:
        """ Get the level counts per year for a given release.
        @param ts: Table suffix as a timestamp.
        @return Level counts per year.
        """

        sql = self._tpl_foslevelcountyear.render(
            project_id=self._project_id, dataset_id=self._dataset_id, ts=ts, year=FosLevelCountYearModule.YEAR_START,
            count=FosLevelCountYearModule.BQ_COUNT)

        return pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
