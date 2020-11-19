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

from concurrent.futures import ThreadPoolExecutor, as_completed
from jinja2 import Environment, PackageLoader
from observatory_platform.dataquality.config import JinjaParams, MagCacheKey, MagParams, MagTableKey
from observatory_platform.dataquality.es_mag import MagFosLevelCountYear
from observatory_platform.dataquality.analyser import MagAnalyserModule
from observatory_platform.utils.es_utils import (
    init_doc,
    search_count_by_release,
    clear_index,
    bulk_index,
    delete_index,
)


class FosLevelCountYearModule(MagAnalyserModule):
    """ MagAnalyser module to compute the number of fields of study labels assigned to papers per level, per year. """

    BQ_COUNT = 'count'

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
        self._tpl_foslevelcountyear = self._tpl_env.get_template('mag_fos_levelcountyear.sql.jinja2')

        init_doc(MagFosLevelCountYear)


    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]

        docs = list()
        with ThreadPoolExecutor(max_workers=MagParams.BQ_SESSION_LIMIT) as executor:
            futures = list()
            for release in releases:
                futures.append(executor.submit(self._construct_es_docs, release))

            for future in as_completed(futures):
                docs.extend(future.result())

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

    def _construct_es_docs(self, release: datetime.date) -> MagFosLevelCountYear:
        """
        Construct the elastic search documents for a given release.
        @param release: Release timestamp.
        @return List of MagFosLevelCountYear docs.
        """

        ts = release.strftime('%Y%m%d')
        logging.info(f'MagFosLevelCountYear processing release {ts}')

        docs = list()

        if search_count_by_release(MagFosLevelCountYear, release.isoformat()) > 0:
            return docs

        levels = self._cache[f'{MagCacheKey.FOS_LEVELS}{ts}']

        for level in levels:
            counts = self._get_bq_counts(ts, level)
            for i in range(len(counts)):
                year = counts[MagTableKey.COL_YEAR][i]
                count = counts[FosLevelCountYearModule.BQ_COUNT][i]
                level = level
                doc = MagFosLevelCountYear(release=release, year=str(year), count=count, level=level)
                docs.append(doc)

        return docs

    def _get_bq_counts(self, ts: str, level: int) -> pd.DataFrame:
        """ Get the level counts per year for a given release.
        @param ts: Table suffix as a timestamp.
        @param level: Field of study level to get counts for.
        @return Level counts per year.
        """

        table_id = f'{MagTableKey.TID_PAPERS}{ts}'
        sql = self._tpl_foslevelcountyear.render(
            project_id=self._project_id, dataset_id=self._dataset_id, ts=ts, level=level, count=FosLevelCountYearModule.BQ_COUNT)
        return pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
