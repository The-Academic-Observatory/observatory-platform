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

from jinja2 import Environment, PackageLoader
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch_dsl import Document
from typing import List

from observatory.dags.dataquality.config import JinjaParams, MagCacheKey, MagParams, MagTableKey
from observatory.dags.dataquality.analyser import MagAnalyserModule
from observatory.dags.dataquality.es_mag import MagFosLevelCount
from observatory.dags.dataquality.es_utils import (
    init_doc,
    clear_index,
    bulk_index,
    delete_index,
    search_count_by_release,
)


class FosLevelCountModule(MagAnalyserModule):
    """ Generate documents to show the number of field of study labels at each level, and the number of documents
    at each level.
    """

    BQ_LEVEL_COUNT = 'lcount'
    BQ_PAP_COUNT = 'pcount'
    BQ_CIT_COUNT = 'ccount'

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
        init_doc(MagFosLevelCount)

        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_select = self._tpl_env.get_template('select_table.sql.jinja2')

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

        clear_index(MagFosLevelCount)

        if index:
            delete_index(MagFosLevelCount)

    def _construct_es_docs(self, release: datetime.date) -> List[Document]:
        """ Construct MagFosLevelCount documents from each release.

        @param release: Release date.
        @return: List of MagFosLevelCount documents created.
        """

        docs = list()
        ts = release.strftime('%Y%m%d')

        if search_count_by_release(MagFosLevelCount, release.isoformat()) > 0:
            return docs

        counts = self._get_bq_counts(ts)
        self._cache[f'{MagCacheKey.FOS_LEVELS}{ts}'] = counts[MagTableKey.COL_LEVEL].to_list()

        for i in range(len(counts)):
            doc = MagFosLevelCount(release=release, level=counts[MagTableKey.COL_LEVEL][i],
                                   level_count=counts[FosLevelCountModule.BQ_LEVEL_COUNT][i],
                                   num_papers=counts[FosLevelCountModule.BQ_PAP_COUNT][i],
                                   num_citations=counts[FosLevelCountModule.BQ_CIT_COUNT][i])
            docs.append(doc)
        return docs

    def _get_bq_counts(self, ts: str) -> pd.DataFrame:
        """ Make BigQuery call to fetch the counts.

        @param ts: Table suffix (timestamp).
        @return: DataFrame of counts.
        """

        columns = [MagTableKey.COL_LEVEL, f'COUNT({MagTableKey.COL_LEVEL}) as {FosLevelCountModule.BQ_LEVEL_COUNT}',
                   f'SUM({MagTableKey.COL_PAP_COUNT}) as {FosLevelCountModule.BQ_PAP_COUNT}',
                   f'SUM({MagTableKey.COL_CIT_COUNT}) as {FosLevelCountModule.BQ_CIT_COUNT}']

        sql = self._tpl_select.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                      table_id=f'{MagTableKey.TID_FOS}{ts}', columns=columns,
                                      group_by=MagTableKey.COL_LEVEL)
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df
