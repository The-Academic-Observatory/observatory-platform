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


import datetime
import logging
import pandas as pd

from jinja2 import Environment, PackageLoader
from concurrent.futures import ThreadPoolExecutor, as_completed

from observatory.dags.dataquality.analyser import MagAnalyserModule
from observatory.dags.dataquality.config import JinjaParams, MagCacheKey, MagParams, MagTableKey

from observatory.dags.dataquality.es_utils import (
    init_doc,
    search_count_by_release,
    clear_index,
    bulk_index,
    delete_index,
)

from observatory.dags.dataquality.es_mag import MagDoiCountsDocType


class DoiCountDocTypeModule(MagAnalyserModule):
    """
    MagAnalyser module to compute Doi counts by DocType.
    """

    BQ_DOC_COUNT = 'doc_count'
    BQ_NULL_COUNT = 'null_count'

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
        init_doc(MagDoiCountsDocType)

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

        clear_index(MagDoiCountsDocType)

        if index:
            delete_index(MagDoiCountsDocType)

    def _construct_es_docs(self, release: datetime.date) -> MagDoiCountsDocType:
        """
        Construct the elastic search documents for a given release.
        @param release: Release timestamp.
        @return List of MagDoiCountsDocType docs.
        """

        ts = release.strftime('%Y%m%d')
        logging.info(f'DoiCountDocTypeModule processing release {ts}')

        docs = list()

        if search_count_by_release(MagDoiCountsDocType, release.isoformat()) > 0:
            return docs

        counts = self._get_bq_counts(ts)
        n_counts = len(counts[MagTableKey.COL_DOC_TYPE])
        for i in range(n_counts):
            count = counts[DoiCountDocTypeModule.BQ_DOC_COUNT][i]
            no_doi = counts[DoiCountDocTypeModule.BQ_NULL_COUNT][i]
            doc_type = counts[MagTableKey.COL_DOC_TYPE][i]
            pno_doi = 0
            if count != 0:
                pno_doi = no_doi / count

            if doc_type is None:
                doc_type = "null"

            doc = MagDoiCountsDocType(release=release.isoformat(), doc_type=doc_type, count=count, no_doi=no_doi,
                                      pno_doi=pno_doi)
            docs.append(doc)

        return docs

    def _get_bq_counts(self, ts: str) -> pd.DataFrame:
        """
        Get the Doi counts by DocType from the BigQuery table.
        @param ts: Timestamp to use as table suffix.
        """

        columns = [MagTableKey.COL_DOC_TYPE,
                   f'SUM(CASE WHEN {MagTableKey.COL_DOC_TYPE} IS NULL THEN 1 ELSE 1 END) AS {DoiCountDocTypeModule.BQ_DOC_COUNT}',
                   f'COUNTIF({MagTableKey.COL_DOI} IS NULL) AS {DoiCountDocTypeModule.BQ_NULL_COUNT}'
                   ]

        sql = self._tpl_select.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                      table_id=f'{MagTableKey.TID_PAPERS}{ts}', columns=columns,
                                      group_by=MagTableKey.COL_DOC_TYPE,
                                      order_by=MagTableKey.COL_DOC_TYPE)
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df
