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

from observatory_platform.dataquality.config import JinjaParams, MagCacheKey, MagParams, MagTableKey
from observatory_platform.dataquality.analyser import MagAnalyserModule
from observatory_platform.dataquality.es_mag import MagDoiCountsDocTypeYear
from observatory_platform.utils.es_utils import (
    init_doc,
    clear_index,
    bulk_index,
    delete_index,
    search_count_by_release,
)


class DoiCountsDocTypeYearModule(MagAnalyserModule):
    """ Compute the doi counts per doctype per year."""

    BQ_DOC_COUNT = 'count'
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
        init_doc(MagDoiCountsDocTypeYear)

        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_count = self._tpl_env.get_template('doi_count_doctype_year.sql.jinja2')

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

        clear_index(MagDoiCountsDocTypeYear)

        if index:
            delete_index(MagDoiCountsDocTypeYear)

    def _construct_es_docs(self, release: datetime.date) -> List[Document]:
        """ Construct MagDoiCountsDocTypeYear docs for each release.

        @param release: Table suffix (timestamp).
        @return List of constructed elastic search documents.
        """

        ts = release.strftime('%Y%m%d')
        doc_types = self._cache[f'{MagCacheKey.DOC_TYPE}{ts}']

        docs = list()
        if search_count_by_release(MagDoiCountsDocTypeYear, release.isoformat()) > 0:
            return docs

        for doc_type in doc_types:
            counts = self._get_bq_counts(ts, doc_type)

            for i in range(len(counts)):
                year = counts[MagTableKey.COL_YEAR][i]

                if pd.isnull(year):
                    year = 'null'
                else:
                    year = int(year)

                count = counts[DoiCountsDocTypeYearModule.BQ_DOC_COUNT][i]
                no_doi = counts[DoiCountsDocTypeYearModule.BQ_NULL_COUNT][i]

                pno_doi = 0
                if count != 0:
                    pno_doi = no_doi / count

                doc = MagDoiCountsDocTypeYear(release=release, doc_type=doc_type, year=str(year),
                                              count=count, no_doi=no_doi, pno_doi=pno_doi)
                docs.append(doc)

        return docs

    def _get_bq_counts(self, ts: str, doc_type: str) -> pd.DataFrame:
        """ Get counts from BigQuery

        @param ts: Table suffix (timestamp).
        @param doc_type: DocType of interest.
        @return DataFrame of counts.
        """

        sql = self._tpl_count.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                     ts=ts, doc_type=doc_type, doc_count=DoiCountsDocTypeYearModule.BQ_DOC_COUNT,
                                     null_count=DoiCountsDocTypeYearModule.BQ_NULL_COUNT
                                     )
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df

