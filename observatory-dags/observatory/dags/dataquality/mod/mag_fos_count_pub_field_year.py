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
from typing import List, Tuple

from observatory.dags.dataquality.config import JinjaParams, MagCacheKey, MagParams, MagTableKey
from observatory.dags.dataquality.analyser import MagAnalyserModule
from observatory.dags.dataquality.es_mag import MagFosCountPubFieldYear
from observatory.dags.dataquality.es_utils import (
    init_doc,
    clear_index,
    bulk_index,
    delete_index,
    search_count_by_release,
)


class FosCountsPubFieldYearModule(MagAnalyserModule):
    """ Compute paper, citation, reference counts per publisher, per field, per year.
        Limit to 1000 publishers per field per year.
    """

    BQ_LIMIT = 1000  # Limit on number of publishers we want to fetch.
    BQ_COUNTS = 'counts'  # SQL column for the counts.
    BQ_PAP_COUNT = 'pcount'  # SQL column for paper count.
    BQ_CIT_COUNT = 'ccount'  # SQL column for the citation count.
    BQ_REF_COUNT = 'rcount'  # SQL column for the reference count.
    YEAR_START = 2000  # Year to start fetching information from.

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
        init_doc(MagFosCountPubFieldYear)

        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_count = self._tpl_env.get_template('mag_fos_count_pub_field_year.sql.jinja2')

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        name = self.name()
        logging.info(f'Running {name}')
        releases = self._cache[MagCacheKey.RELEASES]

        # Maybe increase fd limit later and run in parallel.
        for release in releases:
            if search_count_by_release(MagFosCountPubFieldYear, release.isoformat()) > 0:
                continue

            docs = self._construct_es_docs(release)

            logging.info(f'{self.name()}: indexing {len(docs)} docs of type MagFosCountPubFieldYear.')
            if len(docs) > 0:
                bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagFosCountPubFieldYear)
        if index:
            delete_index(MagFosCountPubFieldYear)

    def _construct_es_docs(self, release: datetime.date) -> List[Document]:
        """ Construct MagDoiCountsDocTypeYear docs for each release.

        @param release: Release date.
        @return List of constructed elastic search documents.
        """

        ts = release.strftime('%Y%m%d')
        fosl0 = self._cache[f'{MagCacheKey.FOSL0}{ts}']
        fos_ids = tuple(fos[0] for fos in fosl0)
        fos_lookup = { fos[0]: fos[1] for fos in fosl0}

        docs = list()
        counts = self._get_bq_counts(ts, fos_ids, FosCountsPubFieldYearModule.YEAR_START)

        for i in range(len(counts)):
            year = counts[MagTableKey.COL_YEAR][i]
            if pd.isnull(year):
                year = 'null'
            else:
                year = str(year)

            fos_id = counts[MagTableKey.COL_FOS_ID][i]
            fos_name = fos_lookup[fos_id]

            pub_counts = counts[FosCountsPubFieldYearModule.BQ_COUNTS][i]
            for j in range(len(pub_counts)):
                publisher = pub_counts[j][MagTableKey.COL_PUBLISHER]
                pap_count = pub_counts[j][FosCountsPubFieldYearModule.BQ_PAP_COUNT]
                cit_count = pub_counts[j][FosCountsPubFieldYearModule.BQ_CIT_COUNT]
                ref_count = pub_counts[j][FosCountsPubFieldYearModule.BQ_REF_COUNT]

                if publisher is None:
                    publisher = 'null'

                doc = MagFosCountPubFieldYear(release=release, year=year, paper_count=pap_count,
                                              citation_count=cit_count, ref_count=ref_count,
                                              publisher=publisher, field_id=fos_id, field_name=fos_name)

                docs.append(doc)

        return docs

    def _get_bq_counts(self, ts: str, fos_ids: Tuple[int], year: int) -> pd.DataFrame:
        """ Get counts from BigQuery

        @param ts: Table suffix (timestamp).
        @param fos_ids: FieldOfStudyIds.
        @param year: Publication year we're interested in.
        @return DataFrame of counts.
        """

        sql = self._tpl_count.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                     ts=ts, year=year, fos_id=fos_ids, limit=FosCountsPubFieldYearModule.BQ_LIMIT,
                                     counts=FosCountsPubFieldYearModule.BQ_COUNTS,
                                     pap_count=FosCountsPubFieldYearModule.BQ_PAP_COUNT,
                                     cit_count=FosCountsPubFieldYearModule.BQ_CIT_COUNT,
                                     ref_count=FosCountsPubFieldYearModule.BQ_REF_COUNT
                                     )
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df
