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

    BQ_LIMIT = 1000
    BQ_PAP_COUNT = 'pcount'
    BQ_CIT_COUNT = 'ccount'
    BQ_REF_COUNT = 'rcount'
    YEAR_START = 1800

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
        year_end = datetime.datetime.now(datetime.timezone.utc).year

        with ThreadPoolExecutor(max_workers=MagParams.BQ_SESSION_LIMIT) as executor:
            for release in releases:
                ts = release.strftime('%Y%m%d')
                fos_ids = self._cache[f'{MagCacheKey.FOSL0}{ts}']

                if search_count_by_release(MagFosCountPubFieldYear, release.isoformat()) > 0:
                    continue

                logging.info(f'{name}: processing {ts}')
                for fos in fos_ids:
                    docs = list()
                    futures = list()

                    for year in range(FosCountsPubFieldYearModule.YEAR_START, year_end + 1):
                        futures.append(executor.submit(self._construct_es_docs, release, ts, fos, year))

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

        clear_index(MagFosCountPubFieldYear)
        if index:
            delete_index(MagFosCountPubFieldYear)

    def _construct_es_docs(self, release: datetime.date, ts: str, fos: Tuple[int, str], year: int) -> List[Document]:
        """ Construct MagDoiCountsDocTypeYear docs for each release.

        @param release: Release date.
        @param ts: Table suffix (timestamp).
        @param fos: FieldOfStudyId, Normalised Name.
        @param year: Publication year we're interested in.
        @return List of constructed elastic search documents.
        """

        docs = list()
        year = str(year)
        counts = self._get_bq_counts(ts, fos[0], year)

        for i in range(len(counts)):
            paper_count = counts[FosCountsPubFieldYearModule.BQ_PAP_COUNT][i]
            if paper_count == 0:
                return docs

            publisher = counts[MagTableKey.COL_PUBLISHER][i]
            citation_count = counts[FosCountsPubFieldYearModule.BQ_CIT_COUNT][i]
            ref_count = counts[FosCountsPubFieldYearModule.BQ_REF_COUNT][i]

            if publisher is None:
                publisher = 'null'

            doc = MagFosCountPubFieldYear(release=release, year=year, paper_count=paper_count,
                                          citation_count=citation_count, ref_count=ref_count,
                                          publisher=publisher, field_id=fos[0], field_name=fos[1])

            docs.append(doc)

        return docs

    def _get_bq_counts(self, ts: str, fos_id: int, year: int) -> pd.DataFrame:
        """ Get counts from BigQuery

        @param ts: Table suffix (timestamp).
        @param fos_id: FieldOfStudyId.
        @param year: Publication year we're interested in.
        @return DataFrame of counts.
        """

        sql = self._tpl_count.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                     ts=ts, year=year, fos_id=fos_id, limit=FosCountsPubFieldYearModule.BQ_LIMIT,
                                     pap_count=FosCountsPubFieldYearModule.BQ_PAP_COUNT,
                                     cit_count=FosCountsPubFieldYearModule.BQ_CIT_COUNT,
                                     ref_count=FosCountsPubFieldYearModule.BQ_REF_COUNT
                                     )
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df
