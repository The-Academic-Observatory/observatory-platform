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
from observatory.dags.dataquality.es_mag import MagFosL0ScoreFieldYear
from observatory.dags.dataquality.es_utils import (
    init_doc,
    clear_index,
    bulk_index,
    delete_index,
    search_count_by_release,
)


class FosL0ScoreFieldYearModule(MagAnalyserModule):
    """ Gets the distribution of saliency scores for field of study labels per field .
        Generates MagFosL0ScoreFieldYear elastic search documents.
    """

    YEAR_START = 1800
    BUCKET_START = 0.0
    BUCKET_END = 1.0
    BUCKET_STEP = 0.01

    BQ_COUNT = 'count'
    BQ_BUCKET = 'bucket'

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
        init_doc(MagFosL0ScoreFieldYear)

        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_histogram = self._tpl_env.get_template('histogram_fosl0.sql.jinja2')

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]
        year_end = datetime.datetime.now(datetime.timezone.utc).year

        docs = list()
        with ThreadPoolExecutor(max_workers=MagParams.BQ_SESSION_LIMIT) as executor:
            for release in releases:
                ts = release.strftime('%Y%m%d')
                fos_ids = self._cache[f'{MagCacheKey.FOSL0}{ts}']

                for fos in fos_ids:
                    futures = list()
                    logging.info(f'Computing release: {ts}, fos: {fos[1]}')

                    for year in range(FosL0ScoreFieldYearModule.YEAR_START, year_end + 1):
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

        clear_index(MagFosL0ScoreFieldYear)
        if index:
            delete_index(MagFosL0ScoreFieldYear)

    def _construct_es_docs(self, release: datetime.date, ts: str, fos: Tuple[int, str], year: int) -> List[Document]:
        """ Construct MagFosL0ScoreFieldYear docs for each release.

        @param release: Release date.
        @param ts: Table suffix (timestamp).
        @param fos: FieldOfStudyId, Normalized Name.
        @param year: Publication year we're interested in.
        @return List of constructed elastic search documents.
        """

        docs = list()
        if search_count_by_release(MagFosL0ScoreFieldYear, release.isoformat()) > 0:
            return docs

        year = str(year)
        counts = self._get_bq_counts(ts, fos[0], year)
        num_buckets = int((FosL0ScoreFieldYearModule.BUCKET_END - FosL0ScoreFieldYearModule.BUCKET_START) \
                          / FosL0ScoreFieldYearModule.BUCKET_STEP)
        histogram = [0] * num_buckets

        for i in range(len(counts)):
            bucket = counts[FosL0ScoreFieldYearModule.BQ_BUCKET][i] - 1
            count = counts[FosL0ScoreFieldYearModule.BQ_COUNT][i]
            histogram[bucket] = count

        for i in range(len(histogram)):
            doc = MagFosL0ScoreFieldYear(release=release, field_id=fos[0], field_name=fos[1], year=year,
                                         count=histogram[i],
                                         score_start=i * FosL0ScoreFieldYearModule.BUCKET_STEP,
                                         score_end=(i + 1) * FosL0ScoreFieldYearModule.BUCKET_STEP)
            docs.append(doc)

        self._cache[f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{ts}-{fos[0]}-{year}'] = histogram

        return docs

    def _get_bq_counts(self, ts: str, fos_id: int, year: int) -> pd.DataFrame:
        """ Get counts from BigQuery

        @param ts: Table suffix (timestamp).
        @param fos_id: FieldOfStudyId.
        @param year: Publication year we're interested in.
        @return DataFrame of counts.
        """

        sql = self._tpl_histogram.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                         ts=ts, fos_id=fos_id, year=year, count=FosL0ScoreFieldYearModule.BQ_COUNT,
                                         bstart=FosL0ScoreFieldYearModule.BUCKET_START + FosL0ScoreFieldYearModule.BUCKET_STEP,
                                         bend=FosL0ScoreFieldYearModule.BUCKET_END,
                                         bstep=FosL0ScoreFieldYearModule.BUCKET_STEP)
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df
