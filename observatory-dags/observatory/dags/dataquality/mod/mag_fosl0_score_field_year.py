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

    YEAR_START = 2000
    BUCKET_START = 0.01
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

        for release in releases:
            if search_count_by_release(MagFosL0ScoreFieldYear, release.isoformat()) > 0:
                continue

            docs = self._construct_es_docs(release)
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

    def _new_histogram(self):
        """ @return: A histogram of 0s
        """

        num_buckets = int((FosL0ScoreFieldYearModule.BUCKET_END - FosL0ScoreFieldYearModule.BUCKET_START) \
                          / FosL0ScoreFieldYearModule.BUCKET_STEP)
        histogram = [0] * num_buckets

        return histogram

    def _get_histograms_dict(self, counts: pd.DataFrame) -> dict:
        """ Initialise a histogram dictionary with the values from bigquery.
            histogram[fos_id][year][bucket_index]

            @param counts: Query response of histograms.
            @return: Histogram dictionary.
        """

        histograms = {}

        for i in range(len(counts)):
            fos_id = counts[MagTableKey.COL_FOS_ID][i]
            year = counts[MagTableKey.COL_YEAR][i]
            bucket = counts[FosL0ScoreFieldYearModule.BQ_BUCKET][i]
            count = counts[FosL0ScoreFieldYearModule.BQ_COUNT][i]

            if fos_id not in histograms:
                histograms[fos_id] = {}

            if year not in histograms[fos_id]:
                histograms[fos_id][year] = self._new_histogram()

            histograms[fos_id][year][bucket] = count

        return histograms


    def _construct_es_docs(self, release: datetime.date) -> List[Document]:
        """ Construct MagFosL0ScoreFieldYear docs for each release.

        @param release: Release date.
        @return: List of constructed elastic search documents.
        """

        ts = release.strftime('%Y%m%d')
        fosl0 = self._cache[f'{MagCacheKey.FOSL0}{ts}']
        fos_ids = (fos[0] for fos in fosl0)
        fos_lookup = { fos[0]: fos[1] for fos in fosl0 }

        docs = list()
        counts = self._get_bq_counts(ts, fos_ids)
        histograms = self._get_histograms_dict(counts)

        for fos_id in histograms:
            for year in histograms[fos_id]:
                for bucket in histograms[fos_id][year]:
                    doc = MagFosL0ScoreFieldYear(release=release, year=year, count=histograms[fos_id][year][bucket],
                                                 field_id=fos_id, field_name=fos_lookup[fos_id],
                                                 score_start=bucket * FosL0ScoreFieldYearModule.BUCKET_STEP,
                                                 score_end=(bucket + 1) * FosL0ScoreFieldYearModule.BUCKET_STEP)

                    docs.append(doc)
                self._cache[f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{ts}-{fos_id}-{year}'] = histograms[fos_id][year][bucket]

        return docs

    def _get_bq_counts(self, ts: str, fos_ids: Tuple[int]) -> pd.DataFrame:
        """ Get counts from BigQuery

        @param ts: Table suffix (timestamp).
        @param fos_ids: List of FieldOfStudyId we're interested in.
        @return DataFrame of counts.
        """

        sql = self._tpl_histogram.render(project_id=self._project_id, dataset_id=self._dataset_id, ts=ts,
                                         fos_ids=fos_ids, year=FosL0ScoreFieldYearModule.YEAR_START,
                                         count=FosL0ScoreFieldYearModule.BQ_COUNT,
                                         bstart=FosL0ScoreFieldYearModule.BUCKET_START + FosL0ScoreFieldYearModule.BUCKET_STEP,
                                         bend=FosL0ScoreFieldYearModule.BUCKET_END,
                                         bstep=FosL0ScoreFieldYearModule.BUCKET_STEP)
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return df
