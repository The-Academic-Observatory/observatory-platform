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
from typing import List, Tuple

from observatory.dags.dataquality.autofetchcache import AutoFetchCache
from observatory.dags.dataquality.config import JinjaParams, MagCacheKey, MagTableKey
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

    YEAR_START = 2000  # The year to start calculating records for.
    BUCKET_START = 0.01  # Starting endpoint for histogram intervals, e.g., first interval is [0, 0.01], then it's 0.01
    BUCKET_END = 1.0  # Last endpoint for histogram intervals, e.g., last interval is [0.99, 1.0], then it's 1.0
    BUCKET_STEP = 0.01  # Width of each interval

    BQ_COUNT = 'count'  # Name of the count column in the SQL query.
    BQ_BUCKET = 'bucket'  # Name of the bucket column in the SQL query.

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

        init_doc(MagFosL0ScoreFieldYear)

        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_histogram = self._tpl_env.get_template('histogram_fosl0.sql.jinja2')

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'{self.name()}: executing.')
        releases = self._cache[MagCacheKey.RELEASES]

        for release in releases:
            # If records exist in elastic search, skip.  This is not robust to partial records (past interrupted loads).
            if search_count_by_release(MagFosL0ScoreFieldYear, release.isoformat()) > 0:
                continue

            docs = self._construct_es_docs(release)

            logging.info(f'{self.name()}: indexing {len(docs)} docs of type MagFosL0ScoreFieldYear.')
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
        """ @return: A zero initialised histogram. """

        num_buckets = int((FosL0ScoreFieldYearModule.BUCKET_END - FosL0ScoreFieldYearModule.BUCKET_START) \
                          / FosL0ScoreFieldYearModule.BUCKET_STEP) + 1
        histogram = [0] * num_buckets

        return histogram

    def _get_histograms_dict(self, counts: pd.DataFrame) -> dict:
        """ Initialise a histogram dictionary with the values from bigquery.
            @param counts: Query response of histograms.
            @return: Histogram dictionary.
        """

        histograms = {}

        # Reconstruct the histograms one interval at a time.
        for i in range(len(counts)):
            fos_id = counts[MagTableKey.COL_FOS_ID][i]
            year = counts[MagTableKey.COL_YEAR][i]
            bucket = counts[FosL0ScoreFieldYearModule.BQ_BUCKET][i]
            count = counts[FosL0ScoreFieldYearModule.BQ_COUNT][i]

            if pd.isnull(year):
                year = 'null'
            else:
                year = str(year)

            if fos_id not in histograms:
                histograms[fos_id] = {}

            if year not in histograms[fos_id]:
                histograms[fos_id][year] = self._new_histogram()

            histograms[fos_id][year][bucket] = count

        return histograms

    def _construct_es_docs(self, release: datetime.date) -> List[MagFosL0ScoreFieldYear]:
        """ Construct MagFosL0ScoreFieldYear docs for each release.

        @param release: Release date.
        @return: List of constructed elastic search documents.
        """

        ts = release.strftime('%Y%m%d')

        fosl0 = self._cache[f'{MagCacheKey.FOSL0}{ts}']
        fos_ids = (fos[0] for fos in fosl0)
        fos_lookup = {fos[0]: fos[1] for fos in fosl0}

        counts = self._get_bq_counts(ts, fos_ids)
        histograms = self._get_histograms_dict(counts)

        docs = list()

        # Create a document for each histogram slice.
        for fos_id in histograms:
            for year in histograms[fos_id]:
                num_buckets = len(histograms[fos_id][year])
                histogram = [0] * num_buckets

                for i in range(num_buckets):
                    count = histograms[fos_id][year][i]
                    histogram[i] = count
                    doc = MagFosL0ScoreFieldYear(release=release, year=year, count=count,
                                                 field_id=fos_id, field_name=fos_lookup[fos_id],
                                                 score_start=i * FosL0ScoreFieldYearModule.BUCKET_STEP,
                                                 score_end=(i + 1) * FosL0ScoreFieldYearModule.BUCKET_STEP)
                    docs.append(doc)

                key = f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{ts}-{fos_id}-{year}'
                self._cache[f'{key}'] = histogram

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
                                         bucket=FosL0ScoreFieldYearModule.BQ_BUCKET,
                                         bstart=FosL0ScoreFieldYearModule.BUCKET_START + FosL0ScoreFieldYearModule.BUCKET_STEP,
                                         bend=FosL0ScoreFieldYearModule.BUCKET_END,
                                         bstep=FosL0ScoreFieldYearModule.BUCKET_STEP)

        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)

        return df
