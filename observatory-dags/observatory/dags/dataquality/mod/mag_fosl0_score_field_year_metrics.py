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

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
from scipy.spatial.distance import jensenshannon
from elasticsearch_dsl.document import IndexMeta

from observatory.dags.dataquality.config import JinjaParams, MagCacheKey, MagParams, MagTableKey
from observatory.dags.dataquality.analyser import MagAnalyserModule
from observatory.dags.dataquality.es_mag import MagFosL0ScoreFieldYearMetricR, MagFosL0ScoreFieldYearMetricY
from observatory.dags.dataquality.es_utils import (
    init_doc,
    clear_index,
    bulk_index,
    delete_index,
    search
)


class FosL0ScoreFieldYearMetricsModule(MagAnalyserModule):
    """ Calculate the Jensen Shannon distance between histograms of two consecutive releases of the saliency scores for
        each field of study label, per year. Generate MagFosL0ScoreFieldYearMetric elastic search documents.
        Currently FosL0ScoreFieldYearModule is a dependency that needs to run first to populate the cache.
    """

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
        init_doc(MagFosL0ScoreFieldYearMetricY)
        init_doc(MagFosL0ScoreFieldYearMetricR)

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]
        year_end = datetime.datetime.now(datetime.timezone.utc).year

        docs = list()
        for r in range(len(releases)):
            release = releases[r]
            ts = release.strftime('%Y%m%d')
            fos_ids = self._cache[f'{MagCacheKey.FOSL0}{ts}']

            for f in range(len(fos_ids)):
                fos = fos_ids[f]
                logging.info(f'Computing release: {ts}, fos: {fos[1]}')

                for year in range(FosL0ScoreFieldYearMetricsModule.YEAR_START, year_end + 1):
                    cyear_crel = self._cache[f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{ts}-{fos[0]}-{year}']
                    pyear_crel = cyear_crel
                    if year-1 >= FosL0ScoreFieldYearMetricsModule.YEAR_START:
                        pyear_crel = self._cache[f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{ts}-{fos[0]}-{year-1}']

                    cyear_prel = cyear_crel
                    if r > 0:
                        pts = releases[r-1].strftime('%Y%m%d')
                        cyear_prel = self._cache[f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{pts}-{fos[0]}-{year}']



                    rel_diff = jensenshannon(cyear_crel, cyear_prel)
                    year_diff = jensenshannon(cyear_crel, pyear_crel)

                    ydoc = MagFosL0ScoreFieldYearMetricY(release=release, field_id=fos[0], field_name=fos[1],
                                                         year=str(year), js_dist=year_diff)

                    rdoc = MagFosL0ScoreFieldYearMetricY(release=release, field_id=fos[0], field_name=fos[1],
                                                         year=str(year), js_dist=rel_diff)
                    docs.append(ydoc)
                    docs.append(rdoc)

        if len(docs) > 0:
            bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagFosL0ScoreFieldYearMetricY)
        clear_index(MagFosL0ScoreFieldYearMetricR)
        if index:
            delete_index(MagFosL0ScoreFieldYearMetricY)
            delete_index(MagFosL0ScoreFieldYearMetricR)

    def _get_histogram(self, es_doc: IndexMeta, release: datetime.date, ts: str, fos: List[int, str], year: int) -> List[int]:
        """ Check cache for histogram information. If it doesn't exist, fetch it from elastic search.  If that doesn't
        exist, throw an error.  Will avoid using auto fetching in cache for time being.

        @param ts: Timestamp
        @param fos: Field of study structure (id, name)
        @param year: Year of interest.
        @return: Histogram of saliency scores.
        """

        key = f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{ts}-{fos[0]}-{year}'
        if key in self._cache:
            return self._cache[key]

        hits = search(es_doc, sort_field='score_start', release=release.isoformat(), year=str(year), field_id=fos[0])

        if len(hits) == 0:
            raise Exception(f'{key} has no cached data.')

        histogram = [x.count for x in hits]