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

from typing import List, Tuple, Union
from scipy.spatial.distance import jensenshannon

from observatory.dags.dataquality.autofetchcache import AutoFetchCache
from observatory.dags.dataquality.config import MagCacheKey
from observatory.dags.dataquality.analyser import MagAnalyserModule
from observatory.dags.dataquality.es_mag import MagFosL0ScoreFieldYearMetricR, MagFosL0ScoreFieldYearMetricY
from observatory.dags.dataquality.es_utils import (
    init_doc,
    clear_index,
    bulk_index,
    delete_index,
    search_count_by_release
)


class FosL0ScoreFieldYearMetricsModule(MagAnalyserModule):
    """ Calculate the Jensen Shannon distance between histograms of two consecutive releases of the saliency scores for
        each field of study label, per year. Generate MagFosL0ScoreFieldYearMetric elastic search documents.
        Currently FosL0ScoreFieldYearModule is a dependency that needs to run first to populate the cache.
    """

    YEAR_START = 2000  # Year to start fetching information from.

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

        init_doc(MagFosL0ScoreFieldYearMetricY)
        init_doc(MagFosL0ScoreFieldYearMetricR)

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'{self.name()}: executing.')
        releases = self._cache[MagCacheKey.RELEASES]
        year_end = datetime.datetime.now(datetime.timezone.utc).year

        docs = list()
        for r in range(len(releases)):
            release = releases[r]
            ts = release.strftime('%Y%m%d')
            fos_ids = self._cache[f'{MagCacheKey.FOSL0}{ts}']

            # If records exist in elastic search, skip.  This is not robust to partial records (past interrupted loads).
            if search_count_by_release(MagFosL0ScoreFieldYearMetricY, release.isoformat()) > 0:
                continue

            # Construct elastic search documents for each level 0 field of study id.
            for fos in fos_ids:
                for year in range(FosL0ScoreFieldYearMetricsModule.YEAR_START, year_end + 1):
                    es_docs = self._construct_es_docs(releases, r, fos, year)
                    docs.extend(es_docs)


        logging.info(f'{self.name()}: indexing {len(docs)} docs of type MagFosL0ScoreFieldYearMetricY, MagFosL0ScoreFieldYearMetricR.')
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

    def _construct_es_docs(self, releases: datetime.date, rel_idx: int, fos: Tuple[int, str], year: int) -> \
            List[Union[MagFosL0ScoreFieldYearMetricY, MagFosL0ScoreFieldYearMetricR]]:
        """ Construct MagFosL0ScoreFieldYearMetricY and MagFosL0ScoreFieldYearMetricR docs for each release.
        @param releases: List of release dates.
        @param rel_idx: Release index for the release we're currently dealing with.
        @param fos: (FieldOfStudyId, Normalized Name).
        @param year: Publication year we're interested in.
        @return List of MagFosL0ScoreFieldYearMetricY and MagFosL0ScoreFieldYearMetricR documents.
        """

        eps = 1e-12
        ts = releases[rel_idx].strftime('%Y%m%d')

        docs = list()

        cyear_crel = self._cache[f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{ts}-{fos[0]}-{year}']

        # Get the previous year, current release
        pyear_crel = cyear_crel
        if year - 1 >= FosL0ScoreFieldYearMetricsModule.YEAR_START:
            pyear_crel = self._cache[f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{ts}-{fos[0]}-{year - 1}']

        # Get the current year, previous release
        cyear_prel = cyear_crel
        if rel_idx > 0:
            pts = releases[rel_idx - 1].strftime('%Y%m%d')
            cyear_prel = self._cache[f'{MagCacheKey.FOSL0_FIELD_YEAR_SCORES}{pts}-{fos[0]}-{year}']

        # Adding for numerical stability
        cyear_crel = [x + eps for x in cyear_crel]
        cyear_prel = [x + eps for x in cyear_prel]
        pyear_crel = [x + eps for x in pyear_crel]

        # Compute the JS distance
        rel_diff = jensenshannon(cyear_crel, cyear_prel)
        year_diff = jensenshannon(cyear_crel, pyear_crel)

        # Create documents
        ydoc = MagFosL0ScoreFieldYearMetricY(release=releases[rel_idx], field_id=fos[0], field_name=fos[1],
                                             year=str(year), js_dist=year_diff)

        rdoc = MagFosL0ScoreFieldYearMetricR(release=releases[rel_idx], field_id=fos[0], field_name=fos[1],
                                             year=str(year), js_dist=rel_diff)

        docs.append(ydoc)
        docs.append(rdoc)

        return docs
