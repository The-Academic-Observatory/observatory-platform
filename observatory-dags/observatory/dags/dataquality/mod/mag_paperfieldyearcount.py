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
import pandas as pd

from observatory.dags.dataquality.autofetchcache import AutoFetchCache
from observatory.dags.dataquality.analyser import MagAnalyserModule
from observatory.dags.dataquality.config import JinjaParams, MagCacheKey, MagTableKey
from jinja2 import Environment, PackageLoader
from typing import Tuple, List

from observatory.dags.dataquality.es_utils import (
    init_doc,
    search_count_by_release,
    clear_index,
    bulk_index,
    delete_index,
)

from observatory.dags.dataquality.es_mag import MagPapersFieldYearCount


class PaperFieldYearCountModule(MagAnalyserModule):
    """
    MagAnalyser module to compute the paper counts per field per year. This includes differences between years, and
    their proportionality.
    """

    YEAR_START = 2000  # Year to start fetching information from.
    BQ_COUNT = 'count'  # SQL column for the counts.

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

        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_count_per_field = self._tpl_env.get_template('mag_fos_count_perfield.sql.jinja2')

        init_doc(MagPapersFieldYearCount)

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'{self.name()}: executing.')
        releases = self._cache[MagCacheKey.RELEASES]

        for release in releases:
            # If records exist in elastic search, skip.  This is not robust to partial records (past interrupted loads).
            if search_count_by_release(MagPapersFieldYearCount, release.isoformat()) > 0:
                ts = release.strftime('%Y%m%d')
                logging.info(f'{self.name()}: release {ts} already in elastic search. Skipping.')
                continue

            docs = self._construct_es_docs(release)

            logging.info(f'{self.name()}: indexing {len(docs)} docs of type MagPapersFieldYearCount.')
            if len(docs) > 0:
                bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagPapersFieldYearCount)

        if index:
            delete_index(MagPapersFieldYearCount)

    def _get_counts_dict(self, counts: pd.DataFrame) -> dict:
        """ Initialise the counts dictionary with values from the bigquery response.
        @param counts: The query response.
        @return: Dictionary of counts.
        """

        counts_dict = {}

        for i in range(len(counts)):
            fos = counts[MagTableKey.COL_FOS_ID][i]
            count = counts[PaperFieldYearCountModule.BQ_COUNT][i]
            year = counts[MagTableKey.COL_YEAR][i]

            if fos not in counts_dict:
                counts_dict[fos] = {}

            if pd.isnull(year):
                year = 'null'
            else:
                year = str(year)

            counts_dict[fos][year] = count

        return counts_dict

    def _construct_es_docs(self, release: datetime.date) -> List[MagPapersFieldYearCount]:
        """
        Construct the elastic search documents for a given release.
        @param release: Release timestamp.
        @return List of MagFosLevelCountYear docs.
        """

        ts = release.strftime('%Y%m%d')
        logging.info(f'Fetching release {ts}')

        fosl0 = self._cache[f'{MagCacheKey.FOSL0}{ts}']
        fos_lookup = {fos[0]: fos[1] for fos in fosl0}
        fos_ids = (fos[0] for fos in fosl0)

        counts = self._get_bq_counts(ts, fos_ids)
        counts_dict = self._get_counts_dict(counts)

        docs = list()

        # Construct elastic search documents for each level 0 field of study and year from YEAR_START.
        for fos in counts_dict:
            for year in counts_dict[fos]:
                count = counts_dict[fos][year]

                # If the year is null or previous year does not exist, return no change.
                if year == 'null' or str(int(year) - 1) not in counts_dict[fos]:
                    delta_pcount = 0
                    delta_count = 0
                else:
                    prev_year = str(int(year) - 1)
                    prev_count = counts_dict[fos][prev_year]
                    delta_count = count - prev_count

                    # Handle zero count edge case when calculating proportions. Return 0.
                    if prev_count == 0:
                        delta_pcount = 0
                    else:
                        delta_pcount = delta_count / prev_count

                doc = MagPapersFieldYearCount(release=release, field_name=fos_lookup[fos], field_id=fos, year=year,
                                              count=count, delta_pcount=delta_pcount, delta_count=delta_count)
                docs.append(doc)

        logging.info(f'{self.name()}: release {ts} constructed {len(docs)} documents.')
        return docs

    def _get_bq_counts(self, ts: str, fos_id: Tuple[int]) -> pd.DataFrame:
        """ Get the paper counts per field per year for each level 0 field of study.
        @param ts: timestamp to use as a suffix for the table id.
        @param fos_id: List of FieldOfStudyId we are interested in.
        @return: Query results.
        """

        sql = self._tpl_count_per_field.render(project_id=self._project_id, dataset_id=self._dataset_id, ts=ts,
                                               fos_id=tuple(fos_id), year=PaperFieldYearCountModule.YEAR_START,
                                               count=PaperFieldYearCountModule.BQ_COUNT)

        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)

        return df
