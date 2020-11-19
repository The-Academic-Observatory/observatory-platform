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

from observatory_platform.dataquality.analyser import MagAnalyserModule
from observatory_platform.dataquality.config import JinjaParams, MagCacheKey, MagParams
from jinja2 import Environment, PackageLoader
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from typing import Iterator, Tuple

from observatory_platform.utils.es_utils import (
    init_doc,
    search_count_by_release,
    clear_index,
    bulk_index,
    delete_index,
)

from observatory_platform.dataquality.utils import proportion_delta
from observatory_platform.dataquality.es_mag import MagPapersFieldYearCount


class PaperFieldYearCountModule(MagAnalyserModule):
    """
    MagAnalyser module to compute the paper counts per field per year.
    """

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
        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_count_per_field = self._tpl_env.get_template('mag_fos_count_perfield.sql.jinja2')
        init_doc(MagPapersFieldYearCount)

    def run(self, **kwargs):
        """ Run the module.
        @param kwargs: Unused.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]

        docs = list()
        for release in releases:
            if search_count_by_release(MagPapersFieldYearCount, release.isoformat()) > 0:
                continue
            ts = release.strftime('%Y%m%d')
            fos_ids = self._cache[f'{MagCacheKey.FOSL0}{ts}']

            year_counts = list()
            logging.info(f'Fetching release {ts}')
            with ThreadPoolExecutor(max_workers=MagParams.BQ_SESSION_LIMIT) as executor:
                futures = list()
                for id, name in fos_ids:
                    futures.append(executor.submit(self._get_year_counts, id, name, ts))

                for future in as_completed(futures):
                    year_counts.append(future.result())

            for id, name, year_count in year_counts:
                year_count = list(year_count)
                n = len(year_count)
                years = [year_count[i][0] for i in range(n)]
                counts = [year_count[i][1] for i in range(n)]

                prev = [0] + counts[:-1]
                delta = proportion_delta(counts, prev)
                for i in range(len(years)):
                    year = years[i]
                    count = counts[i]
                    doc = MagPapersFieldYearCount(release=release, field_name=name, field_id=id, year=str(int(year)),
                                                  count=count, delta_pcount=delta[i], delta_count=counts[i]-prev[i])
                    docs.append(doc)

        logging.info(f'Indexing {len(docs)} MagPapersFieldYearCount documents.')
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

    def _get_year_counts(self, id: int, name: str, ts: str) -> Iterator[Tuple[int, int]]:
        """ Get the paper counts per field per year for each level 0 field of study.
        @param id: FieldOfStudy id to pull data for.
        @param name: FieldOfStudy name.
        @param ts: timestamp to use as a suffix for the table id.
        @return: zip(year, count) information.
        """

        sql = self._tpl_count_per_field.render(project_id=self._project_id, dataset_id=self._dataset_id, release=ts,
                                               fos_id=id)
        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        return (id, name, zip(df['Year'].to_list(), df['count'].to_list()))
