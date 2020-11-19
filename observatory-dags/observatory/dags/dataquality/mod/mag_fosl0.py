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


import datetime
import logging
import pandas as pd

from jinja2 import Environment, PackageLoader
from typing import List, Union
from scipy.spatial.distance import jensenshannon
from elasticsearch_dsl import Document

from observatory_platform.dataquality.utils import proportion_delta
from observatory_platform.dataquality.config import JinjaParams, MagCacheKey, MagParams, MagTableKey
from observatory_platform.dataquality.analyser import MagAnalyserModule

from observatory_platform.utils.es_utils import (
    clear_index,
    bulk_index,
    delete_index,
    get_or_init_doc_count,
    search_by_release
)

from observatory_platform.dataquality.es_mag import MagFosL0Counts, MagFosL0Metrics



class FieldsOfStudyLevel0Module(MagAnalyserModule):
    """ MagAnalyser module to compute a profile on the MAG Level 0 FieldsOfStudy information. """

    ES_FOS_ID = 'field_id'

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
        self._tpl_select = self._tpl_env.get_template('select_table.sql.jinja2')

        self._num_es_metrics = get_or_init_doc_count(MagFosL0Metrics)
        self._num_es_counts = get_or_init_doc_count(MagFosL0Counts)

    def run(self, **kwargs):
        """ Run this module.
        @param kwargs: Not used.
        """

        logging.info(f'Running {self.name()}')
        releases = self._cache[MagCacheKey.RELEASES]
        num_releases = len(releases)

        if num_releases == self._num_es_metrics and num_releases == self._num_es_counts:
            return

        if self._num_es_counts == 0 or self._num_es_metrics == 0:
            logging.info('No data found in elastic search. Calculating for all releases.')
            previous_counts = self._get_bq_counts(releases[0])
        else:
            logging.info('Retrieving elastic search records.')
            previous_counts = FieldsOfStudyLevel0Module._get_es_counts(releases[self._num_es_metrics - 1].isoformat())

            if previous_counts is None:
                logging.warning('Inconsistent records found in elastic search. Recalculating all releases.')
                self._num_es_counts = 0
                self.erase()
                previous_counts = self._get_bq_counts(releases[0])

        # Construct elastic search documents
        docs = self._construct_es_docs(releases, previous_counts)

        # Save documents in ElasticSearch
        bulk_index(docs)

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        clear_index(MagFosL0Metrics)
        clear_index(MagFosL0Counts)

        if index:
            delete_index(MagFosL0Metrics)
            delete_index(MagFosL0Counts)

    def _construct_es_docs(self, releases: List[datetime.date], previous_counts: pd.DataFrame) -> List[Document]:
        """
        Calculate metrics and construct elastic search documents.
        @param releases: List of MAG release dates.
        @param previous_counts: Count information from the previous release.
        @return: List of elastic search documents representing the computed metrics.
        """

        docs = list()
        for i in range(self._num_es_counts, len(releases)):
            release = releases[i]
            current_counts = self._get_bq_counts(release)
            curr_fosid = current_counts[MagTableKey.COL_FOS_ID].to_list()
            curr_fosname = current_counts[MagTableKey.COL_NORM_NAME].to_list()
            prev_fosid = previous_counts[MagTableKey.COL_FOS_ID].to_list()
            prev_fosname = previous_counts[MagTableKey.COL_NORM_NAME].to_list()

            id_unchanged = curr_fosid == prev_fosid
            normalized_unchanged = curr_fosname == prev_fosname

            ts = release.strftime('%Y%m%d')
            self._cache[f'{MagCacheKey.FOSL0}{ts}'] = list(zip(curr_fosid, curr_fosname))

            dppaper = None
            dpcitations = None
            if id_unchanged and normalized_unchanged:
                dppaper = proportion_delta(current_counts[MagTableKey.COL_PAP_COUNT],
                                           previous_counts[MagTableKey.COL_PAP_COUNT])
                dpcitations = proportion_delta(current_counts[MagTableKey.COL_CIT_COUNT],
                                               previous_counts[MagTableKey.COL_CIT_COUNT])

            # Populate counts
            counts = FieldsOfStudyLevel0Module._construct_es_counts(releases[i], current_counts, dppaper, dpcitations)
            logging.info(f'Constructed {len(counts)} MagFosL0Counts documents.')
            docs.extend(counts)

            # Populate metrics
            metrics = FieldsOfStudyLevel0Module._construct_es_metrics(releases[i], current_counts, previous_counts,
                                                                      id_unchanged, normalized_unchanged)
            logging.info(f'Constructed 1 MagFosL0Metrics document.')
            docs.append(metrics)

            # Loop maintenance
            previous_counts = current_counts
        return docs

    @staticmethod
    def _construct_es_counts(release: datetime.date, current_counts: pd.DataFrame, dppaper: pd.DataFrame,
                             dpcitations: pd.DataFrame) -> List[MagFosL0Counts]:
        """ Constructs the MagFosL0Counts documents.
        @param release: MAG release date we are generating a document for.
        @param current_counts: Counts for the current release.
        @param dppaper: Difference of proportions for the paper count between current and last release.
        @param dpcitations: Difference of proportions for the citation count between current and last release.
        @return: List of MagFosL0Counts documents.
        """

        docs = list()
        for i in range(len(current_counts[MagTableKey.COL_PAP_COUNT])):
            fosl0_counts = MagFosL0Counts(release=release)
            fosl0_counts.field_id = current_counts[MagTableKey.COL_FOS_ID][i]
            fosl0_counts.normalized_name = current_counts[MagTableKey.COL_NORM_NAME][i]
            fosl0_counts.paper_count = current_counts[MagTableKey.COL_PAP_COUNT][i]
            fosl0_counts.citation_count = current_counts[MagTableKey.COL_CIT_COUNT][i]
            if dppaper is not None:
                fosl0_counts.delta_ppaper = dppaper[i]
            if dpcitations is not None:
                fosl0_counts.delta_pcitations = dpcitations[i]
            docs.append(fosl0_counts)
        return docs

    @staticmethod
    def _construct_es_metrics(release: datetime.date, current_counts: pd.DataFrame, previous_counts: pd.DataFrame,
                              id_unchanged: bool, normalized_unchanged: bool) -> MagFosL0Metrics:
        """ Constructs the MagFosL0Metrics documents.
        @param release: MAG release date we are generating a document for.
        @param current_counts: Counts for the current release.
        @param current_counts: Counts for the previous release.
        @param id_unchanged: boolean indicating whether the id has changed between releases.
        @param normalized_unchanged: boolean indicating whether the normalized names have changed between releases.
        @return: MagFosL0Metrics document.
        """

        metrics = MagFosL0Metrics(release=release)
        metrics.field_ids_unchanged = id_unchanged
        metrics.normalized_names_unchanged = normalized_unchanged

        if id_unchanged and normalized_unchanged:
            metrics.js_dist_paper = jensenshannon(current_counts[MagTableKey.COL_PAP_COUNT],
                                                  previous_counts[MagTableKey.COL_PAP_COUNT])
            metrics.js_dist_citation = jensenshannon(current_counts[MagTableKey.COL_CIT_COUNT],
                                                     previous_counts[MagTableKey.COL_CIT_COUNT])
        return metrics

    def _get_bq_counts(self, release: datetime.date) -> pd.DataFrame:
        """
        Get the count information from BigQuery table.
        @param release: Release to pull data from.
        @return: Count information.
        """

        ts = release.strftime('%Y%m%d')
        table_id = f'{MagTableKey.TID_FOS}{ts}'
        sql = self._tpl_select.render(
            project_id=self._project_id, dataset_id=self._dataset_id, table_id=table_id,
            columns=[MagTableKey.COL_FOS_ID, MagTableKey.COL_NORM_NAME,
                     MagTableKey.COL_PAP_COUNT, MagTableKey.COL_CIT_COUNT],
            order_by=MagTableKey.COL_FOS_ID,
            where='Level = 0'
        )
        return pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)

    @staticmethod
    def _get_es_counts(release: str) -> Union[None, pd.DataFrame]:
        """ Retrieve the MagFosL0Counts documents already indexed in elastic search for a given release.
        @param release: Relevant release date.
        @return Retrieved count information.
        """

        hits = search_by_release(MagFosL0Counts, release, 'field_id')

        # Something went wrong with ES records. Delete existing and recompute them.
        if len(hits) == 0:
            return None

        data = {
            MagTableKey.COL_FOS_ID: [x.field_id for x in hits],
            MagTableKey.COL_NORM_NAME: [x.normalized_name for x in hits],
            MagTableKey.COL_PAP_COUNT: [x.paper_count for x in hits],
            MagTableKey.COL_CIT_COUNT: [x.citation_count for x in hits],
        }

        return pd.DataFrame(data=data)