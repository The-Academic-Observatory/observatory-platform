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

from collections import OrderedDict
from datetime import timezone
from jinja2 import Environment, PackageLoader
from typing import Union, List, Tuple

from observatory.dags.dataquality.config import (
    JinjaParams,
    MagCacheKey,
    MagTableKey
)

from observatory.dags.dataquality.analyser import (
    DataQualityAnalyser,
    MagAnalyserModule,
)

from observatory.dags.dataquality.autofetchcache import AutoFetchCache

from observatory.dags.dataquality.mod.mag_fosl0 import FieldsOfStudyLevel0Module
from observatory.dags.dataquality.mod.mag_papermetrics import PaperMetricsModule
from observatory.dags.dataquality.mod.mag_paperyearscount import PaperYearsCountModule
from observatory.dags.dataquality.mod.mag_paperfieldyearcount import PaperFieldYearCountModule
from observatory.dags.dataquality.mod.mag_doicountdoctype import DoiCountDocTypeModule
from observatory.dags.dataquality.mod.mag_doicountsdoctypeyear import DoiCountsDocTypeYearModule
from observatory.dags.dataquality.mod.mag_foslevelcount import FosLevelCountModule
from observatory.dags.dataquality.mod.mag_foslevelcountyear import FosLevelCountYearModule
from observatory.dags.dataquality.mod.mag_fos_count_pub_field_year import FosCountsPubFieldYearModule
from observatory.dags.dataquality.mod.mag_fosl0_score_field_year import FosL0ScoreFieldYearModule
from observatory.dags.dataquality.mod.mag_fosl0_score_field_year_metrics import FosL0ScoreFieldYearMetricsModule


class MagAnalyser(DataQualityAnalyser):
    """
    Perform data quality analysis on a Microsoft Academic Graph release, and save the results to ElasticSearch and
    BigQuery (maybe).
    """

    ARG_MODULES = 'modules'

    def __init__(self, project_id: str, dataset_id: str,
                 modules: Union[None, List[MagAnalyserModule]] = None):

        logging.info('Initialising MagAnalyser')

        self._project_id = project_id
        self._dataset_id = dataset_id
        self._end_date = datetime.datetime.now(timezone.utc)
        self._tpl_env = Environment(
            loader=PackageLoader(JinjaParams.PKG_NAME, JinjaParams.TEMPLATE_PATHS))
        self._tpl_releases = self._tpl_env.get_template('get_releases.sql.jinja2')
        self._tpl_select = self._tpl_env.get_template('select_table.sql.jinja2')

        self._cache = AutoFetchCache()
        self._init_cache()
        self._modules = self._load_modules(modules)

    def run(self, **kwargs):
        """ Entry point for the analyser.

        @param kwargs: Optional arguments.
        modules=list() a list of module names to run in order.
        """

        logging.info('Running MagAnalyserModule modules.')
        modules = self._modules

        if MagAnalyser.ARG_MODULES in kwargs:
            modules = self._load_modules(kwargs[MagAnalyser.ARG_MODULES])

        for module in modules.values():
            module.run()

    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by all modules in the analyser.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        for module in self._modules.values():
            module.erase(index, **kwargs)

    def _init_cache(self):
        """ Initialise some common things in the auto fetcher cache. """

        self._init_releases_fetcher()
        self._init_fosl0_fetcher()
        self._init_foslevels_fetcher()
        self._init_doctype_fetcher()

    def _init_releases_fetcher(self):
        """ Initialise the releases cache fetcher. """

        self._cache.set_fetcher(MagCacheKey.RELEASES, lambda _: self._get_releases())

    def _init_fosl0_fetcher(self):
        """ Initialise the fields of study level 0 id/name fetcher. """

        releases = self._cache[MagCacheKey.RELEASES]
        for release in releases:
            ts = release.strftime('%Y%m%d')
            self._cache.set_fetcher(f'{MagCacheKey.FOSL0}{ts}', lambda key: self._get_fosl0(key))

    def _init_foslevels_fetcher(self):
        """ Initialise the fields of study level 0 id/name fetcher. """

        releases = self._cache[MagCacheKey.RELEASES]
        for release in releases:
            ts = release.strftime('%Y%m%d')
            self._cache.set_fetcher(f'{MagCacheKey.FOS_LEVELS}{ts}', lambda key: self._get_fos_levels(key))

    def _init_doctype_fetcher(self):
        """ Initialise the doc_type fetcher for each release. """

        releases = self._cache[MagCacheKey.RELEASES]
        for release in releases:
            ts = release.strftime('%Y%m%d')
            self._cache.set_fetcher(f'{MagCacheKey.DOC_TYPE}{ts}', lambda key: self._get_doc_type(key))

    def _load_modules(self, modules: Union[None, List[MagAnalyserModule]]) -> OrderedDict:
        """ Load the modules into an ordered dictionary for use.
        @param modules: None or a list of modules you want to load into an ordered dictionary of modules.
        @return: Ordered dictionary of modules.
        """

        mods = OrderedDict()
        # Override with user supplied list.
        if modules is not None:
            for module in modules:
                if not issubclass(type(module), MagAnalyserModule):
                    raise ValueError(f'{module} is not a MagAnalyserModule')

                mods[module.name()] = module

        # Use default modules.
        else:
            default_modules = [
                FieldsOfStudyLevel0Module(self._project_id, self._dataset_id, self._cache),
                PaperMetricsModule(self._project_id, self._dataset_id, self._cache),
                PaperYearsCountModule(self._project_id, self._dataset_id, self._cache),
                PaperFieldYearCountModule(self._project_id, self._dataset_id, self._cache),
                DoiCountDocTypeModule(self._project_id, self._dataset_id, self._cache),
                DoiCountsDocTypeYearModule(self._project_id, self._dataset_id, self._cache),
                FosLevelCountModule(self._project_id, self._dataset_id, self._cache),
                FosLevelCountYearModule(self._project_id, self._dataset_id, self._cache),
                FosCountsPubFieldYearModule(self._project_id, self._dataset_id, self._cache),
                FosL0ScoreFieldYearModule(self._project_id, self._dataset_id, self._cache),
                FosL0ScoreFieldYearMetricsModule(self._project_id, self._dataset_id, self._cache),
            ]

            for module in default_modules:
                mods[module.name()] = module

        return mods

    def _get_releases(self) -> List[datetime.date]:
        """ Get the list of MAG releases from BigQuery.
        @return: List of MAG release dates sorted in ascending order.
        """

        sql = self._tpl_releases.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                        table_id=MagTableKey.TID_FOS, end_date=self._end_date)
        rel_list = list(reversed(pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)['suffix']))
        releases = [release.date() for release in rel_list]
        logging.info(f'Found {len(releases)} MAG releases in BigQuery.')
        return releases

    def _get_fosl0(self, key) -> List[Tuple[int, str]]:
        """ Get the level 0 fields of study for a given MAG release from BigQuery.
        @param key: Key used to access cache. Suffix is release date.
        @return: Tuple of (id, name) for each level 0 field of study.
        """

        table_suffix = key[key.find('_') + 1:]
        sql = self._tpl_select.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                      table_id=f'{MagTableKey.TID_FOS}{table_suffix}',
                                      columns=[MagTableKey.COL_FOS_ID, MagTableKey.COL_NORM_NAME],
                                      where='Level = 0',
                                      order_by=MagTableKey.COL_FOS_ID)

        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        ids = df[MagTableKey.COL_FOS_ID].to_list()
        names = df[MagTableKey.COL_NORM_NAME].to_list()

        return list(zip(ids, names))

    def _get_doc_type(self, key) -> List[str]:
        """ Get the DocType list for each release.
        @param key: Key used to access cache. Suffix is release date.
        @return: List of DocType for that release.
        """

        table_suffix = key[key.find('_') + 1:]
        sql = self._tpl_select.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                      table_id=f'{MagTableKey.TID_PAPERS}{table_suffix}',
                                      columns=[f'DISTINCT({MagTableKey.COL_DOC_TYPE})'],
                                      order_by=MagTableKey.COL_DOC_TYPE)

        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        doc_types = df[MagTableKey.COL_DOC_TYPE].to_list()

        return doc_types

    def _get_fos_levels(self, key) -> List[str]:
        """ Get the field of study levels list for each release.
        @param key: Key used to access cache. Suffix is release date.
        @return: List of levels for that release.
        """

        table_suffix = key[key.find('_') + 1:]
        sql = self._tpl_select.render(project_id=self._project_id, dataset_id=self._dataset_id,
                                      table_id=f'{MagTableKey.TID_FOS}{table_suffix}',
                                      columns=[f'DISTINCT({MagTableKey.COL_LEVEL})'],
                                      order_by=MagTableKey.COL_LEVEL)

        df = pd.read_gbq(sql, project_id=self._project_id, progress_bar_type=None)
        doc_types = df[MagTableKey.COL_LEVEL].to_list()

        return doc_types
