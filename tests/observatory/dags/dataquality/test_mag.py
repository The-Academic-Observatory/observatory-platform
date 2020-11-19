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

import unittest
import pandas as pd
import datetime

from unittest.mock import patch
from observatory.dags.dataquality.autofetchcache import AutoFetchCache

from observatory.dags.dataquality.mod.mag_doicountdoctype import DoiCountDocTypeModule
from observatory.dags.dataquality.mod.mag_doicountsdoctypeyear import DoiCountsDocTypeYearModule
from observatory.dags.dataquality.mod.mag_fosl0 import FieldsOfStudyLevel0Module
from observatory.dags.dataquality.mod.mag_paperyearscount import PaperYearsCountModule
from observatory.dags.dataquality.mod.mag_papermetrics import PaperMetricsModule
from observatory.dags.dataquality.mod.mag_paperfieldyearcount import PaperFieldYearCountModule
from observatory.dags.dataquality.mag import MagAnalyser
from observatory.dags.dataquality.analyser import MagAnalyserModule
from observatory.dags.dataquality.config import MagTableKey, MagCacheKey
from observatory.dags.dataquality.mod.mag_foslevelcount import FosLevelCountModule
from observatory.dags.dataquality.mod.mag_foslevelcountyear import FosLevelCountYearModule

class TestMagAnalyser(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    class MockModule(MagAnalyserModule):
        def run(self, **kwargs):
            pass

        def erase(self, **kwargs):
            pass

    @patch('observatory.dags.dataquality.mag.MagAnalyser._init_cache')
    def test_get_releases(self, _):
        analyser = MagAnalyser('project_id', 'dataset_id', modules=[])
        mock_response = {'suffix': [datetime.datetime(2020, 1, 1), datetime.datetime(2010, 1, 1)]}
        with patch('observatory.dags.dataquality.mag.pd.read_gbq', return_value=mock_response) as _:
            releases = analyser._get_releases()
            self.assertEqual(releases[0], datetime.date(2010, 1, 1))
            self.assertEqual(releases[1], datetime.date(2020, 1, 1))

    @patch('observatory.dags.dataquality.mag.MagAnalyser._init_fosl0_fetcher')
    def test_cache_get_releases(self, _):
        releases = {'suffix': [datetime.datetime(2020, 1, 1), datetime.datetime(2019, 1, 1)]}

        with patch('observatory.dags.dataquality.mag.pd.read_gbq', return_value=releases) as _:
            analyser = MagAnalyser('project_id', 'dataset_id', modules=[])
            freleases = analyser._cache[MagCacheKey.RELEASES]
            self.assertEqual(len(freleases), 2)
            self.assertEqual(freleases[0], releases['suffix'][1].date())
            self.assertEqual(freleases[1], releases['suffix'][0].date())

    @patch('observatory.dags.dataquality.mag.MagAnalyser._init_cache')
    def test_load_modules(self, _):
        analyser = MagAnalyser('project_id', 'dataset_id', modules=[])
        self.assertEqual(len(analyser._modules), 0)

        analyser = MagAnalyser('project_id', 'dataset_id', modules=[TestMagAnalyser.MockModule()])
        self.assertEqual(len(analyser._modules), 1)
        self.assertTrue(analyser._modules['MockModule'] is not None)

    @patch('observatory.dags.dataquality.mag.MagAnalyser._init_cache')
    def test_run(self, _):
        with patch('tests.observatory.dags.dataquality.test_mag.TestMagAnalyser.MockModule.run') as mock_run:
            analyser = MagAnalyser('project_id', 'dataset_id', modules=[TestMagAnalyser.MockModule()])
            analyser.run()
            self.assertEqual(mock_run.call_count, 1)


class TestFieldsOfStudyLevel0Module(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = AutoFetchCache(20)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_name(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        self.assertEqual(self.module.name(), 'FieldsOfStudyLevel0Module')

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_get_es_count(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)

        class MockResponse:
            field_id = 1
            normalized_name = 'test'
            paper_count = 2
            citation_count = 3

        with patch('observatory.dags.dataquality.mod.mag_fosl0.search_by_release',
                   return_value=[MockResponse]) as _:
            result = self.module._get_es_counts('testrelease')
            self.assertEqual(result[MagTableKey.COL_FOS_ID][0], 1)
            self.assertEqual(result[MagTableKey.COL_NORM_NAME][0], 'test')
            self.assertEqual(result[MagTableKey.COL_PAP_COUNT][0], 2)
            self.assertEqual(result[MagTableKey.COL_CIT_COUNT][0], 3)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_get_bq_counts(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        mock_response = {MagTableKey.COL_FOS_ID: [1],
                         MagTableKey.COL_NORM_NAME: ['test'],
                         MagTableKey.COL_PAP_COUNT: [2],
                         MagTableKey.COL_CIT_COUNT: [1]}
        with patch('observatory.dags.dataquality.mag.pd.read_gbq', return_value=mock_response) as mock_call:
            counts = self.module._get_bq_counts(datetime.date(2020, 1, 1))
            self.assertEqual(counts[MagTableKey.COL_FOS_ID]
                             [0], 1)
            self.assertEqual(counts[MagTableKey.COL_NORM_NAME][0], 'test')
            self.assertEqual(counts[MagTableKey.COL_PAP_COUNT][0], 2)
            self.assertEqual(counts[MagTableKey.COL_CIT_COUNT][0], 1)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_construct_es_metrics(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        release = datetime.date(2020, 1, 1)
        current_counts = {MagTableKey.COL_PAP_COUNT: [1], MagTableKey.COL_FOS_ID: [2],
                          MagTableKey.COL_NORM_NAME: ['test'],
                          MagTableKey.COL_PAP_COUNT: [3],
                          MagTableKey.COL_CIT_COUNT: [2]}
        previous_counts = current_counts
        metric = self.module._construct_es_metrics(release, current_counts, previous_counts, False, False)
        self.assertEqual(metric.release, release)
        self.assertEqual(metric.field_ids_changed, False)
        self.assertEqual(metric.normalized_names_changed, False)
        self.assertEqual(metric.js_dist_paper, 0)
        self.assertEqual(metric.js_dist_citation, 0)

        metric = self.module._construct_es_metrics(release, current_counts, previous_counts, True, False)
        self.assertEqual(metric.field_ids_changed, True)
        self.assertEqual(metric.normalized_names_changed, False)
        self.assertEqual(metric.js_dist_paper, None)
        self.assertEqual(metric.js_dist_citation, None)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_construct_es_counts(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        release = datetime.date(2020, 1, 1)
        current_counts = {MagTableKey.COL_PAP_COUNT: [1], MagTableKey.COL_FOS_ID: [2],
                          MagTableKey.COL_NORM_NAME: ['test'],
                          MagTableKey.COL_PAP_COUNT: [3],
                          MagTableKey.COL_CIT_COUNT: [2]}

        dppaper = [0.1]
        dpcitations = [0.1]
        docs = self.module._construct_es_counts(release, current_counts, dppaper, dpcitations)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0].release, release)
        self.assertEqual(docs[0].field_id, 2)
        self.assertEqual(docs[0].normalized_name, 'test')
        self.assertEqual(docs[0].paper_count, 3)
        self.assertEqual(docs[0].citation_count, 2)
        self.assertEqual(docs[0].delta_pcitations, 0.1)
        self.assertEqual(docs[0].delta_ppaper, 0.1)

        dppaper = None
        dpcitations = None
        docs = self.module._construct_es_counts(release, current_counts, dppaper, dpcitations)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0].delta_ppaper, None)
        self.assertEqual(docs[0].delta_pcitations, None)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_construct_es_docs_diff_fields(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        releases = [datetime.date(2020, 1, 1)]
        previous_counts = pd.DataFrame(
            {MagTableKey.COL_PAP_COUNT: [1], MagTableKey.COL_FOS_ID: [2],
             MagTableKey.COL_NORM_NAME: ['test'], MagTableKey.COL_PAP_COUNT: [3],
             MagTableKey.COL_CIT_COUNT: [2]})
        mock_response = pd.DataFrame(
            {MagTableKey.COL_FOS_ID: [1], MagTableKey.COL_NORM_NAME: ['test'],
             MagTableKey.COL_PAP_COUNT: [2], MagTableKey.COL_CIT_COUNT: [1]})
        with patch('observatory.dags.dataquality.mag.pd.read_gbq', return_value=mock_response) as _:
            docs = self.module._construct_es_docs(releases, previous_counts)
            self.assertEqual(len(docs), 2)
            counts = docs[0]
            self.assertEqual(counts.release, releases[0])
            self.assertEqual(counts.field_id, 1)
            self.assertEqual(counts.normalized_name, 'test')
            self.assertEqual(counts.paper_count, 2)
            self.assertEqual(counts.citation_count, 1)
            self.assertEqual(counts.delta_pcitations, None)
            self.assertEqual(counts.delta_ppaper, None)

            metric = docs[1]
            self.assertEqual(metric.release, releases[0])
            self.assertEqual(metric.field_ids_changed, True)
            self.assertEqual(metric.normalized_names_changed, False)
            self.assertEqual(metric.js_dist_paper, None)
            self.assertEqual(metric.js_dist_citation, None)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_construct_es_docs_same_fields(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        releases = [datetime.date(2020, 1, 1)]
        previous_counts = pd.DataFrame(
            {MagTableKey.COL_PAP_COUNT: [1], MagTableKey.COL_FOS_ID: [2],
             MagTableKey.COL_NORM_NAME: ['test'], MagTableKey.COL_PAP_COUNT: [3],
             MagTableKey.COL_CIT_COUNT: [2]})
        mock_response = pd.DataFrame(
            {MagTableKey.COL_FOS_ID: [2], MagTableKey.COL_NORM_NAME: ['test'],
             MagTableKey.COL_PAP_COUNT: [2], MagTableKey.COL_CIT_COUNT: [1]})
        with patch('observatory.dags.dataquality.mag.pd.read_gbq', return_value=mock_response) as _:
            docs = self.module._construct_es_docs(releases, previous_counts)
            self.assertEqual(len(docs), 2)
            counts = docs[0]
            self.assertEqual(counts.release, releases[0])
            self.assertEqual(counts.field_id, 2)
            self.assertEqual(counts.normalized_name, 'test')
            self.assertEqual(counts.paper_count, 2)
            self.assertEqual(counts.citation_count, 1)
            self.assertAlmostEqual(counts.delta_pcitations, -1 / 2)
            self.assertAlmostEqual(counts.delta_ppaper, -1 / 3)

            metric = docs[1]
            self.assertEqual(metric.release, releases[0])
            self.assertEqual(metric.field_ids_changed, False)
            self.assertEqual(metric.normalized_names_changed, False)
            self.assertEqual(metric.js_dist_paper, 0)
            self.assertEqual(metric.js_dist_citation, 0)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_run_no_releases(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        self.module._cache[MagCacheKey.RELEASES] = []

        mock_response = pd.DataFrame(
            {MagTableKey.COL_FOS_ID: [2], MagTableKey.COL_NORM_NAME: ['test'],
             MagTableKey.COL_PAP_COUNT: [2], MagTableKey.COL_CIT_COUNT: [1]})
        with patch('observatory.dags.dataquality.mag.pd.read_gbq', return_value=mock_response) as mock_bq:
            self.module.run()
            self.assertEqual(mock_bq.call_count, 0)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_run_fresh(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        self.module._cache[MagCacheKey.RELEASES] = [datetime.date(2020, 1, 1)]

        mock_response = pd.DataFrame(
            {MagTableKey.COL_FOS_ID: [2], MagTableKey.COL_NORM_NAME: ['test'],
             MagTableKey.COL_PAP_COUNT: [2], MagTableKey.COL_CIT_COUNT: [1]})
        with patch('observatory.dags.dataquality.mag.pd.read_gbq', return_value=mock_response) as mock_bq:
            with patch('observatory.dags.dataquality.mod.mag_fosl0.bulk_index') as mock_bulk:
                self.module.run()
                self.assertEqual(mock_bulk.call_count, 1)
                self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 2)
                counts = mock_bulk.call_args_list[0][0][0][0]
                self.assertEqual(counts.release, self.cache[MagCacheKey.RELEASES][0])
                self.assertEqual(counts.field_id, 2)
                self.assertEqual(counts.normalized_name, 'test')
                self.assertEqual(counts.paper_count, 2)
                self.assertEqual(counts.citation_count, 1)
                self.assertEqual(counts.delta_pcitations, 0)
                self.assertEqual(counts.delta_ppaper, 0)

                metric = mock_bulk.call_args_list[0][0][0][1]
                self.assertEqual(metric.release, self.cache[MagCacheKey.RELEASES][0])
                self.assertEqual(metric.field_ids_changed, False)
                self.assertEqual(metric.normalized_names_changed, False)
                self.assertEqual(metric.js_dist_paper, 0)
                self.assertEqual(metric.js_dist_citation, 0)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=1)
    def test_run_update(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        self.module._cache[MagCacheKey.RELEASES] = [datetime.date(2018, 1, 1), datetime.date(2019, 1, 1),
                                                    datetime.date(2020, 1, 1)]

        class EsResponse:
            field_id = 2
            normalized_name = 'test'
            paper_count = 2
            citation_count = 3

        with patch('observatory.dags.dataquality.mod.mag_fosl0.search_by_release', return_value=[EsResponse]) as _:
            mock_response = pd.DataFrame(
                {MagTableKey.COL_FOS_ID: [2], MagTableKey.COL_NORM_NAME: ['test'],
                 MagTableKey.COL_PAP_COUNT: [2], MagTableKey.COL_CIT_COUNT: [1]})
            with patch('observatory.dags.dataquality.mod.mag_fosl0.pd.read_gbq',
                       return_value=mock_response) as mock_bq:
                with patch('observatory.dags.dataquality.mod.mag_fosl0.bulk_index') as mock_bulk:
                    self.module.run()
                    self.assertEqual(mock_bulk.call_count, 1)
                    self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 4)

    @patch('observatory.dags.dataquality.mod.mag_fosl0.get_or_init_doc_count', return_value=0)
    def test_run_inconsistent(self, _):
        self.module = FieldsOfStudyLevel0Module('project_id', 'dataset_id', self.cache)
        self.module._cache[MagCacheKey.RELEASES] = [datetime.date(2018, 1, 1), datetime.date(2019, 1, 1),
                                                    datetime.date(2020, 1, 1)]

        with patch('observatory.dags.dataquality.mod.mag_fosl0.search_by_release', return_value=None) as _:
            mock_response = pd.DataFrame(
                {MagTableKey.COL_FOS_ID: [2], MagTableKey.COL_NORM_NAME: ['test'],
                 MagTableKey.COL_PAP_COUNT: [2], MagTableKey.COL_CIT_COUNT: [1]})
            with patch('observatory.dags.dataquality.mod.mag_fosl0.pd.read_gbq',
                       return_value=mock_response) as mock_bq:
                with patch('observatory.dags.dataquality.mod.mag_fosl0.bulk_index') as mock_bulk:
                    with patch('observatory.dags.dataquality.mod.mag_fosl0.clear_index') as _:
                        self.module.run()
                        self.assertEqual(mock_bulk.call_count, 1)
                        self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 6)


class TestPaperYearsCountModule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = AutoFetchCache(2)

    @patch('observatory.dags.dataquality.mod.mag_paperyearscount.init_doc')
    def test_name(self, _):
        module = PaperYearsCountModule('project_id', 'dataset_id', self.cache)
        self.assertEqual(module.name(), 'PaperYearsCountModule')

    @patch('observatory.dags.dataquality.mod.mag_paperyearscount.init_doc')
    def test_get_paper_year_count(self, _):
        module = PaperYearsCountModule('project_id', 'dataset_id', self.cache)

        mock_response = pd.DataFrame({'Year': [1990, 1991, 1992], 'count': [1, 2, 3]})
        with patch('observatory.dags.dataquality.mod.mag_paperyearscount.pd.read_gbq',
                   return_value=mock_response) as mock_bq:
            year, count = module._get_paper_year_count(datetime.date(1990, 1, 1))
            self.assertEqual(year, [1990, 1991, 1992])
            self.assertEqual(count, [1, 2, 3])

    @patch('observatory.dags.dataquality.mod.mag_paperyearscount.init_doc')
    def test_run_fresh(self, _):
        self.cache[MagCacheKey.RELEASES] = [datetime.date(2018, 1, 1), datetime.date(2019, 1, 1),
                                            datetime.date(2020, 1, 1)]
        module = PaperYearsCountModule('project_id', 'dataset_id', self.cache)
        with patch('observatory.dags.dataquality.mod.mag_paperyearscount.search_count_by_release',
                   return_value=0) as _:
            mock_response = pd.DataFrame({'Year': [1990, 1991, 1992], 'count': [1, 2, 3]})
            with patch('observatory.dags.dataquality.mod.mag_paperyearscount.pd.read_gbq',
                       return_value=mock_response) as _:
                with patch('observatory.dags.dataquality.mod.mag_paperyearscount.bulk_index',
                           return_value=mock_response) as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 1)
                    self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 9)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].delta_pcount, 1000000000, 5)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][1].delta_pcount, 1.0)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][2].delta_pcount, 0.5)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].delta_count, 1)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][1].delta_count, 1)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][2].delta_count, 1)

    @patch('observatory.dags.dataquality.mod.mag_paperyearscount.init_doc')
    def test_run_skip_computed(self, _):
        self.cache[MagCacheKey.RELEASES] = [datetime.date(2018, 1, 1), datetime.date(2019, 1, 1),
                                            datetime.date(2020, 1, 1)]
        module = PaperYearsCountModule('project_id', 'dataset_id', self.cache)
        with patch('observatory.dags.dataquality.mod.mag_paperyearscount.search_count_by_release',
                   return_value=1) as _:
            mock_response = pd.DataFrame({'Year': [1990, 1991, 1992], 'count': [1, 2, 3]})
            with patch('observatory.dags.dataquality.mod.mag_paperyearscount.pd.read_gbq',
                       return_value=mock_response) as _:
                with patch('observatory.dags.dataquality.mod.mag_paperyearscount.bulk_index',
                           return_value=mock_response) as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 0)


class TestPaperMetricsModule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @patch('observatory.dags.dataquality.mod.mag_papermetrics.get_or_init_doc_count', return_value=0)
    def test_name(self, _):
        module = PaperMetricsModule('project_id', 'dataset_id', AutoFetchCache(2))
        self.assertEqual(module.name(), 'PaperMetricsModule')

    @patch('observatory.dags.dataquality.mod.mag_papermetrics.get_or_init_doc_count', return_value=0)
    def test_get_paper_null_counts(self, _):
        module = PaperMetricsModule('project_id', 'dataset_id', AutoFetchCache(2))
        mock_response = pd.DataFrame(
            {MagTableKey.COL_DOI: [1, 2, 3], MagTableKey.COL_DOC_TYPE: [1, 1, 1],
             MagTableKey.COL_YEAR: [1, 1, 0], MagTableKey.COL_FAMILY_ID: [4, 5, 6],
             MagTableKey.COL_TOTAL: 100})
        with patch('observatory.dags.dataquality.mod.mag_papermetrics.pd.read_gbq',
                   return_value=mock_response) as _:
            counts = module._get_paper_null_counts(datetime.date(2019, 1, 1))
            self.assertEqual(counts[MagTableKey.COL_DOI].to_list(), [1, 2, 3])
            self.assertEqual(counts[MagTableKey.COL_DOC_TYPE].to_list(), [1, 1, 1])
            self.assertEqual(counts[MagTableKey.COL_YEAR].to_list(), [1, 1, 0])
            self.assertEqual(counts[MagTableKey.COL_FAMILY_ID].to_list(), [4, 5, 6])

    @patch('observatory.dags.dataquality.mod.mag_papermetrics.get_or_init_doc_count', return_value=0)
    def test_run_fresh(self, _):
        module = PaperMetricsModule('project_id', 'dataset_id', AutoFetchCache(2))
        module._cache[MagCacheKey.RELEASES] = [datetime.date(1990, 1, 1)]
        with patch('observatory.dags.dataquality.mod.mag_papermetrics.search_count_by_release',
                   return_value=0) as _:
            mock_response = pd.DataFrame(
                {MagTableKey.COL_DOI: [1], MagTableKey.COL_DOC_TYPE: [1],
                 MagTableKey.COL_YEAR: [1], MagTableKey.COL_FAMILY_ID: [4],
                 MagTableKey.COL_TOTAL: 10})
            with patch('observatory.dags.dataquality.mod.mag_papermetrics.pd.read_gbq',
                       return_value=mock_response) as _:
                with patch('observatory.dags.dataquality.mod.mag_papermetrics.bulk_index',
                           return_value=mock_response) as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 1)
                    self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 1)

    @patch('observatory.dags.dataquality.mod.mag_papermetrics.get_or_init_doc_count', return_value=1)
    def test_run_noop(self, _):
        module = PaperMetricsModule('project_id', 'dataset_id', AutoFetchCache(2))
        module._cache[MagCacheKey.RELEASES] = [datetime.date(1990, 1, 1)]
        with patch('observatory.dags.dataquality.mod.mag_papermetrics.search_count_by_release',
                   return_value=1) as _:
            mock_response = pd.DataFrame(
                {MagTableKey.COL_DOI: [1], MagTableKey.COL_DOC_TYPE: [1],
                 MagTableKey.COL_YEAR: [1], MagTableKey.COL_FAMILY_ID: [4],
                 MagTableKey.COL_TOTAL: 10})
            with patch('observatory.dags.dataquality.mod.mag_papermetrics.pd.read_gbq',
                       return_value=mock_response) as _:
                with patch('observatory.dags.dataquality.mod.mag_papermetrics.bulk_index',
                           return_value=mock_response) as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 0)

    @patch('observatory.dags.dataquality.mod.mag_papermetrics.get_or_init_doc_count', return_value=1)
    def test_run_update(self, _):
        module = PaperMetricsModule('project_id', 'dataset_id', AutoFetchCache(2))
        module._cache[MagCacheKey.RELEASES] = [datetime.date(1990, 1, 1), datetime.date(1991, 1, 1),
                                               datetime.date(1992, 1, 1)]
        with patch('observatory.dags.dataquality.mod.mag_papermetrics.search_count_by_release',
                   return_value=0) as _:
            mock_response = pd.DataFrame(
                {MagTableKey.COL_DOI: [1], MagTableKey.COL_DOC_TYPE: [1],
                 MagTableKey.COL_YEAR: [1], MagTableKey.COL_FAMILY_ID: [4],
                 MagTableKey.COL_TOTAL: [10]})
            with patch('observatory.dags.dataquality.mod.mag_papermetrics.pd.read_gbq',
                       return_value=mock_response) as _:
                with patch('observatory.dags.dataquality.mod.mag_papermetrics.bulk_index',
                           return_value=mock_response) as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 1)
                    self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 2)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].release, '1991-01-01')
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].total, 10)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].null_year, 1)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].null_doi, 1)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].null_doctype, 1)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].null_familyid, 4)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].pnull_year, 0.1)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].pnull_doi, 0.1)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].pnull_doctype, 0.1)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].pnull_familyid, 0.4)


class TestPaperFieldYearCountModule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = AutoFetchCache(2)

    @patch('observatory.dags.dataquality.mod.mag_paperfieldyearcount.init_doc')
    def test_get_year_counts(self, _):
        mock_response = pd.DataFrame({'Year': [1, 2, 3], 'count': [2, 2, 0]})
        module = PaperFieldYearCountModule('project_id', 'dataset_id', self.cache)
        with patch('observatory.dags.dataquality.mod.mag_paperfieldyearcount.pd.read_gbq',
                   return_value=mock_response):
            year_count = module._get_year_counts(1, 'name', 'testdate')
            counts = list(year_count[2])
            self.assertEqual(len(year_count), 3)
            self.assertEqual(year_count[0], 1)
            self.assertEqual(year_count[1], 'name')
            self.assertEqual(counts[0][0], 1)
            self.assertEqual(counts[1][0], 2)

    @patch('observatory.dags.dataquality.mod.mag_paperfieldyearcount.init_doc')
    def test_run(self, _):
        module = PaperFieldYearCountModule('project_id', 'dataset_id', self.cache)
        module._cache[MagCacheKey.RELEASES] = [datetime.date(1990, 1, 1)]
        module._cache[f'{MagCacheKey.FOSL0}19900101'] = [(1, 'testname')]
        with patch('observatory.dags.dataquality.mod.mag_paperfieldyearcount.search_count_by_release',
                   return_value=0):
            mock_response = pd.DataFrame({'Year': [1, 2, 3], 'count': [2, 2, 0]})
            with patch('observatory.dags.dataquality.mod.mag_paperfieldyearcount.pd.read_gbq',
                       return_value=mock_response):
                with patch('observatory.dags.dataquality.mod.mag_paperfieldyearcount.bulk_index') as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 1)
                    self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 3)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].release, datetime.date(1990, 1, 1))
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].field_name, 'testname')
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].field_id, 1)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].year, '1')
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].count, 2)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][0].delta_pcount, 2000000000, 5)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][1].delta_pcount, 0)
                    self.assertAlmostEqual(mock_bulk.call_args_list[0][0][0][2].delta_pcount, -1)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].delta_count, 2)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][1].delta_count, 0)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][2].delta_count, -2)


class TestDoiCountDocTypeModule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = AutoFetchCache(2)

    @patch('observatory.dags.dataquality.mod.mag_doicountdoctype.init_doc')
    def test_run_fresh(self, _):
        module = DoiCountDocTypeModule('project_id', 'dataset_id', self.cache)
        self.cache[MagCacheKey.RELEASES] = [datetime.date(1990, 1, 1)]
        with patch('observatory.dags.dataquality.mod.mag_doicountdoctype.search_count_by_release', return_value=0):
            mock_response = pd.DataFrame(
                {MagTableKey.COL_DOC_TYPE: ['TestType'], DoiCountDocTypeModule.BQ_DOC_COUNT: [4],
                 DoiCountDocTypeModule.BQ_NULL_COUNT: [3]})

            with patch('observatory.dags.dataquality.mod.mag_doicountdoctype.pd.read_gbq',
                       return_value=mock_response):
                with patch('observatory.dags.dataquality.mod.mag_doicountdoctype.bulk_index') as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 1)
                    self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 1)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].release, '1990-01-01')
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].doc_type, 'TestType')
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].count, 4)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].no_doi, 3)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].pno_doi, 0.75)


class TestDoiCountsDocTypeYearModule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = AutoFetchCache(2)

    @patch('observatory.dags.dataquality.mod.mag_doicountsdoctypeyear.init_doc')
    def test_run_fresh(self, _):
        module = DoiCountsDocTypeYearModule('project_id', 'dataset_id', self.cache)
        self.cache[MagCacheKey.RELEASES] = [datetime.date(1990, 1, 1)]
        self.cache[f'{MagCacheKey.DOC_TYPE}{19900101}'] = ['TestType']

        with patch('observatory.dags.dataquality.mod.mag_doicountsdoctypeyear.search_count_by_release',
                   return_value=0):
            mock_response = pd.DataFrame(
                {'Year': 1990, DoiCountsDocTypeYearModule.BQ_DOC_COUNT: [4],
                 DoiCountsDocTypeYearModule.BQ_NULL_COUNT: [3]})

            with patch('observatory.dags.dataquality.mod.mag_doicountsdoctypeyear.pd.read_gbq',
                       return_value=mock_response):
                with patch('observatory.dags.dataquality.mod.mag_doicountsdoctypeyear.bulk_index') as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 1)
                    self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 1)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].release, datetime.date(1990, 1, 1))
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].doc_type, 'TestType')
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].count, 4)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].no_doi, 3)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].pno_doi, 0.75)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].year, '1990')


class TestFosLevelCountModule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = AutoFetchCache(2)

    @patch('observatory.dags.dataquality.mod.mag_foslevelcount.init_doc')
    def test_run_fresh(self, _):
        module = FosLevelCountModule('project_id', 'dataset_id', self.cache)
        self.cache[MagCacheKey.RELEASES] = [datetime.date(1990, 1, 1)]
        self.cache[f'{MagCacheKey.DOC_TYPE}{19900101}'] = ['TestType']

        with patch('observatory.dags.dataquality.mod.mag_foslevelcount.search_count_by_release', return_value=0):
            mock_response = pd.DataFrame(
                {MagTableKey.COL_LEVEL: 5, FosLevelCountModule.BQ_LEVEL_COUNT: [4],
                 FosLevelCountModule.BQ_CIT_COUNT: [3],
                 FosLevelCountModule.BQ_PAP_COUNT: [2]
                 })

            with patch('observatory.dags.dataquality.mod.mag_foslevelcount.pd.read_gbq',
                       return_value=mock_response):
                with patch('observatory.dags.dataquality.mod.mag_foslevelcount.bulk_index') as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 1)
                    self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 1)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].release, datetime.date(1990, 1, 1))
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].level, 5)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].level_count, 4)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].num_papers, 2)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].num_citations, 3)


class TestFosLevelCountYearModule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = AutoFetchCache(2)

    @patch('observatory.dags.dataquality.mod.mag_foslevelcountyear.init_doc')
    def test_run_fresh(self, _):
        module = FosLevelCountYearModule('project_id', 'dataset_id', self.cache)
        self.cache[MagCacheKey.RELEASES] = [datetime.date(1990, 1, 1)]
        self.cache[f'{MagCacheKey.FOS_LEVELS}{19900101}'] = [0]

        with patch('observatory.dags.dataquality.mod.mag_foslevelcountyear.search_count_by_release', return_value=0):
            mock_response = pd.DataFrame(
                {MagTableKey.COL_YEAR: [5], FosLevelCountYearModule.BQ_COUNT: [4], MagTableKey.COL_LEVEL: [0]
                 })

            with patch('observatory.dags.dataquality.mod.mag_foslevelcountyear.pd.read_gbq',
            return_value=mock_response):
                with patch('observatory.dags.dataquality.mod.mag_foslevelcountyear.bulk_index') as mock_bulk:
                    module.run()
                    self.assertEqual(mock_bulk.call_count, 1)
                    self.assertEqual(len(mock_bulk.call_args_list[0][0][0]), 1)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].release, datetime.date(1990, 1, 1))
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].level, 0)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].count, 4)
                    self.assertEqual(mock_bulk.call_args_list[0][0][0][0].year, '5')


class TestFosL0ScoreFieldYearModule(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = AutoFetchCache(2)

