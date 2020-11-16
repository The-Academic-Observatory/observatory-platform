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

# Author: Cameron Neylon & Richard Hosking

import os.path
import unittest

import pandas as pd

from tests.observatory.test_utils import test_fixtures_path
from observatory.reports.charts import BarComparisonChart
from observatory.reports.charts import BoxScatter
from observatory.reports.charts import CitationCountTimeChart
from observatory.reports.charts import ConfidenceIntervalRank
from observatory.reports.charts import DistributionComparisonChart
from observatory.reports.charts import FunderGraph
from observatory.reports.charts import GenericTimeChart
from observatory.reports.charts import OAAdvantageBarChart
from observatory.reports.charts import OApcTimeChart
from observatory.reports.charts import OutputTypesPieChart
from observatory.reports.charts import OutputTypesTimeChart
from observatory.reports.charts import RankChart
from observatory.reports.charts import ScatterPlot
from observatory.reports.charts import TimePath
from observatory.reports.charts import TimePlot
from observatory.reports.charts import TimePlotLayout


class TestAbstractChart(unittest.TestCase):
    test_class = None
    init_args = {}
    plot_args = {}
    test_data = 'test_oa_data.csv'

    def setUp(self):
        test_data_file = os.path.join(test_fixtures_path(), 'reports', self.test_data)
        self.df = pd.read_csv(test_data_file)
        self.available_ids = list(self.df.id.unique())

    def test_init(self):
        if self.test_class:
            self.chart = self.test_class(self.df, **self.init_args)

    def test_process(self):
        if self.test_class:
            self.chart = self.test_class(self.df, **self.init_args)
            self.chart.process_data()

    def test_plot(self):
        if self.test_class:
            self.chart = self.test_class(self.df, **self.init_args)
            self.chart.process_data()
            self.chart.plot(**self.plot_args)


class TestAbstractChartWithAnimation(TestAbstractChart):
    animate_args = {}

    # def test_animation(self):
    #     if self.test_class:
    #         self.chart = self.test_class(self.df, **self.init_args)
    #         self.chart.process_data()
    #         self.chart.animate(**self.animate_args)


class TestScatterPlot(TestAbstractChartWithAnimation):

    def setUp(self):
        super().setUp()
        self.test_class = ScatterPlot
        self.init_args = {
            'x': 'percent_green',
            'y': 'percent_gold',
            'filter_name': 'published_year',
            'filter_value': 2017
        }
        self.plot_args = {}


class TestTimePlot(TestAbstractChart):

    def setUp(self):
        super().setUp()
        self.test_class = TimePlot
        self.init_args = {
            'year_range': (2016, 2020),
            'unis': self.available_ids[0:4],
            'plot_column': 'percent_oa'
        }
        self.plot_args = {}


class TestTimePlotLayout(TestAbstractChart):

    def setUp(self):
        super().setUp()
        self.test_class = TimePlotLayout
        self.init_args = {
            'plots': [
                {'year_range': (2016, 2020),
                 'unis': self.available_ids[0:4],
                 'y_column': 'percent_oa'},
                {'year_range': (2016, 2020),
                 'unis': self.available_ids[0:3],
                 'y_column': 'percent_gold'},
                {'year_range': (2016, 2020),
                 'unis': self.available_ids[0:4],
                 'y_column': 'percent_green'}
            ]
        }
        self.plot_args = {}


class TestTimePath(TestAbstractChartWithAnimation):

    def setUp(self):
        super().setUp()
        self.test_class = TimePath
        self.init_args = {
            'year_range': (2016, 2020),
            'unis': self.available_ids[0:4],
            'x': 'percent_gold',
            'y': 'percent_green'
        }
        self.plot_args = {}


class TestLayout(TestAbstractChart):
    pass


class TestRankChart(TestAbstractChart):
    def setUp(self):
        super().setUp()
        self.test_class = RankChart
        self.init_args = {
            'rankcol': 'percent_gold',
            'filter_name': 'published_year',
            'filter_value': 2017
        }
        self.plot_args = {}


class TestConfidenceInternalRankChart(TestAbstractChart):
    def setUp(self):
        super().setUp()
        self.test_class = ConfidenceIntervalRank
        self.init_args = {
            'rankcol': 'percent_oa',
            'errorcol': 'percent_oa_err',
            'filter_name': 'published_year',
            'filter_value': 2017
        }
        self.plot_args = {}


class TestBoxScatter(TestAbstractChart):
    def setUp(self):
        super().setUp()
        self.test_class = BoxScatter
        self.init_args = {
            'year': 2017,
            'group_column': 'country',
            'plot_column': 'percent_oa'
        }
        self.plot_args = {}


class TestOutputTypesPieChart(TestAbstractChart):
    def setUp(self):
        self.test_data = 'test_outputs_data.csv'
        super().setUp()
        self.test_class = OutputTypesPieChart
        self.init_args = {
            'identifier': self.available_ids[0],
            'focus_year': 2018
        }
        self.plot_args = {}


class TestGenericTimeChart(TestAbstractChart):
    def setUp(self):
        super().setUp()
        self.test_class = GenericTimeChart
        self.init_args = {
            'columns': ['percent_oa', 'percent_green', 'percent_gold'],
            'identifier': self.available_ids[0]
        }
        self.plot_args = {}


class TestOutputTypesTimeChart(TestAbstractChart):
    def setUp(self):
        self.test_data = 'test_outputs_data.csv'
        super().setUp()
        self.test_class = OutputTypesTimeChart
        self.init_args = {
            'identifier': self.available_ids[0]
        }
        self.plot_args = {}


class TestOApcTimeChart(TestAbstractChart):
    def setUp(self):
        super().setUp()
        self.test_class = OApcTimeChart
        self.init_args = {
            'identifier': self.available_ids[0]
        }
        self.plot_args = {}


class TestCitationCountTimeChart(TestAbstractChart):
    def setUp(self):
        self.test_data = 'test_citations_data.csv'
        super().setUp()
        self.test_class = CitationCountTimeChart
        self.init_args = {
            'identifier': self.available_ids[0]
        }
        self.plot_args = {}


class TestOAAdvantageBarChart(TestAbstractChart):
    def setUp(self):
        self.test_data = 'test_citations_data.csv'
        super().setUp()
        self.test_class = OAAdvantageBarChart
        self.init_args = {
            'focus_year': 2017,
            'identifier': self.available_ids[0]
        }
        self.plot_args = {}


class TestBarComparisonChart(TestAbstractChart):
    def setUp(self):
        super().setUp()
        self.test_class = BarComparisonChart
        self.init_args = {
            'comparison': self.available_ids[0:5],
            'focus_year': 2017,
        }
        self.plot_args = {}


class TestFunderGraph(TestAbstractChart):
    def setUp(self):
        self.test_data = 'test_funding_data.csv'
        super().setUp()
        self.test_class = FunderGraph
        self.init_args = {
            'focus_year': 2018,
            'identifier': self.available_ids[0]
        }

        self.plot_args = {}


class TestDistributionComparisonChart(TestAbstractChart):
    def setUp(self):
        super().setUp()
        self.test_class = DistributionComparisonChart
        self.init_args = {
            'focus_year': 2018,
            'identifier': self.available_ids[0],
            'plot_column': 'percent_oa',
            'comparison': self.available_ids[0:4]
        }
        self.plot_args = {}
