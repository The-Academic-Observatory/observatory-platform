import unittest
import pandas as pd
import os.path

# From charts.py
from academic_observatory.analysis.charts import ScatterPlot
from academic_observatory.analysis.charts import TimePlot
from academic_observatory.analysis.charts import TimePlotLayout
from academic_observatory.analysis.charts import TimePath
from academic_observatory.analysis.charts import Layout

# From rankings.py
from academic_observatory.analysis.charts import RankChart
from academic_observatory.analysis.charts import ConfidenceIntervalRank
from academic_observatory.analysis.charts import BoxScatter

# From reports.py
from academic_observatory.analysis.charts import OutputTypesPieChart
from academic_observatory.analysis.charts import GenericTimeChart
from academic_observatory.analysis.charts import OutputTypesTimeChart
from academic_observatory.analysis.charts import OApcTimeChart
from academic_observatory.analysis.charts import CitationCountTimeChart
from academic_observatory.analysis.charts import OAAdvantageBarChart
from academic_observatory.analysis.charts import BarComparisonChart
from academic_observatory.analysis.charts import FunderGraph

from academic_observatory.analysis.helpers import calculate_confidence_interval


class MockTestClass:
    """Convenient Mock Test Class to Remove pylint Errors"""

    def __init__(self, df, **init_args: dict):
        pass

    def process_data(self, df, **plot_args):
        pass

    def plot(self, df, **plot_args):
        pass


class abstract_test_chart(unittest.TestCase):
    test_class = MockTestClass
    init_args = {}
    plot_args = {}
    test_data = 'test_data/test_oa_data.csv'

    def setUp(self):
        mod_dir = os.path.dirname(__file__)
        test_data_file = os.path.join(mod_dir, self.test_data)
        self.df = pd.read_csv(test_data_file)

    def test_init(self):
        if self.test_class != MockTestClass:
            self.chart = self.test_class(self.df, **self.init_args)

    def test_process(self):
        if self.test_class != MockTestClass:
            self.chart = self.test_class(self.df, **self.init_args)
            self.chart.process_data()

    def test_plot(self):
        if self.test_class != MockTestClass:
            self.chart = self.test_class(self.df, **self.init_args)
            self.chart.process_data()
            self.chart.plot(**self.plot_args)


class test_ScatterPlot(abstract_test_chart):

    def setUp(self):
        self.test_class = ScatterPlot
        self.init_args = {
            'x': 'percent_green',
            'y': 'percent_gold',
            'filter_name': 'published_year',
            'filter_value': 2017
        }
        self.plot_args = {}
        super().setUp()


class test_TimePlot(abstract_test_chart):

    def setUp(self):
        self.test_class = TimePlot
        self.init_args = {
            'year_range': (2010, 2018),
            'unis': ['grid.1032.0',
                     'grid.20861.3d',
                     'grid.4991.5',
                     'grid.6571.5'],
            'plot_column': 'percent_total_oa'
        }
        self.plot_args = {}
        super().setUp()


class test_TimePlotLayout(abstract_test_chart):

    def setUp(self):
        self.test_class = TimePlotLayout
        self.init_args = {
            'plots': [
                {'year_range': (2010, 2018),
                 'unis': ['grid.1032.0',
                          'grid.20861.3d',
                          'grid.4991.5',
                          'grid.6571.5'],
                 'y_column': 'percent_total_oa'},
                {'year_range': (2010, 2018),
                 'unis': ['grid.1032.0',
                          'grid.20861.3d',
                          'grid.6571.5'],
                 'y_column': 'percent_gold'},
                {'year_range': (2010, 2018),
                 'unis': ['grid.1032.0',
                          'grid.20861.3d',
                          'grid.4991.5',
                          'grid.6571.5'],
                 'y_column': 'percent_green'}
            ]
        }
        self.plot_args = {}
        super().setUp()


class test_TimePath(abstract_test_chart):

    def setUp(self):
        self.test_class = TimePath
        self.init_args = {
            'year_range': (2010, 2018),
            'unis': ['grid.1032.0',
                     'grid.20861.3d',
                     'grid.4991.5',
                     'grid.6571.5'],
            'x': 'percent_gold',
            'y': 'percent_green'
        }
        self.plot_args = {}
        super().setUp()


class test_Layout(abstract_test_chart):
    pass


class test_RankChart(abstract_test_chart):
    def setUp(self):
        self.test_class = RankChart
        self.init_args = {
            'rankcol': 'percent_gold',
            'filter_name': 'published_year',
            'filter_value': 2017
        }
        self.plot_args = {}
        super().setUp()


class test_ConfidenceInternalRankChart(abstract_test_chart):
    def setUp(self):
        self.test_class = ConfidenceIntervalRank
        self.init_args = {
            'rankcol': 'percent_total_oa',
            'errorcol': 'percent_total_oa_err',
            'filter_name': 'published_year',
            'filter_value': 2017
        }
        self.plot_args = {}
        super().setUp()


class test_BoxScatter(abstract_test_chart):
    def setUp(self):
        self.test_class = BoxScatter
        self.init_args = {
            'year': 2017,
            'group_column': 'country',
            'plot_column': 'percent_total_oa'
        }
        self.plot_args = {}
        super().setUp()


class test_OutputTypesPieChart(abstract_test_chart):
    def setUp(self):
        self.test_class = OutputTypesPieChart
        self.init_args = {
            'identifier': 'grid.1032.0',
            'focus_year': 2018
        }
        self.test_data = 'test_data/test_outputs_data.csv'
        self.plot_args = {}
        super().setUp()


class test_GenericTimeChart(abstract_test_chart):
    def setUp(self):
        self.test_class = GenericTimeChart
        self.init_args = {
            'columns': ['percent_total_oa', 'percent_green', 'percent_gold'],
            'identifier': 'grid.1032.0'
        }
        self.plot_args = {}
        super().setUp()


class test_OutputTypesTimeChart(abstract_test_chart):
    def setUp(self):
        self.test_class = OutputTypesTimeChart
        self.init_args = {
            'identifier': 'grid.1032.0'
        }
        self.test_data = 'test_data/test_outputs_data.csv'
        self.plot_args = {}
        super().setUp()


class test_OApcTimeChart(abstract_test_chart):
    def setUp(self):
        self.test_class = OApcTimeChart
        self.init_args = {
            'identifier': 'grid.1032.0'
        }
        self.plot_args = {}
        super().setUp()


class test_CitationCountTimeChart(abstract_test_chart):
    def setUp(self):
        self.test_class = CitationCountTimeChart
        self.init_args = {
            'identifier': 'grid.1032.0'
        }
        self.plot_args = {}
        super().setUp()


class test_OAAdvantageBarChart(abstract_test_chart):
    def setUp(self):
        self.test_class = OAAdvantageBarChart
        self.init_args = {
            'focus_year': 2017,
            'identifier': 'grid.1032.0'
        }
        self.plot_args = {}
        super().setUp()


class test_BarComparisonChart(abstract_test_chart):
    def setUp(self):
        self.test_class = BarComparisonChart
        self.init_args = {
            'comparison': ['grid.1032.0',
                           'grid.20861.3d',
                           'grid.4991.5',
                           'grid.6571.5'],
            'focus_year': 2017,
        }
        self.plot_args = {}
        super().setUp()


class test_FunderGraph(abstract_test_chart):
    def setUp(self):
        self.test_class = FunderGraph
        self.init_args = {
            'focus_year': 2018,
            'identifier': 'grid.1032.0'
        }
        self.test_data = 'test_data/test_funding_data.csv'
        self.plot_args = {}
        super().setUp()
