import unittest
from academic_observatory.analysis.charts import ScatterPlot
from academic_observatory.analysis.charts import BarComparisonChart
from academic_observatory.analysis.charts import RankChart
# TODO Tests for all the chart types ##
import pandas as pd
import os.path


class abstract_test_chart(unittest.TestCase):
    test_class = None

    def setUp(self):
        mod_dir = os.path.dirname(__file__)
        test_data_file = os.path.join(mod_dir, 'test_data/test_data.csv')
        self.df = pd.read_csv(test_data_file)

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


class test_BarComparisonChart(abstract_test_chart):

    def setUp(self):
        self.test_class = BarComparisonChart
        self.init_args = {
            'comparison': ['grid.1032.0',
                           'grid.20861.3d',
                           'grid.4991.5',
                           'grid.6571.5'],
            'year': 2017,
        }
        self.plot_args = {}
        super().setUp()


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
