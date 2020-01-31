import unittest
from academic_observatory.analysis.charts import ScatterPlot
from academic_observatory.analysis.charts import BarComparisonChart
from academic_observatory.analysis.charts import RankChart
# TODO Tests for all the chart types ##
import pandas as pd


class abstract_test_chart(unittest.TestCase):
    test_class = None
    init_args = {}
    plot_args = {}

    def setUp(self):
        self.df = pd.read_csv('testdata/test_data.csv')

    def test_init(self):
        self.chart = test_class(self.df, **init_args)

    def test_process(self):
        self.chart = test_class(self.df, **init_args)

    def test_plot(self):
        self.chart = test_class(self.df, **init_args)
        self.chart.process_data()
        self.chart.plot(**plot_args)


class test_ScatterPlot(abstract_test_chart):
    test_class = ScatterPlot
    init_args = {
        'x': 'percent_green',
        'y': 'percent_gold',
        'filter_name': 'published_year',
        'filter_value': 2017
    }


class test_BarComparisonChart(abstract_test_chart):
    test_class = BarComparisonChart
    init_args = {
        'comparison': ['grid.1032.0',
                       'grid.20861.3d',
                       'grid.4991.5',
                       'grid.6571.5'],
        'year': 2017,
    }


class test_RankChart(abstract_test_chart):
    test_class = RankChart
    init_args = {
        'rankcol': 'percent_gold',
        'filter_name': 'published_year',
        'filter_value': 2017
    }
