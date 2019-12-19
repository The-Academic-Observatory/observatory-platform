from academic_observatory.analysis.charts import *
import pandas as pd

test_data = pd.read_csv('/Users/266883j/Documents/academic-observatory/academic_observatory/analysis/tests/example_data.csv')

params = [
            {
            'chart_class': TimePlot,
            'year_range': (2007,2019),
            'unis': ['grid.5335.0','grid.7445.2','grid.83440.3b', 'grid.4425.7', 
              'grid.6571.5','grid.11918.30','grid.5477.1', 'grid.7177.6', 
              'grid.6214.1','grid.6852.9'],
            'plot_column': 'percent_gold'
            },
            {
            'chart_class': TimePlot,
            'year_range': (2007,2019),
            'unis': ['grid.5335.0','grid.7445.2','grid.83440.3b', 'grid.4425.7', 
              'grid.6571.5','grid.11918.30','grid.5477.1', 'grid.7177.6', 
              'grid.6214.1','grid.6852.9'],
            'plot_column': 'percent_green'
            },
            {
            'chart_class': TimePlot,
            'year_range': (2007,2019),
            'unis': ['grid.5335.0','grid.7445.2','grid.83440.3b', 'grid.4425.7', 
              'grid.6571.5','grid.11918.30','grid.5477.1', 'grid.7177.6', 
              'grid.6214.1','grid.6852.9'],
            'plot_column': 'total'
            }
]

tp = Layout(test_data, params)
tp.process_data()
tp.plot(wspace=1.36)